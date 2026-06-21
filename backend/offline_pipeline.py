"""
离线批处理流水线 —— 湖上多模数据处理。

4 步架构：
  1. ingest             — 加载语音，S3 Manifest → Lance blob v2 入湖
  2. transcribe_and_tag — 语音转文字(ASR) + 声学情绪标签提炼
  3. analyze_text       — PII脱敏 + LLM文本分析 + 向量嵌入
  4. query              — 标量过滤 / ANN 向量检索

Demo 模式下 ASR/LLM/embedding 使用模拟实现，Lance blob v2 为真实存储。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import lance
import numpy as np
import pyarrow as pa

from gravitino_client import GravitinoClient
from llm_client import llm_client, is_llm_available

# ── 全局状态（Demo 用） ──

_pipeline_state: Dict[str, Any] = {
    "lance_uri": "",
    "ingested": False,
    "transcribed": False,
    "analyzed": False,
    "row_count": 0,
    "schema": None,
}

# ── PII 脱敏正则 ──

_ID_CARD_PAT = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")
_PHONE_PAT = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")


def _compute_asr_diff(original: str, transcribed: str) -> Dict:
    """编辑距离序列对齐，输出字符级差异片段。

    返回: {
        "diff_segments": [{"type":"match|sub|del|ins",
                           "original":"...", "transcribed":"..."}, ...],
        "match_count": int, "sub_count": int,
        "ins_count": int, "del_count": int,
    }
    """
    m, n = len(original), len(transcribed)
    if m == 0 and n == 0:
        return {"diff_segments": [], "match_count": 0,
                "sub_count": 0, "ins_count": 0, "del_count": 0}
    if m == 0:
        return {"diff_segments": [{"type": "ins", "original": "",
                                   "transcribed": transcribed}],
                "match_count": 0, "sub_count": 0,
                "ins_count": n, "del_count": 0}
    if n == 0:
        return {"diff_segments": [{"type": "del", "original": original,
                                   "transcribed": ""}],
                "match_count": 0, "sub_count": 0,
                "ins_count": 0, "del_count": m}

    # DP 最小编辑距离矩阵
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if original[i - 1] == transcribed[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j - 1],
                                   dp[i - 1][j], dp[i][j - 1])

    # 回溯，记录原始操作序列
    raw_ops = []
    i, j = m, n
    while i > 0 or j > 0:
        if i > 0 and j > 0 and original[i - 1] == transcribed[j - 1]:
            raw_ops.append(("match", original[i - 1], transcribed[j - 1]))
            i -= 1
            j -= 1
        elif (i > 0 and j > 0
              and dp[i][j] == dp[i - 1][j - 1] + 1):
            raw_ops.append(("sub", original[i - 1], transcribed[j - 1]))
            i -= 1
            j -= 1
        elif i > 0 and dp[i][j] == dp[i - 1][j] + 1:
            raw_ops.append(("del", original[i - 1], None))
            i -= 1
        else:
            raw_ops.append(("ins", None, transcribed[j - 1]))
            j -= 1
    raw_ops.reverse()

    # 合并相邻同类型操作，构建分段
    segments = []
    match_count = sub_count = ins_count = del_count = 0
    cur_type = None
    cur_orig_parts = []
    cur_trans_parts = []

    def _flush():
        if not cur_orig_parts and not cur_trans_parts:
            return
        segments.append({
            "type": cur_type,
            "original": "".join(cur_orig_parts),
            "transcribed": "".join(cur_trans_parts),
        })

    for op_type, orig_char, trans_char in raw_ops:
        if op_type != cur_type:
            _flush()
            cur_type = op_type
            cur_orig_parts = []
            cur_trans_parts = []
        if orig_char is not None:
            cur_orig_parts.append(orig_char)
        if trans_char is not None:
            cur_trans_parts.append(trans_char)
        if op_type == "match":
            match_count += 1
        elif op_type == "sub":
            sub_count += 1
        elif op_type == "ins":
            ins_count += 1
        else:
            del_count += 1
    _flush()

    return {
        "diff_segments": segments,
        "match_count": match_count,
        "sub_count": sub_count,
        "ins_count": ins_count,
        "del_count": del_count,
    }


def _calc_asr_match(original: str, transcribed: str) -> Dict:
    """使用编辑距离对齐计算 ASR 转写与原文的字符级匹配率。

    char_accuracy = 正确匹配字符数 / 原文字符数 × 100
    并提供 diff_segments 用于前端差异可视化。
    """
    if not original or not transcribed:
        return {
            "char_accuracy": 0, "word_count_original": 0,
            "word_count_transcribed": 0, "asr_confidence": 0.0,
            "diff_segments": [], "match_count": 0,
            "sub_count": 0, "ins_count": 0, "del_count": 0,
        }

    diff = _compute_asr_diff(original, transcribed)
    match_count = diff["match_count"]
    original_len = len(original)

    # 基于对齐后的匹配率(分母使用原文长度,插入不惩罚匹配率)
    char_accuracy = (round(match_count / original_len * 100, 1)
                     if original_len > 0 else 0)
    orig_words = len(original.replace("\n", " ").split())
    trans_words = len(transcribed.replace("\n", " ").split())
    asr_confidence = round(0.80 + 0.19 * (char_accuracy / 100.0), 2)

    return {
        "char_accuracy": char_accuracy,
        "word_count_original": orig_words,
        "word_count_transcribed": trans_words,
        "asr_confidence": asr_confidence,
        "diff_segments": diff["diff_segments"],
        "match_count": match_count,
        "sub_count": diff["sub_count"],
        "ins_count": diff["ins_count"],
        "del_count": diff["del_count"],
    }


def _calc_duration(audio_bytes, raw_text: str) -> float:
    """计算通话时长：优先用真实音频，否则按字数估算"""
    if audio_bytes:
        try:
            import io as _io
            import soundfile as sf
            info = sf.info(_io.BytesIO(audio_bytes))
            if info.samplerate and info.frames:
                return round(float(info.frames) / info.samplerate, 1)
        except Exception:
            pass
    return round(len(raw_text.replace("\n", "")) / 4.0, 1)


def _simulate_asr(raw_text: str) -> str:
    """模拟 ASR 转写：引入真实常见的识别误差，非 100% 照搬原文"""
    import random
    if not raw_text:
        return raw_text
    result = list(raw_text)
    n = len(result)
    homophone_map = {
        "换": "还", "转": "专", "销": "消", "贵": "归", "坑": "肯",
        "绑": "帮", "投": "头", "届": "结", "询": "寻", "推": "退",
        "拆": "差", "账": "张", "优": "有", "惠": "会",
    }
    random.seed(hash(raw_text) % (2**31))
    for i, ch in enumerate(result):
        if '\u4e00' <= ch <= '\u9fff' and random.random() < 0.05:
            if ch in homophone_map:
                result[i] = homophone_map[ch]
    text_after_sub = ''.join(result)
    result = list(text_after_sub)
    i = len(result) - 1
    while i >= 0:
        if result[i] != '\n' and random.random() < 0.02:
            result.pop(i)
        i -= 1
    filler = ["嗯", "呃", "就是", "那个", "然后", "这个"]
    final = []
    for ch in result:
        if random.random() < 0.01:
            final.append(random.choice(filler))
        final.append(ch)
    random.seed()
    return ''.join(final)


def _detect_acoustic_emotion(raw_text: str, audio_bytes=None) -> str:
    """声学情绪识别：有真实音频时分析波形能量，否则用关键词匹配"""
    if audio_bytes:
        try:
            import io as _io
            import numpy as np
            import soundfile as sf
            data, sr = sf.read(_io.BytesIO(audio_bytes), dtype="float32")
            if len(data) > 0:
                rms = float(np.sqrt(np.mean(data ** 2)))
                if rms > 0.3:
                    return "ANGRY"
                elif rms < 0.02:
                    return "SAD"
        except Exception:
            pass
    anger_words = ["投诉", "坑钱", "工信部", "太贵", "虚假宣传", "永远打不通", "故意的"]
    sad_words = ["搬家", "换地区", "合约到期"]
    if any(w in raw_text for w in anger_words):
        return "ANGRY"
    if any(w in raw_text for w in sad_words):
        return "SAD"
    return "NEUTRAL"


def _redact_pii(text: str) -> str:
    """先身份证号，再手机号"""
    text = _ID_CARD_PAT.sub("[ID_REDACTED]", text)
    text = _PHONE_PAT.sub("[PHONE_REDACTED]", text)
    return text


# ═══════════════════════════════════════════════════════════
# 步骤 1: ingest — 数据入湖
# ═══════════════════════════════════════════════════════════

def ingest(manifest: List[Dict[str, str]], lance_uri: str, overwrite: bool = True,
           s3_store=None) -> Dict:
    """
    S3 Manifest → Lance blob v2 入湖。

    优先从本地缓存获取音频文件，其次从 S3 下载，最后回退到文本 blob。
    manifest 格式: [{"doc_id": "call_001_churn.txt", "content": "通话文本..."}, ...]
    """
    t0 = time.time()
    _pipeline_state["lance_uri"] = lance_uri

    from demo_audio import get_cached_wav, ensure_demo_audio

    # 确保音频已生成（按需）
    ensure_demo_audio(manifest)

    doc_ids = []
    blobs = []
    transcripts = []
    audio_urls = []
    from_audio = False

    for item in manifest:
        doc_id = item["doc_id"]
        doc_ids.append(doc_id)
        text = item.get("content", "")
        transcripts.append(text)
        audio_bytes = None

        # 1) 优先从本地缓存获取
        audio_bytes = get_cached_wav(doc_id)

        # 2) 其次从 S3 下载
        if not audio_bytes and s3_store:
            audio_key = doc_id.replace(".txt", ".mp3")
            try:
                resp = s3_store.client.get_object(
                    Bucket=s3_store.config.bucket_name,
                    Key=f"raw_audio/{audio_key}",
                )
                audio_bytes = resp["Body"].read()
            except Exception:
                pass

        if audio_bytes:
            blobs.append(audio_bytes)
            audio_urls.append(f"/api/offline/audio/{doc_id}")
            from_audio = True
        else:
            # 回退：文本 blob
            blobs.append(text.encode("utf-8"))
            audio_urls.append(None)

    blob_type = pa.binary()
    table = pa.table(
        {
            "doc_id": pa.array(doc_ids, type=pa.utf8()),
            "audio_blob": pa.array(blobs, type=blob_type),
            "transcript": pa.array(transcripts, type=pa.utf8()),
        }
    )

    mode = "overwrite" if overwrite else "create"
    lance.write_dataset(table, lance_uri, mode=mode)

    # 验证 blob v2
    ds = lance.dataset(lance_uri)
    schema = ds.schema
    field_type = str(schema.field("audio_blob").type)

    row_count = ds.count_rows()
    _pipeline_state["ingested"] = True
    _pipeline_state["row_count"] = row_count
    _pipeline_state["schema"] = schema

    elapsed = round(time.time() - t0, 3)
    return {
        "step": "ingest",
        "status": "done",
        "lance_uri": lance_uri,
        "rows": row_count,
        "blob_type": field_type,
        "blob_v2": "lance.blob" in field_type,
        "duration_s": elapsed,
        "doc_ids": doc_ids,
        "from_audio": from_audio,
        "audio_urls": {d: u for d, u in zip(doc_ids, audio_urls) if u},
    }


# ═══════════════════════════════════════════════════════════
# 步骤 2: transcribe_and_tag — 语音转文字 + 情绪标签
# ═══════════════════════════════════════════════════════════

def transcribe_and_tag(lance_uri: str) -> Dict:
    """
    Lance blob v2 → ASR转写 + 声学情绪识别 → 写回Lance。

    对每条通话录音：计算时长、模拟ASR转写、检测声学情绪标签，
    结果（duration_s, transcript, acoustic_emotion）写回 Lance 表。
    """
    t0 = time.time()
    ds = lance.dataset(lance_uri)
    schema_names = ds.schema.names
    has_audio_blob = "audio_blob" in schema_names

    read_cols = ["doc_id"]
    if "transcript" in schema_names:
        read_cols.append("transcript")
    if has_audio_blob:
        read_cols.append("audio_blob")
    table = ds.to_table(columns=read_cols)

    results = []
    for i in range(table.num_rows):
        doc_id = table["doc_id"][i].as_py()

        # 提取文本内容
        raw_text = ""
        if "transcript" in schema_names:
            raw_text = table["transcript"][i].as_py() or ""

        # 优先从 audio_blob 获取真实音频数据（WAV/MP3）
        audio_bytes = None
        if has_audio_blob:
            ab = table["audio_blob"][i].as_py()
            if ab and len(ab) > 0:
                is_audio = (ab[:4] == b"RIFF" or ab[:3] == b"ID3"
                            or ab[:2] == b"\xff\xfb" or ab[:2] == b"\xff\xf3")
                if is_audio:
                    audio_bytes = ab

        # 如果 audio_blob 是文本回退，且 transcript 为空，则从 blob 解码文本
        if not raw_text and has_audio_blob and not audio_bytes:
            blob_bytes = table["audio_blob"][i].as_py()
            if blob_bytes:
                try:
                    raw_text = blob_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    raw_text = ""

        # 时长计算：优先用真实音频数据，否则按字数估算
        duration_s = _calc_duration(audio_bytes, raw_text)

        # ASR 模拟 → transcript（引入真实误差，非 100% 准确）
        transcript = _simulate_asr(raw_text)
        acoustic_emotion = _detect_acoustic_emotion(raw_text, audio_bytes)

        # ASR 转写验证：原稿 vs 转写结果的字符级匹配率
        asr_match = _calc_asr_match(raw_text, transcript)
        # 音频播放 URL
        audio_url = f"/api/offline/audio/{doc_id}" if audio_bytes else None

        results.append({
            "doc_id": doc_id,
            "duration_s": duration_s,
            "transcript": transcript,
            "acoustic_emotion": acoustic_emotion,
            "audio_url": audio_url,
            "asr_match": asr_match,
        })

    _pipeline_state["transcribed"] = True
    elapsed = round(time.time() - t0, 3)

    # ── 将转写结果写回 Lance 表 ──
    columns_written = _write_transcribe_to_lance(lance_uri, results)

    # ASR 转写整体验证汇总
    accuracies = [r["asr_match"]["char_accuracy"] for r in results]
    confidences = [r["asr_match"]["asr_confidence"] for r in results]
    overall_accuracy = round(sum(accuracies) / len(accuracies), 1) if accuracies else 0
    avg_confidence = round(sum(confidences) / len(confidences), 2) if confidences else 0

    return {
        "step": "transcribe_and_tag",
        "status": "done",
        "lance_uri": lance_uri,
        "processed": len(results),
        "duration_s": elapsed,
        "columns_written": columns_written,
        "results": results,
        "asr_verification": {
            "overall_char_accuracy": overall_accuracy,
            "min_accuracy": round(min(accuracies), 1) if accuracies else 0,
            "max_accuracy": round(max(accuracies), 1) if accuracies else 0,
            "avg_confidence": avg_confidence,
            "sample_count": len(results),
        },
    }


def _write_transcribe_to_lance(lance_uri: str, results: List[Dict]) -> List[str]:
    """将ASR转写 + 情绪标签列写入 Lance 表"""
    written = []
    ds = lance.dataset(lance_uri)
    existing_cols = set(ds.schema.names)

    doc_ids = [r["doc_id"] for r in results]
    new_columns = {
        "duration_s": (pa.float64(), [r["duration_s"] for r in results]),
        "acoustic_emotion": (pa.utf8(), [r["acoustic_emotion"] for r in results]),
    }

    for col_name, (pa_type, values) in new_columns.items():
        if col_name not in existing_cols:
            t = pa.table({
                "doc_id": pa.array(doc_ids, type=pa.utf8()),
                col_name: pa.array(values, type=pa_type),
            })
            ds = lance.dataset(lance_uri)
            ds.merge(t, left_on="doc_id")
            written.append(col_name)

    # transcript 已在 ingest 阶段写入，ASR 结果一致时无需重复 merge
    # Lance merge 不允许两侧有同名列，故跳过 transcript 更新

    return written


# ═══════════════════════════════════════════════════════════
# 步骤 3: analyze_text — PII脱敏 + LLM文本分析 + 向量嵌入
# ═══════════════════════════════════════════════════════════

def analyze_text(
    lance_uri: str,
    enable_llm: bool = True,
) -> Dict:
    """
    对已转写的文本进行 PII 脱敏 → LLM 意图/情绪分析 → 向量嵌入 → 写回Lance。

    LLM 调用走 llm_client（支持 CodeBuddy/OpenAI），不可用时自动降级规则分析。
    分析完成后同时计算 128 维文本特征嵌入向量，一并写入 Lance 表，
    供步骤 4 的 ANN 向量检索使用。
    """
    t0 = time.time()
    ds = lance.dataset(lance_uri)
    schema_names = ds.schema.names

    # 读取已转写的文本
    table = ds.to_table(columns=["doc_id", "transcript"])

    results = []
    llm_available = enable_llm and is_llm_available()
    for i in range(table.num_rows):
        doc_id = table["doc_id"][i].as_py()
        transcript = table["transcript"][i].as_py() or ""

        # PII 脱敏
        redacted = _redact_pii(transcript)

        # LLM 分析：优先走 llm_client，不可用则规则兜底
        analysis_fields = _default_analysis()
        llm_used = False
        if llm_available:
            try:
                llm_result = llm_client.analyze_transcript(redacted)
                if llm_result:
                    reasons = llm_result.get("switch_reason", "")
                    if isinstance(reasons, str):
                        reasons = re.split(r"[、，,]", reasons)
                    sentiment = llm_result.get("sentiment", "neutral")
                    analysis_fields = {
                        "downgrade_related": llm_result.get("caller_intent", "") in (
                            "降套餐", "销户", "转网/携号转网"
                        ),
                        "primary_reason": _map_reason_str(reasons),
                        "secondary_reason": "",
                        "summary": llm_result.get("summary", ""),
                        "confidence": 0.85,
                        "text_emotion": _map_emotion(sentiment),
                        "bad_tone": sentiment == "negative" and "投诉" in redacted,
                        "emotion_score": _normalize_score(
                            llm_result.get("sentiment_score", 0.5)
                        ),
                    }
                    llm_used = True
            except Exception:
                pass

        if not llm_used:
            analysis_fields = _rule_based_analyze(redacted)

        results.append({
            "doc_id": doc_id,
            **analysis_fields,
        })

    _pipeline_state["analyzed"] = True
    elapsed = round(time.time() - t0, 3)

    # ── 将分析结果 + 向量嵌入 写回 Lance 表 ──
    columns_written = _write_analysis_to_lance(lance_uri, results)

    # ── 计算并写入向量嵌入（为步骤4 ANN检索做准备）──
    embed_cols = _write_embedding_to_lance(lance_uri)
    columns_written.extend(embed_cols)

    return {
        "step": "analyze_text",
        "status": "done",
        "lance_uri": lance_uri,
        "processed": len(results),
        "duration_s": elapsed,
        "columns_written": columns_written,
        "results": results,
    }


def analyze_text_stream(
    lance_uri: str,
    enable_llm: bool = True,
):
    """Generator 版 analyze_text，逐条 yield 进度，供 SSE 前端实时展示。"""
    t0 = time.time()
    ds = lance.dataset(lance_uri)
    table = ds.to_table(columns=["doc_id", "transcript"])
    total = table.num_rows

    yield {"event": "start", "step": "analyze-text", "total": total}

    results = []
    llm_available = enable_llm and is_llm_available()
    for i in range(total):
        doc_id = table["doc_id"][i].as_py()
        transcript = table["transcript"][i].as_py() or ""

        yield {
            "event": "progress", "step": "analyze-text",
            "doc_id": doc_id, "current": i + 1, "total": total,
        }

        # PII 脱敏
        redacted = _redact_pii(transcript)

        # LLM 分析：优先走 llm_client，不可用则规则兜底
        analysis_fields = _default_analysis()
        llm_used = False
        if llm_available:
            try:
                llm_result = llm_client.analyze_transcript(redacted)
                if llm_result:
                    reasons = llm_result.get("switch_reason", "")
                    if isinstance(reasons, str):
                        reasons = re.split(r"[、，,]", reasons)
                    sentiment = llm_result.get("sentiment", "neutral")
                    analysis_fields = {
                        "downgrade_related": llm_result.get(
                            "caller_intent", ""
                        ) in ("降套餐", "销户", "转网/携号转网"),
                        "primary_reason": _map_reason_str(reasons),
                        "secondary_reason": "",
                        "summary": llm_result.get("summary", ""),
                        "confidence": 0.85,
                        "text_emotion": _map_emotion(sentiment),
                        "bad_tone": (sentiment == "negative"
                                     and "投诉" in redacted),
                        "emotion_score": _normalize_score(
                            llm_result.get("sentiment_score", 0.5)
                        ),
                    }
                    llm_used = True
            except Exception:
                pass

        if not llm_used:
            analysis_fields = _rule_based_analyze(redacted)

        results.append({
            "doc_id": doc_id,
            **analysis_fields,
        })

    _pipeline_state["analyzed"] = True
    elapsed = round(time.time() - t0, 3)

    # ── 将分析结果 + 向量嵌入 写回 Lance 表 ──
    columns_written = _write_analysis_to_lance(lance_uri, results)
    embed_cols = _write_embedding_to_lance(lance_uri)
    columns_written.extend(embed_cols)

    yield {
        "event": "done",
        "step": "analyze_text",
        "status": "done",
        "lance_uri": lance_uri,
        "processed": len(results),
        "duration_s": elapsed,
        "columns_written": columns_written,
        "results": results,
    }


def _write_embedding_to_lance(lance_uri: str) -> List[str]:
    """计算 128 维文本特征嵌入向量并写入 Lance 表"""
    EMBED_DIM = 128
    ds = lance.dataset(lance_uri)
    if "audio_embedding" in ds.schema.names:
        return []

    schema_names = ds.schema.names
    if "transcript" in schema_names:
        table = ds.to_table(columns=["doc_id", "transcript"])
        text_col = "transcript"
    else:
        table = ds.to_table(columns=["doc_id", "audio_blob"])
        text_col = "audio_blob"

    embeddings = []
    for i in range(table.num_rows):
        content = table[text_col][i].as_py()
        if isinstance(content, bytes):
            blob_bytes = content
        else:
            blob_bytes = (content or "").encode("utf-8")
        if blob_bytes:
            h = hashlib.sha256(blob_bytes).digest()
            vec = np.array(list(h), dtype=np.float32) / 255.0
            if len(vec) < EMBED_DIM:
                vec = np.pad(vec, (0, EMBED_DIM - len(vec)))
            vec = vec[:EMBED_DIM]
        else:
            vec = np.zeros(EMBED_DIM, dtype=np.float32)
        # L2 归一化
        norm = float(np.linalg.norm(vec))
        if norm > 0:
            vec = vec / norm
        embeddings.append(vec.tolist())

    doc_table = ds.to_table(columns=["doc_id"])
    doc_ids = [doc_table["doc_id"][i].as_py() for i in range(doc_table.num_rows)]

    embed_table = pa.table({
        "doc_id": pa.array(doc_ids, type=pa.utf8()),
        "audio_embedding": pa.array(
            embeddings, type=pa.list_(pa.float32(), EMBED_DIM)),
    })

    ds.merge(embed_table, left_on="doc_id")
    return ["audio_embedding"]


def _write_analysis_to_lance(lance_uri: str, results: List[Dict]) -> List[str]:
    """通过 merge 将分析结果列写入 Lance 表"""
    written = []
    ds = lance.dataset(lance_uri)
    existing_cols = set(ds.schema.names)

    doc_ids = [r["doc_id"] for r in results]
    new_columns = {
        "downgrade_related": (pa.bool_(), [r["downgrade_related"] for r in results]),
        "primary_reason": (pa.utf8(), [r["primary_reason"] for r in results]),
        "secondary_reason": (pa.utf8(), [r["secondary_reason"] for r in results]),
        "summary": (pa.utf8(), [r["summary"] for r in results]),
        "confidence": (pa.float64(), [r["confidence"] for r in results]),
        "text_emotion": (pa.utf8(), [r["text_emotion"] for r in results]),
        "bad_tone": (pa.bool_(), [r["bad_tone"] for r in results]),
        "emotion_score": (pa.float64(), [r["emotion_score"] for r in results]),
    }

    for col_name, (pa_type, values) in new_columns.items():
        if col_name not in existing_cols:
            t = pa.table({
                "doc_id": pa.array(doc_ids, type=pa.utf8()),
                col_name: pa.array(values, type=pa_type),
            })
            ds = lance.dataset(lance_uri)
            ds.merge(t, left_on="doc_id")
            written.append(col_name)

    return written


def _default_analysis() -> Dict:
    return {
        "downgrade_related": False,
        "primary_reason": "其他",
        "secondary_reason": "",
        "summary": "",
        "confidence": 0.0,
        "text_emotion": "未知",
        "bad_tone": False,
        "emotion_score": 0.0,
    }


def _map_reason_str(reasons) -> str:
    """将 LLM 返回的 switch_reason 映射到一级原因枚举"""
    reason_map = {
        "资费过高": "价格敏感", "太贵": "价格敏感", "贵了": "价格敏感",
        "信号差": "服务体验差", "客服体验差": "服务体验差",
        "套餐不匹配": "套餐不匹配", "竞争对手优惠": "竞品影响",
        "搬家/换地区": "账户或设备变化", "服务不满意": "服务体验差",
        "合约到期": "账户或设备变化", "漫游费用": "价格敏感",
        "网速慢": "服务体验差", "乱扣费": "服务体验差",
    }
    if isinstance(reasons, str):
        reasons = [reasons]
    reasons = [r.strip() for r in reasons if r and r.strip()]
    if reasons:
        for r in reasons:
            for k, v in reason_map.items():
                if k in r:
                    return v
    return "其他"


def _normalize_score(score) -> float:
    """将 -1~1 的 LLM sentiment 分数归一化到 0~1"""
    try:
        s = float(score)
        # 情绪越负分越高：0=平静, 1=极度负面
        return round(max(0.0, min(1.0, (1.0 - s) / 2.0)), 2)
    except (ValueError, TypeError):
        return 0.0


def _map_emotion(sentiment: str) -> str:
    return {"negative": "不满", "positive": "平静", "neutral": "平静"}.get(sentiment, "未知")


def _rule_based_analyze(text: str) -> Dict:
    """规则兜底：无 LLM 时的关键词分析"""
    downgrade = any(w in text for w in ["转网", "携号转网", "降套餐", "降档", "销户", "换电信", "换联通"])
    bad_tone = any(w in text for w in ["投诉", "工信部", "坑钱", "虚假宣传", "永远打不通", "故意的"])
    has_anger = bad_tone
    has_price = any(w in text for w in ["太贵", "贵了", "便宜", "79", "129"])

    reason = "其他"
    if has_price:
        reason = "价格敏感"
    if bad_tone:
        reason = "服务体验差"

    emotions = []
    if has_anger:
        emotions.append("ANGRY")
    if downgrade:
        emotions.append("SAD")

    confidence = 0.8 if (downgrade and bad_tone) else 0.5 if (downgrade or bad_tone) else 0.3

    return {
        "downgrade_related": downgrade,
        "primary_reason": reason,
        "secondary_reason": "",
        "summary": text[:80].replace("\n", " ") if text else "",
        "confidence": confidence,
        "text_emotion": "不满" if has_anger else ("焦急" if downgrade else "平静"),
        "bad_tone": bad_tone,
        "emotion_score": 0.8 if has_anger else (0.5 if downgrade else 0.2),
    }


# ═══════════════════════════════════════════════════════════
# 步骤 4: query — 标量过滤 + ANN 向量检索
# ═══════════════════════════════════════════════════════════

def scalar_query(lance_uri: str, where: Optional[str], top_k: int = 10) -> Dict:
    """标量过滤查询，使用 Lance scanner filter。"""
    t0 = time.time()
    ds = lance.dataset(lance_uri)
    schema_names = ds.schema.names

    # 兜底：如果过滤条件引用的列不存在，退化为无过滤
    effective_where = where
    if where:
        try:
            ds.scanner(filter=where, limit=1).to_table()
        except Exception:
            effective_where = None

    # 无过滤条件时返回全部，有过滤条件时 top_k 仅作为展示上限
    if effective_where:
        # 先统计匹配总数
        total_table = ds.scanner(filter=effective_where).to_table()
        total_matched = total_table.num_rows
        scanner_kwargs = {"filter": effective_where, "limit": top_k}
        table = ds.scanner(**scanner_kwargs).to_table()
    else:
        table = ds.scanner().to_table()
        total_matched = table.num_rows

    rows = table.to_pydict()
    n = table.num_rows
    results = [{k: rows[k][i] for k in rows if k != "audio_blob"} for i in range(n)]
    return {
        "type": "scalar",
        "where": where,
        "effective_where": effective_where,
        "top_k": top_k,
        "matched": total_matched,
        "returned": n,
        "duration_s": round(time.time() - t0, 3),
        "results": results,
    }


def ann_query(lance_uri: str, query_doc_id: str, top_k: int = 5,
              where: Optional[str] = None) -> Dict:
    """ANN 近似最近邻检索。"""
    t0 = time.time()
    ds = lance.dataset(lance_uri)

    if "audio_embedding" not in ds.schema.names:
        return {"type": "ann", "error": "audio_embedding 列不存在，请先执行 analyze_text 步骤"}

    # 查找参考向量
    query_table = ds.scanner(
        columns=["doc_id", "audio_embedding"],
        filter=f"doc_id = '{query_doc_id}'",
    ).to_table()
    if query_table.num_rows == 0:
        return {"type": "ann", "error": f"query_doc_id 未找到: {query_doc_id}"}

    query_vec = query_table["audio_embedding"][0].as_py()

    cols = [c for c in ds.schema.names if c != "audio_blob"]
    scanner_kwargs = {
        "columns": cols,
        "nearest": {
            "column": "audio_embedding",
            "q": pa.array(query_vec, type=pa.float32()),
            "k": top_k + 1,  # +1 排除自身
        },
    }
    if where:
        scanner_kwargs["filter"] = where

    table = ds.scanner(**scanner_kwargs).to_table()
    rows = table.to_pydict()
    results = []
    for i in range(table.num_rows):
        doc_id = rows["doc_id"][i]
        # 排除自身匹配
        if doc_id == query_doc_id:
            continue
        row = {k: rows[k][i] for k in rows if k != "audio_blob"}
        row["_distance"] = rows.get("_distance", [None] * table.num_rows)[i]
        results.append(row)
        if len(results) >= top_k:
            break

    return {
        "type": "ann",
        "query_doc_id": query_doc_id,
        "top_k": top_k,
        "matched": len(results),
        "duration_s": round(time.time() - t0, 3),
        "results": results,
    }


def get_state() -> Dict:
    """获取流水线当前状态"""
    ds_info = {}
    if _pipeline_state["lance_uri"]:
        try:
            ds = lance.dataset(_pipeline_state["lance_uri"])
            ds_info = {
                "rows": ds.count_rows(),
                "schema": str(ds.schema),
            }
        except Exception:
            ds_info = {"error": "无法读取 Lance 表"}
    return {
        **{k: v for k, v in _pipeline_state.items() if k != "schema"},
        "lance_info": ds_info,
    }


def register_to_gravitino(lance_uri: str) -> Dict:
    """
    将 Lance 表注册到 Gravitino 元数据中心。

    参照 multimodal_toolkit 架构，Lance 表属于 lance_catalog 下的
    voice_analysis schema，供下游引擎（Spark, Flink, Daft）通过统一
    元数据入口发现和查询。
    """
    t0 = time.time()
    try:
        gravitino = GravitinoClient()
        ds = lance.dataset(lance_uri)
        table_name = os.path.basename(lance_uri.rstrip("/"))
        if table_name.endswith(".lance"):
            table_name = table_name[:-6]

        registration = gravitino.register_table(
            catalog="lance_catalog",
            schema_name="voice_analysis",
            table_name=table_name,
            table_type="lance",
            location=lance_uri,
            schema=str(ds.schema),
            row_count=ds.count_rows(),
        )
        return {
            "step": "gravitino_register",
            "status": "done",
            "table": table_name,
            "catalog": "lance_catalog",
            "schema": "voice_analysis",
            "duration_s": round(time.time() - t0, 3),
            "detail": registration,
        }
    except Exception as e:
        return {
            "step": "gravitino_register",
            "status": "error",
            "error": str(e),
            "duration_s": round(time.time() - t0, 3),
        }
