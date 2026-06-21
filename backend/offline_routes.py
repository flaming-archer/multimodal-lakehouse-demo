"""
离线批处理流水线 — FastAPI 路由。

4 步流水线：
  1. POST /api/offline/ingest         — 加载语音
  2. POST /api/offline/transcribe     — 语音转文字+情绪标签
  3. POST /api/offline/analyze-text   — PII脱敏+LLM分析+向量嵌入
  4. POST /api/offline/query          — 标量过滤/ANN向量检索
"""
from __future__ import annotations

import asyncio
import json
import lance
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, Response
from pydantic import BaseModel

from offline_pipeline import (
    analyze_text,
    analyze_text_stream,
    ann_query,
    get_state,
    ingest,
    register_to_gravitino,
    scalar_query,
    transcribe_and_tag,
)

router = APIRouter(prefix="/api/offline", tags=["离线批处理"])

# ── S3 实例（由 main.py startup 注入）──
_s3_store = None


def set_s3_store(store):
    global _s3_store
    _s3_store = store

# ── 默认 LANCE URI ──

DEFAULT_LANCE_URI = os.path.join(tempfile.gettempdir(), "offline_demo", "calls.lance")

# ── Demo Manifest 数据 ──

DEMO_MANIFEST = [
    {
        "doc_id": "call_001_churn.txt",
        "content": (
            "客服: 您好，客服中心，请问有什么可以帮您？\n"
            "用户: 我要转网。\n"
            "客服: 请问您为什么要转网呢？\n"
            "用户: 你们的套餐太贵了，一个月199，联通那边才129，"
            "而且信号也不好，我在朝阳区家里经常打不通电话。\n"
            "客服: 了解了，我帮您看看有没有更合适的套餐...\n"
            "用户: 不用了，我已经决定了，帮我办携号转网吧。"
        ),
    },
    {
        "doc_id": "call_002_downgrade.txt",
        "content": (
            "客服: 您好，很高兴为您服务。\n"
            "用户: 我想问一下我的套餐能不能降档。\n"
            "客服: 请问您现在是什么套餐呢？\n"
            "用户: 238的套餐，但是流量根本用不完，每个月就用了20G，太浪费了。\n"
            "客服: 我帮您看看有没有流量少一些的套餐。\n"
            "用户: 好，另外你们客服电话太难打了，等了快10分钟。"
        ),
    },
    {
        "doc_id": "call_003_complaint.txt",
        "content": (
            "客服: 您好，请问有什么可以帮您？\n"
            "用户: 我要投诉！你们乱扣我费用！\n"
            "客服: 不好意思，您能说具体一点吗？\n"
            "用户: 我这个月话费多了50块钱，我查了半天发现是开了个什么彩铃业务，"
            "我从来没开过！你们就是坑钱的！\n"
            "客服: 我帮您查一下业务开通记录...\n"
            "用户: 你们这样我要投诉到工信部去！"
        ),
    },
    {
        "doc_id": "call_004_retention.txt",
        "content": (
            "客服: 您好，请问有什么可以帮您？\n"
            "用户: 你好，我想问一下携号转网怎么办理。\n"
            "客服: 好的，请问您为什么要转网呢？\n"
            "用户: 主要是我搬家了，现在在杭州，但是号码是北京的，"
            "漫游费用太高了。\n"
            "客服: 其实我们现在有全国套餐，没有漫游费的。\n"
            "用户: 是吗？那信号怎么样？\n"
            "客服: 杭州地区我们有5G全覆盖。\n"
            "用户: 那我了解一下，如果合适的话我就不转网了。"
        ),
    },
    {
        "doc_id": "call_005_normal.txt",
        "content": (
            "客服: 您好，请问有什么业务需要办理？\n"
            "用户: 我的合约快到期了，想续约。\n"
            "客服: 好的，请问您的手机号码是？\n"
            "用户: [PHONE_REDACTED]\n"
            "客服: 帮您查到了，您的合约还有2个月到期。"
            "现在续约可以享受优惠。\n"
            "用户: 有什么优惠？\n"
            "客服: 预存200送200，而且套餐费打8折。\n"
            "用户: 那还不错，帮我办吧。"
        ),
    },
    {
        "doc_id": "call_006_churn_angry.txt",
        "content": (
            "客服: 您好，请问有什么可以帮您？\n"
            "用户: 我想销户。\n"
            "客服: 请问您为什么要销户呢？\n"
            "用户: 不用了，我换电信了，他们那边便宜多了，每个月才79。\n"
            "客服: 您现在的套餐是什么呢？\n"
            "用户: 199的套餐，太贵了，而且你们的网速在深圳南山这边"
            "特别慢，看视频都卡。\n"
            "客服: 了解了，我可以帮您投诉网络部门优先处理。\n"
            "用户: 算了，我已经买了电信的卡了，这个号不要了。"
        ),
    },
    {
        "doc_id": "call_007_elder.txt",
        "content": (
            "客服: 您好，请问有什么可以帮您？\n"
            "用户: 我想问一下有没有适合老人的套餐，"
            "我爸妈打电话比较多但不会用流量。\n"
            "客服: 有的，我们有孝心套餐，月费39，含500分钟通话。\n"
            "用户: 那还不错，帮我办一个。"
        ),
    },
    {
        "doc_id": "call_008_complaint_fraud.txt",
        "content": (
            "客服: 您好，请问有什么可以帮您？\n"
            "用户: 我要投诉！你们承诺的5G网速根本达不到，"
            "我在浦东这边测速才20M，你们虚假宣传！"
            "我要投诉到工信部！\n"
            "客服: 我帮您记录一下，会安排技术人员上门检测。\n"
            "用户: 你们每次都这样说，结果呢？我等了一个月也没人来！"
            "就是故意的！"
        ),
    },
]


# ── 请求模型 ──

class IngestRequest(BaseModel):
    manifest: Optional[list] = None
    lance_uri: str = DEFAULT_LANCE_URI
    overwrite: bool = True
    use_demo: bool = True


class AnalyzeRequest(BaseModel):
    lance_uri: str = DEFAULT_LANCE_URI
    use_llm: bool = True


class QueryRequest(BaseModel):
    lance_uri: str = DEFAULT_LANCE_URI
    query_type: str = "scalar"  # "scalar" 或 "ann"
    where: Optional[str] = None
    query_doc_id: Optional[str] = None
    top_k: int = 5


# ── 路由 ──


@router.post("/ingest")
def step_ingest(req: IngestRequest):
    """
    步骤 1: 数据摄取。

    从 S3 下载真实音频文件，以 Lance blob v2 格式入湖。
    """
    manifest = req.manifest or (DEMO_MANIFEST if req.use_demo else [])
    if not manifest:
        raise HTTPException(400, "manifest 不能为空")
    return ingest(manifest, req.lance_uri, req.overwrite, _s3_store)


@router.post("/transcribe")
def step_transcribe(lance_uri: str = Query(default=DEFAULT_LANCE_URI)):
    """
    步骤 2: 语音转文字 + 情绪标签。

    对 Lance 表中的通话录音进行 ASR 转写、时长计算、
    声学情绪标签提炼，结果写回 Lance 表。
    """
    return transcribe_and_tag(lance_uri)


@router.post("/analyze-text")
def step_analyze_text(req: AnalyzeRequest):
    """
    步骤 3: PII脱敏 + LLM文本分析 + 向量嵌入。

    对已转写的文本进行 PII 脱敏、LLM 意图/情绪分析，
    同时计算向量嵌入，所有结果写回 Lance 表，供步骤 4 检索使用。
    """
    result = analyze_text(req.lance_uri, enable_llm=req.use_llm)
    return result


@router.get("/analyze-text-stream")
async def step_analyze_text_stream():
    """
    SSE 流式：步骤 3 智能分析，逐条推送进度让前端实时展示。
    """
    async def generate():
        for update in analyze_text_stream(
            DEFAULT_LANCE_URI, enable_llm=True
        ):
            event_type = update.pop("event")
            yield _sse(event_type, update)
            await asyncio.sleep(0.01)

    return StreamingResponse(
        generate(), media_type="text/event-stream"
    )


@router.post("/query")
def step_query(req: QueryRequest):
    """
    步骤 4: 数据检索 — 标量过滤 + ANN 向量检索。

    - scalar: 标量过滤（如 bad_tone = true）
    - ann: 近似最近邻检索（声学相似录音）
    """
    if req.query_type == "ann":
        if not req.query_doc_id:
            raise HTTPException(400, "ANN 查询需要 query_doc_id")
        return ann_query(req.lance_uri, req.query_doc_id, req.top_k, req.where)
    return scalar_query(req.lance_uri, req.where, req.top_k)


@router.get("/run-all-stream")
async def run_all_pipeline_stream():
    """
    SSE 流式执行 4 步流水线，前端可实时看到每步的代码执行进度。
    步骤：ingest → transcribe → analyze-text → query (+ gravitino注册)
    """
    async def generate():
        t0 = time.time()

        yield _sse("start", {"step": "pipeline", "status": "启动离线流水线...\n", "total_steps": 5})

        # Step 1: ingest — 加载语音
        yield _sse("progress", {"step": "ingest", "line": "from multimodal_toolkit.pipeline import ingest", "msg": ">>> ① 加载语音: Daft.download → Lance blob v2 写入\n", "idx": 1, "total": 4})
        await asyncio.sleep(0.1)
        yield _sse("progress", {"step": "ingest", "line": "df = read_manifest(manifest)", "msg": "  读取 Manifest (8条历史通话)\n", "idx": 1})
        await asyncio.sleep(0.1)
        yield _sse("progress", {"step": "ingest", "line": "df.write_lance(lance_uri, blob_columns=['audio_blob'])", "msg": "  Daft download → Lance blob v2 入湖...\n", "idx": 1})
        r1 = ingest(DEMO_MANIFEST, DEFAULT_LANCE_URI, overwrite=True, s3_store=_s3_store)
        yield _sse("result", {"step": "ingest", "status": "done", "rows": r1["rows"], "duration_s": r1["duration_s"], "msg": f"  ✅ 完成: {r1['rows']} 行写入 Lance ({r1['duration_s']}s)\n\n", "idx": 1, "total": 4})

        # Step 2: transcribe_and_tag — 语音转文字+情绪标签
        yield _sse("progress", {"step": "transcribe", "line": "ds = lance.dataset(lance_uri)\ndf = daft.read_lance(...)", "msg": ">>> ② 语音转文字+情绪标签: ASR转写 → 声学情绪识别\n", "idx": 2, "total": 4})
        await asyncio.sleep(0.1)
        yield _sse("progress", {"step": "transcribe", "line": "df = df.with_column('duration_s', _duration_udf(col('audio_bytes')))", "msg": "  时长计算 + 过滤 (MIN/MAX 门控)\n", "idx": 2})

        ds = lance.dataset(DEFAULT_LANCE_URI)
        table = ds.to_table(columns=["doc_id", "audio_blob"])
        for i in range(table.num_rows):
            doc_id = table["doc_id"][i].as_py()
            yield _sse("progress", {"step": "transcribe", "line": f"asr = _AsrUDF()\ndf = df.with_column('asr', asr(col('audio_bytes'), col('doc_id')))", "msg": f"  [{i+1}/{table.num_rows}] ASR 转写 + 情绪标签: {doc_id}\n", "idx": 2})
            await asyncio.sleep(0.05)

        r2 = transcribe_and_tag(DEFAULT_LANCE_URI)
        yield _sse("result", {"step": "transcribe", "status": "done", "processed": r2["processed"], "duration_s": r2["duration_s"], "msg": f"  ✅ 完成: {r2['processed']} 条转写+情绪标签 ({r2['duration_s']}s)\n\n", "idx": 2, "total": 4})

        # Step 3: analyze_text — PII脱敏 + LLM分析 + 向量嵌入
        yield _sse("progress", {"step": "analyze-text", "line": "llm_client.analyze_transcript(redacted)", "msg": ">>> ③ 文字智能分析: PII脱敏 → LLM 意图/情绪分析 → 向量嵌入\n", "idx": 3, "total": 4})
        await asyncio.sleep(0.1)
        yield _sse("progress", {"step": "analyze-text", "line": "llm_client.analyze_transcript(redacted)", "msg": f"  LLM 分析 ({table.num_rows} 条) + PII 脱敏, 约需 12s/条...\n", "idx": 3})
        r3 = analyze_text(DEFAULT_LANCE_URI, enable_llm=True)
        yield _sse("result", {"step": "analyze-text", "status": "done", "processed": r3["processed"], "duration_s": r3["duration_s"], "msg": f"  ✅ 完成: {r3['processed']} 条分析+嵌入 ({r3['duration_s']}s)\n\n", "idx": 3, "total": 4})

        # Step 4: query — 标量过滤 + ANN 向量检索
        yield _sse("progress", {"step": "query", "line": "ds.scanner(filter='bad_tone = true').to_table()", "msg": ">>> ④ 数据检索: 标量过滤(bad_tone=true) + ANN 向量检索\n", "idx": 4, "total": 5})
        r4_scalar = scalar_query(DEFAULT_LANCE_URI, "bad_tone = true", top_k=5)
        r4_ann = ann_query(DEFAULT_LANCE_URI, "call_006_churn_angry.txt", top_k=5)
        yield _sse("result", {"step": "query", "status": "done", "scalar": len(r4_scalar.get("results", [])), "ann": len(r4_ann.get("results", [])), "msg": f"  ✅ 标量(bad_tone=true) {len(r4_scalar.get('results',[]))} 条, ANN {len(r4_ann.get('results',[]))} 条\n\n", "idx": 4, "total": 5})

        # Step 5: Gravitino 注册
        yield _sse("progress", {"step": "gravitino", "line": "gravitino.register_table(catalog='lance_catalog', schema='voice_analysis', ...)", "msg": ">>> ⑤ gravitino: 注册到统一元数据中心\n", "idx": 5, "total": 5})
        await asyncio.sleep(0.1)
        r5 = register_to_gravitino(DEFAULT_LANCE_URI)
        yield _sse("result", {"step": "gravitino", "status": r5.get("status", "done"), "table": r5.get("table", ""), "msg": f"  ✅ Gravitino: lance_catalog.voice_analysis.{r5.get('table', '')}\n\n", "idx": 5, "total": 5})

        total = round(time.time() - t0, 2)
        yield _sse("done", {"total_duration_s": total, "msg": f"🎉 流水线全部完成！总耗时 {total}s\n", "scalar_top5": r4_scalar.get("results", []), "ann_top5": r4_ann.get("results", []), "ingest_rows": r1.get("rows"), "analyze_count": r3.get("processed")})

    return StreamingResponse(generate(), media_type="text/event-stream")


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.post("/run-all")
def run_all_pipeline():
    """
    一键运行完整 4 步流水线。

    ingest → transcribe → analyze-text → query (+ gravitino注册)
    """
    report = {
        "pipeline": "离线批处理流水线",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "lance_uri": DEFAULT_LANCE_URI,
        "steps": [],
    }
    t0 = time.time()

    # Step 1 — 加载语音
    r1 = ingest(DEMO_MANIFEST, DEFAULT_LANCE_URI, overwrite=True, s3_store=_s3_store)
    report["steps"].append(r1)

    # Step 2 — 语音转文字 + 情绪标签
    r2 = transcribe_and_tag(DEFAULT_LANCE_URI)
    r2_summary = {k: v for k, v in r2.items() if k != "results"}
    r2_summary["results"] = [
        {k: v for k, v in item.items()}
        for item in r2.get("results", [])
    ]
    report["steps"].append(r2_summary)

    # Step 3 — PII脱敏 + LLM分析 + 向量嵌入
    r3 = analyze_text(DEFAULT_LANCE_URI, enable_llm=True)
    r3_summary = {k: v for k, v in r3.items() if k != "results"}
    r3_summary["results"] = [
        {k: v for k, v in item.items()}
        for item in r3.get("results", [])
    ]
    report["steps"].append(r3_summary)

    # Step 4 — 标量过滤 + ANN 向量检索
    r4_scalar = scalar_query(DEFAULT_LANCE_URI, "bad_tone = true", top_k=5)
    r4_ann = ann_query(DEFAULT_LANCE_URI, "call_006_churn_angry.txt", top_k=5)
    report["steps"].append({
        "step": "query",
        "scalar_filter": "bad_tone = true",
        "scalar_matched": r4_scalar.get("matched", 0),
        "ann_matched": r4_ann.get("matched", 0),
        "scalar_top5": r4_scalar.get("results", []),
        "ann_top5": r4_ann.get("results", []),
    })

    # Gravitino 注册
    r5 = register_to_gravitino(DEFAULT_LANCE_URI)
    report["steps"].append(r5)

    report["total_duration_s"] = round(time.time() - t0, 2)
    report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    return report


@router.get("/status")
def pipeline_status():
    """查看流水线当前状态"""
    return get_state()


@router.get("/audio/{doc_id}")
def serve_audio(doc_id: str):
    """从本地缓存或 Lance 表读取音频并返回（mp3/wav）"""
    from demo_audio import get_cached_wav

    # 1) 优先从本地缓存
    data = get_cached_wav(doc_id)
    if data:
        mt = "audio/mpeg" if data[:3] == b"\xff\xf3" or data[:3] == b"\xff\xfb" else "audio/wav"
        return Response(content=data, media_type=mt)

    # 2) 从 Lance 表读取
    try:
        ds = lance.dataset(DEFAULT_LANCE_URI)
        table = ds.scanner(
            columns=["doc_id", "audio_blob"],
            filter=f"doc_id = '{doc_id}'",
        ).to_table()
        if table.num_rows == 0:
            raise HTTPException(404, f"doc_id not found: {doc_id}")
        blob = table["audio_blob"][0].as_py()
        if not blob:
            raise HTTPException(404, "audio blob is empty")
        if blob[:4] == b"RIFF":
            return Response(content=blob, media_type="audio/wav")
        else:
            return Response(content=blob, media_type="audio/mpeg")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
