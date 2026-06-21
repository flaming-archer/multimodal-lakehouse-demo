"""
生成演示用音频文件 —— 使用 edge-tts 中文男/女声合成真人语音。

客服行 → zh-CN-XiaoxiaoNeural (女声)
用户行 → zh-CN-YunxiNeural (男声)
行间无前缀（不朗读"客服:"/"用户:"），通过声音本身区分说话人。

输出: mp3 格式（浏览器原生支持 <audio>），保存到本地缓存。
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import os
import tempfile
from typing import Dict, List, Optional

import edge_tts

FEMALE_VOICE = "zh-CN-XiaoxiaoNeural"
MALE_VOICE = "zh-CN-YunxiNeural"
_CACHE_DIR = os.path.join(tempfile.gettempdir(), "offline_demo", "audio")


def _ensure_cache_dir():
    os.makedirs(_CACHE_DIR, exist_ok=True)


def _run_async(coro):
    """在任意上下文中安全执行 async 协程"""
    try:
        loop = asyncio.get_running_loop()
        # 已有运行中的事件循环，在新线程中执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        return asyncio.run(coro)


def _parse_lines(text: str) -> List[tuple]:
    """
    解析通话文本，返回 [(voice, spoken_text), ...]
    去除"客服:"/"用户:"前缀，通过 voice 区分说话人。
    """
    result = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        if line.startswith("客服:"):
            spoken = line[3:].strip()
            if spoken:
                result.append((FEMALE_VOICE, spoken))
        elif line.startswith("用户:"):
            spoken = line[3:].strip()
            if spoken:
                result.append((MALE_VOICE, spoken))
        else:
            # 无前缀行，用客服声音
            if line:
                result.append((FEMALE_VOICE, line))
    return result


async def _gen_mp3(text: str, voice: str) -> bytes:
    """使用 edge-tts 生成单句 mp3"""
    tmp = tempfile.mktemp(suffix=".mp3")
    communicate = edge_tts.Communicate(text, voice, rate="+5%")
    await communicate.save(tmp)
    with open(tmp, "rb") as f:
        data = f.read()
    os.remove(tmp)
    return data


async def _gen_call_audio_async(text: str) -> bytes:
    """为一条通话生成完整 mp3（按行交错男/女声）"""
    lines = _parse_lines(text)
    # 并发生成所有行的 mp3
    tasks = [_gen_mp3(spoken, voice) for voice, spoken in lines]
    results = await asyncio.gather(*tasks)
    # 简单拼接 mp3（浏览器可正常播放）
    return b"".join(results)


def generate_call_wav(text: str) -> bytes:
    """同步接口：将通话文本转为音频（mp3 字节）"""
    return _run_async(_gen_call_audio_async(text))


def generate_all_demo_wavs(manifest: List[dict]) -> Dict[str, bytes]:
    """为 manifest 中每条通话生成 mp3，保存本地缓存"""
    _ensure_cache_dir()

    async def _gen_all():
        result = {}
        for item in manifest:
            doc_id = item["doc_id"]
            key = doc_id.replace(".txt", "")
            print(f"  [TTS] Generating {key}.mp3 ...")
            data = await _gen_call_audio_async(item["content"])
            result[key] = data
            filepath = os.path.join(_CACHE_DIR, key + ".mp3")
            with open(filepath, "wb") as f:
                f.write(data)
        return result

    return _run_async(_gen_all())


def get_cached_wav(doc_id: str) -> Optional[bytes]:
    """从本地缓存读取 mp3"""
    key = doc_id.replace(".txt", "")
    for ext in [".mp3", ".wav"]:
        filepath = os.path.join(_CACHE_DIR, key + ext)
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                return f.read()
    return None


def ensure_demo_audio(manifest: List[dict]) -> Dict[str, bytes]:
    """确保演示音频已生成，缓存命中则跳过"""
    _ensure_cache_dir()
    existing = set(os.listdir(_CACHE_DIR)) if os.path.exists(_CACHE_DIR) else set()
    result = {}
    for item in manifest:
        key = item["doc_id"].replace(".txt", "")
        fname = key + ".mp3"
        if fname in existing:
            path = os.path.join(_CACHE_DIR, fname)
            with open(path, "rb") as f:
                result[key] = f.read()
    if len(result) == len(manifest):
        return result
    return generate_all_demo_wavs(manifest)


def upload_to_s3(s3_store, manifest: List[dict]) -> List[str]:
    """生成音频并上传到 S3"""
    wavs = generate_all_demo_wavs(manifest)
    keys = []
    for key, data in wavs.items():
        s3_key = f"raw_audio/{key}.mp3"
        try:
            s3_store.client.put_object(
                Bucket=s3_store.config.bucket_name,
                Key=s3_key,
                Body=data,
                ContentType="audio/mpeg",
            )
            keys.append(s3_key)
        except Exception as e:
            print(f"[DemoAudio] S3 upload failed {s3_key}: {e}")
    return keys
