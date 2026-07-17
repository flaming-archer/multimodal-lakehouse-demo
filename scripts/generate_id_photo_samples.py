#!/usr/bin/env python3
"""使用通义万相生成虚构证件照，并派生可重复的质量异常样本。

密钥只从 ``DASHSCOPE_API_KEY`` 环境变量读取。脚本不会打印、保存或上传密钥。
每个基础素材使用独立 prompt 和独立万相任务；异常样本由 OpenCV 确定性生成。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import cv2
import numpy as np
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "images"
METADATA_PATH = OUTPUT_DIR / "generation_metadata.json"
MODEL = os.getenv("DASHSCOPE_IMAGE_MODEL", "wanx2.1-t2i-plus")
API_ROOT = os.getenv("DASHSCOPE_API_ROOT", "https://dashscope.aliyuncs.com/api/v1")
TIMEOUT_S = int(os.getenv("DASHSCOPE_IMAGE_TIMEOUT_S", "600"))

COMMON = (
    "Photorealistic official ID portrait test fixture of a fictional adult who does not resemble any "
    "real person or public figure. Neutral expression, realistic skin texture, head and shoulders, "
    "plain pale blue studio background, even frontal studio lighting, square 1:1 composition. "
    "No text, no logo, no watermark, no document border, no uniform, no celebrity."
)

PROMPTS = {
    "id_standard_woman.jpg": (
        COMMON
        + " One fictional East Asian woman, facing camera directly, both eyes nose mouth and full face contour clearly visible, professional dark blazer."
    ),
    "id_standard_man.jpg": (
        COMMON
        + " One fictional East Asian man, facing camera directly, both eyes nose mouth and full face contour clearly visible, professional dark jacket."
    ),
    "id_group_two.jpg": (
        "Photorealistic ID-photo-style test fixture with exactly two fictional East Asian adults standing shoulder to shoulder, "
        "both faces equally prominent and clearly visible, plain pale blue studio background, even lighting, square composition. "
        "No text, no logo, no watermark, no document border, no celebrity."
    ),
    "id_medical_mask.jpg": (
        COMMON
        + " One fictional East Asian adult facing camera, wearing an opaque surgical mask that fully covers nose, mouth and lower cheeks; only eyes and forehead visible."
    ),
    "id_sunglasses.jpg": (
        COMMON
        + " One fictional East Asian adult facing camera, wearing oversized opaque black sunglasses that completely hide both eyes and eyebrows."
    ),
    "id_hand_occluded.jpg": (
        COMMON
        + " One fictional East Asian adult facing camera, with one open hand held directly in front of the face, covering one eye, most of the nose and part of the mouth."
    ),
    "id_side_profile.jpg": (
        COMMON
        + " One fictional East Asian adult photographed in strong 80-degree side profile, looking away from camera, only one eye visible."
    ),
}


def _request_json(url: str, *, method: str = "GET", payload: dict | None = None) -> dict:
    key = os.getenv("DASHSCOPE_API_KEY", "")
    if not key:
        raise RuntimeError("缺少 DASHSCOPE_API_KEY")
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Authorization", f"Bearer {key}")
    request.add_header("Content-Type", "application/json")
    if method == "POST":
        request.add_header("X-DashScope-Async", "enable")
    for attempt in range(7):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:1000]
            if exc.code == 429 and attempt < 6:
                retry_after = exc.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(60, 5 * (2**attempt))
                print(f"[retry] DashScope 限流，{delay:.0f}s 后重试", flush=True)
                time.sleep(delay)
                continue
            raise RuntimeError(f"DashScope HTTP {exc.code}: {detail}") from exc
    raise AssertionError("unreachable")


def _submit(prompt: str) -> str:
    response = _request_json(
        f"{API_ROOT}/services/aigc/text2image/image-synthesis",
        method="POST",
        payload={
            "model": MODEL,
            "input": {"prompt": prompt},
            "parameters": {
                "size": "1024*1024",
                "n": 1,
                "prompt_extend": True,
                "watermark": False,
            },
        },
    )
    task_id = response.get("output", {}).get("task_id")
    if not task_id:
        raise RuntimeError(f"万相未返回 task_id：{response}")
    return task_id


def _wait(task_id: str) -> str:
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        response = _request_json(f"{API_ROOT}/tasks/{task_id}")
        output = response.get("output", {})
        status = output.get("task_status")
        if status == "SUCCEEDED":
            results = output.get("results") or []
            if not results or not results[0].get("url"):
                raise RuntimeError(f"万相任务成功但没有图片 URL：{response}")
            return results[0]["url"]
        if status in {"FAILED", "CANCELED", "UNKNOWN"}:
            message = output.get("message") or response.get("message") or "未知错误"
            raise RuntimeError(f"万相任务 {task_id} {status}：{message}")
        time.sleep(2)
    raise TimeoutError(f"万相任务超时：{task_id}")


def _download_jpeg(url: str, output: Path) -> None:
    with urllib.request.urlopen(url, timeout=120) as response:
        content = response.read()
    from io import BytesIO

    with Image.open(BytesIO(content)) as image:
        image.convert("RGB").resize((1024, 1024), Image.Resampling.LANCZOS).save(
            output, "JPEG", quality=94, optimize=True
        )


def generate_bases() -> list[dict[str, str]]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing_metadata: dict[str, dict[str, str]] = {}
    if METADATA_PATH.exists():
        existing_metadata = {
            item["file"]: item
            for item in json.loads(METADATA_PATH.read_text(encoding="utf-8"))
            if isinstance(item, dict) and item.get("file")
        }
    metadata = []
    for filename, prompt in PROMPTS.items():
        output = OUTPUT_DIR / filename
        if output.exists():
            print(f"[skip] {output}", flush=True)
            metadata.append(
                existing_metadata.get(
                    filename,
                    {"file": filename, "model": MODEL, "task_id": "unknown", "prompt": prompt},
                )
            )
            continue
        print(f"[submit] {filename} ({MODEL})", flush=True)
        task_id = _submit(prompt)
        print(f"[task] {filename}: {task_id}", flush=True)
        print(f"[wait] {filename}", flush=True)
        url = _wait(task_id)
        _download_jpeg(url, output)
        metadata.append(
            {
                "file": filename,
                "model": MODEL,
                "task_id": task_id,
                "prompt": PROMPTS[filename],
            }
        )
        print(f"[saved] {output}", flush=True)
    return metadata


def _read(name: str) -> np.ndarray:
    image = cv2.imread(str(OUTPUT_DIR / name))
    if image is None:
        raise RuntimeError(f"无法读取基础图片：{name}")
    return image


def _save(name: str, image: np.ndarray) -> None:
    if not cv2.imwrite(str(OUTPUT_DIR / name), image, [cv2.IMWRITE_JPEG_QUALITY, 94]):
        raise RuntimeError(f"写入图片失败：{name}")
    print(f"[derived] {OUTPUT_DIR / name}", flush=True)


def derive_scenarios() -> None:
    source = _read("id_standard_woman.jpg")
    height, width = source.shape[:2]

    # 只模糊脸部中心区域，衣服和背景仍清晰；用于区分整图模糊与脸部模糊。
    face_blurred = source.copy()
    x1, x2 = int(width * 0.27), int(width * 0.73)
    y1, y2 = int(height * 0.12), int(height * 0.66)
    face_blurred[y1:y2, x1:x2] = cv2.GaussianBlur(
        face_blurred[y1:y2, x1:x2], (81, 81), 0
    )
    _save("id_face_blurred.jpg", face_blurred)

    _save("id_full_blurred.jpg", cv2.GaussianBlur(source, (81, 81), 0))

    # 放大并把人脸中心推到左边界，只保留约半张脸，稳定构造严重裁切。
    zoomed = cv2.resize(source, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    cropped = zoomed[int(height * 0.25) : int(height * 0.25) + height, width : width * 2]
    _save("id_face_cropped.jpg", cropped)

    # 完整证件照缩成背景中的小块，人工面积规则可以稳定拒绝。
    tiny = np.full_like(source, (220, 225, 230))
    small = cv2.resize(source, (260, 260), interpolation=cv2.INTER_AREA)
    offset = (width - 260) // 2
    tiny[offset : offset + 260, offset : offset + 260] = small
    _save("id_tiny_face.jpg", tiny)

    overexposed = cv2.convertScaleAbs(source, alpha=1.75, beta=75)
    _save("id_overexposed.jpg", overexposed)

    underexposed = np.clip(source.astype(np.float32) * 0.24, 0, 255).astype(np.uint8)
    _save("id_underexposed.jpg", underexposed)


def main() -> int:
    metadata = generate_bases()
    derive_scenarios()
    METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[done] 生成信息：{METADATA_PATH}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise
