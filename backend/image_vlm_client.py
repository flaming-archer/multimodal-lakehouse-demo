"""OpenAI-compatible 视觉大模型客户端。

该模块只负责把一张已缩放的图片发送给支持 Chat Completions 图片输入的
模型，并把返回值校验成稳定的头像合规结构。它不依赖 Daft，也不会在失败时
偷偷回退到本地规则；调用方会把当前图片标记为 ``llm_failed``。
"""
from __future__ import annotations

import base64
import json
import math
import os
from typing import Any


IMAGE_VLM_API_KEY = os.getenv("IMAGE_VLM_API_KEY", "")
IMAGE_VLM_BASE_URL = os.getenv(
    "IMAGE_VLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
IMAGE_VLM_MODEL = os.getenv("IMAGE_VLM_MODEL", "")
IMAGE_VLM_TIMEOUT_S = float(os.getenv("IMAGE_VLM_TIMEOUT_S", "60"))
IMAGE_VLM_MAX_RETRIES = int(os.getenv("IMAGE_VLM_MAX_RETRIES", "2"))

_FIELDS = {
    "has_face",
    "is_blurry",
    "is_face_blurry",
    "is_avatar",
    "clarity_confidence",
    "avatar_confidence",
    "reason",
}


def is_configured() -> bool:
    """只有 key、地址和视觉模型均配置时才允许进入 VLM 模式。"""
    return bool(IMAGE_VLM_API_KEY and IMAGE_VLM_BASE_URL and IMAGE_VLM_MODEL)


def missing_config() -> list[str]:
    missing = []
    if not IMAGE_VLM_API_KEY:
        missing.append("IMAGE_VLM_API_KEY")
    if not IMAGE_VLM_BASE_URL:
        missing.append("IMAGE_VLM_BASE_URL")
    if not IMAGE_VLM_MODEL:
        missing.append("IMAGE_VLM_MODEL")
    return missing


def build_prompt() -> str:
    """与 multimodal_toolkit.image.prompt.build_image_analysis_prompt 保持一致。"""
    return "\n".join(
        [
            "你是图片质量与头像合规分析助手。请观察图片并只输出严格 JSON，不要解释或使用 Markdown。",
            "",
            "判断标准：",
            "- has_face(bool)：画面中是否存在清晰可见的真人脸部，不要求图片适合作为头像。",
            "- is_blurry(bool)：整张图片的主要内容是否明显模糊、失焦，导致细节难以辨认。",
            "  正常压缩、轻微噪点或背景虚化不算整图模糊。",
            "- is_face_blurry(bool)：存在真人脸部但脸部明显模糊、失焦或无法辨认时为 true；",
            "  没有人脸时必须为 false。",
            "- is_avatar(bool)：是否为适合作为个人头像的真人单人图片。必须只有一个真人作为主要主体，",
            "  脸部清楚可见且占画面合理比例。多人照、卡通、Logo、动物、风景、产品、背景小脸均为 false。",
            "- clarity_confidence(float)：对 is_blurry 判断的置信度，范围 0 到 1。",
            "- avatar_confidence(float)：对 is_avatar 判断的置信度，范围 0 到 1。",
            "- reason(str)：用一句简短中文同时说明清晰度与头像判断依据。",
            "",
            "JSON 必须恰好包含以下字段：has_face、is_blurry、is_face_blurry、is_avatar、",
            "clarity_confidence、avatar_confidence、reason。",
        ]
    )


def _parse_json(content: str) -> dict[str, Any]:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:])
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("VLM 返回值不是 JSON 对象")
    return value


def validate_response(payload: dict[str, Any]) -> dict[str, Any]:
    """严格验证类型和字段间逻辑，避免错误响应污染 Lance 表。"""
    if set(payload) != _FIELDS:
        raise ValueError("VLM JSON 字段不完整或包含未知字段")

    bool_fields = ("has_face", "is_blurry", "is_face_blurry", "is_avatar")
    if any(type(payload.get(name)) is not bool for name in bool_fields):
        raise ValueError("VLM 合规字段必须是 JSON boolean")

    for name in ("clarity_confidence", "avatar_confidence"):
        value = payload.get(name)
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"{name} 必须是 0 到 1 的有限数值")

    if payload["is_face_blurry"] and not payload["has_face"]:
        raise ValueError("无人脸时 is_face_blurry 不能为 true")
    if payload["is_avatar"] and (
        not payload["has_face"] or payload["is_blurry"] or payload["is_face_blurry"]
    ):
        raise ValueError("is_avatar 与人脸或模糊结论矛盾")
    reason = payload.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason 必须是非空字符串")

    return {
        **{name: payload[name] for name in bool_fields},
        "clarity_confidence": float(payload["clarity_confidence"]),
        "avatar_confidence": float(payload["avatar_confidence"]),
        "reason": reason.strip(),
    }


class ImageVLMClient:
    """延迟创建 OpenAI SDK client，避免未选择 VLM 时引入网络副作用。"""

    def __init__(self) -> None:
        if not is_configured():
            raise RuntimeError("视觉大模型未配置：" + ", ".join(missing_config()))
        from openai import OpenAI

        self._client = OpenAI(
            api_key=IMAGE_VLM_API_KEY,
            base_url=IMAGE_VLM_BASE_URL,
            timeout=IMAGE_VLM_TIMEOUT_S,
            max_retries=IMAGE_VLM_MAX_RETRIES,
        )

    def analyze(self, jpeg_bytes: bytes) -> dict[str, Any]:
        encoded = base64.b64encode(jpeg_bytes).decode("ascii")
        response = self._client.chat.completions.create(
            model=IMAGE_VLM_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": build_prompt()},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{encoded}"},
                        },
                    ],
                }
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content
        if not content:
            raise ValueError("VLM 返回空内容")
        return validate_response(_parse_json(content))
