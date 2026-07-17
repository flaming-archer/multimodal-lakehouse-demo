"""图片批处理流水线：Lance 入库、合规分析、ChineseCLIP 向量和查询。

实现刻意保持为普通 Python 模块：不依赖 Daft，也不调用 Gravitino。
``multimodal_toolkit`` 仅是字段和阈值的参考，本模块可以独立部署。
"""
from __future__ import annotations

import json
import mimetypes
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Iterable

ProgressCallback = Callable[[str, int, int, str, str], None]

ROOT_DIR = Path(__file__).resolve().parent.parent
IMAGE_DATA_DIR = Path(os.getenv("IMAGE_DATA_DIR", str(ROOT_DIR / "data" / "images")))
IMAGE_MANIFEST = Path(os.getenv("IMAGE_MANIFEST", str(IMAGE_DATA_DIR / "manifest.json")))
DEFAULT_LANCE_URI = os.getenv(
    "IMAGE_LANCE_URI", os.path.join(tempfile.gettempdir(), "offline_demo", "images.lance")
)

INSIGHTFACE_MODEL = os.getenv("INSIGHTFACE_MODEL", "buffalo_l")
INSIGHTFACE_ROOT = os.getenv("INSIGHTFACE_ROOT", "")
FACE_DET_SIZE = int(os.getenv("FACE_DET_SIZE", "640"))
FACE_DET_THRESH = float(os.getenv("FACE_DET_THRESH", "0.3"))
IMAGE_LONG_EDGE = int(os.getenv("IMAGE_LONG_EDGE", "1024"))
FACE_DET_SCORE_MIN = float(os.getenv("FACE_DET_SCORE_MIN", "0.5"))
MIN_FACE_RATIO = float(os.getenv("MIN_FACE_RATIO", "0.01"))
BLUR_THRESHOLD = float(os.getenv("BLUR_THRESHOLD", "100.0"))
FACE_BLUR_THRESHOLD = float(os.getenv("FACE_BLUR_THRESHOLD", "80.0"))
AVATAR_MIN_FACE_RATIO = float(os.getenv("AVATAR_MIN_FACE_RATIO", "0.03"))

IMAGE_EMBED_MODEL = os.getenv("IMAGE_EMBED_MODEL", "OFA-Sys/chinese-clip-vit-base-patch16")
IMAGE_EMBED_DEVICE = os.getenv("IMAGE_EMBED_DEVICE", "cpu")
IMAGE_EMBED_DIM = int(os.getenv("IMAGE_EMBED_DIM", "512"))
IMAGE_EMBED_BATCH_SIZE = max(1, int(os.getenv("IMAGE_EMBED_BATCH_SIZE", "8")))
# 与 multimodal_toolkit/image/config.py 保持相同默认值。Demo 可以通过环境
# 变量覆盖，但未显式配置时不应表现出不同的请求并发行为。
IMAGE_VLM_CONCURRENCY = max(1, int(os.getenv("IMAGE_VLM_CONCURRENCY", "1")))

_state: dict[str, Any] = {
    "lance_uri": DEFAULT_LANCE_URI,
    "ingested": False,
    "analyzed": False,
    "embedded": False,
    "analysis_backend": None,
    "rows": 0,
}


class PipelineStateError(RuntimeError):
    pass


def _notify(callback: ProgressCallback | None, stage: str, current: int, total: int, doc_id: str, message: str) -> None:
    if callback:
        callback(stage, current, total, doc_id, message)


def _imports():
    import lance
    import pyarrow as pa

    return lance, pa


def _schema():
    _, pa = _imports()
    return pa.schema(
        [
            pa.field("doc_id", pa.string(), nullable=False),
            pa.field("description", pa.string()),
            pa.field("source_path", pa.string()),
            pa.field("mime_type", pa.string()),
            pa.field("image_blob", pa.large_binary()),
            pa.field("width", pa.int32()),
            pa.field("height", pa.int32()),
            pa.field("analysis_status", pa.string()),
            pa.field("analysis_backend", pa.string()),
            pa.field("analysis_error", pa.string()),
            pa.field("face_count", pa.int32()),
            pa.field("face_score", pa.float64()),
            pa.field("face_area_ratio", pa.float64()),
            pa.field("blur_score", pa.float64()),
            pa.field("face_blur_score", pa.float64()),
            pa.field("has_face", pa.bool_()),
            pa.field("is_blurry", pa.bool_()),
            pa.field("is_face_blurry", pa.bool_()),
            pa.field("is_avatar", pa.bool_()),
            pa.field("clarity_confidence", pa.float64()),
            pa.field("avatar_confidence", pa.float64()),
            pa.field("analysis_reason", pa.string()),
            pa.field("embedding_status", pa.string()),
            pa.field("embedding_error", pa.string()),
            pa.field("embedding_model", pa.string()),
            pa.field("image_embedding", pa.list_(pa.float32(), IMAGE_EMBED_DIM)),
        ]
    )


def _table_from_rows(rows: list[dict[str, Any]]):
    _, pa = _imports()
    schema = _schema()
    arrays = [pa.array([row.get(field.name) for row in rows], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_rows(rows: list[dict[str, Any]], lance_uri: str) -> None:
    lance, _ = _imports()
    Path(lance_uri).parent.mkdir(parents=True, exist_ok=True)
    lance.write_dataset(_table_from_rows(rows), lance_uri, mode="overwrite")


def _read_rows(lance_uri: str) -> list[dict[str, Any]]:
    lance, _ = _imports()
    try:
        table = lance.dataset(lance_uri).to_table()
    except Exception as exc:
        raise PipelineStateError("图片表不存在，请先执行图片入库") from exc
    return table.to_pylist()


def _load_manifest() -> list[dict[str, Any]]:
    if not IMAGE_MANIFEST.exists():
        raise FileNotFoundError(f"图片 manifest 不存在：{IMAGE_MANIFEST}")
    entries = json.loads(IMAGE_MANIFEST.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("图片 manifest 必须是非空 JSON 数组")
    ids = [item.get("doc_id") for item in entries]
    if any(not isinstance(doc_id, str) or not doc_id for doc_id in ids):
        raise ValueError("manifest 每行都必须包含非空 doc_id")
    if len(ids) != len(set(ids)):
        raise ValueError("manifest doc_id 不能重复")
    return entries


def ingest(lance_uri: str = DEFAULT_LANCE_URI, callback: ProgressCallback | None = None) -> dict[str, Any]:
    """读取仓库内演示图片并重建 Lance 基础表。缺失文件保留为一行。"""
    started = time.time()
    manifest = _load_manifest()
    rows = []
    for index, item in enumerate(manifest, 1):
        rel_path = item.get("file", "")
        path = IMAGE_DATA_DIR / rel_path
        blob = None
        error = None
        try:
            blob = path.read_bytes()
        except OSError as exc:
            error = f"读取失败：{exc.strerror or exc}"
        mime_type = item.get("mime_type") or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        rows.append(
            {
                "doc_id": item["doc_id"],
                "description": item.get("description", ""),
                "source_path": rel_path,
                "mime_type": mime_type,
                "image_blob": blob,
                "analysis_status": "pending" if blob is not None else "download_failed",
                "analysis_error": error,
                "embedding_status": "pending" if blob is not None else "skipped",
            }
        )
        _notify(callback, "ingest", index, len(manifest), item["doc_id"], "图片写入 Lance")
    _write_rows(rows, lance_uri)
    _state.update(
        lance_uri=lance_uri,
        ingested=True,
        analyzed=False,
        embedded=False,
        analysis_backend=None,
        rows=len(rows),
    )
    return {
        "step": "ingest",
        "status": "done",
        "rows": len(rows),
        "missing": sum(row["image_blob"] is None for row in rows),
        "lance_uri": lance_uri,
        "duration_s": round(time.time() - started, 3),
    }


def decode_image(image_bytes: bytes | None):
    import cv2
    import numpy as np

    if not image_bytes:
        return None
    return cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)


def resize_long_edge(image, long_edge: int = IMAGE_LONG_EDGE):
    import cv2

    height, width = image.shape[:2]
    edge = max(height, width)
    if edge <= long_edge:
        return image, 1.0
    scale = long_edge / edge
    resized = cv2.resize(
        image,
        (max(1, round(width * scale)), max(1, round(height * scale))),
        interpolation=cv2.INTER_AREA,
    )
    return resized, scale


def laplacian_variance(image) -> float:
    import cv2

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


class FaceDetector:
    def __init__(self) -> None:
        from insightface.app import FaceAnalysis

        kwargs: dict[str, Any] = {
            "name": INSIGHTFACE_MODEL,
            "allowed_modules": ["detection"],
            "providers": ["CPUExecutionProvider"],
        }
        if INSIGHTFACE_ROOT:
            kwargs["root"] = INSIGHTFACE_ROOT
        self._model = FaceAnalysis(**kwargs)
        self._model.prepare(
            ctx_id=-1,
            det_thresh=FACE_DET_THRESH,
            det_size=(FACE_DET_SIZE, FACE_DET_SIZE),
        )
        self._lock = threading.Lock()

    def detect(self, image) -> list[dict[str, Any]]:
        with self._lock:
            faces = self._model.get(image)
        result = []
        for face in faces:
            x1, y1, x2, y2 = (float(value) for value in face.bbox)
            result.append({"bbox": (x1, y1, x2, y2), "score": float(face.det_score)})
        result.sort(
            key=lambda value: (value["bbox"][2] - value["bbox"][0])
            * (value["bbox"][3] - value["bbox"][1]),
            reverse=True,
        )
        return result


_face_detector: FaceDetector | None = None
_face_detector_lock = threading.Lock()


def get_face_detector() -> FaceDetector:
    global _face_detector
    if _face_detector is None:
        with _face_detector_lock:
            if _face_detector is None:
                _face_detector = FaceDetector()
    return _face_detector


def _crop(image, bbox):
    height, width = image.shape[:2]
    x1, y1 = max(0, int(bbox[0])), max(0, int(bbox[1]))
    x2, y2 = min(width, int(bbox[2])), min(height, int(bbox[3]))
    if x2 <= x1 or y2 <= y1:
        return None
    return image[y1:y2, x1:x2]


def _empty_analysis(row: dict[str, Any], backend: str) -> None:
    for name in (
        "width", "height", "face_count", "face_score", "face_area_ratio",
        "blur_score", "face_blur_score", "has_face", "is_blurry",
        "is_face_blurry", "is_avatar", "clarity_confidence",
        "avatar_confidence", "analysis_reason",
    ):
        row[name] = None
    row["analysis_backend"] = backend
    row["analysis_error"] = None


def _local_analysis(row: dict[str, Any]) -> None:
    _empty_analysis(row, "local")
    image = decode_image(row.get("image_blob"))
    if image is None:
        row["analysis_status"] = "decode_failed" if row.get("image_blob") else "download_failed"
        row["analysis_error"] = "图片无法解码" if row.get("image_blob") else "图片不存在"
        return

    original_height, original_width = image.shape[:2]
    resized, _ = resize_long_edge(image)
    faces = get_face_detector().detect(resized)
    blur_score = laplacian_variance(resized)
    # 以下原始指标和规则口径与 multimodal_toolkit/image/udfs.py、rules.py
    # 一致：face_count 是 SCRFD 粗筛后的原始框数量；其余人脸指标全部取
    # 面积最大的框。即使另一张较小的人脸能通过业务阈值，也不能替换最大框。
    face_score = 0.0
    face_ratio = 0.0
    face_blur = None
    image_area = float(resized.shape[0] * resized.shape[1])
    if faces:
        largest = faces[0]
        x1, y1, x2, y2 = largest["bbox"]
        face_score = largest["score"]
        face_ratio = max(0.0, (x2 - x1) * (y2 - y1)) / image_area
        crop = _crop(resized, largest["bbox"])
        if crop is not None:
            face_blur = laplacian_variance(crop)

    face_count = len(faces)
    has_face = bool(
        face_count > 0
        and face_score >= FACE_DET_SCORE_MIN
        and face_ratio >= MIN_FACE_RATIO
    )
    is_blurry = blur_score < BLUR_THRESHOLD
    is_face_blurry = bool(has_face and face_blur is not None and face_blur < FACE_BLUR_THRESHOLD)
    is_avatar = bool(
        face_count == 1
        and has_face
        and face_ratio >= AVATAR_MIN_FACE_RATIO
        and not is_blurry
        and not is_face_blurry
    )
    reasons = []
    if not has_face:
        reasons.append("未检测到有效人脸")
    elif face_count != 1:
        reasons.append("头像必须恰好包含一张人脸")
    if has_face and face_ratio < AVATAR_MIN_FACE_RATIO:
        reasons.append("人脸占画面比例过小")
    if is_blurry:
        reasons.append("整张图片明显模糊")
    if is_face_blurry:
        reasons.append("人脸区域明显模糊")

    row.update(
        width=original_width,
        height=original_height,
        analysis_status="ok",
        face_count=face_count,
        face_score=face_score,
        face_area_ratio=face_ratio,
        blur_score=blur_score,
        face_blur_score=face_blur,
        has_face=has_face,
        is_blurry=is_blurry,
        is_face_blurry=is_face_blurry,
        is_avatar=is_avatar,
        analysis_reason="；".join(reasons) if reasons else "单人脸清晰且占比合理",
    )


def _image_as_jpeg(image) -> bytes:
    import cv2

    resized, _ = resize_long_edge(image)
    ok, encoded = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, 90])
    if not ok:
        raise ValueError("图片转 JPEG 失败")
    return encoded.tobytes()


def _vlm_analysis(row: dict[str, Any], client) -> None:
    _empty_analysis(row, "vlm")
    image = decode_image(row.get("image_blob"))
    if image is None:
        row["analysis_status"] = "decode_failed" if row.get("image_blob") else "download_failed"
        row["analysis_error"] = "图片无法解码" if row.get("image_blob") else "图片不存在"
        return
    row["height"], row["width"] = image.shape[:2]
    try:
        result = client.analyze(_image_as_jpeg(image))
        row.update(
            analysis_status="ok",
            has_face=result["has_face"],
            is_blurry=result["is_blurry"],
            is_face_blurry=result["is_face_blurry"],
            is_avatar=result["is_avatar"],
            clarity_confidence=result["clarity_confidence"],
            avatar_confidence=result["avatar_confidence"],
            analysis_reason=result["reason"],
        )
    except Exception as exc:
        row["analysis_status"] = "llm_failed"
        row["analysis_error"] = str(exc)[:500]


def analyze(
    analysis_backend: str,
    lance_uri: str = DEFAULT_LANCE_URI,
    callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    started = time.time()
    if analysis_backend not in {"local", "vlm"}:
        raise ValueError("analysis_backend 必须是 local 或 vlm")
    rows = _read_rows(lance_uri)
    total = len(rows)

    if analysis_backend == "local":
        for index, row in enumerate(rows, 1):
            _local_analysis(row)
            _notify(callback, "analyze", index, total, row["doc_id"], "本地人脸与清晰度分析")
    else:
        from image_vlm_client import ImageVLMClient

        client = ImageVLMClient()
        with ThreadPoolExecutor(max_workers=IMAGE_VLM_CONCURRENCY) as executor:
            futures = {executor.submit(_vlm_analysis, row, client): row for row in rows}
            completed = 0
            for future in as_completed(futures):
                future.result()
                completed += 1
                row = futures[future]
                _notify(callback, "analyze", completed, total, row["doc_id"], "视觉大模型合规分析")

    _write_rows(rows, lance_uri)
    _state.update(analyzed=True, analysis_backend=analysis_backend)
    return {
        "step": "analyze",
        "status": "done",
        "analysis_backend": analysis_backend,
        "processed": total,
        "ok": sum(row.get("analysis_status") == "ok" for row in rows),
        "failed": sum(row.get("analysis_status") != "ok" for row in rows),
        "avatars": sum(row.get("is_avatar") is True for row in rows),
        "duration_s": round(time.time() - started, 3),
        "results": [_public_row(row) for row in rows],
    }


class ChineseClipEmbedder:
    def __init__(self) -> None:
        import torch
        from transformers import ChineseCLIPModel, ChineseCLIPProcessor

        self._torch = torch
        self._device = IMAGE_EMBED_DEVICE
        self._processor = ChineseCLIPProcessor.from_pretrained(IMAGE_EMBED_MODEL)
        self._model = ChineseCLIPModel.from_pretrained(IMAGE_EMBED_MODEL)
        model_dim = getattr(self._model.config, "projection_dim", None)
        if model_dim is not None and model_dim != IMAGE_EMBED_DIM:
            raise ValueError(
                f"ChineseCLIP projection_dim={model_dim}，但 IMAGE_EMBED_DIM={IMAGE_EMBED_DIM}"
            )
        self._model.to(self._device)
        self._model.eval()
        self._lock = threading.Lock()

    def _tensor(self, output):
        return output.pooler_output if hasattr(output, "pooler_output") else output

    def embed_images(self, images: list[Any]) -> list[list[float]]:
        with self._lock, self._torch.no_grad():
            inputs = self._processor(images=images, return_tensors="pt")
            inputs = {name: value.to(self._device) for name, value in inputs.items()}
            features = self._tensor(self._model.get_image_features(**inputs))
            features = self._torch.nn.functional.normalize(features, p=2, dim=-1)
            return features.detach().cpu().to(self._torch.float32).tolist()

    def embed_text(self, text: str) -> list[float]:
        with self._lock, self._torch.no_grad():
            inputs = self._processor(text=[text], padding=True, return_tensors="pt")
            inputs = {name: value.to(self._device) for name, value in inputs.items()}
            features = self._tensor(self._model.get_text_features(**inputs))
            features = self._torch.nn.functional.normalize(features, p=2, dim=-1)
            vector = features[0].detach().cpu().to(self._torch.float32).tolist()
        if len(vector) != IMAGE_EMBED_DIM:
            raise ValueError(f"文本向量维度异常：{len(vector)}")
        return vector


_embedder: ChineseClipEmbedder | None = None
_embedder_lock = threading.Lock()


def get_embedder() -> ChineseClipEmbedder:
    global _embedder
    if _embedder is None:
        with _embedder_lock:
            if _embedder is None:
                _embedder = ChineseClipEmbedder()
    return _embedder


def _to_pil(image_bytes: bytes):
    import io
    from PIL import Image

    return Image.open(io.BytesIO(image_bytes)).convert("RGB")


def _chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def embed(lance_uri: str = DEFAULT_LANCE_URI, callback: ProgressCallback | None = None) -> dict[str, Any]:
    started = time.time()
    rows = _read_rows(lance_uri)
    valid: list[tuple[dict[str, Any], Any]] = []
    for row in rows:
        row["embedding_model"] = IMAGE_EMBED_MODEL
        row["embedding_error"] = None
        row["image_embedding"] = None
        if not row.get("image_blob"):
            row["embedding_status"] = "skipped"
            row["embedding_error"] = "图片不存在"
            continue
        try:
            valid.append((row, _to_pil(row["image_blob"])))
            row["embedding_status"] = "processing"
        except Exception as exc:
            row["embedding_status"] = "failed"
            row["embedding_error"] = f"图片解码失败：{exc}"[:500]

    if not valid:
        _write_rows(rows, lance_uri)
        _state.update(embedded=False)
        raise PipelineStateError("没有可解码的图片，无法生成 ChineseCLIP 向量")

    try:
        model = get_embedder()
    except Exception as exc:
        for row, _ in valid:
            row["embedding_status"] = "failed"
            row["embedding_error"] = f"ChineseCLIP 加载失败：{exc}"[:500]
        _write_rows(rows, lance_uri)
        _state.update(embedded=False)
        raise RuntimeError("ChineseCLIP 模型加载失败，未生成任何图片向量") from exc

    completed = 0
    for batch in _chunks(valid, IMAGE_EMBED_BATCH_SIZE):
        batch_rows = [item[0] for item in batch]
        try:
            vectors = model.embed_images([item[1] for item in batch])
            if len(vectors) != len(batch_rows) or any(len(vector) != IMAGE_EMBED_DIM for vector in vectors):
                raise ValueError("ChineseCLIP 图片向量数量或维度异常")
            for row, vector in zip(batch_rows, vectors):
                row["image_embedding"] = vector
                row["embedding_status"] = "ok"
        except Exception as exc:
            for row in batch_rows:
                row["embedding_status"] = "failed"
                row["embedding_error"] = str(exc)[:500]
        for row in batch_rows:
            completed += 1
            _notify(callback, "embed", completed, len(valid), row["doc_id"], "生成 ChineseCLIP 图片向量")

    embedded_count = sum(row.get("embedding_status") == "ok" for row in rows)
    failed_count = sum(row.get("embedding_status") == "failed" for row in rows)
    _write_rows(rows, lance_uri)
    _state.update(embedded=embedded_count > 0)
    if embedded_count == 0:
        raise RuntimeError("ChineseCLIP 图片编码全部失败，未生成任何可查询向量")
    return {
        "step": "embed",
        "status": "done",
        "processed": len(valid),
        "embedded": embedded_count,
        "failed": failed_count,
        "model": IMAGE_EMBED_MODEL,
        "dimension": IMAGE_EMBED_DIM,
        "duration_s": round(time.time() - started, 3),
    }


def _escape_filter(value: str) -> str:
    return value.replace("'", "''")


def _public_row(row: dict[str, Any]) -> dict[str, Any]:
    result = {name: value for name, value in row.items() if name not in {"image_blob", "image_embedding"}}
    result["preview_url"] = f"/api/image/assets/{row['doc_id']}"
    return result


def list_records(
    limit: int = 100,
    lance_uri: str = DEFAULT_LANCE_URI,
) -> dict[str, Any]:
    """读取图片 Lance 明细，排除 blob 和向量等不适合 JSON 展示的大列。"""
    lance, _ = _imports()
    try:
        ds = lance.dataset(lance_uri)
    except Exception as exc:
        raise PipelineStateError("图片表不存在，请先运行图片流水线") from exc

    public_columns = [
        name for name in ds.schema.names if name not in {"image_blob", "image_embedding"}
    ]
    try:
        records = ds.scanner(columns=public_columns, limit=limit).to_table().to_pylist()
        summary_rows = ds.scanner(
            columns=["analysis_status", "is_avatar", "embedding_status"]
        ).to_table().to_pylist()
    except Exception as exc:
        raise PipelineStateError(f"无法读取图片 Lance 表：{exc}") from exc

    return {
        "dataset": Path(lance_uri).name,
        "lance_uri": lance_uri,
        "count": ds.count_rows(),
        "schema": public_columns,
        "summary": {
            "analyzed": sum(row.get("analysis_status") == "ok" for row in summary_rows),
            "avatars": sum(row.get("is_avatar") is True for row in summary_rows),
            "rejected": sum(row.get("is_avatar") is False for row in summary_rows),
            "failed": sum(
                row.get("analysis_status") not in {None, "pending", "ok"}
                for row in summary_rows
            ),
            "embedded": sum(row.get("embedding_status") == "ok" for row in summary_rows),
        },
        "records": [_public_row(row) for row in records],
    }


def scalar_query(
    where: str | None = None,
    top_k: int = 10,
    lance_uri: str = DEFAULT_LANCE_URI,
) -> dict[str, Any]:
    started = time.time()
    lance, _ = _imports()
    try:
        ds = lance.dataset(lance_uri)
    except Exception as exc:
        raise PipelineStateError("图片表不存在，请先运行流水线") from exc
    kwargs: dict[str, Any] = {"limit": top_k}
    if where:
        kwargs["filter"] = where
    try:
        table = ds.scanner(**kwargs).to_table()
    except Exception as exc:
        raise ValueError(f"无效的 Lance 过滤条件：{exc}") from exc
    return {
        "type": "scalar",
        "where": where,
        "top_k": top_k,
        "matched": table.num_rows,
        "duration_s": round(time.time() - started, 3),
        "results": [_public_row(row) for row in table.to_pylist()],
    }


def text_query(
    text: str,
    top_k: int = 3,
    where: str | None = None,
    lance_uri: str = DEFAULT_LANCE_URI,
) -> dict[str, Any]:
    started = time.time()
    query = text.strip()
    if not query:
        raise ValueError("文本查询不能为空")
    lance, pa = _imports()
    try:
        ds = lance.dataset(lance_uri)
    except Exception as exc:
        raise PipelineStateError("图片表不存在，请先运行流水线") from exc
    if "image_embedding" not in ds.schema.names:
        raise PipelineStateError("图片向量不存在，请先执行向量生成")

    # ingest 创建的统一 schema 已经包含 image_embedding 列，不能用列存在性
    # 判断 embed 是否完成。先检查真实成功行及模型元数据，再加载 1.4GB 的
    # 文本编码模型，避免未执行 embed 时产生昂贵且含糊的失败。
    try:
        embedded_rows = ds.scanner(
            columns=["embedding_status", "embedding_model", "image_embedding"],
            filter="embedding_status = 'ok'",
        ).to_table().to_pylist()
    except Exception as exc:
        raise PipelineStateError(f"无法读取图片向量状态：{exc}") from exc
    if not embedded_rows:
        raise PipelineStateError("没有可用图片向量，请先执行向量生成")
    models = {row.get("embedding_model") for row in embedded_rows}
    if models != {IMAGE_EMBED_MODEL}:
        actual = ", ".join(sorted(str(model) for model in models))
        raise PipelineStateError(
            f"图片向量模型与查询模型不一致：表内={actual}，当前={IMAGE_EMBED_MODEL}；请重新生成向量"
        )
    if any(
        row.get("image_embedding") is None or len(row["image_embedding"]) != IMAGE_EMBED_DIM
        for row in embedded_rows
    ):
        raise PipelineStateError("表内存在空向量或维度不匹配，请重新生成图片向量")

    vector = get_embedder().embed_text(query)
    nearest = {
        "column": "image_embedding",
        "q": pa.array(vector, type=pa.float32()),
        "k": top_k,
    }
    effective_where = "embedding_status = 'ok'"
    if where:
        effective_where = f"({effective_where}) AND ({where})"
    kwargs: dict[str, Any] = {"nearest": nearest, "filter": effective_where}
    try:
        table = ds.scanner(**kwargs).to_table()
    except Exception as exc:
        raise ValueError(f"图片向量查询失败：{exc}") from exc
    return {
        "type": "text",
        "text": query,
        "where": where,
        "effective_where": effective_where,
        "top_k": top_k,
        "model": IMAGE_EMBED_MODEL,
        "duration_s": round(time.time() - started, 3),
        "results": [_public_row(row) for row in table.to_pylist()],
    }


def get_asset(doc_id: str, lance_uri: str = DEFAULT_LANCE_URI) -> tuple[bytes, str]:
    lance, _ = _imports()
    ds = lance.dataset(lance_uri)
    table = ds.scanner(
        columns=["image_blob", "mime_type"],
        filter=f"doc_id = '{_escape_filter(doc_id)}'",
        limit=1,
    ).to_table()
    if table.num_rows == 0 or not table["image_blob"][0].as_py():
        raise FileNotFoundError(doc_id)
    return table["image_blob"][0].as_py(), table["mime_type"][0].as_py() or "application/octet-stream"


def get_status(lance_uri: str = DEFAULT_LANCE_URI) -> dict[str, Any]:
    from image_vlm_client import IMAGE_VLM_MODEL, is_configured, missing_config

    state = dict(_state)
    try:
        lance, _ = _imports()
        ds = lance.dataset(lance_uri)
        state["rows"] = ds.count_rows()
        state["schema"] = ds.schema.names
    except Exception:
        state["schema"] = []
    state["models"] = {
        "face_detector_loaded": _face_detector is not None,
        "chinese_clip_loaded": _embedder is not None,
        "chinese_clip_model": IMAGE_EMBED_MODEL,
        "vlm_configured": is_configured(),
        "vlm_model": IMAGE_VLM_MODEL or None,
        "vlm_missing_config": missing_config(),
    }
    return state
