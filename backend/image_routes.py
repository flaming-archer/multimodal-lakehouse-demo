"""图片批处理 FastAPI 路由。"""
from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from image_pipeline import (
    DEFAULT_LANCE_URI,
    PipelineStateError,
    analyze,
    embed,
    get_asset,
    get_status,
    ingest,
    list_records,
    scalar_query,
    text_query,
)

router = APIRouter(prefix="/api/image", tags=["图片批处理"])
_job_lock = threading.Lock()


class AnalyzeRequest(BaseModel):
    analysis_backend: Literal["local", "vlm"] = "local"


class ImageQueryRequest(BaseModel):
    query_type: Literal["scalar", "text"] = "text"
    text: str | None = None
    where: str | None = None
    top_k: int = Field(default=3, ge=1, le=20)


def _run_exclusive(function, *args, **kwargs):
    if not _job_lock.acquire(blocking=False):
        raise HTTPException(409, "图片流水线正在执行，请稍后重试")
    try:
        return function(*args, **kwargs)
    except PipelineStateError as exc:
        raise HTTPException(409, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except RuntimeError as exc:
        message = str(exc)
        status = 503 if "视觉大模型未配置" in message else 500
        raise HTTPException(status, message) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    finally:
        _job_lock.release()


@router.post("/ingest")
def image_ingest():
    return _run_exclusive(ingest)


@router.post("/analyze")
def image_analyze(request: AnalyzeRequest):
    return _run_exclusive(analyze, request.analysis_backend)


@router.post("/embed")
def image_embed():
    return _run_exclusive(embed)


@router.post("/query")
def image_query(request: ImageQueryRequest):
    if request.query_type == "text":
        return _run_exclusive(
            text_query,
            request.text or "",
            request.top_k,
            request.where,
        )
    return _run_exclusive(scalar_query, request.where, request.top_k)


@router.get("/assets/{doc_id}")
def image_asset(doc_id: str):
    try:
        content, media_type = get_asset(doc_id)
        return Response(content=content, media_type=media_type)
    except FileNotFoundError as exc:
        raise HTTPException(404, f"图片不存在：{doc_id}") from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@router.get("/status")
def image_status():
    return get_status()


@router.get("/records")
def image_records(limit: int = Query(default=100, ge=1, le=200)):
    """浏览图片 Lance 表中的分析明细，不返回图片 blob 和 512 维向量。"""
    return _run_exclusive(list_records, limit)


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.get("/run-all-stream")
async def image_run_all_stream(
    analysis_backend: Literal["local", "vlm"] = Query(default="local"),
):
    """完整运行：入库 → 合规分析 → 图片向量 → 默认文本查询。"""
    if not _job_lock.acquire(blocking=False):
        async def busy_response():
            yield _sse("error", {"status": "error", "message": "图片流水线正在执行，请稍后重试"})

        return StreamingResponse(busy_response(), media_type="text/event-stream")

    events: queue.Queue[tuple[str, dict] | None] = queue.Queue()

    def emit(event: str, payload: dict) -> None:
        events.put((event, payload))

    def callback(stage: str, current: int, total: int, doc_id: str, message: str) -> None:
        emit(
            "progress",
            {
                "step": stage,
                "current": current,
                "total": total,
                "doc_id": doc_id,
                "msg": message,
            },
        )

    def run_job() -> None:
        started = time.time()
        try:
            emit(
                "start",
                {
                    "status": "running",
                    "analysis_backend": analysis_backend,
                    "total_steps": 4,
                    "message": "启动真实图片处理流水线",
                },
            )
            stages = [
                ("ingest", "图片入库", ingest, ()),
                ("analyze", "头像合规分析", analyze, (analysis_backend,)),
                ("embed", "ChineseCLIP 图片向量", embed, ()),
            ]
            summaries = []
            for index, (step, label, function, args) in enumerate(stages, 1):
                emit(
                    "stage",
                    {"step": step, "label": label, "index": index, "total": 4, "status": "running"},
                )
                result = function(*args, DEFAULT_LANCE_URI, callback)
                summaries.append(result)
                emit("result", result)

            emit(
                "stage",
                {"step": "query", "label": "中文文本搜图", "index": 4, "total": 4, "status": "running"},
            )
            query_result = text_query(
                "戴口罩的人脸",
                3,
                None,
                DEFAULT_LANCE_URI,
            )
            emit("result", {"step": "query", "status": "done", **query_result})
            emit(
                "done",
                {
                    "status": "done",
                    "analysis_backend": analysis_backend,
                    "total_duration_s": round(time.time() - started, 3),
                    "steps": summaries,
                    "query": query_result,
                },
            )
        except Exception as exc:
            emit(
                "error",
                {
                    "status": "error",
                    "analysis_backend": analysis_backend,
                    "message": str(exc),
                    "total_duration_s": round(time.time() - started, 3),
                },
            )
        finally:
            _job_lock.release()
            events.put(None)

    # 作业线程持有互斥锁直到真实计算结束。SSE 客户端即使刷新或断开，作业
    # 仍会安全完成，期间新的写任务继续收到 409，不会并发 overwrite Lance。
    try:
        threading.Thread(target=run_job, name="image-pipeline-job", daemon=True).start()
    except Exception:
        _job_lock.release()
        raise

    async def generate():
        while True:
            try:
                item = events.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            if item is None:
                break
            event, payload = item
            yield _sse(event, payload)

    return StreamingResponse(generate(), media_type="text/event-stream")
