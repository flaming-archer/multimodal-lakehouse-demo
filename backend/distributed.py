"""
Distributed processing engine.

Supports both:
  - Real Ray cluster (if ray is installed and cluster is available)
  - ThreadPoolExecutor fallback (for development without Ray)

Usage:
  processor = DistributedProcessor(use_ray=True)
  result = processor.process_batch(items, process_fn)
"""

import os
import time
import sys
from typing import Dict, List, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime

# Detect if we should use real Ray (set via env var or auto-detect)
USE_REAL_RAY = os.getenv("USE_REAL_RAY", "false").lower() == "true"

# Try to import ray
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False


@dataclass
class BatchResult:
    task_id: str
    total: int
    completed: int
    failed: int
    results: List[Dict]
    duration_sec: float
    processed_at: str = field(default_factory=lambda: datetime.now().isoformat())
    engine: str = "ray" if (USE_REAL_RAY and RAY_AVAILABLE) else "threadpool"


class DistributedProcessor:
    """
    Distributed processing engine.

    If USE_REAL_RAY=true and ray is available, uses real Ray cluster.
    Otherwise, falls back to ThreadPoolExecutor.
    """

    def __init__(self, max_workers: int = 4, use_ray: bool = None):
        self.max_workers = max_workers
        self.use_ray = use_ray if use_ray is not None else (USE_REAL_RAY and RAY_AVAILABLE)
        self.ray_initialized = False

        if self.use_ray and RAY_AVAILABLE:
            try:
                if not ray.is_initialized():
                    # Try to connect to Ray cluster
                    ray_address = os.getenv("RAY_ADDRESS", "ray://ray-head:10001")
                    ray.init(address=ray_address, ignore_reinit_error=True)
                self.ray_initialized = True
                print(f"[Distributed] Using REAL Ray cluster")
            except Exception as e:
                print(f"[Distributed] Ray init failed: {e}, falling back to ThreadPoolExecutor")
                self.use_ray = False

    def process_batch(
        self,
        items: List[Any],
        process_fn: Callable,
        task_id: str = "",
    ) -> BatchResult:
        """
        Process a batch of items in parallel.

        Args:
            items: List of items to process
            process_fn: Function that takes one item and returns a result dict
            task_id: Task identifier for tracking

        Returns:
            BatchResult with all processed results
        """
        if not task_id:
            task_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        if self.use_ray and self.ray_initialized:
            return self._process_batch_ray(items, process_fn, task_id)
        else:
            return self._process_batch_local(items, process_fn, task_id)

    def _process_batch_ray(
        self,
        items: List[Any],
        process_fn: Callable,
        task_id: str,
    ) -> BatchResult:
        """Process batch using real Ray."""
        start = time.time()

        # Wrap process_fn as a ray remote function
        @ray.remote
        def ray_process(item):
            try:
                return process_fn(item)
            except Exception as e:
                return {"error": str(e)}

        # Submit all tasks
        futures = [ray_process.remote(item) for item in items]

        # Collect results
        results = ray.get(futures)

        duration = time.time() - start
        failed = sum(1 for r in results if "error" in r)

        return BatchResult(
            task_id=task_id,
            total=len(items),
            completed=len(results) - failed,
            failed=failed,
            results=results,
            duration_sec=round(duration, 2),
            engine="ray",
        )

    def _process_batch_local(
        self,
        items: List[Any],
        process_fn: Callable,
        task_id: str,
    ) -> BatchResult:
        """Process batch using local ThreadPoolExecutor (Ray-compatible API)."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        start = time.time()
        results = []
        failed = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(process_fn, item): i
                for i, item in enumerate(items)
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    failed += 1
                    results.append({
                        "error": str(e),
                        "index": futures[future],
                    })

        duration = time.time() - start

        return BatchResult(
            task_id=task_id,
            total=len(items),
            completed=len(results) - failed,
            failed=failed,
            results=results,
            duration_sec=round(duration, 2),
            engine="threadpool",
        )
