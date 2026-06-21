"""
Real Lance storage integration using pylance.

Lance is a modern columnar data format optimized for:
- Multimodal data (images, audio, text, embeddings)
- Vector search / similarity queries
- Random access reads
- Zero-copy nested data

This module writes voice analysis results to Lance format,
enabling fast vector similarity search across call records.

In production, Lance data is stored on S3 via:
    lance://s3://bucket/path/to/dataset
"""

import os
import json
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional

import pyarrow as pa
import lance

# Suppress Rich library Unicode errors on Windows
import os
os.environ.setdefault("PYTHONUTF8", "1")


# Lance dataset paths (local filesystem for demo, S3 in production)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "lance")
os.makedirs(DATA_DIR, exist_ok=True)


# Schema for voice analysis records
VOICE_ANALYSIS_SCHEMA = pa.schema([
    pa.field("call_id", pa.string(), nullable=False),
    pa.field("transcript", pa.string()),
    pa.field("caller_intent", pa.string()),
    pa.field("switch_reason", pa.string()),
    pa.field("sentiment", pa.string()),
    pa.field("sentiment_score", pa.float32()),
    pa.field("risk_level", pa.string()),
    pa.field("key_entities", pa.string()),  # JSON-encoded dict
    pa.field("suggested_action", pa.string()),
    pa.field("summary", pa.string()),
    pa.field("duration_seconds", pa.int32()),
    pa.field("embedding", pa.list_(pa.float32(), 16)),  # 16-dim demo embedding
    pa.field("processed_at", pa.string()),
])


class LanceVoiceStorage:
    """
    Lance-based storage for voice analysis results.

    Features:
    - Columnar storage with vector embeddings
    - Fast point queries by call_id
    - Similarity search via vector embeddings
    - Schema evolution support
    """

    def __init__(self, dataset_path: Optional[str] = None):
        self.dataset_path = dataset_path or os.path.join(
            DATA_DIR, "voice_analysis.lance"
        )
        self._ds = None  # cached dataset handle

    def _get_dataset(self):
        """Get or create cached Lance dataset handle.
        Avoids reopening the dataset on every query (the #1 perf killer)."""
        if self._ds is None and os.path.exists(self.dataset_path):
            self._ds = lance.dataset(self.dataset_path)
        return self._ds

    def _ensure_dataset(self, records: List[Dict] = None):
        """Create Lance dataset if it doesn't exist."""
        if os.path.exists(self.dataset_path):
            if self._ds is None:
                self._ds = lance.dataset(self.dataset_path)
            return

        lance.write_dataset(
            pa.Table.from_pylist([], schema=VOICE_ANALYSIS_SCHEMA),
            self.dataset_path,
        )
        self._ds = lance.dataset(self.dataset_path)
        # Create index on embedding column for fast ANN search;
        # 16-dim vectors are small enough that flat scan is also acceptable
        # for demo-scale data, so index failures are non-fatal
        try:
            self._ds.create_index(
                column="embedding",
                index_type="IVF_PQ",
                name="embedding_idx",
                replace=True,
            )
        except Exception as e:
            import logging
            logging.getLogger("lance_storage").warning(
                "Index creation skipped (%s), fallback to flat scan", e
            )

    def _generate_embedding(self, text: str, dim: int = 16) -> List[float]:
        """Generate a simple hash-based embedding for demo purposes.
        In production, use a real embedding model (e.g., sentence-transformers).
        """
        import hashlib
        h = hashlib.sha256(text.encode()).digest()
        # Use hash bytes to generate deterministic float vector
        embedding = []
        for i in range(dim):
            val = (h[i * 2 % len(h)] * 256 + h[(i * 2 + 1) % len(h)]) / 65536.0
            embedding.append(val)
        return embedding

    def write_analysis(self, analysis: Dict) -> str:
        """Write a single voice analysis record to Lance."""
        self._ensure_dataset()

        record = {
            "call_id": analysis.get("call_id", ""),
            "transcript": analysis.get("transcript", ""),
            "caller_intent": analysis.get("caller_intent", ""),
            "switch_reason": analysis.get("switch_reason", ""),
            "sentiment": analysis.get("sentiment", "neutral"),
            "sentiment_score": float(analysis.get("sentiment_score", 0.0)),
            "risk_level": analysis.get("risk_level", "low"),
            "key_entities": json.dumps(analysis.get("key_entities", {}),
                                       ensure_ascii=False),
            "suggested_action": analysis.get("suggested_action", ""),
            "summary": analysis.get("summary", ""),
            "duration_seconds": int(analysis.get("duration_seconds", 0)),
            "embedding": self._generate_embedding(
                analysis.get("summary", analysis.get("transcript", ""))
            ),
            "processed_at": datetime.now().isoformat(),
        }

        table = pa.Table.from_pylist([record], schema=VOICE_ANALYSIS_SCHEMA)
        lance.write_dataset(
            table,
            self.dataset_path,
            mode="append",
        )
        # Re-open to keep cache warm after write
        self._ds = lance.dataset(self.dataset_path)
        return record["call_id"]

    def read_by_call_id(self, call_id: str) -> Optional[Dict]:
        """Query a specific call by call_id."""
        ds = self._get_dataset()
        if ds is None:
            return None

        result = ds.to_table(
            filter=f"call_id = '{call_id}'"
        )
        if result.num_rows == 0:
            return None

        return result.to_pylist()[0]

    def list_all(self, limit: int = 100) -> List[Dict]:
        """List all records."""
        ds = self._get_dataset()
        if ds is None:
            return []

        result = ds.to_table(limit=limit)
        records = result.to_pylist()
        for r in records:
            if isinstance(r.get("key_entities"), str):
                try:
                    r["key_entities"] = json.loads(r["key_entities"])
                except json.JSONDecodeError:
                    r["key_entities"] = {}
            if "embedding" in r and hasattr(r["embedding"], "tolist"):
                r["embedding"] = r["embedding"].tolist()
        return records

    def count(self) -> int:
        """Count total records."""
        ds = self._get_dataset()
        if ds is None:
            return 0
        return ds.count_rows()

    def search_similar(self, query_text: str, top_k: int = 5) -> List[Dict]:
        """
        Vector similarity search - find calls similar to the query.
        Uses scanner() with nearest{} for true ANN search — scanner does NOT
        materialize the full table before ranking, unlike to_table(nearest=...).
        """
        ds = self._get_dataset()
        if ds is None:
            return []

        query_embedding = self._generate_embedding(query_text)

        # scanner does streaming ANN: much faster than to_table(nearest=...)
        results = ds.scanner(
            nearest={
                "column": "embedding",
                "q": query_embedding,
                "k": top_k,
            }
        ).to_table()
        return results.to_pylist()

    def get_stats(self) -> Dict[str, Any]:
        """Get dataset statistics."""
        ds = self._get_dataset()
        if ds is None:
            return {"status": "empty", "records": 0}

        records = ds.to_table().to_pylist()

        if not records:
            return {"status": "empty", "records": 0}

        risk_counts = {"high": 0, "medium": 0, "low": 0}
        intents = {}
        for r in records:
            risk = r.get("risk_level", "low")
            risk_counts[risk] = risk_counts.get(risk, 0) + 1
            intent = r.get("caller_intent", "unknown")
            intents[intent] = intents.get(intent, 0) + 1

        return {
            "status": "active",
            "records": len(records),
            "dataset_path": self.dataset_path,
            "format": "Lance",
            "schema_version": str(ds.version),
            "risk_distribution": risk_counts,
            "intent_distribution": intents,
        }
