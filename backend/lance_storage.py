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

    # ── 关键词 bigram embedding（16 维特征向量） ──

    @staticmethod
    def _extract_bigrams(text: str) -> List[str]:
        """从中文文本中提取字符级 bigram，过滤标点和空白。
        例: '我要转网，太贵' -> ['我要', '要转', '转网', '太贵']
        """
        import re
        clean = re.sub(r'[\s，。！？、：；（）《》""''\\-【】\d]+', '', text)
        chars = list(clean)
        if len(chars) < 2:
            return chars  # 单字符直接返回
        bigrams = []
        for i in range(len(chars) - 1):
            bigrams.append(chars[i] + chars[i + 1])
        return bigrams

    @staticmethod
    def _bigram_to_vec(bigrams: List[str], dim: int = 16) -> np.ndarray:
        """将 bigram 列表映射到固定维度向量（基于 hash 分桶计数）。"""
        vec = np.zeros(dim, dtype=np.float32)
        for bg in bigrams:
            bucket = hash(bg) % dim
            vec[bucket] += 1.0
        norm = np.linalg.norm(vec)
        if norm > 1e-8:
            vec = vec / norm
        return vec

    def _generate_embedding(self, text: str, dim: int = 16) -> List[float]:
        """基于关键词 bigram 的语义嵌入（16 维）。
        用于写入时生成 embedding——搜索时会在内存中动态重算。
        """
        bigrams = self._extract_bigrams(text)
        vec = self._bigram_to_vec(bigrams, dim)
        return vec.tolist()

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

        # 每 30 次写入触发自动 compact，减少碎片
        self._write_count = getattr(self, "_write_count", 0) + 1
        if self._write_count % 30 == 0:
            self._compact()

        return record["call_id"]

    def _compact(self):
        """Compact Lance fragments to reduce IO overhead."""
        try:
            ds = self._get_dataset()
            if ds is None:
                return
            frags = len(ds.get_fragments())
            total = ds.count_rows()
            # 碎片数超过数据行数的一半时才 compact
            if frags > max(5, total // 2):
                temp_path = self.dataset_path + ".compact_temp"
                table = ds.to_table()
                lance.write_dataset(table, temp_path)
                import shutil
                shutil.rmtree(self.dataset_path, ignore_errors=True)
                shutil.move(temp_path, self.dataset_path)
                self._ds = lance.dataset(self.dataset_path)
        except Exception:
            pass  # compact 失败不阻塞主流程

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
        关键词 bigram 语义相似度搜索。
        从每条记录提取中文 bigram，与查询 bigram 做 Jaccard +
        bigram向量余弦相似度双重匹配。
        - 256 维 bigram 向量减少 hash 碰撞（存储 schema 保持 16 维兼容）
        - Jaccard 过滤：无意义查询（如"哈哈哈哈"）与真实通话无交集
        """
        ds = self._get_dataset()
        if ds is None:
            return []

        total = ds.count_rows()
        if total == 0:
            return []

        # 一次性读取全表
        table = ds.to_table()
        records = table.to_pylist()

        # 提取每条记录的文本
        texts = []
        for rec in records:
            t = (rec.get("summary") or "").strip()
            if not t:
                t = (rec.get("transcript") or "").strip()
            texts.append(t)

        # 查询 bigram set
        query_bigrams = self._extract_bigrams(query_text)
        q_bigram_set = set(query_bigrams)

        rows = len(records)
        # 使用 256 维向量大幅减少 hash 碰撞
        SEARCH_DIM = 256
        emb_matrix = np.zeros((rows, SEARCH_DIM), dtype=np.float32)
        all_bigram_sets = []

        for i, text in enumerate(texts):
            bigrams = self._extract_bigrams(text)
            all_bigram_sets.append(set(bigrams))
            emb_matrix[i] = self._bigram_to_vec(bigrams, SEARCH_DIM)

        query_emb = self._bigram_to_vec(query_bigrams, SEARCH_DIM)

        # 归一化
        norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        emb_matrix = emb_matrix / norms

        # 余弦相似度
        scores = np.dot(emb_matrix, query_emb)

        # Jaccard 相似度（bigram 交集 / 并集）
        jaccard_scores = np.zeros(rows, dtype=np.float32)
        for i, rec_bigrams in enumerate(all_bigram_sets):
            if not rec_bigrams or not q_bigram_set:
                jaccard_scores[i] = 0.0
            else:
                intersection = len(q_bigram_set & rec_bigrams)
                union = len(q_bigram_set | rec_bigrams)
                jaccard_scores[i] = intersection / union if union > 0 else 0.0

        # 综合分数：85% Jaccard + 15% cosine
        # Jaccard 保证关键词实际出现，cosine 辅助排序
        combined = 0.85 * jaccard_scores + 0.15 * scores

        # Jaccard=0 的结果直接丢弃（无任何 bigram 交集＝语义无关）
        # 如 "哈哈哈哈" 与业务通话之间无任何公共 bigram
        combined[jaccard_scores == 0] = 0.0
        min_similarity = 0.05
        valid_mask = combined >= min_similarity

        # 取 top-k（仅从有效结果中选）
        valid_indices = np.where(valid_mask)[0]
        if len(valid_indices) == 0:
            return []
        valid_indices = valid_indices[
            np.argsort(combined[valid_indices])[::-1]
        ][:top_k]

        # 组装结果
        results = []
        for idx in valid_indices:
            rec = records[idx]
            if isinstance(rec.get("key_entities"), str):
                try:
                    rec["key_entities"] = json.loads(rec["key_entities"])
                except json.JSONDecodeError:
                    rec["key_entities"] = {}
            if "embedding" in rec and hasattr(rec["embedding"], "tolist"):
                rec["embedding"] = rec["embedding"].tolist()
            rec["_similarity"] = float(combined[idx])
            results.append(rec)

        return results

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
