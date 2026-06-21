"""
Real Apache Iceberg table format integration using pyiceberg.

Iceberg is a high-performance table format for huge analytic tables.
Key features:
- ACID transactions
- Time travel / snapshot isolation
- Schema evolution
- Partition evolution
- Hidden partitioning

This module writes daily aggregation metadata to Iceberg tables,
enabling time-travel queries and batch analytics.

In production, Iceberg tables are stored on S3 with a Hive/JDBC catalog:
    pyiceberg catalog with s3://bucket/warehouse
"""

import os
import sys
import json
from datetime import datetime, date
from typing import Dict, Any, List, Optional

# Suppress Rich library Unicode errors on Windows
os.environ.setdefault("PYTHONUTF8", "1")

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, FloatType, DateType, TimestampType,
    DoubleType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField, DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform


# Use local filesystem catalog for demo (SQLite-backed)
# In production: REST catalog or Hive Metastore
DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "iceberg"
)
os.makedirs(DATA_DIR, exist_ok=True)

CATALOG_DIR = os.path.join(DATA_DIR, "catalog")
WAREHOUSE_DIR = os.path.join(DATA_DIR, "warehouse")
os.makedirs(CATALOG_DIR, exist_ok=True)
os.makedirs(WAREHOUSE_DIR, exist_ok=True)


# Schema for churn prediction table
CHURN_SCHEMA = Schema(
    NestedField(1, "date", DateType(), required=False,
                doc="Analysis date"),
    NestedField(2, "total_calls", IntegerType(), required=False,
                doc="Total calls analyzed"),
    NestedField(3, "churn_intent_count", IntegerType(), required=False,
                doc="Number of calls with churn/switch intent"),
    NestedField(4, "high_risk_count", IntegerType(), required=False,
                doc="Number of high-risk calls"),
    NestedField(5, "medium_risk_count", IntegerType(), required=False,
                doc="Number of medium-risk calls"),
    NestedField(6, "low_risk_count", IntegerType(), required=False,
                doc="Number of low-risk calls"),
    NestedField(7, "negative_sentiment_count", IntegerType(), required=False,
                doc="Calls with negative sentiment"),
    NestedField(8, "top_switch_reason", StringType(), required=False,
                doc="Most common churn reason"),
    NestedField(9, "avg_sentiment_score", FloatType(), required=False,
                doc="Average sentiment score"),
    NestedField(10, "processed_at", TimestampType(), required=False,
                doc="Processing timestamp"),
)


class IcebergStorage:
    """
    Iceberg table storage for daily analytics aggregations.

    Tables:
    - lakehouse.churn_risk.churn_predictions: Daily churn risk aggregation
    """

    def __init__(self):
        self.catalog = None
        self._init_catalog()

    def _init_catalog(self):
        """Initialize SQLite-backed Iceberg catalog."""
        try:
            self.catalog = load_catalog(
                "lakehouse",
                **{
                    "type": "sql",
                    "uri": f"sqlite:///{CATALOG_DIR}/lakehouse.db",
                    "warehouse": f"file://{WAREHOUSE_DIR}",
                },
            )
        except Exception:
            # Fallback: use local filesystem
            self.catalog = load_catalog(
                "default",
                **{
                    "type": "sql",
                    "uri": f"sqlite:///{CATALOG_DIR}/lakehouse.db",
                    "warehouse": f"file://{WAREHOUSE_DIR}",
                },
            )

    def _ensure_namespace(self):
        """Create namespace if not exists."""
        try:
            self.catalog.create_namespace_if_not_exists("churn_risk")
        except Exception:
            pass

    def _ensure_table(self) -> str:
        """Create Iceberg table if not exists."""
        self._ensure_namespace()
        table_id = "churn_risk.churn_predictions"

        try:
            self.catalog.load_table(table_id)
            return table_id
        except Exception:
            pass

        try:
            self.catalog.create_table(
                identifier=table_id,
                schema=CHURN_SCHEMA,
                partition_spec=PartitionSpec(
                    PartitionField(
                        source_id=1, field_id=1000,
                        transform=DayTransform(), name="date_day"
                    )
                ),
            )
        except Exception as e:
            print(f"[Iceberg] Table creation warning: {e}")

        return table_id

    def write_daily_aggregation(self, date_str: str, stats: Dict) -> str:
        """Write daily aggregation to Iceberg table."""
        table_id = self._ensure_table()

        row = {
            "date": date.fromisoformat(date_str),
            "total_calls": stats.get("total_calls", 0),
            "churn_intent_count": stats.get("churn_intent_count", 0),
            "high_risk_count": stats.get("high_risk_count", 0),
            "medium_risk_count": stats.get("medium_risk_count", 0),
            "low_risk_count": stats.get("low_risk_count", 0),
            "negative_sentiment_count": stats.get("negative_sentiment_count", 0),
            "top_switch_reason": stats.get("top_switch_reason", "N/A"),
            "avg_sentiment_score": float(stats.get("avg_sentiment_score", 0.0)),
            "processed_at": datetime.now(),
        }

        try:
            table = self.catalog.load_table(table_id)
            import pyarrow as pa
            import traceback

            # Build PyArrow table matching Iceberg schema exactly
            arrow_schema = pa.schema([
                pa.field("date", pa.date32()),
                pa.field("total_calls", pa.int32()),
                pa.field("churn_intent_count", pa.int32()),
                pa.field("high_risk_count", pa.int32()),
                pa.field("medium_risk_count", pa.int32()),
                pa.field("low_risk_count", pa.int32()),
                pa.field("negative_sentiment_count", pa.int32()),
                pa.field("top_switch_reason", pa.string()),
                pa.field("avg_sentiment_score", pa.float32()),
                pa.field("processed_at", pa.timestamp("us")),
            ])

            pdf = pa.table({
                "date": pa.array([row["date"]], type=pa.date32()),
                "total_calls": pa.array([row["total_calls"]], type=pa.int32()),
                "churn_intent_count": pa.array([row["churn_intent_count"]], type=pa.int32()),
                "high_risk_count": pa.array([row["high_risk_count"]], type=pa.int32()),
                "medium_risk_count": pa.array([row["medium_risk_count"]], type=pa.int32()),
                "low_risk_count": pa.array([row["low_risk_count"]], type=pa.int32()),
                "negative_sentiment_count": pa.array([row["negative_sentiment_count"]], type=pa.int32()),
                "top_switch_reason": pa.array([row["top_switch_reason"]], type=pa.string()),
                "avg_sentiment_score": pa.array([row["avg_sentiment_score"]], type=pa.float32()),
                "processed_at": pa.array([row["processed_at"]], type=pa.timestamp("us")),
            })

            table.append(pdf)
            return date_str
        except Exception as e:
            print(f"[Iceberg] Write error: {e}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            # Fallback: save to JSON file
            fallback_path = os.path.join(DATA_DIR, f"churn_{date_str}.json")
            with open(fallback_path, "w", encoding="utf-8") as f:
                json.dump(row, f, ensure_ascii=False, indent=2, default=str)
            return date_str

    def read_table_snapshot(self, table_id: str = "churn_risk.churn_predictions") -> List[Dict]:
        """Read current snapshot of the Iceberg table."""
        try:
            table = self.catalog.load_table(table_id)
            scan = table.scan()
            df = scan.to_arrow()
            records = df.to_pylist()
            # Convert date objects to strings
            for r in records:
                for k, v in r.items():
                    if isinstance(v, (date, datetime)):
                        r[k] = v.isoformat()
            return records
        except Exception as e:
            print(f"[Iceberg] Read error: {e}")
            return []

    def get_snapshots(self, table_id: str = "churn_risk.churn_predictions") -> List[Dict]:
        """List table snapshots for time travel demonstration."""
        try:
            table = self.catalog.load_table(table_id)
            snaps = []
            current = table.current_snapshot()
            if current:
                snaps.append({
                    "snapshot_id": current.snapshot_id,
                    "timestamp": str(current.timestamp_ms),
                    "operation": str(current.operation) if hasattr(current, 'operation') else "append",
                    "summary": str(current.summary) if hasattr(current, 'summary') else {},
                })
            return snaps
        except Exception:
            return []

    def get_table_stats(self, table_id: str = "churn_risk.churn_predictions") -> Dict:
        """Get Iceberg table statistics."""
        records = self.read_table_snapshot(table_id)
        snapshots = self.get_snapshots(table_id)

        total_risk = {"high": 0, "medium": 0, "low": 0}
        for r in records:
            for level in total_risk:
                key = f"{level}_risk_count"
                if key in r:
                    total_risk[level] += int(r[key])

        return {
            "status": "active",
            "table_id": table_id,
            "format": "Apache Iceberg",
            "records": len(records),
            "snapshots": len(snapshots),
            "risk_distribution": total_risk,
            "features": ["ACID transactions", "Time travel", "Partition evolution",
                          "Schema evolution", "Hidden partitioning"],
        }
