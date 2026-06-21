"""
Gravitino metadata management client simulation.
Demonstrates unified metadata management across Lance and Iceberg.
"""

from typing import Dict, List, Any, Optional
import time
from dataclasses import dataclass, field


@dataclass
class CatalogInfo:
    name: str
    catalog_type: str
    provider: str
    properties: Dict[str, str] = field(default_factory=dict)
    schemas: List[str] = field(default_factory=list)


@dataclass
class SchemaInfo:
    name: str
    comment: str
    tables: List[str] = field(default_factory=list)


class GravitinoClient:

    def __init__(self, server_uri: str = "http://localhost:8090",
                 metalake: str = "demo_metalake"):
        self.server_uri = server_uri
        self.metalake = metalake
        self._catalogs: Dict[str, CatalogInfo] = {}
        self._schemas: Dict[str, Dict[str, SchemaInfo]] = {}
        self._registered_tables: Dict[str, Dict] = {}
        self._init_demo()

    def _init_demo(self):
        lance = CatalogInfo(
            name="lance_catalog", catalog_type="lance",
            provider="com.databricks.lance",
            properties={"location": "s3://lakehouse/lance"},
            schemas=["voice_analysis", "camera_signal", "image_processing"]
        )
        iceberg = CatalogInfo(
            name="iceberg_catalog", catalog_type="iceberg",
            provider="com.apache.iceberg",
            properties={"location": "s3://lakehouse/iceberg", "format-version": "2"},
            schemas=["analytics", "churn_risk"]
        )
        fileset = CatalogInfo(
            name="fileset_catalog", catalog_type="fileset",
            provider="hadoop",
            properties={"location": "s3://lakehouse/raw"},
            schemas=["raw_audio", "raw_video", "raw_images"]
        )
        self._catalogs = {
            "lance_catalog": lance,
            "iceberg_catalog": iceberg,
            "fileset_catalog": fileset
        }
        self._schemas = {
            "lance_catalog": {
                "voice_analysis": SchemaInfo(
                    name="voice_analysis", comment="客服语音解析结果",
                    tables=["call_analysis", "intent_classification"]
                ),
                "camera_signal": SchemaInfo(
                    name="camera_signal", comment="监控与信令比对",
                    tables=["checkpoint_data", "daily_comparison"]
                ),
                "image_processing": SchemaInfo(
                    name="image_processing", comment="图像清洗标注去毒",
                    tables=["id_card_ocr", "image_quality"]
                )
            },
            "iceberg_catalog": {
                "analytics": SchemaInfo(
                    name="analytics", comment="批量数据分析",
                    tables=["daily_aggregation", "hourly_metrics"]
                ),
                "churn_risk": SchemaInfo(
                    name="churn_risk", comment="客户流失风险",
                    tables=["churn_prediction", "retention_strategy"]
                )
            },
            "fileset_catalog": {
                "raw_audio": SchemaInfo(name="raw_audio", comment="原始录音", tables=[]),
                "raw_video": SchemaInfo(name="raw_video", comment="原始视频", tables=[]),
                "raw_images": SchemaInfo(name="raw_images", comment="原始图像", tables=[])
            }
        }

    def list_catalogs(self) -> List[Dict]:
        return [
            {"name": c.name, "type": c.catalog_type,
             "provider": c.provider, "schemas": c.schemas}
            for c in self._catalogs.values()
        ]

    def list_schemas(self, catalog: str) -> List[Dict]:
        schemas = self._schemas.get(catalog, {})
        return [
            {"name": s.name, "comment": s.comment, "tables": s.tables}
            for s in schemas.values()
        ]

    def get_metalake_info(self) -> Dict:
        total_schemas = sum(len(s) for s in self._schemas.values())
        total_tables = sum(
            len(s.tables) for schemas in self._schemas.values()
            for s in schemas.values()
        )
        return {
            "metalake": self.metalake,
            "server": self.server_uri,
            "catalogs": len(self._catalogs),
            "schemas": total_schemas,
            "tables": total_tables + len(self._registered_tables),
        }

    def register_table(
        self, catalog: str, schema_name: str, table_name: str,
        table_type: str = "lance", location: str = "",
        schema: str = "", row_count: int = 0
    ) -> Dict:
        """模拟向 Gravitino 注册一张表"""
        key = f"{catalog}.{schema_name}.{table_name}"
        self._registered_tables[key] = {
            "catalog": catalog,
            "schema": schema_name,
            "table": table_name,
            "type": table_type,
            "location": location,
            "schema_def": schema,
            "row_count": row_count,
            "registered_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        # 同步到 schema 的 table 列表
        if catalog in self._schemas:
            cats = self._schemas[catalog]
            if schema_name in cats and table_name not in cats[schema_name].tables:
                cats[schema_name].tables.append(table_name)
        return {"registered": True, "key": key, "table": table_name}
