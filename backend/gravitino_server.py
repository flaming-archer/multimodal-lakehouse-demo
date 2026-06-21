"""
Gravitino-compatible REST API server.

Implements the Gravitino REST API specification for demo purposes.
In production, replace with the Apache Gravitino Java server.

API reference:
- POST /api/metalakes
- GET  /api/metalakes
- GET  /api/metalakes/{name}
- POST /api/metalakes/{name}/catalogs
- GET  /api/metalakes/{name}/catalogs
- GET  /api/metalakes/{name}/catalogs/{catalog}
- POST /api/metalakes/{name}/catalogs/{catalog}/schemas
- GET  /api/metalakes/{name}/catalogs/{catalog}/schemas
- GET  /api/metalakes/{name}/catalogs/{catalog}/schemas/{schema}
- POST /api/metalakes/{name}/catalogs/{catalog}/schemas/{schema}/tables
- GET  /api/metalakes/{name}/catalogs/{catalog}/schemas/{schema}/tables
- GET  /api/metalakes/{name}/catalogs/{catalog}/schemas/{schema}/tables/{table}
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

gravitino = FastAPI(title="Gravitino REST Server", version="1.2.1")

gravitino.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory metadata store ──
_metalakes: Dict[str, Dict] = {}
_catalogs: Dict[str, Dict[str, Dict]] = {}
_schemas: Dict[str, Dict[str, Dict[str, Dict]]] = {}
_tables: Dict[str, Dict[str, Dict[str, Dict[str, Dict]]]] = {}

# ── Pydantic models ──

class MetalakeRequest(BaseModel):
    name: str
    comment: Optional[str] = ""
    properties: Optional[Dict[str, str]] = {}

class CatalogRequest(BaseModel):
    name: str
    type: str  # "relational", "fileset", "messaging"
    provider: str  # "hadoop", "lance", "iceberg", etc.
    comment: Optional[str] = ""
    properties: Optional[Dict[str, str]] = {}

class SchemaRequest(BaseModel):
    name: str
    comment: Optional[str] = ""
    properties: Optional[Dict[str, str]] = {}

class ColumnDef(BaseModel):
    name: str
    type: str
    nullable: Optional[bool] = True
    comment: Optional[str] = ""

class TableRequest(BaseModel):
    name: str
    columns: List[ColumnDef]
    comment: Optional[str] = ""
    properties: Optional[Dict[str, str]] = {}


def _now():
    return datetime.now().isoformat()


# ── Metalake APIs ──

@gravitino.post("/api/metalakes")
def create_metalake(req: MetalakeRequest):
    if req.name in _metalakes:
        raise HTTPException(409, f"Metalake '{req.name}' already exists")
    ml = {
        "name": req.name,
        "comment": req.comment,
        "properties": req.properties,
        "audit": {"creator": "demo", "createTime": _now()},
    }
    _metalakes[req.name] = ml
    _catalogs[req.name] = {}
    return {"code": 0, "metalake": ml}


@gravitino.get("/api/metalakes")
def list_metalakes():
    metalakes = []
    for name, ml in _metalakes.items():
        metalakes.append({"name": name, "comment": ml.get("comment", "")})
    return {"code": 0, "metalakes": metalakes}


@gravitino.get("/api/metalakes/{name}")
def get_metalake(name: str):
    ml = _metalakes.get(name)
    if not ml:
        raise HTTPException(404, f"Metalake '{name}' not found")
    catalog_names = list(_catalogs.get(name, {}).keys())
    return {
        "code": 0,
        "metalake": {**ml, "catalogs": catalog_names},
    }


# ── Catalog APIs ──

@gravitino.post("/api/metalakes/{metalake}/catalogs")
def create_catalog(metalake: str, req: CatalogRequest):
    if metalake not in _metalakes:
        raise HTTPException(404, f"Metalake '{metalake}' not found")
    cat = {
        "name": req.name,
        "type": req.type,
        "provider": req.provider,
        "comment": req.comment,
        "properties": req.properties,
        "audit": {"creator": "demo", "createTime": _now()},
    }
    _catalogs[metalake][req.name] = cat
    key = f"{metalake}/{req.name}"
    _schemas[key] = {}
    return {"code": 0, "catalog": cat}


@gravitino.get("/api/metalakes/{metalake}/catalogs")
def list_catalogs(metalake: str):
    if metalake not in _metalakes:
        raise HTTPException(404, f"Metalake '{metalake}' not found")
    cats = []
    for name, cat in _catalogs.get(metalake, {}).items():
        cats.append({
            "name": name,
            "type": cat["type"],
            "provider": cat["provider"],
        })
    return {"code": 0, "catalogs": cats}


@gravitino.get("/api/metalakes/{metalake}/catalogs/{catalog}")
def get_catalog(metalake: str, catalog: str):
    cat = _catalogs.get(metalake, {}).get(catalog)
    if not cat:
        raise HTTPException(404, f"Catalog '{catalog}' not found")
    key = f"{metalake}/{catalog}"
    schema_names = list(_schemas.get(key, {}).keys())
    return {"code": 0, "catalog": {**cat, "schemas": schema_names}}


# ── Schema APIs ──

@gravitino.post("/api/metalakes/{metalake}/catalogs/{catalog}/schemas")
def create_schema(metalake: str, catalog: str, req: SchemaRequest):
    if _catalogs.get(metalake, {}).get(catalog) is None:
        raise HTTPException(404, f"Catalog '{catalog}' not found")
    key = f"{metalake}/{catalog}"
    schema = {
        "name": req.name,
        "comment": req.comment,
        "properties": req.properties,
        "audit": {"creator": "demo", "createTime": _now()},
    }
    _schemas.setdefault(key, {})[req.name] = schema
    table_key = f"{key}/{req.name}"
    _tables[table_key] = {}
    return {"code": 0, "schema": schema}


@gravitino.get("/api/metalakes/{metalake}/catalogs/{catalog}/schemas")
def list_schemas(metalake: str, catalog: str):
    if _catalogs.get(metalake, {}).get(catalog) is None:
        raise HTTPException(404, f"Catalog '{catalog}' not found")
    key = f"{metalake}/{catalog}"
    schemas = []
    for name, s in _schemas.get(key, {}).items():
        schemas.append({"name": name, "comment": s.get("comment", "")})
    return {"code": 0, "schemas": schemas}


@gravitino.get("/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}")
def get_schema(metalake: str, catalog: str, schema: str):
    key = f"{metalake}/{catalog}"
    s = _schemas.get(key, {}).get(schema)
    if not s:
        raise HTTPException(404, f"Schema '{schema}' not found")
    table_key = f"{key}/{schema}"
    table_names = list(_tables.get(table_key, {}).keys())
    return {"code": 0, "schema": {**s, "tables": table_names}}


# ── Table APIs ──

@gravitino.post(
    "/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables"
)
def create_table(metalake: str, catalog: str, schema: str, req: TableRequest):
    key = f"{metalake}/{catalog}"
    if _schemas.get(key, {}).get(schema) is None:
        raise HTTPException(404, f"Schema '{schema}' not found")
    table_key = f"{key}/{schema}"
    table = {
        "name": req.name,
        "columns": [c.model_dump() for c in req.columns],
        "comment": req.comment,
        "properties": req.properties,
        "audit": {"creator": "demo", "createTime": _now()},
    }
    _tables.setdefault(table_key, {})[req.name] = table
    return {"code": 0, "table": table}


@gravitino.get(
    "/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables"
)
def list_tables(metalake: str, catalog: str, schema: str):
    key = f"{metalake}/{catalog}"
    if _schemas.get(key, {}).get(schema) is None:
        raise HTTPException(404, f"Schema '{schema}' not found")
    table_key = f"{key}/{schema}"
    tables = []
    for name, t in _tables.get(table_key, {}).items():
        tables.append({"name": name, "comment": t.get("comment", "")})
    return {"code": 0, "tables": tables}


@gravitino.get(
    "/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}"
)
def get_table(metalake: str, catalog: str, schema: str, table: str):
    key = f"{metalake}/{catalog}"
    table_key = f"{key}/{schema}"
    t = _tables.get(table_key, {}).get(table)
    if not t:
        raise HTTPException(404, f"Table '{table}' not found")
    return {"code": 0, "table": t}


@gravitino.get("/api/health")
def health():
    return {
        "status": "ok",
        "version": "1.2.1 (demo server)",
        "metalakes": len(_metalakes),
    }


# ── Demo data initializer ──

def init_demo_metalake():
    """Initialize a demo metalake with Lance, Iceberg, and Fileset catalogs."""
    # Create metalake
    create_metalake(MetalakeRequest(
        name="demo_metalake",
        comment="多模态湖仓处理平台演示 Metalake",
        properties={"owner": "demo-platform"},
    ))

    # Lance catalog
    create_catalog("demo_metalake", CatalogRequest(
        name="lance_catalog",
        type="relational",
        provider="lance",
        comment="Lance format catalog for multimodal vector search",
        properties={
            "location": "s3://lakehouse-demo/lance",
            "format": "lance",
        },
    ))

    # Iceberg catalog
    create_catalog("demo_metalake", CatalogRequest(
        name="iceberg_catalog",
        type="relational",
        provider="iceberg",
        comment="Iceberg table format catalog for batch analytics",
        properties={
            "location": "s3://lakehouse-demo/iceberg",
            "format": "iceberg",
            "format-version": "2",
        },
    ))

    # Fileset catalog
    create_catalog("demo_metalake", CatalogRequest(
        name="fileset_catalog",
        type="fileset",
        provider="hadoop",
        comment="Fileset catalog for raw files (audio, video, images)",
        properties={
            "location": "s3://lakehouse-demo/raw",
        },
    ))

    # Lance schemas
    for schema_name, comment in [
        ("voice_analysis", "Voice call analysis results with embeddings"),
        ("camera_signal", "Camera checkpoint vs signal data comparison"),
        ("image_processing", "Image cleaning, labeling, and detoxification"),
    ]:
        create_schema("demo_metalake", "lance_catalog", SchemaRequest(
            name=schema_name, comment=comment,
        ))

    # Iceberg schemas
    for schema_name, comment in [
        ("churn_risk", "Customer churn risk analysis and predictions"),
        ("daily_analytics", "Daily aggregated analytics data"),
    ]:
        create_schema("demo_metalake", "iceberg_catalog", SchemaRequest(
            name=schema_name, comment=comment,
        ))

    # Fileset schemas
    for schema_name, comment in [
        ("raw_audio", "Raw call center audio recordings"),
        ("raw_video", "Raw surveillance video footage"),
        ("raw_images", "Raw image data for processing"),
    ]:
        create_schema("demo_metalake", "fileset_catalog", SchemaRequest(
            name=schema_name, comment=comment,
        ))

    # Lance tables for voice_analysis
    create_table("demo_metalake", "lance_catalog", "voice_analysis", TableRequest(
        name="call_analysis",
        columns=[
            ColumnDef(name="call_id", type="string", nullable=False),
            ColumnDef(name="transcript", type="string"),
            ColumnDef(name="caller_intent", type="string"),
            ColumnDef(name="switch_reason", type="string"),
            ColumnDef(name="sentiment", type="string"),
            ColumnDef(name="risk_level", type="string"),
            ColumnDef(name="embedding", type="array<float>"),
            ColumnDef(name="processed_at", type="timestamp"),
        ],
        comment="Parsed call analysis results with vector embeddings for similarity search",
    ))

    # Iceberg tables for churn_risk
    create_table("demo_metalake", "iceberg_catalog", "churn_risk", TableRequest(
        name="churn_predictions",
        columns=[
            ColumnDef(name="date", type="date", nullable=False),
            ColumnDef(name="total_calls", type="int"),
            ColumnDef(name="churn_intent_count", type="int"),
            ColumnDef(name="high_risk_count", type="int"),
            ColumnDef(name="top_reason", type="string"),
            ColumnDef(name="retention_rate", type="float"),
        ],
        comment="Daily churn risk aggregation for time-series analysis",
    ))

    # Iceberg tables for daily_analytics
    create_table("demo_metalake", "iceberg_catalog", "daily_analytics", TableRequest(
        name="hourly_metrics",
        columns=[
            ColumnDef(name="hour", type="timestamp", nullable=False),
            ColumnDef(name="call_volume", type="int"),
            ColumnDef(name="avg_sentiment", type="float"),
            ColumnDef(name="resolution_rate", type="float"),
        ],
        comment="Hourly call center metrics for operational dashboards",
    ))


if __name__ == "__main__":
    import uvicorn
    init_demo_metalake()
    print("Gravitino demo server initialized with demo_metalake")
    uvicorn.run(gravitino, host="0.0.0.0", port=8090)
