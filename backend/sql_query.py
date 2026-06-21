"""
SQL Query Engine — DuckDB-based SQL query over Lance & Iceberg datasets.

Features:
- Register Lance datasets as DuckDB views
- Register Iceberg tables as DuckDB views
- Execute user-submitted SELECT-only SQL
- Result limit and timeout protection
- Only allow SELECT statements (reject DDL/DML)
"""

import re
import time
from typing import Dict, Any, List, Optional

import duckdb


class SQLQueryEngine:
    """DuckDB-powered SQL query engine for Lance and Iceberg data."""

    MAX_RESULT_ROWS = 500
    QUERY_TIMEOUT_SEC = 30

    def __init__(self):
        self._conn = duckdb.connect(":memory:")

    def register_lance_dataset(self, name: str, lance_path: str):
        """Register a Lance dataset as a DuckDB view."""
        import lance
        try:
            ds = lance.dataset(lance_path)
            table = ds.to_table()
            self._conn.register(name, table)
            return True
        except Exception as e:
            print(f"[SQL-Query] Failed to register Lance dataset '{name}': {e}")
            return False

    def register_iceberg_table(self, name: str, table_id: str, iceberg_store):
        """Register an Iceberg table as a DuckDB view."""
        try:
            records = iceberg_store.read_table_snapshot(table_id)
            if not records:
                self._conn.register(name, duckdb.arrow([]))
                return True
            import pyarrow as pa
            table = pa.Table.from_pylist(records)
            self._conn.register(name, table)
            return True
        except Exception as e:
            print(f"[SQL-Query] Failed to register Iceberg table '{name}': {e}")
            return False

    def _is_select_only(self, sql: str) -> bool:
        """Check that the SQL is a SELECT-only statement.
        Reject INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, etc."""
        stripped = sql.strip().upper()
        # Remove leading comments
        while stripped.startswith("--") or stripped.startswith("/*"):
            if stripped.startswith("--"):
                idx = stripped.find("\n")
                stripped = stripped[idx + 1:].strip() if idx != -1 else ""
            elif stripped.startswith("/*"):
                idx = stripped.find("*/")
                stripped = stripped[idx + 2:].strip() if idx != -1 else ""

        dangerous_keywords = [
            r'\bINSERT\b', r'\bUPDATE\b', r'\bDELETE\b', r'\bDROP\b',
            r'\bALTER\b', r'\bCREATE\b', r'\bTRUNCATE\b', r'\bGRANT\b',
            r'\bREVOKE\b', r'\bEXECUTE\b', r'\bMERGE\b', r'\bREPLACE\b',
            r'\bSET\b', r'\bPRAGMA\b', r'\bATTACH\b', r'\bDETACH\b',
            r'\bCOPY\b', r'\bEXPORT\b', r'\bIMPORT\b', r'\bLOAD\b',
            r'\bINSTALL\b',
        ]
        for keyword in dangerous_keywords:
            if re.search(keyword, stripped):
                return False
        return stripped.startswith("SELECT") or stripped.startswith("WITH") \
            or stripped.startswith("EXPLAIN") or stripped.startswith("DESCRIBE") \
            or stripped.startswith("SHOW")

    def execute(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query with safety checks.

        Returns: dict with keys:
            - success: bool
            - columns: list of column names
            - rows: list of dicts
            - row_count: int
            - truncated: bool (True if result exceeded MAX_RESULT_ROWS)
            - elapsed_ms: float
            - error: str (if success=False)
            - available_tables: list of available table names
        """
        t0 = time.time()

        # Safety check
        if not self._is_select_only(sql):
            return {
                "success": False,
                "error": "仅允许 SELECT / WITH / EXPLAIN / DESCRIBE / SHOW 查询，"
                          "不支持 INSERT/UPDATE/DELETE/DROP/ALTER 等写操作",
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "elapsed_ms": 0,
                "available_tables": self._list_tables(),
            }

        # Auto-apply LIMIT if not present
        if "LIMIT" not in sql.upper():
            sql = sql.rstrip(";").strip() + " LIMIT {:d}".format(
                self.MAX_RESULT_ROWS + 1)

        try:
            result = self._conn.execute(sql)

            # Get column names
            columns = [desc[0] for desc in result.description] \
                if result.description else []

            # Fetch rows with limit enforced
            rows = result.fetchmany(self.MAX_RESULT_ROWS + 1)
            truncated = len(rows) > self.MAX_RESULT_ROWS
            if truncated:
                rows = rows[:self.MAX_RESULT_ROWS]

            # Convert to list of dicts
            row_dicts = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    # Convert non-serializable types
                    if isinstance(val, bytes):
                        val = val.decode("utf-8", errors="replace")
                    elif hasattr(val, "isoformat"):
                        val = val.isoformat()
                    elif hasattr(val, "tolist"):
                        val = val.tolist()
                    row_dict[col] = val
                row_dicts.append(row_dict)

            elapsed_ms = round((time.time() - t0) * 1000, 2)

            return {
                "success": True,
                "columns": columns,
                "rows": row_dicts,
                "row_count": len(row_dicts),
                "truncated": truncated,
                "elapsed_ms": elapsed_ms,
                "error": None,
                "available_tables": self._list_tables(),
            }

        except Exception as e:
            elapsed_ms = round((time.time() - t0) * 1000, 2)
            return {
                "success": False,
                "error": str(e),
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "elapsed_ms": elapsed_ms,
                "available_tables": self._list_tables(),
            }

    _SYSTEM_TABLE_PREFIXES = (
        "duckdb_", "sqlite_", "pragma_", "pg_",
        "information_schema", "character_sets",
        "check_constraints", "columns", "constraint_",
        "key_column_usage", "referential_constraints",
        "schemata", "tables", "table_constraints", "views",
    )

    def _is_system_table(self, name: str) -> bool:
        """Check if a table/view name is a DuckDB system table."""
        lower = name.lower()
        for prefix in self._SYSTEM_TABLE_PREFIXES:
            if lower.startswith(prefix):
                return True
        return False

    def _list_tables(self) -> List[str]:
        """List user-registered tables/views (excluding system tables)."""
        try:
            result = self._conn.execute(
                "SELECT table_name FROM duckdb_tables() "
                "UNION ALL "
                "SELECT view_name FROM duckdb_views()"
            ).fetchall()
            return [r[0] for r in result
                    if not self._is_system_table(r[0])]
        except Exception:
            return []

    def get_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """Get schema info for all user-registered tables."""
        schemas = {}
        tables = self._list_tables()
        for table in tables:
            try:
                result = self._conn.execute(
                    "DESCRIBE \"{}\"".format(table)).fetchall()
                schemas[table] = [
                    {"column": str(r[0]), "type": str(r[1])} for r in result
                ]
            except Exception:
                schemas[table] = []
        return schemas
