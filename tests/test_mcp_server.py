"""Testes para o MCP server do sqlmentor."""

import json

from sqlmentor.mcp_server import analyze_sql, inspect_sql, list_connections, parse_sql
from sqlmentor.mcp_server import test_connection as mcp_test_connection

# ─── parse_sql ────────────────────────────────────────────────────────────────


class TestMCPParseSql:
    def test_returns_valid_json(self):
        result = parse_sql("SELECT id, name FROM users WHERE status = 'ACTIVE'")
        data = json.loads(result)
        assert "sql_type" in data
        assert "tables" in data
        assert "where_columns" in data
        assert "is_parseable" in data

    def test_select_type(self):
        data = json.loads(parse_sql("SELECT * FROM orders"))
        assert data["sql_type"] == "SELECT"

    def test_tables_extracted(self):
        data = json.loads(
            parse_sql("SELECT * FROM hr.employees e JOIN hr.departments d ON e.dept_id = d.id")
        )
        assert len(data["tables"]) >= 2

    def test_with_schema(self):
        data = json.loads(parse_sql("SELECT * FROM employees", schema="HR"))
        assert data["is_parseable"]

    def test_normalized_sql(self):
        data = json.loads(parse_sql("SELECT * FROM t WHERE a = ? AND b = ?", normalized=True))
        assert data["is_parseable"]

    def test_normalized_auto_detect(self):
        data = json.loads(parse_sql("SELECT * FROM t WHERE a = ? AND b = ?"))
        assert data["is_parseable"]

    def test_denorm_mode_bind(self):
        data = json.loads(parse_sql("SELECT * FROM t WHERE a = ? AND b = ?", denorm_mode="bind"))
        assert data["is_parseable"]

    def test_empty_sql(self):
        data = json.loads(parse_sql(""))
        assert isinstance(data, dict)


# ─── list_connections ─────────────────────────────────────────────────────────


class TestMCPListConnections:
    def test_no_connections(self, tmp_connections_file):
        result = json.loads(list_connections())
        assert "connections" in result
        assert result["connections"] == []

    def test_with_connections(self, tmp_connections_file):
        from sqlmentor.connector import add_connection

        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        result = json.loads(list_connections())
        assert len(result["connections"]) == 1
        assert result["connections"][0]["name"] == "dev"


# ─── test_connection ──────────────────────────────────────────────────────────


class TestMCPTestConnection:
    def test_bad_connection(self, tmp_connections_file):
        result = json.loads(mcp_test_connection("nonexistent"))
        assert result["status"] == "error"


# ─── analyze_sql ──────────────────────────────────────────────────────────────


class TestMCPAnalyzeSql:
    def test_no_connection(self, tmp_connections_file):
        result = json.loads(analyze_sql("SELECT * FROM users"))
        assert "error" in result

    def test_bad_connection(self, tmp_connections_file):
        from sqlmentor.connector import add_connection

        add_connection("dev", "badhost", 1521, "ORCL", "scott", "tiger")
        from sqlmentor.connector import set_default_connection

        set_default_connection("dev")
        result = json.loads(analyze_sql("SELECT * FROM users"))
        assert "error" in result

    def test_normalized_with_execute_rejected(self, tmp_connections_file):
        from sqlmentor.connector import add_connection, set_default_connection

        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        result = json.loads(
            analyze_sql(
                "SELECT * FROM t WHERE a = ? AND b = ?",
                normalized=True,
                execute=True,
            )
        )
        assert "error" in result
        assert "normalizado" in result["error"].lower() or "incompatível" in result["error"].lower()


# ─── inspect_sql ──────────────────────────────────────────────────────────────


class TestMCPInspectSql:
    def test_no_connection(self, tmp_connections_file):
        result = json.loads(inspect_sql("abc123def45"))
        assert "error" in result

    def test_bad_connection(self, tmp_connections_file):
        from sqlmentor.connector import add_connection, set_default_connection

        add_connection("dev", "badhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        result = json.loads(inspect_sql("abc123def45"))
        assert "error" in result


# ─── Bind parsing ─────────────────────────────────────────────────────────────


class TestMCPBindParsing:
    def test_binds_with_null(self, tmp_connections_file):
        """Verify null/none bind values are handled (tested indirectly via analyze_sql)."""
        from sqlmentor.connector import add_connection, set_default_connection

        add_connection("dev", "badhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        # Will fail on connection but should not crash on bind parsing
        result = json.loads(analyze_sql("SELECT * FROM t WHERE a = :a", binds="a=null"))
        assert "error" in result  # Connection error, not bind error
