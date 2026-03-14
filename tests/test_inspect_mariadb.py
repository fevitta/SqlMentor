"""Testes para inspect MariaDB (T19 — bloqueado) e funcionalidades relacionadas.

Verifica:
- CLI inspect bloqueia MariaDB com exit 1 (sem conectar)
- MCP inspect_sql retorna JSON error para MariaDB
- Oracle inspect path inalterado
- doctor lida com diagnose MariaDB (sem mode key)
- diagnose_connection retorna schema
- QueryBuilder: sql_text_original e setup_consumers existem
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

pymysql = pytest.importorskip("pymysql", reason="PyMySQL não instalado")

from sqlmentor.cli import app
from sqlmentor.collector import CollectedContext
from sqlmentor.parser import ParsedSQL

runner = CliRunner()


# ─── Helpers ──────────────────────────────────────────────────────────


def _make_parsed():
    return ParsedSQL(
        raw_sql="SELECT 1 FROM orders",
        sql_type="SELECT",
        tables=[{"name": "ORDERS", "schema": "MYDB", "alias": None}],
        where_columns=[],
    )


def _make_ctx(**overrides):
    defaults = {
        "parsed_sql": _make_parsed(),
        "db_type": "mariadb",
        "db_version": "10.6.12-MariaDB",
        "execution_plan": None,
        "runtime_plan": None,
        "runtime_stats": None,
        "tables": [],
        "optimizer_params": {},
        "errors": [],
    }
    defaults.update(overrides)
    return CollectedContext(**defaults)


def _inspect_oracle_patches(monkeypatch, tmp_path):
    """Configura patches para inspect com adapter Oracle."""
    out_file = tmp_path / "output.md"
    ctx = _make_ctx(db_type="oracle")
    mocks = {}

    monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
    monkeypatch.setattr(
        "sqlmentor.connector.get_connection_config",
        lambda name: {"schema": "MYDB", "user": "root", "timeout": 180},
    )

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [
        ("SELECT 1 FROM orders",),  # sql_text_by_id
        (5, 100),  # runtime_stats
    ]
    mock_cursor.__iter__ = MagicMock(return_value=iter([("| 1 | SELECT STATEMENT |",)]))
    mock_cursor.description = [("sql_id",), ("executions",)]

    mock_adapter = MagicMock()
    mock_adapter.db_type = "oracle"

    mock_qb = MagicMock()
    mock_adapter.query_builder = mock_qb

    mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc123"})
    mock_qb.runtime_plan.return_value = ("SELECT * FROM TABLE(DBMS_XPLAN...)", {"sql_id": "abc123"})
    mock_qb.sql_runtime_stats.return_value = ("SELECT ... FROM v$sql", {"sql_id": "abc123"})

    monkeypatch.setattr(
        "sqlmentor.connector.connect_with_adapter",
        MagicMock(return_value=(mock_adapter, mock_conn)),
    )
    mocks["conn"] = mock_conn
    mocks["cursor"] = mock_cursor
    mocks["adapter"] = mock_adapter
    mocks["qb"] = mock_qb

    monkeypatch.setattr("sqlmentor.parser.parse_sql", lambda sql, **kw: _make_parsed())
    monkeypatch.setattr("sqlmentor.collector.collect_context", MagicMock(return_value=ctx))
    mocks["ctx"] = ctx

    mock_to_md = MagicMock(return_value="# Report")
    mock_to_json = MagicMock(return_value='{"report": true}')
    monkeypatch.setattr("sqlmentor.report.to_markdown", mock_to_md)
    monkeypatch.setattr("sqlmentor.report.to_json", mock_to_json)
    mocks["to_markdown"] = mock_to_md

    return out_file, mocks


# ─── TestInspectMariaDBBlocked ────────────────────────────────────────


class TestInspectMariaDBBlocked:
    """T19: inspect é bloqueado para MariaDB com exit 1."""

    def test_cli_exits_with_error(self, monkeypatch):
        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "mariadb", "user": "root", "timeout": 180},
        )

        result = runner.invoke(app, ["inspect", "abc123", "--conn", "test"])

        assert result.exit_code == 1
        assert "não é suportado para MariaDB" in result.output

    def test_cli_suggests_get_sql(self, monkeypatch):
        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "mariadb", "user": "root", "timeout": 180},
        )

        result = runner.invoke(app, ["inspect", "abc123", "--conn", "test"])

        assert "get-sql" in result.output
        assert "analyze" in result.output

    def test_cli_does_not_connect(self, monkeypatch):
        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "mariadb", "user": "root", "timeout": 180},
        )
        mock_connect = MagicMock()
        monkeypatch.setattr("sqlmentor.connector.connect_with_adapter", mock_connect)

        runner.invoke(app, ["inspect", "abc123", "--conn", "test"])

        mock_connect.assert_not_called()

    def test_mcp_returns_error_json(self, monkeypatch):
        from sqlmentor.mcp_server import inspect_sql

        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "mariadb", "user": "root", "timeout": 180},
        )
        mock_connect = MagicMock()
        monkeypatch.setattr("sqlmentor.connector.connect_with_adapter", mock_connect)

        result = json.loads(inspect_sql("abc123", conn="test"))

        assert "error" in result
        assert "MariaDB" in result["error"]
        assert "hint" in result
        assert "get_sql_text" in result["hint"]
        mock_connect.assert_not_called()


# ─── TestInspectOracleCLIUnchanged ────────────────────────────────────


class TestInspectOracleCLIUnchanged:
    """Oracle inspect path unchanged — still uses runtime_plan."""

    def test_oracle_uses_runtime_plan(self, monkeypatch, tmp_path):
        out_file, mocks = _inspect_oracle_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app, ["inspect", "abc123", "--conn", "test", "--output", str(out_file)]
        )

        assert result.exit_code == 0
        qb = mocks["qb"]
        qb.runtime_plan.assert_called_once()


# ─── TestDoctorMariaDB ────────────────────────────────────────────────


class TestDoctorMariaDB:
    """doctor lida com diagnose MariaDB (sem mode key)."""

    def test_perf_schema_on(self, monkeypatch):
        monkeypatch.setattr(
            "sqlmentor.connector.list_connections",
            lambda: {"mariadb-dev": {"host": "localhost", "port": 3306, "service": "mydb"}},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.diagnose_connection",
            lambda name: {
                "status": "ok",
                "version": "10.6.12-MariaDB",
                "major_version": "10",
                "performance_schema": "True",
                "schema": "mydb",
            },
        )
        monkeypatch.setattr(
            "sqlmentor.adapters.list_adapters",
            lambda: ["mariadb"],
        )
        mock_adapter_inst = MagicMock()
        mock_adapter_inst.check_deps.return_value = [
            {"name": "PyMySQL", "status": "ok", "detail": "1.1.0"}
        ]
        monkeypatch.setattr(
            "sqlmentor.adapters.get_adapter",
            lambda db_type: lambda: mock_adapter_inst,
        )

        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "performance_schema: ON" in result.output

    def test_perf_schema_off(self, monkeypatch):
        monkeypatch.setattr(
            "sqlmentor.connector.list_connections",
            lambda: {"mariadb-dev": {"host": "localhost", "port": 3306, "service": "mydb"}},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.diagnose_connection",
            lambda name: {
                "status": "ok",
                "version": "10.6.12-MariaDB",
                "major_version": "10",
                "performance_schema": "False",
                "schema": "mydb",
            },
        )
        monkeypatch.setattr(
            "sqlmentor.adapters.list_adapters",
            lambda: ["mariadb"],
        )
        mock_adapter_inst = MagicMock()
        mock_adapter_inst.check_deps.return_value = [
            {"name": "PyMySQL", "status": "ok", "detail": "1.1.0"}
        ]
        monkeypatch.setattr(
            "sqlmentor.adapters.get_adapter",
            lambda db_type: lambda: mock_adapter_inst,
        )

        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "performance_schema: OFF" in result.output
        assert "analyze --execute" in result.output


# ─── TestMariaDBQueryBuilder ─────────────────────────────────────────


class TestMariaDBInspectQueryBuilder:
    """sql_text_original e setup_consumers existem no query builder."""

    def test_sql_text_original(self):
        from sqlmentor.adapters.mariadb import MariaDBQueryBuilder

        qb = MariaDBQueryBuilder()
        sql, params = qb.sql_text_original("abc123")
        assert "events_statements_history_long" in sql
        assert params == {"digest": "abc123"}

    def test_setup_consumers(self):
        from sqlmentor.adapters.mariadb import MariaDBQueryBuilder

        qb = MariaDBQueryBuilder()
        sql, params = qb.setup_consumers()
        assert "setup_consumers" in sql
        assert "statements_digest" in sql
        assert params == {}


# ─── TestDiagnoseConnectionSchema ─────────────────────────────────────


class TestDiagnoseConnectionSchema:
    """MariaDBAdapter.diagnose_connection retorna schema."""

    def test_returns_schema_key(self):
        from sqlmentor.adapters.mariadb import MariaDBAdapter

        config = {
            "host": "localhost",
            "port": 3306,
            "service": "mydb",
            "user": "root",
            "password": "secret",
        }

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            ("10.6.12-MariaDB",),  # VERSION()
            (1,),  # @@performance_schema
            ("mydb",),  # DATABASE()
        ]

        with patch.object(pymysql, "connect", return_value=mock_conn):
            adapter = MariaDBAdapter()
            info = adapter.diagnose_connection(config)

        assert "schema" in info
        assert info["schema"] == "mydb"
        assert "performance_schema" in info
        assert info["version"] == "10.6.12-MariaDB"
