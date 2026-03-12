"""Testes para inspect MariaDB (T23).

Verifica:
- CLI inspect usa EXPLAIN FORMAT=JSON (não runtime_plan) para MariaDB
- MCP inspect_sql idem
- doctor lida com diagnose MariaDB (sem mode key)
- diagnose_connection retorna schema
"""

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


def _inspect_mariadb_patches(monkeypatch, tmp_path, *, db_type="mariadb"):
    """Configura patches para inspect com adapter MariaDB ou Oracle."""
    out_file = tmp_path / "output.md"
    ctx = _make_ctx()
    mocks = {}

    monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
    monkeypatch.setattr(
        "sqlmentor.connector.get_connection_config",
        lambda name: {"schema": "MYDB", "user": "root", "timeout": 180},
    )

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # sql_text_by_id returns SQL text
    mock_cursor.fetchone.side_effect = [
        ("SELECT 1 FROM orders",),  # sql_text_by_id
        ('{"query_block": {}}',),  # explain_plan (MariaDB) or runtime_stats (Oracle)
        # runtime_stats
        (5, 100),
    ]
    mock_cursor.__iter__ = MagicMock(return_value=iter([("| 1 | SELECT STATEMENT |",)]))
    mock_cursor.description = [("sql_id",), ("executions",)]

    mock_adapter = MagicMock()
    mock_adapter.db_type = db_type

    mock_qb = MagicMock()
    mock_adapter.query_builder = mock_qb

    mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc123"})
    mock_qb.explain_plan.return_value = [("EXPLAIN FORMAT=JSON SELECT 1 FROM orders", {})]
    mock_qb.runtime_plan.return_value = ("SELECT * FROM TABLE(DBMS_XPLAN...)", {"sql_id": "abc123"})
    mock_qb.sql_runtime_stats.return_value = ("SELECT ... FROM perf_schema", {"sql_id": "abc123"})

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


# ─── TestInspectMariaDBCLI ────────────────────────────────────────────


class TestInspectMariaDBCLI:
    """CLI inspect usa EXPLAIN FORMAT=JSON para MariaDB."""

    def test_mariadb_uses_explain_not_runtime(self, monkeypatch, tmp_path):
        out_file, mocks = _inspect_mariadb_patches(monkeypatch, tmp_path, db_type="mariadb")
        result = runner.invoke(
            app, ["inspect", "abc123", "--conn", "test", "--output", str(out_file)]
        )

        assert result.exit_code == 0
        qb = mocks["qb"]
        qb.explain_plan.assert_called_once()
        qb.runtime_plan.assert_not_called()

    def test_mariadb_sets_execution_plan_not_runtime(self, monkeypatch, tmp_path):
        out_file, mocks = _inspect_mariadb_patches(monkeypatch, tmp_path, db_type="mariadb")
        runner.invoke(app, ["inspect", "abc123", "--conn", "test", "--output", str(out_file)])

        ctx = mocks["ctx"]
        # Execution plan was set (estimated plan for MariaDB)
        assert ctx.execution_plan is not None

    def test_mariadb_prints_estimated_message(self, monkeypatch, tmp_path):
        out_file, _mocks = _inspect_mariadb_patches(monkeypatch, tmp_path, db_type="mariadb")
        result = runner.invoke(
            app, ["inspect", "abc123", "--conn", "test", "--output", str(out_file)]
        )

        assert "plano estimado" in result.output.lower() or "MariaDB" in result.output


class TestInspectOracleCLIUnchanged:
    """Oracle inspect path unchanged — still uses runtime_plan."""

    def test_oracle_uses_runtime_plan(self, monkeypatch, tmp_path):
        out_file, mocks = _inspect_mariadb_patches(monkeypatch, tmp_path, db_type="oracle")
        result = runner.invoke(
            app, ["inspect", "abc123", "--conn", "test", "--output", str(out_file)]
        )

        assert result.exit_code == 0
        qb = mocks["qb"]
        qb.runtime_plan.assert_called_once()
        qb.explain_plan.assert_not_called()


# ─── TestInspectMariaDBMCP ────────────────────────────────────────────


class TestInspectMariaDBMCP:
    """MCP inspect_sql usa EXPLAIN FORMAT=JSON para MariaDB."""

    def test_mariadb_uses_explain(self, monkeypatch):
        from sqlmentor.mcp_server import inspect_sql

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
            ('{"query_block": {}}',),  # explain_plan
            (5, 100),  # runtime_stats
        ]
        mock_cursor.description = [("sql_id",), ("executions",)]

        mock_adapter = MagicMock()
        mock_adapter.db_type = "mariadb"
        mock_qb = MagicMock()
        mock_adapter.query_builder = mock_qb
        mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc"})
        mock_qb.explain_plan.return_value = [("EXPLAIN FORMAT=JSON ...", {})]
        mock_qb.sql_runtime_stats.return_value = ("SELECT ...", {"sql_id": "abc"})

        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(return_value=(mock_adapter, mock_conn)),
        )

        ctx = _make_ctx()
        monkeypatch.setattr("sqlmentor.collector.collect_context", MagicMock(return_value=ctx))
        monkeypatch.setattr("sqlmentor.report.to_markdown", MagicMock(return_value="# Report"))

        inspect_sql("abc123", conn="test")

        mock_qb.explain_plan.assert_called_once()
        mock_qb.runtime_plan.assert_not_called()


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
        assert "inspect" in result.output


# ─── TestDiagnoseConnectionSchema ─────────────────────────────────────


# ─── T11: MariaDBQueryBuilder new methods ─────────────────────────────


class TestMariaDBInspectQueryBuilder:
    """T11: sql_text_original e setup_consumers existem no query builder."""

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


# ─── T11: CLI inspect with sql_text_original fallback ──────────────────


class TestInspectMariaDBOriginalText:
    """T11: inspect tenta sql_text_original antes de sql_text_by_id."""

    def test_uses_sql_text_original_first(self, monkeypatch, tmp_path):
        out_file, mocks = _inspect_mariadb_patches(monkeypatch, tmp_path, db_type="mariadb")

        # Override fetchone to simulate sql_text_original returning result
        call_count = [0]

        def mock_fetchone():
            call_count[0] += 1
            if call_count[0] == 1:  # sql_text_original
                return ("SELECT 1 FROM orders",)
            if call_count[0] == 2:  # explain_plan
                return ('{"query_block": {}}',)
            return None

        mocks["cursor"].fetchone = mock_fetchone

        result = runner.invoke(
            app, ["inspect", "abc123", "--conn", "test", "--output", str(out_file)]
        )
        assert result.exit_code == 0
        # sql_text_original should have been called
        mocks["qb"].sql_text_original.assert_called_once_with("abc123")


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
