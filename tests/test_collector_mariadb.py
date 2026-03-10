"""Testes unitários para fluxo MariaDB no collector e report."""

from unittest.mock import MagicMock, patch

import pytest

pymysql = pytest.importorskip("pymysql", reason="PyMySQL não instalado")

from sqlmentor.adapters.mariadb import MariaDBAdapter
from sqlmentor.collector import (
    CollectedContext,
    _collect_explain_plan,
    _collect_runtime_execution,
    _parse_view_tables,
)
from sqlmentor.connector import validate_privileges
from sqlmentor.parser import ParsedSQL
from sqlmentor.report import (
    _detect_plan_blocks,
    _extract_plan_index_names,
    _is_estimated_plan,
)


def _make_parsed_sql(raw: str = "SELECT 1") -> ParsedSQL:
    """Helper: cria ParsedSQL mínimo para testes."""
    return ParsedSQL(
        raw_sql=raw,
        sql_type="SELECT",
    )


# ─── TestMariaDBExplainPlan ─────────────────────────────────────────


class TestMariaDBExplainPlan:
    """Testa _collect_explain_plan com adapter MariaDB (1-step)."""

    def test_single_step_returns_json_lines(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        json_result = '{"query_block": {"table": {"table_name": "t1", "access_type": "ALL"}}}'
        cursor.fetchone.return_value = (json_result,)

        ctx = CollectedContext(parsed_sql=_make_parsed_sql(), db_type="mariadb")
        result = _collect_explain_plan(cursor, "SELECT * FROM t1", ctx, adapter)

        assert result is not None
        assert len(result) >= 1
        # Verifica que EXPLAIN FORMAT=JSON foi executado
        executed_sql = cursor.execute.call_args[0][0]
        assert executed_sql.startswith("EXPLAIN FORMAT=JSON ")

    def test_single_step_empty_result_returns_none(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        cursor.fetchone.return_value = None

        ctx = CollectedContext(parsed_sql=_make_parsed_sql(), db_type="mariadb")
        result = _collect_explain_plan(cursor, "SELECT 1", ctx, adapter)
        assert result is None

    def test_single_step_error_adds_to_errors(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("syntax error")

        ctx = CollectedContext(parsed_sql=_make_parsed_sql(), db_type="mariadb")
        result = _collect_explain_plan(cursor, "INVALID SQL", ctx, adapter)
        assert result is None
        assert any("EXPLAIN PLAN" in e for e in ctx.errors)


# ─── TestMariaDBDDLColumnRemap ──────────────────────────────────────


class TestMariaDBDDLColumnRemap:
    """Testa que execute_query remapeia colunas SHOW CREATE → ddl."""

    def test_create_table_remapped_to_ddl(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        cursor.description = [("Table",), ("Create Table",)]
        cursor.__iter__ = MagicMock(return_value=iter([("orders", "CREATE TABLE orders (id INT)")]))
        cursor.execute = MagicMock()

        rows = adapter.execute_query(cursor, "SHOW CREATE TABLE orders", {})
        assert len(rows) == 1
        # "Create Table" → "ddl" após remap (lowercase + remap)
        assert "ddl" in rows[0]
        assert rows[0]["ddl"] == "CREATE TABLE orders (id INT)"

    def test_non_ddl_columns_unchanged(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        cursor.description = [("name",), ("value",)]
        cursor.__iter__ = MagicMock(return_value=iter([("x", "y")]))
        cursor.execute = MagicMock()

        rows = adapter.execute_query(cursor, "SELECT name, value FROM t", {})
        assert rows[0] == {"name": "x", "value": "y"}


# ─── TestMariaDBRuntimeExecution ────────────────────────────────────


class TestMariaDBRuntimeExecution:
    """Testa _collect_runtime_execution branch MariaDB."""

    def test_analyze_format_json_executed(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        conn = MagicMock()

        json_result = '{"query_block": {"table": {"table_name": "t1", "access_type": "ALL", "r_rows": 10, "r_loops": 1, "r_total_time_ms": 0.5}}}'

        # session_sid → SID
        # ANALYZE → JSON result
        # prev_sql_id → digest
        # sql_runtime_stats → stats
        # session_wait_events → events
        call_count = [0]

        def mock_execute(sql, params=None):
            call_count[0] += 1

        def mock_fetchone():
            n = call_count[0]
            if n == 1:  # session_sid
                return (42,)
            if n == 2:  # ANALYZE FORMAT=JSON
                return (json_result,)
            if n == 3:  # prev_sql_id
                return ("abc123",)
            return None

        cursor.execute = mock_execute
        cursor.fetchone = mock_fetchone

        # Mock execute_query para sql_runtime_stats e wait_events
        stats_result = [{"sql_id": "abc123", "executions": 1}]
        wait_result = [{"event": "wait/io", "total_waits": 5}]
        adapter.execute_query = MagicMock(side_effect=[stats_result, wait_result])

        ctx = CollectedContext(parsed_sql=_make_parsed_sql(), db_type="mariadb")
        _collect_runtime_execution(cursor, conn, "SELECT * FROM t1", ctx, adapter)

        assert ctx.runtime_plan is not None
        assert any("query_block" in line for line in ctx.runtime_plan)
        assert any("ANALYZE re-executa" in e for e in ctx.errors)

    def test_timeout_error_adds_message(self):
        adapter = MariaDBAdapter()
        cursor = MagicMock()
        conn = MagicMock()

        cursor.execute.side_effect = Exception("connection timed out")

        ctx = CollectedContext(parsed_sql=_make_parsed_sql(), db_type="mariadb")
        _collect_runtime_execution(cursor, conn, "SELECT 1", ctx, adapter)

        assert any("timeout" in e.lower() for e in ctx.errors)


# ─── TestReportPlanDispatch ─────────────────────────────────────────


class TestReportPlanDispatch:
    """Testa que _detect_plan_blocks e _is_estimated_plan despacham para MariaDBPlanParser."""

    def test_detect_plan_blocks_mariadb(self):
        lines = [
            '{"query_block": {"table": {"table_name": "t1", "access_type": "ALL", "rows_examined_per_scan": 100}}}'
        ]
        blocks = _detect_plan_blocks(lines, db_type="mariadb")
        assert len(blocks) == 1
        assert blocks[0].name == "t1"
        assert blocks[0].operation == "ALL"

    def test_detect_plan_blocks_oracle_default(self):
        """db_type default usa OraclePlanParser — linhas inválidas retornam vazio."""
        blocks = _detect_plan_blocks(["not a plan"], db_type="oracle")
        assert blocks == []

    def test_is_estimated_plan_mariadb_explain(self):
        lines = ['{"query_block": {"table": {"table_name": "t1", "access_type": "ALL"}}}']
        assert _is_estimated_plan(lines, db_type="mariadb") is True

    def test_is_estimated_plan_mariadb_analyze(self):
        lines = [
            '{"query_block": {"table": {"table_name": "t1", "access_type": "ALL", "r_rows": 10}}}'
        ]
        assert _is_estimated_plan(lines, db_type="mariadb") is False

    def test_extract_plan_index_names_mariadb(self):
        lines = ['{"query_block": {"table": {"table_name": "idx_orders", "access_type": "INDEX"}}}']
        names = _extract_plan_index_names(lines, db_type="mariadb")
        assert "idx_orders" in names


# ─── TestValidatePrivilegesMariaDB ──────────────────────────────────


class TestValidatePrivilegesMariaDB:
    """Testa validate_privileges adapter-aware."""

    def test_uses_adapter_when_provided(self):
        adapter = MagicMock()
        adapter.validate_privileges.return_value = {
            "dangerous_privileges": [],
            "dangerous_roles": [],
        }
        conn = MagicMock()
        conn.user = "testuser"

        # Não deve levantar PermissionError
        validate_privileges(conn, adapter=adapter)
        adapter.validate_privileges.assert_called_once_with(conn)

    def test_raises_on_dangerous_privileges(self):
        adapter = MagicMock()
        adapter.validate_privileges.return_value = {
            "dangerous_privileges": ["DROP"],
            "dangerous_roles": [],
        }
        conn = MagicMock()
        conn.user = "baduser"
        # Remove username attr so getattr falls back to user
        del conn.username

        with pytest.raises(PermissionError, match="baduser"):
            validate_privileges(conn, adapter=adapter)

    def test_falls_back_to_oracle_without_adapter(self):
        """Sem adapter, usa OracleAdapter (backward compat)."""
        with patch("sqlmentor.adapters.get_adapter") as mock_get:
            mock_adapter_cls = MagicMock()
            mock_adapter_instance = MagicMock()
            mock_adapter_instance.validate_privileges.return_value = {
                "dangerous_privileges": [],
                "dangerous_roles": [],
            }
            mock_adapter_cls.return_value = mock_adapter_instance
            mock_get.return_value = mock_adapter_cls

            conn = MagicMock()
            conn.username = "orauser"
            validate_privileges(conn)

            mock_get.assert_called_once_with("oracle")


# ─── TestParseViewTablesDialect ─────────────────────────────────────


class TestParseViewTablesDialect:
    """Testa que _parse_view_tables aceita dialect para sqlglot."""

    def test_mysql_dialect_parses_backtick_identifiers(self):
        ddl = (
            "CREATE VIEW `mydb`.`v1` AS SELECT * FROM `orders` o JOIN `customers` c ON o.cid = c.id"
        )
        tables = _parse_view_tables(ddl, dialect="mysql")
        # Deve encontrar orders e customers
        table_names = {t.upper() for t in tables}
        assert "ORDERS" in table_names
        assert "CUSTOMERS" in table_names

    def test_oracle_dialect_default(self):
        ddl = 'CREATE VIEW "SCHEMA"."V1" AS SELECT * FROM "ORDERS"'
        tables = _parse_view_tables(ddl, dialect="oracle")
        assert any("ORDERS" in t.upper() for t in tables)


# ─── TestCollectedContextDbType ─────────────────────────────────────


class TestCollectedContextDbType:
    """Testa campo db_type em CollectedContext."""

    def test_default_is_oracle(self):
        ctx = CollectedContext(parsed_sql=_make_parsed_sql())
        assert ctx.db_type == "oracle"

    def test_can_set_mariadb(self):
        ctx = CollectedContext(parsed_sql=_make_parsed_sql(), db_type="mariadb")
        assert ctx.db_type == "mariadb"
