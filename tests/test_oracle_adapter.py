"""Testes unitários para OracleAdapter, OracleQueryBuilder e OraclePlanParser."""

from unittest.mock import MagicMock, patch

import oracledb
import pytest

import sqlmentor.adapters.oracle as oracle_mod
from sqlmentor.adapters import get_adapter
from sqlmentor.adapters.base import DatabaseAdapter, PlanParser, QueryBuilder
from sqlmentor.adapters.oracle import (
    OracleAdapter,
    OraclePlanParser,
    OracleQueryBuilder,
    _init_thick_mode_if_available,
    check_thick_mode_available,
)

# ─── OracleAdapter properties ───────────────────────────────────────


class TestOracleAdapterProperties:
    def test_db_type(self):
        adapter = OracleAdapter()
        assert adapter.db_type == "oracle"

    def test_query_builder_type(self):
        adapter = OracleAdapter()
        assert isinstance(adapter.query_builder, OracleQueryBuilder)
        assert isinstance(adapter.query_builder, QueryBuilder)

    def test_plan_parser_type(self):
        adapter = OracleAdapter()
        assert isinstance(adapter.plan_parser, OraclePlanParser)
        assert isinstance(adapter.plan_parser, PlanParser)

    def test_query_builder_cached(self):
        adapter = OracleAdapter()
        qb1 = adapter.query_builder
        qb2 = adapter.query_builder
        assert qb1 is qb2

    def test_plan_parser_cached(self):
        adapter = OracleAdapter()
        pp1 = adapter.plan_parser
        pp2 = adapter.plan_parser
        assert pp1 is pp2

    def test_is_database_adapter(self):
        assert issubclass(OracleAdapter, DatabaseAdapter)


# ─── OracleAdapter.connect ──────────────────────────────────────────


class TestOracleAdapterConnect:
    @pytest.fixture
    def config(self):
        return {
            "host": "localhost",
            "port": 1521,
            "service": "ORCL",
            "user": "scott",
            "password": "tiger",
            "timeout": 60,
        }

    def test_thin_success(self, config):
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn_string"),
        ):
            adapter = OracleAdapter()
            result = adapter.connect(config)
            assert result is mock_conn
            assert mock_conn.call_timeout == 60_000

    def test_dpy3010_fallback_thick(self, config):
        mock_conn = MagicMock()
        err = oracledb.DatabaseError("DPY-3010: connections to older DB not supported")
        with (
            patch.object(oracledb, "connect", side_effect=[err, mock_conn]),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch("sqlmentor.adapters.oracle._init_thick_mode_if_available") as mock_thick,
        ):
            adapter = OracleAdapter()
            result = adapter.connect(config)
            assert result is mock_conn
            mock_thick.assert_called_once()

    def test_other_error_propagates(self, config):
        err = oracledb.DatabaseError("ORA-12154: TNS error")
        with (
            patch.object(oracledb, "connect", side_effect=err),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            pytest.raises(oracledb.DatabaseError, match="ORA-12154"),
        ):
            OracleAdapter().connect(config)

    def test_timeout_from_config(self, config):
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
        ):
            OracleAdapter().connect(config, timeout=None)
            assert mock_conn.call_timeout == 60_000

    def test_explicit_timeout_overrides_config(self, config):
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
        ):
            OracleAdapter().connect(config, timeout=300)
            assert mock_conn.call_timeout == 300_000

    def test_does_not_validate_privileges(self, config):
        """connect() no adapter NÃO chama validate_privileges."""
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
        ):
            adapter = OracleAdapter()
            with patch.object(adapter, "validate_privileges") as mock_vp:
                adapter.connect(config)
                mock_vp.assert_not_called()


# ─── OracleAdapter.test_connection ──────────────────────────────────


class TestOracleAdapterTestConnection:
    def test_success_returns_true(self):
        config = {
            "host": "localhost",
            "port": 1521,
            "service": "ORCL",
            "user": "scott",
            "password": "tiger",
        }
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
        ):
            assert OracleAdapter().test_connection(config) is True
            mock_conn.close.assert_called_once()

    def test_failure_returns_false(self):
        config = {
            "host": "bad",
            "port": 1521,
            "service": "X",
            "user": "x",
            "password": "x",
        }
        with (
            patch.object(oracledb, "connect", side_effect=Exception("conn failed")),
            patch.object(oracledb, "makedsn", return_value="dsn"),
        ):
            assert OracleAdapter().test_connection(config) is False


# ─── OracleAdapter.validate_privileges ──────────────────────────────


class TestOracleAdapterValidatePrivileges:
    def test_clean_user_returns_empty(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.__iter__ = MagicMock(side_effect=[iter([]), iter([])])

        result = OracleAdapter().validate_privileges(conn)
        assert result == {"dangerous_privileges": [], "dangerous_roles": []}

    def test_dangerous_privs_populated(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        call_count = [0]

        def fake_iter():
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([("INSERT ANY TABLE",)])
            return iter([])

        cursor.__iter__ = MagicMock(side_effect=fake_iter)

        result = OracleAdapter().validate_privileges(conn)
        assert result["dangerous_privileges"] == ["INSERT ANY TABLE"]
        assert result["dangerous_roles"] == []

    def test_dangerous_roles_populated(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        call_count = [0]

        def fake_iter():
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([])
            return iter([("DBA",)])

        cursor.__iter__ = MagicMock(side_effect=fake_iter)

        result = OracleAdapter().validate_privileges(conn)
        assert result["dangerous_privileges"] == []
        assert result["dangerous_roles"] == ["DBA"]

    def test_does_not_raise_permission_error(self):
        """Adapter retorna dict, NÃO levanta PermissionError."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.__iter__ = MagicMock(side_effect=[iter([("INSERT ANY TABLE",)]), iter([("DBA",)])])

        # Não deve levantar — quem decide é o connector
        result = OracleAdapter().validate_privileges(conn)
        assert len(result["dangerous_privileges"]) > 0

    def test_cursor_always_closed(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            OracleAdapter().validate_privileges(conn)

        cursor.close.assert_called_once()


# ─── OracleAdapter.diagnose_connection ──────────────────────────────


class TestOracleAdapterDiagnoseConnection:
    @pytest.fixture
    def config(self):
        return {
            "host": "localhost",
            "port": 1521,
            "service": "ORCL",
            "user": "scott",
            "password": "tiger",
        }

    def test_thin_mode(self, config):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [
            ("Oracle Database 19c Enterprise Edition",),
            ("HR",),
        ]
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch.object(oracledb, "is_thin_mode", return_value=True),
        ):
            result = OracleAdapter().diagnose_connection(config)
            assert result["status"] == "ok"
            assert result["mode"] == "thin"
            assert result["needs_thick"] == "False"
            assert result["major_version"] == "19"
            mock_conn.close.assert_called_once()

    def test_thick_mode_fallback(self, config):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [
            ("Oracle Database 11g Release 11.2.0.4.0",),
            ("SCOTT",),
        ]
        err = oracledb.DatabaseError("DPY-3010: connections to older DB not supported")
        with (
            patch.object(oracledb, "connect", side_effect=[err, mock_conn]),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch("sqlmentor.adapters.oracle._init_thick_mode_if_available"),
            patch.object(oracledb, "is_thin_mode", return_value=False),
        ):
            result = OracleAdapter().diagnose_connection(config)
            assert result["mode"] == "thick"
            assert result["needs_thick"] == "True"
            assert result["major_version"] == "11"


# ─── OracleAdapter.execute_query ────────────────────────────────────


class TestOracleAdapterExecuteQuery:
    def test_basic_execution(self):
        cursor = MagicMock()
        cursor.description = [("ID",), ("NAME",)]
        cursor.__iter__ = MagicMock(return_value=iter([(1, "Alice"), (2, "Bob")]))

        rows = OracleAdapter().execute_query(cursor, "SELECT 1", {})
        assert len(rows) == 2
        assert rows[0] == {"id": 1, "name": "Alice"}
        assert rows[1] == {"id": 2, "name": "Bob"}

    def test_lob_conversion(self):
        """LOBs com .read() são convertidos automaticamente."""
        lob = MagicMock()
        lob.read.return_value = "LOB content"

        cursor = MagicMock()
        cursor.description = [("DDL",)]
        cursor.__iter__ = MagicMock(return_value=iter([(lob,)]))

        rows = OracleAdapter().execute_query(cursor, "SELECT 1", {})
        assert rows[0]["ddl"] == "LOB content"
        lob.read.assert_called_once()

    def test_no_description_returns_empty(self):
        cursor = MagicMock()
        cursor.description = None

        rows = OracleAdapter().execute_query(cursor, "DELETE FROM x", {})
        assert rows == []


# ─── OracleAdapter.close_connection ─────────────────────────────────


class TestOracleAdapterCloseConnection:
    def test_calls_close(self):
        conn = MagicMock()
        OracleAdapter().close_connection(conn)
        conn.close.assert_called_once()


# ─── Thick mode functions ───────────────────────────────────────────


class TestThickMode:
    def test_already_initialized_noop(self):
        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = True
            with patch.object(oracledb, "init_oracle_client") as mock_init:
                _init_thick_mode_if_available()
                mock_init.assert_not_called()
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_success_sets_flag(self):
        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = False
            with patch.object(oracledb, "init_oracle_client"):
                _init_thick_mode_if_available()
                assert oracle_mod._thick_mode_initialized is True
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_programming_error_raises_runtime(self):
        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = False
            with (
                patch.object(
                    oracledb,
                    "init_oracle_client",
                    side_effect=oracledb.ProgrammingError("not found"),
                ),
                pytest.raises(RuntimeError, match="Oracle Instant Client"),
            ):
                _init_thick_mode_if_available()
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_check_already_initialized(self):
        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = True
            result = check_thick_mode_available()
            assert result["available"] == "True"
            assert "já" in result["detail"].lower()
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_check_client_found(self):
        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = False
            with patch.object(oracledb, "init_oracle_client"):
                result = check_thick_mode_available()
                assert result["available"] == "True"
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_check_client_not_found(self):
        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = False
            with patch.object(oracledb, "init_oracle_client", side_effect=Exception("not found")):
                result = check_thick_mode_available()
                assert result["available"] == "False"
        finally:
            oracle_mod._thick_mode_initialized = original


# ─── OracleQueryBuilder shim ────────────────────────────────────────


class TestOracleQueryBuilderSessionMethods:
    """Testes dos 3 novos métodos de sessão do OracleQueryBuilder."""

    def test_set_statistics_level_all(self):
        qb = OracleQueryBuilder()
        sql, params = qb.set_statistics_level("ALL")
        assert "STATISTICS_LEVEL" in sql
        assert "ALL" in sql
        assert params == {}

    def test_set_statistics_level_typical(self):
        qb = OracleQueryBuilder()
        sql, _params = qb.set_statistics_level("TYPICAL")
        assert "TYPICAL" in sql

    def test_set_statistics_level_invalid(self):
        qb = OracleQueryBuilder()
        with pytest.raises(ValueError, match="inválido"):
            qb.set_statistics_level("INVALID")

    def test_set_statistics_level_case_insensitive(self):
        qb = OracleQueryBuilder()
        sql, _params = qb.set_statistics_level("all")
        assert "ALL" in sql

    def test_session_sid(self):
        qb = OracleQueryBuilder()
        sql, params = qb.session_sid()
        assert "v$mystat" in sql
        assert params == {}

    def test_prev_sql_id(self):
        qb = OracleQueryBuilder()
        sql, params = qb.prev_sql_id()
        assert "prev_sql_id" in sql
        assert "v$session" in sql
        assert params == {}


class TestOracleQueryBuilderShim:
    def test_explain_plan_delegates(self):
        qb = OracleQueryBuilder()
        result = qb.explain_plan("SELECT 1 FROM DUAL")
        assert isinstance(result, list)
        assert len(result) == 3  # Oracle explain plan has 3 steps

    def test_runtime_plan_delegates(self):
        qb = OracleQueryBuilder()
        sql, _params = qb.runtime_plan("abc123def456")
        assert "DISPLAY_CURSOR" in sql

    def test_dangerous_privileges_delegates(self):
        qb = OracleQueryBuilder()
        sql, _params = qb.dangerous_privileges()
        assert "session_privs" in sql.lower()

    def test_batch_table_stats_delegates(self):
        qb = OracleQueryBuilder()
        sql, params = qb.batch_table_stats([("HR", "USERS")])
        assert "all_tables" in sql.lower()
        assert "o0" in params


# ─── OraclePlanParser stub ──────────────────────────────────────────


class TestOraclePlanParserStub:
    def test_parse_plan_raises(self):
        with pytest.raises(NotImplementedError, match="T5"):
            OraclePlanParser().parse_plan(["line"])

    def test_is_runtime_plan_raises(self):
        with pytest.raises(NotImplementedError, match="T5"):
            OraclePlanParser().is_runtime_plan(["line"])


# ─── Registration ───────────────────────────────────────────────────


class TestRegistration:
    def test_get_adapter_returns_oracle(self):
        result = get_adapter("oracle")
        assert result is OracleAdapter
