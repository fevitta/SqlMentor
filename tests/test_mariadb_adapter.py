"""Testes unitários para MariaDBAdapter, MariaDBQueryBuilder e MariaDBPlanParser."""

from unittest.mock import MagicMock, patch

import pytest

pymysql = pytest.importorskip("pymysql", reason="PyMySQL não instalado")

from sqlmentor.adapters import get_adapter
from sqlmentor.adapters.base import DatabaseAdapter, PlanParser, QueryBuilder
from sqlmentor.adapters.mariadb import MariaDBAdapter, MariaDBPlanParser, MariaDBQueryBuilder

# ─── MariaDBAdapter properties ──────────────────────────────────────


class TestMariaDBAdapterProperties:
    def test_db_type(self):
        adapter = MariaDBAdapter()
        assert adapter.db_type == "mariadb"

    def test_query_builder_type(self):
        adapter = MariaDBAdapter()
        assert isinstance(adapter.query_builder, MariaDBQueryBuilder)
        assert isinstance(adapter.query_builder, QueryBuilder)

    def test_plan_parser_type(self):
        adapter = MariaDBAdapter()
        assert isinstance(adapter.plan_parser, MariaDBPlanParser)
        assert isinstance(adapter.plan_parser, PlanParser)

    def test_query_builder_cached(self):
        adapter = MariaDBAdapter()
        qb1 = adapter.query_builder
        qb2 = adapter.query_builder
        assert qb1 is qb2

    def test_plan_parser_cached(self):
        adapter = MariaDBAdapter()
        pp1 = adapter.plan_parser
        pp2 = adapter.plan_parser
        assert pp1 is pp2

    def test_is_database_adapter(self):
        assert issubclass(MariaDBAdapter, DatabaseAdapter)


# ─── MariaDBAdapter.connect ─────────────────────────────────────────


class TestMariaDBAdapterConnect:
    @pytest.fixture
    def config(self):
        return {
            "host": "localhost",
            "port": 3306,
            "service": "mydb",
            "user": "root",
            "password": "secret",
            "timeout": 60,
        }

    def test_success(self, config):
        mock_conn = MagicMock()
        with patch.object(pymysql, "connect", return_value=mock_conn):
            adapter = MariaDBAdapter()
            result = adapter.connect(config)
            assert result is mock_conn
            pymysql.connect.assert_called_once_with(
                host="localhost",
                port=3306,
                user="root",
                password="secret",
                database="mydb",
                connect_timeout=60,
                charset="utf8mb4",
            )

    def test_failure_propagates(self, config):
        with (
            patch.object(pymysql, "connect", side_effect=pymysql.OperationalError("conn failed")),
            pytest.raises(pymysql.OperationalError, match="conn failed"),
        ):
            MariaDBAdapter().connect(config)

    def test_explicit_timeout_overrides_config(self, config):
        mock_conn = MagicMock()
        with patch.object(pymysql, "connect", return_value=mock_conn):
            MariaDBAdapter().connect(config, timeout=300)
            pymysql.connect.assert_called_once_with(
                host="localhost",
                port=3306,
                user="root",
                password="secret",
                database="mydb",
                connect_timeout=300,
                charset="utf8mb4",
            )

    def test_timeout_from_config(self, config):
        mock_conn = MagicMock()
        with patch.object(pymysql, "connect", return_value=mock_conn):
            MariaDBAdapter().connect(config, timeout=None)
            pymysql.connect.assert_called_once_with(
                host="localhost",
                port=3306,
                user="root",
                password="secret",
                database="mydb",
                connect_timeout=60,
                charset="utf8mb4",
            )


# ─── MariaDBAdapter.test_connection ─────────────────────────────────


class TestMariaDBAdapterTestConnection:
    def test_success_returns_true(self):
        config = {
            "host": "localhost",
            "port": 3306,
            "service": "mydb",
            "user": "root",
            "password": "secret",
        }
        mock_conn = MagicMock()
        with patch.object(pymysql, "connect", return_value=mock_conn):
            assert MariaDBAdapter().test_connection(config) is True
            mock_conn.close.assert_called_once()

    def test_failure_returns_false(self):
        config = {
            "host": "bad",
            "port": 3306,
            "service": "x",
            "user": "x",
            "password": "x",
        }
        with patch.object(pymysql, "connect", side_effect=Exception("conn failed")):
            assert MariaDBAdapter().test_connection(config) is False


# ─── MariaDBAdapter.validate_privileges ─────────────────────────────


class TestMariaDBAdapterValidatePrivileges:
    def test_clean_user_returns_empty(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.__iter__ = MagicMock(side_effect=[iter([]), iter([])])

        result = MariaDBAdapter().validate_privileges(conn)
        assert result == {"dangerous_privileges": [], "dangerous_roles": []}

    def test_dangerous_privs_populated(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        call_count = [0]

        def fake_iter():
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([("INSERT",)])
            return iter([])

        cursor.__iter__ = MagicMock(side_effect=fake_iter)

        result = MariaDBAdapter().validate_privileges(conn)
        assert result["dangerous_privileges"] == ["INSERT"]
        assert result["dangerous_roles"] == []

    def test_cursor_always_closed(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            MariaDBAdapter().validate_privileges(conn)

        cursor.close.assert_called_once()


# ─── MariaDBAdapter.diagnose_connection ─────────────────────────────


class TestMariaDBAdapterDiagnoseConnection:
    @pytest.fixture
    def config(self):
        return {
            "host": "localhost",
            "port": 3306,
            "service": "mydb",
            "user": "root",
            "password": "secret",
        }

    def test_parses_version(self, config):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [
            ("10.6.12-MariaDB",),
            (1,),
        ]
        with patch.object(pymysql, "connect", return_value=mock_conn):
            result = MariaDBAdapter().diagnose_connection(config)
            assert result["status"] == "ok"
            assert result["version"] == "10.6.12-MariaDB"
            assert result["major_version"] == "10"
            assert result["performance_schema"] == "True"
            mock_conn.close.assert_called_once()

    def test_perf_schema_disabled(self, config):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [
            ("11.4.0-MariaDB",),
            (0,),
        ]
        with patch.object(pymysql, "connect", return_value=mock_conn):
            result = MariaDBAdapter().diagnose_connection(config)
            assert result["performance_schema"] == "False"
            assert result["major_version"] == "11"


# ─── MariaDBAdapter.execute_query ───────────────────────────────────


class TestMariaDBAdapterExecuteQuery:
    def test_basic_execution(self):
        cursor = MagicMock()
        cursor.description = [("ID",), ("NAME",)]
        cursor.__iter__ = MagicMock(return_value=iter([(1, "Alice"), (2, "Bob")]))

        rows = MariaDBAdapter().execute_query(cursor, "SELECT 1", {})
        assert len(rows) == 2
        assert rows[0] == {"id": 1, "name": "Alice"}
        assert rows[1] == {"id": 2, "name": "Bob"}

    def test_no_description_returns_empty(self):
        cursor = MagicMock()
        cursor.description = None

        rows = MariaDBAdapter().execute_query(cursor, "DELETE FROM x", {})
        assert rows == []


# ─── MariaDBAdapter.close_connection ────────────────────────────────


class TestMariaDBAdapterCloseConnection:
    def test_calls_close(self):
        conn = MagicMock()
        MariaDBAdapter().close_connection(conn)
        conn.close.assert_called_once()


# ─── MariaDBAdapter.check_deps ──────────────────────────────────────


class TestMariaDBAdapterCheckDeps:
    def test_pymysql_installed(self):
        adapter = MariaDBAdapter()
        with patch("importlib.metadata.version", return_value="1.1.0"):
            results = adapter.check_deps()
        assert len(results) == 1
        assert results[0]["status"] == "ok"
        assert results[0]["name"] == "PyMySQL"
        assert results[0]["detail"] == "1.1.0"

    def test_pymysql_missing(self):
        adapter = MariaDBAdapter()
        with patch("importlib.metadata.version", side_effect=Exception("not found")):
            results = adapter.check_deps()
        assert len(results) == 1
        assert results[0]["status"] == "missing"
        assert results[0]["name"] == "PyMySQL"
        assert "sqlmentor[mariadb]" in results[0]["detail"]


# ─── MariaDBQueryBuilder ────────────────────────────────────────────


class TestMariaDBQueryBuilder:
    def test_db_version(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.db_version()
        assert "VERSION()" in sql
        assert params == {}

    def test_dangerous_privileges(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.dangerous_privileges()
        assert "information_schema" in sql.lower()
        assert "USER_PRIVILEGES" in sql
        assert params == {}

    def test_dangerous_roles_stub(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.dangerous_roles()
        assert "1=0" in sql
        assert params == {}

    def test_explain_plan_stub(self):
        qb = MariaDBQueryBuilder()
        result = qb.explain_plan("SELECT 1")
        assert isinstance(result, list)
        assert len(result) == 1
        assert "1=0" in result[0][0]

    def test_runtime_plan_stub(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.runtime_plan("abc123")
        assert "1=0" in sql
        assert params == {}

    def test_all_stubs_return_valid_tuples(self):
        """Todos os stubs retornam tuple[str, dict] com SQL válido."""
        qb = MariaDBQueryBuilder()
        stub_methods = [
            ("optimizer_params", []),
            ("set_statistics_level", ["ALL"]),
            ("session_sid", []),
            ("prev_sql_id", []),
            ("session_wait_events", [1]),
            ("object_type", ["db", "tbl"]),
            ("table_ddl", ["db", "tbl"]),
            ("function_ddl", ["db", "fn"]),
            ("table_stats", ["db", "tbl"]),
            ("column_stats", ["db", "tbl"]),
            ("indexes", ["db", "tbl"]),
            ("constraints", ["db", "tbl"]),
            ("histograms", ["db", "tbl", "col"]),
            ("table_partitions", ["db", "tbl"]),
            ("index_to_table_map", ["db"]),
            ("sql_runtime_stats", ["abc"]),
            ("sql_text_by_id", ["abc"]),
            ("batch_table_stats", [[("db", "tbl")]]),
            ("batch_column_stats", [[("db", "tbl")]]),
            ("batch_indexes", [[("db", "tbl")]]),
            ("batch_constraints", [[("db", "tbl")]]),
        ]
        for method_name, args in stub_methods:
            method = getattr(qb, method_name)
            result = method(*args)
            assert isinstance(result, tuple), f"{method_name} deve retornar tuple"
            assert len(result) == 2, f"{method_name} deve retornar (sql, params)"
            assert isinstance(result[0], str), f"{method_name}[0] deve ser str"
            assert isinstance(result[1], dict), f"{method_name}[1] deve ser dict"


# ─── MariaDBPlanParser ──────────────────────────────────────────────


class TestMariaDBPlanParser:
    def test_parse_plan_returns_empty(self):
        parser = MariaDBPlanParser()
        assert parser.parse_plan([]) == []
        assert parser.parse_plan(["some line"]) == []

    def test_is_runtime_plan_returns_false(self):
        parser = MariaDBPlanParser()
        assert parser.is_runtime_plan([]) is False
        assert parser.is_runtime_plan(["some line"]) is False


# ─── Registration ───────────────────────────────────────────────────


class TestRegistration:
    def test_get_adapter_returns_mariadb(self):
        result = get_adapter("mariadb")
        assert result is MariaDBAdapter
