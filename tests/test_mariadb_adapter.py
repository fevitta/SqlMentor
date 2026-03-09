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

    def test_all_methods_return_valid_tuples(self):
        """Todos os métodos retornam tuple[str, dict] com SQL válido."""
        qb = MariaDBQueryBuilder()
        methods = [
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
        for method_name, args in methods:
            method = getattr(qb, method_name)
            result = method(*args)
            assert isinstance(result, tuple), f"{method_name} deve retornar tuple"
            assert len(result) == 2, f"{method_name} deve retornar (sql, params)"
            assert isinstance(result[0], str), f"{method_name}[0] deve ser str"
            assert isinstance(result[1], dict), f"{method_name}[1] deve ser dict"


# ─── MariaDBQueryBuilder — Session Methods ──────────────────────────


class TestMariaDBQueryBuilderSessionMethods:
    def test_session_sid_returns_connection_id(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.session_sid()
        assert "CONNECTION_ID()" in sql
        assert "sid" in sql.lower()
        assert params == {}

    def test_prev_sql_id_uses_performance_schema(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.prev_sql_id()
        assert "performance_schema" in sql
        assert "events_statements_history" in sql
        assert "DIGEST" in sql
        assert params == {}

    def test_optimizer_params_uses_global_variables(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.optimizer_params()
        assert "information_schema.GLOBAL_VARIABLES" in sql
        assert "VARIABLE_NAME AS name" in sql
        assert "VARIABLE_VALUE AS value" in sql
        assert "OPTIMIZER_SWITCH" in sql
        assert params == {}

    def test_set_statistics_level_is_nop(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.set_statistics_level("ALL")
        assert sql == "SELECT 1"
        assert params == {}

    def test_set_statistics_level_ignores_value(self):
        """O valor do level é ignorado — MariaDB não tem equivalente."""
        qb = MariaDBQueryBuilder()
        sql1, _ = qb.set_statistics_level("ALL")
        sql2, _ = qb.set_statistics_level("TYPICAL")
        assert sql1 == sql2


# ─── MariaDBQueryBuilder — Object Methods ───────────────────────────


class TestMariaDBQueryBuilderObjectMethods:
    def test_object_type_maps_base_table(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.object_type("mydb", "users")
        assert "information_schema.TABLES" in sql
        assert "'TABLE'" in sql
        assert "'VIEW'" in sql
        assert "object_type" in sql.lower()
        assert params["owner"] == "MYDB"
        assert params["object_name"] == "USERS"

    def test_table_ddl_uses_show_create(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.table_ddl("mydb", "users")
        assert sql.startswith("SHOW CREATE TABLE")
        assert "`mydb`.`users`" in sql
        assert params == {}

    def test_table_ddl_sanitizes_backticks(self):
        qb = MariaDBQueryBuilder()
        sql, _ = qb.table_ddl("my`db", "us`ers")
        assert "`mydb`.`users`" in sql
        assert "``" not in sql

    def test_function_ddl_uses_show_create(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.function_ddl("mydb", "calc_total")
        assert sql.startswith("SHOW CREATE FUNCTION")
        assert "`mydb`.`calc_total`" in sql
        assert params == {}

    def test_function_ddl_sanitizes_backticks(self):
        qb = MariaDBQueryBuilder()
        sql, _ = qb.function_ddl("my`db", "calc`fn")
        assert "`mydb`.`calcfn`" in sql

    def test_table_stats_column_aliases(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.table_stats("mydb", "orders")
        assert "information_schema.TABLES" in sql
        assert "table_name" in sql.lower()
        assert "num_rows" in sql.lower()
        assert "blocks" in sql.lower()
        assert "avg_row_len" in sql.lower()
        assert "last_analyzed" in sql.lower()
        assert "sample_size" in sql.lower()
        assert "partitioned" in sql.lower()
        assert "temporary" in sql.lower()
        assert "degree" in sql.lower()
        assert "compression" in sql.lower()
        assert params["owner"] == "MYDB"
        assert params["table_name"] == "ORDERS"

    def test_column_stats_column_aliases(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.column_stats("mydb", "orders")
        assert "information_schema.COLUMNS" in sql
        assert "column_name" in sql.lower()
        assert "data_type" in sql.lower()
        assert "data_length" in sql.lower()
        assert "nullable" in sql.lower()
        assert "histogram" in sql.lower()
        assert "data_default" in sql.lower()
        assert "ORDINAL_POSITION" in sql
        assert params["owner"] == "MYDB"

    def test_indexes_uses_statistics(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.indexes("mydb", "orders")
        assert "information_schema.STATISTICS" in sql
        assert "index_name" in sql.lower()
        assert "index_type" in sql.lower()
        assert "uniqueness" in sql.lower()
        assert "GROUP_CONCAT" in sql
        assert params["owner"] == "MYDB"

    def test_constraints_joins_three_tables(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.constraints("mydb", "orders")
        assert "TABLE_CONSTRAINTS" in sql
        assert "KEY_COLUMN_USAGE" in sql
        assert "REFERENTIAL_CONSTRAINTS" in sql
        assert "constraint_name" in sql.lower()
        assert "constraint_type" in sql.lower()
        assert "r_constraint_name" in sql.lower()
        assert "r_table_name" in sql.lower()
        assert "r_owner" in sql.lower()
        assert params["owner"] == "MYDB"

    def test_constraints_type_mapping(self):
        """Mapeia tipos MariaDB para letras Oracle: P, R, U, C."""
        qb = MariaDBQueryBuilder()
        sql, _ = qb.constraints("mydb", "orders")
        assert "'PRIMARY KEY' THEN 'P'" in sql
        assert "'FOREIGN KEY' THEN 'R'" in sql
        assert "'UNIQUE' THEN 'U'" in sql
        assert "'CHECK' THEN 'C'" in sql

    def test_histograms_uses_column_statistics(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.histograms("mydb", "orders", "price")
        assert "information_schema.COLUMN_STATISTICS" in sql
        assert params["owner"] == "MYDB"
        assert params["table_name"] == "ORDERS"
        assert params["column_name"] == "PRICE"

    def test_table_partitions_uses_partitions_table(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.table_partitions("mydb", "orders")
        assert "information_schema.PARTITIONS" in sql
        assert "partition_name" in sql.lower()
        assert "partition_position" in sql.lower()
        assert "high_value" in sql.lower()
        assert "PARTITION_NAME IS NOT NULL" in sql
        assert params["owner"] == "MYDB"

    def test_index_to_table_map(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.index_to_table_map("mydb")
        assert "information_schema.STATISTICS" in sql
        assert "index_name" in sql.lower()
        assert "table_name" in sql.lower()
        assert "DISTINCT" in sql
        assert params["owner"] == "MYDB"


# ─── MariaDBQueryBuilder — Runtime Methods ──────────────────────────


class TestMariaDBQueryBuilderRuntimeMethods:
    def test_sql_runtime_stats_column_aliases(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.sql_runtime_stats("abc123")
        assert "performance_schema.events_statements_summary_by_digest" in sql
        assert "sql_id" in sql.lower()
        assert "executions" in sql.lower()
        assert "elapsed_time" in sql.lower()
        assert "cpu_time" in sql.lower()
        assert "buffer_gets" in sql.lower()
        assert "rows_processed" in sql.lower()
        assert "avg_elapsed_ms" in sql.lower()
        assert "avg_cpu_ms" in sql.lower()
        assert "avg_buffer_gets" in sql.lower()
        assert "avg_rows_per_exec" in sql.lower()
        assert params["sql_id"] == "abc123"

    def test_sql_text_by_id(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.sql_text_by_id("abc123")
        assert "performance_schema" in sql
        assert "DIGEST_TEXT" in sql
        assert "sql_fulltext" in sql.lower()
        assert params["sql_id"] == "abc123"

    def test_session_wait_events(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.session_wait_events(42)
        assert "performance_schema.events_waits_summary_by_thread_by_event_name" in sql
        assert "event" in sql.lower()
        assert "total_waits" in sql.lower()
        assert "time_waited_micro" in sql.lower()
        assert "time_waited_ms" in sql.lower()
        assert "average_wait" in sql.lower()
        assert params["session_id"] == 42


# ─── MariaDBQueryBuilder — Batch Methods ────────────────────────────


class TestMariaDBQueryBuilderBatchMethods:
    def test_build_tuple_in_clause_basic(self):
        pairs = [("mydb", "users"), ("mydb", "orders")]
        clause, params = MariaDBQueryBuilder.build_tuple_in_clause(pairs)
        assert "TABLE_SCHEMA = %(o0)s AND TABLE_NAME = %(t0)s" in clause
        assert "TABLE_SCHEMA = %(o1)s AND TABLE_NAME = %(t1)s" in clause
        assert " OR " in clause
        assert params["o0"] == "MYDB"
        assert params["t0"] == "USERS"
        assert params["o1"] == "MYDB"
        assert params["t1"] == "ORDERS"

    def test_build_tuple_in_clause_single_pair(self):
        pairs = [("db", "tbl")]
        clause, params = MariaDBQueryBuilder.build_tuple_in_clause(pairs)
        assert " OR " not in clause
        assert len(params) == 2

    def test_build_tuple_in_clause_uppercases(self):
        pairs = [("myDB", "Users")]
        _, params = MariaDBQueryBuilder.build_tuple_in_clause(pairs)
        assert params["o0"] == "MYDB"
        assert params["t0"] == "USERS"

    def test_batch_table_stats_empty_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_table_stats([])
        assert "1=0" in sql
        assert params == {}

    def test_batch_table_stats_with_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_table_stats([("db", "t1"), ("db", "t2")])
        assert "information_schema.TABLES" in sql
        assert "TABLE_SCHEMA AS owner" in sql
        assert "table_name" in sql.lower()
        assert "num_rows" in sql.lower()
        assert len(params) == 4

    def test_batch_column_stats_empty_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_column_stats([])
        assert "1=0" in sql
        assert params == {}

    def test_batch_column_stats_with_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_column_stats([("db", "t1")])
        assert "information_schema.COLUMNS" in sql
        assert "TABLE_SCHEMA AS owner" in sql
        assert "column_name" in sql.lower()
        assert len(params) == 2

    def test_batch_indexes_empty_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_indexes([])
        assert "1=0" in sql
        assert params == {}

    def test_batch_indexes_with_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_indexes([("db", "t1")])
        assert "information_schema.STATISTICS" in sql
        assert "TABLE_SCHEMA AS owner" in sql
        assert "GROUP_CONCAT" in sql
        assert len(params) == 2

    def test_batch_constraints_empty_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_constraints([])
        assert "1=0" in sql
        assert params == {}

    def test_batch_constraints_with_pairs(self):
        qb = MariaDBQueryBuilder()
        sql, params = qb.batch_constraints([("db", "t1")])
        assert "TABLE_CONSTRAINTS" in sql
        assert "tc.TABLE_SCHEMA AS owner" in sql
        assert "constraint_type" in sql.lower()
        assert len(params) == 2


# ─── MariaDBQueryBuilder — No Oracle-style binds ────────────────────


_ALL_QUERY_METHODS = [
    ("db_version", []),
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
    ("dangerous_privileges", []),
    ("dangerous_roles", []),
    ("batch_table_stats", [[("db", "tbl")]]),
    ("batch_column_stats", [[("db", "tbl")]]),
    ("batch_indexes", [[("db", "tbl")]]),
    ("batch_constraints", [[("db", "tbl")]]),
]


class TestMariaDBQueryBuilderNoOracleBinds:
    """Verifica que nenhuma query usa :param (estilo Oracle) em vez de %(param)s."""

    @pytest.mark.parametrize(("method_name", "args"), _ALL_QUERY_METHODS)
    def test_no_oracle_style_binds(self, method_name, args):
        """Nenhuma query deve usar :param (Oracle bind) — deve usar %(param)s (pyformat)."""
        import re

        qb = MariaDBQueryBuilder()
        method = getattr(qb, method_name)
        result = method(*args)
        sql = result[0] if isinstance(result, tuple) else result[0][0]
        # Padrão: `:word` que não é parte de `::` (cast) ou string literal
        oracle_binds = re.findall(r"(?<!:)(?<!%):\w+", sql)
        assert oracle_binds == [], f"{method_name} usa Oracle-style bind(s): {oracle_binds}"


# ─── MariaDBQueryBuilder — Identifier Sanitization ──────────────────


class TestMariaDBQueryBuilderSanitization:
    def test_sanitize_identifier_strips_backticks(self):
        assert MariaDBQueryBuilder._sanitize_identifier("my`db") == "mydb"
        assert MariaDBQueryBuilder._sanitize_identifier("```") == ""
        assert MariaDBQueryBuilder._sanitize_identifier("clean") == "clean"

    def test_table_ddl_injection_attempt(self):
        """Backticks no input não escapam o identificador."""
        qb = MariaDBQueryBuilder()
        sql, _ = qb.table_ddl("db`.`other_db", "tbl`.`other_tbl")
        assert "db`.`other_db" not in sql
        assert "`db.other_db`.`tbl.other_tbl`" in sql

    def test_function_ddl_injection_attempt(self):
        qb = MariaDBQueryBuilder()
        sql, _ = qb.function_ddl("db`.`x", "fn`.`y")
        assert "db`.`x" not in sql


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
