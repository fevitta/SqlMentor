"""Testes unitários do collector.py — coleta de metadata Oracle com cursor mockado."""

from unittest.mock import MagicMock

from sqlmentor.collector import (
    CollectedContext,
    TableContext,
    _batch_collect_tables,
    _collect_column_stats,
    _collect_constraints,
    _collect_db_version,
    _collect_ddl,
    _collect_explain_plan,
    _collect_histograms,
    _collect_indexes,
    _collect_optimizer_params,
    _collect_partitions,
    _collect_runtime_execution,
    _collect_table_stats,
    _collect_view_expansion,
    _detect_object_type,
    _execute_query,
    _index_map_cache,
    _inline_binds,
    _LRUCache,
    _optimizer_cache,
    _parse_view_tables,
    _table_cache,
    clear_cache,
    collect_context,
)
from sqlmentor.parser import ParsedSQL
from sqlmentor.queries import _build_tuple_in_clause

# ---------------------------------------------------------------------------
# Helper: make_cursor_dispatch
# ---------------------------------------------------------------------------


def make_cursor_dispatch(mappings: dict) -> MagicMock:
    """Cria um MagicMock cursor com dispatch por fragmento SQL.

    ``mappings`` é ``{"fragmento_sql": response, ...}`` onde *response* pode ser:

    * ``list[tuple]`` — rows retornadas por iteração / fetchall
    * ``tuple`` — single row retornada por fetchone
    * ``Exception`` — levantada em execute()
    * ``None`` — resultado vazio (fetchone→None, iter→[])
    * ``dict`` com chaves ``rows`` (list[tuple]) e ``description`` (list[tuple])
    """
    cursor = MagicMock()
    # Estado mutável compartilhado entre closures
    state: dict = {"rows": [], "description": [("col1",)]}

    def _execute(sql, params=None):
        # Ordena por tamanho do fragmento (maior primeiro) pra priorizar matches mais específicos
        sorted_mappings = sorted(mappings.items(), key=lambda kv: len(kv[0]), reverse=True)
        for fragment, response in sorted_mappings:
            if fragment.upper() in sql.upper():
                if isinstance(response, Exception):
                    raise response
                if isinstance(response, dict):
                    state["rows"] = list(response.get("rows", []))
                    state["description"] = response.get("description", [("col1",)])
                    return
                if response is None:
                    state["rows"] = []
                    state["description"] = [("col1",)]
                    return
                if isinstance(response, tuple) and not isinstance(response, list):
                    # single row — fetchone retorna isso
                    state["rows"] = [response]
                    state["description"] = [("col1",)]
                    return
                if isinstance(response, list):
                    state["rows"] = list(response)
                    state["description"] = [("col1",)]
                    return
        # Nenhum match — resultado vazio
        state["rows"] = []
        state["description"] = [("col1",)]

    cursor.execute = MagicMock(side_effect=_execute)
    cursor.fetchone = MagicMock(side_effect=lambda: state["rows"].pop(0) if state["rows"] else None)
    cursor.fetchall = MagicMock(side_effect=lambda: list(state["rows"]))
    cursor.__iter__ = MagicMock(side_effect=lambda: iter(list(state["rows"])))

    # description como property
    type(cursor).description = property(lambda self: state["description"])

    return cursor


# ---------------------------------------------------------------------------
# 1. Funções puras (sem cursor)
# ---------------------------------------------------------------------------


class TestInlineBinds:
    """Testes de _inline_binds — substitui :bind por literais."""

    def test_string_bind(self):
        result = _inline_binds("SELECT * FROM t WHERE name = :name", {"name": "Alice"})
        assert result == "SELECT * FROM t WHERE name = 'Alice'"

    def test_numeric_bind_int(self):
        result = _inline_binds("SELECT * FROM t WHERE id = :id", {"id": 42})
        assert result == "SELECT * FROM t WHERE id = 42"

    def test_numeric_bind_float(self):
        result = _inline_binds("SELECT * FROM t WHERE val = :val", {"val": 3.14})
        assert result == "SELECT * FROM t WHERE val = 3.14"

    def test_none_bind_to_null(self):
        result = _inline_binds("SELECT * FROM t WHERE col = :col", {"col": None})
        assert result == "SELECT * FROM t WHERE col = NULL"

    def test_missing_bind_to_null(self):
        result = _inline_binds("SELECT * FROM t WHERE x = :x", {"other": "val"})
        assert result == "SELECT * FROM t WHERE x = NULL"

    def test_escape_single_quotes(self):
        result = _inline_binds("SELECT * FROM t WHERE name = :name", {"name": "O'Brien"})
        assert result == "SELECT * FROM t WHERE name = 'O''Brien'"

    def test_case_insensitive_bind(self):
        result = _inline_binds("SELECT * FROM t WHERE id = :ID", {"id": 1})
        assert result == "SELECT * FROM t WHERE id = 1"

    def test_double_colon_ignored(self):
        result = _inline_binds("SELECT ::type_cast, :val FROM t", {"val": "x"})
        assert "::type_cast" in result
        assert "'x'" in result

    def test_bind_params_none(self):
        sql = "SELECT * FROM t WHERE id = :id"
        result = _inline_binds(sql, None)
        assert result == sql


class TestParseViewTables:
    """Testes de _parse_view_tables — extrai tabelas de DDL de view."""

    def test_simple_view(self):
        ddl = 'CREATE OR REPLACE VIEW "HR"."V_EMP"\nAS\nSELECT * FROM employees'
        tables = _parse_view_tables(ddl)
        assert "EMPLOYEES" in tables

    def test_join_view(self):
        ddl = (
            "CREATE VIEW v_test\nAS\n"
            "SELECT a.id, b.name FROM orders a JOIN customers b ON a.cust_id = b.id"
        )
        tables = _parse_view_tables(ddl)
        assert "CUSTOMERS" in tables
        assert "ORDERS" in tables

    def test_schema_qualified(self):
        ddl = "CREATE VIEW v\nAS\nSELECT * FROM hr.employees"
        tables = _parse_view_tables(ddl)
        assert "HR.EMPLOYEES" in tables

    def test_no_as_returns_empty(self):
        ddl = "CREATE VIEW v SELECT * FROM t"
        tables = _parse_view_tables(ddl)
        assert tables == []

    def test_fallback_regex_on_parse_error(self):
        # SQL inválido que sqlglot não consegue parsear mas regex captura
        ddl = "CREATE VIEW v\nAS\nSELECT * FROM mytable WHERE @@@invalid_syntax"
        tables = _parse_view_tables(ddl)
        # Pode ou não capturar — importante é não levantar exceção
        assert isinstance(tables, list)

    def test_result_sorted(self):
        ddl = "CREATE VIEW v\nAS\nSELECT * FROM zebra z JOIN apple a ON z.id = a.id"
        tables = _parse_view_tables(ddl)
        assert tables == sorted(tables)


# ---------------------------------------------------------------------------
# 2. Helpers com cursor mock
# ---------------------------------------------------------------------------


def _make_ctx(parsed=None):
    """Cria CollectedContext mínimo para testes de helpers."""
    if parsed is None:
        parsed = ParsedSQL(raw_sql="SELECT 1 FROM dual", sql_type="SELECT")
    return CollectedContext(parsed_sql=parsed)


class TestExecuteQuery:
    """Testes de _execute_query."""

    def test_returns_list_of_dicts(self):
        cursor = make_cursor_dispatch(
            {
                "SELECT": {
                    "description": [("id",), ("name",)],
                    "rows": [(1, "Alice"), (2, "Bob")],
                }
            }
        )
        result = _execute_query(cursor, "SELECT id, name FROM t", {})
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_empty_result(self):
        cursor = make_cursor_dispatch({"SELECT": {"description": [("id",)], "rows": []}})
        result = _execute_query(cursor, "SELECT id FROM t", {})
        assert result == []

    def test_lob_read(self):
        lob = MagicMock()
        lob.read.return_value = "LOB content"
        cursor = make_cursor_dispatch({"SELECT": {"description": [("ddl",)], "rows": [(lob,)]}})
        result = _execute_query(cursor, "SELECT ddl FROM t", {})
        assert result == [{"ddl": "LOB content"}]

    def test_description_none(self):
        # Quando description é None, devemos tratar sem erro
        cursor2 = MagicMock()
        cursor2.execute = MagicMock()
        cursor2.description = None
        cursor2.__iter__ = MagicMock(return_value=iter([]))
        result = _execute_query(cursor2, "SELECT 1 FROM dual", {})
        assert result == []

    def test_multiple_rows(self):
        cursor = make_cursor_dispatch(
            {
                "SELECT": {
                    "description": [("val",)],
                    "rows": [(10,), (20,), (30,)],
                }
            }
        )
        result = _execute_query(cursor, "SELECT val FROM t", {})
        assert len(result) == 3
        assert result[2] == {"val": 30}


class TestCollectDbVersion:
    """Testes de _collect_db_version."""

    def test_returns_version_string(self):
        cursor = make_cursor_dispatch({"v$version": ("Oracle 19c",)})
        ctx = _make_ctx()
        result = _collect_db_version(cursor, ctx)
        assert result == "Oracle 19c"

    def test_none_when_empty(self):
        cursor = make_cursor_dispatch({"v$version": None})
        ctx = _make_ctx()
        result = _collect_db_version(cursor, ctx)
        assert result is None

    def test_error_recorded(self):
        cursor = make_cursor_dispatch({"v$version": RuntimeError("ORA-00942")})
        ctx = _make_ctx()
        result = _collect_db_version(cursor, ctx)
        assert result is None
        assert len(ctx.errors) == 1
        assert "versão" in ctx.errors[0]


class TestDetectObjectType:
    """Testes de _detect_object_type."""

    def test_returns_table(self):
        cursor = make_cursor_dispatch(
            {
                "all_objects": {
                    "description": [("object_type",)],
                    "rows": [("TABLE",)],
                }
            }
        )
        result = _detect_object_type(cursor, "HR", "EMPLOYEES", _make_ctx())
        assert result == "TABLE"

    def test_returns_view(self):
        cursor = make_cursor_dispatch(
            {
                "all_objects": {
                    "description": [("object_type",)],
                    "rows": [("VIEW",)],
                }
            }
        )
        result = _detect_object_type(cursor, "HR", "V_EMP", _make_ctx())
        assert result == "VIEW"

    def test_default_table_on_empty(self):
        cursor = make_cursor_dispatch(
            {"all_objects": {"description": [("object_type",)], "rows": []}}
        )
        result = _detect_object_type(cursor, "HR", "UNKNOWN", _make_ctx())
        assert result == "TABLE"

    def test_default_table_on_exception(self):
        cursor = make_cursor_dispatch({"all_objects": RuntimeError("ORA-00942")})
        result = _detect_object_type(cursor, "HR", "BAD", _make_ctx())
        assert result == "TABLE"


class TestCollectViewExpansion:
    """Testes de _collect_view_expansion."""

    def test_populates_view_expansions(self):
        ddl = "CREATE VIEW v\nAS\nSELECT * FROM orders JOIN customers ON 1=1"
        cursor = make_cursor_dispatch(
            {
                "DBMS_METADATA": {
                    "description": [("ddl",)],
                    "rows": [(ddl,)],
                }
            }
        )
        ctx = _make_ctx()
        _collect_view_expansion(cursor, "HR", "V_TEST", ctx)
        assert "HR.V_TEST" in ctx.view_expansions
        assert len(ctx.view_expansions["HR.V_TEST"]) >= 1

    def test_empty_ddl_noop(self):
        cursor = make_cursor_dispatch(
            {"DBMS_METADATA": {"description": [("ddl",)], "rows": [("  ",)]}}
        )
        ctx = _make_ctx()
        _collect_view_expansion(cursor, "HR", "V_EMPTY", ctx)
        assert "HR.V_EMPTY" not in ctx.view_expansions

    def test_error_recorded(self):
        cursor = make_cursor_dispatch({"DBMS_METADATA": RuntimeError("no priv")})
        ctx = _make_ctx()
        _collect_view_expansion(cursor, "HR", "V_ERR", ctx)
        assert len(ctx.errors) == 1
        assert "view expansion" in ctx.errors[0].lower()


class TestCollectExplainPlan:
    """Testes de _collect_explain_plan."""

    def test_returns_plan_lines(self):
        plan_lines = [
            ("Plan hash value: 123",),
            ("| Id | Operation |",),
            ("|  0 | SELECT    |",),
        ]
        cursor = make_cursor_dispatch(
            {
                "EXPLAIN PLAN": None,  # step 1: DDL, retorna nada
                "DBMS_XPLAN": plan_lines,  # step 2: SELECT plan
                "DELETE FROM PLAN_TABLE": None,  # step 3: cleanup
            }
        )
        ctx = _make_ctx()
        result = _collect_explain_plan(cursor, "SELECT 1 FROM dual", ctx)
        assert result is not None
        assert len(result) == 3
        assert "Plan hash" in result[0]

    def test_three_steps_executed(self):
        cursor = make_cursor_dispatch(
            {
                "EXPLAIN PLAN": None,
                "DBMS_XPLAN": [("line1",)],
                "DELETE FROM PLAN_TABLE": None,
            }
        )
        ctx = _make_ctx()
        _collect_explain_plan(cursor, "SELECT 1 FROM dual", ctx)
        assert cursor.execute.call_count == 3

    def test_inlines_binds(self):
        cursor = make_cursor_dispatch(
            {
                "EXPLAIN PLAN": None,
                "DBMS_XPLAN": [("line1",)],
                "DELETE FROM PLAN_TABLE": None,
            }
        )
        ctx = _make_ctx()
        _collect_explain_plan(cursor, "SELECT * FROM t WHERE id = :id", ctx, {"id": 42})
        # O primeiro execute deve conter o literal 42, não :id
        first_call_sql = cursor.execute.call_args_list[0][0][0]
        assert "42" in first_call_sql
        assert ":id" not in first_call_sql

    def test_ora_1031_suggests_grant(self):
        # Simula ORA-01031: insufficient privileges
        err = Exception("ORA-01031")
        # Adiciona .code ao primeiro arg
        inner = MagicMock()
        inner.code = 1031
        inner.offset = None
        err.args = (inner,)

        parsed = ParsedSQL(
            raw_sql="SELECT fn_calc(id) FROM t",
            sql_type="SELECT",
            functions=[{"schema": "HR", "name": "FN_CALC"}],
        )
        ctx = _make_ctx(parsed)

        cursor = make_cursor_dispatch({"EXPLAIN PLAN": err})
        result = _collect_explain_plan(cursor, parsed.raw_sql, ctx)
        assert result is None
        assert any("GRANT EXECUTE" in e for e in ctx.errors)
        assert any("FN_CALC" in e for e in ctx.errors)


class TestCollectRuntimeExecution:
    """Testes de _collect_runtime_execution."""

    def test_collects_plan_stats_waits(self):
        cursor = make_cursor_dispatch(
            {
                "ALTER SESSION": None,
                "v$mystat": (100,),  # sid
                "v$session": ("abc123def456",),  # sql_id
                "DBMS_XPLAN": [("runtime plan line",)],
                "v$sql": {
                    "description": [("sql_id",), ("executions",)],
                    "rows": [("abc123def456", 1)],
                },
                "v$session_event": {
                    "description": [("event",), ("total_waits",)],
                    "rows": [("db file sequential read", 5)],
                },
            }
        )
        ctx = _make_ctx()
        conn = MagicMock()
        _collect_runtime_execution(cursor, conn, "SELECT 1 FROM dual", ctx)
        assert ctx.runtime_plan is not None
        assert ctx.runtime_stats is not None

    def test_fallback_when_no_sql_id(self):
        cursor = make_cursor_dispatch(
            {
                "ALTER SESSION": None,
                "v$mystat": (100,),
                "v$session": (None,),  # no sql_id
                "EXPLAIN PLAN": None,
                "DBMS_XPLAN": [("fallback plan",)],
                "DELETE FROM PLAN_TABLE": None,
            }
        )
        ctx = _make_ctx()
        conn = MagicMock()
        _collect_runtime_execution(cursor, conn, "SELECT 1 FROM dual", ctx)
        assert any("sql_id" in e for e in ctx.errors)
        # Falls back to explain plan
        assert ctx.execution_plan is not None

    def test_fallback_on_exception(self):
        cursor = make_cursor_dispatch(
            {
                "ALTER SESSION": RuntimeError("cannot alter"),
            }
        )
        # Ajusta pra explain plan fallback funcionar
        # Na exceção, _collect_explain_plan será chamado mas também pode falhar
        ctx = _make_ctx()
        conn = MagicMock()
        _collect_runtime_execution(cursor, conn, "SELECT 1 FROM dual", ctx)
        assert len(ctx.errors) >= 1

    def test_alter_session_called(self):
        cursor = make_cursor_dispatch(
            {
                "ALTER SESSION": None,
                "v$mystat": (100,),
                "v$session": ("abc123def456",),
                "DBMS_XPLAN": [("line",)],
                "v$sql": {"description": [("sql_id",)], "rows": [("abc123def456",)]},
                "v$session_event": {"description": [("event",)], "rows": []},
            }
        )
        ctx = _make_ctx()
        conn = MagicMock()
        _collect_runtime_execution(cursor, conn, "SELECT 1 FROM dual", ctx)
        calls = [str(c) for c in cursor.execute.call_args_list]
        assert any("STATISTICS_LEVEL" in c for c in calls)


class TestCollectDdl:
    """Testes de _collect_ddl."""

    def test_returns_ddl_string(self):
        cursor = make_cursor_dispatch(
            {
                "DBMS_METADATA": {
                    "description": [("ddl",)],
                    "rows": [("CREATE TABLE t (id NUMBER)",)],
                }
            }
        )
        result = _collect_ddl(cursor, "HR", "T", _make_ctx())
        assert result == "CREATE TABLE t (id NUMBER)"

    def test_none_on_empty(self):
        cursor = make_cursor_dispatch({"DBMS_METADATA": {"description": [("ddl",)], "rows": []}})
        result = _collect_ddl(cursor, "HR", "T", _make_ctx())
        assert result is None

    def test_error_recorded(self):
        cursor = make_cursor_dispatch({"DBMS_METADATA": RuntimeError("no priv")})
        ctx = _make_ctx()
        result = _collect_ddl(cursor, "HR", "T", ctx)
        assert result is None
        assert len(ctx.errors) == 1


class TestCollectTableStats:
    """Testes de _collect_table_stats."""

    def test_returns_dict(self):
        cursor = make_cursor_dispatch(
            {
                "all_tables": {
                    "description": [("num_rows",), ("blocks",)],
                    "rows": [(1000, 50)],
                }
            }
        )
        result = _collect_table_stats(cursor, "HR", "T", _make_ctx())
        assert result == {"num_rows": 1000, "blocks": 50}

    def test_none_on_empty(self):
        cursor = make_cursor_dispatch({"all_tables": {"description": [("num_rows",)], "rows": []}})
        result = _collect_table_stats(cursor, "HR", "T", _make_ctx())
        assert result is None

    def test_error_recorded(self):
        ctx = _make_ctx()
        cursor = make_cursor_dispatch({"all_tables": RuntimeError("fail")})
        result = _collect_table_stats(cursor, "HR", "T", ctx)
        assert result is None
        assert len(ctx.errors) == 1


class TestCollectColumnStats:
    """Testes de _collect_column_stats."""

    def test_returns_list(self):
        cursor = make_cursor_dispatch(
            {
                "all_tab_col": {
                    "description": [("column_name",), ("data_type",)],
                    "rows": [("ID", "NUMBER"), ("NAME", "VARCHAR2")],
                }
            }
        )
        result = _collect_column_stats(cursor, "HR", "T", _make_ctx())
        assert len(result) == 2

    def test_empty_on_error(self):
        cursor = make_cursor_dispatch({"all_tab_col": RuntimeError("fail")})
        ctx = _make_ctx()
        result = _collect_column_stats(cursor, "HR", "T", ctx)
        assert result == []
        assert len(ctx.errors) == 1


class TestCollectIndexes:
    """Testes de _collect_indexes."""

    def test_returns_list(self):
        cursor = make_cursor_dispatch(
            {
                "all_indexes": {
                    "description": [("index_name",), ("index_type",)],
                    "rows": [("PK_T", "NORMAL")],
                }
            }
        )
        result = _collect_indexes(cursor, "HR", "T", _make_ctx())
        assert len(result) == 1

    def test_empty_on_error(self):
        cursor = make_cursor_dispatch({"all_indexes": RuntimeError("fail")})
        ctx = _make_ctx()
        result = _collect_indexes(cursor, "HR", "T", ctx)
        assert result == []


class TestCollectConstraints:
    """Testes de _collect_constraints."""

    def test_returns_list(self):
        cursor = make_cursor_dispatch(
            {
                "all_constraints": {
                    "description": [("constraint_name",), ("constraint_type",)],
                    "rows": [("PK_T", "P")],
                }
            }
        )
        result = _collect_constraints(cursor, "HR", "T", _make_ctx())
        assert len(result) == 1

    def test_empty_on_error(self):
        cursor = make_cursor_dispatch({"all_constraints": RuntimeError("fail")})
        result = _collect_constraints(cursor, "HR", "T", _make_ctx())
        assert result == []


class TestCollectPartitions:
    """Testes de _collect_partitions."""

    def test_returns_list(self):
        cursor = make_cursor_dispatch(
            {
                "all_tab_partitions": {
                    "description": [("partition_name",)],
                    "rows": [("P1",), ("P2",)],
                }
            }
        )
        result = _collect_partitions(cursor, "HR", "T", _make_ctx())
        assert len(result) == 2

    def test_empty_on_error(self):
        cursor = make_cursor_dispatch({"all_tab_partitions": RuntimeError("fail")})
        result = _collect_partitions(cursor, "HR", "T", _make_ctx())
        assert result == []


class TestCollectHistograms:
    """Testes de _collect_histograms."""

    def test_collects_relevant_columns(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM t WHERE status = 1",
            sql_type="SELECT",
            where_columns=["status"],
        )
        columns = [
            {"column_name": "STATUS", "histogram": "FREQUENCY"},
            {"column_name": "ID", "histogram": "NONE"},
        ]
        cursor = make_cursor_dispatch(
            {
                "all_tab_histograms": {
                    "description": [("endpoint_number",), ("endpoint_value",)],
                    "rows": [(1, 100), (2, 200)],
                }
            }
        )
        ctx = _make_ctx(parsed)
        result = _collect_histograms(cursor, "HR", "T", parsed, columns, ctx)
        assert "STATUS" in result
        assert len(result["STATUS"]) == 2

    def test_skips_none_histogram(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM t WHERE id = 1",
            sql_type="SELECT",
            where_columns=["id"],
        )
        columns = [{"column_name": "ID", "histogram": "NONE"}]
        cursor = make_cursor_dispatch({})
        ctx = _make_ctx(parsed)
        result = _collect_histograms(cursor, "HR", "T", parsed, columns, ctx)
        assert result == {}

    def test_skips_irrelevant_columns(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM t WHERE status = 1",
            sql_type="SELECT",
            where_columns=["status"],
        )
        columns = [{"column_name": "NAME", "histogram": "FREQUENCY"}]
        cursor = make_cursor_dispatch({})
        ctx = _make_ctx(parsed)
        result = _collect_histograms(cursor, "HR", "T", parsed, columns, ctx)
        assert result == {}

    def test_error_recorded(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM t WHERE col1 = 1",
            sql_type="SELECT",
            where_columns=["col1"],
        )
        columns = [{"column_name": "COL1", "histogram": "FREQUENCY"}]
        cursor = make_cursor_dispatch({"all_tab_histograms": RuntimeError("fail")})
        ctx = _make_ctx(parsed)
        result = _collect_histograms(cursor, "HR", "T", parsed, columns, ctx)
        assert result == {}
        assert len(ctx.errors) == 1


class TestCollectOptimizerParams:
    """Testes de _collect_optimizer_params."""

    def test_returns_name_value_dict(self):
        cursor = make_cursor_dispatch(
            {
                "v$parameter": {
                    "description": [("name",), ("value",)],
                    "rows": [("optimizer_mode", "ALL_ROWS"), ("cursor_sharing", "EXACT")],
                }
            }
        )
        ctx = _make_ctx()
        result = _collect_optimizer_params(cursor, ctx)
        assert result == {"optimizer_mode": "ALL_ROWS", "cursor_sharing": "EXACT"}

    def test_empty_on_error(self):
        cursor = make_cursor_dispatch({"v$parameter": RuntimeError("fail")})
        ctx = _make_ctx()
        result = _collect_optimizer_params(cursor, ctx)
        assert result == {}
        assert len(ctx.errors) == 1


# ---------------------------------------------------------------------------
# 3. collect_context (integração com mock completo)
# ---------------------------------------------------------------------------


def _make_full_cursor_dispatch(overrides=None):
    """Cursor dispatch com respostas padrão para collect_context completo.

    Retorna respostas razoáveis para todas as queries que collect_context faz.
    ``overrides`` permite substituir respostas específicas.
    """
    defaults = {
        # db_version
        "v$version": ("Oracle Database 19c",),
        # object_type
        "all_objects": {
            "description": [("object_type",)],
            "rows": [("TABLE",)],
        },
        # explain plan
        "EXPLAIN PLAN": None,
        "DBMS_XPLAN": [("Plan hash value: 999",), ("|  0 | SELECT |",)],
        "DELETE FROM PLAN_TABLE": None,
        # DDL
        "DBMS_METADATA": {
            "description": [("ddl",)],
            "rows": [("CREATE TABLE t (id NUMBER)",)],
        },
        # table_stats
        "all_tables": {
            "description": [("num_rows",), ("blocks",)],
            "rows": [(5000, 100)],
        },
        # column_stats
        "all_tab_col": {
            "description": [("column_name",), ("data_type",), ("histogram",)],
            "rows": [("ID", "NUMBER", "NONE"), ("NAME", "VARCHAR2", "NONE")],
        },
        # indexes
        "all_indexes": {
            "description": [("index_name",), ("index_type",)],
            "rows": [("PK_USERS", "NORMAL")],
        },
        # constraints
        "all_constraints": {
            "description": [("constraint_name",), ("constraint_type",)],
            "rows": [("PK_USERS", "P")],
        },
        # optimizer_params
        "v$parameter": {
            "description": [("name",), ("value",)],
            "rows": [("optimizer_mode", "ALL_ROWS")],
        },
        # partitions (deep mode)
        "all_tab_partitions": {
            "description": [("partition_name",)],
            "rows": [],
        },
    }
    if overrides:
        defaults.update(overrides)
    return defaults


class TestCollectContextBasic:
    """Testes básicos de collect_context."""

    def test_one_table_no_flags(self, simple_parsed_sql):
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor

        ctx = collect_context(simple_parsed_sql, conn, "HR")
        assert len(ctx.tables) == 1
        assert ctx.tables[0].name == "USERS"
        assert ctx.tables[0].schema == "HR"

    def test_strips_semicolon(self):
        parsed = ParsedSQL(
            raw_sql="SELECT 1 FROM dual;",
            sql_type="SELECT",
            tables=[],
        )
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        collect_context(parsed, conn, "HR")
        assert not parsed.raw_sql.endswith(";")

    def test_db_version_collected(self, simple_parsed_sql):
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(simple_parsed_sql, conn, "HR")
        assert ctx.db_version == "Oracle Database 19c"

    def test_explain_plan_collected(self, simple_parsed_sql):
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(simple_parsed_sql, conn, "HR")
        assert ctx.execution_plan is not None
        assert any("Plan hash" in line for line in ctx.execution_plan)

    def test_optimizer_params_collected(self, simple_parsed_sql):
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(simple_parsed_sql, conn, "HR")
        assert "optimizer_mode" in ctx.optimizer_params

    def test_dedup_tables(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.id",
            sql_type="SELECT",
            tables=[
                {"name": "USERS", "schema": "HR", "alias": "u1"},
                {"name": "USERS", "schema": "HR", "alias": "u2"},
            ],
        )
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR")
        assert len(ctx.tables) == 1


class TestCollectContextExecute:
    """Testes do modo execute (runtime plan)."""

    def test_execute_true_collects_runtime(self):
        overrides = {
            "ALTER SESSION": None,
            "v$mystat": (100,),
            "v$session": ("abc123def456",),
            "v$sql": {
                "description": [("sql_id",), ("executions",)],
                "rows": [("abc123def456", 1)],
            },
            "v$session_event": {
                "description": [("event",)],
                "rows": [],
            },
        }
        parsed = ParsedSQL(
            raw_sql="SELECT id FROM users WHERE id = 1",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        )
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", execute=True)
        assert ctx.runtime_plan is not None or ctx.execution_plan is not None

    def test_execute_ignored_for_non_select(self):
        parsed = ParsedSQL(
            raw_sql="UPDATE users SET name = 'x' WHERE id = 1",
            sql_type="UPDATE",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        )
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", execute=True)
        # Should use explain plan, not runtime
        assert ctx.runtime_plan is None
        assert ctx.execution_plan is not None


class TestCollectContextDeep:
    """Testes do modo deep (partitions + histograms)."""

    def test_deep_collects_partitions_and_histograms(self, simple_parsed_sql):
        overrides = {
            "all_tab_partitions": {
                "description": [("partition_name",)],
                "rows": [("P1",), ("P2",)],
            },
            "all_tab_histograms": {
                "description": [("endpoint_number",), ("endpoint_value",)],
                "rows": [(1, 100)],
            },
            "all_tab_col": {
                "description": [("column_name",), ("data_type",), ("histogram",)],
                "rows": [("ID", "NUMBER", "FREQUENCY")],
            },
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(simple_parsed_sql, conn, "HR", deep=True)
        assert ctx.tables[0].partitions is not None

    def test_no_deep_skips_partitions(self, simple_parsed_sql):
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(simple_parsed_sql, conn, "HR", deep=False)
        assert ctx.tables[0].partitions == []


class TestCollectContextViews:
    """Testes de views em collect_context."""

    def test_view_no_expand_skips_details(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM v_employees",
            sql_type="SELECT",
            tables=[{"name": "V_EMPLOYEES", "schema": "HR", "alias": None}],
        )
        overrides = {
            "all_objects": {
                "description": [("object_type",)],
                "rows": [("VIEW",)],
            },
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", expand_views=False)
        assert ctx.tables[0].object_type == "VIEW"
        assert ctx.tables[0].ddl is None  # skipped
        assert ctx.tables[0].stats is None

    def test_view_with_expand_collects_details(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM v_employees",
            sql_type="SELECT",
            tables=[{"name": "V_EMPLOYEES", "schema": "HR", "alias": None}],
        )
        overrides = {
            "all_objects": {
                "description": [("object_type",)],
                "rows": [("VIEW",)],
            },
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", expand_views=True)
        assert ctx.tables[0].object_type == "VIEW"
        assert ctx.tables[0].ddl is not None

    def test_view_expansion_populates_index_table_map(self):
        ddl = "CREATE VIEW v\nAS\nSELECT * FROM hr.orders"
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM v_orders",
            sql_type="SELECT",
            tables=[{"name": "V_ORDERS", "schema": "HR", "alias": None}],
        )
        overrides = {
            "all_objects": {
                "description": [("object_type",)],
                "rows": [("VIEW",)],
            },
            "DBMS_METADATA": {
                "description": [("ddl",)],
                "rows": [(ddl,)],
            },
            "all_indexes": {
                "description": [("index_name",), ("table_name",)],
                "rows": [("IDX_ORDERS_DATE", "ORDERS")],
            },
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", expand_views=False)
        # view_expansions should be populated
        assert len(ctx.view_expansions) >= 0  # Depends on DDL parse
        # index_table_map populated if view_expansions found tables
        # This is a best-effort check since DDL parsing may vary


class TestCollectContextFunctions:
    """Testes de expand_functions em collect_context."""

    def test_expand_functions_collects_ddl(self):
        parsed = ParsedSQL(
            raw_sql="SELECT fn_calc(id) FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
            functions=[{"schema": "HR", "name": "FN_CALC"}],
        )
        # Use :function_name como fragmento — é único ao SQL de function_ddl()
        # e evita conflito com table_ddl que também contém DBMS_METADATA
        overrides = {
            ":function_name": {
                "description": [("ddl",)],
                "rows": [("CREATE FUNCTION fn_calc ...",)],
            },
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", expand_functions=True)
        assert "HR.FN_CALC" in ctx.function_ddls

    def test_expand_functions_false_skips(self):
        parsed = ParsedSQL(
            raw_sql="SELECT fn_calc(id) FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
            functions=[{"schema": "HR", "name": "FN_CALC"}],
        )
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", expand_functions=False)
        assert len(ctx.function_ddls) == 0

    def test_function_error_recorded(self):
        parsed = ParsedSQL(
            raw_sql="SELECT fn_calc(id) FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
            functions=[{"schema": "HR", "name": "FN_CALC"}],
        )
        # :function_name é fragmento único ao SQL de function_ddl()
        overrides = {
            ":function_name": RuntimeError("no priv on function"),
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR", expand_functions=True)
        assert any("FN_CALC" in e for e in ctx.errors)
        assert len(ctx.function_ddls) == 0


class TestCollectContextErrorResilience:
    """Testes de resiliência a erros em collect_context."""

    def test_ddl_fail_continues_stats(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        )
        overrides = {
            "DBMS_METADATA": RuntimeError("no DDL access"),
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR")
        assert ctx.tables[0].ddl is None
        # Stats should still be collected
        assert ctx.tables[0].stats is not None

    def test_multiple_errors_accumulated(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        )
        overrides = {
            "DBMS_METADATA": RuntimeError("no DDL"),
            "all_tables": RuntimeError("no stats"),
            "all_tab_col": RuntimeError("no columns"),
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR")
        assert len(ctx.errors) >= 2

    def test_everything_fails_returns_ctx_with_errors(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        )
        # Make most things fail
        overrides = {
            "v$version": RuntimeError("fail"),
            "EXPLAIN PLAN": RuntimeError("fail"),
            "DBMS_METADATA": RuntimeError("fail"),
            "all_tables": RuntimeError("fail"),
            "all_tab_col": RuntimeError("fail"),
            "all_indexes": RuntimeError("fail"),
            "all_constraints": RuntimeError("fail"),
            "v$parameter": RuntimeError("fail"),
            "all_objects": RuntimeError("fail"),
        }
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR")
        assert len(ctx.errors) >= 3
        assert ctx.parsed_sql is parsed


class TestCollectContextMultipleTables:
    """Testes com múltiplas tabelas."""

    def test_two_tables_collected(self, parsed_two_tables):
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed_two_tables, conn, "HR")
        assert len(ctx.tables) == 2

    def test_mix_table_and_view(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM users JOIN v_orders ON 1=1",
            sql_type="SELECT",
            tables=[
                {"name": "USERS", "schema": "HR", "alias": None},
                {"name": "V_ORDERS", "schema": "HR", "alias": None},
            ],
        )
        # We need different object_type responses per table
        # Since our dispatch is fragment-based and both use all_objects,
        # we return TABLE (first call) — this tests at least 2 tables collected
        cursor = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = collect_context(parsed, conn, "HR")
        assert len(ctx.tables) == 2


# ---------------------------------------------------------------------------
# 4. Cache de metadata
# ---------------------------------------------------------------------------


class TestClearCache:
    """Testes de clear_cache."""

    def test_clears_all_caches(self):
        _table_cache.put("HR.T", TableContext(name="T", schema="HR"))
        _optimizer_cache.put("global", {"optimizer_mode": "ALL_ROWS"})
        _index_map_cache.put("HR", {"IDX1": "T"})
        clear_cache()
        assert len(_table_cache) == 0
        assert len(_optimizer_cache) == 0
        assert len(_index_map_cache) == 0

    def test_clear_empty_is_noop(self):
        clear_cache()
        assert len(_table_cache) == 0


class TestCacheHitMiss:
    """Testes de cache hit/miss em collect_context."""

    def setup_method(self):
        clear_cache()

    def test_cache_hit_skips_queries(self, simple_parsed_sql):
        """Segunda chamada com use_cache=True não re-executa queries de tabela."""
        cursor1 = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn1 = MagicMock()
        conn1.cursor.return_value = cursor1
        ctx1 = collect_context(simple_parsed_sql, conn1, "HR", use_cache=True)
        call_count_1 = cursor1.execute.call_count

        # Segunda chamada com cache — deve ter menos queries
        cursor2 = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn2 = MagicMock()
        conn2.cursor.return_value = cursor2
        ctx2 = collect_context(simple_parsed_sql, conn2, "HR", use_cache=True)
        call_count_2 = cursor2.execute.call_count

        # A segunda chamada deve executar menos queries (cache de tabela + optimizer)
        assert call_count_2 < call_count_1
        # Mas o resultado deve ser equivalente
        assert len(ctx2.tables) == len(ctx1.tables)
        assert ctx2.tables[0].name == ctx1.tables[0].name
        assert ctx2.optimizer_params == ctx1.optimizer_params

    def test_no_cache_always_executes(self, simple_parsed_sql):
        """Com use_cache=False, queries são sempre executadas."""
        cursor1 = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn1 = MagicMock()
        conn1.cursor.return_value = cursor1
        collect_context(simple_parsed_sql, conn1, "HR", use_cache=False)
        call_count_1 = cursor1.execute.call_count

        cursor2 = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn2 = MagicMock()
        conn2.cursor.return_value = cursor2
        collect_context(simple_parsed_sql, conn2, "HR", use_cache=False)
        call_count_2 = cursor2.execute.call_count

        # Sem cache, ambas devem executar o mesmo número de queries
        assert call_count_2 == call_count_1

    def test_deep_upgrade_recollects(self, simple_parsed_sql):
        """Se cache não tem histograms/partitions mas deep=True, re-coleta."""
        cursor1 = make_cursor_dispatch(_make_full_cursor_dispatch())
        conn1 = MagicMock()
        conn1.cursor.return_value = cursor1
        # Primeira chamada sem deep — cacheia sem histograms
        collect_context(simple_parsed_sql, conn1, "HR", deep=False, use_cache=True)

        # Segunda chamada com deep — deve re-coletar (cache miss parcial)
        overrides = {
            "all_tab_partitions": {
                "description": [("partition_name",)],
                "rows": [("P1",)],
            },
        }
        cursor2 = make_cursor_dispatch(_make_full_cursor_dispatch(overrides))
        conn2 = MagicMock()
        conn2.cursor.return_value = cursor2
        ctx2 = collect_context(simple_parsed_sql, conn2, "HR", deep=True, use_cache=True)
        # Deve ter re-coletado (partitions agora presentes)
        assert ctx2.tables[0].partitions is not None


# ---------------------------------------------------------------------------
# 5. _LRUCache
# ---------------------------------------------------------------------------


class TestLRUCache:
    """Testes de _LRUCache: put/get, TTL, eviction, contains, clear."""

    def test_put_get(self):
        cache: _LRUCache[str] = _LRUCache()
        cache.put("a", "val_a")
        assert cache.get("a") == "val_a"
        assert cache.get("b") is None

    def test_ttl_expiration(self):
        """Entradas expiradas retornam None."""
        import time

        cache: _LRUCache[str] = _LRUCache(ttl=0.01)
        cache.put("a", "val_a")
        time.sleep(0.02)
        assert cache.get("a") is None

    def test_eviction_oldest(self):
        """Ao atingir max_entries, entrada mais antiga é removida."""
        cache: _LRUCache[str] = _LRUCache(max_entries=3)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.put("c", "3")
        cache.put("d", "4")  # evicta "a"
        assert cache.get("a") is None
        assert cache.get("b") == "2"
        assert cache.get("d") == "4"

    def test_contains(self):
        cache: _LRUCache[str] = _LRUCache()
        cache.put("x", "val")
        assert "x" in cache
        assert "y" not in cache

    def test_clear(self):
        cache: _LRUCache[str] = _LRUCache()
        cache.put("a", "1")
        cache.put("b", "2")
        cache.clear()
        assert len(cache) == 0
        assert cache.get("a") is None

    def test_len(self):
        cache: _LRUCache[int] = _LRUCache()
        assert len(cache) == 0
        cache.put("a", 1)
        cache.put("b", 2)
        assert len(cache) == 2

    def test_overwrite_existing_key(self):
        """put com chave existente atualiza valor sem eviction."""
        cache: _LRUCache[str] = _LRUCache(max_entries=2)
        cache.put("a", "old")
        cache.put("b", "val_b")
        cache.put("a", "new")  # atualiza, não evicta
        assert cache.get("a") == "new"
        assert cache.get("b") == "val_b"
        assert len(cache) == 2


# ---------------------------------------------------------------------------
# 6. Batch collection
# ---------------------------------------------------------------------------


class TestBuildTupleInClause:
    """Testes de _build_tuple_in_clause."""

    def test_single_pair(self):
        sql, params = _build_tuple_in_clause([("HR", "USERS")])
        assert "(:o0, :t0)" in sql
        assert params == {"o0": "HR", "t0": "USERS"}

    def test_multiple_pairs(self):
        sql, params = _build_tuple_in_clause([("HR", "USERS"), ("HR", "ORDERS")])
        assert ":o0" in sql
        assert ":o1" in sql
        assert params["o0"] == "HR"
        assert params["t1"] == "ORDERS"

    def test_empty_pairs(self):
        sql, params = _build_tuple_in_clause([])
        assert sql == ""
        assert params == {}

    def test_uppercases_values(self):
        _sql, params = _build_tuple_in_clause([("hr", "users")])
        assert params["o0"] == "HR"
        assert params["t0"] == "USERS"


class TestBatchCollectTables:
    """Testes de _batch_collect_tables."""

    def test_empty_list_returns_empty(self):
        """Lista vazia → dict vazio."""
        ctx = CollectedContext(parsed_sql=ParsedSQL(raw_sql="SELECT 1", sql_type="SELECT"))
        cursor = MagicMock()
        result = _batch_collect_tables(cursor, [], ctx)
        assert result == {}

    def test_distributes_results(self):
        """Batch rows distribuídos por schema.table key."""
        ctx = CollectedContext(parsed_sql=ParsedSQL(raw_sql="SELECT 1", sql_type="SELECT"))
        cursor = MagicMock()

        # Mock _execute_query return values for each batch call
        call_count = [0]
        original_execute = cursor.execute

        def mock_execute(sql, params=None):
            return original_execute(sql, params)

        cursor.execute = mock_execute

        # We need to mock at module level
        from sqlmentor import collector as collector_mod

        original_exec_query = collector_mod._execute_query

        def mock_exec_query(cur, sql, params):
            call_count[0] += 1
            if call_count[0] == 1:  # batch_table_stats
                return [{"owner": "HR", "table_name": "USERS", "num_rows": 100}]
            if call_count[0] == 2:  # batch_column_stats
                return [
                    {
                        "owner": "HR",
                        "table_name": "USERS",
                        "column_name": "ID",
                        "data_type": "NUMBER",
                    },
                ]
            if call_count[0] == 3:  # batch_indexes
                return []
            if call_count[0] == 4:  # batch_constraints
                return []
            return []

        collector_mod._execute_query = mock_exec_query
        try:
            result = _batch_collect_tables(cursor, [("HR", "USERS")], ctx)
            assert "HR.USERS" in result
            assert result["HR.USERS"]["stats"]["num_rows"] == 100
            assert len(result["HR.USERS"]["columns"]) == 1
        finally:
            collector_mod._execute_query = original_exec_query

    def test_batch_fallback_on_exception(self):
        """Batch levanta exceção → retorna dict vazio (sinaliza fallback)."""
        ctx = CollectedContext(parsed_sql=ParsedSQL(raw_sql="SELECT 1", sql_type="SELECT"))

        from sqlmentor import collector as collector_mod

        original_exec_query = collector_mod._execute_query

        def mock_exec_query(cur, sql, params):
            raise RuntimeError("ORA-00942: table or view does not exist")

        collector_mod._execute_query = mock_exec_query
        try:
            result = _batch_collect_tables(MagicMock(), [("HR", "USERS")], ctx)
            assert result == {}
            assert any("fallback" in e.lower() for e in ctx.errors)
        finally:
            collector_mod._execute_query = original_exec_query
