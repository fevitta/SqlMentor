"""Testes de integração: coleta de metadata (stats, columns, indexes, DDL, views)."""

import pytest

from sqlmentor.collector import (
    CollectedContext,
    _batch_collect_tables,
    _collect_column_stats,
    _collect_constraints,
    _collect_ddl,
    _collect_indexes,
    _collect_table_stats,
    _detect_object_type,
    _execute_query,
    collect_context,
)
from sqlmentor.queries import index_to_table_map

pytestmark = pytest.mark.oracle


class TestDetectObjectType:
    """Detecta tipo correto (TABLE vs VIEW) via ALL_OBJECTS."""

    def test_table(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        result = _detect_object_type(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        assert result == "TABLE"

    def test_view(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        result = _detect_object_type(oracle_cursor, oracle_schema, "V_ACTIVE_EMPLOYEES", ctx)
        assert result == "VIEW"

    def test_nonexistent_defaults_to_table(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        result = _detect_object_type(oracle_cursor, oracle_schema, "NAO_EXISTE_XYZ", ctx)
        assert result == "TABLE"


class TestTableStats:
    """Coleta de estatísticas via ALL_TABLES."""

    def test_employees_stats(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        stats = _collect_table_stats(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        assert stats is not None
        assert stats["num_rows"] == 1000

    def test_departments_stats(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        stats = _collect_table_stats(oracle_cursor, oracle_schema, "DEPARTMENTS", ctx)
        assert stats is not None
        assert stats["num_rows"] == 10

    def test_orders_stats(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        stats = _collect_table_stats(oracle_cursor, oracle_schema, "ORDERS", ctx)
        assert stats is not None
        assert stats["num_rows"] == 5000


class TestColumnStats:
    """Coleta de colunas via ALL_TAB_COLUMNS + ALL_TAB_COL_STATISTICS."""

    def test_employees_columns(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        columns = _collect_column_stats(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        col_names = [c["column_name"] for c in columns]
        assert "EMP_ID" in col_names
        assert "FIRST_NAME" in col_names
        assert "SALARY" in col_names
        assert "DEPT_ID" in col_names
        assert "STATUS" in col_names
        assert len(columns) == 8

    def test_column_data_types(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        columns = _collect_column_stats(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        by_name = {c["column_name"]: c for c in columns}
        assert by_name["EMP_ID"]["data_type"] == "NUMBER"
        assert by_name["FIRST_NAME"]["data_type"] == "VARCHAR2"
        assert by_name["HIRE_DATE"]["data_type"] == "DATE"

    def test_column_nullable(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        columns = _collect_column_stats(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        by_name = {c["column_name"]: c for c in columns}
        assert by_name["EMP_ID"]["nullable"] == "N"
        assert by_name["SALARY"]["nullable"] == "Y"


class TestIndexes:
    """Coleta de índices via ALL_INDEXES + ALL_IND_COLUMNS."""

    def test_employees_indexes(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        idx_names = {i["index_name"] for i in idxs}
        # PK gera index implícito + 4 explícitos
        assert len(idxs) == 5
        assert "IDX_EMP_DEPT" in idx_names
        assert "IDX_EMP_NAME" in idx_names
        assert "IDX_EMP_HIRE" in idx_names
        assert "IDX_EMP_EMAIL" in idx_names

    def test_composite_index_columns(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        name_idx = next(i for i in idxs if i["index_name"] == "IDX_EMP_NAME")
        assert "LAST_NAME" in name_idx["columns"]
        assert "FIRST_NAME" in name_idx["columns"]

    def test_unique_index(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        email_idx = next(i for i in idxs if i["index_name"] == "IDX_EMP_EMAIL")
        assert email_idx["uniqueness"] == "UNIQUE"

    def test_orders_indexes(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(oracle_cursor, oracle_schema, "ORDERS", ctx)
        # PK + 3 explícitos
        assert len(idxs) == 4


class TestConstraints:
    """Coleta de constraints via ALL_CONSTRAINTS + ALL_CONS_COLUMNS."""

    def test_employees_pk(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        cons = _collect_constraints(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        pk = [c for c in cons if c["constraint_type"] == "P"]
        assert len(pk) == 1
        assert "EMP_ID" in pk[0]["columns"]

    def test_employees_fk(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        cons = _collect_constraints(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        fks = [c for c in cons if c["constraint_type"] == "R"]
        assert len(fks) == 1
        assert fks[0]["r_table_name"] == "DEPARTMENTS"

    def test_employees_check(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        cons = _collect_constraints(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        checks = [c for c in cons if c["constraint_type"] == "C"]
        # NOT NULL gera check constraints implícitos + 1 explícito (chk_emp_status)
        assert len(checks) >= 1


class TestDDL:
    """Coleta de DDL via DBMS_METADATA.GET_DDL."""

    def test_table_ddl(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        ddl = _collect_ddl(oracle_cursor, oracle_schema, "EMPLOYEES", ctx)
        assert ddl is not None
        assert "CREATE TABLE" in ddl.upper() or "CREATE" in ddl.upper()
        assert "EMPLOYEES" in ddl.upper()

    def test_view_ddl(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        ddl = _collect_ddl(oracle_cursor, oracle_schema, "V_ACTIVE_EMPLOYEES", ctx)
        assert ddl is not None
        assert "VIEW" in ddl.upper()

    def test_function_ddl(self, oracle_cursor, oracle_schema):
        from sqlmentor.queries import function_ddl

        sql, params = function_ddl(oracle_schema, "FN_ANNUAL_SALARY")
        rows = _execute_query(oracle_cursor, sql, params)
        assert len(rows) == 1
        ddl = str(rows[0]["ddl"])
        assert "FUNCTION" in ddl.upper()
        assert "FN_ANNUAL_SALARY" in ddl.upper()


class TestBatchCollect:
    """Coleta batch de múltiplas tabelas em uma query."""

    def test_batch_two_tables(self, oracle_cursor, oracle_schema):
        ctx = CollectedContext(parsed_sql=None)
        pairs = [(oracle_schema, "EMPLOYEES"), (oracle_schema, "ORDERS")]
        result = _batch_collect_tables(oracle_cursor, pairs, ctx)

        assert f"{oracle_schema}.EMPLOYEES" in result
        assert f"{oracle_schema}.ORDERS" in result

        emp_data = result[f"{oracle_schema}.EMPLOYEES"]
        assert "stats" in emp_data
        assert "columns" in emp_data
        assert "indexes" in emp_data
        assert "constraints" in emp_data
        assert emp_data["stats"]["num_rows"] == 1000

    def test_batch_empty_pairs(self, oracle_cursor):
        ctx = CollectedContext(parsed_sql=None)
        result = _batch_collect_tables(oracle_cursor, [], ctx)
        assert result == {}


class TestIndexToTableMap:
    """Mapa index_name → table_name via ALL_INDEXES."""

    def test_maps_known_indexes(self, oracle_cursor, oracle_schema):
        sql, params = index_to_table_map(oracle_schema)
        rows = _execute_query(oracle_cursor, sql, params)
        idx_map = {r["index_name"]: r["table_name"] for r in rows}
        assert idx_map.get("IDX_EMP_DEPT") == "EMPLOYEES"
        assert idx_map.get("IDX_ORD_EMP") == "ORDERS"


class TestCollectContextFull:
    """Fluxo completo de collect_context() contra Oracle real."""

    def test_full_collection(self, oracle_conn, oracle_schema, parsed_employees_orders):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        assert ctx.db_version is not None
        assert "Oracle" in ctx.db_version
        assert ctx.execution_plan is not None
        assert len(ctx.execution_plan) > 0
        assert len(ctx.tables) == 2
        assert len(ctx.optimizer_params) > 0
        assert ctx.errors == []

        # Verifica que ambas as tabelas foram coletadas com metadata
        table_names = {t.name for t in ctx.tables}
        assert "EMPLOYEES" in table_names
        assert "ORDERS" in table_names

        for tctx in ctx.tables:
            assert tctx.stats is not None
            assert len(tctx.columns) > 0
            assert len(tctx.indexes) > 0

    def test_view_expansion(self, oracle_conn, oracle_schema, parsed_view_query):
        ctx = collect_context(
            parsed=parsed_view_query,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        # VIEW deve estar nos tables
        view_tables = [t for t in ctx.tables if t.name == "V_ACTIVE_EMPLOYEES"]
        assert len(view_tables) == 1
        assert view_tables[0].object_type == "VIEW"

        # View expansion deve mapear para tabelas internas
        key = f"{oracle_schema}.V_ACTIVE_EMPLOYEES"
        assert key in ctx.view_expansions
        inner = ctx.view_expansions[key]
        # A view referencia EMPLOYEES e DEPARTMENTS (possivelmente com schema qualifier)
        inner_upper = [t.upper() for t in inner]
        assert any("EMPLOYEES" in t for t in inner_upper)
        assert any("DEPARTMENTS" in t for t in inner_upper)

    def test_optimizer_params_collected(self, oracle_conn, oracle_schema, parsed_single_table):
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        assert "optimizer_mode" in ctx.optimizer_params
        assert ctx.optimizer_params["optimizer_mode"] in ("ALL_ROWS", "FIRST_ROWS", "CHOOSE")
