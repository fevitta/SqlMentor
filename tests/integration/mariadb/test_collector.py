"""Testes de integração MariaDB: coleta de metadata (stats, columns, indexes, DDL, views)."""

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
    collect_context,
)

pytestmark = pytest.mark.mariadb


class TestDetectObjectType:
    """Detecta tipo correto (TABLE vs VIEW) via information_schema.TABLES."""

    def test_table(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        result = _detect_object_type(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        assert result == "TABLE"

    def test_view(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        result = _detect_object_type(
            mariadb_cursor, mariadb_schema, "V_ACTIVE_EMPLOYEES", ctx, mariadb_adapter
        )
        assert result == "VIEW"

    def test_nonexistent_defaults_to_table(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        result = _detect_object_type(
            mariadb_cursor, mariadb_schema, "NAO_EXISTE_XYZ", ctx, mariadb_adapter
        )
        assert result == "TABLE"


class TestTableStats:
    """Coleta de estatísticas via information_schema.TABLES.

    InnoDB TABLE_ROWS é estimativa — usar assertions aproximadas.
    """

    def test_employees_stats(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        stats = _collect_table_stats(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        assert stats is not None
        # InnoDB: TABLE_ROWS é estimativa, não exato
        assert stats["num_rows"] > 900

    def test_departments_stats(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        stats = _collect_table_stats(
            mariadb_cursor, mariadb_schema, "DEPARTMENTS", ctx, mariadb_adapter
        )
        assert stats is not None
        assert stats["num_rows"] == 10

    def test_orders_stats(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        stats = _collect_table_stats(mariadb_cursor, mariadb_schema, "ORDERS", ctx, mariadb_adapter)
        assert stats is not None
        # InnoDB: TABLE_ROWS é estimativa
        assert stats["num_rows"] > 4500


class TestColumnStats:
    """Coleta de colunas via information_schema.COLUMNS."""

    def test_employees_columns(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        columns = _collect_column_stats(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        col_names = [c["column_name"] for c in columns]
        assert "EMP_ID" in col_names
        assert "FIRST_NAME" in col_names
        assert "SALARY" in col_names
        assert "DEPT_ID" in col_names
        assert "STATUS" in col_names
        assert len(columns) == 8

    def test_column_data_types(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        columns = _collect_column_stats(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        by_name = {c["column_name"]: c for c in columns}
        assert "int" in by_name["EMP_ID"]["data_type"].lower()
        assert "varchar" in by_name["FIRST_NAME"]["data_type"].lower()
        assert by_name["HIRE_DATE"]["data_type"].lower() == "date"

    def test_column_nullable(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        columns = _collect_column_stats(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        by_name = {c["column_name"]: c for c in columns}
        assert by_name["EMP_ID"]["nullable"] == "N"
        assert by_name["SALARY"]["nullable"] == "Y"


class TestIndexes:
    """Coleta de índices via information_schema.STATISTICS."""

    def test_employees_indexes(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter)
        idx_names = {i["index_name"] for i in idxs}
        # PK (PRIMARY) + 4 explícitos
        assert len(idxs) >= 5
        assert "IDX_EMP_DEPT" in idx_names
        assert "IDX_EMP_NAME" in idx_names
        assert "IDX_EMP_HIRE" in idx_names
        assert "IDX_EMP_EMAIL" in idx_names

    def test_composite_index_columns(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter)
        name_idx = next(i for i in idxs if i["index_name"] == "IDX_EMP_NAME")
        assert "LAST_NAME" in name_idx["columns"]
        assert "FIRST_NAME" in name_idx["columns"]

    def test_unique_index(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter)
        email_idx = next(i for i in idxs if i["index_name"] == "IDX_EMP_EMAIL")
        assert email_idx["uniqueness"] == "UNIQUE"

    def test_orders_indexes(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        idxs = _collect_indexes(mariadb_cursor, mariadb_schema, "ORDERS", ctx, mariadb_adapter)
        # PRIMARY + 3 explícitos
        assert len(idxs) >= 4


class TestConstraints:
    """Coleta de constraints via TABLE_CONSTRAINTS + KEY_COLUMN_USAGE."""

    def test_employees_pk(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        cons = _collect_constraints(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        pk = [c for c in cons if c["constraint_type"] == "P"]
        assert len(pk) == 1
        assert "EMP_ID" in pk[0]["columns"]

    def test_employees_fk(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        cons = _collect_constraints(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        fks = [c for c in cons if c["constraint_type"] == "R"]
        assert len(fks) >= 1
        fk_targets = {c["r_table_name"] for c in fks}
        assert "DEPARTMENTS" in fk_targets

    def test_employees_check_best_effort(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        """CHECK constraints: MariaDB 10.2+ suporta, mas info_schema pode variar."""
        ctx = CollectedContext(parsed_sql=None)
        cons = _collect_constraints(
            mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter
        )
        # CHECK constraints podem ou não aparecer no information_schema
        # dependendo da versão do MariaDB — apenas verifica que a query não falha
        assert isinstance(cons, list)


class TestDDL:
    """Coleta de DDL via SHOW CREATE TABLE."""

    def test_table_ddl(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        ddl = _collect_ddl(mariadb_cursor, mariadb_schema, "EMPLOYEES", ctx, mariadb_adapter)
        assert ddl is not None
        assert "CREATE TABLE" in ddl.upper()
        assert "EMPLOYEES" in ddl.upper()

    def test_view_ddl(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        ddl = _collect_ddl(
            mariadb_cursor, mariadb_schema, "V_ACTIVE_EMPLOYEES", ctx, mariadb_adapter
        )
        assert ddl is not None
        # SHOW CREATE TABLE em view retorna CREATE ... VIEW
        assert "VIEW" in ddl.upper()


class TestBatchCollect:
    """Coleta batch de múltiplas tabelas em uma query."""

    def test_batch_two_tables(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        pairs = [(mariadb_schema, "EMPLOYEES"), (mariadb_schema, "ORDERS")]
        result = _batch_collect_tables(mariadb_cursor, pairs, ctx, mariadb_adapter)

        assert f"{mariadb_schema}.EMPLOYEES" in result
        assert f"{mariadb_schema}.ORDERS" in result

        emp_data = result[f"{mariadb_schema}.EMPLOYEES"]
        assert "stats" in emp_data
        assert "columns" in emp_data
        assert "indexes" in emp_data
        assert "constraints" in emp_data
        # InnoDB: TABLE_ROWS é estimativa
        assert emp_data["stats"]["num_rows"] > 900

    def test_batch_empty_pairs(self, mariadb_cursor, mariadb_adapter):
        ctx = CollectedContext(parsed_sql=None)
        result = _batch_collect_tables(mariadb_cursor, [], ctx, mariadb_adapter)
        assert result == {}


class TestIndexToTableMap:
    """Mapa index_name → table_name via information_schema.STATISTICS."""

    def test_maps_known_indexes(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        sql, params = mariadb_adapter.query_builder.index_to_table_map(mariadb_schema)
        rows = mariadb_adapter.execute_query(mariadb_cursor, sql, params)
        idx_map = {r["index_name"]: r["table_name"] for r in rows}
        assert idx_map.get("IDX_EMP_DEPT") == "EMPLOYEES"
        assert idx_map.get("IDX_ORD_EMP") == "ORDERS"


class TestCollectContextFull:
    """Fluxo completo de collect_context() contra MariaDB real."""

    def test_full_collection(
        self, mariadb_conn, mariadb_schema, parsed_employees_orders, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        assert ctx.db_type == "mariadb"
        assert ctx.db_version is not None
        assert "MariaDB" in ctx.db_version
        assert ctx.execution_plan is not None
        assert len(ctx.execution_plan) > 0
        assert len(ctx.tables) == 2
        assert len(ctx.optimizer_params) > 0

        table_names = {t.name for t in ctx.tables}
        assert "EMPLOYEES" in table_names
        assert "ORDERS" in table_names

        for tctx in ctx.tables:
            assert tctx.stats is not None
            assert len(tctx.columns) > 0
            assert len(tctx.indexes) > 0

    def test_view_expansion(self, mariadb_conn, mariadb_schema, parsed_view_query, mariadb_adapter):
        ctx = collect_context(
            parsed=parsed_view_query,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        view_tables = [t for t in ctx.tables if t.name == "V_ACTIVE_EMPLOYEES"]
        assert len(view_tables) == 1
        assert view_tables[0].object_type == "VIEW"

        key = f"{mariadb_schema}.V_ACTIVE_EMPLOYEES"
        assert key in ctx.view_expansions
        inner = ctx.view_expansions[key]
        inner_upper = [t.upper() for t in inner]
        assert any("EMPLOYEES" in t for t in inner_upper)
        assert any("DEPARTMENTS" in t for t in inner_upper)

    def test_partitions_with_deep(self, mariadb_conn, mariadb_schema, mariadb_adapter):
        """Coleta partições da tabela ORDER_ARCHIVE com deep=True."""
        from sqlmentor.parser import ParsedSQL

        parsed = ParsedSQL(
            raw_sql="SELECT ARCHIVE_ID, ORDER_ID FROM ORDER_ARCHIVE WHERE ARCHIVE_ID = 1",
            sql_type="SELECT",
            tables=[{"name": "ORDER_ARCHIVE", "schema": None, "alias": None}],
            where_columns=["ARCHIVE_ID"],
        )
        ctx = collect_context(
            parsed=parsed,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            deep=True,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        archive_tables = [t for t in ctx.tables if t.name == "ORDER_ARCHIVE"]
        assert len(archive_tables) == 1
        assert len(archive_tables[0].partitions) >= 4
