"""Testes das funções de formatação do report — _format_*, _build_fk_map, _filter_columns_by_sql."""

from sqlmentor.collector import TableContext
from sqlmentor.report import (
    _build_fk_map,
    _classify_uniform_columns,
    _extract_plan_index_names,
    _filter_columns_by_sql,
    _format_column_stats,
    _format_column_structure,
    _format_indexes,
    _format_optimizer_params,
    _format_partitions,
    _format_runtime_stats,
    _format_small_table,
    _format_table_stats,
    _format_wait_events,
    _strip_ddl_storage,
)

# ─── _format_table_stats ─────────────────────────────────────────────────────


class TestFormatTableStats:
    def test_basic_stats(self):
        result = _format_table_stats({"num_rows": 1000, "blocks": 50})
        assert "**Rows:** 1,000" in result
        assert "**Blocks:** 50" in result

    def test_with_avg_row_len_and_last_analyzed(self):
        result = _format_table_stats(
            {
                "num_rows": 500,
                "blocks": 10,
                "avg_row_len": 120,
                "last_analyzed": "2025-06-15",
            }
        )
        assert "**Avg Row Len:** 120" in result
        assert "**Last Analyzed:** 2025-06-15" in result

    def test_sample_size_warning(self):
        result = _format_table_stats(
            {
                "num_rows": 100_000,
                "sample_size": 5_000,
            }
        )
        assert "**Sample Size:**" in result
        assert "⚠️" in result  # 5% < 10%

    def test_sample_size_no_warning(self):
        result = _format_table_stats(
            {
                "num_rows": 100_000,
                "sample_size": 50_000,
            }
        )
        assert "**Sample Size:**" in result
        assert "⚠️" not in result  # 50% >= 10%

    def test_partitioned_compression_degree(self):
        result = _format_table_stats(
            {
                "num_rows": 1000,
                "partitioned": "YES",
                "compression": "ENABLED",
                "degree": "4",
            }
        )
        assert "**Partitioned:** YES" in result
        assert "**Compression:** ENABLED" in result
        assert "**Parallel Degree:** 4" in result


# ─── _format_column_stats ────────────────────────────────────────────────────


class TestFormatColumnStats:
    def test_basic_columns(self):
        cols = [
            {
                "column_name": "ID",
                "data_type": "NUMBER",
                "nullable": "N",
                "num_distinct": 1000,
                "num_nulls": 0,
                "histogram": "NONE",
            },
            {
                "column_name": "NAME",
                "data_type": "VARCHAR2",
                "data_length": "100",
                "nullable": "Y",
                "num_distinct": 500,
                "num_nulls": 10,
                "histogram": "FREQUENCY",
            },
        ]
        result = _format_column_stats(cols)
        assert "| Coluna | Tipo |" in result
        assert "| ID |" in result
        assert "| NAME |" in result

    def test_varchar_with_length(self):
        cols = [
            {
                "column_name": "DESCR",
                "data_type": "VARCHAR2",
                "data_length": "200",
                "nullable": "Y",
                "num_distinct": 50,
                "num_nulls": 0,
                "histogram": "NONE",
            },
        ]
        result = _format_column_stats(cols)
        assert "VARCHAR2(200)" in result

    def test_fk_column_shown(self):
        cols = [
            {
                "column_name": "USER_ID",
                "data_type": "NUMBER",
                "nullable": "N",
                "num_distinct": 100,
                "num_nulls": 0,
                "histogram": "NONE",
            },
        ]
        fk_map = {"USER_ID": "HR.USERS"}
        result = _format_column_stats(cols, fk_map)
        assert "HR.USERS" in result


# ─── _format_column_structure ────────────────────────────────────────────────


class TestFormatColumnStructure:
    def test_basic(self):
        cols = [
            {"column_name": "ID", "data_type": "NUMBER", "nullable": "N"},
            {"column_name": "NAME", "data_type": "VARCHAR2", "data_length": "50", "nullable": "Y"},
        ]
        result = _format_column_structure(cols)
        assert "| Coluna | Tipo | Nullable |" in result
        assert "| ID | NUMBER | N |" in result
        assert "VARCHAR2(50)" in result

    def test_number_without_length(self):
        cols = [{"column_name": "TOTAL", "data_type": "NUMBER", "nullable": "N"}]
        result = _format_column_structure(cols)
        # NUMBER without data_length should NOT show NUMBER()
        assert "NUMBER()" not in result
        assert "NUMBER" in result


# ─── _format_indexes ─────────────────────────────────────────────────────────


class TestFormatIndexes:
    def test_basic_index(self):
        idxs = [
            {
                "index_name": "PK_USERS",
                "index_type": "NORMAL",
                "uniqueness": "UNIQUE",
                "columns": "ID",
                "distinct_keys": 1000,
                "clustering_factor": 50,
                "blevel": 1,
                "last_analyzed": "2025-01-01",
                "status": "VALID",
            }
        ]
        result = _format_indexes(idxs)
        assert "| PK_USERS |" in result
        assert "VALID" in result

    def test_blevel_warning(self):
        idxs = [
            {
                "index_name": "IDX_BAD",
                "index_type": "NORMAL",
                "uniqueness": "NONUNIQUE",
                "columns": "COL1",
                "blevel": 5,
            }
        ]
        result = _format_indexes(idxs)
        assert "5 ⚠️" in result

    def test_blevel_normal(self):
        idxs = [
            {
                "index_name": "IDX_OK",
                "index_type": "NORMAL",
                "uniqueness": "UNIQUE",
                "columns": "COL1",
                "blevel": 2,
            }
        ]
        result = _format_indexes(idxs)
        assert "⚠️" not in result


# ─── _build_fk_map ──────────────────────────────────────────────────────────


class TestBuildFkMap:
    def test_fk_constraint(self):
        constraints = [
            {
                "constraint_type": "R",
                "columns": "USER_ID",
                "r_owner": "HR",
                "r_table_name": "USERS",
            }
        ]
        result = _build_fk_map(constraints)
        assert result == {"USER_ID": "HR.USERS"}

    def test_non_fk_ignored(self):
        constraints = [
            {"constraint_type": "P", "columns": "ID"},
            {"constraint_type": "U", "columns": "EMAIL"},
        ]
        result = _build_fk_map(constraints)
        assert result == {}

    def test_composite_fk(self):
        constraints = [
            {
                "constraint_type": "R",
                "columns": "COL1, COL2",
                "r_owner": "SCHEMA",
                "r_table_name": "REF_TABLE",
            }
        ]
        result = _build_fk_map(constraints)
        assert result == {"COL1": "SCHEMA.REF_TABLE", "COL2": "SCHEMA.REF_TABLE"}


# ─── _format_partitions ─────────────────────────────────────────────────────


class TestFormatPartitions:
    def test_basic(self):
        parts = [
            {
                "partition_name": "P2024",
                "partition_position": 1,
                "num_rows": 100_000,
                "last_analyzed": "2025-01-01",
            },
            {
                "partition_name": "P2025",
                "partition_position": 2,
                "num_rows": 200_000,
                "last_analyzed": "2025-06-01",
            },
        ]
        result = _format_partitions(parts)
        assert "| P2024 |" in result
        assert "| P2025 |" in result
        lines = result.strip().split("\n")
        assert len(lines) == 4  # header + separator + 2 data rows

    def test_missing_fields(self):
        parts = [{"partition_name": "P1"}]
        result = _format_partitions(parts)
        assert "| P1 |" in result
        assert "?" in result  # missing fields show "?"


# ─── _format_runtime_stats ───────────────────────────────────────────────────


class TestFormatRuntimeStats:
    def test_basic_metrics(self):
        stats = {"sql_id": "abc123", "executions": 10, "avg_elapsed_ms": 150.5}
        result = _format_runtime_stats(stats)
        assert "**SQL ID:** abc123" in result
        assert "**Executions:** 10" in result
        assert "**Avg Elapsed (ms):** 150.5" in result

    def test_hard_parse_warning(self):
        stats = {"loads": 5}
        result = _format_runtime_stats(stats)
        assert "hard parses" in result
        assert "⚠️" in result

    def test_invalidations_warning(self):
        stats = {"invalidations": 3}
        result = _format_runtime_stats(stats)
        assert "invalidações" in result

    def test_version_count_warning(self):
        stats = {"version_count": 10}
        result = _format_runtime_stats(stats)
        assert "child cursors" in result

    def test_parse_calls_warning(self):
        stats = {"parse_calls": 100, "executions": 100}
        result = _format_runtime_stats(stats)
        assert "cursor não está sendo reutilizado" in result

    def test_cpu_bound_info(self):
        stats = {"avg_elapsed_ms": 100, "avg_cpu_ms": 98}
        result = _format_runtime_stats(stats)
        assert "CPU-bound" in result
        assert "CPU-bound" in result  # info marker present in output

    def test_io_bound_warning(self):
        stats = {"avg_elapsed_ms": 100, "avg_cpu_ms": 30}
        result = _format_runtime_stats(stats)
        assert "I/O-bound" in result
        assert "⚠️" in result

    def test_buffer_gets_per_row_warning(self):
        stats = {"avg_buffer_gets": 50_000, "avg_rows_per_exec": 10}
        result = _format_runtime_stats(stats)
        assert "buffer gets/row" in result


# ─── _format_wait_events ────────────────────────────────────────────────────


class TestFormatWaitEvents:
    def test_basic(self):
        events = [
            {
                "event": "db file sequential read",
                "total_waits": 100,
                "time_waited_ms": 25.0,
                "average_wait": 0.25,
            }
        ]
        result = _format_wait_events(events)
        assert "| db file sequential read |" in result

    def test_multiple_events(self):
        events = [
            {"event": "event1", "total_waits": 10, "time_waited_ms": 5.0, "average_wait": 0.5},
            {"event": "event2", "total_waits": 20, "time_waited_ms": 10.0, "average_wait": 0.5},
        ]
        result = _format_wait_events(events)
        data_lines = [
            line
            for line in result.split("\n")
            if line.startswith("|") and "Event" not in line and "---" not in line
        ]
        assert len(data_lines) == 2


# ─── _format_optimizer_params ────────────────────────────────────────────────


class TestFormatOptimizerParams:
    def test_default_values_no_warning(self):
        params = {"optimizer_mode": "ALL_ROWS"}
        result = _format_optimizer_params(params)
        assert "ALL_ROWS" in result
        assert "⚠️" not in result

    def test_non_default_warning(self):
        params = {"optimizer_mode": "FIRST_ROWS"}
        result = _format_optimizer_params(params)
        assert "⚠️" in result
        assert "default: ALL_ROWS" in result

    def test_index_cost_adj_extreme(self):
        params = {"optimizer_index_cost_adj": "5"}
        result = _format_optimizer_params(params)
        assert "extremamente baixo" in result

    def test_cursor_sharing_warning(self):
        params = {"cursor_sharing": "SIMILAR"}
        result = _format_optimizer_params(params)
        assert "literais estão sendo substituídos" in result


# ─── _format_small_table ────────────────────────────────────────────────────


class TestFormatSmallTable:
    def test_basic(self):
        table = TableContext(
            name="LOOKUP",
            schema="HR",
            object_type="TABLE",
            stats={"num_rows": 50, "blocks": 1},
        )
        result = _format_small_table(table)
        assert "**HR.LOOKUP**" in result
        assert "50 rows" in result

    def test_with_pk_and_fk(self):
        table = TableContext(
            name="ORDER_ITEMS",
            schema="HR",
            object_type="TABLE",
            stats={"num_rows": 200, "blocks": 3},
            indexes=[{"uniqueness": "UNIQUE", "columns": "ITEM_ID"}],
            constraints=[
                {
                    "constraint_type": "R",
                    "columns": "ORDER_ID",
                    "r_owner": "HR",
                    "r_table_name": "ORDERS",
                },
            ],
        )
        result = _format_small_table(table)
        assert "PK: ITEM_ID" in result
        assert "FK: ORDER_ID → HR.ORDERS" in result


# ─── _filter_columns_by_sql ─────────────────────────────────────────────────


class TestFilterColumnsBySql:
    def test_filters_by_reference(self):
        cols = [
            {"column_name": "ID"},
            {"column_name": "NAME"},
            {"column_name": "UNUSED_COL"},
        ]
        result = _filter_columns_by_sql(cols, {"ID", "NAME"})
        assert len(result) == 2
        assert all(c["column_name"] in ("ID", "NAME") for c in result)

    def test_fallback_all_if_none_match(self):
        cols = [
            {"column_name": "COL_A"},
            {"column_name": "COL_B"},
        ]
        result = _filter_columns_by_sql(cols, {"NONEXISTENT"})
        assert len(result) == 2  # fallback returns all


# ─── _strip_ddl_storage (R11) ──────────────────────────────────────────────


class TestStripDdlStorage:
    def test_removes_storage_clause(self):
        ddl = "CREATE TABLE T (ID NUMBER)\nSTORAGE(INITIAL 64K NEXT 1M MINEXTENTS 1)"
        result = _strip_ddl_storage(ddl)
        assert "STORAGE" not in result
        assert "CREATE TABLE T (ID NUMBER)" in result

    def test_removes_tablespace(self):
        ddl = 'CREATE TABLE T (ID NUMBER)\nTABLESPACE "USERS_DATA"'
        result = _strip_ddl_storage(ddl)
        assert "TABLESPACE" not in result
        assert "USERS_DATA" not in result

    def test_removes_pctfree_initrans(self):
        ddl = "CREATE TABLE T (ID NUMBER)\nPCTFREE 10 INITRANS 2 MAXTRANS 255"
        result = _strip_ddl_storage(ddl)
        assert "PCTFREE" not in result
        assert "INITRANS" not in result
        assert "MAXTRANS" not in result

    def test_removes_segment_creation(self):
        ddl = "CREATE TABLE T (ID NUMBER)\nSEGMENT CREATION IMMEDIATE"
        result = _strip_ddl_storage(ddl)
        assert "SEGMENT" not in result
        assert "CREATION" not in result

    def test_preserves_column_definitions(self):
        ddl = "CREATE TABLE T (\n  ID NUMBER,\n  NAME VARCHAR2(100),\n  CACHE_FLAG NUMBER\n)"
        result = _strip_ddl_storage(ddl)
        assert "ID NUMBER" in result
        assert "NAME VARCHAR2(100)" in result
        assert "CACHE_FLAG NUMBER" in result

    def test_nested_parens_in_storage(self):
        ddl = (
            "CREATE TABLE T (ID NUMBER)\n"
            "STORAGE(INITIAL 64K NEXT 1M PCTINCREASE 0 "
            "BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)"
        )
        result = _strip_ddl_storage(ddl)
        assert "STORAGE" not in result
        assert "BUFFER_POOL" not in result
        assert "CREATE TABLE T (ID NUMBER)" in result


# ─── _classify_uniform_columns (R10) ──────────────────────────────────────


class TestClassifyUniformColumns:
    def test_uniform_column_filtered(self):
        cols = [
            {"column_name": "ID", "num_distinct": 9000, "histogram": "NONE"},
        ]
        relevant, count = _classify_uniform_columns(cols, 10000, {})
        assert count == 1
        assert len(relevant) == 0

    def test_histogram_column_kept(self):
        cols = [
            {"column_name": "STATUS", "num_distinct": 9000, "histogram": "FREQUENCY"},
        ]
        relevant, count = _classify_uniform_columns(cols, 10000, {})
        assert count == 0
        assert len(relevant) == 1

    def test_low_cardinality_kept(self):
        cols = [
            {"column_name": "STATUS", "num_distinct": 5, "histogram": "NONE"},
        ]
        relevant, count = _classify_uniform_columns(cols, 10000, {})
        assert count == 0
        assert len(relevant) == 1

    def test_fk_column_kept(self):
        cols = [
            {"column_name": "USER_ID", "num_distinct": 9000, "histogram": "NONE"},
        ]
        relevant, count = _classify_uniform_columns(cols, 10000, {"USER_ID": "HR.USERS"})
        assert count == 0
        assert len(relevant) == 1

    def test_no_num_rows_skips(self):
        cols = [
            {"column_name": "ID", "num_distinct": 9000, "histogram": "NONE"},
        ]
        relevant, count = _classify_uniform_columns(cols, None, {})
        assert count == 0
        assert len(relevant) == 1


# ─── _extract_plan_index_names (R9) ──────────────────────────────────────


class TestExtractPlanIndexNames:
    def test_allstats_format(self):
        lines = [
            "|   1 | INDEX RANGE SCAN        | IDX_USERS_NAME               |     1 |     1 |      1 |00:00:00.01 |       2 |       0 |",
            "|   2 | TABLE ACCESS FULL       | USERS                        |     1 |  1000 |   1000 |00:00:00.05 |      50 |       0 |",
        ]
        result = _extract_plan_index_names(lines)
        assert result == {"IDX_USERS_NAME"}

    def test_estimated_format(self):
        lines = [
            "|   1 | INDEX UNIQUE SCAN       | PK_ORDERS                    |   100 |  800 |     2  (0)| 00:00:01 |",
        ]
        result = _extract_plan_index_names(lines)
        assert result == {"PK_ORDERS"}

    def test_no_index_returns_empty(self):
        lines = [
            "|   1 | TABLE ACCESS FULL       | USERS                        |     1 |  1000 |   1000 |00:00:00.05 |      50 |       0 |",
        ]
        result = _extract_plan_index_names(lines)
        assert result == set()


class TestOmitUnreferencedIndexes:
    """Tests for R9 index filtering integrated in to_markdown — tested indirectly."""

    def test_compact_omits_unreferenced(self):
        from sqlmentor.collector import CollectedContext, TableContext
        from sqlmentor.parser import ParsedSQL
        from sqlmentor.report import to_markdown

        plan = [
            "Plan hash value: 999",
            "|   1 | INDEX RANGE SCAN        | IDX_USED                     |     1 |     1 |      1 |00:00:00.01 |       2 |       0 |",
        ]
        table = TableContext(
            name="T",
            schema="HR",
            object_type="TABLE",
            stats={"num_rows": 5000},
            indexes=[
                {
                    "index_name": "IDX_USED",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL1",
                },
                {
                    "index_name": "IDX_UNUSED",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL2",
                },
                {
                    "index_name": "IDX_ALSO_UNUSED",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL3",
                },
            ],
        )
        ctx = CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT COL1 FROM HR.T",
                sql_type="SELECT",
                tables=[{"schema": "HR", "name": "T"}],
            ),
            runtime_plan=plan,
            tables=[table],
        )
        md = to_markdown(ctx, verbosity="compact")
        assert "IDX_USED" in md
        assert "índices não relacionados às cláusulas do SQL omitidos" in md

    def test_full_shows_all(self):
        from sqlmentor.collector import CollectedContext, TableContext
        from sqlmentor.parser import ParsedSQL
        from sqlmentor.report import to_markdown

        plan = [
            "Plan hash value: 999",
            "|   1 | INDEX RANGE SCAN        | IDX_USED                     |     1 |     1 |      1 |00:00:00.01 |       2 |       0 |",
        ]
        table = TableContext(
            name="T",
            schema="HR",
            object_type="TABLE",
            stats={"num_rows": 5000},
            indexes=[
                {
                    "index_name": "IDX_USED",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL1",
                },
                {
                    "index_name": "IDX_UNUSED",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL2",
                },
            ],
        )
        ctx = CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT COL1 FROM HR.T",
                sql_type="SELECT",
                tables=[{"schema": "HR", "name": "T"}],
            ),
            runtime_plan=plan,
            tables=[table],
        )
        md = to_markdown(ctx, verbosity="full")
        assert "IDX_USED" in md
        assert "IDX_UNUSED" in md
        assert "omitidos" not in md

    def test_all_referenced_no_note(self):
        from sqlmentor.collector import CollectedContext, TableContext
        from sqlmentor.parser import ParsedSQL
        from sqlmentor.report import to_markdown

        plan = [
            "Plan hash value: 999",
            "|   1 | INDEX RANGE SCAN        | IDX_A                        |     1 |     1 |      1 |00:00:00.01 |       2 |       0 |",
        ]
        table = TableContext(
            name="T",
            schema="HR",
            object_type="TABLE",
            stats={"num_rows": 5000},
            indexes=[
                {
                    "index_name": "IDX_A",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL1",
                },
            ],
        )
        ctx = CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT COL1 FROM HR.T",
                sql_type="SELECT",
                tables=[{"schema": "HR", "name": "T"}],
            ),
            runtime_plan=plan,
            tables=[table],
        )
        md = to_markdown(ctx, verbosity="compact")
        assert "IDX_A" in md
        assert "omitidos" not in md

    def test_note_shows_count(self):
        from sqlmentor.collector import CollectedContext, TableContext
        from sqlmentor.parser import ParsedSQL
        from sqlmentor.report import to_markdown

        plan = [
            "Plan hash value: 999",
            "|   1 | INDEX RANGE SCAN        | IDX_USED                     |     1 |     1 |      1 |00:00:00.01 |       2 |       0 |",
        ]
        table = TableContext(
            name="T",
            schema="HR",
            object_type="TABLE",
            stats={"num_rows": 5000},
            indexes=[
                {
                    "index_name": "IDX_USED",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL1",
                },
                {
                    "index_name": "IDX_X1",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL2",
                },
                {
                    "index_name": "IDX_X2",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL3",
                },
                {
                    "index_name": "IDX_X3",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "COL4",
                },
            ],
        )
        ctx = CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT COL1 FROM HR.T",
                sql_type="SELECT",
                tables=[{"schema": "HR", "name": "T"}],
            ),
            runtime_plan=plan,
            tables=[table],
        )
        md = to_markdown(ctx, verbosity="compact")
        assert "3 índices não relacionados às cláusulas do SQL omitidos" in md


# ─── --show-sql flag ─────────────────────────────────────────────────────


class TestShowSqlFlag:
    """Testes para o flag show_sql em to_markdown."""

    def _make_ctx(self):
        from sqlmentor.collector import CollectedContext
        from sqlmentor.parser import ParsedSQL

        return CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT COL1 FROM HR.T WHERE COL1 = 1",
                sql_type="SELECT",
                tables=[{"schema": "HR", "name": "T"}],
                where_columns=["COL1"],
            ),
        )

    def test_compact_default_omits_sql_block(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="compact")
        assert "```sql" not in md
        assert "**Tipo:** SELECT" in md
        assert "**Tabelas referenciadas:**" in md

    def test_compact_show_sql_includes_sql_block(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="compact", show_sql=True)
        assert "```sql" in md
        assert "SELECT COL1 FROM HR.T" in md

    def test_full_always_includes_sql_block(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="full")
        assert "```sql" in md
        assert "SELECT COL1 FROM HR.T" in md

    def test_full_ignores_show_sql_false(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="full", show_sql=False)
        assert "```sql" in md


# ─── --show-all-indexes flag ─────────────────────────────────────────────


class TestShowAllIndexesFlag:
    """Testes para o flag show_all_indexes e filtro por colunas do SQL."""

    def _make_ctx(self):
        from sqlmentor.collector import CollectedContext, TableContext
        from sqlmentor.parser import ParsedSQL

        table = TableContext(
            name="PEDIDO",
            schema="VENDAS",
            object_type="TABLE",
            stats={"num_rows": 50000},
            indexes=[
                {
                    "index_name": "IDX_PED_CLIENTE",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "CLIENTE_ID",
                },
                {
                    "index_name": "IDX_PED_DATA",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "DATA_PEDIDO",
                },
                {
                    "index_name": "IDX_PED_STATUS",
                    "index_type": "NORMAL",
                    "uniqueness": "NONUNIQUE",
                    "columns": "STATUS",
                },
                {
                    "index_name": "PK_PEDIDO",
                    "index_type": "NORMAL",
                    "uniqueness": "UNIQUE",
                    "columns": "PED_RECNO",
                },
            ],
        )
        return CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT * FROM VENDAS.PEDIDO WHERE CLIENTE_ID = :B1 ORDER BY DATA_PEDIDO",
                sql_type="SELECT",
                tables=[{"schema": "VENDAS", "name": "PEDIDO"}],
                where_columns=["CLIENTE_ID"],
                order_columns=["DATA_PEDIDO"],
            ),
            tables=[table],
        )

    def test_compact_default_shows_only_sql_relevant_indexes(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="compact")
        assert "IDX_PED_CLIENTE" in md
        assert "IDX_PED_DATA" in md
        assert "IDX_PED_STATUS" not in md or "índices não relacionados" in md

    def test_compact_show_all_indexes_shows_everything(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="compact", show_all_indexes=True)
        assert "IDX_PED_CLIENTE" in md
        assert "IDX_PED_DATA" in md
        assert "IDX_PED_STATUS" in md
        assert "PK_PEDIDO" in md
        assert "índices não relacionados" not in md

    def test_full_always_shows_all_indexes(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="full")
        assert "IDX_PED_CLIENTE" in md
        assert "IDX_PED_STATUS" in md
        assert "PK_PEDIDO" in md

    def test_compact_omitted_count_message(self):
        from sqlmentor.report import to_markdown

        md = to_markdown(self._make_ctx(), verbosity="compact")
        assert "índices não relacionados às cláusulas do SQL omitidos" in md


# ─── _strip_column_projection ────────────────────────────────────────────


class TestStripColumnProjection:
    """Testes para remoção de Column Projection Information no compact."""

    def test_strips_column_projection_section(self):
        from sqlmentor.report import _strip_column_projection

        lines = [
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            "   1 - access(\"T\".\"ID\"=1)",
            "",
            "Column Projection Information (identified by operation id):",
            "-----------------------------------------------------------",
            "   1 - \"T\".\"ID\"[NUMBER,22], \"T\".\"NOME\"[VARCHAR2,100]",
            "   2 - \"T\".\"ID\"[NUMBER,22]",
        ]
        result = _strip_column_projection(lines)
        assert any("Predicate" in l for l in result)
        assert any("access" in l for l in result)
        assert not any("Column Projection" in l for l in result)
        assert not any("NOME" in l for l in result)

    def test_no_column_projection_returns_unchanged(self):
        from sqlmentor.report import _strip_column_projection

        lines = [
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            "   1 - access(\"T\".\"ID\"=1)",
        ]
        result = _strip_column_projection(lines)
        assert result == lines

    def test_compact_plan_excludes_column_projection(self):
        from sqlmentor.collector import CollectedContext
        from sqlmentor.parser import ParsedSQL
        from sqlmentor.report import to_markdown

        plan = [
            "Plan hash value: 999",
            "",
            "| Id | Operation               | Name | Rows  | Bytes | Cost (%CPU)| Time     |",
            "|    |-------------------------|------|-------|-------|------------|----------|",
            "|  0 | SELECT STATEMENT        |      |     1 |    10 |     2   (0)| 00:00:01 |",
            "|  1 |  TABLE ACCESS FULL      | DUAL |     1 |    10 |     2   (0)| 00:00:01 |",
            "",
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            "   1 - filter(NULL IS NOT NULL)",
            "",
            "Column Projection Information (identified by operation id):",
            "-----------------------------------------------------------",
            "   1 - \"DUAL\".\"DUMMY\"[VARCHAR2,1]",
        ]
        ctx = CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT 1 FROM DUAL",
                sql_type="SELECT",
                tables=[{"schema": "SYS", "name": "DUAL"}],
            ),
            execution_plan=plan,
        )
        md = to_markdown(ctx, verbosity="compact")
        assert "Column Projection" not in md
        assert "Predicate Information" in md

    def test_full_plan_keeps_column_projection(self):
        from sqlmentor.collector import CollectedContext
        from sqlmentor.parser import ParsedSQL
        from sqlmentor.report import to_markdown

        plan = [
            "Plan hash value: 999",
            "",
            "| Id | Operation               | Name | Rows  | Bytes | Cost (%CPU)| Time     |",
            "|    |-------------------------|------|-------|-------|------------|----------|",
            "|  0 | SELECT STATEMENT        |      |     1 |    10 |     2   (0)| 00:00:01 |",
            "|  1 |  TABLE ACCESS FULL      | DUAL |     1 |    10 |     2   (0)| 00:00:01 |",
            "",
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            "   1 - filter(NULL IS NOT NULL)",
            "",
            "Column Projection Information (identified by operation id):",
            "-----------------------------------------------------------",
            "   1 - \"DUAL\".\"DUMMY\"[VARCHAR2,1]",
        ]
        ctx = CollectedContext(
            parsed_sql=ParsedSQL(
                raw_sql="SELECT 1 FROM DUAL",
                sql_type="SELECT",
                tables=[{"schema": "SYS", "name": "DUAL"}],
            ),
            execution_plan=plan,
        )
        md = to_markdown(ctx, verbosity="full")
        assert "Column Projection" in md
