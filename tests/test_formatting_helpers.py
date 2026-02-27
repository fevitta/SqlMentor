"""Testes das funções de formatação do report — _format_*, _build_fk_map, _filter_columns_by_sql."""

from sqlmentor.collector import TableContext
from sqlmentor.report import (
    _build_fk_map,
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
