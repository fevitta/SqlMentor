"""Fixtures compartilhadas para testes do sqlmentor."""

from unittest.mock import MagicMock

import pytest

from sqlmentor.collector import CollectedContext, TableContext
from sqlmentor.parser import ParsedSQL


@pytest.fixture
def simple_parsed_sql() -> ParsedSQL:
    """ParsedSQL mínimo para um SELECT simples."""
    return ParsedSQL(
        raw_sql="SELECT id, name FROM users WHERE id = 1",
        sql_type="SELECT",
        tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        where_columns=["id"],
    )


@pytest.fixture
def empty_parsed_sql() -> ParsedSQL:
    """ParsedSQL vazio (parse falhou ou SQL inválido)."""
    return ParsedSQL(
        raw_sql="",
        sql_type="UNKNOWN",
        is_parseable=False,
    )


@pytest.fixture
def table_context() -> TableContext:
    """TableContext mínimo para testes."""
    return TableContext(
        name="USERS",
        schema="HR",
        object_type="TABLE",
        stats={"num_rows": 1000, "blocks": 50, "last_analyzed": "2025-01-01"},
        columns=[
            {"column_name": "ID", "data_type": "NUMBER", "nullable": "N"},
            {"column_name": "NAME", "data_type": "VARCHAR2", "nullable": "Y"},
        ],
        indexes=[
            {
                "index_name": "PK_USERS",
                "index_type": "NORMAL",
                "uniqueness": "UNIQUE",
                "columns": "ID",
            }
        ],
        constraints=[
            {
                "constraint_name": "PK_USERS",
                "constraint_type": "P",
                "columns": "ID",
            }
        ],
    )


@pytest.fixture
def minimal_collected_context(simple_parsed_sql, table_context) -> CollectedContext:
    """CollectedContext mínimo com dados reais para testes de report."""
    return CollectedContext(
        parsed_sql=simple_parsed_sql,
        db_version="Oracle Database 19c Enterprise Edition Release 19.0.0.0.0",
        execution_plan=[
            "Plan hash value: 123456789",
            "",
            "------------------------------------------------------------",
            "| Id  | Operation         | Name  | Rows  | Bytes | Cost  |",
            "------------------------------------------------------------",
            "|   0 | SELECT STATEMENT  |       |     1 |    50 |     2 |",
            "|   1 |  TABLE ACCESS FULL| USERS |     1 |    50 |     2 |",
            "------------------------------------------------------------",
        ],
        tables=[table_context],
        optimizer_params={"optimizer_mode": "ALL_ROWS"},
    )


@pytest.fixture
def empty_collected_context(empty_parsed_sql) -> CollectedContext:
    """CollectedContext totalmente vazio (sem tabelas, sem plano)."""
    return CollectedContext(parsed_sql=empty_parsed_sql)


@pytest.fixture
def tmp_connections_file(tmp_path, monkeypatch):
    """Redireciona CONNECTIONS_FILE para diretório temporário."""
    import sqlmentor.connector as conn_mod

    tmp_config_dir = tmp_path / ".sqlmentor"
    tmp_config_dir.mkdir()
    tmp_file = tmp_config_dir / "connections.yaml"

    monkeypatch.setattr(conn_mod, "CONFIG_DIR", tmp_config_dir)
    monkeypatch.setattr(conn_mod, "CONNECTIONS_FILE", tmp_file)
    return tmp_file


@pytest.fixture
def parsed_with_view() -> ParsedSQL:
    """ParsedSQL com VIEW referenciada."""
    return ParsedSQL(
        raw_sql="SELECT * FROM v_employees WHERE dept_id = 10",
        sql_type="SELECT",
        tables=[{"name": "V_EMPLOYEES", "schema": "HR", "alias": None}],
        where_columns=["dept_id"],
    )


@pytest.fixture
def parsed_two_tables() -> ParsedSQL:
    """ParsedSQL com JOIN de 2 tabelas."""
    return ParsedSQL(
        raw_sql="SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        sql_type="SELECT",
        tables=[
            {"name": "USERS", "schema": "HR", "alias": "u"},
            {"name": "ORDERS", "schema": "HR", "alias": "o"},
        ],
        join_columns=["id", "user_id"],
    )


@pytest.fixture
def parsed_with_functions() -> ParsedSQL:
    """ParsedSQL com funções PL/SQL."""
    return ParsedSQL(
        raw_sql="SELECT fn_calc(id), pkg_util.format(name) FROM users",
        sql_type="SELECT",
        tables=[{"name": "USERS", "schema": "HR", "alias": None}],
        functions=[
            {"schema": "HR", "name": "FN_CALC"},
            {"schema": "HR", "name": "FORMAT"},
        ],
    )


@pytest.fixture
def rich_table_context() -> TableContext:
    """TableContext completo com stats, columns, indexes, partitions, histograms, constraints."""
    return TableContext(
        name="ORDERS",
        schema="HR",
        object_type="TABLE",
        stats={
            "num_rows": 500_000,
            "blocks": 8000,
            "avg_row_len": 120,
            "last_analyzed": "2025-06-15",
            "sample_size": 50_000,
            "partitioned": "YES",
            "compression": "ENABLED",
            "degree": "4",
        },
        columns=[
            {
                "column_name": "ORDER_ID",
                "data_type": "NUMBER",
                "data_length": "22",
                "nullable": "N",
                "num_distinct": 500_000,
                "num_nulls": 0,
                "histogram": "NONE",
            },
            {
                "column_name": "USER_ID",
                "data_type": "NUMBER",
                "data_length": "22",
                "nullable": "N",
                "num_distinct": 10_000,
                "num_nulls": 0,
                "histogram": "FREQUENCY",
            },
            {
                "column_name": "STATUS",
                "data_type": "VARCHAR2",
                "data_length": "20",
                "nullable": "Y",
                "num_distinct": 5,
                "num_nulls": 100,
                "histogram": "FREQUENCY",
            },
        ],
        indexes=[
            {
                "index_name": "PK_ORDERS",
                "index_type": "NORMAL",
                "uniqueness": "UNIQUE",
                "columns": "ORDER_ID",
                "distinct_keys": 500_000,
                "clustering_factor": 7500,
                "blevel": 2,
                "last_analyzed": "2025-06-15",
                "status": "VALID",
            },
            {
                "index_name": "IDX_ORDERS_USER",
                "index_type": "NORMAL",
                "uniqueness": "NONUNIQUE",
                "columns": "USER_ID",
                "distinct_keys": 10_000,
                "clustering_factor": 200_000,
                "blevel": 5,
                "last_analyzed": "2025-06-15",
                "status": "VALID",
            },
        ],
        constraints=[
            {
                "constraint_name": "PK_ORDERS",
                "constraint_type": "P",
                "columns": "ORDER_ID",
            },
            {
                "constraint_name": "FK_ORDERS_USER",
                "constraint_type": "R",
                "columns": "USER_ID",
                "r_owner": "HR",
                "r_table_name": "USERS",
            },
        ],
        partitions=[
            {
                "partition_name": "P2024",
                "partition_position": 1,
                "num_rows": 200_000,
                "last_analyzed": "2025-06-15",
            },
            {
                "partition_name": "P2025",
                "partition_position": 2,
                "num_rows": 300_000,
                "last_analyzed": "2025-06-15",
            },
        ],
        histograms={
            "STATUS": [{"endpoint_value": "ACTIVE", "endpoint_number": 400_000}],
        },
    )


@pytest.fixture
def small_table_context() -> TableContext:
    """TableContext com num_rows < 1000 (formato compacto)."""
    return TableContext(
        name="LOOKUP",
        schema="HR",
        object_type="TABLE",
        stats={"num_rows": 50, "blocks": 1},
        columns=[
            {"column_name": "CODE", "data_type": "VARCHAR2", "nullable": "N"},
            {"column_name": "DESCRIPTION", "data_type": "VARCHAR2", "nullable": "Y"},
        ],
        indexes=[
            {
                "index_name": "PK_LOOKUP",
                "index_type": "NORMAL",
                "uniqueness": "UNIQUE",
                "columns": "CODE",
            }
        ],
        constraints=[
            {
                "constraint_name": "PK_LOOKUP",
                "constraint_type": "P",
                "columns": "CODE",
            }
        ],
    )


@pytest.fixture
def view_table_context() -> TableContext:
    """TableContext tipo VIEW com DDL."""
    return TableContext(
        name="V_ACTIVE_ORDERS",
        schema="HR",
        object_type="VIEW",
        ddl=(
            'CREATE OR REPLACE FORCE VIEW "HR"."V_ACTIVE_ORDERS" '
            '("ORDER_ID", "USER_ID", "STATUS") AS\n'
            "  SELECT ORDER_ID, USER_ID, STATUS\n"
            "  FROM ORDERS\n"
            "  WHERE STATUS = 'ACTIVE'"
        ),
        columns=[
            {"column_name": "ORDER_ID", "data_type": "NUMBER", "nullable": "N"},
            {"column_name": "USER_ID", "data_type": "NUMBER", "nullable": "N"},
            {"column_name": "STATUS", "data_type": "VARCHAR2", "nullable": "Y"},
        ],
    )


# ─── Linhas de plano ALLSTATS para reuso nos testes ─────────────────────

ALLSTATS_PLAN_LINES = [
    "SQL_ID  abc123def456, child number 0",
    "Plan hash value: 987654321",
    "",
    "--------------------------------------------------------------------------------------------------------------",
    "| Id  | Operation                    | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads |",
    "--------------------------------------------------------------------------------------------------------------",
    "|   0 | SELECT STATEMENT             |                |      1 |        |     50 |00:00:00.26 |   10919 |     0 |",
    "|   1 |  HASH JOIN                   |                |      1 |     50 |     50 |00:00:00.26 |   10919 |     0 |",
    "|   2 |   TABLE ACCESS FULL          | USERS          |      1 |  10000 |  10000 |00:00:00.05 |     200 |     0 |",
    "|*  3 |   INDEX RANGE SCAN           | IDX_ORD_USER   |    100 |     10 |    100 |00:00:00.01 |     300 |    50 |",
    "|   4 |   TABLE ACCESS BY INDEX ROWID| ORDERS         |    100 |     10 |    100 |00:00:00.20 |   10419 |     0 |",
    "--------------------------------------------------------------------------------------------------------------",
    "",
    "Predicate Information (identified by operation id):",
    "---------------------------------------------------",
    "",
    '   3 - access("U"."ID"="O"."USER_ID")',
    '   3 - filter(TO_NUMBER("O"."STATUS")=1)',
]


@pytest.fixture
def allstats_plan_lines() -> list[str]:
    """Linhas de plano ALLSTATS LAST para testes."""
    return ALLSTATS_PLAN_LINES.copy()


@pytest.fixture
def rich_collected_context(
    rich_table_context, small_table_context, allstats_plan_lines
) -> CollectedContext:
    """CollectedContext com todos os campos populados para testes de markdown."""
    parsed = ParsedSQL(
        raw_sql="SELECT o.order_id, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = 'ACTIVE' ORDER BY o.order_id GROUP BY u.name",
        sql_type="SELECT",
        tables=[
            {"name": "ORDERS", "schema": "HR", "alias": "o"},
            {"name": "USERS", "schema": "HR", "alias": "u"},
        ],
        where_columns=["status"],
        join_columns=["user_id", "id"],
        order_columns=["order_id"],
        group_columns=["name"],
        subqueries=1,
        functions=[
            {"schema": "HR", "name": "FN_CALC"},
        ],
    )
    return CollectedContext(
        parsed_sql=parsed,
        db_version="Oracle Database 19c Enterprise Edition Release 19.0.0.0.0",
        execution_plan=[
            "Plan hash value: 123456789",
            "------------------------------------------------------------",
            "| Id  | Operation         | Name   | Rows  | Bytes | Cost  |",
            "------------------------------------------------------------",
            "|   0 | SELECT STATEMENT  |        |     1 |    50 |     2 |",
            "|   1 |  TABLE ACCESS FULL| ORDERS |     1 |    50 |     2 |",
            "------------------------------------------------------------",
        ],
        runtime_plan=allstats_plan_lines,
        runtime_stats={
            "sql_id": "abc123def456",
            "child_number": 0,
            "plan_hash_value": 987654321,
            "executions": 5,
            "avg_elapsed_ms": 260.5,
            "avg_cpu_ms": 120.0,
            "avg_buffer_gets": 10919,
            "avg_rows_per_exec": 50,
            "disk_reads": 50,
            "rows_processed": 250,
            "sorts": 1,
            "parse_calls": 5,
            "loads": 3,
            "invalidations": 1,
            "version_count": 8,
        },
        wait_events=[
            {
                "event": "db file sequential read",
                "total_waits": 50,
                "time_waited_ms": 12.5,
                "average_wait": 0.25,
            },
        ],
        view_expansions={
            "V_ACTIVE_ORDERS": ["HR.ORDERS", "HR.AUDIT_LOG"],
            "V_USER_SUMMARY": ["HR.USERS", "HR.AUDIT_LOG"],
        },
        index_table_map={"IDX_ORD_USER": "ORDERS", "PK_USERS": "USERS"},
        tables=[rich_table_context, small_table_context],
        function_ddls={
            "HR.FN_CALC": "CREATE FUNCTION HR.FN_CALC(p_id NUMBER) RETURN NUMBER IS BEGIN RETURN p_id * 2; END;"
        },
        optimizer_params={
            "optimizer_mode": "ALL_ROWS",
            "optimizer_index_cost_adj": "10",
            "cursor_sharing": "SIMILAR",
        },
        errors=["Timeout ao coletar histograms de ORDERS.STATUS"],
    )


@pytest.fixture
def mock_oracle_cursor():
    """Cursor Oracle mockado que retorna dados configuráveis."""
    cursor = MagicMock()
    cursor.description = [("COL1",), ("COL2",)]
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.__iter__ = MagicMock(return_value=iter([]))
    return cursor


@pytest.fixture
def mock_oracle_connection(mock_oracle_cursor):
    """Conexão Oracle mockada."""
    conn = MagicMock()
    conn.cursor.return_value = mock_oracle_cursor
    conn.username = "SQL_TUNER"
    return conn
