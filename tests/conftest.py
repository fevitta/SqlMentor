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
