"""Fixtures para testes de integração com MariaDB real.

Conexão via env vars com defaults matching docker-compose.yml.
Testes são pulados automaticamente quando MariaDB não está acessível.
"""

import os

import pytest

# Marca todos os testes deste diretório como @pytest.mark.mariadb
pytestmark = pytest.mark.mariadb

# -- Configuração via env vars (defaults = docker-compose) -------------------

MARIADB_HOST = os.environ.get("SQLMENTOR_MARIADB_HOST", "127.0.0.1")
MARIADB_PORT = int(os.environ.get("SQLMENTOR_MARIADB_PORT", "3307"))
MARIADB_USER = os.environ.get("SQLMENTOR_MARIADB_USER", "sqlmentor_test")
MARIADB_PASSWORD = os.environ.get("SQLMENTOR_MARIADB_PASSWORD", "TestPwd123")
MARIADB_DATABASE = os.environ.get("SQLMENTOR_MARIADB_DATABASE", "SQLMENTOR_TEST")
MARIADB_SCHEMA = os.environ.get("SQLMENTOR_MARIADB_SCHEMA", "SQLMENTOR_TEST")


def _try_connect():
    """Tenta conectar ao MariaDB. Retorna (connection, None) ou (None, reason)."""
    try:
        import pymysql

        conn = pymysql.connect(
            host=MARIADB_HOST,
            port=MARIADB_PORT,
            user=MARIADB_USER,
            password=MARIADB_PASSWORD,
            database=MARIADB_DATABASE,
            connect_timeout=10,
            charset="utf8mb4",
        )
        return conn, None
    except Exception as e:
        return None, str(e)


# -- Fixtures session-scoped -------------------------------------------------


@pytest.fixture(scope="session")
def mariadb_conn():
    """Conexão MariaDB session-scoped. Pula todos os testes se MariaDB indisponível."""
    conn, reason = _try_connect()
    if conn is None:
        pytest.skip(f"MariaDB não disponível: {reason}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def mariadb_schema():
    """Nome do schema de teste (uppercase, matching information_schema)."""
    return MARIADB_SCHEMA


# -- Fixtures function-scoped ------------------------------------------------


@pytest.fixture
def mariadb_cursor(mariadb_conn):
    """Cursor fresco por teste (function-scoped para isolamento)."""
    cursor = mariadb_conn.cursor()
    yield cursor
    cursor.close()


@pytest.fixture(scope="session")
def mariadb_adapter():
    """MariaDBAdapter instanciado para testes de integração."""
    from sqlmentor.adapters.mariadb import MariaDBAdapter

    return MariaDBAdapter()


# -- Fixtures para testes de inspect (performance_schema) --------------------

_SEED_SQL = (
    "SELECT /* SQLMENTOR_SEED_QUERY */ e.EMP_ID, e.FIRST_NAME, "
    "o.ORDER_ID, o.TOTAL "
    "FROM EMPLOYEES e "
    "JOIN ORDERS o ON e.EMP_ID = o.EMP_ID "
    "WHERE e.DEPT_ID = 10 AND o.STATUS = 'COMPLETED' "
    "ORDER BY o.TOTAL DESC"
)


@pytest.fixture(scope="session")
def seed_query_digest(mariadb_conn):
    """Encontra ou cria entrada em performance_schema para testes de inspect.

    Executa a seed query e obtém o DIGEST via events_statements_history.
    Requer performance-schema-consumer-events-statements-history=ON no server.
    """
    cursor = mariadb_conn.cursor()

    # Executa a seed query para registrar em performance_schema
    cursor.execute(_SEED_SQL)
    cursor.fetchall()

    # Pega o DIGEST da query anterior via events_statements_history
    cursor.execute(
        """
        SELECT DIGEST
        FROM performance_schema.events_statements_history
        WHERE THREAD_ID = (
            SELECT THREAD_ID FROM performance_schema.threads
            WHERE PROCESSLIST_ID = CONNECTION_ID()
        )
        ORDER BY EVENT_ID DESC
        LIMIT 1 OFFSET 1
        """
    )
    row = cursor.fetchone()
    cursor.close()

    if row is None or row[0] is None:
        pytest.skip("Não foi possível obter DIGEST da seed query (performance_schema habilitado?)")
    return row[0]


# -- Fixtures de ParsedSQL pré-montadas --------------------------------------


@pytest.fixture(scope="session")
def parsed_employees_orders():
    """ParsedSQL para JOIN employees/orders."""
    from sqlmentor.parser import ParsedSQL

    return ParsedSQL(
        raw_sql=(
            "SELECT e.EMP_ID, e.FIRST_NAME, o.ORDER_ID, o.TOTAL "
            "FROM EMPLOYEES e JOIN ORDERS o ON e.EMP_ID = o.EMP_ID "
            "WHERE e.DEPT_ID = 10 AND o.STATUS = 'COMPLETED'"
        ),
        sql_type="SELECT",
        tables=[
            {"name": "EMPLOYEES", "schema": None, "alias": "e"},
            {"name": "ORDERS", "schema": None, "alias": "o"},
        ],
        where_columns=["DEPT_ID", "STATUS"],
        join_columns=["EMP_ID"],
    )


@pytest.fixture(scope="session")
def parsed_single_table():
    """ParsedSQL para SELECT simples na tabela EMPLOYEES."""
    from sqlmentor.parser import ParsedSQL

    return ParsedSQL(
        raw_sql="SELECT EMP_ID, FIRST_NAME, SALARY FROM EMPLOYEES WHERE EMP_ID = 1",
        sql_type="SELECT",
        tables=[{"name": "EMPLOYEES", "schema": None, "alias": None}],
        where_columns=["EMP_ID"],
    )


@pytest.fixture(scope="session")
def parsed_view_query():
    """ParsedSQL para SELECT na view V_ACTIVE_EMPLOYEES."""
    from sqlmentor.parser import ParsedSQL

    return ParsedSQL(
        raw_sql="SELECT EMP_ID, FIRST_NAME, DEPT_NAME FROM V_ACTIVE_EMPLOYEES WHERE DEPT_NAME = 'Engineering'",
        sql_type="SELECT",
        tables=[{"name": "V_ACTIVE_EMPLOYEES", "schema": None, "alias": None}],
        where_columns=["DEPT_NAME"],
    )
