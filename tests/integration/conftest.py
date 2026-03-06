"""Fixtures para testes de integração com Oracle real.

Conexão via env vars com defaults matching docker-compose.yml.
Testes são pulados automaticamente quando Oracle não está acessível.
"""

import os
import subprocess

import pytest

# Marca todos os testes deste diretório como @pytest.mark.oracle
pytestmark = pytest.mark.oracle

# -- Configuração via env vars (defaults = docker-compose) -------------------

ORACLE_HOST = os.environ.get("SQLMENTOR_TEST_HOST", "localhost")
ORACLE_PORT = int(os.environ.get("SQLMENTOR_TEST_PORT", "1521"))
ORACLE_SERVICE = os.environ.get("SQLMENTOR_TEST_SERVICE", "XEPDB1")
ORACLE_USER = os.environ.get("SQLMENTOR_TEST_USER", "SQLMENTOR_TEST")
ORACLE_PASSWORD = os.environ.get("SQLMENTOR_TEST_PASSWORD", "TestPwd123")
ORACLE_SCHEMA = os.environ.get("SQLMENTOR_TEST_SCHEMA", "SQLMENTOR_TEST")


def _try_connect():
    """Tenta conectar ao Oracle. Retorna (connection, None) ou (None, reason)."""
    try:
        import oracledb

        dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
        return conn, None
    except Exception as e:
        return None, str(e)


# -- Fixtures session-scoped -------------------------------------------------


@pytest.fixture(scope="session")
def oracle_conn():
    """Conexão Oracle session-scoped. Pula todos os testes se Oracle indisponível."""
    conn, reason = _try_connect()
    if conn is None:
        pytest.skip(f"Oracle não disponível: {reason}")
    yield conn
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def _docker_cleanup():
    """Encerra o container Oracle após todos os testes de integração."""
    yield
    compose_file = os.path.join(os.path.dirname(__file__), "..", "..", "docker-compose.yml")
    if os.path.exists(compose_file):
        subprocess.run(
            ["docker", "compose", "-f", compose_file, "down", "--timeout", "10"],
            capture_output=True,
        )


@pytest.fixture(scope="session")
def oracle_schema():
    """Nome do schema de teste."""
    return ORACLE_SCHEMA


# -- Fixtures function-scoped ------------------------------------------------


@pytest.fixture
def oracle_cursor(oracle_conn):
    """Cursor fresco por teste (function-scoped para isolamento)."""
    cursor = oracle_conn.cursor()
    yield cursor
    cursor.close()


# -- Fixtures para testes de inspect (V$SQL) ---------------------------------

_SEED_SQL = (
    "SELECT /* SQLMENTOR_SEED_QUERY */ e.emp_id, e.first_name, "
    "o.order_id, o.total "
    "FROM SQLMENTOR_TEST.EMPLOYEES e "
    "JOIN SQLMENTOR_TEST.ORDERS o ON e.emp_id = o.emp_id "
    "WHERE e.dept_id = 10 AND o.status = 'COMPLETED' "
    "ORDER BY o.total DESC"
)


@pytest.fixture(scope="session")
def seed_query_sql_id(oracle_conn):
    """Encontra ou cria entrada em V$SQL para testes de inspect.

    Executa a seed query com STATISTICS_LEVEL=ALL para garantir que
    ALLSTATS LAST tenha dados reais (A-Rows, Buffers, etc.).
    """
    cursor = oracle_conn.cursor()

    # Executa com stats para garantir ALLSTATS LAST funcional
    cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = ALL")
    cursor.execute(_SEED_SQL)
    cursor.fetchall()
    cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = TYPICAL")

    # Pega sql_id da query que acabou de rodar
    cursor.execute("SELECT prev_sql_id FROM v$session WHERE sid = SYS_CONTEXT('USERENV', 'SID')")
    row = cursor.fetchone()
    cursor.close()

    if row is None or row[0] is None:
        pytest.skip("Não foi possível obter sql_id da seed query")
    return row[0]


# -- Fixtures de ParsedSQL pré-montadas --------------------------------------


@pytest.fixture(scope="session")
def parsed_employees_orders():
    """ParsedSQL para JOIN employees/orders (sem schema qualifier)."""
    from sqlmentor.parser import ParsedSQL

    return ParsedSQL(
        raw_sql=(
            "SELECT e.emp_id, e.first_name, o.order_id, o.total "
            "FROM EMPLOYEES e JOIN ORDERS o ON e.emp_id = o.emp_id "
            "WHERE e.dept_id = 10 AND o.status = 'COMPLETED'"
        ),
        sql_type="SELECT",
        tables=[
            {"name": "EMPLOYEES", "schema": None, "alias": "e"},
            {"name": "ORDERS", "schema": None, "alias": "o"},
        ],
        where_columns=["dept_id", "status"],
        join_columns=["emp_id"],
    )


@pytest.fixture(scope="session")
def parsed_single_table():
    """ParsedSQL para SELECT simples na tabela EMPLOYEES."""
    from sqlmentor.parser import ParsedSQL

    return ParsedSQL(
        raw_sql="SELECT emp_id, first_name, salary FROM EMPLOYEES WHERE emp_id = 1",
        sql_type="SELECT",
        tables=[{"name": "EMPLOYEES", "schema": None, "alias": None}],
        where_columns=["emp_id"],
    )


@pytest.fixture(scope="session")
def parsed_view_query():
    """ParsedSQL para SELECT na view V_ACTIVE_EMPLOYEES."""
    from sqlmentor.parser import ParsedSQL

    return ParsedSQL(
        raw_sql="SELECT emp_id, first_name, dept_name FROM V_ACTIVE_EMPLOYEES WHERE dept_name = 'Engineering'",
        sql_type="SELECT",
        tables=[{"name": "V_ACTIVE_EMPLOYEES", "schema": None, "alias": None}],
        where_columns=["dept_name"],
    )
