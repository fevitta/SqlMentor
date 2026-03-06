"""Testes de integração: conexão, versão, privilégios."""

import pytest

from sqlmentor.collector import _execute_query
from sqlmentor.connector import validate_privileges
from sqlmentor.queries import db_version, optimizer_params

pytestmark = pytest.mark.oracle


class TestConnection:
    """Valida que a conexão básica funciona contra Oracle real."""

    def test_connect_returns_version(self, oracle_conn):
        """V$VERSION retorna banner contendo 'Oracle'."""
        cursor = oracle_conn.cursor()
        sql, params = db_version()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        cursor.close()
        assert row is not None
        assert "Oracle" in row[0]

    def test_schema_matches_expected(self, oracle_conn, oracle_schema):
        """SYS_CONTEXT retorna o schema esperado."""
        cursor = oracle_conn.cursor()
        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
        row = cursor.fetchone()
        cursor.close()
        assert row is not None
        assert row[0] == oracle_schema

    def test_validate_privileges_passes(self, oracle_conn):
        """validate_privileges() não levanta PermissionError para nosso user."""
        # Não deve levantar exceção — user tem apenas privilégios de leitura
        validate_privileges(oracle_conn)

    def test_optimizer_params_returns_data(self, oracle_conn):
        """optimizer_params() retorna dict não-vazio de V$PARAMETER."""
        cursor = oracle_conn.cursor()
        sql, params = optimizer_params()
        rows = _execute_query(cursor, sql, params)
        cursor.close()
        assert len(rows) > 0
        # Verifica que parâmetros conhecidos estão presentes
        param_names = {r["name"] for r in rows}
        assert "optimizer_mode" in param_names

    def test_session_privs_no_dangerous(self, oracle_conn):
        """Confirma que SESSION_PRIVS não tem privilégios de escrita/DDL."""
        from sqlmentor.queries import dangerous_privileges, dangerous_roles

        cursor = oracle_conn.cursor()

        sql, params = dangerous_privileges()
        cursor.execute(sql, params)
        bad_privs = [row[0] for row in cursor]

        sql, params = dangerous_roles()
        cursor.execute(sql, params)
        bad_roles = [row[0] for row in cursor]

        cursor.close()

        assert bad_privs == [], f"Privilégios perigosos encontrados: {bad_privs}"
        assert bad_roles == [], f"Roles perigosas encontradas: {bad_roles}"


class TestConnectorFlow:
    """Testa o fluxo completo do connector (add → connect → close)."""

    def test_connector_connect_end_to_end(self, tmp_connections_file):
        """Cria profile temporário, conecta via connector.connect(), valida."""
        from sqlmentor.connector import add_connection, connect
        from tests.integration.conftest import (
            ORACLE_HOST,
            ORACLE_PASSWORD,
            ORACLE_PORT,
            ORACLE_SCHEMA,
            ORACLE_SERVICE,
            ORACLE_USER,
        )

        add_connection(
            name="integration-test",
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service=ORACLE_SERVICE,
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            schema=ORACLE_SCHEMA,
        )

        conn = connect("integration-test")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            row = cursor.fetchone()
            cursor.close()
            assert row == (1,)
        finally:
            conn.close()
