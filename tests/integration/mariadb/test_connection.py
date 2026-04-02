"""Testes de integração MariaDB: conexão, versão, privilégios."""

import pytest

pytestmark = pytest.mark.mariadb


class TestConnection:
    """Valida que a conexão básica funciona contra MariaDB real."""

    def test_version_contains_mariadb(self, mariadb_conn):
        """VERSION() retorna string contendo 'MariaDB'."""
        cursor = mariadb_conn.cursor()
        cursor.execute("SELECT VERSION()")
        row = cursor.fetchone()
        cursor.close()
        assert row is not None
        assert "MariaDB" in row[0]

    def test_database_matches_expected(self, mariadb_conn):
        """DATABASE() retorna o banco esperado."""
        cursor = mariadb_conn.cursor()
        cursor.execute("SELECT DATABASE()")
        row = cursor.fetchone()
        cursor.close()
        assert row is not None
        assert row[0] == "SQLMENTOR_TEST"

    def test_validate_privileges_no_dangerous(self, mariadb_conn, mariadb_adapter):
        """validate_privileges() não retorna privilégios perigosos."""
        result = mariadb_adapter.validate_privileges(mariadb_conn)
        assert result["dangerous_privileges"] == [], (
            f"Privilégios perigosos: {result['dangerous_privileges']}"
        )

    def test_optimizer_params_returns_data(self, mariadb_conn, mariadb_adapter):
        """optimizer_params() retorna dados de information_schema.GLOBAL_VARIABLES."""
        cursor = mariadb_conn.cursor()
        qb = mariadb_adapter.query_builder
        sql, params = qb.optimizer_params()
        rows = mariadb_adapter.execute_query(cursor, sql, params)
        cursor.close()
        assert len(rows) > 0
        param_names = {r["name"] for r in rows}
        assert "OPTIMIZER_SWITCH" in param_names

    def test_no_dangerous_privileges_query(self, mariadb_conn, mariadb_adapter):
        """Confirma que queries de privilégios perigosos retornam vazio."""
        cursor = mariadb_conn.cursor()
        qb = mariadb_adapter.query_builder

        sql, params = qb.dangerous_privileges()
        cursor.execute(sql, params or None)
        bad_privs = [row[0] for row in cursor]

        sql, params = qb.dangerous_roles()
        cursor.execute(sql, params or None)
        bad_roles = [row[0] for row in cursor]

        cursor.close()

        assert bad_privs == [], f"Privilégios perigosos encontrados: {bad_privs}"
        assert bad_roles == [], f"Roles perigosas encontradas: {bad_roles}"
