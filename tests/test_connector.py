"""Testes para o módulo connector (CRUD de conexões, validação de privilégios)."""

from unittest.mock import MagicMock

import pytest

from sqlmentor.connector import (
    add_connection,
    get_connection_config,
    get_default_connection,
    list_connections,
    remove_connection,
    resolve_connection,
    set_default_connection,
    validate_privileges,
)

# ─── CRUD de conexões ────────────────────────────────────────────────────────


class TestConnectionCRUD:
    def test_add_and_get(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        cfg = get_connection_config("dev")
        assert cfg["host"] == "localhost"
        assert cfg["port"] == 1521
        assert cfg["service"] == "ORCL"
        assert cfg["user"] == "scott"
        assert cfg["password"] == "tiger"

    def test_add_with_schema(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", schema="HR")
        cfg = get_connection_config("dev")
        assert cfg["schema"] == "HR"

    def test_add_default_schema_is_user_upper(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        cfg = get_connection_config("dev")
        assert cfg["schema"] == "SCOTT"

    def test_add_with_timeout(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", timeout=300)
        cfg = get_connection_config("dev")
        assert cfg["timeout"] == 300

    def test_add_default_timeout(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        cfg = get_connection_config("dev")
        assert cfg["timeout"] == 180

    def test_update_existing(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        add_connection("dev", "newhost", 1522, "NEWORCL", "admin", "pass")
        cfg = get_connection_config("dev")
        assert cfg["host"] == "newhost"
        assert cfg["port"] == 1522

    def test_remove_existing(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        assert remove_connection("dev") is True
        with pytest.raises(ValueError, match="não encontrada"):
            get_connection_config("dev")

    def test_remove_nonexistent(self, tmp_connections_file):
        assert remove_connection("nonexistent") is False

    def test_get_nonexistent_raises(self, tmp_connections_file):
        with pytest.raises(ValueError, match="não encontrada"):
            get_connection_config("nonexistent")


# ─── list_connections ─────────────────────────────────────────────────────────


class TestListConnections:
    def test_empty(self, tmp_connections_file):
        result = list_connections()
        assert result == {}

    def test_masks_password(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "secret123")
        result = list_connections()
        assert result["dev"]["password"] == "****"
        assert "secret123" not in str(result)

    def test_multiple_connections(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        add_connection("prod", "prodhost", 1521, "PROD", "app", "pass")
        result = list_connections()
        assert "dev" in result
        assert "prod" in result


# ─── Default connection ──────────────────────────────────────────────────────


class TestDefaultConnection:
    def test_no_default(self, tmp_connections_file):
        assert get_default_connection() is None

    def test_set_and_get_default(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        assert get_default_connection() == "dev"

    def test_change_default(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        add_connection("prod", "prodhost", 1521, "PROD", "app", "pass")
        set_default_connection("dev")
        set_default_connection("prod")
        assert get_default_connection() == "prod"

    def test_set_default_nonexistent_raises(self, tmp_connections_file):
        with pytest.raises(ValueError, match="não encontrada"):
            set_default_connection("nonexistent")


# ─── resolve_connection ──────────────────────────────────────────────────────


class TestResolveConnection:
    def test_explicit_connection(self, tmp_connections_file):
        assert resolve_connection("myconn") == "myconn"

    def test_default_when_none(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        assert resolve_connection(None) == "dev"

    def test_error_when_no_default(self, tmp_connections_file):
        with pytest.raises(ValueError, match="Nenhuma conexão"):
            resolve_connection(None)


# ─── validate_privileges ─────────────────────────────────────────────────────


class TestValidatePrivileges:
    def test_clean_user_passes(self):
        """Usuário sem privilégios perigosos deve passar sem erro."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        # Simula: nenhum privilégio perigoso e nenhuma role perigosa
        cursor.execute.return_value = None
        cursor.__iter__ = MagicMock(side_effect=[iter([]), iter([])])
        cursor.fetchall.return_value = []

        # Não deve levantar exceção
        validate_privileges(conn)

    def test_dangerous_privilege_raises(self):
        """Usuário com INSERT ANY TABLE deve ser rejeitado."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        conn.username = "BAD_USER"

        call_count = [0]

        def fake_iter():
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([("INSERT ANY TABLE",)])  # Privilegio perigoso
            return iter([])  # Nenhuma role perigosa

        cursor.__iter__ = MagicMock(side_effect=fake_iter)

        with pytest.raises(PermissionError, match="INSERT ANY TABLE"):
            validate_privileges(conn)

    def test_dangerous_role_raises(self):
        """Usuário com role DBA deve ser rejeitado."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        conn.username = "DBA_USER"

        call_count = [0]

        def fake_iter():
            call_count[0] += 1
            if call_count[0] == 1:
                return iter([])  # Nenhum privilegio perigoso
            return iter([("DBA",)])  # Role perigosa

        cursor.__iter__ = MagicMock(side_effect=fake_iter)

        with pytest.raises(PermissionError, match="DBA"):
            validate_privileges(conn)

    def test_cursor_always_closed(self):
        """Cursor deve ser fechado mesmo em caso de erro."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            validate_privileges(conn)

        cursor.close.assert_called_once()
