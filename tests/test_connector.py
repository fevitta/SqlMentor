"""Testes para o módulo connector (CRUD de conexões, validação de privilégios)."""

from unittest.mock import MagicMock, patch

import oracledb
import pytest

import sqlmentor.adapters.oracle as oracle_mod
from sqlmentor.connector import (
    _validate_db_type,
    add_connection,
    connect,
    diagnose_connection,
    get_connection_config,
    get_default_connection,
    list_connections,
    remove_connection,
    resolve_connection,
    set_default_connection,
    validate_privileges,
)
from sqlmentor.connector import test_connection as _test_connection

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

    def test_add_default_type_is_oracle(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        cfg = get_connection_config("dev")
        assert cfg["type"] == "oracle"

    def test_add_with_explicit_type(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", db_type="oracle")
        cfg = get_connection_config("dev")
        assert cfg["type"] == "oracle"

    def test_add_invalid_type_raises(self, tmp_connections_file):
        with pytest.raises(ValueError, match="não suportado"):
            add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", db_type="redis")

    def test_add_type_case_insensitive(self, tmp_connections_file):
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", db_type="ORACLE")
        cfg = get_connection_config("dev")
        assert cfg["type"] == "oracle"


# ─── db_type validation ──────────────────────────────────────────────────────


class TestDbTypeValidation:
    def test_validate_oracle(self):
        assert _validate_db_type("oracle") == "oracle"

    def test_validate_case_insensitive(self):
        assert _validate_db_type("ORACLE") == "oracle"
        assert _validate_db_type("Oracle") == "oracle"

    def test_validate_with_whitespace(self):
        assert _validate_db_type("  oracle  ") == "oracle"

    def test_validate_invalid_raises(self):
        with pytest.raises(ValueError, match="não suportado"):
            _validate_db_type("redis")

    def test_validate_error_lists_supported(self):
        with pytest.raises(ValueError, match="oracle"):
            _validate_db_type("nosql")


# ─── backward compat (profiles sem type) ────────────────────────────────────


class TestBackwardCompat:
    def test_profile_without_type_gets_oracle(self, tmp_connections_file):
        """Profile salvo sem campo 'type' recebe default 'oracle' ao carregar."""
        import yaml

        # Escreve profile sem 'type' diretamente no YAML
        connections = {
            "legacy": {
                "host": "legacyhost",
                "port": 1521,
                "service": "ORCL",
                "user": "scott",
                "password": "tiger",
                "schema": "SCOTT",
                "timeout": 180,
            }
        }
        with open(tmp_connections_file, "w") as f:
            yaml.dump(connections, f)

        cfg = get_connection_config("legacy")
        assert cfg["type"] == "oracle"

    def test_list_shows_type(self, tmp_connections_file):
        """list_connections inclui campo type."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        result = list_connections()
        assert result["dev"]["type"] == "oracle"

    def test_list_legacy_profile_shows_oracle(self, tmp_connections_file):
        """Profile sem type no YAML aparece como 'oracle' no list."""
        import yaml

        connections = {
            "legacy": {
                "host": "legacyhost",
                "port": 1521,
                "service": "ORCL",
                "user": "scott",
                "password": "tiger",
                "schema": "SCOTT",
                "timeout": 180,
            }
        }
        with open(tmp_connections_file, "w") as f:
            yaml.dump(connections, f)

        result = list_connections()
        assert result["legacy"]["type"] == "oracle"


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


# ─── connect ────────────────────────────────────────────────────────────────


class TestConnect:
    def test_thin_mode_success(self, tmp_connections_file):
        """Conexão thin funciona, timeout setado, validate_privileges chamado."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", timeout=60)
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn_string"),
            patch("sqlmentor.connector.validate_privileges"),
        ):
            result = connect("dev")
            assert result is mock_conn
            assert mock_conn.call_timeout == 60_000

    def test_dpy3010_fallback_thick(self, tmp_connections_file):
        """DPY-3010 na 1ª tentativa → thick mode → reconecta."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        err = oracledb.DatabaseError("DPY-3010: connections to older DB not supported")
        with (
            patch.object(oracledb, "connect", side_effect=[err, mock_conn]),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch("sqlmentor.adapters.oracle._init_thick_mode_if_available") as mock_thick,
            patch("sqlmentor.connector.validate_privileges"),
        ):
            result = connect("dev")
            assert result is mock_conn
            mock_thick.assert_called_once()

    def test_other_db_error_raises(self, tmp_connections_file):
        """Erro não-DPY-3010 → re-raise direto."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        err = oracledb.DatabaseError("ORA-12154: TNS error")
        with (
            patch.object(oracledb, "connect", side_effect=err),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            pytest.raises(oracledb.DatabaseError, match="ORA-12154"),
        ):
            connect("dev")

    def test_privilege_error_closes_conn(self, tmp_connections_file):
        """validate_privileges → PermissionError → conn.close() + re-raise."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch("sqlmentor.connector.validate_privileges", side_effect=PermissionError("bad")),
        ):
            with pytest.raises(PermissionError, match="bad"):
                connect("dev")
            mock_conn.close.assert_called_once()

    def test_timeout_from_config(self, tmp_connections_file):
        """timeout=None usa valor do profile."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger", timeout=300)
        mock_conn = MagicMock()
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch("sqlmentor.connector.validate_privileges"),
        ):
            connect("dev", timeout=None)
            assert mock_conn.call_timeout == 300_000

    def test_non_oracle_type_raises_value_error(self, tmp_connections_file):
        """Profile com type não suportado levanta ValueError via get_adapter."""
        import yaml

        # Cria profile manualmente com type 'postgresql'
        connections = {
            "pg": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "service": "mydb",
                "user": "admin",
                "password": "secret",
                "schema": "PUBLIC",
                "timeout": 180,
            }
        }
        with open(tmp_connections_file, "w") as f:
            yaml.dump(connections, f)

        with pytest.raises(ValueError, match="não suportado"):
            connect("pg")


# ─── check_thick_mode_available ─────────────────────────────────────────────


class TestCheckThickMode:
    def test_already_initialized(self):
        """Flag True → retorna cached."""
        from sqlmentor.connector import check_thick_mode_available

        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = True
            result = check_thick_mode_available()
            assert result["available"] == "True"
            assert "já" in result["detail"].lower()
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_client_found(self):
        """init_oracle_client OK → available=True."""
        from sqlmentor.connector import check_thick_mode_available

        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = False
            with patch.object(oracledb, "init_oracle_client"):
                result = check_thick_mode_available()
                assert result["available"] == "True"
        finally:
            oracle_mod._thick_mode_initialized = original

    def test_client_not_found(self):
        """Exception → available=False."""
        from sqlmentor.connector import check_thick_mode_available

        original = oracle_mod._thick_mode_initialized
        try:
            oracle_mod._thick_mode_initialized = False
            with patch.object(oracledb, "init_oracle_client", side_effect=Exception("not found")):
                result = check_thick_mode_available()
                assert result["available"] == "False"
        finally:
            oracle_mod._thick_mode_initialized = original


# ─── test_connection ────────────────────────────────────────────────────────


class TestTestConnection:
    def test_success(self, tmp_connections_file):
        """Mock cursor retorna version + schema → dict correto."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [("Oracle 19c",), ("HR",)]

        with patch("sqlmentor.connector.connect", return_value=mock_conn):
            result = _test_connection("dev")
            assert result["status"] == "ok"
            assert result["version"] == "Oracle 19c"
            assert result["schema"] == "HR"
            mock_conn.close.assert_called_once()

    def test_conn_always_closed(self, tmp_connections_file):
        """Mesmo com erro no cursor, conn.close() chamado."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = RuntimeError("DB error")

        with patch("sqlmentor.connector.connect", return_value=mock_conn):
            with pytest.raises(RuntimeError, match="DB error"):
                _test_connection("dev")
            mock_conn.close.assert_called_once()

    def test_connect_called_with_name(self, tmp_connections_file):
        """Verifica que connect() é chamado com o nome certo."""
        add_connection("prod", "prodhost", 1521, "PROD", "app", "pass")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [("Oracle 19c",), ("PROD",)]

        with patch("sqlmentor.connector.connect", return_value=mock_conn) as mock_connect:
            _test_connection("prod")
            mock_connect.assert_called_once_with("prod")

    def test_non_oracle_type_raises_value_error(self, tmp_connections_file):
        """Profile com type não suportado levanta ValueError via get_adapter."""
        import yaml

        connections = {
            "pg": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "service": "mydb",
                "user": "admin",
                "password": "secret",
                "schema": "PUBLIC",
                "timeout": 180,
            }
        }
        with open(tmp_connections_file, "w") as f:
            yaml.dump(connections, f)

        with pytest.raises(ValueError, match="não suportado"):
            _test_connection("pg")


# ─── diagnose_connection ────────────────────────────────────────────────────


class TestDiagnoseConnection:
    def test_thin_mode_success(self, tmp_connections_file):
        """Retorna mode=thin, needs_thick=False, major_version extraído."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [
            ("Oracle Database 19c Enterprise Edition",),
            ("HR",),
        ]
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch.object(oracledb, "is_thin_mode", return_value=True),
        ):
            result = diagnose_connection("dev")
            assert result["status"] == "ok"
            assert result["mode"] == "thin"
            assert result["needs_thick"] == "False"
            assert result["major_version"] == "19"
            assert result["schema"] == "HR"
            mock_conn.close.assert_called_once()

    def test_dpy3010_fallback_thick(self, tmp_connections_file):
        """DPY-3010 → thick mode → mode=thick, needs_thick=True."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [
            ("Oracle Database 11g Release 11.2.0.4.0",),
            ("SCOTT",),
        ]
        err = oracledb.DatabaseError("DPY-3010: connections to older DB not supported")
        with (
            patch.object(oracledb, "connect", side_effect=[err, mock_conn]),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            patch("sqlmentor.adapters.oracle._init_thick_mode_if_available"),
            patch.object(oracledb, "is_thin_mode", return_value=False),
        ):
            result = diagnose_connection("dev")
            assert result["mode"] == "thick"
            assert result["needs_thick"] == "True"
            assert result["major_version"] == "11"

    def test_other_error_raises(self, tmp_connections_file):
        """Erro não-DPY-3010 → re-raise."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        err = oracledb.DatabaseError("ORA-12154: TNS error")
        with (
            patch.object(oracledb, "connect", side_effect=err),
            patch.object(oracledb, "makedsn", return_value="dsn"),
            pytest.raises(oracledb.DatabaseError, match="ORA-12154"),
        ):
            diagnose_connection("dev")

    def test_conn_always_closed(self, tmp_connections_file):
        """finally garante close mesmo com erro no cursor."""
        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = RuntimeError("query failed")
        with (
            patch.object(oracledb, "connect", return_value=mock_conn),
            patch.object(oracledb, "makedsn", return_value="dsn"),
        ):
            with pytest.raises(RuntimeError, match="query failed"):
                diagnose_connection("dev")
            mock_conn.close.assert_called_once()

    def test_non_oracle_type_raises_value_error(self, tmp_connections_file):
        """Profile com type não suportado levanta ValueError via get_adapter."""
        import yaml

        connections = {
            "pg": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "service": "mydb",
                "user": "admin",
                "password": "secret",
                "schema": "PUBLIC",
                "timeout": 180,
            }
        }
        with open(tmp_connections_file, "w") as f:
            yaml.dump(connections, f)

        with pytest.raises(ValueError, match="não suportado"):
            diagnose_connection("pg")
