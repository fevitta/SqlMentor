"""Testes para get-sql (CLI) e get_sql_text (MCP) — T18.

Verifica:
- Oracle: recupera SQL via V$SQL, salva em arquivo
- MariaDB: tenta sql_text_original primeiro, fallback DIGEST_TEXT com warning
- Not found: exit 1 com mensagem clara
- MariaDB consumer diagnostic quando not found
- Erros de conexão
"""

import json
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

pymysql = pytest.importorskip("pymysql", reason="PyMySQL não instalado")

from sqlmentor.cli import app

runner = CliRunner()


# ─── Helpers ──────────────────────────────────────────────────────────


def _get_sql_patches(monkeypatch, *, dialect="oracle"):
    """Configura patches para get-sql."""
    monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
    monkeypatch.setattr(
        "sqlmentor.connector.get_connection_config",
        lambda name: {"type": dialect, "user": "root", "timeout": 600},
    )

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_adapter = MagicMock()
    mock_adapter.db_type = dialect
    mock_qb = MagicMock()
    mock_adapter.query_builder = mock_qb

    mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc123"})
    mock_qb.sql_text_original.return_value = ("SELECT SQL_TEXT ...", {"digest": "abc123"})
    mock_qb.setup_consumers.return_value = ("SELECT ...", {})

    monkeypatch.setattr(
        "sqlmentor.connector.connect_with_adapter",
        MagicMock(return_value=(mock_adapter, mock_conn)),
    )

    return {"cursor": mock_cursor, "conn": mock_conn, "adapter": mock_adapter, "qb": mock_qb}


# ─── TestGetSqlCLI ────────────────────────────────────────────────────


class TestGetSqlCLI:
    """CLI get-sql: recupera SQL de statement já executado."""

    def test_oracle_outputs_sql(self, monkeypatch):
        mocks = _get_sql_patches(monkeypatch, dialect="oracle")
        # V$SQL retorna LOB-like object
        mock_lob = MagicMock()
        mock_lob.read.return_value = "SELECT * FROM orders WHERE id = 1"
        mocks["cursor"].fetchone.return_value = (mock_lob,)

        result = runner.invoke(app, ["get-sql", "abc123", "--conn", "test"])

        assert result.exit_code == 0
        assert "SELECT * FROM orders WHERE id = 1" in result.output
        mocks["qb"].sql_text_by_id.assert_called_once_with("abc123")

    def test_oracle_saves_to_file(self, monkeypatch, tmp_path):
        mocks = _get_sql_patches(monkeypatch, dialect="oracle")
        mocks["cursor"].fetchone.return_value = ("SELECT 1 FROM dual",)

        out_file = tmp_path / "output.sql"
        result = runner.invoke(
            app, ["get-sql", "abc123", "--conn", "test", "--output", str(out_file)]
        )

        assert result.exit_code == 0
        assert out_file.exists()
        assert out_file.read_text() == "SELECT 1 FROM dual"

    def test_mariadb_tries_original_first(self, monkeypatch):
        mocks = _get_sql_patches(monkeypatch, dialect="mariadb")
        # sql_text_original returns result on first call
        mocks["cursor"].fetchone.return_value = ("SELECT * FROM orders WHERE id = 1",)

        result = runner.invoke(app, ["get-sql", "abc123", "--conn", "test"])

        assert result.exit_code == 0
        assert "SELECT * FROM orders WHERE id = 1" in result.output
        mocks["qb"].sql_text_original.assert_called_once_with("abc123")

    def test_mariadb_fallback_digest_warning(self, monkeypatch):
        mocks = _get_sql_patches(monkeypatch, dialect="mariadb")
        call_count = [0]

        def mock_fetchone():
            call_count[0] += 1
            if call_count[0] == 1:  # sql_text_original → None
                return None
            if call_count[0] == 2:  # sql_text_by_id → normalized
                return ("SELECT * FROM ? WHERE ? = ?",)
            return None

        mocks["cursor"].fetchone = mock_fetchone

        result = runner.invoke(app, ["get-sql", "abc123", "--conn", "test"])

        assert result.exit_code == 0
        assert "SELECT * FROM ? WHERE ? = ?" in result.output
        # Warning sobre normalização vai para stderr (captured together pelo CliRunner)
        assert "normalizado" in result.output.lower() or "DIGEST_TEXT" in result.output

    def test_not_found_exits_1(self, monkeypatch):
        mocks = _get_sql_patches(monkeypatch, dialect="oracle")
        mocks["cursor"].fetchone.return_value = None

        result = runner.invoke(app, ["get-sql", "abc123", "--conn", "test"])

        assert result.exit_code == 1
        assert "não encontrado" in result.output

    def test_mariadb_consumer_diagnostic(self, monkeypatch):
        mocks = _get_sql_patches(monkeypatch, dialect="mariadb")
        # All fetchone return None (not found)
        mocks["cursor"].fetchone.return_value = None
        # setup_consumers returns consumers OFF
        mocks["cursor"].__iter__ = MagicMock(
            return_value=iter(
                [
                    ("statements_digest", "NO"),
                    ("events_statements_history", "YES"),
                    ("events_statements_history_long", "YES"),
                ]
            )
        )

        result = runner.invoke(app, ["get-sql", "abc123", "--conn", "test"])

        assert result.exit_code == 1
        assert "statements_digest" in result.output

    def test_connection_error(self, monkeypatch):
        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "oracle", "user": "root", "timeout": 600},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(side_effect=ConnectionError("refused")),
        )

        result = runner.invoke(app, ["get-sql", "abc123", "--conn", "test"])

        assert result.exit_code == 1
        assert "conexão" in result.output.lower() or "refused" in result.output


# ─── TestGetSqlMCP ────────────────────────────────────────────────────


class TestGetSqlMCP:
    """MCP get_sql_text: retorna JSON com sql_text e source."""

    def test_oracle_returns_json(self, monkeypatch):
        from sqlmentor.mcp_server import get_sql_text

        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "oracle", "user": "root", "timeout": 600},
        )

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("SELECT 1 FROM dual",)

        mock_adapter = MagicMock()
        mock_adapter.db_type = "oracle"
        mock_qb = MagicMock()
        mock_adapter.query_builder = mock_qb
        mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc123"})

        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(return_value=(mock_adapter, mock_conn)),
        )

        result = json.loads(get_sql_text("abc123", conn="test"))

        assert result["sql_text"] == "SELECT 1 FROM dual"
        assert result["source"] == "v$sql"
        assert result["statement_id"] == "abc123"
        assert result["char_count"] == len("SELECT 1 FROM dual")

    def test_mariadb_original(self, monkeypatch):
        from sqlmentor.mcp_server import get_sql_text

        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "mariadb", "user": "root", "timeout": 600},
        )

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # sql_text_original returns result
        mock_cursor.fetchone.return_value = ("SELECT * FROM orders WHERE id = 1",)

        mock_adapter = MagicMock()
        mock_adapter.db_type = "mariadb"
        mock_qb = MagicMock()
        mock_adapter.query_builder = mock_qb
        mock_qb.sql_text_original.return_value = ("SELECT SQL_TEXT ...", {"digest": "abc"})
        mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc"})

        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(return_value=(mock_adapter, mock_conn)),
        )

        result = json.loads(get_sql_text("abc123", conn="test"))

        assert result["source"] == "original"
        assert "SELECT * FROM orders" in result["sql_text"]

    def test_mariadb_normalized(self, monkeypatch):
        from sqlmentor.mcp_server import get_sql_text

        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "mariadb", "user": "root", "timeout": 600},
        )

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        call_count = [0]

        def mock_fetchone():
            call_count[0] += 1
            if call_count[0] == 1:  # sql_text_original → None
                return None
            if call_count[0] == 2:  # sql_text_by_id → normalized
                return ("SELECT * FROM ? WHERE ? = ?",)
            return None

        mock_cursor.fetchone = mock_fetchone

        mock_adapter = MagicMock()
        mock_adapter.db_type = "mariadb"
        mock_qb = MagicMock()
        mock_adapter.query_builder = mock_qb
        mock_qb.sql_text_original.return_value = ("SELECT ...", {"digest": "abc"})
        mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc"})

        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(return_value=(mock_adapter, mock_conn)),
        )

        result = json.loads(get_sql_text("abc123", conn="test"))

        assert result["source"] == "normalized"

    def test_not_found_error(self, monkeypatch):
        from sqlmentor.mcp_server import get_sql_text

        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "oracle", "user": "root", "timeout": 600},
        )

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        mock_adapter = MagicMock()
        mock_adapter.db_type = "oracle"
        mock_qb = MagicMock()
        mock_adapter.query_builder = mock_qb
        mock_qb.sql_text_by_id.return_value = ("SELECT ...", {"sql_id": "abc"})

        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(return_value=(mock_adapter, mock_conn)),
        )

        result = json.loads(get_sql_text("abc123", conn="test"))

        assert "error" in result
        assert "não encontrado" in result["error"]

    def test_connection_error(self, monkeypatch):
        from sqlmentor.mcp_server import get_sql_text

        monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
        monkeypatch.setattr(
            "sqlmentor.connector.get_connection_config",
            lambda name: {"type": "oracle", "user": "root", "timeout": 600},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.connect_with_adapter",
            MagicMock(side_effect=ConnectionError("refused")),
        )

        result = json.loads(get_sql_text("abc123", conn="test"))

        assert "error" in result
        assert "refused" in result["error"]
