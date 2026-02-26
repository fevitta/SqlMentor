"""Testes para o CLI (Typer) do sqlmentor."""

from typer.testing import CliRunner

from sqlmentor.cli import app

runner = CliRunner()


# ─── parse command ────────────────────────────────────────────────────────────


class TestParseCLI:
    def test_parse_file(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id, name FROM users WHERE status = 'ACTIVE'")
        result = runner.invoke(app, ["parse", str(sql_file)])
        assert result.exit_code == 0
        assert "SELECT" in result.output
        assert "USERS" in result.output.upper()

    def test_parse_sql_inline(self):
        result = runner.invoke(app, ["parse", "--sql", "SELECT * FROM orders"])
        assert result.exit_code == 0
        assert "ORDERS" in result.output.upper()

    def test_parse_both_file_and_sql_errors(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1 FROM DUAL")
        result = runner.invoke(app, ["parse", str(sql_file), "--sql", "SELECT 1"])
        assert result.exit_code == 1

    def test_parse_no_input_errors(self):
        result = runner.invoke(app, ["parse"])
        assert result.exit_code == 1

    def test_parse_missing_file(self):
        result = runner.invoke(app, ["parse", "/nonexistent/query.sql"])
        assert result.exit_code == 1
        assert "não encontrado" in result.output.lower() or "not found" in result.output.lower()

    def test_parse_empty_file(self, tmp_path):
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")
        result = runner.invoke(app, ["parse", str(sql_file)])
        assert result.exit_code == 1

    def test_parse_with_schema(self, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM employees")
        result = runner.invoke(app, ["parse", str(sql_file), "--schema", "HR"])
        assert result.exit_code == 0

    def test_parse_normalized_flag(self):
        result = runner.invoke(
            app,
            ["parse", "--sql", "SELECT * FROM t WHERE a = ? AND b = ?", "--normalized"],
        )
        assert result.exit_code == 0

    def test_parse_shows_columns(self):
        result = runner.invoke(
            app,
            ["parse", "--sql", "SELECT * FROM users WHERE id = 1 ORDER BY name"],
        )
        assert result.exit_code == 0
        # Output should contain WHERE and ORDER BY info
        assert "WHERE" in result.output or "where" in result.output.lower()


# ─── config commands ──────────────────────────────────────────────────────────


class TestConfigCLI:
    def test_config_list_empty(self, tmp_connections_file):
        result = runner.invoke(app, ["config", "list"])
        assert result.exit_code == 0
        assert "Nenhuma" in result.output or "conexão" in result.output.lower()

    def test_config_add_and_list(self, tmp_connections_file):
        result = runner.invoke(
            app,
            [
                "config",
                "add",
                "--name",
                "test",
                "--host",
                "localhost",
                "--port",
                "1521",
                "--service",
                "ORCL",
                "--user",
                "scott",
            ],
            input="tiger\n",  # Password prompt
        )
        # May fail on connection validation but should save
        assert "salva" in result.output.lower() or result.exit_code == 0

    def test_config_remove_nonexistent(self, tmp_connections_file):
        result = runner.invoke(app, ["config", "remove", "--name", "nonexistent"])
        assert result.exit_code == 0
        assert "não encontrada" in result.output.lower()


# ─── doctor command ───────────────────────────────────────────────────────────


class TestDoctorCLI:
    def test_doctor_runs(self):
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "Python" in result.output or "python" in result.output.lower()


# ─── analyze error paths ─────────────────────────────────────────────────────


class TestAnalyzeErrors:
    def test_analyze_no_input(self):
        result = runner.invoke(app, ["analyze"])
        assert result.exit_code == 1

    def test_analyze_no_connection(self, tmp_path, tmp_connections_file):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM users")
        result = runner.invoke(app, ["analyze", str(sql_file)])
        assert result.exit_code == 1
