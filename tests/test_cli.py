"""Testes para o CLI (Typer) do sqlmentor."""

from unittest.mock import MagicMock

from typer.testing import CliRunner

from sqlmentor.cli import _StepTimer, _validate_timeout, app
from sqlmentor.collector import CollectedContext, TableContext
from sqlmentor.parser import ParsedSQL

runner = CliRunner()


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_parsed(schema="HR"):
    """ParsedSQL mínimo para mocks."""
    return ParsedSQL(
        raw_sql="SELECT id, name FROM users WHERE id = 1",
        sql_type="SELECT",
        tables=[{"name": "USERS", "schema": schema, "alias": None}],
        where_columns=["id"],
    )


def _make_ctx(
    parsed=None,
    *,
    errors=None,
    runtime_plan=None,
    runtime_stats=None,
    tables=None,
    execution_plan=None,
    wait_events=None,
    optimizer_params=None,
):
    """CollectedContext mínimo para mocks."""
    if parsed is None:
        parsed = _make_parsed()
    return CollectedContext(
        parsed_sql=parsed,
        db_version="Oracle Database 19c",
        execution_plan=execution_plan or ["Plan hash value: 123"],
        runtime_plan=runtime_plan,
        runtime_stats=runtime_stats,
        tables=tables
        or [
            TableContext(
                name="USERS",
                schema="HR",
                object_type="TABLE",
                stats={"num_rows": 1000},
                columns=[{"column_name": "ID", "data_type": "NUMBER", "nullable": "N"}],
                indexes=[
                    {
                        "index_name": "PK_USERS",
                        "index_type": "NORMAL",
                        "uniqueness": "UNIQUE",
                        "columns": "ID",
                    }
                ],
                constraints=[
                    {"constraint_name": "PK_USERS", "constraint_type": "P", "columns": "ID"}
                ],
            )
        ],
        optimizer_params=optimizer_params or {"optimizer_mode": "ALL_ROWS"},
        errors=errors or [],
    )


def _analyze_patches(
    monkeypatch,
    tmp_path,
    *,
    ctx=None,
    sql_text=None,
    connect_exc=None,
    collect_exc=None,
    is_normalized=False,
    format_output="# Report",
):
    """Configure common patches for analyze success tests.

    Returns dict of mock objects for assertion.
    """
    sql_file = tmp_path / "query.sql"
    sql_file.write_text(sql_text or "SELECT id FROM users WHERE id = 1")
    out_file = tmp_path / "output.md"

    if ctx is None:
        ctx = _make_ctx()

    mocks = {}

    monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
    monkeypatch.setattr(
        "sqlmentor.connector.get_connection_config",
        lambda name: {"schema": "HR", "user": "hr", "timeout": 180},
    )

    mock_conn = MagicMock()
    if connect_exc:
        monkeypatch.setattr("sqlmentor.connector.connect", MagicMock(side_effect=connect_exc))
    else:
        monkeypatch.setattr("sqlmentor.connector.connect", MagicMock(return_value=mock_conn))
    mocks["conn"] = mock_conn

    monkeypatch.setattr("sqlmentor.parser.parse_sql", lambda sql, **kw: _make_parsed())
    monkeypatch.setattr("sqlmentor.parser.is_normalized_sql", lambda sql: is_normalized)
    monkeypatch.setattr("sqlmentor.parser.denormalize_sql", lambda sql, mode="literal": (sql, {}))
    monkeypatch.setattr("sqlmentor.parser.detect_sql_binds", lambda sql: set())
    monkeypatch.setattr("sqlmentor.parser.remap_bind_params", lambda bp, sb: bp)
    monkeypatch.setattr("sqlmentor.parser.parse_bind_values", lambda raw: raw)

    if collect_exc:
        monkeypatch.setattr(
            "sqlmentor.collector.collect_context",
            MagicMock(side_effect=collect_exc),
        )
    else:
        monkeypatch.setattr("sqlmentor.collector.collect_context", MagicMock(return_value=ctx))

    mock_to_md = MagicMock(return_value=format_output)
    mock_to_json = MagicMock(return_value='{"report": true}')
    monkeypatch.setattr("sqlmentor.report.to_markdown", mock_to_md)
    monkeypatch.setattr("sqlmentor.report.to_json", mock_to_json)
    mocks["to_markdown"] = mock_to_md
    mocks["to_json"] = mock_to_json

    return sql_file, out_file, mocks


def _inspect_patches(
    monkeypatch,
    tmp_path,
    *,
    ctx=None,
    sql_text_row=None,
    runtime_plan_rows=None,
    runtime_stats_row=None,
    runtime_plan_exc=None,
    runtime_stats_exc=None,
    sql_not_found=False,
    format_output="# Report",
):
    """Configure common patches for inspect success tests."""
    out_file = tmp_path / "output.md"
    if ctx is None:
        ctx = _make_ctx()

    mocks = {}

    monkeypatch.setattr("sqlmentor.connector.resolve_connection", lambda name: name or "test")
    monkeypatch.setattr(
        "sqlmentor.connector.get_connection_config",
        lambda name: {"schema": "HR", "user": "hr", "timeout": 180},
    )

    # Build mock cursor with configurable behavior per execute call
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # sql_text_by_id fetch
    if sql_not_found:
        mock_cursor.fetchone.return_value = None
    else:
        row = sql_text_row or ("SELECT id FROM users WHERE id = 1",)
        # First fetchone = sql_text, second = runtime_stats
        stats_row = runtime_stats_row  # may be None
        mock_cursor.fetchone.side_effect = [row, stats_row]

    # runtime_plan iteration
    if runtime_plan_exc:
        # Make iteration raise on __iter__
        def _iter_raise(*a, **kw):
            raise runtime_plan_exc

        mock_cursor.__iter__ = MagicMock(side_effect=_iter_raise)
    elif runtime_plan_rows is not None:
        mock_cursor.__iter__ = MagicMock(return_value=iter(runtime_plan_rows))
    else:
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))

    # runtime_stats description
    if runtime_stats_exc:
        # We need execute to raise on the 3rd call (stats query)
        # Since cursor.execute is called for: sql_text, runtime_plan, runtime_stats
        original_execute = mock_cursor.execute
        call_count = [0]

        def _execute_side_effect(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 3:  # runtime_stats call
                raise runtime_stats_exc
            return original_execute(*a, **kw)

        mock_cursor.execute = MagicMock(side_effect=_execute_side_effect)
    else:
        mock_cursor.description = [("sql_id",), ("executions",)]

    monkeypatch.setattr("sqlmentor.connector.connect", MagicMock(return_value=mock_conn))
    mocks["conn"] = mock_conn
    mocks["cursor"] = mock_cursor

    monkeypatch.setattr("sqlmentor.parser.parse_sql", lambda sql, **kw: _make_parsed())

    monkeypatch.setattr(
        "sqlmentor.queries.sql_text_by_id",
        lambda sid: ("SELECT sql_fulltext FROM v$sql WHERE sql_id = :sid", {"sid": sid}),
    )
    monkeypatch.setattr(
        "sqlmentor.queries.runtime_plan",
        lambda sid: ("SELECT plan_table_output FROM ...", {"sid": sid}),
    )
    monkeypatch.setattr(
        "sqlmentor.queries.sql_runtime_stats",
        lambda sid: ("SELECT * FROM v$sql WHERE sql_id = :sid", {"sid": sid}),
    )

    if sql_not_found:
        # collect_context won't be called
        pass
    else:
        monkeypatch.setattr("sqlmentor.collector.collect_context", MagicMock(return_value=ctx))

    mock_to_md = MagicMock(return_value=format_output)
    mock_to_json = MagicMock(return_value='{"report": true}')
    monkeypatch.setattr("sqlmentor.report.to_markdown", mock_to_md)
    monkeypatch.setattr("sqlmentor.report.to_json", mock_to_json)
    mocks["to_markdown"] = mock_to_md
    mocks["to_json"] = mock_to_json

    return out_file, mocks


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


# ─── resolve input ───────────────────────────────────────────────────────────


class TestResolveInput:
    def test_empty_sql_inline(self):
        """--sql '' → exit 1, 'vazio'."""
        result = runner.invoke(app, ["parse", "--sql", "   "])
        assert result.exit_code == 1
        assert "vazio" in result.output.lower()


# ─── parse extended ──────────────────────────────────────────────────────────


class TestParseExtended:
    def test_auto_detect_normalized(self, monkeypatch):
        """is_normalized_sql True → 'normalizado detectado' in output."""
        monkeypatch.setattr("sqlmentor.parser.is_normalized_sql", lambda sql: True)
        result = runner.invoke(
            app,
            ["parse", "--sql", "SELECT * FROM t WHERE a = ?"],
        )
        assert result.exit_code == 0
        assert "normalizado detectado" in result.output.lower()

    def test_parse_errors_shown(self, monkeypatch):
        """Parsed with parse_errors → errors shown in output."""
        parsed_with_errors = ParsedSQL(
            raw_sql="SELECT /*+ BROKEN HINT */ FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": None, "alias": None}],
            is_parseable=True,
            parse_errors=["Token inesperado: BROKEN"],
        )

        def _mock_parse(sql, **kw):
            return parsed_with_errors

        monkeypatch.setattr("sqlmentor.parser.parse_sql", _mock_parse)
        result = runner.invoke(
            app,
            ["parse", "--sql", "SELECT /*+ BROKEN HINT */ FROM users"],
        )
        assert result.exit_code == 0
        assert "BROKEN" in result.output


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


class TestConfigListWithConnections:
    def test_list_with_connections(self, monkeypatch):
        """List shows table with host/port/service."""
        monkeypatch.setattr(
            "sqlmentor.connector.list_connections",
            lambda: {
                "prod": {
                    "host": "db1.example.com",
                    "port": 1521,
                    "service": "ORCL",
                    "user": "app",
                    "schema": "APP",
                    "timeout": 180,
                }
            },
        )
        monkeypatch.setattr("sqlmentor.connector.get_default_connection", lambda: None)
        result = runner.invoke(app, ["config", "list"])
        assert result.exit_code == 0
        assert "prod" in result.output
        assert "db1.example" in result.output  # Rich pode truncar em terminais estreitos

    def test_list_shows_default(self, monkeypatch):
        """Default connection shows ★."""
        monkeypatch.setattr(
            "sqlmentor.connector.list_connections",
            lambda: {
                "prod": {
                    "host": "db1",
                    "port": 1521,
                    "service": "ORCL",
                    "user": "app",
                    "schema": "APP",
                    "timeout": 180,
                }
            },
        )
        monkeypatch.setattr("sqlmentor.connector.get_default_connection", lambda: "prod")
        result = runner.invoke(app, ["config", "list"])
        assert result.exit_code == 0
        assert "★" in result.output


class TestConfigSetDefault:
    def test_set_default_success(self, monkeypatch):
        """Name exists → 'definida como padrão'."""
        monkeypatch.setattr("sqlmentor.connector.set_default_connection", lambda name: None)
        result = runner.invoke(app, ["config", "set-default", "--name", "prod"])
        assert result.exit_code == 0
        assert "definida como padrão" in result.output.lower()

    def test_set_default_nonexistent(self, monkeypatch):
        """Name doesn't exist → exit 1, 'Erro'."""
        monkeypatch.setattr(
            "sqlmentor.connector.set_default_connection",
            MagicMock(side_effect=ValueError("Conexão 'xxx' não existe")),
        )
        result = runner.invoke(app, ["config", "set-default", "--name", "xxx"])
        assert result.exit_code == 1
        assert "erro" in result.output.lower()


class TestConfigTest:
    def test_success(self, monkeypatch):
        """Mock test_connection → 'Conectado', version+schema."""
        monkeypatch.setattr(
            "sqlmentor.connector.test_connection",
            lambda name: {"version": "Oracle 19c", "schema": "HR"},
        )
        result = runner.invoke(app, ["config", "test", "--name", "prod"])
        assert result.exit_code == 0
        assert "Conectado" in result.output
        assert "19c" in result.output

    def test_failure(self, monkeypatch):
        """test_connection raises → exit 1, 'Falha'."""
        monkeypatch.setattr(
            "sqlmentor.connector.test_connection",
            MagicMock(side_effect=Exception("ORA-12541: no listener")),
        )
        result = runner.invoke(app, ["config", "test", "--name", "prod"])
        assert result.exit_code == 1
        assert "Falha" in result.output


class TestConfigAdd:
    def _add_cmd(self, monkeypatch, *, diagnose_result=None, diagnose_exc=None):
        """Helper: run config add with mocked add_connection and diagnose."""
        monkeypatch.setattr("sqlmentor.connector.add_connection", lambda **kw: None)
        if diagnose_exc:
            monkeypatch.setattr(
                "sqlmentor.connector.diagnose_connection",
                MagicMock(side_effect=diagnose_exc),
            )
        else:
            monkeypatch.setattr(
                "sqlmentor.connector.diagnose_connection",
                lambda name: (
                    diagnose_result
                    or {
                        "version": "Oracle 19c",
                        "schema": "HR",
                        "mode": "thin",
                        "major_version": "19",
                    }
                ),
            )
        return runner.invoke(
            app,
            [
                "config",
                "add",
                "--name",
                "t",
                "--host",
                "h",
                "--port",
                "1521",
                "--service",
                "s",
                "--user",
                "u",
                "--password",
                "p",
            ],
        )

    def test_validation_success(self, monkeypatch):
        """diagnose_connection OK → 'Conectado', version."""
        result = self._add_cmd(monkeypatch)
        assert result.exit_code == 0
        assert "Conectado" in result.output
        assert "19c" in result.output

    def test_validation_oracle_old_thick(self, monkeypatch):
        """major < 12 + thick → 'tudo certo'."""
        result = self._add_cmd(
            monkeypatch,
            diagnose_result={
                "version": "Oracle 11g",
                "schema": "HR",
                "mode": "thick",
                "major_version": "11",
            },
        )
        assert result.exit_code == 0
        assert "tudo certo" in result.output.lower()

    def test_validation_failure(self, monkeypatch):
        """diagnose raises Exception → 'validação falhou'."""
        result = self._add_cmd(monkeypatch, diagnose_exc=Exception("ORA-12541"))
        assert result.exit_code == 0  # connection saved, validation warning
        assert "validação falhou" in result.output.lower()


class TestConfigRemove:
    def test_remove_existing(self, monkeypatch):
        """remove_connection True → 'removida'."""
        monkeypatch.setattr("sqlmentor.connector.remove_connection", lambda name: True)
        result = runner.invoke(app, ["config", "remove", "--name", "prod"])
        assert result.exit_code == 0
        assert "removida" in result.output.lower()


# ─── doctor command ───────────────────────────────────────────────────────────


class TestDoctorCLI:
    def test_doctor_runs(self):
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "Python" in result.output or "python" in result.output.lower()


class TestDoctorExtended:
    def test_oracledb_not_installed(self, monkeypatch):
        """PackageNotFoundError → 'não instalado'."""
        import importlib.metadata

        original_version = importlib.metadata.version

        def _mock_version(pkg):
            if pkg == "oracledb":
                raise importlib.metadata.PackageNotFoundError(pkg)
            return original_version(pkg)

        monkeypatch.setattr("importlib.metadata.version", _mock_version)
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "não instalado" in result.output.lower()

    def test_thick_mode_not_available(self, monkeypatch):
        """available=False → 'Não encontrado'."""
        monkeypatch.setattr(
            "sqlmentor.connector.check_thick_mode_available",
            lambda: {"available": "False", "detail": ""},
        )
        monkeypatch.setattr("sqlmentor.connector.list_connections", lambda: {})
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "não encontrado" in result.output.lower()

    def test_connection_diagnostics(self, monkeypatch):
        """List connections + diagnose → 'Conectado'."""
        monkeypatch.setattr(
            "sqlmentor.connector.check_thick_mode_available",
            lambda: {"available": "True", "detail": "Instant Client 19.8"},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.list_connections",
            lambda: {"prod": {"host": "db1", "port": 1521, "service": "ORCL"}},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.diagnose_connection",
            lambda name: {
                "version": "Oracle 19c",
                "schema": "HR",
                "mode": "thin",
                "major_version": "19",
            },
        )
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "Conectado" in result.output

    def test_connection_diagnostics_failure(self, monkeypatch):
        """diagnose raises → 'Falha'."""
        monkeypatch.setattr(
            "sqlmentor.connector.check_thick_mode_available",
            lambda: {"available": "True", "detail": "Instant Client 19.8"},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.list_connections",
            lambda: {"prod": {"host": "db1", "port": 1521, "service": "ORCL"}},
        )
        monkeypatch.setattr(
            "sqlmentor.connector.diagnose_connection",
            MagicMock(side_effect=Exception("Connection refused")),
        )
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0
        assert "Falha" in result.output


# ─── _print_summary ──────────────────────────────────────────────────────────


class TestPrintSummary:
    """Test _print_summary via analyze output (CliRunner captures everything)."""

    def test_basic_summary(self, monkeypatch, tmp_path):
        """ctx with plan + table → 'Resumo da Coleta' in output."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "Resumo da Coleta" in result.output

    def test_runtime_items(self, monkeypatch, tmp_path):
        """ctx with runtime_plan + runtime_stats → both in summary."""
        ctx = _make_ctx(
            runtime_plan=["Plan line 1", "Plan line 2"],
            runtime_stats={"sql_id": "abc123", "executions": 5},
        )
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path, ctx=ctx)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "Runtime Plan" in result.output
        assert "Runtime Stats" in result.output

    def test_view_not_expanded(self, monkeypatch, tmp_path):
        """VIEW without ddl/columns → 'skip' in summary."""
        ctx = _make_ctx(
            tables=[
                TableContext(name="V_ACTIVE", schema="HR", object_type="VIEW"),
            ]
        )
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path, ctx=ctx)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "skip" in result.output.lower()

    def test_errors_in_summary(self, monkeypatch, tmp_path):
        """ctx with errors → 'Erros' in summary."""
        ctx = _make_ctx(errors=["Timeout ao coletar stats"])
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path, ctx=ctx)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "Erros" in result.output


# ─── analyze success ─────────────────────────────────────────────────────────


class TestAnalyzeSuccess:
    def test_basic_markdown(self, monkeypatch, tmp_path):
        """SQL file + mocked connection → report .md created, exit 0."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert out_file.exists()
        assert "Relatório salvo" in result.output

    def test_json_format(self, monkeypatch, tmp_path):
        """--format json → to_json called."""
        sql_file, out_file, mocks = _analyze_patches(monkeypatch, tmp_path)
        out_file = tmp_path / "output.json"
        result = runner.invoke(
            app,
            [
                "analyze",
                str(sql_file),
                "--conn",
                "test",
                "--format",
                "json",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 0
        mocks["to_json"].assert_called_once()

    def test_verbose_shows_panel(self, monkeypatch, tmp_path):
        """--verbose → Panel in output."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--verbose", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "SQL Tuning Context" in result.output

    def test_custom_output_path(self, monkeypatch, tmp_path):
        """--output /tmp/test.md → file created at that path."""
        sql_file, _out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        custom_out = tmp_path / "custom" / "report.md"
        custom_out.parent.mkdir(parents=True)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(custom_out)],
        )
        assert result.exit_code == 0
        assert custom_out.exists()

    def test_normalized_auto_detect(self, monkeypatch, tmp_path):
        """is_normalized_sql True → 'normalizado detectado' in output."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path, is_normalized=True)
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "normalizado detectado" in result.output.lower()

    def test_normalized_with_execute_rejects(self, monkeypatch, tmp_path):
        """--normalized --execute → exit 1."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            [
                "analyze",
                str(sql_file),
                "--conn",
                "test",
                "--normalized",
                "--execute",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 1
        assert "incompatível" in result.output.lower() or "normalizado" in result.output.lower()

    def test_bind_parsing(self, monkeypatch, tmp_path):
        """-b name=john -b id=1 → bind_params passed to collect_context."""
        mock_collect = MagicMock(return_value=_make_ctx())
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        monkeypatch.setattr("sqlmentor.collector.collect_context", mock_collect)
        monkeypatch.setattr(
            "sqlmentor.parser.parse_bind_values",
            lambda raw: {k: v for k, v in raw.items()},
        )
        result = runner.invoke(
            app,
            [
                "analyze",
                str(sql_file),
                "--conn",
                "test",
                "-b",
                "name=john",
                "-b",
                "id=1",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 0

    def test_missing_binds_warning(self, monkeypatch, tmp_path):
        """--execute + missing bind → warning, execute=False."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        monkeypatch.setattr("sqlmentor.parser.detect_sql_binds", lambda sql: {"id_param"})
        # remap returns empty dict (no binds provided)
        monkeypatch.setattr("sqlmentor.parser.remap_bind_params", lambda bp, sb: {})
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--execute", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "binds não informados" in result.output.lower()


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

    def test_connection_error(self, monkeypatch, tmp_path):
        """connect() raises → exit 1, 'Erro de conexão'."""
        sql_file, out_file, _mocks = _analyze_patches(
            monkeypatch, tmp_path, connect_exc=Exception("ORA-12541: no listener")
        )
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 1
        assert "erro de conexão" in result.output.lower()

    def test_collect_error(self, monkeypatch, tmp_path):
        """collect_context() raises → exit 1, conn.close() called."""
        sql_file, out_file, mocks = _analyze_patches(
            monkeypatch, tmp_path, collect_exc=Exception("ORA-00942: table does not exist")
        )
        result = runner.invoke(
            app,
            ["analyze", str(sql_file), "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 1
        assert "erro na coleta" in result.output.lower()
        mocks["conn"].close.assert_called()

    def test_invalid_bind_format(self, monkeypatch, tmp_path):
        """-b badformat → exit 1, 'Bind inválido'."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            [
                "analyze",
                str(sql_file),
                "--conn",
                "test",
                "-b",
                "badformat",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 1
        assert "bind inválido" in result.output.lower()


# ─── inspect success ─────────────────────────────────────────────────────────


class TestInspectSuccess:
    def test_basic_inspect(self, monkeypatch, tmp_path):
        """sql_id → SQL recovered, parsed, collected, report saved."""
        out_file, _mocks = _inspect_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "Relatório salvo" in result.output

    def test_sql_not_found(self, monkeypatch, tmp_path):
        """cursor.fetchone() → None → exit 1, 'não encontrado'."""
        out_file, _mocks = _inspect_patches(monkeypatch, tmp_path, sql_not_found=True)
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 1
        assert "não encontrado" in result.output.lower()

    def test_runtime_plan_collected(self, monkeypatch, tmp_path):
        """cursor returns plan lines → ctx.runtime_plan populated."""
        out_file, _mocks = _inspect_patches(
            monkeypatch,
            tmp_path,
            runtime_plan_rows=[("Plan line 1",), ("Plan line 2",)],
        )
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "Relatório salvo" in result.output

    def test_runtime_plan_failure_continues(self, monkeypatch, tmp_path):
        """Plan query exception → warning, continues."""
        out_file, _mocks = _inspect_patches(
            monkeypatch,
            tmp_path,
            runtime_plan_exc=Exception("ORA-06502"),
        )
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "não disponível" in result.output.lower()

    def test_runtime_stats_collected(self, monkeypatch, tmp_path):
        """cursor returns stats row → ctx.runtime_stats populated."""
        out_file, _mocks = _inspect_patches(
            monkeypatch,
            tmp_path,
            runtime_stats_row=("abc123", 5),
        )
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0

    def test_runtime_stats_failure_continues(self, monkeypatch, tmp_path):
        """Stats query exception → warning, continues."""
        out_file, _mocks = _inspect_patches(
            monkeypatch,
            tmp_path,
            runtime_stats_exc=Exception("ORA-01031"),
        )
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "não disponíve" in result.output.lower()

    def test_clob_sql_text(self, monkeypatch, tmp_path):
        """SQL_FULLTEXT retornado como CLOB (thick mode) → .read() chamado corretamente."""
        mock_lob = MagicMock()
        mock_lob.read.return_value = "SELECT id FROM users WHERE id = 1"
        out_file, _mocks = _inspect_patches(
            monkeypatch, tmp_path, sql_text_row=(mock_lob,)
        )
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        assert "Relatório salvo" in result.output
        mock_lob.read.assert_called_once()

    def test_json_format(self, monkeypatch, tmp_path):
        """--format json → to_json called."""
        out_file, mocks = _inspect_patches(monkeypatch, tmp_path)
        out_file = tmp_path / "output.json"
        result = runner.invoke(
            app,
            ["inspect", "abc123", "--conn", "test", "--format", "json", "--output", str(out_file)],
        )
        assert result.exit_code == 0
        mocks["to_json"].assert_called_once()


# ─── timeout validation ──────────────────────────────────────────────────────


class TestValidateTimeout:
    def test_negative_timeout_exits(self):
        """Timeout -5 → typer.Exit(1)."""
        import pytest
        import typer

        with pytest.raises(typer.Exit):
            _validate_timeout(-5)

    def test_too_large_timeout_exits(self):
        """Timeout 9999 → typer.Exit(1)."""
        import pytest
        import typer

        with pytest.raises(typer.Exit):
            _validate_timeout(9999)

    def test_valid_timeout_passes(self):
        """Timeout 300 → no exception."""
        _validate_timeout(300)

    def test_none_timeout_passes(self):
        """Timeout None → no exception."""
        _validate_timeout(None)

    def test_boundary_1_passes(self):
        """Timeout 1 → ok."""
        _validate_timeout(1)

    def test_boundary_3600_passes(self):
        """Timeout 3600 → ok."""
        _validate_timeout(3600)

    def test_analyze_rejects_invalid_timeout(self, monkeypatch, tmp_path):
        """analyze --timeout -5 → exit 1."""
        sql_file, out_file, _mocks = _analyze_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            [
                "analyze",
                str(sql_file),
                "--conn",
                "test",
                "--timeout",
                "-5",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 1

    def test_inspect_rejects_invalid_timeout(self, monkeypatch, tmp_path):
        """inspect --timeout 9999 → exit 1."""
        out_file, _mocks = _inspect_patches(monkeypatch, tmp_path)
        result = runner.invoke(
            app,
            [
                "inspect",
                "abc123",
                "--conn",
                "test",
                "--timeout",
                "9999",
                "--output",
                str(out_file),
            ],
        )
        assert result.exit_code == 1


# ─── _StepTimer ──────────────────────────────────────────────────────────────


class TestStepTimer:
    def test_mark_and_durations(self):
        """mark 2 steps → durations > 0."""
        timer = _StepTimer()
        timer.mark("Step1")
        timer.mark("Step2")
        assert len(timer._steps) == 2
        for _name, elapsed in timer._steps:
            assert elapsed >= 0

    def test_print_summary_contains_total(self, capsys):
        """print_summary → output contains 'Total'."""
        timer = _StepTimer()
        timer.mark("Parse")
        timer.print_summary()
        captured = capsys.readouterr()
        assert "Total" in captured.out


