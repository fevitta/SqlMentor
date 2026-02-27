"""Testes para o MCP server do sqlmentor."""

import json
from unittest.mock import MagicMock, patch

from sqlmentor.collector import CollectedContext
from sqlmentor.mcp_server import (
    _validate_timeout_mcp,
    analyze_sql,
    inspect_sql,
    list_connections,
    parse_sql,
)
from sqlmentor.mcp_server import test_connection as mcp_test_connection
from sqlmentor.parser import ParsedSQL

# ─── parse_sql ────────────────────────────────────────────────────────────────


class TestMCPParseSql:
    def test_returns_valid_json(self):
        result = parse_sql("SELECT id, name FROM users WHERE status = 'ACTIVE'")
        data = json.loads(result)
        assert "sql_type" in data
        assert "tables" in data
        assert "where_columns" in data
        assert "is_parseable" in data

    def test_select_type(self):
        data = json.loads(parse_sql("SELECT * FROM orders"))
        assert data["sql_type"] == "SELECT"

    def test_tables_extracted(self):
        data = json.loads(
            parse_sql("SELECT * FROM hr.employees e JOIN hr.departments d ON e.dept_id = d.id")
        )
        assert len(data["tables"]) >= 2

    def test_with_schema(self):
        data = json.loads(parse_sql("SELECT * FROM employees", schema="HR"))
        assert data["is_parseable"]

    def test_normalized_sql(self):
        data = json.loads(parse_sql("SELECT * FROM t WHERE a = ? AND b = ?", normalized=True))
        assert data["is_parseable"]

    def test_normalized_auto_detect(self):
        data = json.loads(parse_sql("SELECT * FROM t WHERE a = ? AND b = ?"))
        assert data["is_parseable"]

    def test_denorm_mode_bind(self):
        data = json.loads(parse_sql("SELECT * FROM t WHERE a = ? AND b = ?", denorm_mode="bind"))
        assert data["is_parseable"]

    def test_empty_sql(self):
        data = json.loads(parse_sql(""))
        assert isinstance(data, dict)


# ─── list_connections ─────────────────────────────────────────────────────────


class TestMCPListConnections:
    def test_no_connections(self, tmp_connections_file):
        result = json.loads(list_connections())
        assert "connections" in result
        assert result["connections"] == []

    def test_with_connections(self, tmp_connections_file):
        from sqlmentor.connector import add_connection

        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        result = json.loads(list_connections())
        assert len(result["connections"]) == 1
        assert result["connections"][0]["name"] == "dev"


# ─── test_connection ──────────────────────────────────────────────────────────


class TestMCPTestConnection:
    def test_bad_connection(self, tmp_connections_file):
        result = json.loads(mcp_test_connection("nonexistent"))
        assert result["status"] == "error"

    def test_success(self):
        with patch(
            "sqlmentor.connector.test_connection",
            return_value={"version": "Oracle 19c", "schema": "HR"},
        ):
            result = json.loads(mcp_test_connection("dev"))
            assert result["status"] == "ok"
            assert result["version"] == "Oracle 19c"
            assert result["schema"] == "HR"


# ─── analyze_sql ──────────────────────────────────────────────────────────────


class TestMCPAnalyzeSql:
    def test_no_connection(self, tmp_connections_file):
        result = json.loads(analyze_sql("SELECT * FROM users"))
        assert "error" in result

    def test_bad_connection(self, tmp_connections_file):
        from sqlmentor.connector import add_connection

        add_connection("dev", "badhost", 1521, "ORCL", "scott", "tiger")
        from sqlmentor.connector import set_default_connection

        set_default_connection("dev")
        result = json.loads(analyze_sql("SELECT * FROM users"))
        assert "error" in result

    def test_normalized_with_execute_rejected(self, tmp_connections_file):
        from sqlmentor.connector import add_connection, set_default_connection

        add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        result = json.loads(
            analyze_sql(
                "SELECT * FROM t WHERE a = ? AND b = ?",
                normalized=True,
                execute=True,
            )
        )
        assert "error" in result
        assert "normalizado" in result["error"].lower() or "incompatível" in result["error"].lower()


# ─── inspect_sql ──────────────────────────────────────────────────────────────


class TestMCPInspectSql:
    def test_no_connection(self, tmp_connections_file):
        result = json.loads(inspect_sql("abc123def45"))
        assert "error" in result

    def test_bad_connection(self, tmp_connections_file):
        from sqlmentor.connector import add_connection, set_default_connection

        add_connection("dev", "badhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        result = json.loads(inspect_sql("abc123def45"))
        assert "error" in result


# ─── Bind parsing ─────────────────────────────────────────────────────────────


class TestMCPBindParsing:
    def test_binds_with_null(self, tmp_connections_file):
        """Verify null/none bind values are handled (tested indirectly via analyze_sql)."""
        from sqlmentor.connector import add_connection, set_default_connection

        add_connection("dev", "badhost", 1521, "ORCL", "scott", "tiger")
        set_default_connection("dev")
        # Will fail on connection but should not crash on bind parsing
        result = json.loads(analyze_sql("SELECT * FROM t WHERE a = :a", binds="a=null"))
        assert "error" in result  # Connection error, not bind error


# ─── analyze_sql: normalized auto-detection ─────────────────────────────────


def _make_mock_ctx():
    """Cria CollectedContext mínimo para testes de MCP."""
    parsed = ParsedSQL(raw_sql="SELECT 1 FROM dual", sql_type="SELECT")
    return CollectedContext(parsed_sql=parsed)


def _setup_analyze_patches(
    tmp_connections_file,
    *,
    collect_return=None,
    collect_side_effect=None,
    to_markdown_return="# Report",
    to_json_return='{"report": true}',
):
    """Helper que configura mocks para analyze_sql success path.

    Retorna dict de patches para usar com contextmanager.
    """
    from sqlmentor.connector import add_connection, set_default_connection

    add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
    set_default_connection("dev")

    mock_conn = MagicMock()
    ctx = collect_return or _make_mock_ctx()

    patches = {
        "connect": patch("sqlmentor.connector.connect", return_value=mock_conn),
        "collect": patch(
            "sqlmentor.collector.collect_context",
            return_value=ctx,
            side_effect=collect_side_effect,
        ),
        "to_markdown": patch("sqlmentor.report.to_markdown", return_value=to_markdown_return),
        "to_json": patch("sqlmentor.report.to_json", return_value=to_json_return),
    }
    return patches, mock_conn, ctx


class TestMCPAnalyzeSqlNormalized:
    def test_auto_detect_normalized(self, tmp_connections_file):
        """is_normalized_sql retorna True → denormalize_sql chamado automaticamente."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
            patch("sqlmentor.parser.is_normalized_sql", return_value=True) as mock_detect,
            patch("sqlmentor.parser.denormalize_sql", return_value=("SELECT 1", {})) as mock_denorm,
        ):
            analyze_sql("SELECT * FROM t WHERE a = ?", conn="dev")
            mock_detect.assert_called_once()
            mock_denorm.assert_called_once()

    def test_denormalize_called_with_mode(self, tmp_connections_file):
        """denormalize_sql recebe mode correto."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
            patch("sqlmentor.parser.denormalize_sql", return_value=("SELECT 1", {})) as mock_denorm,
        ):
            analyze_sql(
                "SELECT * FROM t WHERE a = ?", conn="dev", normalized=True, denorm_mode="bind"
            )
            mock_denorm.assert_called_once_with("SELECT * FROM t WHERE a = ?", mode="bind")


class TestMCPAnalyzeSqlBinds:
    def test_bind_parsing_key_value(self, tmp_connections_file):
        """'a=1,b=hello' é parseado corretamente."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"] as mock_collect,
            patches["to_markdown"],
        ):
            analyze_sql("SELECT * FROM t WHERE a = :a AND b = :b", conn="dev", binds="a=1,b=hello")
            # collect_context foi chamado com bind_params que contém a e b
            call_kwargs = mock_collect.call_args[1]
            bind_params = call_kwargs.get("bind_params")
            assert bind_params is not None
            # Os binds são parseados e remapeados
            assert len(bind_params) >= 2

    def test_bind_parsing_skips_invalid(self, tmp_connections_file):
        """Pares inválidos sem '=' são ignorados."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"] as mock_collect,
            patches["to_markdown"],
        ):
            analyze_sql("SELECT * FROM t WHERE a = :a", conn="dev", binds="badpair,a=1")
            call_kwargs = mock_collect.call_args[1]
            bind_params = call_kwargs.get("bind_params")
            assert bind_params is not None

    def test_missing_binds_returns_error(self, tmp_connections_file):
        """execute=True + binds faltantes → JSON error com hint."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            result = json.loads(
                analyze_sql(
                    "SELECT * FROM t WHERE a = :a AND b = :b",
                    conn="dev",
                    execute=True,
                    binds="a=1",
                )
            )
            assert "error" in result
            assert "B" in result["error"].upper()  # bind faltante

    def test_missing_binds_closes_conn(self, tmp_connections_file):
        """conn.close() chamado antes de retornar erro de binds faltantes."""
        patches, mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            analyze_sql(
                "SELECT * FROM t WHERE a = :a AND b = :b",
                conn="dev",
                execute=True,
                binds="a=1",
            )
            mock_conn.close.assert_called()


class TestMCPAnalyzeSqlCollect:
    def test_collect_success_markdown(self, tmp_connections_file):
        """collect_context OK → to_markdown chamado."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"] as mock_md,
        ):
            result = analyze_sql("SELECT * FROM users", conn="dev")
            mock_md.assert_called_once()
            assert result == "# Report"

    def test_collect_success_json(self, tmp_connections_file):
        """output_format=json → to_json chamado."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_json"] as mock_json,
            patches["to_markdown"],
        ):
            result = analyze_sql("SELECT * FROM users", conn="dev", output_format="json")
            mock_json.assert_called_once()
            assert result == '{"report": true}'

    def test_collect_exception_returns_error(self, tmp_connections_file):
        """collect_context raises → JSON error + conn.close()."""
        patches, _mock_conn, _ = _setup_analyze_patches(
            tmp_connections_file, collect_side_effect=RuntimeError("boom")
        )
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            result = json.loads(analyze_sql("SELECT * FROM users", conn="dev"))
            assert "error" in result
            assert "boom" in result["error"]

    def test_conn_closed_in_finally(self, tmp_connections_file):
        """Mesmo com sucesso, conn.close() chamado via finally."""
        patches, mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            analyze_sql("SELECT * FROM users", conn="dev")
            mock_conn.close.assert_called()

    def test_verbosity_passed_to_markdown(self, tmp_connections_file):
        """verbosity é passado a to_markdown."""
        patches, _mock_conn, _ = _setup_analyze_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"] as mock_md,
        ):
            analyze_sql("SELECT * FROM users", conn="dev", verbosity="full")
            call_kwargs = mock_md.call_args[1]
            assert call_kwargs.get("verbosity") == "full"


# ─── inspect_sql: success paths ─────────────────────────────────────────────


def _setup_inspect_patches(tmp_connections_file, **overrides):
    """Helper que configura mocks para inspect_sql success path."""
    from sqlmentor.connector import add_connection, set_default_connection

    add_connection("dev", "localhost", 1521, "ORCL", "scott", "tiger")
    set_default_connection("dev")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Defaults: SQL text encontrado, runtime plan OK, stats OK
    mock_cursor.fetchone.side_effect = overrides.get(
        "fetchone_side_effect",
        [("SELECT * FROM users",), ("Oracle 19c", 1)],  # sql_text, runtime_stats
    )
    mock_cursor.__iter__ = MagicMock(
        side_effect=overrides.get(
            "iter_side_effect",
            lambda: iter([("Plan line 1",), ("Plan line 2",)]),
        )
    )
    mock_cursor.description = overrides.get(
        "description",
        [("sql_id",), ("executions",)],
    )

    ctx = overrides.get("ctx", _make_mock_ctx())

    patches = {
        "connect": patch("sqlmentor.connector.connect", return_value=mock_conn),
        "collect": patch("sqlmentor.collector.collect_context", return_value=ctx),
        "to_markdown": patch("sqlmentor.report.to_markdown", return_value="# Inspect Report"),
        "to_json": patch("sqlmentor.report.to_json", return_value='{"inspect": true}'),
    }
    return patches, mock_conn, mock_cursor, ctx


class TestMCPInspectSqlSuccess:
    def test_sql_retrieved_from_shared_pool(self, tmp_connections_file):
        """cursor retorna SQL text → parsed e coletado."""
        patches, _mock_conn, _mock_cursor, _ctx = _setup_inspect_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"] as mock_collect,
            patches["to_markdown"],
        ):
            result = inspect_sql("abc123def456", conn="dev")
            # collect_context foi chamado
            mock_collect.assert_called_once()
            assert result == "# Inspect Report"

    def test_sql_id_not_found_returns_error(self, tmp_connections_file):
        """fetchone → None → JSON error com hint."""
        patches, _mock_conn, _mock_cursor, _ctx = _setup_inspect_patches(
            tmp_connections_file,
            fetchone_side_effect=[None],  # sql_text not found
        )
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            result = json.loads(inspect_sql("abc123def456", conn="dev"))
            assert "error" in result
            assert "não encontrado" in result["error"] or "shared pool" in result["error"]

    def test_sql_text_as_plain_string(self, tmp_connections_file):
        """SQL text retornado como string pura → parsed e coletado."""
        patches, _mock_conn, _mock_cursor, _ctx = _setup_inspect_patches(
            tmp_connections_file,
            fetchone_side_effect=[("SELECT * FROM big_table",), ("Oracle 19c", 1)],
        )
        with (
            patches["connect"],
            patches["collect"] as mock_collect,
            patches["to_markdown"],
        ):
            inspect_sql("abc123def456", conn="dev")
            mock_collect.assert_called_once()
            # O SQL parseado deve conter o texto do shared pool
            call_args = mock_collect.call_args
            assert call_args[1]["parsed"].raw_sql == "SELECT * FROM big_table"

    def test_runtime_plan_collected(self, tmp_connections_file):
        """runtime_plan query retorna linhas → ctx.runtime_plan populado."""
        ctx = _make_mock_ctx()
        patches, _mock_conn, _mock_cursor, _ = _setup_inspect_patches(tmp_connections_file, ctx=ctx)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            inspect_sql("abc123def456", conn="dev")
            # ctx.runtime_plan should be set (from cursor iteration)
            assert ctx.runtime_plan is not None

    def test_runtime_plan_failure_continues(self, tmp_connections_file):
        """Exception no runtime_plan → warning, continua sem erro."""
        ctx = _make_mock_ctx()
        call_count = [0]

        def failing_iter():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("plan not available")
            return iter([])

        patches, _mock_conn, mock_cursor, _ = _setup_inspect_patches(
            tmp_connections_file,
            ctx=ctx,
            iter_side_effect=failing_iter,
        )
        # Override cursor behavior: execute raises on runtime_plan query
        original_execute = mock_cursor.execute
        exec_count = [0]

        def custom_execute(sql, params=None):
            exec_count[0] += 1
            if exec_count[0] == 2:  # 2nd execute is runtime_plan
                raise RuntimeError("plan not available")
            return original_execute(sql, params)

        mock_cursor.execute = MagicMock(side_effect=custom_execute)

        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            result = inspect_sql("abc123def456", conn="dev")
            # Should not be an error JSON — continues gracefully
            assert "error" not in result.lower() or "inspect" in result.lower()

    def test_runtime_stats_collected(self, tmp_connections_file):
        """sql_runtime_stats retorna row → ctx.runtime_stats populado."""
        ctx = _make_mock_ctx()
        patches, _mock_conn, _mock_cursor, _ = _setup_inspect_patches(tmp_connections_file, ctx=ctx)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            inspect_sql("abc123def456", conn="dev")
            assert ctx.runtime_stats is not None

    def test_runtime_stats_failure_continues(self, tmp_connections_file):
        """Exception nas stats → warning, continua sem erro."""
        ctx = _make_mock_ctx()
        exec_count = [0]

        patches, _mock_conn, mock_cursor, _ = _setup_inspect_patches(tmp_connections_file, ctx=ctx)

        def custom_execute(sql, params=None):
            exec_count[0] += 1
            if exec_count[0] == 3:  # 3rd execute is sql_runtime_stats
                raise RuntimeError("stats not available")

        mock_cursor.execute = MagicMock(side_effect=custom_execute)

        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            result = inspect_sql("abc123def456", conn="dev")
            assert "# Inspect Report" in result

    def test_output_json(self, tmp_connections_file):
        """output_format=json → to_json chamado."""
        patches, _mock_conn, _mock_cursor, _ctx = _setup_inspect_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_json"] as mock_json,
            patches["to_markdown"],
        ):
            result = inspect_sql("abc123def456", conn="dev", output_format="json")
            mock_json.assert_called_once()
            assert result == '{"inspect": true}'

    def test_output_markdown(self, tmp_connections_file):
        """output_format=markdown → to_markdown chamado."""
        patches, _mock_conn, _mock_cursor, _ctx = _setup_inspect_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"] as mock_md,
        ):
            inspect_sql("abc123def456", conn="dev", output_format="markdown")
            mock_md.assert_called_once()

    def test_conn_closed_in_finally(self, tmp_connections_file):
        """conn.close() sempre chamado."""
        patches, mock_conn, _mock_cursor, _ctx = _setup_inspect_patches(tmp_connections_file)
        with (
            patches["connect"],
            patches["collect"],
            patches["to_markdown"],
        ):
            inspect_sql("abc123def456", conn="dev")
            mock_conn.close.assert_called()


# ─── timeout validation MCP ──────────────────────────────────────────────────


class TestMCPValidateTimeout:
    def test_negative_timeout_returns_error(self):
        """timeout -1 → JSON error."""
        result = _validate_timeout_mcp(-1)
        assert result is not None
        data = json.loads(result)
        assert "error" in data

    def test_too_large_timeout_returns_error(self):
        """timeout 5000 → JSON error."""
        result = _validate_timeout_mcp(5000)
        assert result is not None
        data = json.loads(result)
        assert "error" in data

    def test_zero_timeout_passes(self):
        """timeout 0 (default) → None."""
        assert _validate_timeout_mcp(0) is None

    def test_valid_timeout_passes(self):
        """timeout 300 → None."""
        assert _validate_timeout_mcp(300) is None

    def test_analyze_sql_rejects_invalid_timeout(self, tmp_connections_file):
        """analyze_sql with timeout=-1 → JSON error, no connection attempt."""
        result = json.loads(analyze_sql("SELECT 1", timeout=-1))
        assert "error" in result

    def test_inspect_sql_rejects_invalid_timeout(self, tmp_connections_file):
        """inspect_sql with timeout=5000 → JSON error."""
        result = json.loads(inspect_sql("abc123def456", timeout=5000))
        assert "error" in result


# ─── get_status ──────────────────────────────────────────────────────────────


class TestMCPGetStatus:
    def test_returns_valid_json(self):
        """get_status returns valid JSON with version and status."""
        from sqlmentor.mcp_server import get_status

        result = json.loads(get_status())
        assert result["status"] == "ok"
        assert "version" in result

    def test_cache_reflects_state(self):
        """After populating cache, get_status reports counts."""
        from sqlmentor.collector import TableContext, _table_cache
        from sqlmentor.mcp_server import get_status

        _table_cache.put("HR.TEST", TableContext(name="TEST", schema="HR"))
        result = json.loads(get_status())
        assert result["cache"]["tables"] >= 1
