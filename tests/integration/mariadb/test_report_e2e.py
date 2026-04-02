"""Testes de integração MariaDB: end-to-end collect_context → report (Markdown/JSON)."""

import json

import pytest

from sqlmentor.collector import collect_context
from sqlmentor.report import to_json, to_markdown

pytestmark = pytest.mark.mariadb


class TestMarkdownReport:
    """Gera relatório Markdown a partir de dados reais do MariaDB."""

    def test_markdown_has_expected_sections(
        self, mariadb_conn, mariadb_schema, parsed_employees_orders, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        md = to_markdown(ctx)
        assert isinstance(md, str)
        assert len(md) > 100

        md_upper = md.upper()
        assert "PLAN" in md_upper
        assert "EMPLOYEES" in md_upper
        assert "ORDERS" in md_upper
        assert "OTIMIZADOR" in md_upper

    def test_markdown_compact_shorter_than_full(
        self, mariadb_conn, mariadb_schema, parsed_employees_orders, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        md_compact = to_markdown(ctx, verbosity="compact")
        md_full = to_markdown(ctx, verbosity="full")
        assert len(md_compact) <= len(md_full)


class TestJSONReport:
    """Gera relatório JSON a partir de dados reais do MariaDB."""

    def test_json_is_valid(
        self, mariadb_conn, mariadb_schema, parsed_employees_orders, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        json_str = to_json(ctx)
        assert isinstance(json_str, str)
        data = json.loads(json_str)
        assert isinstance(data, dict)

    def test_json_has_key_fields(
        self, mariadb_conn, mariadb_schema, parsed_employees_orders, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        data = json.loads(to_json(ctx))
        assert "tables" in data
        assert "optimizer_params" in data
        assert len(data["tables"]) == 2

    def test_json_db_type_is_mariadb(
        self, mariadb_conn, mariadb_schema, parsed_employees_orders, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        data = json.loads(to_json(ctx))
        assert data.get("db_type") == "mariadb"


class TestRuntimeReport:
    """Relatório com plano runtime (execute=True)."""

    def test_runtime_markdown_has_plan(
        self, mariadb_conn, mariadb_schema, parsed_single_table, mariadb_adapter
    ):
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            execute=True,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        md = to_markdown(ctx)
        md_upper = md.upper()
        assert "PLAN" in md_upper
        assert "EMPLOYEES" in md_upper
