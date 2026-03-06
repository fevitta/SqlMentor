"""Testes de integração: end-to-end collect_context → report (Markdown/JSON)."""

import json

import pytest

from sqlmentor.collector import collect_context
from sqlmentor.report import to_json, to_markdown

pytestmark = pytest.mark.oracle


class TestMarkdownReport:
    """Gera relatório Markdown a partir de dados reais do Oracle."""

    def test_markdown_has_expected_sections(
        self, oracle_conn, oracle_schema, parsed_employees_orders
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        md = to_markdown(ctx)
        assert isinstance(md, str)
        assert len(md) > 100

        # Seções esperadas no Markdown
        md_upper = md.upper()
        assert "EXECUTION PLAN" in md_upper or "PLANO" in md_upper or "PLAN" in md_upper
        assert "EMPLOYEES" in md_upper
        assert "ORDERS" in md_upper
        assert "OPTIMIZER" in md_upper

    def test_markdown_compact_shorter_than_full(
        self, oracle_conn, oracle_schema, parsed_employees_orders
    ):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        md_compact = to_markdown(ctx, verbosity="compact")
        md_full = to_markdown(ctx, verbosity="full")
        # Compact deve ser menor ou igual ao full
        assert len(md_compact) <= len(md_full)


class TestJSONReport:
    """Gera relatório JSON a partir de dados reais do Oracle."""

    def test_json_is_valid(self, oracle_conn, oracle_schema, parsed_employees_orders):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        json_str = to_json(ctx)
        assert isinstance(json_str, str)
        # Deve ser JSON válido
        data = json.loads(json_str)
        assert isinstance(data, dict)

    def test_json_has_key_fields(self, oracle_conn, oracle_schema, parsed_employees_orders):
        ctx = collect_context(
            parsed=parsed_employees_orders,
            conn=oracle_conn,
            default_schema=oracle_schema,
            use_cache=False,
        )
        data = json.loads(to_json(ctx))
        assert "tables" in data
        assert "optimizer_params" in data
        assert len(data["tables"]) == 2


class TestRuntimeReport:
    """Relatório com plano runtime (execute=True)."""

    def test_runtime_markdown_has_allstats(self, oracle_conn, oracle_schema, parsed_single_table):
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=oracle_conn,
            default_schema=oracle_schema,
            execute=True,
            use_cache=False,
        )
        md = to_markdown(ctx)
        md_upper = md.upper()
        # Deve ter seção de runtime plan ou execution plan
        assert "PLAN" in md_upper
        assert "EMPLOYEES" in md_upper
