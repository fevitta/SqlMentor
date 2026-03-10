"""Testes de integração MariaDB: EXPLAIN FORMAT=JSON e ANALYZE FORMAT=JSON."""

import json

import pytest

from sqlmentor.collector import CollectedContext, _collect_explain_plan, collect_context

pytestmark = pytest.mark.mariadb


class TestExplainFormatJSON:
    """Testa geração de plano estimado via EXPLAIN FORMAT=JSON."""

    def test_simple_select(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        sql = f"SELECT EMP_ID, FIRST_NAME FROM `{mariadb_schema}`.EMPLOYEES WHERE EMP_ID = 1"  # noqa: S608
        ctx = CollectedContext(parsed_sql=None, db_type="mariadb")
        plan = _collect_explain_plan(mariadb_cursor, sql, ctx, mariadb_adapter)
        assert plan is not None
        assert len(plan) > 0
        # Plano MariaDB é JSON: deve conter query_block
        plan_text = "\n".join(plan)
        data = json.loads(plan_text)
        assert "query_block" in data

    def test_join_plan(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        sql = (
            f"SELECT e.EMP_ID, o.ORDER_ID FROM `{mariadb_schema}`.EMPLOYEES e "  # noqa: S608
            f"JOIN `{mariadb_schema}`.ORDERS o ON e.EMP_ID = o.EMP_ID "
            f"WHERE e.DEPT_ID = 10"
        )
        ctx = CollectedContext(parsed_sql=None, db_type="mariadb")
        plan = _collect_explain_plan(mariadb_cursor, sql, ctx, mariadb_adapter)
        assert plan is not None
        plan_text = "\n".join(plan)
        data = json.loads(plan_text)
        # JOIN deve produzir nested_loop no plano
        plan_str = json.dumps(data)
        assert "nested_loop" in plan_str or "table" in plan_str

    def test_plan_is_valid_json(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        sql = f"SELECT 1 FROM `{mariadb_schema}`.DEPARTMENTS WHERE DEPT_ID = 10"  # noqa: S608
        ctx = CollectedContext(parsed_sql=None, db_type="mariadb")
        plan = _collect_explain_plan(mariadb_cursor, sql, ctx, mariadb_adapter)
        assert plan is not None
        # Deve ser JSON parseável
        data = json.loads("\n".join(plan))
        assert isinstance(data, dict)


class TestAnalyzeFormatJSON:
    """Testa execução com ANALYZE (runtime plan via execute=True)."""

    def test_runtime_plan_populated(
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
        # Com execute=True, MariaDB usa ANALYZE FORMAT=JSON → runtime_plan
        assert ctx.runtime_plan is not None
        assert len(ctx.runtime_plan) > 0

    def test_runtime_plan_has_r_rows(
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
        assert ctx.runtime_plan is not None
        plan_text = "\n".join(ctx.runtime_plan)
        # ANALYZE FORMAT=JSON deve conter r_rows (runtime data)
        assert "r_rows" in plan_text

    def test_runtime_stats_populated(
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
        assert ctx.runtime_stats is not None
        assert ctx.runtime_stats.get("executions", 0) >= 1


class TestMariaDBPlanParser:
    """Testa que o plan parser produz PlanBlocks a partir de plano real."""

    def test_parser_produces_blocks(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        sql = (
            f"SELECT e.EMP_ID, o.ORDER_ID FROM `{mariadb_schema}`.EMPLOYEES e "  # noqa: S608
            f"JOIN `{mariadb_schema}`.ORDERS o ON e.EMP_ID = o.EMP_ID "
            f"WHERE e.DEPT_ID = 10"
        )
        ctx = CollectedContext(parsed_sql=None, db_type="mariadb")
        plan = _collect_explain_plan(mariadb_cursor, sql, ctx, mariadb_adapter)
        assert plan is not None

        blocks = mariadb_adapter.plan_parser.parse_plan(plan)
        # MariaDB 10.6 EXPLAIN sem nested_loop usa duplicate "table" keys no JSON.
        # json.loads mantém só a última — parser produz ≥1 bloco.
        assert len(blocks) >= 1
        for block in blocks:
            assert block.id is not None
            assert block.operation is not None
            assert block.buffers is None  # MariaDB não reporta buffers
            assert block.reads is None  # MariaDB não reporta reads

    def test_is_runtime_plan_detection(
        self, mariadb_conn, mariadb_schema, parsed_single_table, mariadb_adapter
    ):
        """Plano ANALYZE (execute=True) é detectado como runtime."""
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=mariadb_conn,
            default_schema=mariadb_schema,
            execute=True,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        assert ctx.runtime_plan is not None
        assert mariadb_adapter.plan_parser.is_runtime_plan(ctx.runtime_plan) is True

    def test_estimated_plan_not_runtime(self, mariadb_cursor, mariadb_schema, mariadb_adapter):
        """Plano EXPLAIN (estimado) NÃO é detectado como runtime."""
        sql = f"SELECT EMP_ID FROM `{mariadb_schema}`.EMPLOYEES WHERE EMP_ID = 1"  # noqa: S608
        ctx = CollectedContext(parsed_sql=None, db_type="mariadb")
        plan = _collect_explain_plan(mariadb_cursor, sql, ctx, mariadb_adapter)
        assert plan is not None
        assert mariadb_adapter.plan_parser.is_runtime_plan(plan) is False
