"""Testes de integração: EXPLAIN PLAN estimado e runtime (execute=True)."""

import pytest

from sqlmentor.collector import CollectedContext, _collect_explain_plan, collect_context

pytestmark = pytest.mark.oracle


class TestExplainPlanEstimado:
    """Testa geração de plano estimado via EXPLAIN PLAN FOR."""

    def test_simple_select(self, oracle_cursor, oracle_schema):
        sql = f"SELECT emp_id, first_name FROM {oracle_schema}.EMPLOYEES WHERE emp_id = 1"  # noqa: S608
        ctx = CollectedContext(parsed_sql=None)
        plan = _collect_explain_plan(oracle_cursor, sql, ctx)
        assert plan is not None
        assert len(plan) > 0
        # Plano deve conter cabeçalho com colunas padrão
        plan_text = "\n".join(plan)
        assert "Id" in plan_text
        assert "Operation" in plan_text

    def test_join_plan(self, oracle_cursor, oracle_schema):
        sql = (
            f"SELECT e.emp_id, o.order_id FROM {oracle_schema}.EMPLOYEES e "  # noqa: S608
            f"JOIN {oracle_schema}.ORDERS o ON e.emp_id = o.emp_id "
            f"WHERE e.dept_id = 10"
        )
        ctx = CollectedContext(parsed_sql=None)
        plan = _collect_explain_plan(oracle_cursor, sql, ctx)
        assert plan is not None
        plan_text = "\n".join(plan).upper()
        # Deve conter algum tipo de join (HASH JOIN, NESTED LOOPS, MERGE JOIN)
        assert any(op in plan_text for op in ["HASH JOIN", "NESTED LOOPS", "MERGE JOIN"])

    def test_plan_with_bind_params(self, oracle_cursor, oracle_schema):
        sql = f"SELECT emp_id FROM {oracle_schema}.EMPLOYEES WHERE dept_id = :dept AND status = :st"  # noqa: S608
        ctx = CollectedContext(parsed_sql=None)
        bind_params = {"dept": 10, "st": "ACTIVE"}
        plan = _collect_explain_plan(oracle_cursor, sql, ctx, bind_params=bind_params)
        assert plan is not None
        assert len(plan) > 0

    def test_plan_table_cleanup(self, oracle_cursor, oracle_schema):
        """PLAN_TABLE deve estar limpa após coleta (statement_id removido)."""
        sql = f"SELECT 1 FROM {oracle_schema}.DEPARTMENTS WHERE dept_id = 10"  # noqa: S608
        ctx = CollectedContext(parsed_sql=None)
        _collect_explain_plan(oracle_cursor, sql, ctx)

        oracle_cursor.execute(
            "SELECT COUNT(*) FROM PLAN_TABLE WHERE statement_id = 'SQLMENTOR_PLAN'"
        )
        row = oracle_cursor.fetchone()
        assert row[0] == 0


class TestRuntimeExecution:
    """Testa execução real com GATHER_PLAN_STATISTICS (execute=True)."""

    def test_runtime_plan_populated(self, oracle_conn, oracle_schema, parsed_single_table):
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=oracle_conn,
            default_schema=oracle_schema,
            execute=True,
            use_cache=False,
        )
        # Com execute=True, deve ter runtime_plan (não execution_plan)
        assert ctx.runtime_plan is not None
        assert len(ctx.runtime_plan) > 0

    def test_runtime_plan_has_actual_rows(self, oracle_conn, oracle_schema, parsed_single_table):
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=oracle_conn,
            default_schema=oracle_schema,
            execute=True,
            use_cache=False,
        )
        assert ctx.runtime_plan is not None
        plan_text = "\n".join(ctx.runtime_plan)
        # ALLSTATS LAST deve ter A-Rows (actual rows)
        assert "A-Rows" in plan_text or "A-Time" in plan_text

    def test_runtime_stats_populated(self, oracle_conn, oracle_schema, parsed_single_table):
        ctx = collect_context(
            parsed=parsed_single_table,
            conn=oracle_conn,
            default_schema=oracle_schema,
            execute=True,
            use_cache=False,
        )
        assert ctx.runtime_stats is not None
        assert ctx.runtime_stats.get("executions", 0) >= 1
        assert "sql_id" in ctx.runtime_stats
