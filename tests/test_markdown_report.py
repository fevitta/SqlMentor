"""Testes de to_markdown() com contextos ricos — seções condicionais e verbosidade."""

from sqlmentor.collector import CollectedContext, TableContext
from sqlmentor.parser import ParsedSQL
from sqlmentor.report import to_markdown

# ─── Modo minimal ───────────────────────────────────────────────────────────


class TestMinimalVerbosity:
    def test_has_hotspots_section(self, rich_collected_context):
        result = to_markdown(rich_collected_context, verbosity="minimal")
        assert "Hotspots" in result

    def test_has_runtime_stats(self, rich_collected_context):
        result = to_markdown(rich_collected_context, verbosity="minimal")
        assert "Runtime Stats" in result

    def test_has_optimizer_params(self, rich_collected_context):
        result = to_markdown(rich_collected_context, verbosity="minimal")
        assert "Parâmetros do Otimizador" in result

    def test_has_errors(self, rich_collected_context):
        result = to_markdown(rich_collected_context, verbosity="minimal")
        assert "Erros na Coleta" in result

    def test_no_table_sections(self, rich_collected_context):
        result = to_markdown(rich_collected_context, verbosity="minimal")
        assert "Tabela:" not in result
        assert "Colunas e Estatísticas" not in result


# ─── Seção SQL Original ─────────────────────────────────────────────────────


class TestSqlOriginalSection:
    def test_join_columns_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "**Colunas em JOIN:**" in result

    def test_order_columns_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "**Colunas em ORDER BY:**" in result

    def test_group_columns_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "**Colunas em GROUP BY:**" in result

    def test_subqueries_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "**Subqueries:**" in result

    def test_functions_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "**Funções PL/SQL:**" in result


# ─── Runtime Plan ───────────────────────────────────────────────────────────


class TestRuntimePlanSection:
    def test_runtime_plan_present(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "Runtime Execution Plan" in result

    def test_executions_warning(self, rich_collected_context):
        # rich_collected_context has executions=5
        result = to_markdown(rich_collected_context)
        assert "SQL_ID já existia" in result

    def test_hotspots_after_runtime(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        runtime_pos = result.index("Runtime Execution Plan")
        assert "Hotspots" in result[runtime_pos:]


# ─── Implicit Conversions ───────────────────────────────────────────────────


class TestImplicitConversions:
    def test_conversion_detected(self, rich_collected_context):
        # The allstats_plan_lines fixture has TO_NUMBER in predicates
        result = to_markdown(rich_collected_context)
        assert "Implicit Conversions" in result

    def test_no_conversion(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context)
        assert "Implicit Conversions" not in result


# ─── View Expansion ─────────────────────────────────────────────────────────


class TestViewExpansion:
    def test_view_listed(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "View Expansion Summary" in result

    def test_shared_tables_between_views(self, rich_collected_context):
        # V_ACTIVE_ORDERS and V_USER_SUMMARY both have HR.AUDIT_LOG
        result = to_markdown(rich_collected_context)
        assert "compartilhadas" in result.lower() or "AUDIT_LOG" in result

    def test_user_tables_listed(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "Tabelas diretas no SQL" in result


# ─── Function DDLs ──────────────────────────────────────────────────────────


class TestFunctionDDLs:
    def test_function_ddl_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "Função:" in result or "FN_CALC" in result

    def test_functions_without_ddl(self):
        parsed = ParsedSQL(
            raw_sql="SELECT fn_calc(id) FROM users",
            sql_type="SELECT",
            tables=[{"name": "USERS", "schema": "HR", "alias": None}],
            functions=[{"schema": "HR", "name": "FN_CALC"}],
        )
        ctx = CollectedContext(
            parsed_sql=parsed,
            tables=[TableContext(name="USERS", schema="HR")],
        )
        result = to_markdown(ctx)
        assert "--expand-functions" in result


# ─── Table Sections ─────────────────────────────────────────────────────────


class TestTableSections:
    def test_view_without_details(self):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM v_active",
            sql_type="SELECT",
            tables=[{"name": "V_ACTIVE", "schema": "HR", "alias": None}],
        )
        view_ctx = TableContext(name="V_ACTIVE", schema="HR", object_type="VIEW")
        ctx = CollectedContext(parsed_sql=parsed, tables=[view_ctx])
        result = to_markdown(ctx)
        assert "--expand-views" in result

    def test_view_with_ddl(self, view_table_context):
        parsed = ParsedSQL(
            raw_sql="SELECT * FROM v_active_orders",
            sql_type="SELECT",
            tables=[{"name": "V_ACTIVE_ORDERS", "schema": "HR", "alias": None}],
        )
        ctx = CollectedContext(parsed_sql=parsed, tables=[view_table_context])
        result = to_markdown(ctx)
        assert "### DDL" in result

    def test_small_tables_section(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "Tabelas Pequenas" in result

    def test_columns_with_stats(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "Colunas e Estatísticas" in result

    def test_indexes_section(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "### Índices" in result

    def test_partitions_section(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "### Partições" in result

    def test_histograms_section(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "### Histogramas" in result


# ─── Errors Section ─────────────────────────────────────────────────────────


class TestErrorsSection:
    def test_errors_shown(self, rich_collected_context):
        result = to_markdown(rich_collected_context)
        assert "Erros na Coleta" in result
        assert "Timeout" in result

    def test_no_errors(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context)
        assert "Erros na Coleta" not in result
