"""Testes das funções de poda, formatação e geração de report."""

import json

from sqlmentor.report import _prune_dead_operations, _strip_view_column_list, to_json, to_markdown


def test_prune_dead_operations():
    plan = [
        "SQL_ID  bp85n659525m6, child number 0",
        "Plan hash value: 790398113",
        "|   0 | SELECT STATEMENT  |  |      1 |        |      1 |00:00:00.97 |     137K|",
        "|*  1 |  COUNT STOPKEY    |  |      1 |        |      1 |00:00:00.01 |       3 |",
        "|* 26 |     INDEX UNIQUE SCAN  | PK_GRUPO_ATIVIDADE  |      0 |      1 |      0 |00:00:00.01 |       0 |",
        "|  27 |    TABLE ACCESS BY INDEX ROWID  | GRUPO_ATIVIDADE  |      0 |      1 |      0 |00:00:00.01 |       0 |",
        "| 381 | TABLE ACCESS BY INDEX ROWID  | FUNCIONARIO  |  62502 |      1 |  59897 |00:00:00.15 |   79881 |",
    ]
    result, _pruned_ids = _prune_dead_operations(plan)
    # Deve manter linhas 0, 1, 381 (Starts>0) e remover 26, 27 (Starts=0, A-Rows=0)
    kept_ids = [ln for ln in result if ln.startswith("|")]
    assert len(kept_ids) == 3, f"Esperava 3 linhas, got {len(kept_ids)}: {kept_ids}"
    assert "omitidas" in result[-1]
    print("P1 OK: 2 operações mortas removidas")


def test_strip_view_column_list():
    ddl = (
        'CREATE OR REPLACE FORCE VIEW "SAMPLE_SCHEMA"."VW_ENTITY_A_DETAIL" '
        '("COL1", "COL2", "COL3") AS\n'
        "  SELECT\n"
        "    T1.COL1,\n"
        "    T1.COL2\n"
        "  FROM TABELA T1"
    )
    result = _strip_view_column_list(ddl)
    assert '("COL1"' not in result, "Lista de colunas não foi removida"
    assert "SELECT" in result, "SELECT sumiu"
    assert "CREATE OR REPLACE FORCE VIEW" in result, "Header sumiu"
    print("P3 OK: lista de colunas removida da DDL")
    print(result[:200])


def test_strip_view_no_column_list():
    """DDL sem lista de colunas não deve ser alterada."""
    ddl = "CREATE VIEW SCHEMA.V AS\n  SELECT 1 FROM DUAL"
    result = _strip_view_column_list(ddl)
    assert result == ddl
    print("P3 OK: DDL sem lista de colunas inalterada")


# ─── to_json ──────────────────────────────────────────────────────────────────


class TestToJson:
    def test_returns_valid_json(self, minimal_collected_context):
        result = to_json(minimal_collected_context)
        data = json.loads(result)
        assert isinstance(data, dict)

    def test_contains_expected_keys(self, minimal_collected_context):
        data = json.loads(to_json(minimal_collected_context))
        assert "tables" in data
        assert "execution_plan" in data or "db_version" in data

    def test_empty_context(self, empty_collected_context):
        result = to_json(empty_collected_context)
        data = json.loads(result)
        assert isinstance(data, dict)


# ─── to_markdown ──────────────────────────────────────────────────────────────


class TestToMarkdown:
    def test_returns_string(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_sql_section(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context)
        assert "SQL" in result

    def test_contains_plan_section(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context)
        # Plan should be in the report since we provided execution_plan
        assert "Plan" in result or "plan" in result.lower() or "Plano" in result

    def test_empty_context_no_crash(self, empty_collected_context):
        """to_markdown com contexto vazio não deve explodir."""
        result = to_markdown(empty_collected_context)
        assert isinstance(result, str)

    def test_full_verbosity(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context, verbosity="full")
        assert isinstance(result, str)

    def test_compact_verbosity(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context, verbosity="compact")
        assert isinstance(result, str)

    def test_minimal_verbosity(self, minimal_collected_context):
        result = to_markdown(minimal_collected_context, verbosity="minimal")
        assert isinstance(result, str)

    def test_minimal_shorter_than_full(self, minimal_collected_context):
        full = to_markdown(minimal_collected_context, verbosity="full")
        minimal = to_markdown(minimal_collected_context, verbosity="minimal")
        assert len(minimal) <= len(full)

    def test_invalid_verbosity_raises(self, minimal_collected_context):
        import pytest

        with pytest.raises(ValueError):
            to_markdown(minimal_collected_context, verbosity="invalid")


if __name__ == "__main__":
    test_prune_dead_operations()
    test_strip_view_column_list()
    test_strip_view_no_column_list()
    print("\nTodos os testes passaram.")
