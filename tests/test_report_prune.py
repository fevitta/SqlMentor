"""Teste rápido das novas funções de poda do report."""
from sqlmentor.report import _prune_dead_operations, _strip_view_column_list


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
    result = _prune_dead_operations(plan)
    # Deve manter linhas 0, 1, 381 (Starts>0) e remover 26, 27 (Starts=0, A-Rows=0)
    kept_ids = [l for l in result if l.startswith("|")]
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


if __name__ == "__main__":
    test_prune_dead_operations()
    test_strip_view_column_list()
    test_strip_view_no_column_list()
    print("\nTodos os testes passaram.")
