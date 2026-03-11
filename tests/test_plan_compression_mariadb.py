"""Testes de validação das regras R1-R12 com PlanBlocks MariaDB.

MariaDB produz PlanBlocks com buffers=None e reads=None, e operações
diferentes de Oracle (ex: ALL, REF, RANGE vs TABLE ACCESS FULL, INDEX RANGE SCAN).
Verifica que:
- R5 thresholds de buffers/reads são ignorados (None)
- R1, R2, R3, R7, R8 nunca disparam (padrões Oracle ausentes)
- R6, R9-R12 funcionam normalmente
- _compress_plan propaga db_type corretamente
"""

import json

from sqlmentor.report import (
    PlanBlock,
    _apply_thresholds,
    _collapse_config_fields,
    _collapse_low_cost_nested_loops,
    _collapse_situation_history,
    _collapse_union_all_branches,
    _collapse_view_zero_rows,
    _compress_plan,
    _detect_plan_blocks,
    _extract_plan_index_names,
)

# ─── Fixtures MariaDB ────────────────────────────────────────────────


def _mariadb_block(**kwargs) -> PlanBlock:
    """Cria PlanBlock típico de MariaDB (buffers=None, reads=None)."""
    defaults = {
        "id": "1",
        "operation": "ALL",
        "name": "orders",
        "starts": 1,
        "e_rows": 100,
        "a_rows": 95,
        "a_time_ms": 0.5,
        "buffers": None,
        "reads": None,
        "indent": 0,
    }
    defaults.update(kwargs)
    return PlanBlock(**defaults)


ESTIMATED_PLAN_JSON = json.dumps(
    {
        "query_block": {
            "select_id": 1,
            "nested_loop": [
                {
                    "table": {
                        "table_name": "orders",
                        "access_type": "ALL",
                        "rows_examined_per_scan": 1000,
                        "rows": 1000,
                    }
                },
                {
                    "table": {
                        "table_name": "items",
                        "access_type": "ref",
                        "rows_examined_per_scan": 5,
                        "rows": 5,
                    }
                },
            ],
        }
    }
)

ANALYZE_PLAN_JSON = json.dumps(
    {
        "query_block": {
            "select_id": 1,
            "nested_loop": [
                {
                    "table": {
                        "table_name": "orders",
                        "access_type": "ALL",
                        "rows_examined_per_scan": 1000,
                        "r_loops": 1,
                        "r_rows": 980,
                        "r_total_time_ms": 1.2,
                    }
                },
                {
                    "table": {
                        "table_name": "items",
                        "access_type": "ref",
                        "rows_examined_per_scan": 5,
                        "r_loops": 980,
                        "r_rows": 4800,
                        "r_total_time_ms": 3.5,
                    }
                },
            ],
        }
    }
)


# ─── T22: TestMariaDBThresholds (R5) ─────────────────────────────────


class TestMariaDBThresholds:
    """R5 com PlanBlocks MariaDB: buffers=None e reads=None são ignorados."""

    def test_buffers_none_reads_none_not_immune(self):
        """buffers=None, reads=None → não marca imune por buffers/reads."""
        block = _mariadb_block(starts=1, a_time_ms=0.1, e_rows=10, a_rows=10)
        _apply_thresholds([block])
        assert not block.immune

    def test_starts_above_threshold_immune(self):
        """starts=150 > 100 → imune."""
        block = _mariadb_block(starts=150)
        _apply_thresholds([block])
        assert block.immune

    def test_a_time_above_threshold_immune(self):
        """a_time_ms=150 > 100 → imune."""
        block = _mariadb_block(a_time_ms=150.0)
        _apply_thresholds([block])
        assert block.immune

    def test_cardinality_ratio_immune(self):
        """e_rows=10, a_rows=200 → ratio 20x > 10x → imune."""
        block = _mariadb_block(e_rows=10, a_rows=200)
        _apply_thresholds([block])
        assert block.immune

    def test_cardinality_ratio_within_threshold_not_immune(self):
        """e_rows=10, a_rows=50 → ratio 5x < 10x → não imune."""
        block = _mariadb_block(e_rows=10, a_rows=50, starts=1, a_time_ms=0.1)
        _apply_thresholds([block])
        assert not block.immune

    def test_mixed_blocks_partial_immunity(self):
        """Múltiplos blocks: apenas os que atendem threshold ficam imunes."""
        blocks = [
            _mariadb_block(id="1", starts=1, a_time_ms=0.1, e_rows=10, a_rows=10),
            _mariadb_block(id="2", starts=200),
            _mariadb_block(id="3", a_time_ms=0.01, starts=1, e_rows=5, a_rows=5),
        ]
        _apply_thresholds(blocks)
        assert not blocks[0].immune
        assert blocks[1].immune
        assert not blocks[2].immune


# ─── T22: TestMariaDBCollapseInactive ─────────────────────────────────


class TestMariaDBCollapseInactive:
    """Regras que nunca disparam com operações MariaDB."""

    def test_r1_no_sort_aggregate(self):
        """R1: _collapse_config_fields retorna vazio — MariaDB não tem SORT AGGREGATE."""
        blocks = [
            _mariadb_block(id="1", operation="ALL", indent=0),
            _mariadb_block(id="2", operation="REF", indent=1),
            _mariadb_block(id="3", operation="RANGE", indent=1),
            _mariadb_block(id="4", operation="ALL", indent=0),
        ]
        result = _collapse_config_fields(blocks)
        assert result == []

    def test_r2_no_sort_aggregate(self):
        """R2: _collapse_situation_history retorna vazio — sem SORT AGGREGATE."""
        blocks = [
            _mariadb_block(id="1", operation="ALL", indent=0),
            _mariadb_block(id="2", operation="REF", indent=1),
            _mariadb_block(id="3", operation="RANGE", indent=1),
        ]
        result = _collapse_situation_history(blocks, {})
        assert result == []

    def test_r3_no_view_operation(self):
        """R3: _collapse_view_zero_rows retorna vazio — MariaDB expande views, sem operação VIEW."""
        blocks = [
            _mariadb_block(id="1", operation="ALL", a_rows=0, indent=0),
            _mariadb_block(id="2", operation="REF", a_rows=0, indent=1),
        ]
        result = _collapse_view_zero_rows(blocks)
        assert result == []

    def test_r7_union_result_collapses_identical_branches(self):
        """R7: UNION RESULT (MariaDB) colapsa ≥3 branches idênticos, como UNION-ALL Oracle."""
        blocks = [
            _mariadb_block(id="1", operation="UNION RESULT", indent=0),
            _mariadb_block(id="2", operation="ALL", name="t1", indent=1),
            _mariadb_block(id="3", operation="ALL", name="t1", indent=1),
            _mariadb_block(id="4", operation="ALL", name="t1", indent=1),
        ]
        result = _collapse_union_all_branches(blocks)
        assert len(result) == 1
        assert result[0].collapsed_ids == {"2", "3", "4"}
        assert "[COLAPSADO:" in result[0].replacement_lines[0]

    def test_r7_union_result_fewer_than_three_no_collapse(self):
        """R7: UNION RESULT com <3 branches não colapsa."""
        blocks = [
            _mariadb_block(id="1", operation="UNION RESULT", indent=0),
            _mariadb_block(id="2", operation="ALL", name="t1", indent=1),
            _mariadb_block(id="3", operation="ALL", name="t1", indent=1),
        ]
        result = _collapse_union_all_branches(blocks)
        assert result == []

    def test_r8_no_nested_loops(self):
        """R8: _collapse_low_cost_nested_loops retorna vazio — MariaDB não tem NESTED LOOPS."""
        blocks = [
            _mariadb_block(id="1", operation="ALL", starts=200, indent=0),
            _mariadb_block(id="2", operation="REF", starts=200, indent=1),
        ]
        result = _collapse_low_cost_nested_loops(blocks)
        assert result == []


# ─── T22: TestMariaDBCompressPlan ─────────────────────────────────────


class TestMariaDBCompressPlan:
    """Full pipeline _compress_plan com db_type='mariadb'."""

    def test_estimated_plan_no_collapses(self):
        """Plano estimado simples → sem colapsos, plano retornado intacto."""
        plan_lines = ESTIMATED_PLAN_JSON.splitlines()
        new_plan, _new_preds = _compress_plan(plan_lines, [], "compact", db_type="mariadb")
        # Sem colapsos — plano retornado como está
        assert len(new_plan) == len(plan_lines)

    def test_analyze_plan_no_collapses(self):
        """Plano ANALYZE simples → sem colapsos, plano retornado intacto."""
        plan_lines = ANALYZE_PLAN_JSON.splitlines()
        new_plan, _new_preds = _compress_plan(plan_lines, [], "compact", db_type="mariadb")
        assert len(new_plan) == len(plan_lines)

    def test_full_verbosity_passthrough(self):
        """verbosity='full' → sem compressão, retorna original."""
        plan_lines = ESTIMATED_PLAN_JSON.splitlines()
        new_plan, new_preds = _compress_plan(plan_lines, [], "full", db_type="mariadb")
        assert new_plan == plan_lines
        assert new_preds == []

    def test_empty_plan(self):
        """Plano vazio → sem erros."""
        new_plan, new_preds = _compress_plan([], [], "compact", db_type="mariadb")
        assert new_plan == []
        assert new_preds == []


# ─── T22: TestMariaDBDetectPlanBlocks ────────────────────────────────


class TestMariaDBDetectPlanBlocks:
    """_detect_plan_blocks com db_type='mariadb' usa MariaDBPlanParser."""

    def test_dispatches_to_mariadb_parser(self):
        """Verifica que db_type='mariadb' produz PlanBlocks com buffers=None."""
        plan_lines = ESTIMATED_PLAN_JSON.splitlines()
        blocks = _detect_plan_blocks(plan_lines, db_type="mariadb")
        assert len(blocks) == 2
        assert blocks[0].operation == "ALL"
        assert blocks[0].name == "orders"
        assert blocks[0].buffers is None
        assert blocks[0].reads is None

    def test_analyze_plan_has_runtime_stats(self):
        """ANALYZE plan → PlanBlocks com a_rows e starts preenchidos."""
        plan_lines = ANALYZE_PLAN_JSON.splitlines()
        blocks = _detect_plan_blocks(plan_lines, db_type="mariadb")
        assert len(blocks) == 2
        assert blocks[0].a_rows == 980
        assert blocks[0].starts == 1
        assert blocks[1].starts == 980

    def test_empty_plan(self):
        """Plano vazio → lista vazia."""
        blocks = _detect_plan_blocks([], db_type="mariadb")
        assert blocks == []


# ─── T22: TestMariaDBExtractIndexNames (R9) ─────────────────────────


class TestMariaDBExtractIndexNames:
    """R9: extração de nomes de índice depende de 'INDEX' na operação."""

    def test_index_access_type_extracted(self):
        """access_type=INDEX → nome extraído."""
        plan_json = json.dumps(
            {
                "query_block": {
                    "select_id": 1,
                    "table": {
                        "table_name": "orders",
                        "access_type": "index",
                        "rows_examined_per_scan": 10,
                    },
                }
            }
        )
        names = _extract_plan_index_names(plan_json.splitlines(), db_type="mariadb")
        assert "orders" in names

    def test_range_access_type_not_extracted(self):
        """access_type=RANGE → 'INDEX' ausente → nome NÃO extraído."""
        plan_json = json.dumps(
            {
                "query_block": {
                    "select_id": 1,
                    "table": {
                        "table_name": "orders",
                        "access_type": "range",
                        "rows_examined_per_scan": 10,
                    },
                }
            }
        )
        names = _extract_plan_index_names(plan_json.splitlines(), db_type="mariadb")
        assert "orders" not in names

    def test_ref_access_type_not_extracted(self):
        """access_type=ref → 'INDEX' ausente → nome NÃO extraído."""
        plan_json = json.dumps(
            {
                "query_block": {
                    "select_id": 1,
                    "table": {
                        "table_name": "items",
                        "access_type": "ref",
                        "rows_examined_per_scan": 5,
                    },
                }
            }
        )
        names = _extract_plan_index_names(plan_json.splitlines(), db_type="mariadb")
        assert names == set()

    def test_all_access_type_not_extracted(self):
        """access_type=ALL → nome NÃO extraído."""
        plan_json = json.dumps(
            {
                "query_block": {
                    "select_id": 1,
                    "table": {
                        "table_name": "users",
                        "access_type": "ALL",
                        "rows_examined_per_scan": 1000,
                    },
                }
            }
        )
        names = _extract_plan_index_names(plan_json.splitlines(), db_type="mariadb")
        assert names == set()
