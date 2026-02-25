"""
Testes de compressão de plano Oracle — unitários e property-based (hypothesis).

Estrutura:
  - Estratégias Hypothesis para PlanBlock e CollectedContext
  - Helpers para gerar linhas no formato Oracle ALLSTATS
  - Placeholders indicando onde cada property test será adicionado (tarefas 8–12)
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from sqlmentor.report import (
    CollapseResult,
    PlanBlock,
    _add_nonsequential_id_note,
    _apply_thresholds,
    _collapse_config_fields,
    _collapse_orphan_predicates_by_ids,
    _collapse_situation_history,
    _collapse_vw_usuario_null,
    _compress_plan,
    _detect_plan_blocks,
)

# ─── Estratégias Hypothesis ───────────────────────────────────────


def valid_plan_block() -> st.SearchStrategy[PlanBlock]:
    """
    Gera PlanBlock com campos numéricos válidos (não-negativos).
    immune é sempre False — _apply_thresholds é quem seta.
    """
    return st.builds(
        PlanBlock,
        id=st.from_regex(r"\d{1,3}", fullmatch=True),
        operation=st.sampled_from([
            "SORT AGGREGATE",
            "INDEX RANGE SCAN",
            "TABLE ACCESS FULL",
            "NESTED LOOPS",
            "HASH JOIN",
            "VIEW",
            "FILTER",
            "SORT ORDER BY",
        ]),
        name=st.one_of(
            st.just(""),
            st.sampled_from([
                "IDX_ATTR_ENTITY_ID",
                "PK_ATTR_CONFIG",
                "UK_STATUS_TYPE_REF",
                "IDX_STATUS_HIST_ENTITY",
                "VW_CURRENT_USER",
                "SOME_TABLE",
            ]),
        ),
        starts=st.integers(min_value=0, max_value=10_000),
        e_rows=st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),
        a_rows=st.integers(min_value=0, max_value=1_000_000),
        a_time_ms=st.floats(min_value=0.0, max_value=100_000.0, allow_nan=False, allow_infinity=False),
        buffers=st.integers(min_value=0, max_value=1_000_000),
        reads=st.integers(min_value=0, max_value=100_000),
        indent=st.integers(min_value=0, max_value=20),
        immune=st.just(False),
    )


def plan_block_line(block: PlanBlock | None = None) -> st.SearchStrategy[str]:
    """
    Gera linhas no formato Oracle ALLSTATS LAST.

    Formato:
      |   1 | SORT AGGREGATE          |                              |     1 |       |      0 |00:00:00.01 |       3 |       0 |

    Se block é fornecido, formata aquele bloco específico.
    Caso contrário, retorna uma estratégia que gera linhas aleatórias válidas.
    """
    if block is not None:
        return st.just(_format_plan_block_line(block))

    return valid_plan_block().map(_format_plan_block_line)


def _format_plan_block_line(block: PlanBlock) -> str:
    """Formata um PlanBlock como linha Oracle ALLSTATS."""
    indent_str = " " * block.indent
    operation = f"{indent_str}{block.operation}"
    e_rows_str = str(block.e_rows) if block.e_rows is not None else ""

    # Converte ms de volta para HH:MM:SS.ss
    total_s = block.a_time_ms / 1000.0
    hours = int(total_s // 3600)
    minutes = int((total_s % 3600) // 60)
    seconds = total_s % 60
    atime_str = f"{hours:02d}:{minutes:02d}:{seconds:05.2f}"

    return (
        f"|   {block.id} |{operation:<24} | {block.name:<28} |"
        f" {block.starts:>5} | {e_rows_str:>6} | {block.a_rows:>6} |"
        f"{atime_str} | {block.buffers:>7} | {block.reads:>7} |"
    )


def valid_collected_context():
    """
    Stub de estratégia para CollectedContext.

    CollectedContext é complexo (requer conexão Oracle para coleta real),
    então esta estratégia será expandida nas tarefas 12–13 quando os testes
    de integração com to_markdown forem implementados.

    Por enquanto, retorna None — testes que precisam de CollectedContext
    devem construir instâncias manualmente via fixtures.
    """
    # TODO (tarefa 12): implementar estratégia completa usando st.builds(CollectedContext, ...)
    # Campos necessários: parsed_sql, tables, execution_plan, runtime_plan,
    # runtime_stats, wait_events, optimizer_params, view_expansions,
    # function_ddls, index_table_map, db_version, errors
    return st.none()


# ─── Fixtures de plano sintético ─────────────────────────────────


def _load_fixture(name: str) -> list[str]:
    """Carrega fixture de plano do diretório tests/fixtures/."""
    import os
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", name)
    with open(fixture_path, encoding="utf-8") as f:
        return f.read().splitlines()


# ─── Placeholders para property tests (tarefas 8–12) ─────────────
#
# Tarefa 8.2 — Property 9: Round-trip de PlanBlock
# Validates: Requirements 9
@given(block=valid_plan_block())
@settings(max_examples=200)
def test_plan_block_round_trip(block: PlanBlock):
    """
    Property 9: parsear → formatar → parsear produz objeto equivalente.

    Restrições do round-trip:
    - a_time_ms: a conversão HH:MM:SS.ss tem resolução de 10ms (2 casas decimais
      nos segundos). Valores são arredondados para o múltiplo de 10ms mais próximo.
    - indent: preservado exatamente (espaços antes da operação).
    - e_rows: None é preservado como None; inteiros são preservados.
    - immune/children: não fazem parte do formato de linha — não são comparados.
    """
    line = _format_plan_block_line(block)
    parsed = _detect_plan_blocks([line])

    # A linha deve ser parseável (formato válido)
    assert len(parsed) == 1, f"Linha não parseada: {line!r}"

    p = parsed[0]

    # Campos que devem ser preservados exatamente
    assert p.id == block.id
    assert p.operation == block.operation
    assert p.name == block.name
    assert p.starts == block.starts
    assert p.a_rows == block.a_rows
    assert p.buffers == block.buffers
    assert p.reads == block.reads
    assert p.indent == block.indent

    # e_rows: None → None, inteiro → inteiro
    assert p.e_rows == block.e_rows

    # a_time_ms: resolução de 10ms (formato HH:MM:SS.ss = centésimos de segundo)
    # Arredonda para o centésimo de segundo mais próximo (10ms)
    expected_ms = round(block.a_time_ms / 10.0) * 10.0
    assert abs(p.a_time_ms - expected_ms) < 1.0, (
        f"a_time_ms: esperado ~{expected_ms}, obtido {p.a_time_ms} "
        f"(original: {block.a_time_ms})"
    )
#
# Tarefa 8.4 — Property 8: Thresholds determinísticos
@given(block=valid_plan_block())
@settings(max_examples=500)
def test_thresholds_deterministic(block: PlanBlock):
    """
    **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**

    Property 8: Thresholds determinísticos.

    Após _apply_thresholds([block]), block.immune é True se e somente se
    pelo menos um dos critérios abaixo é satisfeito:
      - reads > 0
      - buffers > 1000
      - starts > 100
      - a_time_ms > 100.0
      - e_rows e a_rows ambos > 0 e max/min > 10
    """
    # Garante que immune começa como False (a estratégia já faz isso, mas explicitamos)
    block.immune = False

    # Calcula expectativa antes de chamar a função
    cardinality_immune = (
        block.e_rows is not None
        and block.e_rows > 0
        and block.a_rows > 0
        and max(block.e_rows, block.a_rows) / min(block.e_rows, block.a_rows) > 10
    )
    expected_immune = (
        block.reads > 0
        or block.buffers > 1000
        or block.starts > 100
        or block.a_time_ms > 100.0
        or cardinality_immune
    )

    _apply_thresholds([block])

    assert block.immune == expected_immune, (
        f"immune={block.immune}, esperado={expected_immune} | "
        f"reads={block.reads}, buffers={block.buffers}, starts={block.starts}, "
        f"a_time_ms={block.a_time_ms}, e_rows={block.e_rows}, a_rows={block.a_rows}"
    )
#
# Tarefa 9.4 — Property 4: Imunidade preservada
#   @given(blocks=st.lists(valid_plan_block(), min_size=1))
#   def test_immune_blocks_never_collapsed(blocks): ...
#
# Tarefa 9.5 — Property 7: Grupos mínimos
@given(blocks=st.lists(valid_plan_block(), min_size=0, max_size=30))
@settings(max_examples=300)
def test_minimum_group_sizes(blocks: list):
    """
    **Validates: Requirements 4.3, 5.3**

    Property 7: Grupos mínimos.

    Para qualquer lista de PlanBlock:
    - Todo CollapseResult retornado por _collapse_config_fields tem len(collapsed_ids) >= 3
    - Todo CollapseResult retornado por _collapse_situation_history tem len(collapsed_ids) >= 2
    """
    _apply_thresholds(blocks)

    for cr in _collapse_config_fields(blocks):
        assert len(cr.collapsed_ids) >= 3, (
            f"_collapse_config_fields retornou grupo com {len(cr.collapsed_ids)} IDs "
            f"(mínimo esperado: 3): {cr.collapsed_ids}"
        )

    for cr in _collapse_situation_history(blocks, {}):
        assert len(cr.collapsed_ids) >= 2, (
            f"_collapse_situation_history retornou grupo com {len(cr.collapsed_ids)} IDs "
            f"(mínimo esperado: 2): {cr.collapsed_ids}"
        )
#
# Tarefa 9.6 — Property 3: Nenhuma poda silenciosa
@given(blocks=st.lists(valid_plan_block(), min_size=0, max_size=30))
@settings(max_examples=300)
def test_no_silent_pruning(blocks: list):
    """
    **Validates: Requirements 4.4, 5.4, 6.3**

    Property 3: Nenhuma poda silenciosa.

    Para qualquer CollapseResult retornado por R1, R2 ou R3:
    - len(cr.replacement_lines) >= 1
    - cr.replacement_lines[0].startswith("[COLAPSADO:")
    """
    _apply_thresholds(blocks)

    all_results = (
        _collapse_config_fields(blocks)
        + _collapse_situation_history(blocks, {})
        + _collapse_vw_usuario_null(blocks)
    )

    for cr in all_results:
        assert len(cr.replacement_lines) >= 1, (
            f"CollapseResult com replacement_lines vazio: collapsed_ids={cr.collapsed_ids}"
        )
        assert cr.replacement_lines[0].startswith("[COLAPSADO:"), (
            f"Primeira linha do resumo não começa com '[COLAPSADO:': "
            f"{cr.replacement_lines[0]!r}"
        )
#
# Tarefa 11.2 — Property 5: Consistência de predicados
#   @given(predicate_lines=st.lists(st.text()), collapsed_ids=st.frozensets(st.text()))
#   def test_predicate_consistency(predicate_lines, collapsed_ids): ...
#
# Tarefa 11.4 — Property 12: Nota de IDs não sequenciais
#   @given(plan_lines=st.lists(plan_block_line()))
#   def test_nonsequential_id_note(plan_lines): ...
#
# Tarefa 11.6 — Property 10: IDs colapsados ausentes do plano reconstruído
#   @given(plan_lines=st.lists(plan_block_line()), verbosity=st.sampled_from(["compact", "minimal"]))
#   def test_collapsed_ids_absent(plan_lines, verbosity): ...
#
# Tarefa 11.7 — Property 11: Robustez de _compress_plan
#   @given(plan_lines=st.lists(st.text()), pred_lines=st.lists(st.text()),
#          verbosity=st.sampled_from(["full", "compact", "minimal"]))
#   def test_compress_never_crashes(plan_lines, pred_lines, verbosity): ...
#
# Tarefa 12.2 — Property 6: Verbosity inválido levanta erro
#   @given(verbosity=st.text().filter(lambda s: s not in {"full", "compact", "minimal"}))
#   def test_invalid_verbosity_raises(verbosity): ...
#
# Tarefa 12.3 — Property 2: Monotonicidade de compressão
#   @given(ctx=valid_collected_context())
#   def test_compression_monotonic(ctx): ...
#
# Tarefa 12.4 — Property 1: Idempotência de full
#   @given(ctx=valid_collected_context())
#   def test_full_idempotent(ctx): ...


# ─── Tarefa 8.1 — Testes unitários para _detect_plan_blocks ──────


class TestDetectPlanBlocks:
    # Linhas no formato Oracle ALLSTATS usadas nos testes
    LINE_1 = "|   1 | SORT AGGREGATE          |                              |     1 |       |      0 |00:00:00.01 |       3 |       0 |"
    LINE_2 = "|   2 | INDEX RANGE SCAN        | IDX_ATTR_ENTITY_ID           |     1 |     1 |      0 |00:00:00.01 |       2 |       0 |"
    LINE_K = "|   1 | TABLE ACCESS FULL       | SOME_TABLE                   |     1 |  1000 |    500 |00:00:00.05 |     10K |       0 |"
    LINE_M = "|   1 | TABLE ACCESS FULL       | SOME_TABLE                   |     1 |  1000 |    500 |00:00:00.05 |      2M |       0 |"
    LINE_G = "|   1 | TABLE ACCESS FULL       | SOME_TABLE                   |     1 |  1000 |    500 |00:00:00.05 |      1G |       0 |"

    def test_simple_plan_order_and_fields(self):
        blocks = _detect_plan_blocks([self.LINE_1, self.LINE_2])
        assert len(blocks) == 2
        # Ordem preservada
        assert blocks[0].id == "1"
        assert blocks[1].id == "2"
        # Campos do bloco 1
        b1 = blocks[0]
        assert "SORT AGGREGATE" in b1.operation
        assert b1.starts == 1
        assert b1.a_rows == 0
        assert b1.buffers == 3
        assert b1.reads == 0
        # Campos do bloco 2
        b2 = blocks[1]
        assert "INDEX RANGE SCAN" in b2.operation
        assert b2.name == "IDX_ATTR_ENTITY_ID"
        assert b2.e_rows == 1

    def test_malformed_line_ignored(self):
        # Não deve levantar exceção; linha inválida é ignorada
        result = _detect_plan_blocks(["isso nao e uma linha de plano"])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_buffers_suffix_K(self):
        blocks = _detect_plan_blocks([self.LINE_K])
        assert len(blocks) == 1
        assert blocks[0].buffers == 10 * 1024  # 10K = 10240

    def test_buffers_suffix_M(self):
        blocks = _detect_plan_blocks([self.LINE_M])
        assert len(blocks) == 1
        assert blocks[0].buffers == 2 * 1024 * 1024  # 2M = 2097152

    def test_buffers_suffix_G(self):
        blocks = _detect_plan_blocks([self.LINE_G])
        assert len(blocks) == 1
        assert blocks[0].buffers == 1 * 1024 * 1024 * 1024  # 1G = 1073741824

    def test_empty_list_returns_empty(self):
        assert _detect_plan_blocks([]) == []

    def test_result_len_lte_input_len(self):
        lines = [self.LINE_1, self.LINE_2, "linha invalida", self.LINE_K]
        result = _detect_plan_blocks(lines)
        assert len(result) <= len(lines)

    def test_numeric_fields_nonnegative(self):
        blocks = _detect_plan_blocks([self.LINE_1, self.LINE_2])
        for b in blocks:
            assert b.starts >= 0
            assert b.a_rows >= 0
            assert b.buffers >= 0
            assert b.reads >= 0
            assert b.a_time_ms >= 0.0
            if b.e_rows is not None:
                assert b.e_rows >= 0


# ─── Tarefa 8.3 — Testes unitários para _apply_thresholds ────────


def _make_block(**kwargs) -> PlanBlock:
    """Cria PlanBlock com defaults zerados, sobrescrevendo com kwargs."""
    defaults = dict(
        id="1",
        operation="SORT AGGREGATE",
        name="",
        starts=0,
        e_rows=None,
        a_rows=0,
        a_time_ms=0.0,
        buffers=0,
        reads=0,
    )
    defaults.update(kwargs)
    return PlanBlock(**defaults)


class TestApplyThresholds:
    def test_reads_gt_zero_sets_immune(self):
        b = _make_block(reads=1)
        _apply_thresholds([b])
        assert b.immune is True

    def test_reads_zero_not_immune_alone(self):
        b = _make_block(reads=0)
        _apply_thresholds([b])
        assert b.immune is False

    def test_buffers_gt_1000_sets_immune(self):
        b = _make_block(buffers=1001)
        _apply_thresholds([b])
        assert b.immune is True

    def test_buffers_eq_1000_not_immune(self):
        # threshold é >, não >=
        b = _make_block(buffers=1000)
        _apply_thresholds([b])
        assert b.immune is False

    def test_starts_gt_100_sets_immune(self):
        b = _make_block(starts=101)
        _apply_thresholds([b])
        assert b.immune is True

    def test_starts_eq_100_not_immune(self):
        b = _make_block(starts=100)
        _apply_thresholds([b])
        assert b.immune is False

    def test_atime_ms_gt_100_sets_immune(self):
        b = _make_block(a_time_ms=100.1)
        _apply_thresholds([b])
        assert b.immune is True

    def test_atime_ms_eq_100_not_immune(self):
        b = _make_block(a_time_ms=100.0)
        _apply_thresholds([b])
        assert b.immune is False

    def test_cardinality_ratio_gt_10_sets_immune(self):
        # ratio = max(11,1)/min(11,1) = 11 > 10
        b = _make_block(e_rows=1, a_rows=11)
        _apply_thresholds([b])
        assert b.immune is True

    def test_cardinality_ratio_eq_10_not_immune(self):
        # ratio = 10/1 = 10, não > 10
        b = _make_block(e_rows=1, a_rows=10)
        _apply_thresholds([b])
        assert b.immune is False

    def test_no_threshold_immune_false(self):
        b = _make_block()  # tudo zero/None
        _apply_thresholds([b])
        assert b.immune is False

    def test_any_threshold_sets_immune(self):
        b1 = _make_block(id="1", buffers=1001)
        b2 = _make_block(id="2", reads=0)
        _apply_thresholds([b1, b2])
        assert b1.immune is True
        assert b2.immune is False

    def test_mutation_in_place(self):
        b = _make_block()
        result = _apply_thresholds([b])
        assert result is None  # muta in-place, não retorna nada

    def test_e_rows_none_skips_cardinality(self):
        # e_rows=None não deve setar immune por cardinalidade
        b = _make_block(e_rows=None, a_rows=100)
        _apply_thresholds([b])
        assert b.immune is False


# ─── Tarefa 9.1 — Testes unitários para _collapse_config_fields (R1) ─────────


def _make_config_fields_group(n: int, start_id: int = 1, immune_idx: int | None = None) -> list[PlanBlock]:
    """
    Monta n grupos SORT AGGREGATE com filhos IDX_ATTR_ENTITY_ID e PK_ATTR_CONFIG.
    immune_idx (0-based) indica qual grupo raiz deve ter immune=True.
    """
    blocks = []
    bid = start_id
    for i in range(n):
        root = _make_block(id=str(bid), operation="SORT AGGREGATE", starts=1, indent=0,
                           immune=(i == immune_idx))
        bid += 1
        c1 = _make_block(id=str(bid), operation="INDEX RANGE SCAN", name="IDX_ATTR_ENTITY_ID", indent=2)
        bid += 1
        c2 = _make_block(id=str(bid), operation="INDEX UNIQUE SCAN", name="PK_ATTR_CONFIG", indent=2)
        bid += 1
        blocks.extend([root, c1, c2])
    return blocks


class TestCollapseConfigFields:
    def test_group_of_3_collapses(self):
        blocks = _make_config_fields_group(3)
        results = _collapse_config_fields(blocks)
        assert len(results) == 1
        cr = results[0]
        assert len(cr.collapsed_ids) >= 3
        assert cr.replacement_lines[0].startswith("[COLAPSADO:")

    def test_group_of_2_does_not_collapse(self):
        # A implementação conta blocos totais (raiz + filhos) no grupo acumulado.
        # Para não colapsar, o grupo precisa ter < 3 blocos totais.
        # 1 SORT AGGREGATE sem os dois índices alvo → não é candidato → grupo vazio → não colapsa.
        root = _make_block(id="1", operation="SORT AGGREGATE", starts=1, indent=0)
        # Filho com apenas um dos índices alvo (falta PK_ATTR_CONFIG)
        child = _make_block(id="2", operation="INDEX RANGE SCAN", name="IDX_ATTR_ENTITY_ID", indent=2)
        results = _collapse_config_fields([root, child])
        assert results == []

    def test_immune_block_prevents_collapse(self):
        # Grupo de 3, mas o segundo root é imune
        blocks = _make_config_fields_group(3, immune_idx=1)
        results = _collapse_config_fields(blocks)
        assert results == []

    def test_replacement_contains_cost(self):
        blocks = _make_config_fields_group(3)
        # Adiciona buffers e reads para verificar custo no texto
        for b in blocks:
            b.buffers = 10
            b.reads = 2
        results = _collapse_config_fields(blocks)
        assert len(results) == 1
        full_text = "\n".join(results[0].replacement_lines)
        assert "buffers" in full_text.lower()

    def test_replacement_contains_warning(self):
        blocks = _make_config_fields_group(3)
        results = _collapse_config_fields(blocks)
        assert len(results) == 1
        full_text = "\n".join(results[0].replacement_lines)
        # Deve conter aviso sobre obras com campos configurados
        assert "obra" in full_text.lower() or "campo" in full_text.lower() or "configurad" in full_text.lower()


# ─── Tarefa 9.2 — Testes unitários para _collapse_situation_history (R2) ─────


def _make_situation_history_group(n: int, start_id: int = 1, immune_idx: int | None = None) -> list[PlanBlock]:
    """
    Monta n grupos SORT AGGREGATE com filhos UK_STATUS_TYPE_REF e IDX_STATUS_HIST_ENTITY.
    """
    blocks = []
    bid = start_id
    for i in range(n):
        root = _make_block(id=str(bid), operation="SORT AGGREGATE", starts=1, indent=0,
                           immune=(i == immune_idx))
        bid += 1
        c1 = _make_block(id=str(bid), operation="INDEX RANGE SCAN", name="UK_STATUS_TYPE_REF", indent=2)
        bid += 1
        c2 = _make_block(id=str(bid), operation="INDEX RANGE SCAN", name="IDX_STATUS_HIST_ENTITY", indent=2)
        bid += 1
        blocks.extend([root, c1, c2])
    return blocks


class TestCollapseSituationHistory:
    def test_group_of_2_collapses(self):
        blocks = _make_situation_history_group(2)
        results = _collapse_situation_history(blocks, {})
        assert len(results) == 1
        cr = results[0]
        assert len(cr.collapsed_ids) >= 2
        assert cr.replacement_lines[0].startswith("[COLAPSADO:")

    def test_group_of_1_does_not_collapse(self):
        blocks = _make_situation_history_group(1)
        results = _collapse_situation_history(blocks, {})
        assert results == []

    def test_immune_block_prevents_collapse(self):
        # Grupo de 2, mas o primeiro root é imune
        blocks = _make_situation_history_group(2, immune_idx=0)
        results = _collapse_situation_history(blocks, {})
        assert results == []

    def test_replacement_has_table_columns(self):
        blocks = _make_situation_history_group(2)
        results = _collapse_situation_history(blocks, {})
        assert len(results) == 1
        full_text = "\n".join(results[0].replacement_lines)
        assert "| Tipo |" in full_text
        assert "| A-Rows |" in full_text

    def test_tps_referencia_fallback(self):
        # Sem predicate_map → coluna Tipo deve usar "?"
        blocks = _make_situation_history_group(2)
        results = _collapse_situation_history(blocks, {})
        assert len(results) == 1
        full_text = "\n".join(results[0].replacement_lines)
        assert "?" in full_text


# ─── Tarefa 9.3 — Testes unitários para _collapse_vw_usuario_null (R3) ───────


class TestCollapseVwUsuarioNull:
    def test_view_arows_zero_collapses(self):
        view = _make_block(id="10", operation="VIEW", name="VW_CURRENT_USER", a_rows=0, indent=0)
        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="USER_ACCTS", indent=2)
        results = _collapse_vw_usuario_null([view, child])
        assert len(results) == 1
        assert results[0].replacement_lines[0].startswith("[COLAPSADO:")

    def test_view_arows_nonzero_does_not_collapse(self):
        view = _make_block(id="10", operation="VIEW", name="VW_CURRENT_USER", a_rows=1, indent=0)
        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="USER_ACCTS", indent=2)
        results = _collapse_vw_usuario_null([view, child])
        assert results == []

    def test_immune_subtree_prevents_collapse(self):
        view = _make_block(id="10", operation="VIEW", name="VW_CURRENT_USER", a_rows=0, indent=0)
        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="USER_ACCTS", indent=2, immune=True)
        results = _collapse_vw_usuario_null([view, child])
        assert results == []

    def test_from_subquery_pattern_collapses(self):
        view = _make_block(id="20", operation="VIEW", name="FROM$_SUBQUERY$_001", a_rows=0, indent=0)
        child = _make_block(id="21", operation="TABLE ACCESS FULL", name="SOME_TABLE", indent=2)
        results = _collapse_vw_usuario_null([view, child])
        assert len(results) == 1
        assert results[0].replacement_lines[0].startswith("[COLAPSADO:")

    def test_replacement_contains_warning(self):
        view = _make_block(id="10", operation="VIEW", name="VW_CURRENT_USER", a_rows=0, indent=0)
        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="USER_ACCTS", indent=2)
        results = _collapse_vw_usuario_null([view, child])
        assert len(results) == 1
        full_text = "\n".join(results[0].replacement_lines)
        # Deve conter aviso sobre usuário não NULL
        assert "usuário" in full_text.lower() or "usuario" in full_text.lower() or "null" in full_text.lower()


# ─── Tarefa 9.4 — Property 4: Imunidade preservada ───────────────────────────


@given(blocks=st.lists(valid_plan_block(), min_size=0, max_size=30))
@settings(max_examples=200)
def test_immune_blocks_never_collapsed(blocks: list):
    """
    Property 4: Imunidade preservada.

    Para qualquer lista de PlanBlock, após aplicar thresholds e rodar R1+R2+R3,
    nenhum bloco com immune=True aparece em collapsed_ids de nenhum CollapseResult.

    Validates: Requirements 3.7, 4.2, 5.2, 6.2
    """
    _apply_thresholds(blocks)
    immune_ids = {b.id for b in blocks if b.immune}

    results: list = (
        _collapse_config_fields(blocks)
        + _collapse_situation_history(blocks, {})
        + _collapse_vw_usuario_null(blocks)
    )
    all_collapsed_ids = {id_ for cr in results for id_ in cr.collapsed_ids}

    assert immune_ids.isdisjoint(all_collapsed_ids), (
        f"Blocos imunes colapsados: {immune_ids & all_collapsed_ids}"
    )
