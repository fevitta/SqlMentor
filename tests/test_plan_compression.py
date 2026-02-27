"""

Testes de compressão de plano Oracle — unitários e property-based (hypothesis).


Estrutura:

  - Estratégias Hypothesis para PlanBlock e CollectedContext

  - Helpers para gerar linhas no formato Oracle ALLSTATS

  - Placeholders indicando onde cada property test será adicionado (tarefas 8–12)
"""

import re
import re as _re_module

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from sqlmentor.collector import CollectedContext
from sqlmentor.parser import ParsedSQL
from sqlmentor.report import (
    PlanBlock,
    _add_nonsequential_id_note,
    _apply_thresholds,
    _collapse_config_fields,
    _collapse_orphan_predicates_by_ids,
    _collapse_situation_history,
    _collapse_view_zero_rows,
    _compress_plan,
    _detect_plan_blocks,
    _split_plan_predicates,
    to_markdown,
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

        operation=st.sampled_from(

            [

                "SORT AGGREGATE",

                "INDEX RANGE SCAN",

                "TABLE ACCESS FULL",

                "NESTED LOOPS",

                "HASH JOIN",

                "VIEW",

                "FILTER",

                "SORT ORDER BY",

            ]

        ),

        name=st.one_of(

            st.just(""),

            st.from_regex(r"[A-Z][A-Z0-9_]{2,20}", fullmatch=True),

        ),

        starts=st.integers(min_value=0, max_value=10_000),

        e_rows=st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),

        a_rows=st.integers(min_value=0, max_value=1_000_000),

        a_time_ms=st.floats(

            min_value=0.0, max_value=100_000.0, allow_nan=False, allow_infinity=False

        ),

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
    # O f-string {:05.2f} usa arredondamento IEEE 754 (round-half-to-even em C),
    # que pode divergir do round() do Python em half-points dependendo da plataforma.
    # Simulamos o round-trip real: formata → parseia, e comparamos com o parseado.
    # Tolerância de 10ms (1 centésimo de segundo) é o máximo de erro possível.

    assert abs(p.a_time_ms - block.a_time_ms) <= 10.0, (
        f"a_time_ms: diff > 10ms. parseado={p.a_time_ms}, original={block.a_time_ms}"
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

        + _collapse_view_zero_rows(blocks)
    )


    for cr in all_results:

        assert len(cr.replacement_lines) >= 1, (

            f"CollapseResult com replacement_lines vazio: collapsed_ids={cr.collapsed_ids}"
        )

        assert cr.replacement_lines[0].startswith("[COLAPSADO:"), (

            f"Primeira linha do resumo não começa com '[COLAPSADO:': {cr.replacement_lines[0]!r}"
        )



#

# Tarefa 11.2 — Property 5: Consistência de predicados



@given(

    pred_entries=st.lists(

        st.from_regex(r"   \d{1,3} - (access|filter)\([^)]+\)", fullmatch=True),

        min_size=0,

        max_size=20,

    ),

    collapsed_ids=st.frozensets(st.from_regex(r"\d{1,3}", fullmatch=True), max_size=5),
)

@settings(max_examples=300)

def test_predicate_consistency(pred_entries, collapsed_ids):
    """

    Property 5: Consistência de predicados.

    Validates: Requirements 7.1, 7.4


    A função só filtra predicados dentro da seção "Predicate Information",

    portanto o header é sempre incluído para ativar o modo de filtragem.
    """

    # Sempre inclui o header para que a função entre no modo de filtragem

    predicate_lines = ["Predicate Information (identified by operation id):"] + list(pred_entries)


    result = _collapse_orphan_predicates_by_ids(predicate_lines, set(collapsed_ids))

    assert isinstance(result, list)


    _PRED_ID = _re_module.compile(r"^\s*(\d+)\s*-\s*(access|filter)\(")

    for line in result:

        m = _PRED_ID.match(line.strip() if isinstance(line, str) else "")

        if m:

            assert m.group(1) not in collapsed_ids, (

                f"Linha com ID colapsado encontrada no resultado: {line!r}"
            )



# Tarefa 11.4 — Property 12: Nota de IDs não sequenciais

@given(

    ids=st.lists(

        st.integers(min_value=1, max_value=100),

        min_size=0,

        max_size=20,

        unique=True,

    ).map(sorted)
)

@settings(max_examples=300)

def test_nonsequential_id_note(ids):
    """

    Property 12: Nota de IDs não sequenciais.

    Validates: Requirements 8.1, 8.2
    """

    plan_lines = ["Plan hash value: 9999999"]

    for id_val in ids:

        plan_lines.append(

            f"|   {id_val} | TABLE ACCESS FULL       | T                            |"

            f"     1 |     1 |      1 |00:00:00.01 |       1 |       0 |"
        )


    result = _add_nonsequential_id_note(plan_lines)


    has_gaps = any(ids[i + 1] - ids[i] > 1 for i in range(len(ids) - 1)) if len(ids) > 1 else False


    note_present = any("IDs não sequenciais" in line for line in result)


    if has_gaps:

        assert note_present, f"Nota ausente para IDs com salto: {ids}"

    else:

        assert not note_present, f"Nota inserida indevidamente para IDs sequenciais: {ids}"



# Tarefa 11.6 — Property 10: IDs colapsados ausentes do plano reconstruído

@given(

    blocks=st.lists(valid_plan_block(), min_size=0, max_size=20),

    verbosity=st.sampled_from(["compact", "minimal"]),
)

@settings(max_examples=200)

def test_collapsed_ids_absent_from_reconstructed_plan(blocks, verbosity):
    """

    Property 10: IDs colapsados ausentes do plano reconstruído.

    Validates: Requirements 9.3, 9.5
    """

    plan_lines = ["Plan hash value: 9999999"]

    for b in blocks:

        plan_lines.append(_format_plan_block_line(b))


    result_plan, _result_preds = _compress_plan(plan_lines, [], verbosity)


    _apply_thresholds(blocks)
    all_collapses = (

        _collapse_config_fields(blocks)

        + _collapse_situation_history(blocks, {})

        + _collapse_view_zero_rows(blocks)
    )

    all_collapsed_ids = {cid for cr in all_collapses for cid in cr.collapsed_ids}


    _PLAN_ID_RE = _re_module.compile(r"\|\*?\s*(\d+)\s*\|")

    for line in result_plan:

        m = _PLAN_ID_RE.match(line)

        if m:

            assert m.group(1) not in all_collapsed_ids, (

                f"ID colapsado {m.group(1)!r} encontrado no plano reconstruído"
            )


    for cr in all_collapses:

        summary_count = sum(

            1
            for line in result_plan

            if line.startswith("[COLAPSADO:") and line in cr.replacement_lines
        )

        if cr.collapsed_ids:

            assert summary_count <= 1, (

                f"CollapseResult emitiu {summary_count} resumos (esperado: ≤1)"
            )



# Tarefa 11.7 — Property 11: Robustez de _compress_plan

@given(

    plan_lines=st.lists(st.text(max_size=200), max_size=50),

    pred_lines=st.lists(st.text(max_size=200), max_size=20),

    verbosity=st.sampled_from(["full", "compact", "minimal"]),
)

@settings(max_examples=300)

def test_compress_never_crashes(plan_lines, pred_lines, verbosity):
    """

    Property 11: Robustez de _compress_plan.

    Validates: Requirements 9.6
    """

    result_plan, result_preds = _compress_plan(plan_lines, pred_lines, verbosity)

    assert isinstance(result_plan, list)

    assert isinstance(result_preds, list)

    assert all(isinstance(line, str) for line in result_plan)

    assert all(isinstance(line, str) for line in result_preds)



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

    LINE_2 = "|   2 | INDEX RANGE SCAN        | IDX_GENERIC_A                |     1 |     1 |      0 |00:00:00.01 |       2 |       0 |"

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

        assert b2.name == "IDX_GENERIC_A"

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



def _make_config_fields_group(

    n: int, start_id: int = 1, immune_idx: int | None = None

) -> list[PlanBlock]:
    """

    Monta n grupos SORT AGGREGATE com filhos INDEX SCAN genéricos (≥2 índices).

    Padrão agnóstico — sem nomes de objetos reais.

    immune_idx (0-based) indica qual grupo raiz deve ter immune=True.
    """

    blocks = []

    bid = start_id

    for i in range(n):

        root = _make_block(

            id=str(bid), operation="SORT AGGREGATE", starts=1, indent=0, immune=(i == immune_idx)
        )

        bid += 1

        c1 = _make_block(

            id=str(bid), operation="INDEX RANGE SCAN", name="IDX_GENERIC_A", indent=2
        )

        bid += 1

        c2 = _make_block(

            id=str(bid),

            operation="INDEX UNIQUE SCAN",

            name="IDX_GENERIC_B",

            indent=2,
        )

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

        # 1 SORT AGGREGATE com apenas 1 filho INDEX → não atinge mínimo de 2 índices → não é candidato

        root = _make_block(id="1", operation="SORT AGGREGATE", starts=1, indent=0)

        child = _make_block(

            id="2", operation="INDEX RANGE SCAN", name="IDX_GENERIC_A", indent=2
        )

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

        # Deve conter aviso genérico sobre custo potencial

        assert "⚠️" in full_text or "verifique" in full_text.lower() or "custo" in full_text.lower()



# ─── Tarefa 9.2 — Testes unitários para _collapse_situation_history (R2) ─────



def _make_situation_history_group(

    n: int, start_id: int = 1, immune_idx: int | None = None

) -> list[PlanBlock]:
    """

    Monta n grupos SORT AGGREGATE com filhos INDEX SCAN genéricos (≥2 índices).

    Padrão agnóstico — sem nomes de objetos reais.
    """

    blocks = []

    bid = start_id

    for i in range(n):

        root = _make_block(

            id=str(bid), operation="SORT AGGREGATE", starts=1, indent=0, immune=(i == immune_idx)
        )

        bid += 1

        c1 = _make_block(

            id=str(bid), operation="INDEX RANGE SCAN", name="IDX_GENERIC_C", indent=2
        )

        bid += 1

        c2 = _make_block(

            id=str(bid), operation="INDEX RANGE SCAN", name="IDX_GENERIC_D", indent=2
        )

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

        assert "| Filtro |" in full_text

        assert "| A-Rows |" in full_text


    def test_tps_referencia_fallback(self):

        # Sem predicate_map → coluna Filtro deve usar "?"

        blocks = _make_situation_history_group(2)

        results = _collapse_situation_history(blocks, {})

        assert len(results) == 1

        full_text = "\n".join(results[0].replacement_lines)

        assert "?" in full_text



# ─── Tarefa 9.3 — Testes unitários para _collapse_view_zero_rows (R3) ───────



class TestCollapseVwUsuarioNull:

    def test_view_arows_zero_collapses(self):

        view = _make_block(id="10", operation="VIEW", name="ANY_VIEW", a_rows=0, indent=0)

        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="SOME_TABLE", indent=2)

        results = _collapse_view_zero_rows([view, child])

        assert len(results) == 1

        assert results[0].replacement_lines[0].startswith("[COLAPSADO:")


    def test_view_arows_nonzero_does_not_collapse(self):

        view = _make_block(id="10", operation="VIEW", name="ANY_VIEW", a_rows=1, indent=0)

        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="SOME_TABLE", indent=2)

        results = _collapse_view_zero_rows([view, child])

        assert results == []


    def test_immune_subtree_prevents_collapse(self):

        view = _make_block(id="10", operation="VIEW", name="ANY_VIEW", a_rows=0, indent=0)

        child = _make_block(

            id="11", operation="TABLE ACCESS FULL", name="SOME_TABLE", indent=2, immune=True
        )

        results = _collapse_view_zero_rows([view, child])

        assert results == []


    def test_view_without_name_collapses(self):

        # VIEW sem nome também deve ser colapsada — agnóstico ao nome

        view = _make_block(id="20", operation="VIEW", name="", a_rows=0, indent=0)

        child = _make_block(id="21", operation="TABLE ACCESS FULL", name="SOME_TABLE", indent=2)

        results = _collapse_view_zero_rows([view, child])

        assert len(results) == 1

        assert results[0].replacement_lines[0].startswith("[COLAPSADO:")


    def test_replacement_contains_warning(self):

        view = _make_block(id="10", operation="VIEW", name="ANY_VIEW", a_rows=0, indent=0)

        child = _make_block(id="11", operation="TABLE ACCESS FULL", name="SOME_TABLE", indent=2)

        results = _collapse_view_zero_rows([view, child])

        assert len(results) == 1

        full_text = "\n".join(results[0].replacement_lines)

        assert "⚠️" in full_text or "custo" in full_text.lower()



# ─── Tarefa 9.4 — Property 4: Imunidade preservada ───────────────────────────



@given(blocks=st.lists(valid_plan_block(), min_size=0, max_size=30, unique_by=lambda b: b.id))

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

        + _collapse_view_zero_rows(blocks)
    )

    all_collapsed_ids = {id_ for cr in results for id_ in cr.collapsed_ids}


    assert immune_ids.isdisjoint(all_collapsed_ids), (

        f"Blocos imunes colapsados: {immune_ids & all_collapsed_ids}"
    )



# ─── Tarefa 11.1 — Testes unitários para _collapse_orphan_predicates_by_ids ──



class TestCollapseOrphanPredicates:

    PRED_HEADER = "Predicate Information (identified by operation id):"

    PRED_2 = "   2 - access(\"CAMPO\"='VALOR')"

    PRED_3 = '   3 - filter("OUTRO_CAMPO" IS NOT NULL)'

    PRED_5 = '   5 - access("X"=:B1)'


    def test_collapsed_ids_removes_predicate_lines(self):

        lines = [self.PRED_HEADER, self.PRED_2, self.PRED_3]

        result = _collapse_orphan_predicates_by_ids(lines, {"2"})

        ids_in_result = {

            m.group(1)
            for line in result

            for m in [re.match(r"^\s*(\d+)\s*-\s*(access|filter)\(", line.strip())]
            if m

        }

        assert "2" not in ids_in_result


    def test_non_collapsed_ids_preserved(self):

        lines = [self.PRED_HEADER, self.PRED_2, self.PRED_3, self.PRED_5]

        result = _collapse_orphan_predicates_by_ids(lines, {"2"})

        ids_in_result = {

            m.group(1)
            for line in result

            for m in [re.match(r"^\s*(\d+)\s*-\s*(access|filter)\(", line.strip())]
            if m

        }

        assert "3" in ids_in_result

        assert "5" in ids_in_result


    def test_empty_collapsed_ids_returns_unchanged(self):

        lines = [self.PRED_HEADER, self.PRED_2, self.PRED_3]

        result = _collapse_orphan_predicates_by_ids(lines, set())
        assert result == lines


    def test_pruned_adds_note(self):

        lines = [self.PRED_HEADER, self.PRED_2, self.PRED_3]

        result = _collapse_orphan_predicates_by_ids(lines, {"2", "3"})

        assert any("predicados de blocos colapsados omitidos" in line for line in result)



# ─── Tarefa 11.3 — Testes unitários para _add_nonsequential_id_note ──────────



PLAN_HASH_LINE = "Plan hash value: 1234567890"

LINE_ID_1 = "|   1 | SORT AGGREGATE          |                              |     1 |       |      0 |00:00:00.01 |       3 |       0 |"

LINE_ID_2 = "|   2 | INDEX RANGE SCAN        | IDX_GENERIC_A                |     1 |     1 |      0 |00:00:00.01 |       2 |       0 |"

LINE_ID_3 = "|   3 | TABLE ACCESS FULL       | SOME_TABLE                   |     1 |  1000 |    500 |00:00:00.05 |      10 |       0 |"

LINE_ID_5 = "|   5 | TABLE ACCESS FULL       | OTHER_TABLE                  |     1 |   100 |     50 |00:00:00.02 |       5 |       0 |"



class TestAddNonsequentialIdNote:

    def test_sequential_ids_no_note(self):

        lines = [PLAN_HASH_LINE, LINE_ID_1, LINE_ID_2, LINE_ID_3]

        result = _add_nonsequential_id_note(lines)

        assert not any("IDs não sequenciais" in line for line in result)


    def test_nonsequential_ids_inserts_note(self):

        lines = [PLAN_HASH_LINE, LINE_ID_1, LINE_ID_3, LINE_ID_5]

        result = _add_nonsequential_id_note(lines)

        assert any("IDs não sequenciais" in line for line in result)


    def test_note_position_after_plan_hash(self):

        lines = [PLAN_HASH_LINE, LINE_ID_1, LINE_ID_3]

        result = _add_nonsequential_id_note(lines)

        hash_idx = next(i for i, ln in enumerate(result) if "Plan hash value" in ln)

        note_idx = next((i for i, ln in enumerate(result) if "IDs não sequenciais" in ln), None)

        assert note_idx is not None

        assert note_idx == hash_idx + 1


    def test_no_plan_hash_line_no_note(self):

        # Sem linha "Plan hash value", nota não deve ser inserida

        lines = [LINE_ID_1, LINE_ID_3, LINE_ID_5]

        result = _add_nonsequential_id_note(lines)

        assert not any("IDs não sequenciais" in line for line in result)



# ─── Tarefa 11.5 — Testes unitários para _compress_plan ──────────────────────



def _make_config_fields_plan_lines(n_groups: int) -> list[str]:

    """Gera linhas de plano com n_groups de scalar subqueries com padrão index lookup."""

    lines = ["Plan hash value: 1234567890"]

    bid = 1

    for _ in range(n_groups):

        lines.append(

            f"|   {bid} | SORT AGGREGATE          |                              |"

            f"     1 |       |      0 |00:00:00.01 |       3 |       0 |"
        )

        bid += 1

        lines.append(

            f"|   {bid} |  INDEX RANGE SCAN       | IDX_GENERIC_A                |"

            f"     1 |     1 |      0 |00:00:00.01 |       2 |       0 |"
        )

        bid += 1

        lines.append(

            f"|   {bid} |  INDEX UNIQUE SCAN      | IDX_GENERIC_B                |"

            f"     1 |     1 |      0 |00:00:00.01 |       1 |       0 |"
        )

        bid += 1
    return lines



class TestCompressPlan:

    def test_full_verbosity_returns_unchanged(self):

        plan = _make_config_fields_plan_lines(3)

        preds = ['   1 - access("X"=:B1)']

        result_plan, result_preds = _compress_plan(plan, preds, "full")
        assert result_plan == plan
        assert result_preds == preds


    def test_no_parseable_lines_returns_original(self):

        plan = ["-- sem linhas parseáveis", "outra linha qualquer"]

        preds = []

        result_plan, result_preds = _compress_plan(plan, preds, "compact")
        assert result_plan == plan
        assert result_preds == preds


    def test_collapsable_plan_removes_collapsed_ids(self):

        plan = _make_config_fields_plan_lines(3)

        result_plan, _preds = _compress_plan(plan, [], "compact")

        # IDs 1–9 devem ter sido colapsados; nenhum deve aparecer como linha de plano normal

        _PLAN_ID_RE = re.compile(r"\|\*?\s*(\d+)\s*\|")

        ids_in_result = {m.group(1) for line in result_plan for m in [_PLAN_ID_RE.match(line)] if m}

        # Deve haver pelo menos um bloco [COLAPSADO:] no resultado

        assert any(line.startswith("[COLAPSADO:") for line in result_plan)

        # Nenhum ID original deve aparecer como linha de plano normal

        for id_val in [str(i) for i in range(1, 10)]:

            assert id_val not in ids_in_result


    def test_each_collapse_result_emits_one_summary(self):

        plan = _make_config_fields_plan_lines(3)

        result_plan, _preds = _compress_plan(plan, [], "compact")

        # Conta blocos [COLAPSADO:] — deve haver exatamente 1 para este plano

        summary_count = sum(1 for line in result_plan if line.startswith("[COLAPSADO:"))

        assert summary_count == 1



# ─── Tarefa 12 — Testes para to_markdown e integração CLI/MCP ────



def _make_minimal_ctx(**kwargs) -> CollectedContext:

    """Cria CollectedContext mínimo para testes de to_markdown."""

    parsed = ParsedSQL(raw_sql="SELECT 1 FROM DUAL", sql_type="SELECT")

    defaults = dict(parsed_sql=parsed)

    defaults.update(kwargs)

    return CollectedContext(**defaults)



def _make_runtime_plan_with_collapses() -> list[str]:

    """Plano com 3 grupos R1 — compact deve colapsar em [COLAPSADO:."""

    lines = [

        "SQL_ID  abc123, child number 0",

        "-------------------------------------",

        "Plan hash value: 1234567890",

        "",

        "| Id | Operation               | Name                          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |",

        "|----|-------------------------|-------------------------------|--------|--------|--------|------------|---------|--------|",

    ]

    bid = 1

    for _ in range(3):

        lines += [

            f"|   {bid} | SORT AGGREGATE          |                               |      1 |        |      0 |00:00:00.01 |       3 |      0 |",

            f"|   {bid + 1} |  INDEX RANGE SCAN       | IDX_GENERIC_A                 |      1 |      1 |      0 |00:00:00.01 |       2 |      0 |",

            f"|   {bid + 2} |  INDEX UNIQUE SCAN      | IDX_GENERIC_B                 |      1 |      1 |      0 |00:00:00.01 |       1 |      0 |",

        ]

        bid += 3
    return lines



# ─── Tarefa 12.1 — Testes unitários para to_markdown com verbosity ────────────



class TestToMarkdownVerbosity:

    def test_invalid_verbosity_raises_value_error(self):

        ctx = _make_minimal_ctx()

        with pytest.raises(ValueError, match="verbosity"):

            to_markdown(ctx, verbosity="verbose")


    def test_invalid_verbosity_message_lists_valid_values(self):

        ctx = _make_minimal_ctx()

        with pytest.raises(ValueError) as exc_info:

            to_markdown(ctx, verbosity="wrong")

        msg = str(exc_info.value)

        assert "full" in msg

        assert "compact" in msg

        assert "minimal" in msg


    def test_full_produces_no_colapsado_blocks(self):

        ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

        result = to_markdown(ctx, verbosity="full")

        assert "[COLAPSADO:" not in result


    def test_compact_produces_colapsado_blocks_for_complex_plan(self):

        ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

        result = to_markdown(ctx, verbosity="compact")

        assert "[COLAPSADO:" in result


    def test_minimal_has_no_execution_plan_section(self):

        ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

        result = to_markdown(ctx, verbosity="minimal")

        # minimal não deve conter o bloco de código do plano de execução

        assert "Runtime Execution Plan" not in result

        assert "Execution Plan" not in result


    def test_minimal_has_no_ddl(self):

        from sqlmentor.collector import TableContext


        table = TableContext(

            schema="SCHEMA",

            name="MINHA_TABELA",

            object_type="TABLE",

            ddl="CREATE TABLE MINHA_TABELA (ID NUMBER)",

            stats={"num_rows": 10000},

            columns=[],

            indexes=[],

            constraints=[],
        )

        ctx = _make_minimal_ctx(tables=[table])

        result = to_markdown(ctx, verbosity="minimal")

        assert "CREATE TABLE" not in result


    def test_default_verbosity_is_compact(self):

        ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

        result_default = _strip_timestamps(to_markdown(ctx))

        result_compact = _strip_timestamps(to_markdown(ctx, verbosity="compact"))
        assert result_default == result_compact


    def test_full_returns_string(self):

        ctx = _make_minimal_ctx()

        assert isinstance(to_markdown(ctx, verbosity="full"), str)


    def test_compact_returns_string(self):

        ctx = _make_minimal_ctx()

        assert isinstance(to_markdown(ctx, verbosity="compact"), str)


    def test_minimal_returns_string(self):

        ctx = _make_minimal_ctx()

        assert isinstance(to_markdown(ctx, verbosity="minimal"), str)



# ─── Tarefa 12.2 — Property 6: Verbosity inválido levanta erro ───────────────



@given(verbosity=st.text().filter(lambda s: s not in {"full", "compact", "minimal"}))

@settings(max_examples=200)

def test_invalid_verbosity_always_raises(verbosity: str):
    """

    Property 6: Verbosity inválido levanta erro.

    Validates: Requirements 1.5


    Para qualquer string fora do conjunto {"full", "compact", "minimal"},

    to_markdown sempre levanta ValueError.
    """

    ctx = _make_minimal_ctx()

    with pytest.raises(ValueError):

        to_markdown(ctx, verbosity=verbosity)



# ─── Tarefa 12.3 — Property 2: Monotonicidade de compressão ──────────────────



def test_compression_monotonic_with_complex_plan():
    """

    Property 2: Monotonicidade de compressão.

    Validates: Requirements 1.3, 1.4


    Para um plano com views complexas:

    len(minimal) <= len(compact) <= len(full)


    Usa plano fixo (não hypothesis) porque CollectedContext completo

    requer conexão Oracle para geração arbitrária.
    """

    ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

    minimal_lines = to_markdown(ctx, verbosity="minimal").splitlines()

    compact_lines = to_markdown(ctx, verbosity="compact").splitlines()

    full_lines = to_markdown(ctx, verbosity="full").splitlines()


    assert len(minimal_lines) <= len(compact_lines), (

        f"minimal ({len(minimal_lines)}) > compact ({len(compact_lines)})"
    )

    assert len(compact_lines) <= len(full_lines), (

        f"compact ({len(compact_lines)}) > full ({len(full_lines)})"
    )



def test_compression_monotonic_without_plan():
    """

    Monotonicidade também vale para ctx sem plano de execução.
    """

    ctx = _make_minimal_ctx()

    minimal_lines = to_markdown(ctx, verbosity="minimal").splitlines()

    compact_lines = to_markdown(ctx, verbosity="compact").splitlines()

    full_lines = to_markdown(ctx, verbosity="full").splitlines()


    assert len(minimal_lines) <= len(compact_lines)

    assert len(compact_lines) <= len(full_lines)



# ─── Tarefa 12.4 — Property 1: Idempotência de full ─────────────────────────



def _strip_timestamps(text: str) -> str:

    """Remove timestamps do output para comparação de idempotência."""
    import re


    # Padrão: datas como "2025-01-25 18:12:21" ou "25 18:12:21"

    return re.sub(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "TIMESTAMP", text)



def test_full_idempotent_with_complex_plan():
    """

    Property 1: Idempotência de full.

    Validates: Requirements 1.2, 9.2


    to_markdown(ctx, "full") chamado duas vezes produz output idêntico

    (desconsiderando timestamps gerados em runtime).
    """

    ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

    result1 = _strip_timestamps(to_markdown(ctx, verbosity="full"))

    result2 = _strip_timestamps(to_markdown(ctx, verbosity="full"))

    assert result1 == result2



def test_full_idempotent_without_plan():

    """Idempotência de full também vale para ctx sem plano."""

    ctx = _make_minimal_ctx()

    r1 = _strip_timestamps(to_markdown(ctx, verbosity="full"))

    r2 = _strip_timestamps(to_markdown(ctx, verbosity="full"))

    assert r1 == r2



def test_compact_idempotent():

    """compact também deve ser determinístico (mesmo input → mesmo output)."""

    ctx = _make_minimal_ctx(runtime_plan=_make_runtime_plan_with_collapses())

    r1 = _strip_timestamps(to_markdown(ctx, verbosity="compact"))

    r2 = _strip_timestamps(to_markdown(ctx, verbosity="compact"))

    assert r1 == r2



# ─── Tarefa 13 — Testes de integração com fixture de plano real ──────────────



class TestIntegrationRealPlan:
    """

    Testes de integração usando a fixture sample_plan.txt.

    Valida que compact produz menos linhas que full e que full é estável.
    """


    def _load_plan_lines(self) -> list[str]:

        return _load_fixture("sample_plan.txt")


    # ─── Tarefa 13.2 — compact produz menos linhas que full ──────


    def test_compact_produces_fewer_lines_than_full(self):
        """

        Validates: Requirements 1.3


        compact deve colapsar R1 (4 grupos), R2 (3 grupos) e R3 (VIEW com A-Rows=0),

        produzindo menos linhas que full.
        """

        plan_lines = self._load_plan_lines()

        plan_only, pred_lines = _split_plan_predicates(plan_lines)


        full_plan, full_preds = _compress_plan(plan_only, pred_lines, "full")

        compact_plan, compact_preds = _compress_plan(plan_only, pred_lines, "compact")


        full_total = len(full_plan) + len(full_preds)

        compact_total = len(compact_plan) + len(compact_preds)


        assert compact_total < full_total, (

            f"compact ({compact_total} linhas) não é menor que full ({full_total} linhas)"
        )


    def test_compact_contains_colapsado_blocks(self):

        """compact deve gerar pelo menos um bloco [COLAPSADO: para a fixture."""

        plan_lines = self._load_plan_lines()

        plan_only, pred_lines = _split_plan_predicates(plan_lines)

        compact_plan, _ = _compress_plan(plan_only, pred_lines, "compact")

        assert any(line.startswith("[COLAPSADO:") for line in compact_plan)


    def test_full_contains_no_colapsado_blocks(self):

        """full não deve gerar nenhum bloco [COLAPSADO:."""

        plan_lines = self._load_plan_lines()

        plan_only, pred_lines = _split_plan_predicates(plan_lines)

        full_plan, _ = _compress_plan(plan_only, pred_lines, "full")

        assert not any(line.startswith("[COLAPSADO:") for line in full_plan)


    def test_compact_via_to_markdown_fewer_lines_than_full(self):
        """

        Validates: Requirements 1.3


        Verifica monotonicidade via to_markdown com a fixture real.
        """

        ctx = _make_minimal_ctx(runtime_plan=self._load_plan_lines())

        full_lines = to_markdown(ctx, verbosity="full").splitlines()

        compact_lines = to_markdown(ctx, verbosity="compact").splitlines()

        assert len(compact_lines) < len(full_lines), (

            f"compact ({len(compact_lines)} linhas) não é menor que full ({len(full_lines)} linhas)"
        )


    # ─── Tarefa 13.3 — regressão de full contra baseline ─────────


    def test_full_regression_stable(self):
        """

        Validates: Requirements 1.2, 10.1, 10.2


        full chamado duas vezes com a mesma fixture produz output idêntico

        (desconsiderando timestamps).
        """

        ctx = _make_minimal_ctx(runtime_plan=self._load_plan_lines())

        result1 = _strip_timestamps(to_markdown(ctx, verbosity="full"))

        result2 = _strip_timestamps(to_markdown(ctx, verbosity="full"))

        assert result1 == result2, "full não é estável — output difere entre chamadas"


    def test_compact_regression_stable(self):

        """compact também deve ser determinístico com a fixture real."""

        ctx = _make_minimal_ctx(runtime_plan=self._load_plan_lines())

        result1 = _strip_timestamps(to_markdown(ctx, verbosity="compact"))

        result2 = _strip_timestamps(to_markdown(ctx, verbosity="compact"))

        assert result1 == result2, "compact não é estável — output difere entre chamadas"


    def test_full_plan_preserves_all_original_ids(self):
        """

        Validates: Requirements 1.2


        full deve preservar todos os IDs do plano original sem remoção.
        """
        import re


        plan_lines = self._load_plan_lines()

        plan_only, pred_lines = _split_plan_predicates(plan_lines)


        _PLAN_ID_RE = re.compile(r"\|\*?\s*(\d+)\s*\|")

        original_ids = {m.group(1) for line in plan_only for m in [_PLAN_ID_RE.match(line)] if m}


        full_plan, _ = _compress_plan(plan_only, pred_lines, "full")

        full_ids = {m.group(1) for line in full_plan for m in [_PLAN_ID_RE.match(line)] if m}


        assert original_ids == full_ids, f"full removeu IDs: {original_ids - full_ids}"


    def test_compact_removes_collapsed_ids_from_plan(self):
        """

        Validates: Requirements 9.5


        compact não deve conter nenhum ID que foi colapsado.
        """
        import re

        from sqlmentor.report import (
            _apply_thresholds,
            _collapse_config_fields,
            _collapse_situation_history,
            _collapse_view_zero_rows,
            _detect_plan_blocks,
        )


        plan_lines = self._load_plan_lines()

        plan_only, pred_lines = _split_plan_predicates(plan_lines)


        blocks = _detect_plan_blocks(plan_only)

        _apply_thresholds(blocks)
        all_collapses = (

            _collapse_config_fields(blocks)

            + _collapse_situation_history(blocks, {})

            + _collapse_view_zero_rows(blocks)
        )

        all_collapsed_ids = {cid for cr in all_collapses for cid in cr.collapsed_ids}


        compact_plan, _ = _compress_plan(plan_only, pred_lines, "compact")


        _PLAN_ID_RE = re.compile(r"\|\*?\s*(\d+)\s*\|")

        ids_in_compact = {

            m.group(1) for line in compact_plan for m in [_PLAN_ID_RE.match(line)] if m

        }


        overlap = all_collapsed_ids & ids_in_compact

        assert not overlap, f"IDs colapsados ainda presentes no plano compact: {overlap}"

