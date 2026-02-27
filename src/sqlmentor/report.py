"""

Gerador de relatórios para consumo por IA.


Suporta Markdown (otimizado pra colar em chat) e JSON (pra integração via API).
"""


import json
import re

from dataclasses import dataclass, field
from datetime import datetime

from typing import Any


from sqlmentor.collector import CollectedContext, TableContext


# ─── Dataclasses para compressão do plano ────────────────────────



@dataclass

class PlanBlock:

    """Representa uma operação do plano de execução Oracle."""


    id: str

    operation: str

    name: str

    starts: int

    e_rows: int | None

    a_rows: int

    a_time_ms: float

    buffers: int

    reads: int

    indent: int = 0

    immune: bool = False

    children: list["PlanBlock"] = field(default_factory=list)



@dataclass

class CollapseResult:

    """Resultado de um colapso de blocos do plano."""


    collapsed_ids: set[str]

    replacement_lines: list[str]



# Regex para parsear linha do plano ALLSTATS (runtime)

# Colunas: Id | Operation | Name | Starts | E-Rows | A-Rows | A-Time | Buffers | Reads

# Captura indentação da coluna Operation separadamente para inferir hierarquia

_PLAN_ROW = re.compile(

    r"\|\*?\s*(\d+)\s*\|"  # Id

    r"(\s*)(\S[^|]*?)\s*\|"  # (indent_spaces)(Operation) — captura espaços iniciais

    r"\s*(.*?)\s*\|"  # Name

    r"\s*(\d+)\s*\|"  # Starts

    r"\s*(\d*)\s*\|"  # E-Rows (pode estar vazio)

    r"\s*(\d+)\s*\|"  # A-Rows

    r"\s*(\d+:\d+:\d+\.\d+)\s*\|"  # A-Time

    r"\s*(\d+[KMG]?)\s*\|"  # Buffers

    r"\s*(\d+)\s*\|",  # Reads
)


# Regex para parsear linha do plano EXPLAIN PLAN (estimado)

# Colunas: Id | Operation | Name | Rows | Bytes | Cost (%CPU) | Time

_PLAN_ROW_ESTIMATED = re.compile(

    r"\|\*?\s*(\d+)\s*\|"  # Id

    r"(\s*)(\S[^|]*?)\s*\|"  # (indent_spaces)(Operation)

    r"\s*(.*?)\s*\|"  # Name

    r"\s*(\d*)\s*\|"  # Rows (pode estar vazio)

    r"\s*(\d*[KMG]?)\s*\|"  # Bytes (pode estar vazio)

    r"\s*(\d*)\s*[^|]*\|"  # Cost (%CPU) — ignora o (%CPU)

    r"\s*(\d+:\d+:\d+)?\s*\|",  # Time (pode estar vazio)
)


_BUFFERS_MULTIPLIER = {"K": 1024, "M": 1024**2, "G": 1024**3}


# Regex genérico para extrair apenas o Id de qualquer linha de plano Oracle

# Funciona tanto com ALLSTATS quanto com EXPLAIN PLAN

_PLAN_ROW_ID = re.compile(r"^\|\*?\s*(\d+)\s*\|")


# Thresholds de proteção (R5)

_THRESHOLD_BUFFERS = 1_000

_THRESHOLD_STARTS = 100

_THRESHOLD_ATIME_MS = 100.0

_CARDINALITY_RATIO = 10



def _parse_buffers(s: str) -> int:

    """Converte '10K', '2M', '137K' etc. para inteiro."""

    s = s.strip()

    if not s:

        return 0

    suffix = s[-1].upper()

    if suffix in _BUFFERS_MULTIPLIER:

        return int(float(s[:-1]) * _BUFFERS_MULTIPLIER[suffix])
    return int(s)



def _parse_atime_ms(s: str) -> float:

    """Converte 'HH:MM:SS.ss' para milissegundos."""

    parts = s.split(":")

    if len(parts) != 3:

        return 0.0

    return (int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])) * 1000



def _detect_plan_blocks(plan_lines: list[str]) -> list[PlanBlock]:
    """

    Parseia o plano em lista plana de PlanBlock.

    Suporta dois formatos Oracle:

    - ALLSTATS LAST (runtime): colunas Starts/E-Rows/A-Rows/A-Time/Buffers/Reads

    - EXPLAIN PLAN (estimado): colunas Rows/Bytes/Cost/Time


    No formato estimado, campos de runtime (starts, a_rows, buffers, reads, a_time_ms)

    ficam zerados — R5 (imunidade por threshold) fica inativo, mas R1/R2/R3 funcionam.

    A indentação da coluna Operation é preservada em PlanBlock.indent.
    """

    blocks: list[PlanBlock] = []

    for line in plan_lines:

        # Tenta formato ALLSTATS primeiro

        m = _PLAN_ROW.match(line)

        if m:

            indent = len(m.group(2))

            e_rows_str = m.group(6).strip()

            blocks.append(

                PlanBlock(

                    id=m.group(1),

                    operation=m.group(3).strip(),

                    name=m.group(4).strip(),

                    starts=int(m.group(5)),

                    e_rows=int(e_rows_str) if e_rows_str else None,

                    a_rows=int(m.group(7)),

                    a_time_ms=_parse_atime_ms(m.group(8)),

                    buffers=_parse_buffers(m.group(9)),

                    reads=int(m.group(10)),

                    indent=indent,
                )
            )
            continue


        # Tenta formato EXPLAIN PLAN (estimado)

        m2 = _PLAN_ROW_ESTIMATED.match(line)

        if m2:

            indent = len(m2.group(2))

            e_rows_str = m2.group(5).strip()

            blocks.append(

                PlanBlock(

                    id=m2.group(1),

                    operation=m2.group(3).strip(),

                    name=m2.group(4).strip(),

                    starts=0,  # não disponível no plano estimado

                    e_rows=int(e_rows_str) if e_rows_str else None,

                    a_rows=0,  # não disponível no plano estimado

                    a_time_ms=0.0,  # não disponível no plano estimado

                    buffers=0,  # não disponível no plano estimado

                    reads=0,  # não disponível no plano estimado

                    indent=indent,
                )
            )


    return blocks



def _apply_thresholds(blocks: list[PlanBlock]) -> None:

    """Marca blocks.immune=True para operações que atendem R5."""

    for b in blocks:

        if b.reads > 0:

            b.immune = True
            continue

        if b.buffers > _THRESHOLD_BUFFERS:

            b.immune = True
            continue

        if b.starts > _THRESHOLD_STARTS:

            b.immune = True
            continue

        if b.a_time_ms > _THRESHOLD_ATIME_MS:

            b.immune = True
            continue

        if b.e_rows and b.a_rows and b.e_rows > 0 and b.a_rows > 0:

            ratio = max(b.e_rows, b.a_rows) / min(b.e_rows, b.a_rows)

            if ratio > _CARDINALITY_RATIO:

                b.immune = True



def _is_scalar_index_subquery(b: PlanBlock, blocks: list[PlanBlock], i: int) -> tuple[bool, list[PlanBlock]]:
    """

    Verifica se o bloco b é raiz de uma scalar subquery com filhos exclusivamente INDEX SCAN.

    Retorna (é_candidato, child_blocks_incluindo_raiz).

    Critérios agnósticos:

    - b.operation == "SORT AGGREGATE" e b.starts <= 1

    - Todos os filhos diretos (indent > b.indent) são operações INDEX (RANGE/UNIQUE/FULL SCAN)

    - Há pelo menos 2 filhos INDEX
    """

    if b.operation != "SORT AGGREGATE" or b.starts > 1:

        return False, []

    j = i + 1

    child_blocks: list[PlanBlock] = [b]

    index_children = 0

    while j < len(blocks) and blocks[j].indent > b.indent:

        child = blocks[j]

        child_blocks.append(child)

        if "INDEX" in child.operation.upper():

            index_children += 1

        elif child.operation.upper() not in ("NESTED LOOPS", "NESTED LOOPS OUTER"):

            # Filho não-index e não-join → não é o padrão esperado

            return False, []

        j += 1

    if index_children >= 2:

        return True, child_blocks

    return False, []



def _collapse_config_fields(blocks: list[PlanBlock]) -> list[CollapseResult]:
    """

    Detecta e colapsa grupos de scalar subqueries com padrão de índices (R1).

    Padrão agnóstico: SORT AGGREGATE (starts≤1) cujos filhos são todos INDEX SCAN (≥2 índices).

    Mínimo: ≥ 3 grupos consecutivos com o padrão.
    """

    results: list[CollapseResult] = []


    group: list[PlanBlock] = []

    group_root_ids: list[str] = []


    def _flush_group():

        nonlocal group, group_root_ids

        if len(group_root_ids) >= 3:

            immune_any = any(b.immune for b in group)

            if not immune_any:

                all_ids = {b.id for b in group}

                total_buffers = sum(b.buffers for b in group)

                total_reads = sum(b.reads for b in group)

                a_rows_nonzero = [

                    b for b in group if b.operation == "SORT AGGREGATE" and b.a_rows > 0

                ]

                lines = [

                    f"[COLAPSADO: {len(group_root_ids)} scalar subqueries — padrão index lookup repetido]",

                    "  Resultado: A-Rows=0 em todos"

                    if not a_rows_nonzero

                    else f"  Resultado: {len(a_rows_nonzero)} com A-Rows>0",

                    f"  Custo total: {total_buffers:,} buffers, {total_reads} reads",

                    "  ⚠️ Verifique se esses lookups têm custo real nos seus dados.",

                ]

                results.append(CollapseResult(collapsed_ids=all_ids, replacement_lines=lines))

        group.clear()

        group_root_ids.clear()


    i = 0

    while i < len(blocks):

        b = blocks[i]

        is_candidate, child_blocks = _is_scalar_index_subquery(b, blocks, i)

        if is_candidate:

            group.extend(child_blocks)

            group_root_ids.append(b.id)

            i += len(child_blocks)
            continue

        else:

            _flush_group()

        i += 1


    _flush_group()
    return results



def _collapse_situation_history(

    blocks: list[PlanBlock], predicate_map: dict[str, list[str]]

) -> list[CollapseResult]:
    """

    Detecta e colapsa scalar subqueries repetidas com padrão de index lookup (R2).

    Padrão agnóstico: SORT AGGREGATE (starts≤1) cujos filhos são todos INDEX SCAN (≥2 índices).

    Mínimo: ≥ 2 grupos consecutivos com o padrão.

    Extrai valor de filtro de igualdade dos predicados para exibir na tabela resumo.
    """

    results: list[CollapseResult] = []

    # Regex genérico: captura qualquer coluna = 'valor' nos predicados

    _FILTER_VAL = re.compile(r"(\w+)\s*=\s*'([^']+)'")


    group_entries: list[tuple[PlanBlock, list[PlanBlock]]] = []


    def _flush():

        nonlocal group_entries

        if len(group_entries) >= 2:

            all_blocks = [b for _, entry in group_entries for b in entry]

            immune_any = any(b.immune for b in all_blocks)

            if not immune_any:

                all_ids = {b.id for b in all_blocks}

                total_buffers = sum(b.buffers for b in all_blocks)

                table_rows = []

                for root_block, child_blocks in group_entries:

                    # Busca primeiro filtro de igualdade nos predicados dos filhos

                    filter_val = "?"

                    for b in child_blocks:

                        for p in predicate_map.get(b.id, []):

                            m = _FILTER_VAL.search(p)

                            if m:

                                filter_val = f"{m.group(1)}={m.group(2)}"

                                break

                        if filter_val != "?":

                            break

                    table_rows.append((filter_val, root_block.a_rows, root_block.buffers))


                lines = [

                    f"[COLAPSADO: {len(group_entries)} scalar subqueries — padrão index lookup repetido]",

                    "  | Filtro | A-Rows | Buffers |",

                    "  |--------|--------|---------|",

                ]

                for fval, ar, buf in table_rows:

                    lines.append(f"  | {fval} | {ar} | {buf} |")

                lines.append(f"  Custo total: {total_buffers:,} buffers")

                results.append(CollapseResult(collapsed_ids=all_ids, replacement_lines=lines))

        group_entries.clear()


    i = 0

    while i < len(blocks):

        b = blocks[i]

        is_candidate, child_blocks = _is_scalar_index_subquery(b, blocks, i)

        if is_candidate:

            group_entries.append((b, child_blocks))

            i += len(child_blocks)
            continue

        else:

            _flush()

        i += 1


    _flush()
    return results



def _collapse_view_zero_rows(blocks: list[PlanBlock]) -> list[CollapseResult]:
    """

    Detecta e colapsa subárvores VIEW com A-Rows=0 (R3).

    Padrão agnóstico: qualquer operação VIEW com a_rows == 0 e subtree sem imunes.

    Só aplicada em planos runtime — no estimado a_rows é sempre 0.
    """

    results: list[CollapseResult] = []

    i = 0

    while i < len(blocks):

        b = blocks[i]

        if "VIEW" in b.operation.upper() and b.a_rows == 0:

            j = i + 1

            subtree: list[PlanBlock] = [b]

            while j < len(blocks) and blocks[j].indent > b.indent:

                subtree.append(blocks[j])

                j += 1

            immune_any = any(sb.immune for sb in subtree)

            if not immune_any:

                all_ids = {sb.id for sb in subtree}

                total_buffers = sum(sb.buffers for sb in subtree)

                view_label = b.name if b.name else b.operation

                lines = [

                    f"[COLAPSADO: VIEW '{view_label}' — A-Rows=0, {total_buffers:,} buffers]",

                    "  ⚠️ Esta subárvore pode ter custo significativo com outros dados de entrada.",

                ]

                results.append(CollapseResult(collapsed_ids=all_ids, replacement_lines=lines))

                i = j
                continue

        i += 1
    return results



def _build_predicate_map(plan_lines: list[str]) -> dict[str, list[str]]:
    """

    Constrói mapa id → lista de predicados a partir da seção Predicate Information.
    """

    pred_map: dict[str, list[str]] = {}

    in_predicates = False

    current_id: str | None = None

    _PRED_ID = re.compile(r"^\s*(\d+)\s*-\s*(access|filter)\((.+)")


    for line in plan_lines:

        stripped = line.strip()

        if stripped.startswith("Predicate Information"):

            in_predicates = True
            continue

        if not in_predicates:
            continue

        m = _PRED_ID.match(stripped)

        if m:

            current_id = m.group(1)

            pred_map.setdefault(current_id, []).append(m.group(3))

        elif current_id and stripped and not stripped.startswith("---"):

            if re.match(r"^[A-Z]", stripped) and not stripped[0].isdigit():

                in_predicates = False

                current_id = None

            else:

                pred_map[current_id].append(stripped)

    return pred_map



def _add_nonsequential_id_note(plan_lines: list[str]) -> list[str]:
    """

    Adiciona nota no cabeçalho do plano quando IDs não sequenciais são detectados (R6).
    """

    ids = []

    for line in plan_lines:

        m = _PLAN_ROW_ID.match(line)

        if m:

            ids.append(int(m.group(1)))


    has_gaps = any(ids[i + 1] - ids[i] > 1 for i in range(len(ids) - 1)) if len(ids) > 1 else False

    if not has_gaps:
        return plan_lines


    # Insere nota após a linha "Plan hash value"

    result = []

    inserted = False

    for line in plan_lines:
        result.append(line)

        if not inserted and line.strip().startswith("Plan hash value"):
            result.append(

                "ℹ️ IDs não sequenciais são normais — operações internas de views/subqueries"
            )

            result.append("   são numeradas pelo Oracle mas omitidas do DBMS_XPLAN.")

            inserted = True
    return result



def _split_plan_predicates(lines: list[str]) -> tuple[list[str], list[str]]:
    """

    Divide a lista de linhas do plano em duas partes:

    - plan_lines: tudo antes da seção "Predicate Information" (exclusive)

    - predicate_lines: a partir da linha "Predicate Information" (inclusive)


    Se não houver seção "Predicate Information", retorna (lines, []).
    """

    for i, line in enumerate(lines):

        if "Predicate Information" in line:

            return lines[:i], lines[i:]

    return lines, []



def _is_estimated_plan(plan_lines: list[str]) -> bool:
    """

    Detecta se o plano é EXPLAIN PLAN (estimado) ou ALLSTATS LAST (runtime).

    O plano estimado tem cabeçalho com colunas 'Rows | Bytes | Cost'.
    """

    for line in plan_lines:

        if "| Rows  |" in line or "| Rows |" in line:

            return True

        if "| Starts |" in line:

            return False

    # Fallback: tenta parsear a primeira linha de dados

    for line in plan_lines:

        if _PLAN_ROW.match(line):

            return False

        if _PLAN_ROW_ESTIMATED.match(line):

            return True

    return False


def _compress_plan(

    plan_lines: list[str],

    predicate_lines: list[str],

    verbosity: str,

) -> tuple[list[str], list[str]]:
    """

    Aplica compressão ao plano e predicados conforme nível de verbosidade.

    Retorna (plano_comprimido, predicados_comprimidos).


    Orquestra R1–R6 em sequência.

    R3 (colapso de VW_CURRENT_USER com a_rows=0) só é aplicada em planos runtime,

    pois no plano estimado a_rows é sempre 0 — colapsar seria incorreto.
    """

    if verbosity == "full":

        return plan_lines, predicate_lines


    blocks = _detect_plan_blocks(plan_lines)

    _apply_thresholds(blocks)

    pred_map = _build_predicate_map(plan_lines)

    is_estimated = _is_estimated_plan(plan_lines)


    # Coleta todos os colapsos

    all_collapses: list[CollapseResult] = []

    all_collapses.extend(_collapse_config_fields(blocks))

    all_collapses.extend(_collapse_situation_history(blocks, pred_map))

    # R3 só faz sentido em planos runtime — no estimado a_rows é sempre 0

    if not is_estimated:

        all_collapses.extend(_collapse_view_zero_rows(blocks))


    # Conjunto de todos os IDs colapsados

    all_collapsed_ids: set[str] = set()

    for cr in all_collapses:
        all_collapsed_ids.update(cr.collapsed_ids)


    if not all_collapsed_ids:

        # Nada a colapsar — só adiciona nota de IDs não sequenciais

        return _add_nonsequential_id_note(plan_lines), predicate_lines


    # Reconstrói o plano substituindo blocos colapsados pelos resumos

    # Mapeia id → CollapseResult para lookup rápido

    id_to_collapse: dict[str, CollapseResult] = {}

    for cr in all_collapses:

        for cid in cr.collapsed_ids:

            id_to_collapse[cid] = cr


    new_plan: list[str] = []

    emitted_collapses: set[int] = set()  # id(CollapseResult) já emitidos

    for line in plan_lines:

        m = _PLAN_ROW_ID.match(line)

        if m:

            line_id = m.group(1)

            if line_id in all_collapsed_ids:

                cr = id_to_collapse[line_id]

                cr_key = id(cr)

                if cr_key not in emitted_collapses:

                    emitted_collapses.add(cr_key)

                    new_plan.extend(cr.replacement_lines)

                # Pula a linha original
                continue

        new_plan.append(line)


    # Adiciona nota de IDs não sequenciais

    new_plan = _add_nonsequential_id_note(new_plan)


    # Colapsa predicados dos IDs removidos (R4)

    new_preds = _collapse_orphan_predicates_by_ids(predicate_lines, all_collapsed_ids)


    return new_plan, new_preds



def _collapse_orphan_predicates_by_ids(plan_lines: list[str], collapsed_ids: set[str]) -> list[str]:
    """

    Remove predicados cujos IDs foram colapsados.

    Adiciona nota de quantos foram omitidos.
    """

    if not collapsed_ids:
        return plan_lines


    result = []

    pruned = 0

    in_predicates = False

    skipping = False

    _PRED_ID = re.compile(r"^\s*(\d+)\s*-\s*(access|filter)\(")


    for line in plan_lines:

        stripped = line.strip()

        if stripped.startswith("Predicate Information"):

            in_predicates = True

            skipping = False
            result.append(line)
            continue

        if not in_predicates:
            result.append(line)
            continue


        m = _PRED_ID.match(stripped)

        if m:

            pred_id = m.group(1)

            if pred_id in collapsed_ids:

                skipping = True

                pruned += 1
                continue

            else:

                skipping = False
                result.append(line)
                continue


        if skipping and stripped and not stripped.startswith("---"):

            if re.match(r"^[A-Z]", stripped) and not stripped[0].isdigit():

                in_predicates = False

                skipping = False
                result.append(line)

            else:

                pruned += 1
            continue

        result.append(line)


    if pruned > 0:

        result.append(f"({pruned} predicados de blocos colapsados omitidos — ver resumos acima)")

    return result



def to_markdown(ctx: CollectedContext, verbosity: str = "compact") -> str:
    """

    Gera relatório Markdown estruturado pro LLM analisar.


    Formato otimizado pra context window de LLM — conciso mas completo.


    Args:

        ctx: Contexto coletado do Oracle.

        verbosity: Nível de compressão do plano.

            "full"    — sem compressão além de P1/P3 já existentes.

            "compact" — todas as podas ativas (default).

            "minimal" — só hotspots + runtime stats + optimizer params.
    """

    _VALID_VERBOSITY = ("full", "compact", "minimal")

    if verbosity not in _VALID_VERBOSITY:

        raise ValueError(f"verbosity inválido: '{verbosity}'. Use: {', '.join(_VALID_VERBOSITY)}")


    lines: list[str] = []


    lines.append("# SQL Tuning Context Report")

    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if ctx.db_version:

        lines.append(f"**Database:** {ctx.db_version}")
    lines.append("")


    # ─── Modo minimal: só hotspots + runtime stats + optimizer params ─

    if verbosity == "minimal":

        plan_source = ctx.runtime_plan or ctx.execution_plan or []

        hotspots = _format_hotspots(plan_source) if plan_source else ""

        if hotspots:

            lines.append("## Hotspots")

            lines.append(hotspots)
            lines.append("")

        if ctx.runtime_stats:

            lines.append("## Runtime Stats (V$SQL)")

            lines.append(_format_runtime_stats(ctx.runtime_stats))
            lines.append("")

        if ctx.optimizer_params:

            lines.append("## Parâmetros do Otimizador")

            lines.append(_format_optimizer_params(ctx.optimizer_params))
            lines.append("")

        if ctx.errors:

            lines.append("## ⚠️ Erros na Coleta")

            for err in ctx.errors:

                lines.append(f"- {err}")
            lines.append("")

        return "\n".join(lines)


    # ─── SQL Original ────────────────────────────────────────────

    lines.append("## 1. SQL Original")

    lines.append(f"**Tipo:** {ctx.parsed_sql.sql_type}")

    lines.append(f"**Tabelas referenciadas:** {', '.join(ctx.parsed_sql.table_names)}")


    if ctx.parsed_sql.where_columns:
        lines.append(

            f"**Colunas em WHERE:** {', '.join(sorted(set(ctx.parsed_sql.where_columns)))}"
        )

    if ctx.parsed_sql.join_columns:

        lines.append(f"**Colunas em JOIN:** {', '.join(sorted(set(ctx.parsed_sql.join_columns)))}")

    if ctx.parsed_sql.order_columns:
        lines.append(

            f"**Colunas em ORDER BY:** {', '.join(sorted(set(ctx.parsed_sql.order_columns)))}"
        )

    if ctx.parsed_sql.group_columns:
        lines.append(

            f"**Colunas em GROUP BY:** {', '.join(sorted(set(ctx.parsed_sql.group_columns)))}"
        )

    if ctx.parsed_sql.subqueries:

        lines.append(f"**Subqueries:** {ctx.parsed_sql.subqueries}")

    if ctx.parsed_sql.functions:

        func_names = sorted(f"{f['schema']}.{f['name']}" for f in ctx.parsed_sql.functions)

        lines.append(f"**Funções PL/SQL:** {', '.join(func_names)}")

    lines.append("")

    lines.append("```sql")

    lines.append(ctx.parsed_sql.raw_sql)

    lines.append("```")
    lines.append("")


    # ─── Plano de Execução ────────────────────────────────────────

    section = 2

    if ctx.execution_plan:

        lines.append(f"## {section}. Execution Plan (Estimado)")

        lines.append("```")

        plan_cleaned = _strip_sql_from_plan(ctx.execution_plan)

        plan_cleaned, pruned_ids = _prune_dead_operations(plan_cleaned)

        plan_cleaned = _prune_orphan_predicates(plan_cleaned, pruned_ids)

        plan_lines, predicate_lines = _split_plan_predicates(plan_cleaned)

        plan_compressed, pred_compressed = _compress_plan(plan_lines, predicate_lines, verbosity)

        for line in plan_compressed + pred_compressed:
            lines.append(line)

        lines.append("```")
        lines.append("")

        section += 1


    # ─── Runtime: Plano Real ──────────────────────────────────────

    if ctx.runtime_plan:

        lines.append(f"## {section}. Runtime Execution Plan (ALLSTATS LAST)")
        lines.append(

            "> Coletado com `STATISTICS_LEVEL = ALL` na sessão. "

            "O plano mostra a última execução (LAST)."
        )

        executions = ctx.runtime_stats.get("executions", 1) if ctx.runtime_stats else 1

        if executions and executions > 1:
            lines.append(

                f"> ⚠️ SQL_ID já existia no shared pool com {executions} execuções. "

                "Stats de V$SQL são acumuladas, mas o plano ALLSTATS LAST é da última execução."
            )
        lines.append("")

        lines.append("```")

        plan_cleaned = _strip_sql_from_plan(ctx.runtime_plan)

        plan_cleaned, pruned_ids = _prune_dead_operations(plan_cleaned)

        plan_cleaned = _prune_orphan_predicates(plan_cleaned, pruned_ids)

        # Separa plan_lines de predicate_lines antes de comprimir

        plan_lines, predicate_lines = _split_plan_predicates(plan_cleaned)

        plan_compressed, pred_compressed = _compress_plan(plan_lines, predicate_lines, verbosity)

        for line in plan_compressed + pred_compressed:
            lines.append(line)

        lines.append("```")
        lines.append("")

        section += 1


        # ─── Hotspots ────────────────────────────────────────────

        hotspots = _format_hotspots(ctx.runtime_plan)

        if hotspots:

            lines.append(f"## {section}. Hotspots")

            lines.append(hotspots)
            lines.append("")

            section += 1


    # ─── Implicit Conversions ─────────────────────────────────────

    plan_source = ctx.runtime_plan or ctx.execution_plan or []

    conversions = _extract_implicit_conversions(plan_source)

    if conversions:

        lines.append(f"## {section}. Implicit Conversions (Predicate Info)")

        for conv in conversions:

            lines.append(f"- Id {conv['id']}: `{conv['function']}` — conversão aplicada na coluna")
        lines.append("")

        section += 1


    # ─── Runtime: Métricas de Execução ────────────────────────────

    if ctx.runtime_stats:

        lines.append(f"## {section}. Runtime Stats (V$SQL)")

        lines.append(_format_runtime_stats(ctx.runtime_stats))
        lines.append("")

        section += 1


    # ─── Runtime: Wait Events ─────────────────────────────────────

    if ctx.wait_events:

        lines.append(f"## {section}. Wait Events")

        lines.append(_format_wait_events(ctx.wait_events))
        lines.append("")

        section += 1


    # ─── View Expansion Summary ──────────────────────────────────

    if ctx.view_expansions:

        lines.append(f"## {section}. View Expansion Summary")
        lines.append("")


        # Extrai tabelas realmente acessadas no plano via mapa de índices

        plan_source = ctx.runtime_plan or ctx.execution_plan or []

        plan_ops = _parse_plan_operations(plan_source) if plan_source else []

        plan_tables = _extract_plan_tables(plan_ops, ctx.index_table_map)


        # Tabelas do SQL original (sem views)

        user_tables = set()

        for t in ctx.tables:

            if t.object_type != "VIEW":

                user_tables.add(f"{t.schema}.{t.name}")


        for view_name, inner_tables in ctx.view_expansions.items():

            lines.append(f"**{view_name}** ({len(inner_tables)} tabelas internas)")

            in_plan = []

            not_in_plan = []

            for tbl in inner_tables:

                short_name = tbl.split(".")[-1].upper()

                if short_name in plan_tables:

                    in_plan.append(tbl)

                else:

                    not_in_plan.append(tbl)


            if in_plan:

                lines.append(f"- Acessadas no plano: {', '.join(in_plan)}")

            if not_in_plan:
                lines.append(

                    f"- Não acessadas (join elimination ou não necessárias): {', '.join(not_in_plan)}"
                )
            lines.append("")


        # Tabelas compartilhadas entre views

        if len(ctx.view_expansions) > 1:

            view_table_lists = list(ctx.view_expansions.values())

            shared = set(view_table_lists[0])

            for vt in view_table_lists[1:]:

                shared &= set(vt)

            if shared:

                lines.append(f"**Tabelas compartilhadas entre views:** {', '.join(sorted(shared))}")
                lines.append("")


        if user_tables:

            lines.append(f"**Tabelas diretas no SQL do usuário:** {', '.join(sorted(user_tables))}")
            lines.append("")


        section += 1


    # ─── Funções PL/SQL ──────────────────────────────────────────

    if ctx.function_ddls:

        for func_key, ddl_text in ctx.function_ddls.items():

            lines.append(f"## {section}. Função: {func_key}")
            lines.append("")

            lines.append("### DDL")

            lines.append("```sql")

            lines.append(ddl_text)

            lines.append("```")
            lines.append("")

            section += 1

    elif ctx.parsed_sql.functions:

        # Funções detectadas mas DDL não coletada (sem --expand-functions)

        lines.append(f"## {section}. Funções PL/SQL Referenciadas")
        lines.append("")

        for func in ctx.parsed_sql.functions:

            lines.append(f"- {func['schema']}.{func['name']}")
        lines.append("")

        lines.append("*(use `--expand-functions` para incluir DDL)*")
        lines.append("")

        section += 1


    # ─── Contexto por Tabela ──────────────────────────────────────

    # Pré-calcula colunas referenciadas no SQL pra filtrar output

    referenced_cols = _get_sql_referenced_columns(ctx)


    # Separa tabelas pequenas (< 1000 rows) pra formato compacto

    small_tables: list[TableContext] = []

    normal_tables: list[TableContext] = []

    for table in ctx.tables:

        num_rows = (table.stats or {}).get("num_rows", None)

        if table.object_type == "VIEW" or num_rows is None or num_rows >= 1000:

            normal_tables.append(table)

        else:

            small_tables.append(table)


    # Ordena pra garantir saída determinística entre execuções

    normal_tables.sort(key=lambda t: f"{t.schema}.{t.name}")

    small_tables.sort(key=lambda t: f"{t.schema}.{t.name}")


    for table in normal_tables:

        obj_label = "View" if table.object_type == "VIEW" else "Tabela"

        lines.append(f"## {section}. {obj_label}: {table.schema}.{table.name}")
        lines.append("")

        section += 1


        # View sem detalhes (--expand-views não foi passado)

        if table.object_type == "VIEW" and not table.ddl and not table.columns:

            lines.append("*(view — use `--expand-views` para detalhar)*")
            lines.append("")
            continue


        # DDL só pra views (mostra o SELECT interno)

        if table.ddl and table.object_type == "VIEW":

            lines.append("### DDL")

            lines.append("```sql")

            lines.append(_strip_view_column_list(table.ddl.strip()))

            lines.append("```")
            lines.append("")


        # Stats (só pra tabelas — views não têm stats em ALL_TABLES)

        if table.stats:

            lines.append("### Estatísticas Gerais")

            lines.append(_format_table_stats(table.stats))
            lines.append("")


        # Columns — filtra pra mostrar só colunas referenciadas no SQL

        # Monta mapa de FKs: coluna → "SCHEMA.TABELA" referenciada

        fk_map = _build_fk_map(table.constraints)


        if table.columns:

            display_cols = _filter_columns_by_sql(table.columns, referenced_cols)

            has_stats = any(c.get("num_distinct") is not None for c in display_cols)

            omitted = len(table.columns) - len(display_cols)

            if has_stats:

                lines.append("### Colunas e Estatísticas")

                lines.append(_format_column_stats(display_cols, fk_map))

            else:

                lines.append("### Colunas")

                lines.append(_format_column_structure(display_cols))

            if omitted > 0:

                lines.append(f"\n*({omitted} colunas não referenciadas no SQL omitidas)*")
            lines.append("")


        # Indexes

        if table.indexes:

            lines.append("### Índices")

            lines.append(_format_indexes(table.indexes))
            lines.append("")


        # Partitions

        if table.partitions:

            lines.append("### Partições")

            lines.append(_format_partitions(table.partitions))
            lines.append("")


        # Histograms

        if table.histograms:

            lines.append("### Histogramas")

            for col_name, hist_data in table.histograms.items():

                lines.append(f"**{col_name}:** {len(hist_data)} buckets")
            lines.append("")


    # ─── Tabelas pequenas (< 1000 rows) — formato compacto ───────

    if small_tables:

        lines.append(f"## {section}. Tabelas Pequenas (< 1.000 rows)")
        lines.append("")

        section += 1

        for table in small_tables:

            lines.append(_format_small_table(table))
        lines.append("")


    # ─── Optimizer Params ─────────────────────────────────────────

    if ctx.optimizer_params:

        lines.append("## Parâmetros do Otimizador")

        lines.append(_format_optimizer_params(ctx.optimizer_params))
        lines.append("")


    # ─── Erros ────────────────────────────────────────────────────

    if ctx.errors:

        lines.append("## ⚠️ Erros na Coleta")

        for err in ctx.errors:

            lines.append(f"- {err}")
        lines.append("")


    return "\n".join(lines)



def _strip_sql_from_plan(plan_lines: list[str]) -> list[str]:

    """Remove o SQL repetido do output do DISPLAY_CURSOR.


    O Oracle repete o SQL no topo do plano (SQL_ID + texto da query).

    Como já temos o SQL na seção 1, isso é duplicação pura.

    Mantém só o Plan hash value em diante.
    """

    result = []

    found_plan_hash = False

    for line in plan_lines:

        stripped = line.strip()

        if not found_plan_hash:

            # Mantém linha do SQL_ID (útil como referência)

            if stripped.startswith("SQL_ID"):
                result.append(line)
                continue

            # Pula tudo até achar "Plan hash value"

            if stripped.startswith("Plan hash value"):

                found_plan_hash = True
                result.append(line)
                continue

            # Pula linhas de separação (----) antes do plan hash

            if stripped and all(c == "-" for c in stripped):
                continue

            # Pula o SQL repetido
            continue
        result.append(line)

    # Fallback: se não achou Plan hash value, retorna tudo

    return result if found_plan_hash else plan_lines



def _prune_dead_operations(plan_lines: list[str]) -> tuple[list[str], set[str]]:

    """Remove operações com Starts=0 AND A-Rows=0 do plano ALLSTATS LAST.


    Essas operações nunca executaram em runtime — são ramos eliminados

    pelo otimizador (ex: UNION ALL branches não acessados em views).

    Reduz drasticamente o tamanho de planos de views complexas.


    Retorna (linhas_filtradas, ids_podados) para permitir poda de

    predicados órfãos via _prune_orphan_predicates.
    """

    result = []

    pruned = 0

    pruned_ids: set[str] = set()

    id_pattern = re.compile(r"\|\*?\s*(\d+)\s*\|")

    dead_pattern = re.compile(

        r"\|\*?\s*\d+\s*\|"  # Id

        r"\s*.+?\s*\|"  # Operation

        r"\s*.*?\s*\|"  # Name

        r"\s*0\s*\|"  # Starts = 0

        r"\s*\d*\s*\|"  # E-Rows (qualquer)

        r"\s*0\s*\|"  # A-Rows = 0
    )

    for line in plan_lines:

        if dead_pattern.match(line):

            m = id_pattern.match(line)

            if m:

                pruned_ids.add(m.group(1))

            pruned += 1
            continue
        result.append(line)


    if pruned > 0:
        result.append("")

        result.append(f"({pruned} operações com Starts=0/A-Rows=0 omitidas — ramos não executados)")


    return result, pruned_ids



def _prune_orphan_predicates(plan_lines: list[str], pruned_ids: set[str]) -> list[str]:

    """Remove predicados cujos Ids foram podados por _prune_dead_operations.


    A seção 'Predicate Information' referencia Ids do plano. Se a operação

    foi removida (Starts=0/A-Rows=0), o predicado é órfão — não agrega

    valor pra análise de tuning.


    Formato das linhas de predicado:

        '   16 - access("IDX"."COL"=:B1)'

        '         filter("X"."Y"="Z"."W")'   ← continuação (sem Id)
    """

    if not pruned_ids:
        return plan_lines


    result = []

    pruned_preds = 0

    in_predicates = False

    skipping = False

    pred_id_pattern = re.compile(r"^\s*(\d+)\s*-\s*(access|filter)\(")


    for line in plan_lines:

        stripped = line.strip()


        # Detecta início da seção de predicados

        if stripped.startswith("Predicate Information"):

            in_predicates = True

            skipping = False
            result.append(line)
            continue


        if not in_predicates:
            result.append(line)
            continue


        # Linha de predicado com Id

        m = pred_id_pattern.match(stripped)

        if m:

            pred_id = m.group(1)

            if pred_id in pruned_ids:

                skipping = True

                pruned_preds += 1
                continue

            else:

                skipping = False
                result.append(line)
                continue


        # Linha de continuação (indentada, sem Id) — segue o estado anterior

        if skipping and stripped and not stripped.startswith("---"):

            # Continuação de predicado órfão — pula

            # Mas se parece início de nova seção, para de pular

            if re.match(r"^[A-Z]", stripped) and not stripped[0].isdigit():

                in_predicates = False

                skipping = False
                result.append(line)

            else:

                pruned_preds += 1
            continue


        # Separadores, linhas em branco, etc.
        result.append(line)


    if pruned_preds > 0:

        result.append(f"({pruned_preds} predicados de operações não executadas omitidos)")

    return result



def _strip_view_column_list(ddl: str) -> str:

    """Remove a lista de colunas do CREATE VIEW (...) AS e linhas vazias.


    A lista de aliases entre parênteses após CREATE VIEW é redundante —

    os aliases já aparecem no SELECT. Em views grandes (200+ colunas),

    essa lista sozinha pode ter 100+ linhas de ruído puro.


    Também remove linhas em branco do DDL — são separadores visuais

    do desenvolvedor original que só desperdiçam tokens no contexto LLM.
    """

    # Padrão: CREATE [OR REPLACE] [FORCE] VIEW "SCHEMA"."NAME" (col1, col2, ...) AS\n  SELECT

    # Queremos trocar tudo até "AS\n" por uma versão sem a lista de colunas

    match = re.match(

        r"(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+"

        r'["\w.]+(?:\s*\.\s*["\w.]+)*)'  # CREATE ... VIEW "SCHEMA"."NAME"

        r"\s*\([^)]+\)"  # (col1, col2, ...) — lista de colunas

        r"\s+AS\b",  # AS

        ddl,

        re.IGNORECASE | re.DOTALL,
    )

    if match:

        # Encontra onde o AS termina pra pegar o SELECT

        as_end = match.end()

        ddl = match.group(1) + " AS\n" + ddl[as_end:].lstrip()


    # Remove linhas em branco (só whitespace) — economiza tokens sem perder semântica

    cleaned_lines = [line for line in ddl.splitlines() if line.strip()]

    return "\n".join(cleaned_lines)



def _get_sql_referenced_columns(ctx: CollectedContext) -> set[str]:

    """Retorna set de nomes de colunas (upper) referenciadas no SQL (WHERE, JOIN, ORDER, GROUP, SELECT)."""

    cols = set()
    for col_ref in (

        ctx.parsed_sql.where_columns

        + ctx.parsed_sql.join_columns

        + ctx.parsed_sql.order_columns

        + ctx.parsed_sql.group_columns

    ):

        # col_ref pode ser "ALIAS.COL" ou "COL"

        parts = col_ref.split(".")

        cols.add(parts[-1].upper())


    # Também extrai colunas do SELECT list e predicados do plano

    # Pega colunas dos índices usados no plano (são relevantes)

    plan_source = ctx.runtime_plan or ctx.execution_plan or []

    for line in plan_source:

        # Predicate info: extrai nomes de colunas entre aspas

        for match in re.finditer(r'"(\w+)"', line):

            cols.add(match.group(1).upper())

    return cols



def _filter_columns_by_sql(

    columns: list[dict[str, Any]], referenced: set[str]

) -> list[dict[str, Any]]:

    """Filtra colunas para mostrar apenas as referenciadas no SQL ou com stats relevantes."""

    if not referenced:
        return columns

    filtered = []

    for col in columns:

        col_name = col.get("column_name", "").upper()

        if col_name in referenced:
            filtered.append(col)

    return filtered if filtered else columns  # fallback: mostra tudo se nenhuma bater



# Parâmetros do otimizador relevantes pra tuning com seus defaults Oracle

_OPTIMIZER_DEFAULTS: dict[str, tuple[str, str]] = {

    "optimizer_mode": ("ALL_ROWS", "Modo do otimizador"),

    "optimizer_index_cost_adj": (

        "100",

        "Ajuste de custo de índice (default 100, <100 favorece índices)",

    ),

    "optimizer_index_caching": ("0", "% estimado de índice em cache (default 0)"),

    "optimizer_dynamic_sampling": ("2", "Nível de amostragem dinâmica"),

    "optimizer_features_enable": (None, "Versão de features do otimizador"),

    "cursor_sharing": ("EXACT", "Compartilhamento de cursores"),

    "db_file_multiblock_read_count": (None, "Blocos por multiblock read"),

    "star_transformation_enabled": ("FALSE", "Star transformation"),

    "parallel_degree_policy": ("MANUAL", "Política de paralelismo"),

    "result_cache_mode": ("MANUAL", "Cache de resultados"),

}



def _format_optimizer_params(params: dict[str, str]) -> str:

    """Formata parâmetros do otimizador — só os relevantes, com warnings pra valores não-default."""

    lines = []

    warnings = []


    for name, (default_val, _description) in _OPTIMIZER_DEFAULTS.items():

        value = params.get(name)

        if value is None:
            continue


        line = f"- **{name}:** {value}"


        # Detecta valores não-default que merecem atenção
        if (

            default_val is not None

            and str(value).strip().upper() != str(default_val).strip().upper()

        ):

            line += f" ⚠️ (default: {default_val})"

            # Warnings específicos

            if name == "optimizer_index_cost_adj":

                try:

                    val_int = int(value)

                    if val_int <= 10:

                        warnings.append(

                            f"⚠️ **optimizer_index_cost_adj = {val_int}** — "

                            f"extremamente baixo (default 100). O otimizador vai preferir "

                            f"índices quase sempre, mesmo quando full scan seria mais eficiente."
                        )

                except ValueError:
                    pass

            elif name == "cursor_sharing" and str(value).upper() != "EXACT":

                warnings.append(

                    f"⚠️ **cursor_sharing = {value}** — "

                    "literais estão sendo substituídos por binds automaticamente. "

                    "Pode mascarar problemas de bind variables no código."
                )
        lines.append(line)


    if warnings:
        lines.append("")

        for w in warnings:

            lines.append(f"- {w}")


    return "\n".join(lines)



def to_json(ctx: CollectedContext) -> str:

    """Gera relatório JSON estruturado para integração via API."""


    data: dict[str, Any] = {

        "generated_at": datetime.now().isoformat(),

        "db_version": ctx.db_version,

        "sql": {

            "raw": ctx.parsed_sql.raw_sql,

            "type": ctx.parsed_sql.sql_type,

            "tables": ctx.parsed_sql.table_names,

            "where_columns": sorted(set(ctx.parsed_sql.where_columns)),

            "join_columns": sorted(set(ctx.parsed_sql.join_columns)),

            "order_columns": sorted(set(ctx.parsed_sql.order_columns)),

            "group_columns": sorted(set(ctx.parsed_sql.group_columns)),

            "subqueries": ctx.parsed_sql.subqueries,

            "functions": sorted(f"{f['schema']}.{f['name']}" for f in ctx.parsed_sql.functions),

        },

        "execution_plan": ctx.execution_plan,

        "runtime_plan": ctx.runtime_plan,

        "runtime_stats": ctx.runtime_stats,

        "wait_events": ctx.wait_events,

        "view_expansions": ctx.view_expansions,

        "function_ddls": ctx.function_ddls,

        "tables": [

            _table_to_dict(t) for t in sorted(ctx.tables, key=lambda t: f"{t.schema}.{t.name}")

        ],

        "optimizer_params": ctx.optimizer_params,

        "errors": ctx.errors,

    }


    return json.dumps(data, indent=2, default=str, ensure_ascii=False)



def _table_to_dict(table: TableContext) -> dict[str, Any]:

    return {

        "schema": table.schema,

        "name": table.name,

        "object_type": table.object_type,

        "ddl": table.ddl,

        "stats": table.stats,

        "columns": table.columns,

        "indexes": table.indexes,

        "constraints": table.constraints,

        "partitions": table.partitions,

        "histograms": table.histograms,

    }



def _format_table_stats(stats: dict[str, Any]) -> str:

    """Formata stats de tabela como texto compacto."""

    parts = []

    if stats.get("num_rows") is not None:
        parts.append(

            f"**Rows:** {stats['num_rows']:,}"

            if isinstance(stats["num_rows"], (int, float))

            else f"**Rows:** {stats['num_rows']}"
        )

    if stats.get("blocks"):
        parts.append(

            f"**Blocks:** {stats['blocks']:,}"

            if isinstance(stats["blocks"], (int, float))

            else f"**Blocks:** {stats['blocks']}"
        )

    if stats.get("avg_row_len"):

        parts.append(f"**Avg Row Len:** {stats['avg_row_len']}")

    if stats.get("last_analyzed"):

        parts.append(f"**Last Analyzed:** {stats['last_analyzed']}")

    # Sample size com warning se amostra é pequena

    num_rows = stats.get("num_rows", 0) or 0

    sample = stats.get("sample_size")

    if sample is not None and num_rows > 0:

        pct = (sample / num_rows) * 100 if num_rows else 0

        sample_str = (

            f"**Sample Size:** {sample:,} ({pct:.0f}%)"

            if isinstance(sample, (int, float))

            else f"**Sample Size:** {sample}"
        )

        if pct < 10:

            sample_str += " ⚠️"
        parts.append(sample_str)

    if stats.get("partitioned"):

        parts.append(f"**Partitioned:** {stats['partitioned']}")

    if stats.get("compression"):

        parts.append(f"**Compression:** {stats['compression']}")

    if stats.get("degree"):

        parts.append(f"**Parallel Degree:** {stats['degree']}")

    return " | ".join(parts)


def _format_column_stats(

    columns: list[dict[str, Any]], fk_map: dict[str, str] | None = None

) -> str:

    """Formata estatísticas de colunas como tabela markdown, com indicação de FK."""

    fk_map = fk_map or {}

    lines = [

        "| Coluna | Tipo | Nullable | Distinct | Nulls | Histogram | FK |",

        "|--------|------|----------|----------|-------|-----------|----|",

    ]

    for col in columns:

        name = col.get("column_name", "?")

        dtype = col.get("data_type", "?")

        length = col.get("data_length", "")

        dtype_full = (

            f"{dtype}({length})"

            if length and dtype in ("VARCHAR2", "CHAR", "RAW", "NUMBER")

            else dtype
        )

        nullable = col.get("nullable", "?")

        distinct = col.get("num_distinct", "?")

        nulls = col.get("num_nulls", "?")

        hist = col.get("histogram", "NONE")

        fk_ref = fk_map.get(name.upper(), "")
        lines.append(

            f"| {name} | {dtype_full} | {nullable} | {distinct} | {nulls} | {hist} | {fk_ref} |"
        )

    return "\n".join(lines)



def _format_column_structure(columns: list[dict[str, Any]]) -> str:

    """Formata colunas sem stats (views) — só tipo e nullable."""

    lines = [

        "| Coluna | Tipo | Nullable |",

        "|--------|------|----------|",

    ]

    for col in columns:

        name = col.get("column_name", "?")

        dtype = col.get("data_type", "?")

        length = col.get("data_length", "")

        dtype_full = (

            f"{dtype}({length})"

            if length and dtype in ("VARCHAR2", "CHAR", "RAW", "NUMBER")

            else dtype
        )

        nullable = col.get("nullable", "?")

        lines.append(f"| {name} | {dtype_full} | {nullable} |")

    return "\n".join(lines)



def _format_indexes(idxs: list[dict[str, Any]]) -> str:

    """Formata índices como tabela markdown."""

    lines = [

        "| Nome | Tipo | Unique | Colunas | Distinct Keys | Clustering Factor | BLevel | Last Analyzed | Status |",

        "|------|------|--------|---------|---------------|-------------------|--------|---------------|--------|",

    ]

    for idx in idxs:

        name = idx.get("index_name", "?")

        itype = idx.get("index_type", "?")

        uniq = idx.get("uniqueness", "?")

        cols = idx.get("columns", "?")

        dk = idx.get("distinct_keys", "?")

        cf = idx.get("clustering_factor", "?")

        blevel = idx.get("blevel", "?")

        analyzed = idx.get("last_analyzed", "?")

        status = idx.get("status", "?")

        # blevel > 3 é red flag

        bl_str = f"{blevel} ⚠️" if isinstance(blevel, (int, float)) and blevel > 3 else str(blevel)
        lines.append(

            f"| {name} | {itype} | {uniq} | {cols} | {dk} | {cf} | {bl_str} | {analyzed} | {status} |"
        )

    return "\n".join(lines)



def _format_constraints(cons: list[dict[str, Any]]) -> str:

    """Deprecated — FK info agora vai inline nas colunas via _build_fk_map."""
    return ""



def _build_fk_map(constraints: list[dict[str, Any]]) -> dict[str, str]:

    """Constrói mapa coluna → tabela referenciada a partir das constraints FK.


    Retorna dict tipo {"FUNC_ID": "SCHEMA.FUNC"} pra cada coluna que é FK.

    Constraints PK/UNIQUE/CHECK são ignoradas.
    """

    fk_map: dict[str, str] = {}

    for c in constraints:

        if c.get("constraint_type") != "R":
            continue

        cols = c.get("columns", "")

        r_owner = c.get("r_owner", "")

        r_table = c.get("r_table_name", "")

        if not cols or not r_table:
            continue

        ref = f"{r_owner}.{r_table}" if r_owner else r_table

        # FK pode ter múltiplas colunas (composite FK)

        for col in cols.split(","):

            col = col.strip().upper()

            if col:

                fk_map[col] = ref

    return fk_map



def _format_small_table(table: TableContext) -> str:

    """Formato compacto pra tabelas pequenas — uma linha com o essencial."""

    parts = [f"**{table.schema}.{table.name}**"]


    num_rows = (table.stats or {}).get("num_rows", "?")

    blocks = (table.stats or {}).get("blocks", "?")

    parts.append(f"{num_rows} rows, {blocks} blocks")


    # PK dos índices

    pk_cols = []

    for idx in table.indexes:

        if idx.get("uniqueness") == "UNIQUE":

            pk_cols.append(idx.get("columns", ""))

    if pk_cols:

        parts.append(f"PK: {pk_cols[0]}")


    # FKs

    fk_map = _build_fk_map(table.constraints)

    for col, ref in fk_map.items():

        parts.append(f"FK: {col} → {ref}")


    return "- " + " | ".join(parts)



def _format_partitions(parts: list[dict[str, Any]]) -> str:

    """Formata info de partições."""

    lines = [

        "| Partição | Posição | Rows | Last Analyzed |",

        "|----------|---------|------|---------------|",

    ]

    for p in parts:

        name = p.get("partition_name", "?")

        pos = p.get("partition_position", "?")

        rows = p.get("num_rows", "?")

        analyzed = p.get("last_analyzed", "?")

        lines.append(f"| {name} | {pos} | {rows} | {analyzed} |")

    return "\n".join(lines)



def _format_runtime_stats(stats: dict[str, Any]) -> str:

    """Formata métricas de execução de V$SQL com warnings de saúde."""

    lines = []

    mapping = [

        ("sql_id", "SQL ID"),

        ("child_number", "Child Number"),

        ("plan_hash_value", "Plan Hash Value"),

        ("executions", "Executions"),

        ("avg_elapsed_ms", "Avg Elapsed (ms)"),

        ("avg_cpu_ms", "Avg CPU (ms)"),

        ("avg_buffer_gets", "Avg Buffer Gets"),

        ("avg_rows_per_exec", "Avg Rows/Exec"),

        ("disk_reads", "Disk Reads"),

        ("rows_processed", "Rows Processed"),

        ("sorts", "Sorts"),

        ("parse_calls", "Parse Calls"),

        ("loads", "Loads (hard parses)"),

        ("invalidations", "Invalidations"),

        ("version_count", "Version Count (children)"),

    ]

    for key, label in mapping:

        val = stats.get(key)

        if val is not None:

            lines.append(f"- **{label}:** {val}")


    # Warnings de saúde do SQL

    warnings = []

    loads = stats.get("loads", 0) or 0

    if loads > 1:

        warnings.append(

            f"⚠️ {loads} hard parses — possível falta de bind variables ou invalidação frequente"
        )


    invalidations = stats.get("invalidations", 0) or 0

    if invalidations > 0:

        warnings.append(

            f"⚠️ {invalidations} invalidações — DDL recente ou stats regathered nas tabelas"
        )


    version_count = stats.get("version_count", 0) or 0

    if version_count > 5:

        warnings.append(

            f"⚠️ {version_count} child cursors — possível instabilidade de plano ou bind mismatch"
        )


    parse_calls = stats.get("parse_calls", 0) or 0

    executions = stats.get("executions", 0) or 0

    if executions > 0 and parse_calls >= executions:

        ratio = parse_calls / executions

        if ratio >= 1.0:

            warnings.append(

                f"⚠️ Parse calls ({parse_calls}) ≈ executions ({executions}) — "

                "cursor não está sendo reutilizado entre execuções (soft parse a cada call)"
            )


    avg_elapsed = stats.get("avg_elapsed_ms", 0) or 0

    avg_cpu = stats.get("avg_cpu_ms", 0) or 0

    if avg_elapsed > 0 and avg_cpu > 0:

        cpu_pct = (avg_cpu / avg_elapsed) * 100

        if cpu_pct >= 95:

            warnings.append(

                f"ℹ️ Query é CPU-bound ({cpu_pct:.0f}% CPU) — waits de I/O são irrelevantes"
            )

        elif cpu_pct < 50:

            warnings.append(

                f"⚠️ Query é I/O-bound ({cpu_pct:.0f}% CPU, {100 - cpu_pct:.0f}% wait) — investigar wait events"
            )


    avg_buffer_gets = stats.get("avg_buffer_gets", 0) or 0

    avg_rows = stats.get("avg_rows_per_exec", 0) or 0

    if avg_rows > 0 and avg_buffer_gets > 0:

        gets_per_row = avg_buffer_gets / avg_rows

        if gets_per_row > 100:

            warnings.append(

                f"⚠️ {gets_per_row:.0f} buffer gets/row — eficiência de I/O lógico baixa, "

                "possível full scan ou índice ineficiente"
            )


    if warnings:
        lines.append("")

        lines.append("**SQL Health:**")

        for w in warnings:

            lines.append(f"- {w}")


    return "\n".join(lines)



def _format_wait_events(events: list[dict[str, Any]]) -> str:

    """Formata wait events como tabela markdown."""

    lines = [

        "| Event | Total Waits | Time Waited (ms) | Avg Wait |",

        "|-------|-------------|-------------------|----------|",

    ]

    for e in events:

        event = e.get("event", "?")

        waits = e.get("total_waits", "?")

        time_ms = e.get("time_waited_ms", "?")

        avg = e.get("average_wait", "?")

        lines.append(f"| {event} | {waits} | {time_ms} | {avg} |")

    return "\n".join(lines)



def _parse_plan_operations(plan_lines: list[str]) -> list[dict[str, Any]]:

    """Parseia linhas do DISPLAY_CURSOR e extrai operações com métricas."""

    ops = []

    # Padrão: |  Id | Operation | Name | Starts | E-Rows | A-Rows | A-Time | Buffers | ...

    # Linhas de dados: |*  3 |    HASH JOIN OUTER  |   | 1 | 1557 | 50 | 00:00:00.26 | 10919 | ...

    for line in plan_lines:

        # Linha de operação: começa com | seguido de * ou espaço e número

        match = re.match(

            r"\|\*?\s*(\d+)\s*\|"  # Id

            r"\s*(.+?)\s*\|"  # Operation

            r"\s*(.*?)\s*\|"  # Name

            r"\s*(\d+)\s*\|"  # Starts

            r"\s*(\d*)\s*\|"  # E-Rows (pode estar vazio)

            r"\s*(\d+)\s*\|"  # A-Rows

            r"\s*(\d+:\d+:\d+\.\d+)\s*\|"  # A-Time

            r"\s*(\d+)\s*\|",  # Buffers

            line,
        )

        if match:

            e_rows_str = match.group(5).strip()
            ops.append(

                {

                    "id": int(match.group(1)),

                    "operation": match.group(2).strip(),

                    "name": match.group(3).strip(),

                    "starts": int(match.group(4)),

                    "e_rows": int(e_rows_str) if e_rows_str else 0,

                    "a_rows": int(match.group(6)),

                    "a_time": match.group(7),

                    "buffers": int(match.group(8)),

                }
            )
    return ops



def _extract_plan_tables(ops: list[dict[str, Any]], index_table_map: dict[str, str]) -> set[str]:
    """

    Extrai nomes de tabelas acessadas no plano.


    Usa TABLE ACCESS → nome direto da tabela.

    Usa INDEX scans → resolve via mapa index_name→table_name do ALL_INDEXES.
    """

    tables = set()

    for o in ops:

        op = o["operation"].upper()

        name = o["name"].upper()

        if not name:
            continue

        if "TABLE ACCESS" in op or "MAT_VIEW" in op:

            tables.add(name)

        elif "INDEX" in op:

            # Resolve índice → tabela via mapa coletado do banco

            resolved = index_table_map.get(name)

            if resolved:

                tables.add(resolved)

    return tables



def _extract_implicit_conversions(plan_lines: list[str]) -> list[dict[str, str]]:

    """Extrai conversões implícitas (TO_NUMBER, TO_CHAR, TO_DATE) da Predicate Information."""

    conversions = []

    in_predicates = False


    for line in plan_lines:

        stripped = line.strip()

        if stripped.startswith("Predicate Information"):

            in_predicates = True
            continue

        if not in_predicates:
            continue

        # Fim da seção de predicados (linha em branco ou nova seção)

        if in_predicates and stripped == "":
            continue

        if stripped.startswith("---"):
            continue

        # Linha de predicado: "  12 - filter(TO_NUMBER("FNC_HIERARCHY")=1)"

        pred_match = re.match(r"\s*(\d+)\s*-\s*(access|filter)\((.+)\)", stripped)

        if not pred_match:

            # Pode ser continuação ou outra seção

            if re.match(r"^[A-Z]", stripped) and not stripped[0].isdigit():

                in_predicates = False
            continue


        pred_id = pred_match.group(1)

        pred_body = pred_match.group(3)


        # Busca TO_NUMBER/TO_CHAR/TO_DATE aplicados em colunas

        for conv_match in re.finditer(

            r'(TO_NUMBER|TO_CHAR|TO_DATE)\s*\(\s*("?\w+"?\.?"?\w+"?)',

            pred_body,

        ):

            func = conv_match.group(1)

            col = conv_match.group(2)

            conversions.append({"id": pred_id, "function": f"{func}({col})"})


    return conversions



def _time_to_seconds(time_str: str) -> float:

    """Converte 'HH:MM:SS.ss' pra segundos."""

    parts = time_str.split(":")

    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])



def _format_hotspots(plan_lines: list[str]) -> str:

    """Gera seção Hotspots a partir do plano real.


    Foca em desvios de cardinalidade e efeito multiplicador —

    top N por buffers/tempo são redundantes (operações pai acumulam filhos).


    Operações com mesmo (Operation, Name) são agrupadas pra evitar

    repetição quando views são expandidas múltiplas vezes no plano.
    """

    ops = _parse_plan_operations(plan_lines)

    if not ops:
        return ""


    lines: list[str] = []


    # Operações com Starts >= 10 (efeito multiplicador) — agrupadas por (Operation, Name)

    high_starts = [o for o in ops if o["starts"] >= 10]

    if high_starts:

        # Agrupa por (operation, name) somando métricas

        grouped: dict[tuple[str, str], dict[str, Any]] = {}

        for o in high_starts:

            key = (o["operation"], o["name"])

            if key not in grouped:

                grouped[key] = {

                    "operation": o["operation"],

                    "name": o["name"],

                    "ids": [o["id"]],

                    "starts": o["starts"],

                    "a_rows": o["a_rows"],

                    "buffers": o["buffers"],

                    "occurrences": 1,

                }

            else:

                g = grouped[key]

                g["ids"].append(o["id"])

                g["starts"] += o["starts"]

                g["a_rows"] += o["a_rows"]

                g["buffers"] += o["buffers"]

                g["occurrences"] += 1


        sorted_groups = sorted(grouped.values(), key=lambda g: g["starts"], reverse=True)

        lines.append("### Operações com efeito multiplicador (Starts ≥ 10)")
        lines.append(

            "| Operation | Name | Ocorrências | Starts (total) | A-Rows (total) | Buffers (total) |"
        )
        lines.append(

            "|-----------|------|-------------|----------------|----------------|-----------------|"
        )

        for g in sorted_groups:

            occ = f"{g['occurrences']}×" if g["occurrences"] > 1 else "1"
            lines.append(

                f"| {g['operation']} | {g['name']} | {occ} "

                f"| {g['starts']:,} | {g['a_rows']:,} | {g['buffers']:,} |"
            )
        lines.append("")


    # Maiores desvios de cardinalidade (E-Rows vs A-Rows) — também deduplicados

    deviations = []

    for o in ops:

        if o["e_rows"] > 0 and o["a_rows"] > 0:

            ratio = max(o["e_rows"], o["a_rows"]) / min(o["e_rows"], o["a_rows"])

            if ratio >= 5:

                deviations.append({**o, "ratio": ratio})

    if deviations:

        # Agrupa por (operation, name) mantendo o maior ratio

        dev_grouped: dict[tuple[str, str], dict[str, Any]] = {}

        for d in deviations:

            key = (d["operation"], d["name"])

            if key not in dev_grouped:

                dev_grouped[key] = {

                    "operation": d["operation"],

                    "name": d["name"],

                    "e_rows": d["e_rows"],

                    "a_rows": d["a_rows"],

                    "ratio": d["ratio"],

                    "occurrences": 1,

                }

            else:

                g = dev_grouped[key]

                g["occurrences"] += 1

                # Mantém o pior caso (maior ratio)

                if d["ratio"] > g["ratio"]:

                    g["e_rows"] = d["e_rows"]

                    g["a_rows"] = d["a_rows"]

                    g["ratio"] = d["ratio"]


        sorted_devs = sorted(dev_grouped.values(), key=lambda g: g["ratio"], reverse=True)

        lines.append("### Desvios de cardinalidade (E-Rows vs A-Rows, ratio ≥ 5x)")

        lines.append("| Operation | Name | Ocorrências | E-Rows | A-Rows | Ratio |")

        lines.append("|-----------|------|-------------|--------|--------|-------|")

        for g in sorted_devs[:10]:

            direction = "↑" if g["a_rows"] > g["e_rows"] else "↓"

            occ = f"{g['occurrences']}×" if g["occurrences"] > 1 else "1"
            lines.append(

                f"| {g['operation']} | {g['name']} | {occ} "

                f"| {g['e_rows']:,} | {g['a_rows']:,} | {g['ratio']:.1f}x {direction} |"
            )
        lines.append("")


    return "\n".join(lines)

