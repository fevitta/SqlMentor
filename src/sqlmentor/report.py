"""
Gerador de relatórios para consumo por IA.

Suporta Markdown (otimizado pra colar em chat) e JSON (pra integração via API).
"""

import json
from datetime import datetime
from typing import Any

from sqlmentor.collector import CollectedContext, TableContext


def to_markdown(ctx: CollectedContext) -> str:
    """
    Gera relatório Markdown estruturado pro LLM analisar.

    Formato otimizado pra context window de LLM — conciso mas completo.
    """
    lines: list[str] = []

    lines.append("# SQL Tuning Context Report")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if ctx.db_version:
        lines.append(f"**Database:** {ctx.db_version}")
    lines.append("")

    # ─── SQL Original ────────────────────────────────────────────
    lines.append("## 1. SQL Original")
    lines.append(f"**Tipo:** {ctx.parsed_sql.sql_type}")
    lines.append(f"**Tabelas referenciadas:** {', '.join(ctx.parsed_sql.table_names)}")

    if ctx.parsed_sql.where_columns:
        lines.append(f"**Colunas em WHERE:** {', '.join(sorted(set(ctx.parsed_sql.where_columns)))}")
    if ctx.parsed_sql.join_columns:
        lines.append(f"**Colunas em JOIN:** {', '.join(sorted(set(ctx.parsed_sql.join_columns)))}")
    if ctx.parsed_sql.order_columns:
        lines.append(f"**Colunas em ORDER BY:** {', '.join(sorted(set(ctx.parsed_sql.order_columns)))}")
    if ctx.parsed_sql.group_columns:
        lines.append(f"**Colunas em GROUP BY:** {', '.join(sorted(set(ctx.parsed_sql.group_columns)))}")
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
        for line in ctx.execution_plan:
            lines.append(line)
        lines.append("```")
        lines.append("")
        section += 1

    # ─── Runtime: Plano Real ──────────────────────────────────────
    if ctx.runtime_plan:
        lines.append(f"## {section}. Runtime Execution Plan (ALLSTATS LAST)")
        lines.append("> Coletado com `STATISTICS_LEVEL = ALL` na sessão. "
                      "O plano mostra a última execução (LAST).")
        executions = ctx.runtime_stats.get("executions", 1) if ctx.runtime_stats else 1
        if executions and executions > 1:
            lines.append(f"> ⚠️ SQL_ID já existia no shared pool com {executions} execuções. "
                          "Stats de V$SQL são acumuladas, mas o plano ALLSTATS LAST é da última execução.")
        lines.append("")
        lines.append("```")
        for line in _strip_sql_from_plan(ctx.runtime_plan):
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
                lines.append(f"- Não acessadas (join elimination ou não necessárias): {', '.join(not_in_plan)}")
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
            lines.append(table.ddl.strip())
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
            if stripped and all(c == '-' for c in stripped):
                continue
            # Pula o SQL repetido
            continue
        result.append(line)
    # Fallback: se não achou Plan hash value, retorna tudo
    return result if found_plan_hash else plan_lines


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
        import re
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
    "optimizer_index_cost_adj": ("100", "Ajuste de custo de índice (default 100, <100 favorece índices)"),
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

    for name, (default_val, description) in _OPTIMIZER_DEFAULTS.items():
        value = params.get(name)
        if value is None:
            continue

        line = f"- **{name}:** {value}"

        # Detecta valores não-default que merecem atenção
        if default_val is not None and str(value).strip().upper() != str(default_val).strip().upper():
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
        "tables": [_table_to_dict(t) for t in sorted(ctx.tables, key=lambda t: f"{t.schema}.{t.name}")],
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
        parts.append(f"**Rows:** {stats['num_rows']:,}" if isinstance(stats['num_rows'], (int, float)) else f"**Rows:** {stats['num_rows']}")
    if stats.get("blocks"):
        parts.append(f"**Blocks:** {stats['blocks']:,}" if isinstance(stats['blocks'], (int, float)) else f"**Blocks:** {stats['blocks']}")
    if stats.get("avg_row_len"):
        parts.append(f"**Avg Row Len:** {stats['avg_row_len']}")
    if stats.get("last_analyzed"):
        parts.append(f"**Last Analyzed:** {stats['last_analyzed']}")
    # Sample size com warning se amostra é pequena
    num_rows = stats.get("num_rows", 0) or 0
    sample = stats.get("sample_size")
    if sample is not None and num_rows > 0:
        pct = (sample / num_rows) * 100 if num_rows else 0
        sample_str = f"**Sample Size:** {sample:,} ({pct:.0f}%)" if isinstance(sample, (int, float)) else f"**Sample Size:** {sample}"
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


def _format_column_stats(columns: list[dict[str, Any]], fk_map: dict[str, str] | None = None) -> str:
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
        dtype_full = f"{dtype}({length})" if length and dtype in ("VARCHAR2", "CHAR", "RAW", "NUMBER") else dtype
        nullable = col.get("nullable", "?")
        distinct = col.get("num_distinct", "?")
        nulls = col.get("num_nulls", "?")
        hist = col.get("histogram", "NONE")
        fk_ref = fk_map.get(name.upper(), "")
        lines.append(f"| {name} | {dtype_full} | {nullable} | {distinct} | {nulls} | {hist} | {fk_ref} |")
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
        dtype_full = f"{dtype}({length})" if length and dtype in ("VARCHAR2", "CHAR", "RAW", "NUMBER") else dtype
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
        lines.append(f"| {name} | {itype} | {uniq} | {cols} | {dk} | {cf} | {bl_str} | {analyzed} | {status} |")
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
        warnings.append(f"⚠️ {loads} hard parses — possível falta de bind variables ou invalidação frequente")

    invalidations = stats.get("invalidations", 0) or 0
    if invalidations > 0:
        warnings.append(f"⚠️ {invalidations} invalidações — DDL recente ou stats regathered nas tabelas")

    version_count = stats.get("version_count", 0) or 0
    if version_count > 5:
        warnings.append(f"⚠️ {version_count} child cursors — possível instabilidade de plano ou bind mismatch")

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
                f"⚠️ Query é I/O-bound ({cpu_pct:.0f}% CPU, {100-cpu_pct:.0f}% wait) — investigar wait events"
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
    import re

    ops = []
    # Padrão: |  Id | Operation | Name | Starts | E-Rows | A-Rows | A-Time | Buffers | ...
    # Linhas de dados: |*  3 |    HASH JOIN OUTER  |   | 1 | 1557 | 50 | 00:00:00.26 | 10919 | ...
    for line in plan_lines:
        # Linha de operação: começa com | seguido de * ou espaço e número
        match = re.match(
            r'\|\*?\s*(\d+)\s*\|'       # Id
            r'\s*(.+?)\s*\|'             # Operation
            r'\s*(.*?)\s*\|'             # Name
            r'\s*(\d+)\s*\|'             # Starts
            r'\s*(\d*)\s*\|'             # E-Rows (pode estar vazio)
            r'\s*(\d+)\s*\|'             # A-Rows
            r'\s*(\d+:\d+:\d+\.\d+)\s*\|'  # A-Time
            r'\s*(\d+)\s*\|',            # Buffers
            line,
        )
        if match:
            e_rows_str = match.group(5).strip()
            ops.append({
                "id": int(match.group(1)),
                "operation": match.group(2).strip(),
                "name": match.group(3).strip(),
                "starts": int(match.group(4)),
                "e_rows": int(e_rows_str) if e_rows_str else 0,
                "a_rows": int(match.group(6)),
                "a_time": match.group(7),
                "buffers": int(match.group(8)),
            })
    return ops

def _extract_plan_tables(
    ops: list[dict[str, Any]], index_table_map: dict[str, str]
) -> set[str]:
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
    import re

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
        pred_match = re.match(r'\s*(\d+)\s*-\s*(access|filter)\((.+)\)', stripped)
        if not pred_match:
            # Pode ser continuação ou outra seção
            if re.match(r'^[A-Z]', stripped) and not stripped[0].isdigit():
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
            conversions.append({"id": pred_id, "function": f'{func}({col})'})

    return conversions






def _time_to_seconds(time_str: str) -> float:
    """Converte 'HH:MM:SS.ss' pra segundos."""
    parts = time_str.split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def _format_hotspots(plan_lines: list[str]) -> str:
    """Gera seção Hotspots a partir do plano real.

    Foca em desvios de cardinalidade e efeito multiplicador —
    top N por buffers/tempo são redundantes (operações pai acumulam filhos).
    """
    ops = _parse_plan_operations(plan_lines)
    if not ops:
        return ""

    lines: list[str] = []

    # Operações com Starts >= 10 (efeito multiplicador)
    high_starts = [o for o in ops if o["starts"] >= 10]
    if high_starts:
        high_starts.sort(key=lambda o: o["starts"], reverse=True)
        lines.append("### Operações com efeito multiplicador (Starts ≥ 10)")
        lines.append("| Id | Operation | Name | Starts | A-Rows | Buffers |")
        lines.append("|----|-----------|------|--------|--------|---------|")
        for o in high_starts:
            lines.append(f"| {o['id']} | {o['operation']} | {o['name']} | {o['starts']:,} | {o['a_rows']:,} | {o['buffers']:,} |")
        lines.append("")

    # Maiores desvios de cardinalidade (E-Rows vs A-Rows)
    deviations = []
    for o in ops:
        if o["e_rows"] > 0 and o["a_rows"] > 0:
            ratio = max(o["e_rows"], o["a_rows"]) / min(o["e_rows"], o["a_rows"])
            if ratio >= 5:
                deviations.append({**o, "ratio": ratio})
    if deviations:
        deviations.sort(key=lambda o: o["ratio"], reverse=True)
        lines.append("### Desvios de cardinalidade (E-Rows vs A-Rows, ratio ≥ 5x)")
        lines.append("| Id | Operation | Name | E-Rows | A-Rows | Ratio |")
        lines.append("|----|-----------|------|--------|--------|-------|")
        for o in deviations[:10]:
            direction = "↑" if o["a_rows"] > o["e_rows"] else "↓"
            lines.append(f"| {o['id']} | {o['operation']} | {o['name']} | {o['e_rows']:,} | {o['a_rows']:,} | {o['ratio']:.1f}x {direction} |")
        lines.append("")

    return "\n".join(lines)


