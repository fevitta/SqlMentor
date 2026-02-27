"""
Coletor de contexto Oracle para tuning de SQL.

Dado um SQL parseado e uma conexão, coleta todo o metadata necessário
para que uma IA possa analisar e sugerir melhorias.
"""

import logging
import time as _time
from dataclasses import dataclass, field
from typing import Any

import oracledb
import sqlglot
from sqlglot import exp

from sqlmentor.parser import ParsedSQL
from sqlmentor.queries import (
    batch_column_stats,
    batch_constraints,
    batch_indexes,
    batch_table_stats,
    column_stats,
    constraints,
    db_version,
    explain_plan,
    function_ddl,
    histograms,
    index_to_table_map,
    indexes,
    object_type,
    optimizer_params,
    runtime_plan,
    session_wait_events,
    sql_runtime_stats,
    table_ddl,
    table_partitions,
    table_stats,
)

logger = logging.getLogger(__name__)

# ── Cache TTL + LRU ──────────────────────────────────────────────────

_CACHE_TTL_SECONDS: int = 300  # 5 minutos
_CACHE_MAX_ENTRIES: int = 500


@dataclass
class _CacheEntry[T]:
    value: T
    created_at: float = field(default_factory=_time.monotonic)

    def is_expired(self, ttl: float) -> bool:
        return (_time.monotonic() - self.created_at) > ttl


class _LRUCache[T]:
    """Cache in-memory com TTL e eviction LRU por created_at."""

    def __init__(
        self, max_entries: int = _CACHE_MAX_ENTRIES, ttl: float = _CACHE_TTL_SECONDS
    ) -> None:
        self._store: dict[str, _CacheEntry[T]] = {}
        self._max_entries = max_entries
        self._ttl = ttl

    def get(self, key: str) -> T | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.is_expired(self._ttl):
            del self._store[key]
            return None
        return entry.value

    def put(self, key: str, value: T) -> None:
        if key not in self._store and len(self._store) >= self._max_entries:
            oldest = min(self._store, key=lambda k: self._store[k].created_at)
            del self._store[oldest]
        self._store[key] = _CacheEntry(value=value)

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def __len__(self) -> int:
        return len(self._store)

    def clear(self) -> None:
        self._store.clear()


_table_cache: _LRUCache["TableContext"] = _LRUCache()
_optimizer_cache: _LRUCache[dict[str, str]] = _LRUCache()
_index_map_cache: _LRUCache[dict[str, str]] = _LRUCache()


def clear_cache() -> None:
    """Limpa todo o cache de metadata (tabelas, otimizador, índices)."""
    _table_cache.clear()
    _optimizer_cache.clear()
    _index_map_cache.clear()


@dataclass
class TableContext:
    """Contexto coletado de uma tabela ou view."""

    name: str
    schema: str
    object_type: str = "TABLE"  # TABLE ou VIEW
    ddl: str | None = None
    stats: dict[str, Any] | None = None
    columns: list[dict[str, Any]] = field(default_factory=list)
    indexes: list[dict[str, Any]] = field(default_factory=list)
    constraints: list[dict[str, Any]] = field(default_factory=list)
    partitions: list[dict[str, Any]] = field(default_factory=list)
    histograms: dict[str, list[dict]] = field(default_factory=dict)


@dataclass
class CollectedContext:
    """Contexto completo coletado para análise."""

    parsed_sql: ParsedSQL
    db_version: str | None = None
    execution_plan: list[str] | None = None
    runtime_plan: list[str] | None = None
    runtime_stats: dict[str, Any] | None = None
    wait_events: list[dict[str, Any]] = field(default_factory=list)
    view_expansions: dict[str, list[str]] = field(default_factory=dict)
    index_table_map: dict[str, str] = field(default_factory=dict)
    tables: list[TableContext] = field(default_factory=list)
    function_ddls: dict[str, str] = field(default_factory=dict)  # "SCHEMA.FUNC" → DDL
    optimizer_params: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def _execute_query(cursor: oracledb.Cursor, sql: str, params: dict) -> list[dict[str, Any]]:
    """Executa query e retorna resultado como lista de dicts."""
    cursor.execute(sql, params)
    columns = [col[0].lower() for col in cursor.description or []]
    rows = []
    for row in cursor:
        row_dict = {}
        for i, val in enumerate(row):
            # Converte LOBs pra string
            if hasattr(val, "read"):
                val = val.read()
            row_dict[columns[i]] = val
        rows.append(row_dict)
    return rows


def _batch_collect_tables(
    cursor: oracledb.Cursor,
    pairs: list[tuple[str, str]],
    ctx: CollectedContext,
) -> dict[str, dict[str, Any]]:
    """Coleta stats, columns, indexes e constraints para múltiplas tabelas em batch.

    Args:
        cursor: Cursor Oracle ativo.
        pairs: Lista de (schema, table_name).
        ctx: Contexto para registro de erros.

    Returns:
        Dict keyed por "SCHEMA.TABLE" com sub-dicts: stats, columns, indexes, constraints.
    """
    if not pairs:
        return {}

    result: dict[str, dict[str, Any]] = {f"{s}.{n}": {} for s, n in pairs}

    try:
        # Table stats
        sql, params = batch_table_stats(pairs)
        rows = _execute_query(cursor, sql, params)
        for row in rows:
            key = f"{row.get('owner', '')}.{row.get('table_name', '')}"
            if key in result:
                result[key]["stats"] = row

        # Column stats
        sql, params = batch_column_stats(pairs)
        rows = _execute_query(cursor, sql, params)
        for row in rows:
            key = f"{row.get('owner', '')}.{row.get('table_name', '')}"
            if key in result:
                result[key].setdefault("columns", []).append(row)

        # Indexes
        sql, params = batch_indexes(pairs)
        rows = _execute_query(cursor, sql, params)
        for row in rows:
            key = f"{row.get('owner', '')}.{row.get('table_name', '')}"
            if key in result:
                result[key].setdefault("indexes", []).append(row)

        # Constraints
        sql, params = batch_constraints(pairs)
        rows = _execute_query(cursor, sql, params)
        for row in rows:
            key = f"{row.get('owner', '')}.{row.get('table_name', '')}"
            if key in result:
                result[key].setdefault("constraints", []).append(row)

    except Exception as e:
        logger.warning("Batch collection failed, will fallback to per-table: %s", e)
        ctx.errors.append(f"Batch collection fallback: {e}")
        return {}  # Empty dict signals caller to use per-table fallback

    return result


def collect_context(
    parsed: ParsedSQL,
    conn: oracledb.Connection,
    default_schema: str,
    deep: bool = False,
    expand_views: bool = False,
    expand_functions: bool = False,
    execute: bool = False,
    bind_params: dict[str, str | int | float | None] | None = None,
    use_cache: bool = True,
) -> CollectedContext:
    """
    Coleta todo o contexto necessário para análise de SQL.

    Args:
        parsed: SQL parseado com tabelas identificadas.
        conn: Conexão Oracle ativa.
        default_schema: Schema padrão para tabelas não qualificadas.
        deep: Se True, coleta histogramas e partições (mais lento).
        expand_views: Se True, coleta DDL e colunas de views.
        expand_functions: Se True, coleta DDL de funções PL/SQL referenciadas.

    Returns:
        CollectedContext com toda a metadata coletada.
    """
    ctx = CollectedContext(parsed_sql=parsed)
    cursor = conn.cursor()

    # Remove terminador SQL*Plus (;) — Oracle não aceita via cursor.execute()
    if parsed.raw_sql.endswith(";"):
        parsed.raw_sql = parsed.raw_sql.rstrip(";").strip()

    # 0. Versão do banco
    ctx.db_version = _collect_db_version(cursor, ctx)

    # 1. Execution Plan
    if parsed.sql_type in ("SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"):
        if execute and parsed.sql_type == "SELECT":
            # Executa a query real com GATHER_PLAN_STATISTICS e coleta plano + stats
            _collect_runtime_execution(cursor, conn, parsed.raw_sql, ctx, bind_params)
        else:
            # Plano estimado via EXPLAIN PLAN
            ctx.execution_plan = _collect_explain_plan(cursor, parsed.raw_sql, ctx, bind_params)
    elif parsed.sql_type in ("PROCEDURE", "TRIGGER", "FUNCTION", "PACKAGE"):
        # Pra PL/SQL, tenta extrair SELECTs internos e rodar explain de cada
        # (v2 - por ora só coleta metadata das tabelas)
        pass

    # 2. Metadata de cada tabela — two-phase: detect+DDL per-table, then batch
    collected_objects: set[str] = set()
    # Phase 1: detect object type, collect DDL, identify tables for batch
    tables_for_batch: list[tuple[str, str, str, TableContext]] = []  # (schema, name, key, tctx)
    for table in parsed.tables:
        schema = (table.get("schema") or default_schema).upper()
        name = table["name"].upper()
        obj_key = f"{schema}.{name}"

        # Pula se já coletou esse objeto (mesma view/tabela referenciada 2x)
        if obj_key in collected_objects:
            continue
        collected_objects.add(obj_key)

        # Cache hit: retorna TableContext cacheado se disponível
        cached = _table_cache.get(obj_key) if use_cache else None
        if cached is not None and (not deep or (cached.histograms or cached.partitions)):
            logger.info(f"Cache hit: {obj_key}")
            ctx.tables.append(cached)
            # View expansion pode estar cacheada junto, mas precisamos garantir
            if cached.object_type == "VIEW":
                _collect_view_expansion(cursor, schema, name, ctx)
            continue

        logger.info(f"Coletando contexto: {schema}.{name}")
        tctx = TableContext(name=name, schema=schema)

        # Detecta tipo do objeto (TABLE, VIEW, etc.)
        tctx.object_type = _detect_object_type(cursor, schema, name, ctx)

        # View expansion: coleta tabelas internas da view (sempre, é barato)
        if tctx.object_type == "VIEW":
            _collect_view_expansion(cursor, schema, name, ctx)

        # Views: só coleta detalhes se --expand-views foi passado
        if tctx.object_type == "VIEW" and not expand_views:
            if use_cache:
                _table_cache.put(obj_key, tctx)
            ctx.tables.append(tctx)
            continue

        # DDL (per-table, uses DBMS_METADATA)
        tctx.ddl = _collect_ddl(cursor, schema, name, ctx)

        # Mark for batch collection of stats/columns/indexes/constraints
        tables_for_batch.append((schema, name, obj_key, tctx))

    # Phase 2: Batch-query stats/columns/indexes/constraints
    batch_pairs = [(s, n) for s, n, _k, _t in tables_for_batch]
    batch_data = _batch_collect_tables(cursor, batch_pairs, ctx) if batch_pairs else {}

    for schema, name, obj_key, tctx in tables_for_batch:
        key = f"{schema}.{name}"
        data = batch_data.get(key)

        if data:
            # Batch succeeded — use batch results
            tctx.stats = data.get("stats")
            tctx.columns = data.get("columns", [])
            tctx.indexes = data.get("indexes", [])
            tctx.constraints = data.get("constraints", [])
        else:
            # Fallback to per-table collection
            tctx.stats = _collect_table_stats(cursor, schema, name, ctx)
            tctx.columns = _collect_column_stats(cursor, schema, name, ctx)
            tctx.indexes = _collect_indexes(cursor, schema, name, ctx)
            tctx.constraints = _collect_constraints(cursor, schema, name, ctx)

        # Deep mode: partitions + histograms (always per-table)
        if deep:
            tctx.partitions = _collect_partitions(cursor, schema, name, ctx)
            tctx.histograms = _collect_histograms(cursor, schema, name, parsed, tctx.columns, ctx)

        if use_cache:
            _table_cache.put(obj_key, tctx)
        ctx.tables.append(tctx)

    # 3. Mapa index_name → table_name (pra cruzar plano com view expansion)
    if ctx.view_expansions:
        # Coleta schemas de todas as tabelas internas das views
        schemas_to_map = set()
        for inner_tables in ctx.view_expansions.values():
            for tbl in inner_tables:
                parts = tbl.split(".")
                if len(parts) == 2:
                    schemas_to_map.add(parts[0].upper())
        # Inclui schema das tabelas diretas também (podem ter índices no plano)
        for t in ctx.tables:
            schemas_to_map.add(t.schema)
        for schema in schemas_to_map:
            cached_map = _index_map_cache.get(schema) if use_cache else None
            if cached_map is not None:
                logger.info(f"Cache hit: index_map({schema})")
                ctx.index_table_map.update(cached_map)
                continue
            try:
                sql, params = index_to_table_map(schema)
                rows = _execute_query(cursor, sql, params)
                schema_map: dict[str, str] = {}
                for row in rows:
                    idx_name = row.get("index_name", "")
                    tbl_name = row.get("table_name", "")
                    if idx_name and tbl_name:
                        schema_map[idx_name.upper()] = tbl_name.upper()
                ctx.index_table_map.update(schema_map)
                if use_cache:
                    _index_map_cache.put(schema, schema_map)
            except Exception as e:
                ctx.errors.append(f"Erro ao coletar mapa de índices ({schema}): {e}")

    # 4. DDL de funções PL/SQL referenciadas no SQL
    if expand_functions and parsed.functions:
        for func in parsed.functions:
            func_key = f"{func['schema']}.{func['name']}"
            if func_key in ctx.function_ddls:
                continue
            try:
                sql, params = function_ddl(func["schema"], func["name"])
                rows = _execute_query(cursor, sql, params)
                if rows:
                    ddl_text = str(rows[0].get("ddl", ""))
                    if ddl_text.strip():
                        ctx.function_ddls[func_key] = ddl_text.strip()
            except Exception as e:
                ctx.errors.append(f"Erro ao coletar DDL de {func_key}: {e}")

    # 5. Optimizer params
    cached_opt = _optimizer_cache.get("global") if use_cache else None
    if cached_opt is not None:
        logger.info("Cache hit: optimizer_params")
        ctx.optimizer_params = cached_opt
    else:
        ctx.optimizer_params = _collect_optimizer_params(cursor, ctx)
        if use_cache and ctx.optimizer_params:
            _optimizer_cache.put("global", ctx.optimizer_params)

    cursor.close()
    return ctx


def _collect_db_version(cursor: oracledb.Cursor, ctx: CollectedContext) -> str | None:
    """Coleta versão do banco Oracle."""
    try:
        sql, params = db_version()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar versão do banco: {e}")
        return None


def _detect_object_type(
    cursor: oracledb.Cursor, schema: str, name: str, ctx: CollectedContext
) -> str:
    """Detecta se o objeto é TABLE, VIEW, etc."""
    try:
        sql, params = object_type(schema, name)
        rows = _execute_query(cursor, sql, params)
        if rows:
            return str(rows[0].get("object_type", "TABLE"))
        return "TABLE"
    except Exception:
        return "TABLE"


def _collect_view_expansion(
    cursor: oracledb.Cursor, schema: str, name: str, ctx: CollectedContext
) -> None:
    """Coleta tabelas internas de uma view via DDL + sqlglot parse."""
    view_key = f"{schema}.{name}"
    try:
        sql, params = table_ddl(schema, name)
        rows = _execute_query(cursor, sql, params)
        if not rows:
            return

        ddl_text = str(rows[0].get("ddl", ""))
        if not ddl_text.strip():
            return

        # Extrai o SELECT da view (tudo depois de "AS")
        view_tables = _parse_view_tables(ddl_text)
        if view_tables:
            ctx.view_expansions[view_key] = view_tables

    except Exception as e:
        ctx.errors.append(f"Erro ao coletar view expansion de {view_key}: {e}")


def _parse_view_tables(ddl_text: str) -> list[str]:
    """Extrai tabelas referenciadas na DDL de uma view via sqlglot."""
    # Tenta encontrar o SELECT dentro da DDL da view
    # DDL típica: CREATE OR REPLACE VIEW "SCHEMA"."VIEW" AS SELECT ... FROM ...
    upper = ddl_text.upper()
    as_pos = upper.find("\nAS\n")
    if as_pos == -1:
        as_pos = upper.find(" AS\n")
    if as_pos == -1:
        as_pos = upper.find(" AS ")
    if as_pos == -1:
        return []

    select_sql = ddl_text[as_pos + 3 :].strip().rstrip(";").strip()
    if not select_sql:
        return []

    try:
        statements = sqlglot.parse(select_sql, dialect="oracle")
    except sqlglot.errors.ParseError:
        # Fallback: regex simples pra FROM/JOIN
        import re

        tables = set()
        for match in re.finditer(
            r'\b(?:FROM|JOIN)\s+("?\w+"?\.)?"?(\w+)"?', select_sql, re.IGNORECASE
        ):
            schema_part = (match.group(1) or "").strip(".").strip('"')
            table_name = match.group(2).strip('"')
            if schema_part:
                tables.add(f"{schema_part}.{table_name}")
            else:
                tables.add(table_name)
        return sorted(tables)

    tables = set()
    for stmt in statements:
        if stmt is None:
            continue
        for table in stmt.find_all(exp.Table):
            if table.name:
                tname = table.name.upper()
                tschema = (table.db or "").upper()
                if tschema:
                    tables.add(f"{tschema}.{tname}")
                else:
                    tables.add(tname)
    return sorted(tables)


def _inline_binds(sql_text: str, bind_params: dict | None) -> str:
    """Substitui :bind por literais no SQL para uso em EXPLAIN PLAN (DDL).

    EXPLAIN PLAN FOR é DDL — Oracle não aceita bind variables via cursor.execute().
    Precisamos injetar os valores como literais diretamente no texto SQL.
    Isso é seguro porque o EXPLAIN PLAN não modifica dados.
    """
    if not bind_params:
        return sql_text

    import re

    def _replacer(match: re.Match) -> str:
        name = match.group(1)
        # Busca case-insensitive
        for k, v in bind_params.items():
            if k.upper() == name.upper():
                if v is None:
                    return "NULL"
                if isinstance(v, int | float):
                    return str(v)
                # String: escapa aspas simples
                return f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"
        # Bind não fornecido — substitui por NULL pra não quebrar o EXPLAIN
        return "NULL"

    return re.sub(r"(?<!:):([A-Za-z_]\w*)", _replacer, sql_text)


def _collect_explain_plan(
    cursor: oracledb.Cursor,
    sql_text: str,
    ctx: CollectedContext,
    bind_params: dict[str, str | int | float | None] | None = None,
) -> list[str] | None:
    """Coleta o plano de execução.

    EXPLAIN PLAN FOR é DDL e não aceita bind variables via cursor.execute().
    Os binds são substituídos por literais diretamente no SQL.
    """
    try:
        # Substitui binds por literais (EXPLAIN PLAN é DDL, não aceita binds)
        inlined_sql = _inline_binds(sql_text, bind_params)
        steps = explain_plan(inlined_sql)

        # Step 1: EXPLAIN PLAN FOR ... (sem binds, já inlined)
        explain_stmt, params = steps[0]
        cursor.execute(explain_stmt, params)

        # Step 2: Busca resultado
        sql, params = steps[1]
        cursor.execute(sql, params)
        plan_lines = [row[0] for row in cursor]

        # Step 3: Limpa PLAN_TABLE
        sql, params = steps[2]
        cursor.execute(sql, params)

        return plan_lines
    except Exception as e:
        msg = f"Erro ao coletar EXPLAIN PLAN: {e}"
        # Extrai offset do erro Oracle pra indicar a linha problemática
        if hasattr(e, "args") and e.args and hasattr(e.args[0], "offset"):
            offset = e.args[0].offset
            if offset and offset > 0:
                # O offset é relativo ao explain_stmt completo
                # Desconta o prefixo "EXPLAIN PLAN SET STATEMENT_ID = '...' FOR "
                prefix_len = len(explain_stmt) - len(inlined_sql)
                adj_offset = offset - prefix_len
                if 0 <= adj_offset < len(inlined_sql):
                    # Calcula linha e coluna no SQL com binds inlined
                    before = inlined_sql[:adj_offset]
                    line_no = before.count("\n") + 1
                    line_start = before.rfind("\n") + 1
                    line_end = inlined_sql.find("\n", adj_offset)
                    if line_end == -1:
                        line_end = len(inlined_sql)
                    offending_line = inlined_sql[line_start:line_end].strip()
                    msg += f'\n- Line {line_no}: "{offending_line}"'

        # Sugere GRANTs de EXECUTE pra funções PL/SQL quando ORA-01031
        err_code = getattr(e.args[0], "code", 0) if e.args else 0
        if err_code == 1031 and ctx.parsed_sql.functions:
            msg += "\nHelp: https://docs.oracle.com/error-help/db/ora-01031/"
            msg += "\nFix: conceda EXECUTE nas funções PL/SQL referenciadas:"
            for fn in ctx.parsed_sql.functions:
                schema = fn.get("schema", "")
                name = fn.get("name", "")
                qualified = f"{schema}.{name}" if schema else name
                msg += f"\n  GRANT EXECUTE ON {qualified} TO SQLMENTOR_EXEC_ROLE;"

        ctx.errors.append(msg)
        return None


def _collect_runtime_execution(
    cursor: oracledb.Cursor,
    conn: oracledb.Connection,
    sql_text: str,
    ctx: CollectedContext,
    bind_params: dict[str, str | int | float | None] | None = None,
) -> None:
    """
    Executa a query com GATHER_PLAN_STATISTICS e coleta plano real + métricas.

    Só pra SELECTs — DMLs não são executados por segurança.
    A query é executada e os resultados descartados (fetchall).
    """
    try:
        # Ativa coleta de estatísticas na sessão
        cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = ALL")

        # Pega SID antes de executar a query
        cursor.execute("SELECT sid FROM v$mystat WHERE ROWNUM = 1")
        row = cursor.fetchone()
        sid = row[0] if row else None

        # Executa a query real (descarta resultado)
        cursor.execute(sql_text, bind_params or {})
        cursor.fetchall()

        # Pega sql_id da query que acabou de rodar (prev_sql_id = a anterior à atual)
        cursor.execute(
            "SELECT prev_sql_id FROM v$session WHERE sid = SYS_CONTEXT('USERENV', 'SID')"
        )
        row = cursor.fetchone()
        sql_id = row[0] if row else None

        if not sql_id:
            ctx.errors.append("Não foi possível obter sql_id da query executada")
            ctx.execution_plan = _collect_explain_plan(cursor, sql_text, ctx, bind_params)
            return

        # Plano real com ALLSTATS LAST usando sql_id explícito
        sql, params = runtime_plan(sql_id)
        cursor.execute(sql, params)
        ctx.runtime_plan = [r[0] for r in cursor]

        # Métricas de V$SQL
        sql, params = sql_runtime_stats(sql_id)
        rows = _execute_query(cursor, sql, params)
        ctx.runtime_stats = rows[0] if rows else None

        # Wait events da sessão
        if sid:
            sql, params = session_wait_events(sid)
            ctx.wait_events = _execute_query(cursor, sql, params)

        # Restaura STATISTICS_LEVEL
        cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = TYPICAL")

    except Exception as e:
        ctx.errors.append(f"Erro na execução runtime: {e}")
        # Fallback pro plano estimado
        ctx.execution_plan = _collect_explain_plan(cursor, sql_text, ctx, bind_params)


def _collect_ddl(
    cursor: oracledb.Cursor, schema: str, table_name: str, ctx: CollectedContext
) -> str | None:
    """Coleta DDL da tabela."""
    try:
        sql, params = table_ddl(schema, table_name)
        rows = _execute_query(cursor, sql, params)
        if rows:
            return str(rows[0].get("ddl", ""))
        return None
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar DDL de {schema}.{table_name}: {e}")
        return None


def _collect_table_stats(
    cursor: oracledb.Cursor, schema: str, table_name: str, ctx: CollectedContext
) -> dict[str, Any] | None:
    """Coleta estatísticas da tabela."""
    try:
        sql, params = table_stats(schema, table_name)
        rows = _execute_query(cursor, sql, params)
        return rows[0] if rows else None
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar stats de {schema}.{table_name}: {e}")
        return None


def _collect_column_stats(
    cursor: oracledb.Cursor, schema: str, table_name: str, ctx: CollectedContext
) -> list[dict[str, Any]]:
    """Coleta estatísticas de colunas."""
    try:
        sql, params = column_stats(schema, table_name)
        return _execute_query(cursor, sql, params)
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar column stats de {schema}.{table_name}: {e}")
        return []


def _collect_indexes(
    cursor: oracledb.Cursor, schema: str, table_name: str, ctx: CollectedContext
) -> list[dict[str, Any]]:
    """Coleta índices."""
    try:
        sql, params = indexes(schema, table_name)
        return _execute_query(cursor, sql, params)
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar índices de {schema}.{table_name}: {e}")
        return []


def _collect_constraints(
    cursor: oracledb.Cursor, schema: str, table_name: str, ctx: CollectedContext
) -> list[dict[str, Any]]:
    """Coleta constraints."""
    try:
        sql, params = constraints(schema, table_name)
        return _execute_query(cursor, sql, params)
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar constraints de {schema}.{table_name}: {e}")
        return []


def _collect_partitions(
    cursor: oracledb.Cursor, schema: str, table_name: str, ctx: CollectedContext
) -> list[dict[str, Any]]:
    """Coleta info de partições."""
    try:
        sql, params = table_partitions(schema, table_name)
        return _execute_query(cursor, sql, params)
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar partições de {schema}.{table_name}: {e}")
        return []


def _collect_histograms(
    cursor: oracledb.Cursor,
    schema: str,
    table_name: str,
    parsed: ParsedSQL,
    columns: list[dict],
    ctx: CollectedContext,
) -> dict[str, list[dict]]:
    """Coleta histogramas das colunas usadas em WHERE/JOIN."""
    result = {}

    # Identifica colunas relevantes (WHERE + JOIN)
    relevant_cols = set()
    for col_ref in parsed.where_columns + parsed.join_columns:
        # col_ref pode ser "alias.col" ou "col"
        parts = col_ref.split(".")
        relevant_cols.add(parts[-1].upper())

    for col_info in columns:
        col_name = col_info.get("column_name", "").upper()
        histogram_type = col_info.get("histogram", "NONE")

        if col_name in relevant_cols and histogram_type != "NONE":
            try:
                sql, params = histograms(schema, table_name, col_name)
                rows = _execute_query(cursor, sql, params)
                if rows:
                    result[col_name] = rows
            except Exception as e:
                ctx.errors.append(
                    f"Erro ao coletar histograma {schema}.{table_name}.{col_name}: {e}"
                )
    return result


def _collect_optimizer_params(cursor: oracledb.Cursor, ctx: CollectedContext) -> dict[str, str]:
    """Coleta parâmetros do otimizador."""
    try:
        sql, params = optimizer_params()
        rows = _execute_query(cursor, sql, params)
        return {row["name"]: row["value"] for row in rows}
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar optimizer params: {e}")
        return {}
