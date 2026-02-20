"""
Coletor de contexto Oracle para tuning de SQL.

Dado um SQL parseado e uma conexão, coleta todo o metadata necessário
para que uma IA possa analisar e sugerir melhorias.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import oracledb
import sqlglot
from sqlglot import exp

from sql_tuner.parser import ParsedSQL
from sql_tuner.queries import (
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


def _execute_query(
    cursor: oracledb.Cursor, sql: str, params: dict
) -> list[dict[str, Any]]:
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


def collect_context(
    parsed: ParsedSQL,
    conn: oracledb.Connection,
    default_schema: str,
    deep: bool = False,
    expand_views: bool = False,
    expand_functions: bool = False,
    execute: bool = False,
    bind_params: dict[str, str] | None = None,
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

    # 2. Metadata de cada tabela (deduplica por schema.name)
    collected_objects: set[str] = set()
    for table in parsed.tables:
        schema = (table.get("schema") or default_schema).upper()
        name = table["name"].upper()
        obj_key = f"{schema}.{name}"

        # Pula se já coletou esse objeto (mesma view/tabela referenciada 2x)
        if obj_key in collected_objects:
            continue
        collected_objects.add(obj_key)

        logger.info(f"Coletando contexto: {schema}.{name}")
        tctx = TableContext(name=name, schema=schema)

        # Detecta tipo do objeto (TABLE, VIEW, etc.)
        tctx.object_type = _detect_object_type(cursor, schema, name, ctx)

        # View expansion: coleta tabelas internas da view (sempre, é barato)
        if tctx.object_type == "VIEW":
            _collect_view_expansion(cursor, schema, name, ctx)

        # Views: só coleta detalhes se --expand-views foi passado
        if tctx.object_type == "VIEW" and not expand_views:
            ctx.tables.append(tctx)
            continue

        # DDL
        tctx.ddl = _collect_ddl(cursor, schema, name, ctx)

        # Table stats
        tctx.stats = _collect_table_stats(cursor, schema, name, ctx)

        # Column stats
        tctx.columns = _collect_column_stats(cursor, schema, name, ctx)

        # Indexes
        tctx.indexes = _collect_indexes(cursor, schema, name, ctx)

        # Constraints
        tctx.constraints = _collect_constraints(cursor, schema, name, ctx)

        # Deep mode: partitions + histograms
        if deep:
            tctx.partitions = _collect_partitions(cursor, schema, name, ctx)
            tctx.histograms = _collect_histograms(
                cursor, schema, name, parsed, tctx.columns, ctx
            )

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
            try:
                sql, params = index_to_table_map(schema)
                rows = _execute_query(cursor, sql, params)
                for row in rows:
                    idx_name = row.get("index_name", "")
                    tbl_name = row.get("table_name", "")
                    if idx_name and tbl_name:
                        ctx.index_table_map[idx_name.upper()] = tbl_name.upper()
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
    ctx.optimizer_params = _collect_optimizer_params(cursor, ctx)

    cursor.close()
    return ctx


def _collect_db_version(
    cursor: oracledb.Cursor, ctx: CollectedContext
) -> str | None:
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
            return rows[0].get("object_type", "TABLE")
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

    select_sql = ddl_text[as_pos + 3:].strip().rstrip(";").strip()
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
            schema_part = (match.group(1) or "").strip('.').strip('"')
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



def _collect_explain_plan(
    cursor: oracledb.Cursor, sql_text: str, ctx: CollectedContext,
    bind_params: dict[str, str] | None = None,
) -> list[str] | None:
    """Coleta o plano de execução."""
    try:
        steps = explain_plan(sql_text)

        # Step 1: EXPLAIN PLAN FOR ...
        # EXPLAIN PLAN FOR aceita binds — Oracle resolve como literals no plano
        sql, params = steps[0]
        params.update(bind_params or {})
        cursor.execute(sql, params)

        # Step 2: Busca resultado
        sql, params = steps[1]
        cursor.execute(sql, params)
        plan_lines = [row[0] for row in cursor]

        # Step 3: Limpa PLAN_TABLE
        sql, params = steps[2]
        cursor.execute(sql, params)

        return plan_lines
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar EXPLAIN PLAN: {e}")
        return None


def _collect_runtime_execution(
    cursor: oracledb.Cursor,
    conn: oracledb.Connection,
    sql_text: str,
    ctx: CollectedContext,
    bind_params: dict[str, str] | None = None,
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
        cursor.execute(
            "SELECT sid FROM v$mystat WHERE ROWNUM = 1"
        )
        row = cursor.fetchone()
        sid = row[0] if row else None

        # Executa a query real (descarta resultado)
        cursor.execute(sql_text, bind_params or {})
        cursor.fetchall()

        # Pega sql_id da query que acabou de rodar (prev_sql_id = a anterior à atual)
        cursor.execute(
            "SELECT prev_sql_id FROM v$session "
            "WHERE sid = SYS_CONTEXT('USERENV', 'SID')"
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


def _collect_optimizer_params(
    cursor: oracledb.Cursor, ctx: CollectedContext
) -> dict[str, str]:
    """Coleta parâmetros do otimizador."""
    try:
        sql, params = optimizer_params()
        rows = _execute_query(cursor, sql, params)
        return {row["name"]: row["value"] for row in rows}
    except Exception as e:
        ctx.errors.append(f"Erro ao coletar optimizer params: {e}")
        return {}
