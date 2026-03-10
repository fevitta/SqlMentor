"""
MCP Server para sqlmentor — expõe tools de análise SQL via Model Context Protocol.

Roda localmente via stdio. O Kiro (ou outro cliente MCP) spawna esse processo
e se comunica via JSON-RPC sobre stdin/stdout.

Entry point: sqlmentor-mcp
"""

import contextlib
import json
import logging

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "sqlmentor",
    instructions="Coleta contexto de banco de dados (plano de execução, DDLs, índices, stats) para tuning de SQL assistido por IA.",
)


def _validate_timeout_mcp(timeout: int) -> str | None:
    """Valida timeout MCP: deve ser 0 (default) ou entre 1 e 3600."""
    if timeout != 0 and (timeout < 1 or timeout > 3600):
        return json.dumps(
            {"error": f"Timeout inválido: {timeout}. Deve ser 0 (default) ou entre 1 e 3600."}
        )
    return None


@mcp.tool()
def list_connections() -> str:
    """Lista os profiles de conexão configurados.

    Retorna os nomes e detalhes (host, porta, service, user, schema)
    de todas as conexões salvas em ~/.sqlmentor/connections.yaml.
    Use antes de analyze_sql para saber qual profile usar.
    """
    from sqlmentor.connector import get_default_connection
    from sqlmentor.connector import list_connections as _list

    connections = _list()
    if not connections:
        return json.dumps(
            {
                "connections": [],
                "hint": "Nenhuma conexão configurada. Use o CLI: sqlmentor config add --name <nome> --host <host> --service <service> --user <user>",
            }
        )

    default_name = get_default_connection()
    result = []
    for name, cfg in connections.items():
        result.append(
            {
                "name": name,
                "host": cfg.get("host", "?"),
                "port": cfg.get("port", "?"),
                "service": cfg.get("service", "?"),
                "user": cfg.get("user", "?"),
                "schema": cfg.get("schema", cfg.get("user", "?")).upper(),
                "timeout": cfg.get("timeout", 180),
                "default": name == default_name,
            }
        )
    return json.dumps({"connections": result})


@mcp.tool()
def test_connection(conn: str) -> str:
    """Testa uma conexão e retorna versão do banco e schema.

    Args:
        conn: Nome do profile de conexão (ex: "prod", "dev").
    """
    from sqlmentor.connector import test_connection as _test

    try:
        info = _test(conn)
        return json.dumps({"status": "ok", "version": info["version"], "schema": info["schema"]})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp.tool()
def parse_sql(
    sql_text: str, schema: str = "", normalized: bool = False, denorm_mode: str = "literal"
) -> str:
    """Parse offline de SQL — extrai tabelas, colunas, joins, subqueries sem conectar no banco.

    Útil para entender a estrutura da query antes de decidir se precisa de conexão.
    Auto-detecta SQL normalizado (Datadog, OEM, etc.) e desnormaliza antes do parse.

    Args:
        sql_text: O SQL completo (SELECT, INSERT, UPDATE, DELETE, ou bloco PL/SQL).
        schema: Schema padrão para tabelas não qualificadas (opcional).
        normalized: Se True, trata o SQL como normalizado (Datadog, OEM, etc.). Auto-detectado se omitido.
        denorm_mode: Estratégia de desnormalização se SQL normalizado: "literal" (default, '?' → '1') ou "bind" ('?' → :dn1, :dn2...).
    """
    from sqlmentor.parser import denormalize_sql, is_normalized_sql
    from sqlmentor.parser import parse_sql as _parse

    # Auto-detecção de SQL normalizado (Datadog, OEM, etc.)
    if normalized or is_normalized_sql(sql_text):
        sql_text, _ = denormalize_sql(sql_text, mode=denorm_mode)

    parsed = _parse(sql_text, default_schema=schema or None)
    return json.dumps(
        {
            "sql_type": parsed.sql_type,
            "tables": parsed.table_names,
            "where_columns": parsed.where_columns,
            "join_columns": parsed.join_columns,
            "order_columns": parsed.order_columns,
            "group_columns": parsed.group_columns,
            "subqueries": parsed.subqueries,
            "functions": [f"{f['schema']}.{f['name']}" for f in parsed.functions],
            "is_parseable": parsed.is_parseable,
            "parse_errors": parsed.parse_errors,
        }
    )


@mcp.tool()
def analyze_sql(
    sql_text: str,
    conn: str = "",
    schema: str = "",
    deep: bool = False,
    expand_views: bool = False,
    expand_functions: bool = False,
    execute: bool = False,
    binds: str = "",
    output_format: str = "markdown",
    timeout: int = 0,
    normalized: bool = False,
    denorm_mode: str = "literal",
    verbosity: str = "compact",
    no_cache: bool = False,
    show_sql: bool = False,
    show_all_indexes: bool = False,
) -> str:
    """Analisa um SQL conectando no banco e coleta contexto completo para tuning.

    Retorna relatório com: plano de execução, DDLs, estatísticas de tabelas/colunas,
    índices, constraints, parâmetros do otimizador, e opcionalmente plano real com estatísticas de runtime.

    Args:
        sql_text: O SQL completo a ser analisado.
        conn: Nome do profile de conexão. Se omitido, usa a conexão padrão (use list_connections para ver disponíveis e qual é o default).
        schema: Schema padrão (sobrescreve o do profile). Opcional.
        deep: Se True, coleta histogramas e partições (mais lento, mais completo).
        expand_views: Se True, coleta DDL e colunas de views referenciadas.
        expand_functions: Se True, coleta DDL de funções PL/SQL referenciadas.
        execute: Se True, executa a query real e coleta plano com ALLSTATS LAST + métricas de runtime. Requer binds se o SQL tiver bind variables.
        binds: Bind variables no formato "nome=valor,nome2=valor2". Necessário com execute=True se o SQL usa :param.
        output_format: "markdown" (padrão, otimizado pra LLM) ou "json" (pra integração).
        timeout: Timeout em segundos para operações no banco. 0 = usa o default do profile (180s).
        normalized: Se True, trata o SQL como normalizado (Datadog, OEM, etc.). Auto-detectado se omitido. Incompatível com execute=True.
        denorm_mode: Estratégia de desnormalização: "literal" (default, '?' → '1') ou "bind" ('?' → :dn1, :dn2...). Bind gera plano com seletividade padrão do otimizador.
        verbosity: Nível de compressão do plano: "full" (sem compressão), "compact" (default, todas as podas), "minimal" (só hotspots+stats).
        no_cache: Se True, ignora cache e força re-coleta de metadata. Útil quando tabelas/índices foram alterados.
        show_sql: Se True, inclui texto SQL completo no relatório. Omitido por padrão no compact para economizar tokens.
        show_all_indexes: Se True, mostra todos os índices. Por padrão, só mostra índices cujas colunas são relevantes ao SQL.
    """
    from sqlmentor.collector import clear_cache, collect_context
    from sqlmentor.connector import connect_with_adapter, get_connection_config, resolve_connection
    from sqlmentor.parser import (
        denormalize_sql,
        detect_sql_binds,
        is_normalized_sql,
        parse_bind_values,
        remap_bind_params,
    )
    from sqlmentor.parser import parse_sql as _parse
    from sqlmentor.report import to_json, to_markdown

    if err := _validate_timeout_mcp(timeout):
        return err

    if no_cache:
        clear_cache()

    # Resolve conexão (explícita > default > erro)
    try:
        conn = resolve_connection(conn or None)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    # Auto-detecção de SQL normalizado (Datadog, OEM, etc.)
    if not normalized and is_normalized_sql(sql_text):
        normalized = True

    # Desnormaliza SQL se veio de ferramenta de monitoramento
    if normalized:
        if execute:
            return json.dumps(
                {
                    "error": "SQL normalizado detectado (placeholders '?'). Incompatível com execute=True — os literais originais foram perdidos.",
                    "hint": "Use sem execute para obter plano estimado e metadata.",
                }
            )
        sql_text, _denorm_binds = denormalize_sql(sql_text, mode=denorm_mode)

    # Resolve schema
    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())
    parsed = _parse(sql_text, default_schema=effective_schema)

    # Conecta
    try:
        adapter, db_conn = connect_with_adapter(conn, timeout=timeout if timeout > 0 else None)
    except Exception as e:
        return json.dumps({"error": f"Falha na conexão '{conn}': {e}"})

    # Parseia binds
    bind_params: dict[str, str | int | float | None] = {}
    if binds:
        raw_binds: dict[str, str] = {}
        for pair in binds.split(","):
            pair = pair.strip()
            if "=" not in pair:
                continue
            key, val = pair.split("=", 1)
            raw_binds[key.strip()] = val.strip()
        bind_params = parse_bind_values(raw_binds)

    # Detecta binds no SQL e remapeia case
    unique_sql_binds = detect_sql_binds(sql_text)
    bind_params = remap_bind_params(bind_params, unique_sql_binds)

    # Verifica binds faltantes se execute=True
    if execute and unique_sql_binds:
        sql_binds_upper = {b.upper() for b in unique_sql_binds}
        provided_upper = {k.upper() for k in bind_params}
        missing = sql_binds_upper - provided_upper
        if missing:
            db_conn.close()
            return json.dumps(
                {
                    "error": f"Binds faltantes para --execute: {', '.join(sorted(missing))}",
                    "hint": f"Passe binds='{'  ,'.join(f'{n}=<valor>' for n in sorted(missing))}'",
                    "fallback": "Chamando sem execute para obter plano estimado.",
                }
            )

    # Coleta
    try:
        ctx = collect_context(
            parsed=parsed,
            conn=db_conn,
            default_schema=effective_schema,
            deep=deep,
            expand_views=expand_views,
            expand_functions=expand_functions,
            execute=execute,
            bind_params=bind_params or None,
            use_cache=not no_cache,
            adapter=adapter,
        )
    except Exception as e:
        db_conn.close()
        return json.dumps({"error": f"Erro na coleta: {e}"})
    finally:
        with contextlib.suppress(Exception):
            db_conn.close()

    # Relatório
    if output_format.lower() == "json":
        if verbosity != "compact":
            logger.warning(
                "verbosity='%s' ignorado com format=json — JSON sempre retorna dados completos",
                verbosity,
            )
        return to_json(ctx)
    return to_markdown(
        ctx, verbosity=verbosity, show_sql=show_sql, show_all_indexes=show_all_indexes
    )


@mcp.tool()
def inspect_sql(
    statement_id: str,
    conn: str = "",
    schema: str = "",
    deep: bool = False,
    expand_views: bool = False,
    expand_functions: bool = False,
    output_format: str = "markdown",
    timeout: int = 0,
    verbosity: str = "compact",
    no_cache: bool = False,
    show_sql: bool = False,
    show_all_indexes: bool = False,
) -> str:
    """Coleta contexto de um SQL já executado via statement_id, sem re-executar a query.

    Útil para queries longas que já rodaram (pelo dev, pelo sistema, etc.).
    Puxa o plano real e métricas do banco (ex: V$SQL e DBMS_XPLAN no Oracle).

    Args:
        statement_id: Identificador do statement no banco (ex: sql_id Oracle "abc123def", queryid PostgreSQL).
        conn: Nome do profile de conexão. Se omitido, usa a conexão padrão.
        schema: Schema padrão (sobrescreve o do profile). Opcional.
        deep: Se True, coleta histogramas e partições.
        expand_views: Se True, coleta DDL e colunas de views.
        expand_functions: Se True, coleta DDL de funções PL/SQL.
        output_format: "markdown" (padrão) ou "json".
        timeout: Timeout em segundos. 0 = usa default do profile (180s).
        verbosity: Nível de compressão do plano: "full" (sem compressão), "compact" (default, todas as podas), "minimal" (só hotspots+stats).
        no_cache: Se True, ignora cache e força re-coleta de metadata. Útil quando tabelas/índices foram alterados.
        show_sql: Se True, inclui texto SQL completo no relatório.
        show_all_indexes: Se True, mostra todos os índices.
    """
    from sqlmentor.collector import clear_cache, collect_context
    from sqlmentor.connector import connect_with_adapter, get_connection_config, resolve_connection
    from sqlmentor.parser import parse_sql as _parse
    from sqlmentor.report import to_json, to_markdown

    if err := _validate_timeout_mcp(timeout):
        return err

    if no_cache:
        clear_cache()

    # Resolve conexão (explícita > default > erro)
    try:
        conn = resolve_connection(conn or None)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())

    try:
        adapter, db_conn = connect_with_adapter(conn, timeout=timeout if timeout > 0 else None)
    except Exception as e:
        return json.dumps({"error": f"Falha na conexão '{conn}': {e}"})

    qb = adapter.query_builder
    cursor = db_conn.cursor()

    # Recupera SQL original do shared pool
    try:
        sql_query, params = qb.sql_text_by_id(statement_id)
        cursor.execute(sql_query, params)
        row = cursor.fetchone()
        if not row or not row[0]:
            db_conn.close()
            return json.dumps(
                {
                    "error": f"Statement '{statement_id}' não encontrado no shared pool.",
                    "hint": "O cursor pode ter sido expurgado. Tente re-executar a query.",
                }
            )
        sql_text = str(row[0]).read() if hasattr(row[0], "read") else str(row[0])  # type: ignore[attr-defined]
    except Exception as e:
        db_conn.close()
        return json.dumps({"error": f"Erro ao buscar SQL: {e}"})

    # Parse
    parsed = _parse(sql_text, default_schema=effective_schema)

    # Plano e métricas — fluxo difere entre MariaDB e Oracle
    runtime_plan_lines = None
    execution_plan_lines = None
    runtime_stats_data = None

    if adapter.db_type == "mariadb":
        # MariaDB: sem planos históricos — usa EXPLAIN FORMAT=JSON no SQL recuperado
        try:
            steps = qb.explain_plan(sql_text)
            explain_sql, explain_params = steps[0]
            cursor.execute(explain_sql, explain_params)
            row = cursor.fetchone()
            if row and row[0]:
                execution_plan_lines = str(row[0]).splitlines()
        except Exception as e:
            logger.warning(
                "Falha ao gerar plano estimado para statement_id '%s': %s", statement_id, e
            )

        try:
            sql_query, params = qb.sql_runtime_stats(statement_id)
            cursor.execute(sql_query, params)
            columns = [col[0].lower() for col in cursor.description or []]
            row = cursor.fetchone()
            runtime_stats_data = dict(zip(columns, row, strict=False)) if row else None
        except Exception as e:
            logger.warning(
                "Falha ao recuperar métricas para statement_id '%s': %s", statement_id, e
            )
    else:
        # Oracle: plano real via DBMS_XPLAN.DISPLAY_CURSOR
        try:
            sql_query, params = qb.runtime_plan(statement_id)
            cursor.execute(sql_query, params)
            runtime_plan_lines = [r[0] for r in cursor]
        except Exception as e:
            logger.warning(
                "Falha ao recuperar plano real para statement_id '%s': %s", statement_id, e
            )

        try:
            sql_query, params = qb.sql_runtime_stats(statement_id)
            cursor.execute(sql_query, params)
            columns = [col[0].lower() for col in cursor.description or []]
            row = cursor.fetchone()
            runtime_stats_data = dict(zip(columns, row, strict=False)) if row else None
        except Exception as e:
            logger.warning(
                "Falha ao recuperar métricas para statement_id '%s': %s", statement_id, e
            )

    cursor.close()

    # Coleta metadata das tabelas
    try:
        ctx = collect_context(
            parsed=parsed,
            conn=db_conn,
            default_schema=effective_schema,
            deep=deep,
            expand_views=expand_views,
            expand_functions=expand_functions,
            execute=False,
            use_cache=not no_cache,
            adapter=adapter,
        )
    except Exception as e:
        db_conn.close()
        return json.dumps({"error": f"Erro na coleta: {e}"})
    finally:
        with contextlib.suppress(Exception):
            db_conn.close()

    # Injeta plano e métricas
    if adapter.db_type == "mariadb":
        if execution_plan_lines:
            ctx.execution_plan = execution_plan_lines
    else:
        if runtime_plan_lines:
            ctx.runtime_plan = runtime_plan_lines
    if runtime_stats_data:
        ctx.runtime_stats = runtime_stats_data

    if output_format.lower() == "json":
        if verbosity != "compact":
            logger.warning(
                "verbosity='%s' ignorado com format=json — JSON sempre retorna dados completos",
                verbosity,
            )
        return to_json(ctx)
    return to_markdown(
        ctx, verbosity=verbosity, show_sql=show_sql, show_all_indexes=show_all_indexes
    )


@mcp.tool()
def get_status() -> str:
    """Retorna status do servidor MCP: versão, estado e estatísticas de cache.
    Não requer conexão Oracle. Útil para health-check.
    """
    from sqlmentor import __version__
    from sqlmentor.collector import _index_map_cache, _optimizer_cache, _table_cache

    return json.dumps(
        {
            "version": __version__,
            "status": "ok",
            "cache": {
                "tables": len(_table_cache),
                "optimizer": len(_optimizer_cache),
                "index_maps": len(_index_map_cache),
            },
        }
    )


def main():
    """Entry point para o MCP Server (stdio)."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
