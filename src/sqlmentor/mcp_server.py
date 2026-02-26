"""
MCP Server para sqlmentor — expõe tools de análise SQL via Model Context Protocol.

Roda localmente via stdio. O Kiro (ou outro cliente MCP) spawna esse processo
e se comunica via JSON-RPC sobre stdin/stdout.

Entry point: sqlmentor-mcp
"""

import json
import logging

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "sqlmentor",
    instructions="Coleta contexto Oracle (plano de execução, DDLs, índices, stats) para tuning de SQL assistido por IA.",
)


@mcp.tool()
def list_connections() -> str:
    """Lista os profiles de conexão Oracle configurados.

    Retorna os nomes e detalhes (host, porta, service, user, schema)
    de todas as conexões salvas em ~/.sqlmentor/connections.yaml.
    Use antes de analyze_sql para saber qual profile usar.
    """
    from sqlmentor.connector import get_default_connection
    from sqlmentor.connector import list_connections as _list

    connections = _list()
    if not connections:
        return json.dumps({
            "connections": [],
            "hint": "Nenhuma conexão configurada. Use o CLI: sqlmentor config add --name <nome> --host <host> --service <service> --user <user>",
        })

    default_name = get_default_connection()
    result = []
    for name, cfg in connections.items():
        result.append({
            "name": name,
            "host": cfg.get("host", "?"),
            "port": cfg.get("port", "?"),
            "service": cfg.get("service", "?"),
            "user": cfg.get("user", "?"),
            "schema": cfg.get("schema", cfg.get("user", "?")).upper(),
            "timeout": cfg.get("timeout", 180),
            "default": name == default_name,
        })
    return json.dumps({"connections": result})


@mcp.tool()
def test_connection(conn: str) -> str:
    """Testa uma conexão Oracle e retorna versão do banco e schema.

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
def parse_sql(sql_text: str, schema: str = "", denorm_mode: str = "literal") -> str:
    """Parse offline de SQL — extrai tabelas, colunas, joins, subqueries sem conectar no banco.

    Útil para entender a estrutura da query antes de decidir se precisa de conexão.
    Auto-detecta SQL normalizado (Datadog, OEM, etc.) e desnormaliza antes do parse.

    Args:
        sql_text: O SQL completo (SELECT, INSERT, UPDATE, DELETE, ou bloco PL/SQL).
        schema: Schema padrão para tabelas não qualificadas (opcional).
        denorm_mode: Estratégia de desnormalização se SQL normalizado: "literal" (default, '?' → '1') ou "bind" ('?' → :dn1, :dn2...).
    """
    from sqlmentor.parser import denormalize_sql, is_normalized_sql
    from sqlmentor.parser import parse_sql as _parse

    # Auto-detecção de SQL normalizado (Datadog, OEM, etc.)
    if is_normalized_sql(sql_text):
        sql_text, _ = denormalize_sql(sql_text, mode=denorm_mode)

    parsed = _parse(sql_text, default_schema=schema or None)
    return json.dumps({
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
    })


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
) -> str:
    """Analisa um SQL conectando no Oracle e coleta contexto completo para tuning.

    Retorna relatório com: plano de execução, DDLs, estatísticas de tabelas/colunas,
    índices, constraints, parâmetros do otimizador, e opcionalmente plano real com ALLSTATS.

    Args:
        sql_text: O SQL completo a ser analisado.
        conn: Nome do profile de conexão Oracle. Se omitido, usa a conexão padrão (use list_connections para ver disponíveis e qual é o default).
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
    """
    import re

    from sqlmentor.collector import collect_context
    from sqlmentor.connector import connect, get_connection_config, resolve_connection
    from sqlmentor.parser import denormalize_sql, is_normalized_sql
    from sqlmentor.parser import parse_sql as _parse
    from sqlmentor.report import to_json, to_markdown

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
            return json.dumps({
                "error": "SQL normalizado detectado (placeholders '?'). Incompatível com execute=True — os literais originais foram perdidos.",
                "hint": "Use sem execute para obter plano estimado e metadata.",
            })
        sql_text, _denorm_binds = denormalize_sql(sql_text, mode=denorm_mode)

    # Resolve schema
    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())
    parsed = _parse(sql_text, default_schema=effective_schema)

    # Conecta
    try:
        oracle_conn = connect(conn, timeout=timeout if timeout > 0 else None)
    except Exception as e:
        return json.dumps({"error": f"Falha na conexão '{conn}': {e}"})

    # Parseia binds
    bind_params: dict[str, str | int | float | None] = {}
    if binds:
        for pair in binds.split(","):
            pair = pair.strip()
            if "=" not in pair:
                continue
            key, val = pair.split("=", 1)
            key, val = key.strip(), val.strip()
            # Trata null/None como Python None (Oracle NULL)
            if val.lower() in ("null", "none"):
                bind_params[key] = None
            else:
                try:
                    bind_params[key] = int(val)
                except ValueError:
                    try:
                        bind_params[key] = float(val)
                    except ValueError:
                        bind_params[key] = val

    # Detecta binds no SQL e remapeia case
    sql_bind_names = re.findall(r'(?<!:):([A-Za-z_]\w*)', sql_text)
    seen_upper: set[str] = set()
    unique_sql_binds: list[str] = []
    for name in sql_bind_names:
        if name.upper() not in seen_upper:
            seen_upper.add(name.upper())
            unique_sql_binds.append(name)

    if bind_params and unique_sql_binds:
        provided_upper = {k.upper(): v for k, v in bind_params.items()}
        remapped: dict[str, str | int | float | None] = {}
        for sql_name in unique_sql_binds:
            if sql_name.upper() in provided_upper:
                remapped[sql_name] = provided_upper[sql_name.upper()]
        bind_params = remapped

    # Verifica binds faltantes se execute=True
    if execute and unique_sql_binds:
        sql_binds_upper = {b.upper() for b in unique_sql_binds}
        provided_upper = {k.upper() for k in bind_params}
        missing = sql_binds_upper - provided_upper
        if missing:
            oracle_conn.close()
            return json.dumps({
                "error": f"Binds faltantes para --execute: {', '.join(sorted(missing))}",
                "hint": f"Passe binds='{'  ,'.join(f'{n}=<valor>' for n in sorted(missing))}'",
                "fallback": "Chamando sem execute para obter plano estimado.",
            })

    # Coleta
    try:
        ctx = collect_context(
            parsed=parsed,
            conn=oracle_conn,
            default_schema=effective_schema,
            deep=deep,
            expand_views=expand_views,
            expand_functions=expand_functions,
            execute=execute,
            bind_params=bind_params or None,
        )
    except Exception as e:
        oracle_conn.close()
        return json.dumps({"error": f"Erro na coleta: {e}"})
    finally:
        try:
            oracle_conn.close()
        except Exception:
            pass

    # Relatório
    if output_format.lower() == "json":
        return to_json(ctx)
    return to_markdown(ctx, verbosity=verbosity)

@mcp.tool()
def inspect_sql(
    sql_id: str,
    conn: str = "",
    schema: str = "",
    deep: bool = False,
    expand_views: bool = False,
    expand_functions: bool = False,
    output_format: str = "markdown",
    timeout: int = 0,
    verbosity: str = "compact",
) -> str:
    """Coleta contexto de um SQL já executado via sql_id, sem re-executar a query.

    Útil para queries longas que já rodaram (pelo dev, pelo sistema, etc.).
    Puxa o plano real e métricas do shared pool Oracle via V$SQL e DBMS_XPLAN.

    Args:
        sql_id: SQL_ID da query no shared pool Oracle (ex: "abc123def").
        conn: Nome do profile de conexão Oracle. Se omitido, usa a conexão padrão.
        schema: Schema padrão (sobrescreve o do profile). Opcional.
        deep: Se True, coleta histogramas e partições.
        expand_views: Se True, coleta DDL e colunas de views.
        expand_functions: Se True, coleta DDL de funções PL/SQL.
        output_format: "markdown" (padrão) ou "json".
        timeout: Timeout em segundos. 0 = usa default do profile (180s).
        verbosity: Nível de compressão do plano: "full" (sem compressão), "compact" (default, todas as podas), "minimal" (só hotspots+stats).
    """
    from sqlmentor.collector import collect_context
    from sqlmentor.connector import connect, get_connection_config, resolve_connection
    from sqlmentor.parser import parse_sql as _parse
    from sqlmentor.queries import runtime_plan, sql_runtime_stats, sql_text_by_id
    from sqlmentor.report import to_json, to_markdown

    # Resolve conexão (explícita > default > erro)
    try:
        conn = resolve_connection(conn or None)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())

    try:
        oracle_conn = connect(conn, timeout=timeout if timeout > 0 else None)
    except Exception as e:
        return json.dumps({"error": f"Falha na conexão '{conn}': {e}"})

    cursor = oracle_conn.cursor()

    # Recupera SQL original do shared pool
    try:
        sql_query, params = sql_text_by_id(sql_id)
        cursor.execute(sql_query, params)
        row = cursor.fetchone()
        if not row or not row[0]:
            oracle_conn.close()
            return json.dumps({
                "error": f"SQL_ID '{sql_id}' não encontrado no shared pool (V$SQL).",
                "hint": "O cursor pode ter sido expurgado. Tente re-executar a query.",
            })
        sql_text = str(row[0]).read() if hasattr(row[0], "read") else str(row[0])
    except Exception as e:
        oracle_conn.close()
        return json.dumps({"error": f"Erro ao buscar SQL: {e}"})

    # Parse
    parsed = _parse(sql_text, default_schema=effective_schema)

    # Plano real via sql_id
    runtime_plan_lines = None
    try:
        sql_query, params = runtime_plan(sql_id)
        cursor.execute(sql_query, params)
        runtime_plan_lines = [r[0] for r in cursor]
    except Exception:
        pass

    # Métricas V$SQL
    runtime_stats_data = None
    try:
        sql_query, params = sql_runtime_stats(sql_id)
        cursor.execute(sql_query, params)
        columns = [col[0].lower() for col in cursor.description or []]
        row = cursor.fetchone()
        runtime_stats_data = dict(zip(columns, row)) if row else None
    except Exception:
        pass

    cursor.close()

    # Coleta metadata das tabelas
    try:
        ctx = collect_context(
            parsed=parsed,
            conn=oracle_conn,
            default_schema=effective_schema,
            deep=deep,
            expand_views=expand_views,
            expand_functions=expand_functions,
            execute=False,
        )
    except Exception as e:
        oracle_conn.close()
        return json.dumps({"error": f"Erro na coleta: {e}"})
    finally:
        try:
            oracle_conn.close()
        except Exception:
            pass

    # Injeta plano real e métricas
    if runtime_plan_lines:
        ctx.runtime_plan = runtime_plan_lines
    if runtime_stats_data:
        ctx.runtime_stats = runtime_stats_data

    if output_format.lower() == "json":
        return to_json(ctx)
    return to_markdown(ctx, verbosity=verbosity)


def main():
    """Entry point para o MCP Server (stdio)."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
