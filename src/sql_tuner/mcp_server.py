"""
MCP Server para sql-tuner — expõe tools de análise SQL via Model Context Protocol.

Roda localmente via stdio. O Kiro (ou outro cliente MCP) spawna esse processo
e se comunica via JSON-RPC sobre stdin/stdout.

Entry point: sql-tuner-mcp
"""

import json
import logging
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "sql-tuner",
    description="Coleta contexto Oracle (plano de execução, DDLs, índices, stats) para tuning de SQL assistido por IA.",
)


@mcp.tool()
def list_connections() -> str:
    """Lista os profiles de conexão Oracle configurados.

    Retorna os nomes e detalhes (host, porta, service, user, schema)
    de todas as conexões salvas em ~/.sql-tuner/connections.yaml.
    Use antes de analyze_sql para saber qual profile usar.
    """
    from sql_tuner.connector import list_connections as _list

    connections = _list()
    if not connections:
        return json.dumps({
            "connections": [],
            "hint": "Nenhuma conexão configurada. Use o CLI: sql-tuner config add --name <nome> --host <host> --service <service> --user <user>",
        })

    result = []
    for name, cfg in connections.items():
        result.append({
            "name": name,
            "host": cfg.get("host", "?"),
            "port": cfg.get("port", "?"),
            "service": cfg.get("service", "?"),
            "user": cfg.get("user", "?"),
            "schema": cfg.get("schema", cfg.get("user", "?")).upper(),
        })
    return json.dumps({"connections": result})


@mcp.tool()
def test_connection(conn: str) -> str:
    """Testa uma conexão Oracle e retorna versão do banco e schema.

    Args:
        conn: Nome do profile de conexão (ex: "prod", "dev").
    """
    from sql_tuner.connector import test_connection as _test

    try:
        info = _test(conn)
        return json.dumps({"status": "ok", "version": info["version"], "schema": info["schema"]})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp.tool()
def parse_sql(sql_text: str, schema: str = "") -> str:
    """Parse offline de SQL — extrai tabelas, colunas, joins, subqueries sem conectar no banco.

    Útil para entender a estrutura da query antes de decidir se precisa de conexão.

    Args:
        sql_text: O SQL completo (SELECT, INSERT, UPDATE, DELETE, ou bloco PL/SQL).
        schema: Schema padrão para tabelas não qualificadas (opcional).
    """
    from sql_tuner.parser import parse_sql as _parse

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
    conn: str,
    schema: str = "",
    deep: bool = False,
    expand_views: bool = False,
    expand_functions: bool = False,
    execute: bool = False,
    binds: str = "",
    output_format: str = "markdown",
) -> str:
    """Analisa um SQL conectando no Oracle e coleta contexto completo para tuning.

    Retorna relatório com: plano de execução, DDLs, estatísticas de tabelas/colunas,
    índices, constraints, parâmetros do otimizador, e opcionalmente plano real com ALLSTATS.

    Args:
        sql_text: O SQL completo a ser analisado.
        conn: Nome do profile de conexão Oracle (use list_connections para ver disponíveis).
        schema: Schema padrão (sobrescreve o do profile). Opcional.
        deep: Se True, coleta histogramas e partições (mais lento, mais completo).
        expand_views: Se True, coleta DDL e colunas de views referenciadas.
        expand_functions: Se True, coleta DDL de funções PL/SQL referenciadas.
        execute: Se True, executa a query real e coleta plano com ALLSTATS LAST + métricas de runtime. Requer binds se o SQL tiver bind variables.
        binds: Bind variables no formato "nome=valor,nome2=valor2". Necessário com execute=True se o SQL usa :param.
        output_format: "markdown" (padrão, otimizado pra LLM) ou "json" (pra integração).
    """
    import re

    from sql_tuner.collector import collect_context
    from sql_tuner.connector import connect, get_connection_config
    from sql_tuner.parser import parse_sql as _parse
    from sql_tuner.report import to_json, to_markdown

    # Parse
    cfg = get_connection_config(conn)
    effective_schema = schema or cfg.get("schema", cfg.get("user", "").upper())
    parsed = _parse(sql_text, default_schema=effective_schema)

    # Conecta
    try:
        oracle_conn = connect(conn)
    except Exception as e:
        return json.dumps({"error": f"Falha na conexão '{conn}': {e}"})

    # Parseia binds
    bind_params: dict[str, str | int | float] = {}
    if binds:
        for pair in binds.split(","):
            pair = pair.strip()
            if "=" not in pair:
                continue
            key, val = pair.split("=", 1)
            key, val = key.strip(), val.strip()
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
        remapped: dict[str, str | int | float] = {}
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
    return to_markdown(ctx)


def main():
    """Entry point para o MCP Server (stdio)."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
