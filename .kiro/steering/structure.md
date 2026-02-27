# Estrutura do Projeto

```
sqlmentor/
├── pyproject.toml              # Build config, dependências, entry points (sqlmentor + sqlmentor-mcp)
├── connections.example.yaml    # Exemplo de config de conexões
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI (Python 3.12, lint, format, mypy, tests)
├── scripts/
│   └── oracle_create_user.sql  # Script DBA para criar user read-only
├── tests/
│   ├── conftest.py             # Fixtures compartilhadas (ParsedSQL, CollectedContext, mock Oracle)
│   ├── test_parser.py          # Testes: parse_sql, normalize, binds, functions
│   ├── test_connector.py       # Testes: CRUD conexões, validate_privileges, resolve
│   ├── test_cli.py             # Testes: comandos CLI via CliRunner
│   ├── test_mcp_server.py      # Testes: tools MCP (parse, analyze, inspect, list, test)
│   ├── test_report_prune.py    # Testes: to_markdown, to_json, verbosity, pruning
│   ├── test_formatting_helpers.py # Testes: R9-R11 e helpers de formatação
│   └── test_plan_compression.py # Testes: regras R1-R8, R12 de compressão do plano
├── powers/
│   └── sqlmentor/              # Kiro Power (distribuição pro time)
│       ├── POWER.md            # Frontmatter + overview + workflow + troubleshooting
│       ├── mcp.json            # Config MCP apontando pro sqlmentor-mcp
│       └── steering/
│           └── analysis.md     # Metodologia de análise Oracle (DBA sênior)
└── src/sqlmentor/
    ├── __init__.py             # Versão do pacote
    ├── cli.py                  # Entry point CLI Typer (comandos: analyze, inspect, parse, config)
    ├── mcp_server.py           # Entry point MCP Server (tools: list_connections, test_connection, parse_sql, analyze_sql, inspect_sql)
    ├── parser.py               # Parse SQL → tabelas/colunas via sqlglot + fallback regex para PL/SQL
    ├── connector.py            # CRUD de conexões Oracle (~/.sqlmentor/connections.yaml)
    ├── collector.py            # Orquestra coleta de metadata Oracle (dataclasses: TableContext, CollectedContext)
    ├── report.py               # Gera Markdown/JSON a partir de CollectedContext
    └── queries/
        ├── __init__.py         # Todas as queries Oracle (cada função retorna tuple sql+params)
        └── oracle.py           # Re-export de queries/__init__ por conveniência
```

## Entry points

- `sqlmentor` → `cli.py:app` (CLI Typer)
- `sqlmentor-mcp` → `mcp_server.py:main` (MCP Server via stdio)

## Fluxo principal (compartilhado entre CLI e MCP)

1. `parser.py` extrai tabelas e colunas (sqlglot para DML, regex para PL/SQL)
2. `connector.py` abre conexão Oracle via profile salvo
3. `collector.py` coleta metadata de cada tabela (DDL, stats, índices, constraints, explain plan)
4. `report.py` formata tudo em Markdown ou JSON

A CLI (`cli.py`) e o MCP Server (`mcp_server.py`) são apenas interfaces diferentes sobre o mesmo core.

## Padrões

- Novos bancos de dados devem seguir o padrão de `queries/` — um módulo com funções que retornam `(sql, params)`.
- Dataclasses em `collector.py` são o contrato entre coleta e relatório.
- CLI e MCP Server usam lazy imports para não carregar oracledb no startup.
- Mudanças em flags/parâmetros do `analyze`/`inspect` devem ser replicadas em `cli.py`, `mcp_server.py`, e `powers/sqlmentor/POWER.md`.
