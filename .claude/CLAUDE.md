# SqlMentor

CLI Python para Oracle 11g+ e MariaDB 10.6+ **[BETA]**. Extrai metadata de SQL (plano, DDLs, índices, stats, constraints, optimizer params) e gera relatórios Markdown/JSON otimizados para LLMs. Licença MIT.

Público-alvo: DBAs e desenvolvedores que querem contexto estruturado para tuning assistido por IA.

## Interfaces

- **CLI** (`sqlmentor`): interface principal, relatórios salvos em `reports/`. Comandos: `analyze`, `inspect` (Oracle only), `get-sql`, `parse`, `config`, `doctor`
- **Claude Code Agent** (`.claude/agents/sqlmentor.md`): agente DBA Oracle/MariaDB sênior que opera o CLI e produz análises de tuning
- **MCP Server** (`sqlmentor-mcp`): alternativo, integração com IDEs via Model Context Protocol (stdio)
- **Kiro Power** (`powers/sqlmentor/`): MCP + metodologia de análise para times que usam Kiro

## Estrutura

```
sqlmentor/
├── CLAUDE.md                     # Este arquivo
├── LICENSE                       # MIT
├── pyproject.toml                # Build, deps, entry points
├── .claude/
│   ├── settings.json             # MCP server config + permissões
│   ├── agents/sqlmentor.md       # Agente DBA Oracle/MariaDB sênior
│   └── rules/
│       ├── architecture.md       # Pipeline compressão R1-R12, contratos de dados
│       └── sync-checklist.md     # Checklists para alterações
├── scripts/
│   ├── oracle_create_user.sql    # Script DBA para criar user read-only Oracle
│   ├── mariadb_create_user.sql   # Script DBA para criar user read-only MariaDB
│   └── batch_inspect.py          # Batch inspect (CSV → relatórios em 3 verbosidades)
├── tests/                        # pytest + hypothesis
│   └── fixtures/sample_plan.txt  # Fixture de plano Oracle para testes de compressão
├── powers/sqlmentor/             # Kiro Power (POWER.md + mcp.json + steering)
└── src/sqlmentor/
    ├── cli.py                    # CLI Typer (analyze, inspect, get-sql, parse, config, doctor)
    ├── mcp_server.py             # MCP Server (analyze_sql, inspect_sql, get_sql_text, parse_sql, list/test_connections)
    ├── parser.py                 # Parse SQL via sqlglot + regex fallback
    ├── connector.py              # CRUD conexões (~/.sqlmentor/connections.yaml)
    ├── collector.py              # Coleta metadata Oracle (TableContext, CollectedContext)
    ├── report.py                 # Markdown/JSON + compressão R1-R12
    └── queries/                  # Queries Oracle (funções retornam tuple[str, dict])
```

Entry points: `sqlmentor` → `cli.py:app` | `sqlmentor-mcp` → `mcp_server.py:main`

## Tech Stack

Python 3.12+ · setuptools · Typer + Rich · FastMCP (stdio) · sqlglot (Oracle/MySQL) · oracledb (thin/thick) · PyMySQL · PyYAML · pytest + hypothesis · ruff · mypy · GitHub Actions CI (cobertura ≥ 90%)

## Comandos

```bash
pip install -e .                                         # install dev (Oracle + MariaDB)
sqlmentor analyze <file.sql> --conn <profile>            # plano estimado
sqlmentor analyze <file.sql> --conn <profile> --execute  # plano real
sqlmentor inspect <sql_id> --conn <profile>              # plano real via V$SQL (Oracle only)
sqlmentor get-sql <statement_id> --conn <profile>       # recupera texto SQL
sqlmentor parse <file.sql> --schema <SCHEMA>             # parse offline
sqlmentor config add oracle  -n prod -h host -s ORCL -u user   # conexão Oracle
sqlmentor config add mariadb -n dev  -h host -d mydb -u user   # conexão MariaDB
sqlmentor config list|test|remove                        # gerenciar conexões
sqlmentor doctor                                         # diagnóstico de ambiente
task lint                                                # ruff check + format
task test                                                # pytest
task test-cov                                            # pytest com cobertura
```

## Convenções

- **Idioma**: docstrings/comentários em PT-BR, código/variáveis em inglês
- **Queries Oracle**: sempre bind variables (`:param`), nunca f-strings com input do usuário. Exceção: `EXPLAIN PLAN FOR` e `DBMS_XPLAN` (não aceitam binds — usar f-string com `_validate_sql_id`)
- **Imports**: lazy dentro dos comandos CLI/MCP para startup rápido
- **Compressão**: novas regras Rn nunca usam nomes de objetos do schema como critério — apenas indicadores estruturais do plano
- **Query functions**: retornam `tuple[str, dict]` (sql, params) prontas para `cursor.execute()`
- **Dataclasses** em `collector.py` são o contrato entre coleta e relatório

## Regras de Sincronização

CLI, MCP e Kiro Power são interfaces sobre o mesmo core. Ao alterar flags/params:

1. `src/sqlmentor/cli.py` — opção Typer
2. `src/sqlmentor/mcp_server.py` — parâmetro na tool MCP
3. `powers/sqlmentor/POWER.md` — documentação
4. `README.md` — exemplos e tabela de flags

Exceções: `config` e `doctor` são só CLI; `list_connections` e `test_connection` são só MCP.
Flags só CLI (sem equivalente MCP): `--verbose`, `--output`, `--debug`.

O agente Claude Code descobre flags via `--help` — não requer atualização de parâmetros. Mas mudanças na metodologia de análise devem ser sincronizadas entre `.claude/agents/sqlmentor.md` e `powers/sqlmentor/steering/analysis.md`.

## Cache e Timeout

- **Cache LRU**: TTL de 1 hora, máximo 500 entradas. `--no-cache` limpa tudo.
- **Timeout de execução**: default 600s (10 min), sem limite superior. Quando `--execute` excede o `call_timeout`, o collector detecta `DPY-4011` e orienta a usar `sqlmentor inspect <sql_id>` (Oracle) ou `sqlmentor get-sql <digest>` (MariaDB). Fallback automático para plano estimado.

## Detalhes Técnicos

Para pipeline de compressão (R1-R12), thresholds, contrato de dados e checklists, veja `.claude/rules/`.
