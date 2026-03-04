# SqlMentor

CLI + MCP Server Python para Oracle 11g+. Extrai metadata de SQL (plano, DDLs, índices, stats, constraints, optimizer params) e gera relatórios Markdown/JSON otimizados para LLMs.

## Documentação Detalhada

A documentação técnica completa está em `.kiro/steering/`:

| Arquivo | Conteúdo |
|---------|----------|
| `.kiro/steering/product.md` | Visão do produto, público-alvo, interfaces (CLI, MCP, Kiro Power) |
| `.kiro/steering/architecture.md` | Fluxos (analyze, inspect, MCP), pipeline de compressão R1-R12, thresholds R5, contrato de dados, checklists de consistência |
| `.kiro/steering/structure.md` | Árvore de diretórios, entry points, fluxo principal, padrões de código |
| `.kiro/steering/tech.md` | Tech stack, comandos dev, regras de sincronização CLI/MCP, convenções |

**Leia esses arquivos antes de qualquer alteração significativa.**

## Quick Reference

### Estrutura

```
src/sqlmentor/
  cli.py          # CLI Typer (analyze, inspect, parse, config, doctor)
  mcp_server.py   # MCP Server (analyze_sql, inspect_sql, parse_sql, list/test_connections)
  parser.py       # Parse SQL via sqlglot + regex fallback
  connector.py    # CRUD conexões (~/.sqlmentor/connections.yaml)
  collector.py    # Coleta metadata Oracle (TableContext, CollectedContext)
  report.py       # Markdown/JSON + compressão R1-R12
  queries/        # Queries Oracle (funções retornam tuple[str, dict])
tests/            # pytest + hypothesis
powers/sqlmentor/ # Kiro Power (POWER.md + mcp.json + steering)
```

### Comandos

```bash
pip install -e .                                    # install dev
sqlmentor analyze <file.sql> --conn <profile>       # plano estimado
sqlmentor analyze <file.sql> --conn <profile> --execute  # plano real
sqlmentor inspect <sql_id> --conn <profile>         # plano real via V$SQL (sem re-executar)
task lint                                           # ruff check + format
task test                                           # pytest com cobertura
```

### Regras Essenciais

- **Sincronização**: alterações em flags/params devem ser replicadas em `cli.py`, `mcp_server.py`, `powers/sqlmentor/POWER.md` e `README.md`
- **Queries Oracle**: sempre bind variables (`:param`), nunca f-strings com input do usuário (exceção: `EXPLAIN PLAN FOR` e `DBMS_XPLAN`)
- **Compressão**: novas regras Rn nunca usam nomes de objetos do schema como critério — apenas indicadores estruturais do plano
- **Imports**: lazy dentro dos comandos CLI/MCP para startup rápido
- **Idioma**: docstrings/comentários em PT-BR, código/variáveis em inglês
- **CI**: Python 3.12, cobertura >= 90%
