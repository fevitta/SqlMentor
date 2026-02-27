# Tech Stack

- **Linguagem:** Python 3.10+
- **Build:** setuptools (pyproject.toml)
- **CLI:** Typer com Rich para output formatado
- **MCP Server:** FastMCP (mcp SDK Python) via stdio
- **SQL parsing:** sqlglot (dialeto Oracle)
- **DB driver:** oracledb (modo thin, sem Oracle Client)
- **Config:** PyYAML (conexões em ~/.sqlmentor/connections.yaml)
- **Testes:** pytest + hypothesis (property-based) + pytest-cov (cobertura), taskipy (task runner), ruff (lint/format)
- **CI:** GitHub Actions (Python 3.10/3.12) — lint, format check, testes

## Comandos

```bash
# Instalar em modo dev (registra ambos entry points)
pip install -e .

# CLI
sqlmentor --help
sqlmentor analyze <arquivo.sql> --conn <profile>
sqlmentor inspect <sql_id> --conn <profile>
sqlmentor parse <arquivo.sql> --schema <SCHEMA>
sqlmentor config add|list|test|remove
sqlmentor doctor

# MCP Server (normalmente iniciado pelo IDE, não manualmente)
sqlmentor-mcp
```

## ⚠️ Regra de Sincronização de Interfaces

CLI e MCP Server são interfaces sobre o mesmo core. Ao adicionar, remover ou alterar qualquer comando/tool/parâmetro, TODOS os arquivos abaixo devem ser atualizados na mesma operação:

1. `src/sqlmentor/cli.py` — comando CLI
2. `src/sqlmentor/mcp_server.py` — tool MCP equivalente
3. `powers/sqlmentor/POWER.md` — documentação da tool
4. `README.md` — exemplos de uso, tabela de flags, lista de subcomandos

Exceções: `config` e `doctor` são só CLI; `list_connections` e `test_connection` são só MCP.
Flags somente CLI (não precisam de equivalente MCP): `--verbose` (controle de display no terminal), `--output` (escrita em arquivo — MCP retorna conteúdo direto).

Nunca registrar a mesma função duas vezes com `@mcp.tool()`. Nunca editar apenas um dos arquivos sem verificar os outros dois.

## ⚠️ Regra de Atualização de Documentação Técnica

Ao alterar qualquer lógica de compressão do plano (`report.py`), fluxos de execução, contratos de dados ou parâmetros públicos, `.kiro/steering/architecture.md` deve ser atualizado na mesma operação. Isso inclui:

- Adicionar/remover/alterar regras R1–Rn: atualizar tabela de regras, exemplos de formato e checklist
- Alterar thresholds R5: atualizar tabela de imunidade
- Alterar `verbosity`: atualizar tabela de parâmetro
- Alterar dataclasses `CollectedContext`, `TableContext`, `PlanBlock`: atualizar contrato de dados
- Alterar fluxos CLI/MCP: atualizar diagramas mermaid correspondentes

## Convenções

- Todas as queries Oracle usam bind variables (`:param`) — nunca f-strings com input do usuário.
  - **Exceção**: `EXPLAIN PLAN FOR` e `DBMS_XPLAN.DISPLAY_CURSOR` não aceitam binds no Oracle. Nesses casos, usa-se f-string com validação prévia do input (`_validate_sql_id` para sql_id).
- Funções de query retornam `tuple[str, dict]` (sql, params) prontas para `cursor.execute()`.
- Imports pesados (oracledb, connector, collector) são lazy dentro dos comandos CLI e MCP para manter o startup rápido.
- Docstrings e comentários em português.
- Código e nomes de variáveis/funções em inglês.
