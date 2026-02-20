# Tech Stack

- **Linguagem:** Python 3.9+
- **Build:** setuptools (pyproject.toml)
- **CLI:** Typer com Rich para output formatado
- **MCP Server:** FastMCP (mcp SDK Python) via stdio
- **SQL parsing:** sqlglot (dialeto Oracle)
- **DB driver:** oracledb (modo thin, sem Oracle Client)
- **Config:** PyYAML (conexões em ~/.sqlmentor/connections.yaml)
- **Testes:** nenhum framework configurado ainda

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

# MCP Server (normalmente iniciado pelo IDE, não manualmente)
sqlmentor-mcp
```

## ⚠️ Regra de Sincronização de Interfaces

CLI e MCP Server são interfaces sobre o mesmo core. Ao adicionar, remover ou alterar qualquer comando/tool/parâmetro, TODOS os arquivos abaixo devem ser atualizados na mesma operação:

1. `src/sql_tuner/cli.py` — comando CLI
2. `src/sql_tuner/mcp_server.py` — tool MCP equivalente
3. `powers/sqlmentor/POWER.md` — documentação da tool

Exceções: `config` é só CLI; `list_connections` e `test_connection` são só MCP.

Nunca registrar a mesma função duas vezes com `@mcp.tool()`. Nunca editar apenas um dos arquivos sem verificar os outros dois.

## Convenções

- Todas as queries Oracle usam bind variables (`:param`) — nunca f-strings com input do usuário.
- Funções de query retornam `tuple[str, dict]` (sql, params) prontas para `cursor.execute()`.
- Imports pesados (oracledb, connector, collector) são lazy dentro dos comandos CLI e MCP para manter o startup rápido.
- Docstrings e comentários em português.
- Código e nomes de variáveis/funções em inglês.
