# Tech Stack

- **Linguagem:** Python 3.9+
- **Build:** setuptools (pyproject.toml)
- **CLI:** Typer com Rich para output formatado
- **MCP Server:** FastMCP (mcp SDK Python) via stdio
- **SQL parsing:** sqlglot (dialeto Oracle)
- **DB driver:** oracledb (modo thin, sem Oracle Client)
- **Config:** PyYAML (conexões em ~/.sql-tuner/connections.yaml)
- **Testes:** nenhum framework configurado ainda

## Comandos

```bash
# Instalar em modo dev (registra ambos entry points)
pip install -e .

# CLI
sql-tuner --help
sql-tuner analyze <arquivo.sql> --conn <profile>
sql-tuner parse <arquivo.sql> --schema <SCHEMA>
sql-tuner config add|list|test|remove

# MCP Server (normalmente iniciado pelo IDE, não manualmente)
sql-tuner-mcp
```

## Convenções

- Todas as queries Oracle usam bind variables (`:param`) — nunca f-strings com input do usuário.
- Funções de query retornam `tuple[str, dict]` (sql, params) prontas para `cursor.execute()`.
- Imports pesados (oracledb, connector, collector) são lazy dentro dos comandos CLI e MCP para manter o startup rápido.
- Docstrings e comentários em português.
- Código e nomes de variáveis/funções em inglês.
- Mudanças em parâmetros/flags devem ser replicadas em: `cli.py`, `mcp_server.py`, e `powers/sql-tuner/POWER.md`.
