# Tech Stack

- **Linguagem:** Python 3.9+
- **Build:** setuptools (pyproject.toml)
- **CLI:** Typer com Rich para output formatado
- **SQL parsing:** sqlglot (dialeto Oracle)
- **DB driver:** oracledb (modo thin, sem Oracle Client)
- **Config:** PyYAML (conexões em ~/.sql-tuner/connections.yaml)
- **Testes:** nenhum framework configurado ainda

## Comandos

```bash
# Instalar em modo dev
pip install -e .

# Entry point
sql-tuner --help
sql-tuner analyze <arquivo.sql> --conn <profile>
sql-tuner parse <arquivo.sql> --schema <SCHEMA>
sql-tuner config add|list|test|remove
```

## Convenções

- Todas as queries Oracle usam bind variables (`:param`) — nunca f-strings com input do usuário.
- Funções de query retornam `tuple[str, dict]` (sql, params) prontas para `cursor.execute()`.
- Imports pesados (oracledb, connector, collector) são lazy dentro dos comandos CLI para manter o startup rápido.
- Docstrings e comentários em português.
- Código e nomes de variáveis/funções em inglês.
