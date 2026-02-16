# Estrutura do Projeto

```
sql-tuner/
├── pyproject.toml              # Build config, dependências, entry point
├── connections.example.yaml    # Exemplo de config de conexões
├── scripts/
│   └── oracle_create_user.sql  # Script DBA para criar user read-only
└── src/sql_tuner/
    ├── __init__.py             # Versão do pacote
    ├── cli.py                  # Entry point Typer (comandos: analyze, parse, config)
    ├── parser.py               # Parse SQL → tabelas/colunas via sqlglot + fallback regex para PL/SQL
    ├── connector.py            # CRUD de conexões Oracle (~/.sql-tuner/connections.yaml)
    ├── collector.py            # Orquestra coleta de metadata Oracle (dataclasses: TableContext, CollectedContext)
    ├── report.py               # Gera Markdown/JSON a partir de CollectedContext
    └── queries/
        ├── __init__.py         # Todas as queries Oracle (cada função retorna tuple sql+params)
        └── oracle.py           # Re-export de queries/__init__ por conveniência
```

## Fluxo principal

1. `cli.py` lê o arquivo SQL e resolve o schema
2. `parser.py` extrai tabelas e colunas (sqlglot para DML, regex para PL/SQL)
3. `connector.py` abre conexão Oracle via profile salvo
4. `collector.py` coleta metadata de cada tabela (DDL, stats, índices, constraints, explain plan)
5. `report.py` formata tudo em Markdown ou JSON

## Padrões

- Novos bancos de dados devem seguir o padrão de `queries/` — um módulo com funções que retornam `(sql, params)`.
- Dataclasses em `collector.py` são o contrato entre coleta e relatório.
- CLI usa lazy imports para não carregar oracledb no startup.
