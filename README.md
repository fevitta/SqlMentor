# SqlMentor

CLI + MCP Server Python para coleta de contexto Oracle 11g+, otimizado para tuning de SQL assistido por IA.

Dado um SQL (query, procedure, trigger, function), o `sqlmentor` conecta no banco Oracle, extrai automaticamente todo o metadata relevante (plano de execução real, DDLs, índices, estatísticas, constraints, parâmetros do otimizador) e gera um relatório estruturado (Markdown ou JSON) pronto para ser consumido por um LLM.

## Instalação

```bash
pip install -e .
```

Pré-requisitos: Python 3.12+ e acesso a um Oracle 11g+ (driver `oracledb` em modo thin, sem Oracle Client).

> Para Oracle < 12c (modo thick), veja [docs/oracle-instant-client.md](docs/oracle-instant-client.md).

## Uso Rápido

```bash
# Configurar conexão
sqlmentor config add --name prod --host 192.168.0.1 --port 1521 --service ORCL --user SQLMENTOR --schema SQLMENTOR
sqlmentor config set-default -n prod

# Análise com plano estimado
sqlmentor analyze minha_query.sql

# Plano real (executa a query com ALLSTATS LAST)
sqlmentor analyze minha_query.sql --execute

# Com bind variables
sqlmentor analyze minha_query.sql --execute -b id=123 -b status=A

# Inspecionar query já executada (sem re-executar)
sqlmentor inspect <sql_id>

# Parse offline (sem conexão)
sqlmentor parse minha_query.sql --schema SCHEMA

# Diagnóstico do ambiente
sqlmentor doctor
```

## Flags

| Flag | Descrição |
|------|-----------|
| `--execute` | Executa a query real, coleta plano com ALLSTATS LAST + runtime stats |
| `--deep` | Histogramas e partições |
| `--expand-views` | DDL e colunas internas das views |
| `--expand-functions` | DDL de funções PL/SQL referenciadas |
| `--verbosity compact\|full\|minimal` | Nível de compressão do relatório (default: compact) |
| `--show-sql` | Inclui texto SQL completo no relatório |
| `--show-all-indexes` | Mostra todos os índices (não só os referenciados no SQL) |
| `--no-cache` | Força re-coleta de metadata |
| `--format json` | Relatório em JSON |
| `--debug` | Mostra queries e tempos internos |
| `-b nome=valor` | Bind variables para `--execute` |

## O que é coletado

| Dado | Flag |
|------|------|
| Plano real (ALLSTATS LAST) + runtime stats (V$SQL) + wait events | `--execute` |
| Plano estimado (EXPLAIN PLAN) | padrão |
| Hotspots + conversões implícitas + view expansion | sempre |
| Estatísticas, colunas, índices, constraints + FKs | sempre |
| Parâmetros do otimizador (com alertas de valores atípicos) | sempre |
| DDL de views / funções PL/SQL | `--expand-views` / `--expand-functions` |
| Histogramas + partições | `--deep` |

Relatórios são salvos automaticamente em `reports/` em Markdown otimizado para colar direto num chat com LLM.

## MCP Server

Integração com IDEs (Kiro, Claude Desktop, etc.) via Model Context Protocol:

```json
{
  "mcpServers": {
    "sqlmentor": {
      "command": "sqlmentor-mcp",
      "args": []
    }
  }
}
```

### Tools

| Tool | Descrição |
|------|-----------|
| `list_connections` | Lista profiles de conexão configurados |
| `test_connection` | Testa um profile (retorna versão e schema) |
| `parse_sql` | Parse offline — tabelas, colunas, joins |
| `analyze_sql` | Análise completa: conecta, coleta contexto, retorna relatório |
| `inspect_sql` | Contexto de SQL já executado via sql_id |

### Workflow típico

```python
list_connections()                                          # ver profiles
parse_sql(sql_text="SELECT ...", schema="HR")               # parse offline
analyze_sql(sql_text="SELECT ...", conn="prod")             # plano estimado
analyze_sql(sql_text="SELECT ...", conn="prod", execute=True, binds="id=123")  # plano real
inspect_sql(sql_id="abc123xyz", conn="prod")                # via sql_id
```

### Kiro Power

Para times que usam Kiro: `powers/sqlmentor/` empacota MCP + metodologia de análise DBA sênior. Instale via Powers UI → Add Custom Power → caminho de `powers/sqlmentor`.

## Desenvolvimento

```bash
pip install -e ".[dev]"     # instalar com deps de dev
task test                   # pytest
task test-cov               # pytest com cobertura
task lint                   # ruff check
ruff format src/ tests/     # formatação
```

CI (GitHub Actions): Python 3.12, ruff check, ruff format --check, mypy, pytest com cobertura ≥ 90%.

## Roadmap

- [ ] Suporte a outros bancos de dados
- [ ] Suporte a versões mais novas do Oracle
- [ ] Análise de procedures (EXPLAIN de cada SQL interno)

## Licença

[MIT](LICENSE)
