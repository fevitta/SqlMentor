# sql-tuner

CLI para coleta de contexto Oracle 11g+, otimizado para tuning de SQL assistido por IA.

Dado um arquivo SQL (query, procedure, trigger, function), o `sql-tuner` conecta no banco Oracle, extrai automaticamente todo o metadata relevante (plano de execução real, DDLs, índices, estatísticas, constraints, parâmetros do otimizador) e gera um relatório estruturado (Markdown ou JSON) pronto para ser consumido por um LLM.

## Instalação

```bash
pip install -e .
```

### Pré-requisitos

- Python 3.9+
- Acesso a um Oracle 11g+ (driver `oracledb` em modo thin, sem Oracle Client)

## Uso Rápido

### 1. Configurar conexão

```bash
sql-tuner config add \
  --name producao \
  --host 10.0.1.50 \
  --port 1521 \
  --service PROD \
  --user ANALISTA \
  --schema TELTELECOM
# (senha será solicitada de forma segura)
```

### 2. Analisar um SQL

```bash
# Relatório salvo em reports/ automaticamente
sql-tuner analyze minha_query.sql --conn producao

# Executa a query real e coleta plano com ALLSTATS LAST + métricas de runtime
sql-tuner analyze minha_query.sql --conn producao --execute

# Detalha views (DDL, colunas internas)
sql-tuner analyze minha_query.sql --conn producao --expand-views

# Análise profunda (histogramas e partições)
sql-tuner analyze minha_query.sql --conn producao --deep

# Formato JSON
sql-tuner analyze minha_query.sql --conn producao --format json

# Saída customizada
sql-tuner analyze minha_query.sql --conn producao --output meu_relatorio.md
```

### 3. Parse offline (sem conexão)

```bash
sql-tuner parse minha_query.sql --schema TELTELECOM
```

### 4. Gerenciar conexões

```bash
sql-tuner config list
sql-tuner config test --name producao
sql-tuner config remove --name producao
```

## O que é coletado

| Dado | Descrição | Flag |
|------|-----------|------|
| Plano real (ALLSTATS LAST) | Executa a query e coleta plano com stats reais | `--execute` |
| Plano estimado | `EXPLAIN PLAN` via `DBMS_XPLAN` | padrão |
| Runtime stats (V$SQL) | Elapsed, CPU, buffer gets, waits | `--execute` |
| Wait events | Top waits da sessão | `--execute` |
| Hotspots | Operações com efeito multiplicador e desvios de cardinalidade | `--execute` |
| Conversões implícitas | Detecta `TO_CHAR`, `TO_NUMBER` etc. nos predicados | sempre |
| View expansion | Tabelas internas das views, cruzadas com o plano | sempre |
| DDL de views | `DBMS_METADATA.GET_DDL` | `--expand-views` |
| Estatísticas de tabela | `ALL_TABLES` (rows, blocks, last_analyzed) | sempre |
| Colunas (filtradas) | Só colunas referenciadas no SQL (WHERE, JOIN, ORDER, GROUP) | sempre |
| Índices | Todos os índices das tabelas referenciadas | sempre |
| Constraints + FKs | PK, FK, UK, CHECK com tabela referenciada | sempre |
| Parâmetros do otimizador | `V$PARAMETER` com alertas de valores atípicos | sempre |
| Histogramas detalhados | `ALL_TAB_HISTOGRAMS` | `--deep` |
| Partições | `ALL_TAB_PARTITIONS` | `--deep` |

## Relatórios

Reports são salvos automaticamente em `reports/` com o formato:

```
reports/report_20260216_180154_desc_unificado.md
reports/report_20260216_173034_8ff00107.md   (quando via --sql inline)
```

O relatório Markdown é otimizado para colar direto num chat com LLM. Inclui seções de diagnóstico automático (SQL Health, hotspots, conversões implícitas) que ajudam a IA a focar nos problemas reais.

## Funcionalidades

- Parse de SQL via sqlglot (dialeto Oracle) com fallback regex pra PL/SQL
- Detecção automática de CTEs (WITH ... AS) — não confunde alias de CTE com tabela real
- View expansion: identifica tabelas internas de views e cruza com o plano de execução
- Tabelas pequenas (< 1.000 rows) em formato compacto pra economizar context window
- Alertas automáticos: `optimizer_index_cost_adj` fora do padrão, parse calls excessivos, buffer gets/row alto

## MCP Server

O `sql-tuner` inclui um MCP Server que permite integração direta com IDEs como Kiro, Claude Desktop, etc. A IA chama as tools do sql-tuner automaticamente, sem o dev precisar rodar comandos no terminal.

### Tools disponíveis

| Tool | Descrição |
|------|-----------|
| `list_connections` | Lista profiles de conexão Oracle configurados |
| `test_connection` | Testa se um profile funciona (retorna versão e schema) |
| `parse_sql` | Parse offline — extrai tabelas, colunas, joins sem conectar |
| `analyze_sql` | Análise completa: conecta no Oracle, coleta contexto, retorna relatório |

### Configuração manual (mcp.json)

```json
{
  "mcpServers": {
    "sql-tuner": {
      "command": "sql-tuner-mcp",
      "args": []
    }
  }
}
```

Pré-requisito: `pip install -e .` para registrar o entry point `sql-tuner-mcp`.

### Kiro Power

Para times que usam Kiro, o Power em `powers/sql-tuner/` empacota o MCP Server + documentação + metodologia de análise. Instale via Powers UI → "Add Custom Power" → Local Directory → caminho absoluto de `powers/sql-tuner`.

O Power inclui um steering file (`analysis.md`) com a metodologia completa de análise de DBA sênior Oracle, carregado sob demanda quando a IA vai analisar um relatório.

## Estrutura do Projeto

```
sql-tuner/
├── pyproject.toml
├── connections.example.yaml
├── scripts/
│   └── oracle_create_user.sql
├── reports/                    # Relatórios gerados
├── powers/
│   └── sql-tuner/              # Kiro Power (MCP + docs + steering)
│       ├── POWER.md
│       ├── mcp.json
│       └── steering/
│           └── analysis.md     # Metodologia de análise Oracle
└── src/sql_tuner/
    ├── __init__.py
    ├── cli.py                  # Entry point CLI (sql-tuner)
    ├── mcp_server.py           # Entry point MCP (sql-tuner-mcp)
    ├── parser.py               # Parse SQL → tabelas/colunas (sqlglot + regex)
    ├── connector.py            # CRUD de conexões (~/.sql-tuner/connections.yaml)
    ├── collector.py            # Coleta metadata Oracle
    ├── report.py               # Gera Markdown/JSON
    └── queries/
        ├── __init__.py         # Queries Oracle (cada fn retorna tuple sql+params)
        └── oracle.py
```

## Roadmap

- [ ] Suporte MariaDB
- [x] MCP Server pra integração com Kiro/Claude Desktop
- [x] Kiro Power com metodologia de análise embutida
- [ ] Análise de procedures (EXPLAIN de cada SQL interno)
- [ ] Cache de metadata (evita re-coletar pra mesmas tabelas)
