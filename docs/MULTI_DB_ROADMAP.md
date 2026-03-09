# Roadmap: Suporte Multi-Database (PostgreSQL + MariaDB)

## Visão Geral

Adicionar suporte a PostgreSQL e MariaDB ao SqlMentor, que hoje é 100% Oracle.
Estratégia: adapter pattern com implementação faseada.

**Estimativa total: 5-6 meses**

---

## Grafo de Dependências

```
FASE 1 — Abstração (Fundação)
│
├─ T1  Interfaces base (DatabaseAdapter, QueryBuilder, PlanParser)
│   │
│   ├─ T2  Refatorar connector.py → OracleAdapter
│   │   │
│   │   └─ T3  Refatorar queries/__init__.py → queries/oracle.py
│   │       │
│   │       └─ T4  Refatorar collector.py → usar adapter
│   │           │
│   │           └─ T5  Refatorar report.py → PlanParser plugável
│   │               │
│   │               └─ T6  Refatorar CLI/MCP → dialect-aware
│   │                   │
│   │                   └─ T7  Testes de regressão Oracle
│   │
│   └─ T8  Estender schema de conexão (campo `type`)
│       │
│       └─ (T2 depende de T8 também)
│
├─ T9  parser.py → config por dialeto (system tables, builtins, binds)
│
│
FASE 2 — PostgreSQL
│  (depende de: T1-T9 completas)
│
├─ T10 PostgreSQLAdapter (conexão via psycopg/asyncpg)
│   │
│   ├─ T11 queries/postgresql.py (30+ queries: pg_catalog, information_schema)
│   │   │
│   │   └─ T12 Coleta de metadata PostgreSQL no collector
│   │       │
│   │       └─ T14 Testes de integração PostgreSQL (Docker)
│   │
│   ├─ T13 PostgreSQLPlanParser (EXPLAIN JSON → PlanBlock)
│   │   │
│   │   └─ T15 Validar regras R1-R12 com planos PostgreSQL
│   │       │
│   │       └─ (T14 depende de T15 também)
│   │
│   └─ T16 inspect PostgreSQL (pg_stat_statements → query_id)
│       │
│       └─ (T14 depende de T16 também)
│
│
FASE 3 — MariaDB
│  (depende de: T1-T9 completas; pode rodar em paralelo com Fase 2)
│
├─ T17 MariaDBAdapter (conexão via mysql-connector / PyMySQL)
│   │
│   ├─ T18 queries/mariadb.py (30+ queries: information_schema, performance_schema)
│   │   │
│   │   └─ T19 Coleta de metadata MariaDB no collector
│   │       │
│   │       └─ T21 Testes de integração MariaDB (Docker)
│   │
│   ├─ T20 MariaDBPlanParser (EXPLAIN FORMAT=JSON → PlanBlock)
│   │   │
│   │   └─ T22 Validar regras R1-R12 com planos MariaDB
│   │       │
│   │       └─ (T21 depende de T22 também)
│   │
│   └─ T23 inspect MariaDB (performance_schema.events_statements → digest)
│       │
│       └─ (T21 depende de T23 também)
│
│
FASE 4 — Finalização
│  (depende de: Fase 2 E Fase 3 completas)
│
├─ T24 CI multi-database (GitHub Actions matrix: Oracle + PG + MariaDB)
│   │
│   └─ T25 Documentação (README, POWER.md, agents, CLAUDE.md)
│       │
│       └─ T26 Release com suporte multi-DB
```

---

## Fase 1 — Abstração (Fundação) ✅ CONCLUÍDA

> Refatorar o código Oracle existente em adapters plugáveis sem quebrar funcionalidade.
> **Pré-requisito para todas as fases seguintes.**

Todas as 9 tarefas (T1-T9) concluídas. Adapter pattern implementado, Oracle refatorado, testes de regressão passando com cobertura ≥ 90%.

---

## Fase 2 — PostgreSQL

> **Depende de:** Fase 1 completa (T1-T9, especialmente T7 aprovado).
> Pode rodar em paralelo com Fase 3 se houver desenvolvedores separados.

### T10: PostgreSQLAdapter

| Campo | Valor |
|---|---|
| **Descrição** | Implementar `DatabaseAdapter` para PostgreSQL usando `psycopg` (v3) |
| **Depende de** | T1, T2 |
| **Bloqueia** | T11, T12, T14, T16 |
| **Esforço** | 4 dias |

**Escopo:**
- Conexão via `psycopg.connect()` com suporte a SSL
- `test_connection()` via `SELECT version()`
- `validate_privileges()` via `has_schema_privilege()`, `has_table_privilege()`
- `diagnose_connection()` verifica `pg_stat_statements` habilitado
- Dependência opcional: `psycopg[binary]` no `pyproject.toml`

---

### T11: queries/postgresql.py

| Campo | Valor |
|---|---|
| **Descrição** | Implementar `PostgreSQLQueryBuilder` com 30+ queries usando `pg_catalog` e `information_schema` |
| **Depende de** | T3, T10 |
| **Bloqueia** | T12, T14 |
| **Esforço** | 8 dias |

**Queries necessárias (equivalências):**

| Oracle | PostgreSQL |
|---|---|
| `ALL_TABLES` → table_stats | `pg_stat_user_tables` + `pg_class` |
| `ALL_TAB_COLUMNS` → column_stats | `information_schema.columns` + `pg_stats` |
| `ALL_INDEXES` → indexes | `pg_indexes` + `pg_stat_user_indexes` |
| `ALL_CONSTRAINTS` → constraints | `information_schema.table_constraints` + `key_column_usage` |
| `DBMS_METADATA.GET_DDL` → table_ddl | `pg_dump --schema-only` ou montagem via catálogo |
| `DBMS_XPLAN.DISPLAY` → explain_plan | `EXPLAIN (FORMAT JSON)` |
| `DBMS_XPLAN.DISPLAY_CURSOR` → runtime_plan | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` |
| `V$SQL` → runtime_stats | `pg_stat_statements` |
| `V$SESSION_EVENT` → wait_events | `pg_stat_activity` + `pg_stat_user_tables` (I/O) |
| `V$PARAMETER` → optimizer_params | `pg_settings` (filtrado) |
| `ALL_TAB_HISTOGRAMS` → histograms | `pg_stats` (most_common_vals, histogram_bounds) |
| `ALL_TAB_PARTITIONS` → partitions | `pg_partitioned_table` + `pg_inherits` |

---

### T12: Coleta de metadata PostgreSQL

| Campo | Valor |
|---|---|
| **Descrição** | Integrar `PostgreSQLQueryBuilder` no collector via adapter; adaptar fluxo de coleta |
| **Depende de** | T4, T9, T11 |
| **Bloqueia** | T14 |
| **Esforço** | 4 dias |

**Diferenças do fluxo Oracle:**
- Sem LOBs — resultados são strings diretas
- DDL pode não estar disponível como texto único (montar via catálogo)
- `EXPLAIN ANALYZE` executa a query (equivalente a `--execute`) — alertar usuário
- Sem `ALTER SESSION SET STATISTICS_LEVEL` — PostgreSQL coleta stats automaticamente com `ANALYZE`

---

### T13: PostgreSQLPlanParser

| Campo | Valor |
|---|---|
| **Descrição** | Implementar `PlanParser` que converte output JSON do `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` em `list[PlanBlock]` |
| **Depende de** | T1, T5 |
| **Bloqueia** | T15 |
| **Esforço** | 5 dias |

**Mapeamento de campos:**

| PlanBlock | PostgreSQL JSON |
|---|---|
| `operation` | `"Node Type"` |
| `name` | `"Relation Name"` ou `"Index Name"` |
| `e_rows` | `"Plan Rows"` |
| `a_rows` | `"Actual Rows"` |
| `a_time_ms` | `"Actual Total Time"` (converter de ms) |
| `buffers` | `"Shared Hit Blocks"` + `"Shared Read Blocks"` |
| `reads` | `"Shared Read Blocks"` |
| `starts` | `"Actual Loops"` |
| `indent` | profundidade na árvore JSON (recursivo) |

---

### T14: Testes de integração PostgreSQL

| Campo | Valor |
|---|---|
| **Descrição** | Suite de testes com PostgreSQL 15+ em Docker, similar aos testes Oracle existentes |
| **Depende de** | T10, T11, T12, T15, T16 |
| **Bloqueia** | T24 |
| **Esforço** | 4 dias |

**Escopo:**
- `docker-compose.yml` com PostgreSQL 15 + `pg_stat_statements` habilitado
- Schema de teste com tabelas, índices, constraints, partições
- Testes: analyze (estimado), analyze --execute (real), inspect, parse
- CI condicional (como Oracle: `if: github.event.inputs.run_pg_tests`)

---

### T15: Validar regras R1-R12 com planos PostgreSQL

| Campo | Valor |
|---|---|
| **Descrição** | Verificar que as regras de compressão produzem resultados corretos com PlanBlocks gerados pelo PostgreSQLPlanParser |
| **Depende de** | T5, T13 |
| **Bloqueia** | T14 |
| **Esforço** | 3 dias |

**Riscos:**
- PostgreSQL não tem conceito de `UNION-ALL` como operação no plano (usa `Append`) — R7 precisa mapear
- `Nested Loop` no PG tem formato diferente — R8 pode precisar ajuste nos nomes de operação
- `VIEW` como operação não existe no PG (usa `Subquery Scan`) — R3 precisa mapear

---

### T16: inspect PostgreSQL (pg_stat_statements)

| Campo | Valor |
|---|---|
| **Descrição** | Implementar equivalente do `inspect` Oracle para PostgreSQL usando `pg_stat_statements.query_id` |
| **Depende de** | T6, T10 |
| **Bloqueia** | T14 |
| **Esforço** | 3 dias |

**Diferenças do Oracle:**
- Oracle `sql_id` → PG `queryid` (bigint, não string)
- Oracle `V$SQL` tem plano em cache → PG não guarda plano, precisa re-executar `EXPLAIN` do `query` salvo
- Runtime stats: `calls`, `total_exec_time`, `rows`, `shared_blks_hit`, `shared_blks_read`
- Requer extensão `pg_stat_statements` habilitada — `doctor` deve verificar

---

## Fase 3 — MariaDB

> **Depende de:** Fase 1 completa (T1-T9).
> Pode rodar em paralelo com Fase 2.

### T17: MariaDBAdapter

| Campo | Valor |
|---|---|
| **Descrição** | Implementar `DatabaseAdapter` para MariaDB usando `mysql-connector-python` ou `PyMySQL` |
| **Depende de** | T1, T2 |
| **Bloqueia** | T18, T19, T21, T23 |
| **Esforço** | 3 dias |

**Escopo:**
- Conexão via `mysql.connector.connect()` com suporte a SSL
- `test_connection()` via `SELECT VERSION()`
- `validate_privileges()` via `SHOW GRANTS`
- `diagnose_connection()` verifica `performance_schema` habilitado
- Dependência opcional: `mysql-connector-python` ou `PyMySQL`

---

### T18: queries/mariadb.py

| Campo | Valor |
|---|---|
| **Descrição** | Implementar `MariaDBQueryBuilder` com 30+ queries usando `information_schema` e `performance_schema` |
| **Depende de** | T3, T17 |
| **Bloqueia** | T19, T21 |
| **Esforço** | 7 dias |

**Queries necessárias (equivalências):**

| Oracle | MariaDB |
|---|---|
| `ALL_TABLES` → table_stats | `information_schema.TABLES` |
| `ALL_TAB_COLUMNS` → column_stats | `information_schema.COLUMNS` + `information_schema.STATISTICS` |
| `ALL_INDEXES` → indexes | `information_schema.STATISTICS` |
| `ALL_CONSTRAINTS` → constraints | `information_schema.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` |
| `DBMS_METADATA.GET_DDL` → table_ddl | `SHOW CREATE TABLE` |
| `DBMS_XPLAN.DISPLAY` → explain_plan | `EXPLAIN FORMAT=JSON` |
| `DBMS_XPLAN.DISPLAY_CURSOR` → runtime_plan | `ANALYZE FORMAT=JSON` (MariaDB 10.1+) |
| `V$SQL` → runtime_stats | `performance_schema.events_statements_summary_by_digest` |
| `V$PARAMETER` → optimizer_params | `SHOW VARIABLES` (filtrado) |
| `ALL_TAB_HISTOGRAMS` → histograms | `information_schema.COLUMN_STATISTICS` (MariaDB 10.8+) ou `mysql.column_stats` |
| `ALL_TAB_PARTITIONS` → partitions | `information_schema.PARTITIONS` |

---

### T19: Coleta de metadata MariaDB

| Campo | Valor |
|---|---|
| **Descrição** | Integrar `MariaDBQueryBuilder` no collector via adapter |
| **Depende de** | T4, T9, T18 |
| **Bloqueia** | T21 |
| **Esforço** | 3 dias |

**Diferenças do fluxo Oracle:**
- DDL disponível diretamente via `SHOW CREATE TABLE` (mais simples que Oracle)
- Sem LOBs
- `ANALYZE` no MariaDB atualiza stats da tabela (não é idempotente!) — usar `EXPLAIN ANALYZE` com cuidado
- Storage engine (InnoDB vs MyISAM) afeta quais stats estão disponíveis

---

### T20: MariaDBPlanParser

| Campo | Valor |
|---|---|
| **Descrição** | Implementar `PlanParser` que converte output JSON do `EXPLAIN FORMAT=JSON` / `ANALYZE FORMAT=JSON` em `list[PlanBlock]` |
| **Depende de** | T1, T5 |
| **Bloqueia** | T22 |
| **Esforço** | 5 dias |

**Mapeamento de campos:**

| PlanBlock | MariaDB JSON |
|---|---|
| `operation` | `"table"."access_type"` (ALL, ref, range, index, etc.) |
| `name` | `"table"."table_name"` |
| `e_rows` | `"table"."rows_examined_per_scan"` |
| `a_rows` | `"table"."r_rows"` (ANALYZE) |
| `a_time_ms` | `"table"."r_total_time_ms"` (ANALYZE) |
| `buffers` | não disponível diretamente — estimar via `Handler_read_*` |
| `reads` | não disponível diretamente |
| `starts` | `"table"."r_loops"` (ANALYZE) |

**Nota:** MariaDB tem menos métricas de I/O no plano que Oracle/PG. Campo `buffers` e `reads` podem ficar como `None`.

---

### T21: Testes de integração MariaDB

| Campo | Valor |
|---|---|
| **Descrição** | Suite de testes com MariaDB 10.6+ em Docker |
| **Depende de** | T17, T18, T19, T22, T23 |
| **Bloqueia** | T24 |
| **Esforço** | 3 dias |

**Escopo:**
- `docker-compose.yml` com MariaDB 10.6 + `performance_schema` habilitado
- Schema de teste com tabelas InnoDB, índices, constraints, partições
- Testes: analyze (estimado), analyze --execute (real), inspect, parse
- CI condicional

---

### T22: Validar regras R1-R12 com planos MariaDB

| Campo | Valor |
|---|---|
| **Descrição** | Verificar regras de compressão com PlanBlocks do MariaDB |
| **Depende de** | T5, T20 |
| **Bloqueia** | T21 |
| **Esforço** | 3 dias |

**Riscos:**
- MariaDB não reporta `buffers`/`reads` no plano — thresholds R5 baseados nesses campos ficam inativos
- Planos MariaDB são mais rasos (menos níveis de profundidade) — R1, R2, R7 podem ter menos efeito
- `access_type` (ALL, ref, range) é diferente das operações Oracle (TABLE ACCESS FULL, INDEX RANGE SCAN) — mapeamento necessário

---

### T23: inspect MariaDB (performance_schema → digest)

| Campo | Valor |
|---|---|
| **Descrição** | Implementar equivalente do `inspect` para MariaDB usando `DIGEST` do `performance_schema` |
| **Depende de** | T6, T17 |
| **Bloqueia** | T21 |
| **Esforço** | 3 dias |

**Diferenças do Oracle:**
- Oracle `sql_id` → MariaDB `DIGEST` (hash hex de 64 chars)
- Stats em `events_statements_summary_by_digest`: `COUNT_STAR`, `SUM_TIMER_WAIT`, `SUM_ROWS_SENT`, `SUM_ROWS_EXAMINED`
- Texto original em `events_statements_history` (limitado, pode ter sido truncado)
- Requer `performance_schema = ON` — `doctor` deve verificar

---

## Fase 4 — Finalização

> **Depende de:** Fase 2 E Fase 3 completas.

### T24: CI multi-database

| Campo | Valor |
|---|---|
| **Descrição** | GitHub Actions com matrix strategy para rodar testes nos 3 bancos |
| **Depende de** | T14, T21 |
| **Bloqueia** | T25 |
| **Esforço** | 2 dias |

**Escopo:**
```yaml
strategy:
  matrix:
    database: [oracle, postgresql, mariadb]
```
- Services Oracle, PG e MariaDB via Docker
- Cobertura ≥ 90% mantida
- Testes de integração condicionais por input

---

### T25: Documentação

| Campo | Valor |
|---|---|
| **Descrição** | Atualizar toda documentação para refletir suporte multi-DB |
| **Depende de** | T24 |
| **Bloqueia** | T26 |
| **Esforço** | 3 dias |

**Arquivos a atualizar (sync-checklist):**
- `README.md` — exemplos com os 3 bancos, tabela de features por DB
- `CLAUDE.md` — tech stack, comandos, convenções
- `.claude/rules/architecture.md` — adapter pattern, novos módulos
- `.claude/agents/sqlmentor.md` — suporte multi-DB na metodologia
- `powers/sqlmentor/POWER.md` — documentação Kiro Power
- `powers/sqlmentor/steering/analysis.md` — metodologia multi-DB
- `scripts/oracle_create_user.sql` → adicionar scripts equivalentes PG/MariaDB

---

### T26: Release multi-DB

| Campo | Valor |
|---|---|
| **Descrição** | Release com suporte a PostgreSQL e MariaDB |
| **Depende de** | T25 |
| **Bloqueia** | — (tarefa final) |
| **Esforço** | 1 dia |

**Escopo:**
- Bump de versão (minor ou major, a decidir)
- Changelog com breaking changes (se houver)
- Dependências opcionais no `pyproject.toml`: `sqlmentor[postgresql]`, `sqlmentor[mariadb]`
- Publicação PyPI

---

## Matriz de Dependências

| Tarefa | Depende de | Bloqueia |
|---|---|---|
| **T1** | — | T2, T3, T5, T8, T9, T10, T13, T17, T20 |
| **T2** | T1, T8 | T3, T4, T10, T17 |
| **T3** | T1, T2 | T4, T11, T18 |
| **T4** | T2, T3 | T5, T6, T7, T12, T19 |
| **T5** | T1, T4 | T7, T13, T15, T20, T22 |
| **T6** | T4 | T7, T16, T23 |
| **T7** | T4, T5, T6 | Fase 2, Fase 3 |
| **T8** | T1 | T2 |
| **T9** | T1 | T12, T19 |
| **T10** | T1, T2 | T11, T12, T14, T16 |
| **T11** | T3, T10 | T12, T14 |
| **T12** | T4, T9, T11 | T14 |
| **T13** | T1, T5 | T15 |
| **T14** | T10, T11, T12, T15, T16 | T24 |
| **T15** | T5, T13 | T14 |
| **T16** | T6, T10 | T14 |
| **T17** | T1, T2 | T18, T19, T21, T23 |
| **T18** | T3, T17 | T19, T21 |
| **T19** | T4, T9, T18 | T21 |
| **T20** | T1, T5 | T22 |
| **T21** | T17, T18, T19, T22, T23 | T24 |
| **T22** | T5, T20 | T21 |
| **T23** | T6, T17 | T21 |
| **T24** | T14, T21 | T25 |
| **T25** | T24 | T26 |
| **T26** | T25 | — |

## Caminho Crítico

O caminho mais longo (que define a duração mínima do projeto):

```
T1 → T8 → T2 → T3 → T4 → T5 → T13 → T15 → T14 → T24 → T25 → T26
                              ↓
                              T6 → T16 ──────────↗
```

**~26 dias no caminho crítico** (sem paralelismo), mas com 2+ desenvolvedores, Fase 2 e 3 podem correr em paralelo após T7, reduzindo para ~18-20 semanas no total.

## Notas sobre Paralelismo

- **T9** (parser dialeto) pode rodar em paralelo com T2-T6
- **T13** (PG PlanParser) pode começar assim que T5 terminar, em paralelo com T12
- **T20** (MariaDB PlanParser) idem, em paralelo com T19
- **Fase 2 inteira** pode rodar em paralelo com **Fase 3** se houver devs separados
- Dentro de cada fase, adapter (T10/T17) e plan parser (T13/T20) são independentes entre si
