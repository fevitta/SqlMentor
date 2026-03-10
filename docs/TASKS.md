# Fase 3 — MariaDB

> Adicionar suporte a MariaDB ao SqlMentor usando o adapter pattern da Fase 1.
> **Depende de:** Fase 1 completa (T1-T9) ✅

## Status: IN PROGRESS (6/7)

## Ordem de Execução

```
T17 ──┬→ T18 ──→ T19 ──→ T21
      └→ T23 ──────────────↗
T20 ──→ T22 ──────────────↗
```

T20 (MariaDBPlanParser) pode rodar em paralelo com T17-T19.

## Tarefas

### T17: MariaDBAdapter
- **Status**: [x] DONE
- **Depende de**: T1 ✅, T2 ✅
- **Bloqueia**: T18, T19, T21, T23
- **Esforço**: 3 dias
- **Entregas**:
  - [x] `src/sqlmentor/adapters/mariadb.py` — `MariaDBAdapter` implementando `DatabaseAdapter`
  - [x] Conexão via `pymysql.connect()` (PyMySQL driver)
  - [x] `test_connection()` via `SELECT 1`
  - [x] `validate_privileges()` via `information_schema.USER_PRIVILEGES`
  - [x] `diagnose_connection()` verifica `performance_schema` habilitado
  - [x] Dependência opcional: `PyMySQL` no `pyproject.toml` (`sqlmentor[mariadb]`)

---

### T18: queries/mariadb.py
- **Status**: [x] DONE
- **Depende de**: T3 ✅, T17 ✅
- **Bloqueia**: T19, T21
- **Esforço**: 7 dias
- **Entregas**:
  - [x] `MariaDBQueryBuilder` em `adapters/mariadb.py` (queries inline, não arquivo separado)
  - [x] 18 query methods + `build_tuple_in_clause()` + `_sanitize_identifier()`
  - [x] Queries contra `information_schema` e `performance_schema`
  - [x] Assinaturas mantêm `tuple[str, dict]` com pyformat `%(name)s`
  - [x] 90 testes (62 novos) incluindo parametrized no-Oracle-binds check

**Equivalências Oracle → MariaDB:**

| Oracle | MariaDB |
|---|---|
| `ALL_TABLES` → table_stats | `information_schema.TABLES` |
| `ALL_TAB_COLUMNS` → column_stats | `information_schema.COLUMNS` + `STATISTICS` |
| `ALL_INDEXES` → indexes | `information_schema.STATISTICS` |
| `ALL_CONSTRAINTS` → constraints | `information_schema.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` |
| `DBMS_METADATA.GET_DDL` → table_ddl | `SHOW CREATE TABLE` |
| `DBMS_XPLAN.DISPLAY` → explain_plan | `EXPLAIN FORMAT=JSON` |
| `DBMS_XPLAN.DISPLAY_CURSOR` → runtime_plan | `ANALYZE FORMAT=JSON` (MariaDB 10.1+) |
| `V$SQL` → runtime_stats | `performance_schema.events_statements_summary_by_digest` |
| `V$PARAMETER` → optimizer_params | `SHOW VARIABLES` (filtrado) |
| `ALL_TAB_HISTOGRAMS` → histograms | `information_schema.COLUMN_STATISTICS` (10.8+) ou `mysql.column_stats` |
| `ALL_TAB_PARTITIONS` → partitions | `information_schema.PARTITIONS` |

---

### T19: Coleta de metadata MariaDB
- **Status**: [x] DONE
- **Depende de**: T4 ✅, T9 ✅, T18 ✅
- **Bloqueia**: T21
- **Esforço**: 3 dias
- **Entregas**:
  - [x] `db_type` em `CollectedContext` — propaga tipo do adapter para report
  - [x] `_collect_explain_plan` generalizado: 1-step (MariaDB) vs 3-step (Oracle)
  - [x] `_collect_runtime_execution` branch MariaDB: ANALYZE FORMAT=JSON + warning
  - [x] DDL column remap em `execute_query`: `Create Table` → `ddl`
  - [x] `validate_privileges` adapter-aware (backward compat sem adapter)
  - [x] `_parse_view_tables` com dialect parametrizável (mysql vs oracle)
  - [x] `report.py`: `db_type` em `_detect_plan_blocks`, `_is_estimated_plan`, `_compress_plan`, `_extract_plan_index_names`
  - [x] `explain_plan` → `EXPLAIN FORMAT=JSON`, `runtime_plan` → performance_schema
  - [x] Timeout MariaDB: `"timed out"` pattern
  - [x] Testes: `test_collector_mariadb.py` (9 testes) + adapter stubs atualizados

---

### T20: MariaDBPlanParser
- **Status**: [x] DONE
- **Depende de**: T1 ✅, T5 ✅
- **Bloqueia**: T22
- **Esforço**: 5 dias
- **Entregas**:
  - [x] `MariaDBPlanParser` implementando `PlanParser` em `adapters/mariadb.py`
  - [x] Converter output JSON de `EXPLAIN FORMAT=JSON` / `ANALYZE FORMAT=JSON` em `list[PlanBlock]`
  - [x] `buffers` e `reads` como `int | None` em `PlanBlock` (8 None-guards em `report.py`)
  - [x] DFS traversal: table, nested_loop, ordering_operation, grouping_operation, duplicates_removal, union_result, subqueries
  - [x] `is_runtime_plan()` — detecção recursiva de `r_rows`
  - [x] 4 fixtures JSON + 34 novos testes (125 total no arquivo)

**Mapeamento de campos:**

| PlanBlock | MariaDB JSON |
|---|---|
| `operation` | `"table"."access_type"` (ALL, ref, range, index, etc.) |
| `name` | `"table"."table_name"` |
| `e_rows` | `"table"."rows_examined_per_scan"` |
| `a_rows` | `"table"."r_rows"` (ANALYZE) |
| `a_time_ms` | `"table"."r_total_time_ms"` (ANALYZE) |
| `buffers` | não disponível — `None` |
| `reads` | não disponível — `None` |
| `starts` | `"table"."r_loops"` (ANALYZE) |

---

### T22: Validar regras R1-R12 com planos MariaDB
- **Status**: [x] DONE
- **Depende de**: T5 ✅, T20 ✅
- **Bloqueia**: T21
- **Esforço**: 3 dias
- **Entregas**:
  - [x] `tests/test_plan_compression_mariadb.py` — 22 testes validando R1-R12 com MariaDB
  - [x] R5 thresholds: `buffers=None`/`reads=None` ignorados; `starts`, `a_time_ms`, cardinality ratio funcionam
  - [x] R1, R2, R3, R7, R8 nunca disparam (padrões Oracle ausentes no MariaDB)
  - [x] R6, R9-R12 funcionam normalmente
  - [x] `_compress_plan` propaga `db_type="mariadb"` corretamente
  - [x] `_extract_plan_index_names` extrai nomes apenas com `access_type=INDEX`

**Achados:**
- MariaDB não reporta `buffers`/`reads` — R5 thresholds desses campos ficam inativos (None-guard)
- Sem SORT AGGREGATE, VIEW, UNION-ALL, NESTED LOOPS → R1, R2, R3, R7, R8 são no-ops
- R9 funciona parcialmente: só `access_type=index` contém "INDEX"; `ref`/`range`/`ALL` não extraem nomes

---

### T23: inspect MariaDB (performance_schema → digest)
- **Status**: [x] DONE
- **Depende de**: T6 ✅, T17 ✅
- **Bloqueia**: T21
- **Esforço**: 3 dias
- **Entregas**:
  - [x] CLI `inspect`: branch MariaDB usa `EXPLAIN FORMAT=JSON` no SQL recuperado (plano estimado)
  - [x] MCP `inspect_sql`: mesma lógica MariaDB
  - [x] CLI `doctor`: lida com diagnose MariaDB (sem `mode` key, verifica `performance_schema`)
  - [x] `MariaDBAdapter.diagnose_connection` retorna `schema` key
  - [x] `tests/test_inspect_mariadb.py` — 8 testes (CLI, MCP, doctor, diagnose)
  - [x] Testes existentes atualizados para novo campo `schema` em diagnose

**Diferenças do Oracle:**
- Oracle `sql_id` → MariaDB `DIGEST` (hash hex de 64 chars)
- MariaDB não armazena planos históricos — inspect usa EXPLAIN FORMAT=JSON (estimado)
- Plano vai em `ctx.execution_plan` (não `ctx.runtime_plan`)
- Requer `performance_schema = ON`

---

### T21: Testes de integração MariaDB
- **Status**: [ ] TODO
- **Depende de**: T17, T18, T19, T22, T23
- **Bloqueia**: T24 (CI multi-database)
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] `docker-compose.yml` com MariaDB 10.6 + `performance_schema` habilitado
  - [ ] Schema de teste com tabelas InnoDB, índices, constraints, partições
  - [ ] Testes: analyze (estimado), analyze --execute (real), inspect, parse
  - [ ] CI condicional (`if: github.event.inputs.run_mariadb_tests`)

---

## Resumo de Progresso

| Tarefa | Status | Depende de |
|--------|--------|------------|
| T17 MariaDBAdapter | ✅ DONE | T1 ✅, T2 ✅ |
| T18 queries/mariadb.py | ✅ DONE | T3 ✅, T17 ✅ |
| T19 Coleta metadata | ✅ DONE | T4 ✅, T9 ✅, T18 ✅ |
| T20 MariaDBPlanParser | ✅ DONE | T1 ✅, T5 ✅ |
| T22 Validar R1-R12 | ✅ DONE | T5 ✅, T20 ✅ |
| T23 inspect MariaDB | ✅ DONE | T6 ✅, T17 ✅ |
| T21 Integração MariaDB | ⬜ TODO | T17 ✅-T23 ✅ |

**Progresso**: 6/7 tarefas concluídas
