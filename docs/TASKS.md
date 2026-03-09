# Fase 3 — MariaDB

> Adicionar suporte a MariaDB ao SqlMentor usando o adapter pattern da Fase 1.
> **Depende de:** Fase 1 completa (T1-T9) ✅

## Status: TODO (0/7)

## Ordem de Execução

```
T17 ──┬→ T18 ──→ T19 ──→ T21
      └→ T23 ──────────────↗
T20 ──→ T22 ──────────────↗
```

T20 (MariaDBPlanParser) pode rodar em paralelo com T17-T19.

## Tarefas

### T17: MariaDBAdapter
- **Status**: [ ] TODO
- **Depende de**: T1 ✅, T2 ✅
- **Bloqueia**: T18, T19, T21, T23
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] `src/sqlmentor/adapters/mariadb.py` — `MariaDBAdapter` implementando `DatabaseAdapter`
  - [ ] Conexão via `mysql.connector.connect()` com suporte a SSL
  - [ ] `test_connection()` via `SELECT VERSION()`
  - [ ] `validate_privileges()` via `SHOW GRANTS`
  - [ ] `diagnose_connection()` verifica `performance_schema` habilitado
  - [ ] Dependência opcional: `mysql-connector-python` ou `PyMySQL` no `pyproject.toml`

---

### T18: queries/mariadb.py
- **Status**: [ ] TODO
- **Depende de**: T3 ✅, T17
- **Bloqueia**: T19, T21
- **Esforço**: 7 dias
- **Entregas**:
  - [ ] `src/sqlmentor/queries/mariadb.py` — `MariaDBQueryBuilder` implementando `QueryBuilder`
  - [ ] 30+ queries usando `information_schema` e `performance_schema`
  - [ ] Assinaturas mantêm `tuple[str, dict]`

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
- **Status**: [ ] TODO
- **Depende de**: T4 ✅, T9 ✅, T18
- **Bloqueia**: T21
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] Integrar `MariaDBQueryBuilder` no collector via adapter
  - [ ] DDL via `SHOW CREATE TABLE` (mais simples que Oracle)
  - [ ] Sem LOBs — resultados são strings diretas
  - [ ] `EXPLAIN ANALYZE` com alerta ao usuário (pode afetar stats da tabela)
  - [ ] Tratar diferenças de storage engine (InnoDB vs MyISAM)

---

### T20: MariaDBPlanParser
- **Status**: [ ] TODO
- **Depende de**: T1 ✅, T5 ✅
- **Bloqueia**: T22
- **Esforço**: 5 dias
- **Entregas**:
  - [ ] `MariaDBPlanParser` implementando `PlanParser` em `adapters/mariadb.py`
  - [ ] Converter output JSON de `EXPLAIN FORMAT=JSON` / `ANALYZE FORMAT=JSON` em `list[PlanBlock]`
  - [ ] `buffers` e `reads` como `None` (MariaDB não reporta I/O no plano)

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
- **Status**: [ ] TODO
- **Depende de**: T5 ✅, T20
- **Bloqueia**: T21
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] Verificar regras de compressão com PlanBlocks do MariaDB
  - [ ] Mapear `access_type` (ALL, ref, range) → operações equivalentes Oracle
  - [ ] R5 thresholds de `buffers`/`reads` ficam inativos (campos `None`)
  - [ ] Testar R1, R2, R7 com planos mais rasos do MariaDB

**Riscos:**
- MariaDB não reporta `buffers`/`reads` no plano — thresholds R5 baseados nesses campos ficam inativos
- Planos MariaDB são mais rasos (menos níveis de profundidade) — R1, R2, R7 podem ter menos efeito
- `access_type` (ALL, ref, range) é diferente das operações Oracle (TABLE ACCESS FULL, INDEX RANGE SCAN) — mapeamento necessário

---

### T23: inspect MariaDB (performance_schema → digest)
- **Status**: [ ] TODO
- **Depende de**: T6 ✅, T17
- **Bloqueia**: T21
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] Implementar inspect para MariaDB usando `DIGEST` do `performance_schema`
  - [ ] Stats: `COUNT_STAR`, `SUM_TIMER_WAIT`, `SUM_ROWS_SENT`, `SUM_ROWS_EXAMINED`
  - [ ] Texto original em `events_statements_history` (pode ter truncamento)
  - [ ] `doctor` verifica `performance_schema = ON`

**Diferenças do Oracle:**
- Oracle `sql_id` → MariaDB `DIGEST` (hash hex de 64 chars)
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
| T17 MariaDBAdapter | ⬜ TODO | T1 ✅, T2 ✅ |
| T18 queries/mariadb.py | ⬜ TODO | T3 ✅, T17 |
| T19 Coleta metadata | ⬜ TODO | T4 ✅, T9 ✅, T18 |
| T20 MariaDBPlanParser | ⬜ TODO | T1 ✅, T5 ✅ |
| T22 Validar R1-R12 | ⬜ TODO | T5 ✅, T20 |
| T23 inspect MariaDB | ⬜ TODO | T6 ✅, T17 |
| T21 Integração MariaDB | ⬜ TODO | T17-T23 |

**Progresso**: 0/7 tarefas concluídas
