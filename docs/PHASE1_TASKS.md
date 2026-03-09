# Fase 1 — Abstração (Fundação)

> Refatorar o código Oracle existente em adapters plugáveis sem quebrar funcionalidade.

## Status: IN PROGRESS (6/9)

## Ordem de Execução

```
T1 ──→ T8 ──→ T2 ──→ T3 ──→ T4 ──┬→ T5 ──→ T7
T1 ──→ T9 (paralelo com T2-T6)    └→ T6 ──↗
```

## Tarefas

### T1: Definir interfaces base
- **Status**: [x] DONE
- **Depende de**: —
- **Bloqueia**: T2, T3, T5, T8, T9
- **Esforço**: 3 dias
- **Entregas**:
  - [x] `src/sqlmentor/adapters/__init__.py`
  - [x] `src/sqlmentor/adapters/base.py` — ABCs: `DatabaseAdapter`, `QueryBuilder`, `PlanParser`
  - [x] Factory function `get_adapter(type: str) → DatabaseAdapter`
  - [x] Tipagem: `ConnectionConfig` TypedDict ou dataclass

### T8: Estender schema de conexão
- **Status**: [x] DONE
- **Depende de**: T1
- **Bloqueia**: T2
- **Esforço**: 1 dia
- **Entregas**:
  - [x] Campo `type: oracle|postgresql|mariadb` em `connections.yaml`
  - [x] Default `type: oracle` (backward compat)
  - [x] `config add` solicita tipo de banco
  - [x] Validação no load do profile

### T9: parser.py → config por dialeto
- **Status**: [x] DONE
- **Depende de**: T1
- **Bloqueia**: T12, T19 (Fases 2/3)
- **Esforço**: 2 dias
- **Entregas**:
  - [x] `_ORACLE_SYSTEM_TABLES` → dicionário por dialect
  - [x] `_BUILTIN_FUNCTIONS` → dicionário por dialect
  - [x] `parse_sql()` aceita param `dialect`
  - [x] Bind syntax parametrizada (`:param`, `%(name)s`, `?`)

### T2: Refatorar connector.py → OracleAdapter
- **Status**: [x] DONE
- **Depende de**: T1, T8
- **Bloqueia**: T3, T4
- **Esforço**: 4 dias
- **Entregas**:
  - [x] `src/sqlmentor/adapters/oracle.py` — `OracleAdapter` implementando `DatabaseAdapter`
  - [x] `oracledb` import, thick mode, DSN movidos para adapter
  - [x] `connector.py` instancia adapter via factory
  - [x] `test_connection()` e `diagnose_connection()` delegam ao adapter

### T3: Refatorar queries → queries/oracle.py
- **Status**: [x] DONE
- **Depende de**: T1, T2
- **Bloqueia**: T4
- **Esforço**: 3 dias
- **Entregas**:
  - [x] `queries/__init__.py` → re-export / factory
  - [x] `queries/oracle.py` — `OracleQueryBuilder` implementando `QueryBuilder`
  - [x] Assinaturas mantêm `tuple[str, dict]`

### T4: Refatorar collector.py → usar adapter
- **Status**: [x] DONE
- **Depende de**: T2, T3
- **Bloqueia**: T5, T6, T7
- **Esforço**: 5 dias
- **Entregas**:
  - [x] Remover `import oracledb` do collector
  - [x] LOB `.read()` encapsulado no adapter
  - [x] `ALTER SESSION`, `SYS_CONTEXT`, `EXPLAIN PLAN FOR` delegados ao adapter
  - [x] `_collect_explain_plan()` e `_collect_runtime_execution()` via adapter
  - [x] Batch collection via `adapter.query_builder.batch_*()`

### T5: Refatorar report.py → PlanParser plugável
- **Status**: [ ] TODO
- **Depende de**: T1, T4
- **Bloqueia**: T7
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] Regexes `_PLAN_ROW` / `_PLAN_ROW_ESTIMATED` → `OraclePlanParser`
  - [ ] `_detect_plan_blocks()` delega para parser do adapter
  - [ ] R1-R12 continuam sobre `list[PlanBlock]` (sem mudança)
  - [ ] `to_markdown()` aceita dialect para labels contextuais

### T6: Refatorar CLI/MCP → dialect-aware
- **Status**: [ ] TODO
- **Depende de**: T4
- **Bloqueia**: T7
- **Esforço**: 2 dias
- **Entregas**:
  - [ ] `analyze` e `inspect` instanciam adapter via profile
  - [ ] `inspect` aceita `--statement-id` genérico (alias de sql_id/query_id/digest)
  - [ ] `doctor` verifica dependências do adapter ativo
  - [ ] Help texts parametrizados por dialect

### T7: Testes de regressão Oracle
- **Status**: [ ] TODO
- **Depende de**: T4, T5, T6
- **Bloqueia**: Fase 2, Fase 3
- **Esforço**: 3 dias
- **Entregas**:
  - [ ] `pytest` completo — cobertura ≥ 90% mantida
  - [ ] Testes de integração Oracle (Docker) passando
  - [ ] Fixtures de plano validadas com `OraclePlanParser`

---

## Resumo de Progresso

| Tarefa | Status | Depende de |
|--------|--------|------------|
| T1 Interfaces base | ✅ DONE | — |
| T8 Schema conexão | ✅ DONE | T1 |
| T9 Parser dialeto | ✅ DONE | T1 |
| T2 OracleAdapter | ✅ DONE | T1, T8 |
| T3 queries/oracle.py | ✅ DONE | T1, T2 |
| T4 Collector adapter | ✅ DONE | T2, T3 |
| T5 PlanParser plugável | ⬜ TODO | T1, T4 |
| T6 CLI/MCP dialect | ⬜ TODO | T4 |
| T7 Regressão Oracle | ⬜ TODO | T4, T5, T6 |

**Progresso**: 6/9 tarefas concluídas
