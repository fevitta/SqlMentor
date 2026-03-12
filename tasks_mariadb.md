# Tasks MariaDB — Melhorias no Relatório

## T1. Optimizer Params vazio no relatório
**Severidade:** Bug
**Status:** CONCLUÍDO (`9bfd19f` — normaliza keys para lowercase)
**Arquivos:** `src/sqlmentor/report.py:1898-1952`

`_format_optimizer_params()` agora recebe `db_type` e seleciona defaults corretos, mas os 13 params coletados não batem com as keys em `_MARIADB_OPTIMIZER_DEFAULTS` — possível mismatch de case ou naming entre o que `INFORMATION_SCHEMA.GLOBAL_VARIABLES` retorna e o que o dict espera.

**Evidência:** Resumo mostra 13 params coletados, seção renderiza vazia em todos os 9 reports de teste.

**Fix:** Debugar o dict coletado vs as keys em `_MARIADB_OPTIMIZER_DEFAULTS` e alinhar.

---

## T2. View Expansion Summary classifica tabelas como "não acessadas"
**Severidade:** Bug
**Status:** PARCIAL (`a6b3106` — DFS agora extrai tabelas de `materialized`, mas comparação com case mismatch persiste → ver T12)
**Arquivos:** `src/sqlmentor/report.py`, `src/sqlmentor/adapters/mariadb.py`

`_detect_plan_blocks()` agora é chamado corretamente, mas o `MariaDBPlanParser` não desce na estrutura `"materialized"` do JSON — não extrai os table_names das subqueries materializadas. Resultado: PlanBlocks retornados não contêm as tabelas reais da view, comparação falha → tudo "não acessado".

**Evidência:** Zeus execute mostra "Não acessadas: tb_dw_zeus_d0, tb_dw_zeus_historico", mas o plano JSON mostra ambas com `access_type: ref/range` e `r_rows > 0`.

**Fix:** No `MariaDBPlanParser`, tratar a key `"materialized"` (além de `"materialized_from_subquery"`) ao percorrer o JSON DFS.

---

## T3. Base tables da view não coletadas com --expand-views
**Severidade:** Feature faltante
**Arquivos:** `src/sqlmentor/collector.py:250, 423-444`

`collect_context()` itera só sobre `parsed.tables`. Quando view é detectada, `_collect_view_expansion()` extrai nomes das tabelas base em `ctx.view_expansions`, mas nunca adiciona ao pipeline de coleta.

**Fix:** Após view expansion, adicionar tabelas base ao pipeline (DDL, stats, indexes, constraints).

---

## T4. Nota Oracle no contexto MariaDB
**Severidade:** Bug (cosmético)
**Status:** CONCLUÍDO (código corrigido em `2ecf8fe`, testes em `10e5526`)

Header e notas no relatório Markdown já fazem branching por `db_type`. Labels CLI permanecem Oracle-centric → ver T13.

---

## T5. Pipeline de compressão R1-R12 inoperante para MariaDB
**Severidade:** Feature faltante
**Arquivos:** `src/sqlmentor/report.py:666-746`, `src/sqlmentor/adapters/mariadb.py:570-631`

O `MariaDBPlanParser` converte JSON → `PlanBlock` corretamente, mas as regras de colapso procuram padrões Oracle (`SORT AGGREGATE`, `VIEW`, `NESTED LOOPS`, `UNION-ALL`). Resultado: R1-R3, R7-R8 nunca disparam; R4, R12 dependem de "Predicate Information" (inexistente); R5 perde 2/5 thresholds (`buffers=None`, `reads=None`).

| Regra | Status MariaDB |
|-------|---------------|
| R1, R2 | Nunca dispara (requer SORT AGGREGATE) |
| R3 | Nunca dispara (requer VIEW operation) |
| R4, R12 | N/A (sem Predicate Information) |
| R5 | Parcial (buffers/reads = None) |
| R6 | Funciona |
| R7 | Nunca dispara (procura UNION-ALL, MariaDB usa UNION RESULT) |
| R8 | Nunca dispara (procura NESTED LOOPS) |
| R9-R11 | Funcionam (operam sobre metadata) |

**Fix:** Criar regras de colapso MariaDB-aware ou abordagem alternativa para JSON estruturado.

---

## T6. Parser não passa dialect para sqlglot em conexões MariaDB
**Severidade:** Bug (crítico)
**Status:** CONCLUÍDO (`96b6a1e` — dialect param no MCP parse_sql)
**Arquivos:** `src/sqlmentor/cli.py:285,524`, `src/sqlmentor/mcp_server.py:109,213`, `src/sqlmentor/parser.py:358`

`parse_sql()` tem `dialect="oracle"` como default. CLI e MCP **nunca passam o dialect** ao chamar `parse_sql()`, mesmo quando `cfg["type"] == "mariadb"`. Resultado: SQL com backticks (sintaxe padrão MySQL/MariaDB) falha no sqlglot com "Invalid expression / Unexpected token".

O mapeamento `_SQLGLOT_DIALECT = {"mariadb": "mysql"}` (parser.py:24-28) já existe, e o `cfg["type"]` está disponível no CLI antes da chamada — basta passar `dialect=cfg.get("type", "oracle")`.

**Evidência:** SQL 3 (`select \`fat\`.\`insert_in\`...`) retorna Tipo=UNKNOWN, zero tabelas, relatório vazio.

**Fix:** Passar `dialect` derivado do tipo de conexão ao chamar `parse_sql()` em cli.py e mcp_server.py.

---

## T7. Schema default usa username em vez de database para MariaDB
**Severidade:** Bug
**Status:** CONCLUÍDO (`bbacbe0` — schema default vazio para MariaDB)
**Arquivos:** `src/sqlmentor/cli.py:276-281,480-485`, `src/sqlmentor/connector.py:132`

Quando `--schema` não é passado, `effective_schema` resolve para `cfg.get("schema", user_fallback)`. Para MariaDB, `connector.py:132` salva `schema` como o username, e o campo `database` (que tem o valor correto, ex: `gso`) é ignorado.

**Cadeia atual:** `CLI flag > cfg.schema > username`
**Cadeia correta p/ MariaDB:** `CLI flag > cfg.database > cfg.schema > username`

**Evidência:** SQL 2 sem `--schema` → tabelas resolvem como `SQLMENTOR.tb_dw_vtal_base_unica` → DDL falha com "Table doesn't exist".

**Fix:** Na resolução de `effective_schema`, para MariaDB usar `cfg.get("database")` antes de `cfg.get("schema")`.

---

## T8. Relatório parcial não coleta plano quando parse falha
**Severidade:** Bug
**Arquivos:** `src/sqlmentor/collector.py:230-239`, `src/sqlmentor/cli.py:296-299`

Quando `parsed.sql_type == "UNKNOWN"` (falha do parser), o collector não entra no bloco `if parsed.sql_type in ("SELECT", ...)` → plano nunca é coletado. O SQL é válido no banco, só o sqlglot não conseguiu parsear.

**Impacto:** Com T6 corrigido, este cenário diminui. Mas para SQLs com sintaxe que sqlglot não suporte (CTEs recursivos, etc.), o relatório fica vazio mesmo com `--execute`.

**Fix:** Quando `sql_type == "UNKNOWN"` e `execute=True`, tentar coletar o plano via ANALYZE/EXPLAIN mesmo assim (o banco aceita o SQL).

---

## T9. Colunas em WHERE inclui valores literais como colunas
**Severidade:** Bug (cosmético)
**Arquivos:** `src/sqlmentor/parser.py`

O relatório do SQL 2 mostra:
```
Colunas em WHERE: , Cancelada, Fechado em WFM, VTAL.data, VTAL.estado, ...
```

`Cancelada` e `Fechado em WFM` são **valores literais** (strings), não colunas. O parser os inclui erroneamente na lista de colunas do WHERE. Também há uma vírgula inicial (string vazia).

**Fix:** Filtrar literais string do resultado de extração de colunas WHERE no parser.

---

## T10. EXPLAIN estimado falha com `%Y` em SQLs MariaDB
**Severidade:** Bug (alto — afeta todo SQL com DATE_FORMAT)
**Status:** CONCLUÍDO (`57d9c9f` — params=None evita mogrify)
**Arquivos:** `src/sqlmentor/adapters/mariadb.py` (método `explain_plan`)

PyMySQL interpreta `%Y`, `%m`, `%d` etc. como format specifiers Python quando `cursor.execute(sql, {})` é chamado — mesmo com dict vazio, o operador `%` é aplicado internamente por `mogrify()`.

**Evidência:** Zeus estimado falha com `unsupported format character 'Y' (0x59) at index 908`. Fat estimado idem no index 2608. VTAL estimado funciona (sem `%Y` no SQL).

**Afeta:** Qualquer SQL MariaDB que use `DATE_FORMAT()`, `STR_TO_DATE()` ou qualquer função com `%` seguido de letra.

**Fix:** Passar `None` em vez de `{}` para params quando não há binds. Com `params=None`, PyMySQL não aplica `%` formatting.

---

## T11. Implement `sqlmentor inspect <digest>` para MariaDB
**Severidade:** Feature
**Status:** ABERTO
**Arquivos:** `src/sqlmentor/cli.py` (cmd inspect), `src/sqlmentor/adapters/mariadb.py`, `src/sqlmentor/collector.py`

Equivalente MariaDB do `inspect <sql_id>` Oracle. Usa `performance_schema` para obter stats históricas e o SQL original pelo DIGEST hash.

### Pré-requisitos no banco

| Requisito | Como verificar | Default RDS | Persistência RDS |
|-----------|---------------|-------------|------------------|
| `performance_schema = ON` | `SHOW VARIABLES LIKE 'performance_schema'` | ON (via Performance Insights) | Parameter Group (static, reboot) |
| `statements_digest = YES` | `SELECT * FROM setup_consumers WHERE NAME = 'statements_digest'` | YES | Automático |
| `events_statements_history_long = YES` | `SELECT * FROM setup_consumers WHERE NAME = 'events_statements_history_long'` | **NO** | **Não persistível via Parameter Group** |

O consumer `events_statements_history_long` é necessário para obter o `SQL_TEXT` original. Sem ele, o inspect não funciona.

**Problema no RDS:** os parâmetros `performance-schema-consumer-*` não existem no Parameter Group do RDS MariaDB. A ativação via `UPDATE setup_consumers` é volátil (perde no reboot/failover). Workaround: criar Event Scheduler que reative os consumers periodicamente:

```sql
-- Ativar event_scheduler no Parameter Group do RDS (dynamic, sem reboot)
-- Depois criar o evento:
CREATE EVENT IF NOT EXISTS enable_perf_schema_history
ON SCHEDULE EVERY 1 HOUR
DO
  UPDATE performance_schema.setup_consumers
  SET ENABLED = 'YES'
  WHERE NAME IN ('events_statements_history', 'events_statements_history_long')
    AND ENABLED = 'NO';
```

### Fonte de dados

| Fonte | Dados |
|-------|-------|
| `events_statements_history_long` | `SQL_TEXT` original (com literais), `DIGEST`, `THREAD_ID`, timing individual |
| `events_statements_summary_by_digest` | stats agregadas: executions, avg/min/max time, rows_examined/sent, first/last_seen |

### Fluxo

```
sqlmentor inspect <digest> --conn mariadb
  1. Validar digest (hex 32 chars)
  2. Buscar SQL_TEXT em events_statements_history_long WHERE DIGEST = ?
     - Se tabela vazia / consumer OFF:
       ABORTAR com mensagem orientando o usuário:
       "⚠ O consumer events_statements_history_long está desabilitado.
        Para usar inspect no MariaDB, ative-o:
          UPDATE performance_schema.setup_consumers
          SET ENABLED = 'YES'
          WHERE NAME = 'events_statements_history_long';
        Nota: esta configuração é volátil no RDS (perde no reboot).
        Para persistir, crie um Event Scheduler. Veja: sqlmentor doctor"
     - Se digest não encontrado no history:
       ABORTAR: "Digest não encontrado no histórico. Re-execute a query e tente novamente."
  3. Usar SQL_TEXT obtido para:
     a. Parse (tabelas, colunas, tipo)
     b. ANALYZE FORMAT=JSON (plano runtime real)
  4. Buscar stats agregadas em summary_by_digest WHERE DIGEST = ?
  5. Coletar metadata das tabelas (DDL, indexes, stats, constraints)
  6. Gerar relatório: SQL original + stats históricas + plano + metadata + wait events
```

### Diferença inspect vs analyze

| | `analyze --execute` | `inspect <digest>` |
|---|-----------|-------------------|
| Input | SQL (arquivo/inline) | Digest hash (32 hex chars) |
| SQL | Fornecido pelo usuário | Obtido de `history_long` |
| Plano | ANALYZE FORMAT=JSON | ANALYZE FORMAT=JSON |
| Stats históricas | Não | Sim (executions, avg_time, rows_examined, first/last_seen) |
| Requer `--sql`? | Sim | Não (obtém do history) |

O inspect é o fluxo "já rodei no DBeaver, agora quero analisar pelo digest".

### Dados do relatório

- **SQL original:** obtido de `history_long.SQL_TEXT`
- **Stats históricas:** COUNT_STAR (executions), AVG/MIN/MAX_TIMER_WAIT, SUM_ROWS_EXAMINED, SUM_ROWS_SENT, SUM_CREATED_TMP_TABLES, SUM_SELECT_FULL_JOIN, SUM_SORT_ROWS, FIRST_SEEN, LAST_SEEN
- **Plano runtime:** ANALYZE FORMAT=JSON
- **Metadata:** DDL, indexes, stats, constraints das tabelas
- **Wait events:** da sessão do ANALYZE

### sqlmentor doctor

Adicionar checagem MariaDB no `doctor`:
- `performance_schema = ON` → OK/FAIL
- `statements_digest = YES` → OK/FAIL
- `events_statements_history_long = YES` → OK/WARN ("inspect MariaDB requer este consumer ativo")
- Se WARN: mostrar comando de ativação + sugestão do Event Scheduler

---

## T12. View Expansion compara tabelas com case mismatch
**Severidade:** Bug
**Status:** ABERTO
**Arquivos:** `src/sqlmentor/report.py` (View Expansion Summary)

T2 corrigiu o DFS do `MariaDBPlanParser` para extrair tabelas de nós `materialized`, mas a comparação no View Expansion Summary usa `.upper()` em `plan_tables` enquanto `view_tables` (de `ctx.view_expansions`) preserva case original. Resultado: todas as tabelas da view aparecem como "Não acessadas" mesmo estando no plano.

**Evidência:** Zeus report — "Não acessadas: tb_dw_zeus_d0, tb_dw_zeus_historico" mas ambas têm `access_type: ALL/range` e `r_rows > 0` no plano JSON.

**Fix:** Normalizar case na comparação: `plan_tables = {b.name.upper() for b in blocks}` e comparar com `view_table.upper()`.

---

## T13. CLI summary labels Oracle-centric para MariaDB
**Severidade:** Bug (cosmético)
**Arquivos:** `src/sqlmentor/cli.py` (resumo da coleta)

O resumo exibido no terminal após `analyze --execute` mostra "Runtime Plan (ALLSTATS LAST)" e "Runtime Stats (V$SQL)" para conexões MariaDB. Deveria usar "Runtime Plan (ANALYZE FORMAT=JSON)" e "Runtime Stats".

**Evidência:** Todas as 3 execuções MariaDB mostram labels Oracle no summary.

**Fix:** Condicional por `db_type` no bloco de resumo da CLI.

---

## T14. Runtime Stats seção usa termos Oracle (V$SQL) para MariaDB
**Severidade:** Bug
**Arquivos:** `src/sqlmentor/report.py` (seção Runtime Stats)

A seção "Runtime Stats" no relatório Markdown usa labels Oracle para MariaDB: "SQL ID" (→ Digest), "Child Number", "Plan Hash Value", "Version Count (children)", "Loads (hard parses)", "Parse Calls" — nenhum desses campos existe no `performance_schema` do MariaDB.

**Evidência:** Todos os 3 reports MariaDB mostram esses labels com valores zerados ou genéricos.

**Fix:** Branching por `db_type` na formatação de runtime stats. MariaDB deve mostrar: Digest, Executions, Avg Elapsed, Rows Examined, Rows Sent, Tmp Tables, Full Joins.

---

## T15. Runtime Stats retorna mesma sessão para todos os SQLs
**Severidade:** Bug
**Arquivos:** `src/sqlmentor/collector.py` ou `src/sqlmentor/adapters/mariadb.py` (coleta de runtime stats)

Os 3 SQLs (faturamento, vtal, zeus) retornam o mesmo `SQL ID: 8010d1ddb07dc9949b5ad1e52878c807` e contagens incrementais (`Executions: 18→20→22`). O collector parece retornar stats da sessão inteira, não da query específica.

**Evidência:** Comparar `SQL ID` e `Executions` entre os 3 reports gerados na mesma sessão.

**Fix:** Investigar a query que coleta runtime stats do MariaDB — provavelmente está buscando por `THREAD_ID` da sessão em vez de filtrar pelo `DIGEST` da query analisada.

---

## T16. Table stats labels Oracle-centric para MariaDB
**Severidade:** Cosmético
**Arquivos:** `src/sqlmentor/report.py` (seção Estatísticas Gerais da tabela)

Labels da seção "Estatísticas Gerais" usam termos Oracle que não se aplicam a MariaDB:
- "Blocks" → deveria ser "Pages" (InnoDB)
- "BLevel: None" → não existe no MariaDB (remover)
- "Sample Size: N (100%)" → MariaDB sempre analisa 100% (remover ou simplificar)
- "Parallel Degree: 1" → não aplicável a MariaDB (remover)
- "Clustering Factor: None" → não disponível em MariaDB (remover da tabela de índices)

**Fix:** Condicional por `db_type` na formatação de stats e índices.

---

## T17. optimizer_switch sempre mostra warning ⚠️
**Severidade:** Cosmético
**Arquivos:** `src/sqlmentor/report.py` (`_MARIADB_OPTIMIZER_DEFAULTS`)

O default de `optimizer_switch` em `_MARIADB_OPTIMIZER_DEFAULTS` é `""` (string vazia). Como o valor real é sempre uma string longa de flags, o warning ⚠️ dispara em 100% dos reports. Isso reduz o signal-to-noise ratio.

**Fix:** Remover `optimizer_switch` dos defaults (exibir sempre sem comparação) ou tratá-lo como caso especial que não mostra warning.
