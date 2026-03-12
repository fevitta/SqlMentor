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
**Status:** CONCLUÍDO (`a6b3106` — DFS extraia tabelas de `materialized`; T12 corrige case mismatch)
**Arquivos:** `src/sqlmentor/report.py`, `src/sqlmentor/adapters/mariadb.py`

`_detect_plan_blocks()` agora é chamado corretamente, mas o `MariaDBPlanParser` não desce na estrutura `"materialized"` do JSON — não extrai os table_names das subqueries materializadas. Resultado: PlanBlocks retornados não contêm as tabelas reais da view, comparação falha → tudo "não acessado".

**Evidência:** Zeus execute mostra "Não acessadas: tb_dw_zeus_d0, tb_dw_zeus_historico", mas o plano JSON mostra ambas com `access_type: ref/range` e `r_rows > 0`.

**Fix:** No `MariaDBPlanParser`, tratar a key `"materialized"` (além de `"materialized_from_subquery"`) ao percorrer o JSON DFS.

---

## T3. Base tables da view não coletadas com --expand-views
**Severidade:** Feature faltante
**Status:** CONCLUÍDO (resolvido por T2 DFS fix + T12 case fix — base tables agora coletadas corretamente)
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
**Status:** CONCLUÍDO (early return após R5 thresholds para db_type="mariadb")
**Arquivos:** `src/sqlmentor/report.py:669-672`

MariaDB usa planos JSON — regras de colapso Oracle (pipe-format) não se aplicam. `_compress_plan` agora retorna plan_lines inalterado após aplicar R5 thresholds para MariaDB. R9-R11 continuam operando sobre metadata normalmente.

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
**Status:** CONCLUÍDO (condição alterada para incluir UNKNOWN sem exigir execute)
**Arquivos:** `src/sqlmentor/collector.py:232`

Quando `parsed.sql_type == "UNKNOWN"`, o collector agora tenta coletar plano estimado via EXPLAIN/EXPLAIN FORMAT=JSON mesmo sem `execute=True`. O banco aceita o SQL mesmo quando sqlglot não conseguiu parsear.

---

## T9. Colunas em WHERE inclui valores literais como colunas
**Severidade:** Bug (cosmético)
**Status:** CONCLUÍDO (filtros adicionais no parser)
**Arquivos:** `src/sqlmentor/parser.py:454-460`

Adicionados filtros para: literais com espaço misparsed como coluna (ex: "Fechado em WFM") e nomes de coluna vazios que geravam artifacts "alias.".

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
**Status:** CONCLUÍDO (sql_text_original, setup_consumers, doctor checks, CLI/MCP fallback)
**Arquivos:** `src/sqlmentor/adapters/mariadb.py`, `src/sqlmentor/cli.py`, `src/sqlmentor/mcp_server.py`

Implementado:
- `sql_text_original()`: busca SQL original em `events_statements_history_long`
- `setup_consumers()`: verifica estado dos consumers do performance_schema
- CLI/MCP: tenta `sql_text_original` antes de `sql_text_by_id` (DIGEST_TEXT como fallback)
- Diagnóstico de consumers: se `events_statements_history_long` está OFF, mostra instruções de ativação
- Doctor: verifica `statements_digest` e `events_statements_history_long` consumers

---

## T12. View Expansion compara tabelas com case mismatch
**Severidade:** Bug
**Status:** CONCLUÍDO (lookup case-insensitive no index_table_map)
**Arquivos:** `src/sqlmentor/report.py:1227-1230`

Corrigido lookup no `index_table_map` para tentar tanto a chave uppercased (`clean`) quanto a original (`b.name`), resolvendo o mismatch entre MariaDB (lowercase) e Oracle (uppercase).

---

## T13. CLI summary labels Oracle-centric para MariaDB
**Severidade:** Bug (cosmético)
**Status:** CONCLUÍDO (branching por db_type no resumo CLI)
**Arquivos:** `src/sqlmentor/cli.py:668-677`

Labels agora mostram "Runtime Plan (ANALYZE FORMAT=JSON)" e "Runtime Stats" para MariaDB.

---

## T14. Runtime Stats seção usa termos Oracle (V$SQL) para MariaDB
**Severidade:** Bug
**Status:** CONCLUÍDO (MariaDB mapping com Digest, Rows Sent, Sort Merge Passes; warnings Oracle omitidos)
**Arquivos:** `src/sqlmentor/report.py:2266-2371`

`_format_runtime_stats` agora recebe `db_type`. MariaDB usa mapping com: Digest, Executions, Avg Elapsed, Avg Rows/Exec, Rows Sent, Sort Merge Passes, Sort Rows. Warnings Oracle (hard parses, invalidations, version count, cursor reuse) omitidos para MariaDB. CPU/IO analysis mantido para ambos.

---

## T15. Runtime Stats retorna mesma sessão para todos os SQLs
**Severidade:** Bug
**Status:** CONCLUÍDO (last_analyze_stats filtra ANALYZE wrapper e queries internas)
**Arquivos:** `src/sqlmentor/adapters/mariadb.py`, `src/sqlmentor/collector.py:655-664`

Adicionado `last_analyze_stats()` que busca em `events_statements_history` filtrando `SQL_TEXT NOT LIKE 'ANALYZE%%'` e queries internas. Complementa com stats históricas do `summary_by_digest`.

---

## T16. Table stats labels Oracle-centric para MariaDB
**Severidade:** Cosmético
**Status:** CONCLUÍDO (branching por db_type em _format_table_stats e _format_indexes)
**Arquivos:** `src/sqlmentor/report.py`

- `_format_table_stats`: "Blocks" → "Pages" para MariaDB; omite Sample Size e Parallel Degree
- `_format_indexes`: omite Clustering Factor e BLevel para MariaDB

---

## T17. optimizer_switch sempre mostra warning ⚠️
**Severidade:** Cosmético
**Status:** CONCLUÍDO (default alterado para None — warning não dispara)
**Arquivos:** `src/sqlmentor/report.py:1882`

Default de `optimizer_switch` alterado de `""` para `None`. Quando `default_val is None`, a condição de warning é False — o valor é exibido sem ⚠️.
