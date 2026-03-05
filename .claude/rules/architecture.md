# Arquitetura — Pipeline de Compressão e Contratos de Dados

## Fluxo Principal (compartilhado CLI e MCP)

1. `parser.py` extrai tabelas e colunas (sqlglot para DML, regex para PL/SQL)
2. `connector.py` abre conexão Oracle via profile salvo
3. `collector.py` coleta metadata de cada tabela (DDL, stats, índices, constraints, explain plan)
4. `report.py` formata tudo em Markdown ou JSON

## Pipeline de Compressão (`report.py`)

Ativado quando `verbosity != "full"`. Ordem determinística:

```
_compress_plan → _detect_plan_blocks → _apply_thresholds (R5)
  → R1 (_collapse_config_fields)
  → R2 (_collapse_situation_history)
  → R7 (_collapse_union_all_branches)
  → R3 (_collapse_view_zero_rows)
  → R8 (_collapse_low_cost_nested_loops)
  → R4 (_collapse_orphan_predicates_by_ids)
  → R6 (_add_nonsequential_id_note)
  → R12 (_deduplicate_predicates)
```

R9, R10 e R11 operam sobre metadados em `to_markdown()`, não sobre o plano.

### Thresholds de Imunidade (R5)

Um bloco com `immune=True` nunca é colapsado.

| Critério | Threshold | Razão |
|----------|-----------|-------|
| `reads > 0` | qualquer | acesso a disco |
| `buffers > 1.000` | 1.000 | custo I/O lógico |
| `starts > 100` | 100 | efeito multiplicador |
| `a_time_ms > 100ms` | 100ms | operação lenta |
| `max/min(e_rows, a_rows) > 10x` | 10x | desvio de cardinalidade |

### Regras de Colapso

| Regra | Função | Padrão | Mínimo |
|-------|--------|--------|--------|
| R1 | `_collapse_config_fields` | SORT AGGREGATE (starts≤1) + ≥2 INDEX SCAN filhos | ≥3 grupos |
| R2 | `_collapse_situation_history` | SORT AGGREGATE (starts≤1) + ≥2 INDEX SCAN filhos | ≥2 grupos |
| R3 | `_collapse_view_zero_rows` | VIEW com a_rows==0 | subárvore sem imune |
| R4 | `_collapse_orphan_predicates_by_ids` | predicados de IDs colapsados | qualquer |
| R6 | `_add_nonsequential_id_note` | salto >1 entre IDs | qualquer |
| R7 | `_collapse_union_all_branches` | UNION-ALL ≥3 branches idênticos | ≥3 |
| R8 | `_collapse_low_cost_nested_loops` | NL starts≥100, buf/iter≤3, rows/iter≤1 | subtree sem imune |
| R9 | filtro de índices | índices não referenciados no SQL/plano | metadados |
| R10 | `_classify_uniform_columns` | colunas uniformes (>80% distinct) | metadados |
| R11 | `_strip_ddl_storage` | STORAGE/TABLESPACE/PCTFREE da DDL | metadados |
| R12 | `_deduplicate_predicates` | predicados idênticos diferindo só no ID | ≥2 |

> Regra de ouro: nenhuma regra usa nomes de objetos do schema como critério — apenas indicadores estruturais.

Todo colapso é explícito: `replacement_lines[0]` sempre começa com `[COLAPSADO:`.

### Parâmetro `verbosity`

| Valor | Comportamento | Default |
|-------|---------------|---------|
| `compact` | Todas as podas R1-R12 ativas | sim |
| `full` | Sem compressão adicional | não |
| `minimal` | Só hotspots + runtime stats + optimizer params | não |

## Contrato de Dados

```
CollectedContext (collector.py)
├── parsed_sql: ParsedSQL
├── db_version: str | None
├── execution_plan: list[str] | None     # EXPLAIN PLAN estimado
├── runtime_plan: list[str] | None       # ALLSTATS LAST (execute=True ou inspect)
├── runtime_stats: dict | None           # V$SQL métricas
├── wait_events: list[dict]
├── view_expansions: dict[str, list[str]]
├── index_table_map: dict[str, str]
├── tables: list[TableContext]
├── function_ddls: dict[str, str]
├── optimizer_params: dict[str, str]     # dict, não list[dict]
└── errors: list[str]

TableContext (collector.py)
├── schema, name, object_type (TABLE|VIEW)
├── ddl: str | None
├── stats, columns, indexes, constraints: list/dict
├── histograms: dict    # só deep=True
└── partitions: list    # só deep=True

PlanBlock (report.py)
├── id, operation, name
├── starts, e_rows, a_rows, a_time_ms
├── buffers, reads      # já convertidos de K/M/G
├── indent              # proxy de profundidade
├── immune: bool        # setado só por _apply_thresholds
└── children: list      # reservado, não usado nas regras
```

## Batch Collection

Para queries com muitas tabelas, `collect_context` usa coleta em duas fases:

1. **Per-table**: detecta object_type, coleta DDL via DBMS_METADATA
2. **Batch**: 4 queries batch (stats, columns, indexes, constraints) distribuídas por SCHEMA.TABLE
3. **Fallback**: se batch falhar (ex: Oracle 11g), usa coleta per-table
4. **Deep mode**: partitions e histograms permanecem per-table após batch
