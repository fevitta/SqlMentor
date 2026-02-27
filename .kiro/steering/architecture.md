---
inclusion: always
---

# Arquitetura e Fluxos Técnicos

Documentação técnica de referência para garantir continuidade e consistência em novas alterações.

## Fluxo CLI — `sqlmentor analyze`

~~~mermaid
flowchart TD
    A([sqlmentor analyze arquivo.sql]) --> B{arquivo ou sql?}
    B -->|arquivo| C[read_text UTF-8]
    B -->|sql inline| D[usa string direta]
    C --> E{SQL normalizado?}
    D --> E
    E -->|sim| F[denormalize_sql]
    E -->|sim + execute| ERR1[ERRO: normalizado incompativel com execute]
    E -->|nao| G
    F --> G[resolve schema]
    G --> H[parse_sql sqlglot + fallback regex]
    H --> I{tabelas identificadas?}
    I -->|sim| J[connect Oracle]
    I -->|nao| WARN[aviso parcial - continua]
    WARN --> J
    J --> K{execute?}
    K -->|nao| N2
    K -->|sim| L{binds completos?}
    L -->|faltam| M[desabilita execute - mostra comando sugerido]
    L -->|ok| N[collect_context execute=True]
    M --> N2[collect_context execute=False]
    N --> O[to_markdown ou to_json verbosity=compact]
    N2 --> O
    O --> P[salva em reports/]
    P --> Q[_print_summary tabela Rich]
~~~

## Fluxo CLI — `sqlmentor inspect`

~~~mermaid
flowchart TD
    A([sqlmentor inspect sql_id]) --> B[connect Oracle]
    B --> C[sql_text_by_id - V$SQL]
    C --> D{row encontrado?}
    D -->|nao| ERR[ERRO: SQL_ID nao encontrado]
    D -->|sim| E[parse_sql sqlglot + fallback regex]
    E --> F[runtime_plan DBMS_XPLAN ALLSTATS LAST]
    F --> G[sql_runtime_stats V$SQL]
    G --> H[collect_context execute=False]
    H --> I[injeta runtime_plan e runtime_stats no ctx]
    I --> J[to_markdown ou to_json verbosity=compact]
    J --> K[salva em reports/]
~~~

## Fluxo MCP — `analyze_sql` / `inspect_sql`

~~~mermaid
flowchart TD
    A([IDE chama analyze_sql]) --> B[resolve_connection]
    B --> C{SQL normalizado?}
    C -->|sim| D[denormalize_sql]
    C -->|sim + execute| ERR[retorna JSON de erro]
    C -->|nao| E
    D --> E[parse_sql]
    E --> F[connect Oracle]
    F --> G{binds faltantes com execute=True?}
    G -->|nao| H[collect_context]
    G -->|sim| ERR2[retorna JSON de erro com hint]
    H --> I[to_markdown ou to_json verbosity=compact]
    I --> J[retorna string ao IDE]
~~~

## Pipeline de Compressão do Plano (`report.py`)

Ativado quando `verbosity != "full"`. Executado dentro de `to_markdown()` após `_strip_sql_from_plan` e `_prune_dead_operations`.

~~~mermaid
flowchart TD
    A([_compress_plan]) --> B{verbosity == full?}
    B -->|sim| RET[retorna input sem modificacao]
    B -->|nao| C[_detect_plan_blocks - regex _PLAN_ROW]
    C --> D[_apply_thresholds - marca immune=True R5]
    D --> E[_build_predicate_map → pred_map]
    E --> F[_collapse_config_fields R1 - recebe blocks]
    F --> G[_collapse_situation_history R2 - recebe blocks + pred_map]
    G --> G2[_collapse_union_all_branches R7 - recebe blocks]
    G2 --> H[_collapse_view_zero_rows R3 - recebe blocks]
    H --> H2[_collapse_low_cost_nested_loops R8 - recebe blocks]
    H2 --> I{all_collapsed_ids vazio?}
    I -->|sim| J[_add_nonsequential_id_note R6]
    J --> RET2[retorna plan_lines original + predicate_lines original]
    I -->|nao| K[reconstroi plano - substitui blocos colapsados por replacement_lines]
    K --> L[_add_nonsequential_id_note R6]
    L --> M[_collapse_orphan_predicates_by_ids R4 - recebe predicate_lines + all_collapsed_ids]
    M --> M2[_deduplicate_predicates R12 - agrupa predicados identicos]
    M2 --> RET3[retorna plan_comprimido + predicados_comprimidos]
~~~

### Thresholds de Imunidade (R5)

Um bloco com `immune=True` nunca é colapsado por nenhuma regra.

| Critério | Threshold | Razão |
|----------|-----------|-------|
| `reads > 0` | qualquer | acesso a disco — sempre relevante |
| `buffers > 1.000` | 1.000 | custo real de I/O lógico |
| `starts > 100` | 100 | efeito multiplicador |
| `a_time_ms > 100ms` | 100ms | operação lenta |
| `max/min(e_rows, a_rows) > 10x` | 10x | desvio de cardinalidade |

### Regras de Colapso

| Regra | Função | Padrão detectado | Mínimo para colapsar |
|-------|--------|-----------------|----------------------|
| R1 | `_collapse_config_fields` | `SORT AGGREGATE` (starts≤1) com ≥2 filhos INDEX SCAN consecutivos | ≥ 3 grupos consecutivos |
| R2 | `_collapse_situation_history` | `SORT AGGREGATE` (starts≤1) com ≥2 filhos INDEX SCAN consecutivos | ≥ 2 grupos consecutivos |
| R3 | `_collapse_view_zero_rows` | `VIEW` com `a_rows == 0` (qualquer nome) | subárvore inteira sem imune |
| R4 | `_collapse_orphan_predicates_by_ids` | predicados de IDs colapsados por R1/R2/R3 | qualquer ID colapsado |
| R6 | `_add_nonsequential_id_note` | salto > 1 entre IDs consecutivos | qualquer salto |
| R7 | `_collapse_union_all_branches` | `UNION-ALL` com ≥3 branches filhos idênticos | ≥3 branches |
| R8 | `_collapse_low_cost_nested_loops` | `NESTED LOOPS` starts≥100, buf/iter≤3, rows/iter≤1 | subtree sem imune |
| R9 | `_extract_plan_index_names` | Índices não referenciados no plano omitidos | metadados |
| R10 | `_classify_uniform_columns` | Colunas uniformes (>80% distinct, sem histograma, não FK) | metadados |
| R11 | `_strip_ddl_storage` | Remove STORAGE/TABLESPACE/PCTFREE/etc. da DDL | metadados |
| R12 | `_deduplicate_predicates` | Agrupa predicados idênticos diferindo só no ID | ≥2 iguais |

> ⚠️ Regra de ouro: nenhuma regra pode usar nomes de tabelas, índices, views ou qualquer objeto do schema como critério de detecção. Padrões são baseados exclusivamente em indicadores estruturais do plano (operação, cardinalidade, starts, indent).

> R1–R8 e R12 são aplicadas em `_compress_plan()`. R9, R10 e R11 são aplicadas diretamente em `to_markdown()` (operam sobre metadados, não sobre o plano).

## Contrato de Dados — Dataclasses Principais

~~~
CollectedContext (collector.py)
├── parsed_sql: ParsedSQL          ← output de parser.py
├── db_version: str | None
├── execution_plan: list[str] | None  ← EXPLAIN PLAN estimado
├── runtime_plan: list[str] | None    ← ALLSTATS LAST (só com execute=True ou inspect)
├── runtime_stats: dict | None        ← V$SQL métricas
├── wait_events: list[dict]
├── view_expansions: dict[str, list[str]]
├── index_table_map: dict[str, str]
├── tables: list[TableContext]
├── function_ddls: dict[str, str]
├── optimizer_params: dict[str, str]  ← ⚠️ dict, não list[dict]
└── errors: list[str]

TableContext (collector.py)
├── schema: str
├── name: str
├── object_type: str               ← TABLE ou VIEW
├── ddl: str | None
├── stats: dict | None
├── columns: list[dict]
├── indexes: list[dict]
├── constraints: list[dict]
├── histograms: dict[str, list]    ← só com deep=True
└── partitions: list[dict]         ← só com deep=True

PlanBlock (report.py)
├── id: str
├── operation: str
├── name: str
├── starts: int
├── e_rows: int | None
├── a_rows: int
├── a_time_ms: float
├── buffers: int                   ← já convertido de K/M/G para inteiro
├── reads: int
├── indent: int                    ← proxy de profundidade na árvore
├── immune: bool                   ← setado só por _apply_thresholds
└── children: list[PlanBlock]      ← não usado nas regras de colapso, reservado
~~~

## Parâmetro `verbosity`

| Valor | Comportamento | Default? |
|-------|---------------|----------|
| `compact` | Todas as podas R1–R12 ativas. Reduz ~40% em planos com views complexas. | ✅ sim |
| `full` | Sem compressão adicional além de P1/P3 já existentes. Comportamento legado. | não |
| `minimal` | Só hotspots + runtime stats + optimizer params. Sem plano, sem DDL. | não |

Qualquer outro valor levanta `ValueError` em `to_markdown()`.

## Regras de Consistência — Checklist para Novas Alterações

### Ao adicionar/remover/alterar parâmetro em `analyze` ou `inspect`

- [ ] `src/sqlmentor/cli.py` — opção Typer no comando correspondente
- [ ] `src/sqlmentor/mcp_server.py` — parâmetro na tool correspondente
- [ ] `powers/sqlmentor/POWER.md` — documentação do parâmetro
- [ ] `README.md` — tabela de flags e exemplos de uso

Exceções: `config` e `doctor` são só CLI. `list_connections` e `test_connection` são só MCP.

### Ao adicionar nova regra de compressão (Rn)

- [ ] Implementar função `_collapse_*` em `report.py` seguindo a assinatura `(blocks) → list[CollapseResult]`
- [ ] Adicionar chamada em `_compress_plan` na sequência determinística (após R5, antes de R4 e R6)
- [ ] Garantir que nenhum bloco com `immune=True` é incluído em `cr.collapsed_ids`
- [ ] Garantir que `cr.replacement_lines[0]` começa com `[COLAPSADO:`
- [ ] Adicionar testes unitários e atualizar esta documentação
- [ ] ⚠️ PROIBIDO usar nomes de objetos do schema (tabelas, índices, views) como critério de detecção — usar apenas indicadores estruturais do plano

### Formato dos blocos colapsados

Todo colapso é explícito — nenhuma omissão silenciosa. O primeiro elemento de `replacement_lines` sempre começa com `[COLAPSADO:`. Exemplos por regra:

**R1 — scalar subqueries com index lookup repetido (≥3 grupos):**
```
[COLAPSADO: N scalar subqueries — padrão index lookup repetido]
  Resultado: A-Rows=0 em todos  (ou: N com A-Rows>0)
  Custo total: X buffers, Y reads
  ⚠️ Verifique se esses lookups têm custo real nos seus dados.
```

**R2 — scalar subqueries com index lookup repetido (≥2 grupos):**
```
[COLAPSADO: N scalar subqueries — padrão index lookup repetido]
  | Filtro | A-Rows | Buffers |
  |--------|--------|---------|
  | COL=VALOR | 3 | 10 |
  Custo total: X buffers
```

**R3 — VIEW com A-Rows=0 (runtime):**
```
[COLAPSADO: VIEW 'NOME_VIEW' — A-Rows=0, X buffers]
  ⚠️ Esta subárvore pode ter custo significativo com outros dados de entrada.
```

**R4 — predicados órfãos:**
```
(N predicados de blocos colapsados omitidos — ver resumos acima)
```

**R6 — IDs não sequenciais:**
```
ℹ️ IDs não sequenciais são normais — operações internas de views/subqueries
   são numeradas pelo Oracle mas omitidas do DBMS_XPLAN.
```

**R7 — UNION ALL com branches idênticos (≥3):**
```
[COLAPSADO: N branches UNION-ALL idênticos — padrão: OP1 → OP2]
  A-Rows total: X | Buffers total: Y
```

**R8 — NESTED LOOPS de baixo custo (starts≥100, buf/iter≤3):**
```
[COLAPSADO: NESTED LOOPS — N starts, buf/iter=X, rows/iter=Y]
  Custo total: Z buffers
```

**R9 — Índices não referenciados (aplicado em `to_markdown`):**
Omite da seção de índices aqueles que não aparecem na coluna Name do plano de execução.

**R10 — Colunas uniformes (aplicado em `to_markdown`):**
Move colunas com distribuição uniforme (>80% distinct, sem histograma, não FK) para nota resumida.

**R11 — DDL storage (aplicado em `to_markdown`):**
Remove cláusulas STORAGE(...), TABLESPACE, PCTFREE, INITRANS, LOGGING etc. da DDL de views.

**R12 — Predicados duplicados:**
```
3, 7, 12 - access("T"."COL"="S"."COL")
```

### Decisão de design: `verbosity` como nível, não flags booleanos

`verbosity` é um nível (`full` / `compact` / `minimal`), não um conjunto de flags por feature (`--collapse-subqueries`, `--collapse-predicates`, etc.). Motivo: flags individuais criam combinações inválidas e transferem complexidade para o usuário. O nível é uma abstração coerente — o usuário escolhe quanto quer ver, não quais algoritmos ativar.

### Ao adicionar nova query Oracle

- [ ] Implementar em `src/sqlmentor/queries/__init__.py` retornando `tuple[str, dict]`
- [ ] Nunca interpolar input do usuário — usar bind variables (`:param`)
- [ ] Chamar via `cursor.execute(*fn())` no collector ou no comando CLI/MCP

### Ao adicionar novo campo em `CollectedContext` ou `TableContext`

- [ ] Atualizar `collector.py` (coleta)
- [ ] Atualizar `report.py` (formatação no Markdown/JSON)
- [ ] Verificar se `to_json` em `report.py` serializa o novo campo corretamente
