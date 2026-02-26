---
title: "Plan Compression: Poda Inteligente do Plano de Execução"
version: "1.0"
status: draft
---

# Spec: Plan Compression

## Contexto

O `report.py` já implementa P1 (poda de operações com Starts=0/A-Rows=0) e P3 (strip da lista de colunas do CREATE VIEW). Mesmo assim, relatórios de views complexas como `VW_ENTITY_A_DETAIL` chegam a ~2283 linhas — ainda acima do ideal para consumo por LLM.

A análise em `reports/analise_081123.md` identificou os padrões restantes que podem ser comprimidos sem perda de informação para tuning.

---

## Objetivo

Reduzir o tamanho dos relatórios em ~40% adicionais (de ~2283 para ~1400 linhas) sem distorcer a análise de performance, usando compressão com contexto agregado obrigatório.

**Regra de ouro:** nenhuma poda é silenciosa. Toda omissão gera um bloco de resumo com custo agregado e aviso de que o custo é específico desta execução.

---

## Nível de Verbosidade (`--verbosity`)

Parâmetro único que controla o grau de compressão. Não é um booleano por feature — é um nível que ativa conjuntos de podas.

| Nível | Comportamento |
|-------|---------------|
| `full` | Comportamento atual — nenhuma compressão além de P1/P3 já existentes |
| `compact` | Todas as podas desta spec ativas (default novo) |
| `minimal` | Só hotspots + runtime stats + parâmetros do otimizador (sem plano, sem DDL) |

**Justificativa para não ser booleano:** um flag por feature (`--collapse-subqueries`, `--collapse-predicates`, etc.) cria combinações inválidas e dificulta o uso. O nível de verbosidade é uma abstração coerente — o usuário escolhe o quanto quer ver, não quais algoritmos ativar.

**Propagação:**
- CLI: `sqlmentor analyze <sql> --verbosity compact`
- MCP: parâmetro `verbosity` nas tools `analyze_sql` e `inspect_sql`
- Default: `compact` (breaking change controlado — o comportamento atual vira `full`)

---

## Requisitos

### R1 — Colapsar scalar subqueries de campos configurados (P2a)

**Contexto:** A view `VW_ENTITY_A_DETAIL` tem ~35 scalar subqueries que verificam campos configurados por obra (EMPREENDIMENTO_1, PEP, OT, etc.). Cada uma gera um bloco de 6–8 linhas no plano com padrão idêntico: `SORT AGGREGATE → NESTED LOOPS → IDX_ATTR_ENTITY_ID → PK_ATTR_CONFIG`.

**Critério de detecção:** Sequência de blocos consecutivos onde:
- Operação raiz é `SORT AGGREGATE`
- Todos os filhos usam os mesmos índices (IDX_ATTR_ENTITY_ID + PK_ATTR_CONFIG)
- Starts=1 em todos
- A-Rows=0 ou 1 em todos

**Saída quando colapsado:**
```
[COLAPSADO: N scalar subqueries — campos configurados por obra]
  Índices: IDX_ATTR_ENTITY_ID → PK_ATTR_CONFIG
  Resultado: A-Rows=0 em todos (campos não configurados para esta obra)
  Custo total: X buffers, Y Reads
  ⚠️ Em obras com campos configurados, esses blocos terão custo real.
  Campos verificados: EMPREENDIMENTO_1, EMPREENDIMENTO_2, PEP, PEP_2, ...
```

**Quando NÃO colapsar:** Se qualquer bloco do grupo tiver A-Rows > 1 ou Buffers > threshold (ver R5), manter todos os blocos do grupo expandidos.

**Redução estimada:** ~200 linhas → ~10 linhas.

---

### R2 — Colapsar scalar subqueries de histórico de situação (P2b)

**Contexto:** A view tem 7–8 scalar subqueries `DATA_ALT_SIT_*` com padrão idêntico: `SORT AGGREGATE → NESTED LOOPS → UK_STATUS_TYPE_REF → IDX_STATUS_HIST_ENTITY`. Diferem apenas no valor de `TYPE_REF`.

**Critério de detecção:** Sequência de blocos onde:
- Operação raiz é `SORT AGGREGATE`
- Filhos usam UK_STATUS_TYPE_REF + IDX_STATUS_HIST_ENTITY
- Starts=1 em todos

**Saída quando colapsado:** Tabela resumo preservando A-Rows por tipo (informação relevante — indica quais situações têm histórico):
```
[COLAPSADO: 8 scalar subqueries DATA_ALT_SIT_* — histórico de situações]
  Padrão: UK_STATUS_TYPE_REF → IDX_STATUS_HIST_ENTITY
  | Tipo               | A-Rows | Buffers |
  |--------------------|--------|---------|
  | STATUS_ACTIVE      | 3      | 10      |
  | SITUACAO_OPER      | 0      | 10      |
  | SITUACAO_PROJETO   | 1      | 10      |
  | SITUACAO_ASBUILTS  | 0      | 10      |
  | SITUACAO_AUTARQUIA | 0      | 10      |
  | SITUACAO_MEDICAO   | 3      | 10      |
  | SGM.FCT_URA        | 0      | 10      |
  Custo total: 80 buffers
```

O valor de `TYPE_REF` é extraído da Predicate Information correspondente ao Id do bloco.

**Redução estimada:** ~56 linhas → ~15 linhas.

---

### R3 — Colapsar expansões de VW_CURRENT_USER com A-Rows=0 (P2c)

**Contexto:** A view expande `VW_CURRENT_USER` múltiplas vezes (uma por campo USU_RECNO_*). Cada expansão tem ~75 linhas. As que retornam A-Rows=0 (usuário é NULL) ainda executam a VIEW inteira antes de filtrar.

**Critério de detecção:** Subárvore que:
- Contém um nó `VIEW` com nome `VW_CURRENT_USER` (ou detectado via `from$_subquery$_NNNN`)
- O nó pai da VIEW tem A-Rows=0
- Buffers da subárvore inteira < threshold (ver R5)

**Saída quando colapsado:**
```
[COLAPSADO: VW_CURRENT_USER para <campo> — A-Rows=0, X buffers — usuário NULL nesta execução]
  ⚠️ Quando o usuário não é NULL, esta subárvore consome ~137K buffers.
```

**Quando NÃO colapsar:** Se A-Rows > 0 no nó pai da VIEW, manter expandido — é o gargalo.

**Redução estimada:** ~75 linhas por expansão colapsada.

---

### R4 — Colapsar Predicate Information repetitiva (P7)

**Contexto:** A seção `Predicate Information` tem blocos de predicados estruturalmente idênticos — mesmos access/filter patterns, diferindo apenas no `from$_subquery$_NNNN` e no valor de `TYPE_REF`. São ~280 linhas de predicados quase idênticos para os 7 blocos de situação histórica.

**Critério de detecção:** Grupos de predicados onde:
- Os Ids correspondem a blocos já colapsados por R2 ou R3
- O padrão de access/filter é idêntico (mesmas colunas, mesma estrutura)

**Comportamento:** Quando um bloco do plano é colapsado (R1, R2, R3), os predicados correspondentes também são colapsados. A nota de colapso no plano inclui os Ids omitidos para referência.

**Saída:**
```
(N predicados de blocos colapsados omitidos — ver resumos acima)
```

**Redução estimada:** ~240 linhas → ~5 linhas.

---

### R5 — Thresholds de proteção (nunca colapsar)

Qualquer operação que atenda a qualquer um dos critérios abaixo é **imune a colapso**, independente do nível de verbosidade:

| Critério | Threshold | Justificativa |
|----------|-----------|---------------|
| Buffers | > 1.000 | Operação com custo real de I/O lógico |
| Reads (disco) | > 0 | Acesso a disco — sempre relevante |
| A-Rows vs E-Rows | ratio > 10x | Desvio de cardinalidade — problema de estatísticas |
| Starts | > 100 | Efeito multiplicador — loop implícito |
| A-Time | > 100ms | Operação lenta |

Se um bloco candidato a colapso contém qualquer operação imune, o bloco inteiro é mantido expandido.

---

### R6 — Nota sobre IDs não sequenciais (P11)

**Contexto:** O Oracle numera operações internas de views/subqueries mas não as exibe no DBMS_XPLAN. Isso gera IDs não sequenciais (ex: 12, 13, 14, 15, 16, 17, 20, 21...) que podem confundir a IA.

**Implementação:** Adicionar nota fixa no cabeçalho do plano quando IDs não sequenciais forem detectados:
```
ℹ️ IDs não sequenciais são normais — operações internas de views/subqueries
   são numeradas pelo Oracle mas omitidas do DBMS_XPLAN.
```

**Detecção:** Verificar se há saltos > 1 entre IDs consecutivos no plano.

---

### R7 — Propagação para CLI e MCP (sincronização obrigatória)

Conforme regra de sincronização do `tech.md`, toda mudança de interface deve ser replicada em:

1. `src/sqlmentor/cli.py` — adicionar `--verbosity [full|compact|minimal]` nos comandos `analyze` e `inspect`
2. `src/sqlmentor/mcp_server.py` — adicionar parâmetro `verbosity` nas tools `analyze_sql` e `inspect_sql`
3. `powers/sqlmentor/POWER.md` — documentar o parâmetro `verbosity`
4. `README.md` — atualizar tabela de flags e exemplos

---

## Design de Implementação

### Onde vive a lógica

Toda a lógica de compressão fica em `report.py`. O `collector.py` não muda — ele coleta tudo, o report decide o que mostrar.

### Assinatura de `to_markdown`

```python
def to_markdown(ctx: CollectedContext, verbosity: str = "compact") -> str:
```

O `verbosity` é passado de `cli.py` e `mcp_server.py` para `to_markdown`. Internamente, `to_markdown` passa para as funções de formatação do plano.

### Funções novas em `report.py`

```python
def _compress_plan(
    plan_lines: list[str],
    predicate_lines: list[str],
    verbosity: str,
) -> tuple[list[str], list[str]]:
    """
    Aplica compressão ao plano e predicados conforme nível de verbosidade.
    Retorna (plano_comprimido, predicados_comprimidos).
    
    Orquestra R1–R5 em sequência:
    1. _detect_plan_blocks() — identifica blocos candidatos
    2. _apply_thresholds() — marca blocos imunes (R5)
    3. _collapse_config_fields() — R1
    4. _collapse_situation_history() — R2
    5. _collapse_vw_usuario_null() — R3
    6. _collapse_orphan_predicates() — R4 (usa IDs colapsados dos passos anteriores)
    """

def _detect_plan_blocks(plan_lines: list[str]) -> list[PlanBlock]:
    """
    Parseia o plano em blocos estruturados.
    Cada PlanBlock tem: id, operation, name, starts, e_rows, a_rows, 
    a_time_ms, buffers, reads, children: list[PlanBlock].
    """

def _apply_thresholds(blocks: list[PlanBlock]) -> None:
    """Marca blocks.immune=True para operações que atendem R5."""

def _collapse_config_fields(blocks: list[PlanBlock]) -> list[CollapseResult]:
    """Detecta e colapsa grupos de scalar subqueries de campos configurados (R1)."""

def _collapse_situation_history(
    blocks: list[PlanBlock], predicate_map: dict[str, list[str]]
) -> list[CollapseResult]:
    """Detecta e colapsa scalar subqueries de histórico de situação (R2)."""

def _collapse_vw_usuario_null(blocks: list[PlanBlock]) -> list[CollapseResult]:
    """Detecta e colapsa expansões de VW_CURRENT_USER com A-Rows=0 (R3)."""

@dataclass
class PlanBlock:
    id: str
    operation: str
    name: str
    starts: int
    e_rows: int | None
    a_rows: int
    a_time_ms: float
    buffers: int
    reads: int
    immune: bool = False
    children: list["PlanBlock"] = field(default_factory=list)

@dataclass
class CollapseResult:
    collapsed_ids: set[str]
    replacement_lines: list[str]  # linhas do bloco de resumo
```

### Parsing do plano

O plano é texto fixo do Oracle. O parser precisa extrair os campos numéricos de cada linha. Regex de referência:

```python
PLAN_ROW = re.compile(
    r'\|\*?\s*(\d+)\s*\|'      # Id
    r'\s*(.+?)\s*\|'            # Operation
    r'\s*(.*?)\s*\|'            # Name
    r'\s*(\d+)\s*\|'            # Starts
    r'\s*(\d*)\s*\|'            # E-Rows
    r'\s*(\d+)\s*\|'            # A-Rows
    r'\s*(\S+)\s*\|'            # A-Time
    r'\s*(\d+[KMG]?)\s*\|'     # Buffers
    r'\s*(\d+)\s*\|'            # Reads
)
```

A hierarquia (parent/child) é inferida pela indentação da coluna Operation.

### Extração de TYPE_REF dos predicados

Para R2, o valor de `TYPE_REF` (ex: `'STATUS_ACTIVE'`) é extraído da Predicate Information:

```python
# Linha de predicado: "302 - access("TYPE_REF"='STATUS_ACTIVE')"
TPS_REF_PATTERN = re.compile(r"TYPE_REF\s*=\s*'([^']+)'")
```

---

## Tarefas de Implementação

### Task 1 — Dataclass `PlanBlock` e parser do plano
- Criar `PlanBlock` dataclass em `report.py`
- Implementar `_detect_plan_blocks(plan_lines)` que parseia o plano em árvore de `PlanBlock`
- Implementar `_apply_thresholds(blocks)` com os critérios de R5
- Testes: plano simples (sem views), plano com views, plano com IDs não sequenciais

### Task 2 — Colapso de campos configurados (R1)
- Implementar `_collapse_config_fields(blocks)`
- Critério: grupos de SORT AGGREGATE com mesmos índices filhos, todos Starts=1
- Saída: bloco de resumo com lista de campos e custo agregado
- Testes: grupo com A-Rows=0 em todos (colapsa), grupo com A-Rows>0 em algum (não colapsa), grupo com operação imune (não colapsa)

### Task 3 — Colapso de histórico de situação (R2)
- Implementar `_collapse_situation_history(blocks, predicate_map)`
- Extrai TYPE_REF dos predicados para montar tabela resumo
- Saída: tabela com A-Rows por tipo de situação
- Testes: 7 tipos de situação, mix de A-Rows=0 e A-Rows>0

### Task 4 — Colapso de VW_CURRENT_USER nula (R3)
- Implementar `_collapse_vw_usuario_null(blocks)`
- Detecta subárvores com VIEW + A-Rows=0 no pai
- Saída: nota com aviso de custo potencial
- Testes: expansão com A-Rows=0 (colapsa), expansão com A-Rows=1 (não colapsa)

### Task 5 — Colapso de predicados órfãos (R4)
- Estender `_prune_orphan_predicates` para aceitar IDs colapsados (além de IDs podados)
- Adicionar nota de colapso na seção de predicados
- Testes: predicados de blocos colapsados são removidos, predicados de blocos mantidos ficam

### Task 6 — Nota de IDs não sequenciais (R6)
- Implementar detecção de saltos em IDs consecutivos
- Adicionar nota no cabeçalho do plano quando detectado
- Testes: plano com IDs sequenciais (sem nota), plano com saltos (com nota)

### Task 7 — Orquestrador `_compress_plan` e parâmetro `verbosity`
- Implementar `_compress_plan(plan_lines, predicate_lines, verbosity)`
- Integrar Tasks 1–6 em sequência
- Adicionar `verbosity: str = "compact"` em `to_markdown`
- `full` → pula `_compress_plan`, comportamento atual
- `compact` → executa `_compress_plan` completo
- `minimal` → retorna só hotspots + runtime stats + optimizer params (sem plano, sem DDL)

### Task 8 — Propagação CLI/MCP (R7)
- `cli.py`: adicionar `--verbosity` em `analyze` e `inspect` (Typer option com choices)
- `mcp_server.py`: adicionar `verbosity: str = "compact"` em `analyze_sql` e `inspect_sql`
- `powers/sqlmentor/POWER.md`: documentar parâmetro
- `README.md`: atualizar tabela de flags

---

## Critérios de Aceitação

1. Com `--verbosity compact`, o relatório de `VW_ENTITY_A_DETAIL` deve ter menos de 1.500 linhas (vs ~2.283 atual)
2. Nenhum bloco é colapsado silenciosamente — todo colapso gera bloco de resumo com custo agregado
3. Operações com Buffers > 1.000 nunca são colapsadas
4. Com `--verbosity full`, o output é idêntico ao atual (zero regressão)
5. Com `--verbosity minimal`, o output tem menos de 100 linhas
6. CLI e MCP aceitam o parâmetro `verbosity` com os três valores válidos
7. Valor inválido de `verbosity` gera erro claro com os valores aceitos

---

## Não está no escopo

- Compressão da DDL da view (além do P3 já implementado)
- Detecção automática de anti-patterns (cadeia FUN_SUPERVISOR, SELECT *) — isso é análise, não compressão
- Cache de planos comprimidos
- Suporte a outros bancos além de Oracle
