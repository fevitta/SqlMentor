# Requirements Document

## Introduction

Esta feature adiciona compressão de planos de execução Oracle aos relatórios gerados pelo SqlMentor. Um parâmetro `verbosity` (`full` | `compact` | `minimal`) controla o nível de compressão aplicado pela função `to_markdown()` em `report.py`. Seis regras de compressão (R1–R6) colapsam padrões repetitivos de scalar subqueries e views sem distorcer a análise de performance — toda poda gera um bloco de resumo com custo agregado. O comportamento atual do sistema vira `full`; o novo default é `compact`.

## Glossary

- **Report**: módulo `report.py` responsável por formatar `CollectedContext` em Markdown ou JSON
- **Compressor**: conjunto de funções internas de `report.py` que implementam as regras R1–R6
- **PlanBlock**: dataclass que representa uma operação do plano de execução Oracle com campos numéricos extraídos
- **CollapseResult**: dataclass que representa o resultado de um colapso — IDs removidos e linhas de resumo substitutivas
- **verbosity**: parâmetro de controle de compressão com valores `full`, `compact` e `minimal`
- **immune**: flag de `PlanBlock` que indica que o bloco não pode ser colapsado por nenhuma regra
- **CLI**: interface de linha de comando `sqlmentor` implementada em `cli.py`
- **MCP_Server**: servidor MCP `sqlmentor-mcp` implementado em `mcp_server.py`
- **CollectedContext**: dataclass de `collector.py` que agrega todos os metadados Oracle coletados para um SQL

## Requirements

### Requirement 1: Parâmetro verbosity em to_markdown

**User Story:** Como desenvolvedor usando o SqlMentor, quero controlar o nível de detalhe do relatório gerado, para que eu possa escolher entre análise completa e contexto compacto conforme a necessidade.

#### Acceptance Criteria

1. THE Report SHALL expor `to_markdown(ctx: CollectedContext, verbosity: str = "compact") -> str` como interface pública
2. WHEN `verbosity` é `"full"`, THE Report SHALL produzir output idêntico ao comportamento anterior à feature (sem compressão adicional além de P1/P3 já existentes)
3. WHEN `verbosity` é `"compact"`, THE Report SHALL aplicar as regras de compressão R1–R6 ao plano de execução
4. WHEN `verbosity` é `"minimal"`, THE Report SHALL incluir apenas hotspots, runtime stats e optimizer params, omitindo plano de execução e DDLs
5. IF `verbosity` não é `"full"`, `"compact"` ou `"minimal"`, THEN THE Report SHALL levantar `ValueError` com mensagem listando os valores aceitos
6. THE Report SHALL usar `"compact"` como valor default de `verbosity`

---

### Requirement 2: Parsing de plano em PlanBlock (R0)

**User Story:** Como desenvolvedor mantendo o SqlMentor, quero que o plano de execução Oracle seja parseado em estrutura navegável, para que as regras de compressão possam operar sobre campos numéricos tipados.

#### Acceptance Criteria

1. WHEN `_detect_plan_blocks` recebe uma lista de strings, THE Compressor SHALL retornar uma lista de `PlanBlock` na mesma ordem das linhas do plano
2. WHEN uma linha não casa com o padrão `_PLAN_ROW`, THE Compressor SHALL ignorar a linha sem levantar exceção
3. THE Compressor SHALL garantir que `len(resultado) ≤ len(plan_lines)` para qualquer entrada
4. THE Compressor SHALL garantir que `buffers`, `reads` e `starts` de todo `PlanBlock` retornado são não-negativos
5. WHEN buffers contém sufixo K, M ou G, THE Compressor SHALL converter para inteiro antes de armazenar em `PlanBlock.buffers`
6. THE Pretty_Printer SHALL formatar `PlanBlock` de volta para linha de texto compatível com o formato Oracle
7. FOR ALL `PlanBlock` válidos, parsear então formatar então parsear SHALL produzir um objeto equivalente (propriedade de round-trip)

---

### Requirement 3: Thresholds de imunidade (R5)

**User Story:** Como DBA analisando performance, quero que operações com custo real significativo nunca sejam colapsadas, para que o relatório comprimido não omita informações críticas de tuning.

#### Acceptance Criteria

1. WHEN `block.reads > 0`, THE Compressor SHALL marcar `block.immune = True`
2. WHEN `block.buffers > 1000`, THE Compressor SHALL marcar `block.immune = True`
3. WHEN `block.starts > 100`, THE Compressor SHALL marcar `block.immune = True`
4. WHEN `block.a_time_ms > 100`, THE Compressor SHALL marcar `block.immune = True`
5. WHEN `max(block.e_rows, block.a_rows) / min(block.e_rows, block.a_rows) > 10` e ambos são maiores que zero, THE Compressor SHALL marcar `block.immune = True`
6. WHEN nenhum threshold é atingido, THE Compressor SHALL manter `block.immune = False`
7. THE Compressor SHALL aplicar `_apply_thresholds` antes de qualquer regra de colapso R1–R3

---

### Requirement 4: Colapso de campos configurados por obra (R1)

**User Story:** Como DBA analisando planos de `VW_ENTITY_A_DETAIL`, quero que grupos repetitivos de scalar subqueries de campos configurados sejam colapsados em um resumo, para que o relatório não seja dominado por dezenas de operações idênticas de baixo custo.

#### Acceptance Criteria

1. WHEN um grupo de ≥ 3 `SORT AGGREGATE` consecutivos com `starts == 1` cujos filhos contêm `IDX_ATTR_ENTITY_ID` e `PK_ATTR_CONFIG` é detectado, THE Compressor SHALL colapsar o grupo em um `CollapseResult`
2. WHEN qualquer bloco do grupo tem `immune = True`, THE Compressor SHALL descartar o grupo inteiro sem colapsar nenhum bloco
3. THE Compressor SHALL garantir que `len(cr.collapsed_ids) ≥ 3` para todo `CollapseResult` retornado por `_collapse_config_fields`
4. THE Compressor SHALL incluir em `cr.replacement_lines` custo total agregado (buffers + reads) do grupo colapsado
5. THE Compressor SHALL incluir em `cr.replacement_lines` aviso de que obras com campos configurados podem ter custo real

---

### Requirement 5: Colapso de histórico de situações (R2)

**User Story:** Como DBA analisando planos com múltiplos tipos de situação, quero que scalar subqueries `DATA_ALT_SIT_*` sejam colapsadas em uma tabela resumo, para que eu veja a distribuição de A-Rows por tipo sem poluição visual.

#### Acceptance Criteria

1. WHEN um grupo de ≥ 2 `SORT AGGREGATE` consecutivos com `starts == 1` cujos filhos contêm `UK_STATUS_TYPE_REF` e `IDX_STATUS_HIST_ENTITY` é detectado, THE Compressor SHALL colapsar o grupo em um `CollapseResult`
2. WHEN qualquer bloco do grupo tem `immune = True`, THE Compressor SHALL descartar o grupo inteiro sem colapsar nenhum bloco
3. THE Compressor SHALL garantir que `len(cr.collapsed_ids) ≥ 2` para todo `CollapseResult` retornado por `_collapse_situation_history`
4. THE Compressor SHALL incluir em `cr.replacement_lines` uma tabela com colunas Tipo, A-Rows e Buffers por entrada do grupo
5. WHEN `TYPE_REF` não pode ser extraído dos predicados, THE Compressor SHALL usar `"?"` como fallback na coluna Tipo

---

### Requirement 6: Colapso de VW_CURRENT_USER com A-Rows=0 (R3)

**User Story:** Como DBA analisando planos com joins a VW_CURRENT_USER, quero que expansões da view com resultado NULL sejam colapsadas, para que o plano não exiba dezenas de linhas de uma subárvore irrelevante nesta execução.

#### Acceptance Criteria

1. WHEN um nó `VIEW` com nome `VW_CURRENT_USER` ou `FROM$_SUBQUERY$_*` tem `a_rows == 0` e nenhum nó da subárvore é `immune`, THE Compressor SHALL colapsar a subárvore em um `CollapseResult`
2. WHEN qualquer nó da subárvore de `VW_CURRENT_USER` tem `immune = True`, THE Compressor SHALL preservar a subárvore inteira sem colapsar
3. THE Compressor SHALL incluir em `cr.replacement_lines` o total de buffers da subárvore colapsada
4. THE Compressor SHALL incluir em `cr.replacement_lines` aviso de que quando o usuário não é NULL a subárvore pode ter custo significativo

---

### Requirement 7: Remoção de predicados órfãos (R4)

**User Story:** Como DBA lendo o relatório comprimido, quero que predicados de operações colapsadas sejam removidos da seção Predicate Information, para que não haja referências a IDs inexistentes no plano.

#### Acceptance Criteria

1. WHEN um ID aparece em `all_collapsed_ids`, THE Compressor SHALL remover todas as linhas de predicado com esse ID da seção Predicate Information
2. WHEN predicados são removidos, THE Compressor SHALL adicionar nota `(N predicados de blocos colapsados omitidos — ver resumos acima)` ao final da seção
3. WHEN `collapsed_ids` é vazio, THE Compressor SHALL retornar `predicate_lines` sem modificação
4. THE Compressor SHALL preservar todas as linhas de predicado cujos IDs não estão em `collapsed_ids`

---

### Requirement 8: Nota de IDs não sequenciais (R6)

**User Story:** Como DBA lendo o relatório, quero ser informado quando o plano tem saltos de IDs, para que eu não confunda IDs omitidos por compressão com IDs omitidos pelo Oracle.

#### Acceptance Criteria

1. WHEN o plano comprimido contém salto > 1 entre IDs consecutivos, THE Compressor SHALL inserir nota explicativa após a linha `Plan hash value`
2. WHEN os IDs do plano são sequenciais sem saltos, THE Compressor SHALL não inserir nenhuma nota adicional
3. THE Compressor SHALL aplicar `_add_nonsequential_id_note` após todas as regras R1–R4

---

### Requirement 9: Orquestração determinística (algoritmo principal)

**User Story:** Como desenvolvedor mantendo o SqlMentor, quero que a compressão seja aplicada em ordem determinística e que nenhum bloco imune seja colapsado por nenhuma regra, para que o resultado seja previsível e auditável.

#### Acceptance Criteria

1. THE Compressor SHALL aplicar as regras na ordem: R5 (thresholds) → R1 (config fields) → R2 (situation history) → R3 (VW_CURRENT_USER) → R4 (orphan predicates) → R6 (nonsequential note)
2. WHEN `verbosity` é `"full"`, THE Compressor SHALL retornar `(plan_lines, predicate_lines)` sem nenhuma modificação
3. THE Compressor SHALL garantir que cada `CollapseResult` emite exatamente um bloco de resumo no plano reconstruído, sem duplicação
4. WHEN `_detect_plan_blocks` retorna lista vazia, THE Compressor SHALL retornar o plano original sem modificação
5. THE Compressor SHALL garantir que nenhum ID em `all_collapsed_ids` aparece no plano reconstruído
6. THE Compressor SHALL nunca levantar exceção para qualquer combinação de `plan_lines` e `predicate_lines` válidos

---

### Requirement 10: Integração CLI e MCP Server

**User Story:** Como usuário do SqlMentor, quero controlar a verbosidade tanto pela CLI quanto pelo MCP Server, para que eu tenha a mesma capacidade de compressão em ambas as interfaces.

#### Acceptance Criteria

1. THE CLI SHALL aceitar `--verbosity` como opção nos comandos `analyze` e `inspect` com valores `full`, `compact` e `minimal`
2. THE MCP_Server SHALL aceitar `verbosity` como parâmetro nas tools `analyze_sql` e `inspect_sql` com os mesmos três valores
3. WHEN `--verbosity` não é especificado na CLI, THE CLI SHALL usar `"compact"` como default
4. WHEN `verbosity` não é especificado no MCP_Server, THE MCP_Server SHALL usar `"compact"` como default
5. IF um valor inválido de `verbosity` é fornecido via CLI, THEN THE CLI SHALL exibir mensagem de erro descritiva ao usuário
6. IF um valor inválido de `verbosity` é fornecido via MCP_Server, THEN THE MCP_Server SHALL retornar erro descritivo ao caller
