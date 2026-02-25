# Implementation Plan: plan-compression

## Overview

A implementação principal (dataclasses, regex, funções R1–R6, orquestrador, integração CLI/MCP) já está concluída. Este plano cobre o que ainda falta: correção de bug, testes unitários, property-based tests com `hypothesis`, fixtures de integração e atualização de documentação.

## Tasks

- [x] 1. Implementar dataclasses e parsing (PlanBlock, CollapseResult, _detect_plan_blocks)
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [x] 2. Implementar _apply_thresholds (R5)
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_

- [x] 3. Implementar regras de colapso R1–R3 e orquestrador _compress_plan
  - _Requirements: 4.1–4.5, 5.1–5.5, 6.1–6.4, 9.1–9.6_

- [x] 4. Implementar _collapse_orphan_predicates_by_ids (R4) e _add_nonsequential_id_note (R6)
  - _Requirements: 7.1–7.4, 8.1–8.3_

- [x] 5. Integrar verbosity em to_markdown, cli.py e mcp_server.py
  - _Requirements: 1.1–1.6, 10.1–10.6_

- [ ] 6. Corrigir bug em to_markdown — separação de plan_lines/predicate_lines e chamada de _compress_plan
  - Em `to_markdown()`, após `_prune_orphan_predicates`, o `plan_cleaned` contém plano e predicados misturados numa única lista. `_compress_plan` espera os dois separados.
  - Implementar função auxiliar `_split_plan_predicates(lines: list[str]) -> tuple[list[str], list[str]]` que divide a lista em `plan_lines` (antes da seção `Predicate Information`) e `predicate_lines` (a partir da linha `Predicate Information` inclusive)
  - Chamar `_split_plan_predicates(plan_cleaned)` antes de `_compress_plan`
  - Corrigir a chamada para `plan_compressed, pred_compressed = _compress_plan(plan_lines, predicate_lines, verbosity)`
  - Reconstruir o output concatenando `plan_compressed + pred_compressed` para renderizar no bloco de código do relatório
  - _Requirements: 9.1, 9.4, 9.5_

- [ ] 7. Adicionar hypothesis como dev dependency e criar estrutura de testes
  - Adicionar `hypothesis>=6.0` em `[project.optional-dependencies]` ou `[project.dependency-groups]` no `pyproject.toml`
  - Criar `tests/test_plan_compression.py` com imports e helpers de estratégias Hypothesis
  - Criar `tests/fixtures/` com pelo menos um plano real (ex: `sample_plan.txt`) para testes de integração
  - _Requirements: 2.7, 9.6_

- [ ] 8. Escrever testes unitários para funções de parsing e thresholds
  - [ ] 8.1 Testes para `_detect_plan_blocks`
    - Plano simples sem views: verifica ordem e campos numéricos
    - Linha malformada: deve ser ignorada sem exceção
    - Sufixos K/M/G em buffers: verifica conversão correta
    - Lista vazia: retorna lista vazia
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [ ]* 8.2 Escrever property test — Property 9: Round-trip de PlanBlock
    - **Property 9: Round-trip de PlanBlock**
    - **Validates: Requirements 2.7**
    - Estratégia: gerar `PlanBlock` válidos com `st.builds`, formatar para linha, parsear de volta, comparar campos

  - [ ] 8.3 Testes para `_apply_thresholds`
    - Cada threshold individualmente (reads, buffers, starts, a_time_ms, cardinalidade)
    - Bloco sem nenhum threshold: `immune = False`
    - Combinação de thresholds: `immune = True` se qualquer um atingido
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [ ]* 8.4 Escrever property test — Property 8: Thresholds determinísticos
    - **Property 8: Thresholds determinísticos**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
    - Estratégia: gerar `PlanBlock` com campos numéricos arbitrários, verificar que `immune` é `True` sse pelo menos um critério é satisfeito

- [ ] 9. Escrever testes unitários para regras de colapso R1–R3
  - [ ] 9.1 Testes para `_collapse_config_fields`
    - Grupo com ≥ 3 blocos sem imune: colapsa, `len(collapsed_ids) >= 3`
    - Grupo com bloco imune: não colapsa nenhum
    - Grupo com < 3 blocos: não colapsa
    - Verifica que `replacement_lines[0]` começa com `[COLAPSADO:`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [ ] 9.2 Testes para `_collapse_situation_history`
    - Grupo com ≥ 2 blocos sem imune: colapsa, tabela com colunas Tipo/A-Rows/Buffers
    - Grupo com bloco imune: não colapsa
    - TYPE_REF ausente nos predicados: usa `"?"` como fallback
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ] 9.3 Testes para `_collapse_vw_usuario_null`
    - Nó VIEW com `a_rows == 0` e subárvore sem imune: colapsa
    - Nó VIEW com `a_rows > 0`: não colapsa
    - Subárvore com nó imune: preserva tudo
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ]* 9.4 Escrever property test — Property 4: Imunidade preservada
    - **Property 4: Imunidade preservada**
    - **Validates: Requirements 3.7, 4.2, 5.2, 6.2**
    - Estratégia: gerar lista de `PlanBlock`, aplicar thresholds, rodar R1+R2+R3, verificar que `immune_ids ∩ collapsed_ids = ∅`

  - [ ]* 9.5 Escrever property test — Property 7: Grupos mínimos
    - **Property 7: Grupos mínimos**
    - **Validates: Requirements 4.3, 5.3**
    - Estratégia: para qualquer lista de blocos, `_collapse_config_fields` nunca retorna `CollapseResult` com `len(collapsed_ids) < 3`; `_collapse_situation_history` nunca retorna com `len(collapsed_ids) < 2`

  - [ ]* 9.6 Escrever property test — Property 3: Nenhuma poda silenciosa
    - **Property 3: Nenhuma poda silenciosa**
    - **Validates: Requirements 4.4, 5.4, 6.3**
    - Estratégia: para qualquer `CollapseResult` retornado por R1/R2/R3, `len(cr.replacement_lines) >= 1` e `cr.replacement_lines[0].startswith("[COLAPSADO:")`

- [ ] 10. Checkpoint — Garantir que todos os testes unitários passam
  - Garantir que todos os testes passam, perguntar ao usuário se houver dúvidas.

- [ ] 11. Escrever testes para R4, R6 e orquestrador
  - [ ] 11.1 Testes para `_collapse_orphan_predicates_by_ids`
    - IDs colapsados: linhas de predicado correspondentes removidas
    - IDs não colapsados: linhas preservadas integralmente
    - `collapsed_ids` vazio: retorna `predicate_lines` sem modificação
    - Verifica nota de omissão quando `pruned > 0`
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ]* 11.2 Escrever property test — Property 5: Consistência de predicados
    - **Property 5: Consistência de predicados**
    - **Validates: Requirements 7.1, 7.4**
    - Estratégia: gerar `predicate_lines` e `collapsed_ids` arbitrários, verificar que nenhuma linha com ID colapsado aparece no resultado e todas as linhas com ID não colapsado são preservadas

  - [ ] 11.3 Testes para `_add_nonsequential_id_note`
    - IDs sequenciais (1,2,3): sem nota inserida
    - IDs com salto (1,3,5): nota inserida após `Plan hash value`
    - _Requirements: 8.1, 8.2_

  - [ ]* 11.4 Escrever property test — Property 12: Nota de IDs não sequenciais
    - **Property 12: Nota de IDs não sequenciais**
    - **Validates: Requirements 8.1, 8.2**
    - Estratégia: gerar planos com IDs sequenciais e não sequenciais, verificar presença/ausência da nota

  - [ ] 11.5 Testes para `_compress_plan`
    - `verbosity="full"`: retorna input sem modificação
    - Plano sem linhas parseáveis: retorna original sem modificação
    - Plano com grupos colapsáveis: IDs colapsados ausentes do resultado
    - Cada `CollapseResult` emite exatamente um bloco de resumo (sem duplicação)
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [ ]* 11.6 Escrever property test — Property 10: IDs colapsados ausentes do plano reconstruído
    - **Property 10: IDs colapsados ausentes do plano reconstruído**
    - **Validates: Requirements 9.3, 9.5**

  - [ ]* 11.7 Escrever property test — Property 11: Robustez de _compress_plan
    - **Property 11: Robustez de _compress_plan**
    - **Validates: Requirements 9.6**
    - Estratégia: `@given(st.lists(st.text()), st.lists(st.text()), st.sampled_from(["full", "compact", "minimal"]))` — nunca levanta exceção, sempre retorna `(list, list)`

- [ ] 12. Escrever testes para to_markdown e integração CLI/MCP
  - [ ] 12.1 Testes para `to_markdown` com verbosity
    - `verbosity` inválido levanta `ValueError` com mensagem descritiva
    - `verbosity="full"` produz output sem blocos `[COLAPSADO:`
    - `verbosity="compact"` produz output com blocos `[COLAPSADO:` para planos com views complexas
    - `verbosity="minimal"` produz output sem seção de plano de execução e sem DDLs
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [ ]* 12.2 Escrever property test — Property 6: Verbosity inválido levanta erro
    - **Property 6: Verbosity inválido levanta erro**
    - **Validates: Requirements 1.5**
    - Estratégia: `@given(st.text().filter(lambda s: s not in {"full", "compact", "minimal"}))` — sempre levanta `ValueError`

  - [ ]* 12.3 Escrever property test — Property 2: Monotonicidade de compressão
    - **Property 2: Monotonicidade de compressão**
    - **Validates: Requirements 1.3, 1.4**
    - Estratégia: para qualquer `CollectedContext` válido, `len(minimal) <= len(compact) <= len(full)`

  - [ ]* 12.4 Escrever property test — Property 1: Idempotência de full
    - **Property 1: Idempotência de full**
    - **Validates: Requirements 1.2, 9.2**
    - Estratégia: `to_markdown(ctx, "full")` chamado duas vezes produz output idêntico

- [ ] 13. Teste de integração com fixture de plano real
  - Usar fixture `tests/fixtures/sample_plan.txt` (ou equivalente)
  - Verificar que `compact` produz menos linhas que `full` para o plano real
  - Verificar que `full` produz output idêntico ao baseline (regressão)
  - _Requirements: 1.2, 1.3, 10.1, 10.2_

- [ ] 14. Checkpoint final — Garantir que todos os testes passam
  - Garantir que todos os testes passam, perguntar ao usuário se houver dúvidas.

- [ ] 15. Atualizar documentação
  - [ ] 15.1 Atualizar `powers/sqlmentor/POWER.md` com parâmetro `verbosity` nas tools `analyze_sql` e `inspect_sql`
    - Documentar os três valores (`full`, `compact`, `minimal`) e seus comportamentos
    - _Requirements: 10.2_

  - [ ] 15.2 Atualizar `README.md` com exemplos de uso de `--verbosity`
    - Adicionar exemplos para CLI (`--verbosity compact`, `--verbosity minimal`)
    - Adicionar exemplos para MCP (`verbosity="compact"`)
    - _Requirements: 10.1, 10.2_

## Notes

- Tarefas marcadas com `*` são opcionais e podem ser puladas para MVP mais rápido
- A tarefa 6 (bug fix) deve ser executada antes dos testes para não mascarar falhas
- Property tests dependem de estratégias Hypothesis customizadas para `PlanBlock` e `CollectedContext`
- Fixtures de plano real devem ser anonimizadas (sem dados de produção)
