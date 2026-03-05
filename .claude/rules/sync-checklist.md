# Checklists de Consistência

## Ao adicionar/alterar parâmetro em `analyze` ou `inspect`

- [ ] `src/sqlmentor/cli.py` — opção Typer
- [ ] `src/sqlmentor/mcp_server.py` — parâmetro na tool MCP
- [ ] `powers/sqlmentor/POWER.md` — documentação
- [ ] `README.md` — tabela de flags e exemplos

Exceções: `config`/`doctor` só CLI. `list_connections`/`test_connection` só MCP.

## Ao adicionar nova regra de compressão (Rn)

- [ ] Implementar `_collapse_*` em `report.py` com assinatura `(blocks) → list[CollapseResult]`
- [ ] Adicionar chamada em `_compress_plan` (após R5, antes de R4 e R6)
- [ ] Garantir que blocos com `immune=True` não são colapsados
- [ ] Garantir que `replacement_lines[0]` começa com `[COLAPSADO:`
- [ ] Testes unitários
- [ ] Atualizar `.claude/rules/architecture.md`
- [ ] PROIBIDO usar nomes de objetos do schema como critério

## Ao adicionar nova query Oracle

- [ ] Implementar em `queries/__init__.py` retornando `tuple[str, dict]`
- [ ] Bind variables (`:param`), nunca f-string com input do usuário
- [ ] Chamar via `cursor.execute(*fn())`

## Ao adicionar campo em `CollectedContext` ou `TableContext`

- [ ] `collector.py` — coleta
- [ ] `report.py` — formatação Markdown/JSON
- [ ] Verificar serialização em `to_json`

## Ao alterar metodologia de análise

- [ ] `.claude/agents/sqlmentor.md` — seção "Metodologia de análise"
- [ ] `powers/sqlmentor/steering/analysis.md` — metodologia Kiro Power

Ambos contêm a mesma metodologia e devem estar sincronizados.
