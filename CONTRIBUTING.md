# Contribuindo com o SqlMentor

## Setup local

```bash
# Clonar e instalar
git clone https://github.com/fevitta/SqlMentor.git
cd SqlMentor
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Instalar pre-commit hooks
pre-commit install
```

## Rodando os checks

```bash
task lint            # ruff check src/ tests/
ruff format src/ tests/
mypy src/sqlmentor/
task test            # pytest
task test-cov        # pytest com cobertura (mínimo 90%)
```

Todos esses checks rodam automaticamente na CI ao abrir um PR.

## Convenções

- **Idioma**: docstrings e comentários em português, código e variáveis em inglês
- **Queries Oracle**: sempre bind variables (`:param`), nunca f-strings com input do usuário
- **Imports**: lazy dentro dos comandos CLI/MCP (não importar oracledb no top-level)
- **Testes**: cobertura mínima de 90%. Novos features devem incluir testes

## Processo de PR

1. Crie um branch a partir de `master`
2. Faça suas alterações com testes
3. Certifique-se de que `task lint`, `mypy` e `task test-cov` passam localmente
4. Abra o PR contra `master` com descrição do que foi feito e por quê
5. A CI vai rodar automaticamente — todos os checks devem passar
6. Aguarde review do maintainer

## Regras de sincronização

Ao alterar flags/parâmetros de `analyze` ou `inspect`, atualize todos:

1. `src/sqlmentor/cli.py`
2. `src/sqlmentor/mcp_server.py`
3. `powers/sqlmentor/POWER.md`
4. `README.md`

Detalhes em `.claude/rules/sync-checklist.md`.
