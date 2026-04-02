# SqlMentor

CLI Python para coleta de contexto Oracle 11g+ e MariaDB 10.6+ **[BETA]**, otimizado para tuning de SQL assistido por IA.

Dado um SQL (query, procedure, trigger, function), o `sqlmentor` conecta no banco, extrai automaticamente todo o metadata relevante (plano de execução, DDLs, índices, estatísticas, constraints, parâmetros do otimizador) e gera um relatório estruturado (Markdown ou JSON) pronto para ser consumido por um LLM.

## Instalação

```bash
pip install sqlmentor                # instalar via PyPI
pip install --upgrade sqlmentor      # atualizar para última versão
```

Para desenvolvimento (clone do repo):

```bash
pip install -e ".[dev]"
```

Inclui drivers Oracle (`oracledb`) e MariaDB (`PyMySQL`) automaticamente.

Pré-requisitos: Python 3.12+ e acesso a um Oracle 11g+ ou MariaDB 10.6+.

> Para Oracle < 12c (modo thick), veja [docs/oracle-instant-client.md](docs/oracle-instant-client.md).

## Uso Rápido — Oracle

```bash
# Configurar conexão
sqlmentor config add oracle -n prod -h 192.168.0.1 -s ORCL -u SQLMENTOR --schema SQLMENTOR

# Plano estimado
sqlmentor analyze minha_query.sql --conn prod

# Plano real (executa a query com ALLSTATS LAST + métricas V$SQL)
sqlmentor analyze minha_query.sql --conn prod --execute

# Com bind variables
sqlmentor analyze minha_query.sql --conn prod --execute -b id=123 -b status=A

# Inspecionar query já executada (sem re-executar) — Oracle only
sqlmentor inspect <sql_id> --conn prod

# Recuperar texto SQL de um statement via V$SQL
sqlmentor get-sql <sql_id> --conn prod
```

## Uso Rápido — MariaDB [BETA]

```bash
# Configurar conexão
sqlmentor config add mariadb -n dev -h 192.168.0.1 -d mydb -u SQLMENTOR --schema mydb

# Plano estimado (EXPLAIN FORMAT=JSON)
sqlmentor analyze minha_query.sql --conn dev

# Plano real (ANALYZE FORMAT=JSON + métricas do performance_schema)
sqlmentor analyze minha_query.sql --conn dev --execute

# Recuperar texto SQL por DIGEST (tenta SQL original, fallback para DIGEST_TEXT)
sqlmentor get-sql <digest> --conn dev
```

> **Setup requerido para MariaDB `--execute`:** o `performance_schema` deve estar ativo e os consumers habilitados. Use `sqlmentor doctor --conn dev` para verificar. Se necessário:
>
> ```sql
> -- my.cnf (requer restart)
> [mysqld]
> performance_schema = ON
>
> -- Ativar consumers (sem restart, mas não persiste)
> UPDATE performance_schema.setup_consumers
>    SET ENABLED = 'YES'
>  WHERE NAME IN ('statements_digest', 'events_statements_history_long');
> ```
>
> O consumer `events_statements_history_long` é necessário para `get-sql` recuperar o SQL original (não normalizado).

## Comandos Comuns

```bash
# Parse offline (sem conexão ao banco)
sqlmentor parse minha_query.sql --schema SCHEMA

# Diagnóstico do ambiente
sqlmentor doctor

# Gerenciar conexões
sqlmentor config list
sqlmentor config test -n prod
sqlmentor config set-default -n prod
sqlmentor config remove -n old
```

## Flags

| Flag | Descrição |
|------|-----------|
| `--execute`, `-x` | Plano real: ALLSTATS LAST + V$SQL (Oracle) · ANALYZE FORMAT=JSON + performance_schema (MariaDB) |
| `--deep`, `-d` | Histogramas e partições |
| `--expand-views` | DDL e colunas internas das views |
| `--expand-functions` | DDL de funções PL/SQL referenciadas |
| `--verbosity compact\|full\|minimal` | Nível de compressão do relatório (default: `compact`) |
| `--show-sql` | Inclui texto SQL completo no relatório |
| `--show-all-indexes` | Mostra todos os índices (não só os referenciados no SQL) |
| `--normalized`, `-n` | SQL normalizado (Datadog, OEM, etc.) — auto-detectado se omitido |
| `--denorm-mode literal\|bind` | Estratégia de desnormalização: `literal` ('?' → '1') ou `bind` ('?' → :dn1) |
| `--timeout`, `-t` | Timeout em segundos (sobrescreve o do profile, default: 600) |
| `--output`, `-o` | Arquivo de saída (se omitido, salva em `reports/` automaticamente) |
| `--verbose`, `-v` | Imprime o relatório completo no console |
| `--no-cache` | Força re-coleta de metadata |
| `--format json`, `-f json` | Relatório em JSON |
| `--debug` | Mostra queries e tempos internos |
| `-b nome=valor` | Bind variables para `--execute` (repetível) |

## O que é coletado

| Dado | Oracle | MariaDB [BETA] | Flag |
|------|--------|----------------|------|
| Plano estimado | EXPLAIN PLAN | EXPLAIN FORMAT=JSON | padrão |
| Plano real + runtime stats | ALLSTATS LAST + V$SQL + wait events | ANALYZE FORMAT=JSON + performance_schema | `--execute` |
| Hotspots + conversões implícitas + view expansion | sim | sim | sempre |
| Estatísticas, colunas, índices, constraints + FKs | sim | sim | sempre |
| Parâmetros do otimizador (com alertas de atípicos) | sim | sim | sempre |
| DDL de views / funções | sim | sim | `--expand-views` / `--expand-functions` |
| Histogramas + partições | sim | sim | `--deep` |

Relatórios são salvos automaticamente em `reports/` com timestamp no nome (ex: `minha_query_20250115_143022.md`).

## MariaDB [BETA] — Limitações conhecidas

- **`inspect` não disponível**: MariaDB não tem equivalente ao shared pool Oracle (V$SQL + DBMS_XPLAN). Use `get-sql` para recuperar o SQL e `analyze --execute` para plano real.
- **Compressão R1-R8**: regras de colapso do plano operam sobre formato pipe Oracle. Para MariaDB, apenas R5 (thresholds de imunidade) e R9-R12 (metadata) são aplicados.
- **Buffers/Reads**: métricas de I/O lógico/físico não estão disponíveis no plano MariaDB. Hotspots usam `r_rows`, `r_filtered` e `r_total_time_ms`.
- **performance_schema**: precisa estar ativo para `--execute` e `get-sql`. Sem ele, apenas plano estimado funciona.

## Integração com IA

### Claude Code (recomendado)

O repositório inclui um agente DBA sênior (`.claude/agents/sqlmentor.md`) que opera o CLI e produz análises de tuning orientadas por evidência. Qualquer dev que clone o repo herda o agente automaticamente.

```bash
# No Claude Code, invoque o agente:
@sqlmentor analise a query em minha_query.sql usando a conexão prod
```

### MCP Server (alternativo)

Para IDEs com suporte a Model Context Protocol (Kiro, Claude Desktop, etc.):

```json
{
  "mcpServers": {
    "sqlmentor": {
      "command": "sqlmentor-mcp",
      "args": []
    }
  }
}
```

<details>
<summary>Tools MCP disponíveis</summary>

| Tool | Descrição |
|------|-----------|
| `list_connections` | Lista profiles de conexão configurados |
| `test_connection` | Testa um profile (retorna versão e schema) |
| `parse_sql` | Parse offline — tabelas, colunas, joins. Aceita `dialect` |
| `analyze_sql` | Análise completa: conecta, coleta contexto, retorna relatório |
| `inspect_sql` | Contexto de SQL já executado via sql_id (**Oracle only**) |
| `get_sql_text` | Recupera texto SQL de um statement já executado |
| `get_status` | Status do servidor MCP |

</details>

### Kiro Power

Para times que usam Kiro: `powers/sqlmentor/` empacota MCP + metodologia de análise DBA sênior. Instale via Powers UI → Add Custom Power → caminho de `powers/sqlmentor`.

## Desenvolvimento

```bash
pip install -e ".[dev]"     # instalar com deps de dev
task test                   # pytest
task test-cov               # pytest com cobertura
task lint                   # ruff check
ruff format src/ tests/     # formatação
```

CI (GitHub Actions): Python 3.12, ruff check, ruff format --check, mypy, pytest com cobertura ≥ 90%.

## Roadmap

- [ ] MariaDB: compressão R1-R8 adaptada para plano JSON
- [ ] Análise de procedures (EXPLAIN de cada SQL interno)
- [ ] MariaDB: sair do beta após validação em produção

## Licença

[MIT](LICENSE)
