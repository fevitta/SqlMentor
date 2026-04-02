---
name: "sqlmentor"
displayName: "SqlMentor — SQL Performance Analysis"
description: "Coleta contexto Oracle/MariaDB (plano de execução, DDLs, índices, estatísticas, constraints) e analisa SQL para tuning assistido por IA. Gera relatórios estruturados otimizados para consumo por LLMs."
keywords: ["sqlmentor", "oracle", "mariadb", "tuning", "performance", "dba", "explain-plan"]
author: "Felipe"
---

# SqlMentor — SQL Performance Analysis

## Overview

SqlMentor é uma CLI Python que conecta em bancos Oracle 11g+ e MariaDB 10.6+ **[BETA]**, coletando toda a metadata necessária para análise de performance de SQL: plano de execução, DDLs, estatísticas de tabelas e colunas, índices, constraints, e parâmetros do otimizador.

O objetivo é gerar relatórios estruturados (Markdown/JSON) otimizados para que uma IA possa analisar e sugerir otimizações com base em evidência concreta extraída do banco.

## Available Steering Files

- **analysis** — Metodologia de análise SQL de DBA sênior Oracle/MariaDB. Carregue este steering SEMPRE que for analisar um relatório gerado pelo sqlmentor.

## Tools Disponíveis

### list_connections
Lista os profiles de conexão configurados (Oracle/MariaDB) e qual é a padrão. Use primeiro para saber qual `conn` passar, ou omita para usar a padrão.

### test_connection
Testa se um profile de conexão funciona. Retorna versão do banco e schema.

### parse_sql
Parse offline — extrai tabelas, colunas, joins, subqueries sem conectar no banco. Auto-detecta e desnormaliza SQL normalizado (Datadog, OEM). Aceita `dialect` ("oracle", "mariadb", "postgresql") para parse correto de sintaxe específica (ex: backtick identifiers).

### analyze_sql
A tool principal. Conecta no Oracle ou MariaDB, coleta contexto completo e retorna relatório estruturado. Veja os parâmetros na descrição da tool.

### inspect_sql (Oracle only)
Coleta contexto de um SQL já executado via `statement_id` (ex: sql_id Oracle), sem re-executar. Puxa plano real e métricas do banco via V$SQL e DBMS_XPLAN. Não disponível para MariaDB — use `analyze_sql` com `execute=True` ou `get_sql_text`.

### get_sql_text
Recupera o texto SQL de um statement já executado, sem coletar contexto. MariaDB: tenta SQL original primeiro, fallback para DIGEST_TEXT (normalizado). Oracle: recupera via V$SQL. Retorna JSON com `sql_text`, `source`, `statement_id`, `char_count`.

## Conexão Padrão

Defina uma conexão como padrão para não precisar passar `conn` toda vez:

```bash
sqlmentor config set-default -n prod
```

Depois disso, as tools usam essa conexão automaticamente quando `conn` é omitido. O `list_connections` mostra qual é a padrão.

## Workflow Recomendado

### Com sql_id (caminho rápido — Oracle only)
1. Chame `inspect_sql` com o `statement_id` (ex: sql_id Oracle) — já traz plano real e métricas do shared pool
2. Carregue o steering `analysis` e analise o relatório
3. Se precisar de mais contexto: `deep=True`, `expand_views=True`, `expand_functions=True`

### Com digest (MariaDB [BETA])
1. Chame `get_sql_text` com o `statement_id` (DIGEST) para recuperar o SQL
2. Chame `analyze_sql` com o SQL recuperado (ou `execute=True` para plano real)
3. Nota: `inspect_sql` não disponível para MariaDB. Requer `performance_schema` ativo.

### Sem sql_id
1. Chame `analyze_sql` (sem `execute` — rápido, plano estimado)
2. Carregue o steering `analysis` e analise o relatório
3. Se a análise precisa de mais assertividade, sugira ao usuário rodar com `execute=True` + binds para obter plano real com ALLSTATS
4. Se precisar de mais contexto: `deep=True`, `expand_views=True`, `expand_functions=True`

## Troubleshooting

Use `sqlmentor doctor` na CLI para verificar Python, oracledb/PyMySQL, Instant Client e testar todas as conexões de uma vez. Para MariaDB, verifica também o estado do `performance_schema` e seus consumers.
