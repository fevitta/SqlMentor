---
name: "sqlmentor"
displayName: "SqlMentor — Oracle Performance Analysis"
description: "Coleta contexto Oracle (plano de execução, DDLs, índices, estatísticas, constraints) e analisa SQL para tuning assistido por IA. Gera relatórios estruturados otimizados para consumo por LLMs."
keywords: ["sqlmentor", "oracle", "tuning", "performance", "dba", "explain-plan"]
author: "Felipe"
---

# SqlMentor — SQL Performance Analysis

## Overview

SqlMentor é uma CLI Python que conecta em bancos Oracle 11g+ e coleta toda a metadata necessária para análise de performance de SQL: plano de execução, DDLs, estatísticas de tabelas e colunas, índices, constraints, e parâmetros do otimizador.

O objetivo é gerar relatórios estruturados (Markdown/JSON) otimizados para que uma IA possa analisar e sugerir otimizações com base em evidência concreta extraída do banco.

## Available Steering Files

- **analysis** — Metodologia de análise SQL de DBA sênior Oracle. Carregue este steering SEMPRE que for analisar um relatório gerado pelo sqlmentor.

## Tools Disponíveis

### list_connections
Lista os profiles de conexão Oracle configurados e qual é a padrão. Use primeiro para saber qual `conn` passar, ou omita para usar a padrão.

### test_connection
Testa se um profile de conexão funciona. Retorna versão do banco e schema.

### parse_sql
Parse offline — extrai tabelas, colunas, joins, subqueries sem conectar no banco. Auto-detecta e desnormaliza SQL normalizado (Datadog, OEM).

### analyze_sql
A tool principal. Conecta no Oracle, coleta contexto completo e retorna relatório estruturado. Veja os parâmetros na descrição da tool.

### inspect_sql
Coleta contexto de um SQL já executado via `sql_id`, sem re-executar. Puxa plano real e métricas do shared pool Oracle. Veja os parâmetros na descrição da tool.

## Conexão Padrão

Defina uma conexão como padrão para não precisar passar `conn` toda vez:

```bash
sqlmentor config set-default -n prod
```

Depois disso, as tools usam essa conexão automaticamente quando `conn` é omitido. O `list_connections` mostra qual é a padrão.

## Workflow Recomendado

### Com sql_id (caminho rápido)
1. Chame `inspect_sql` com o `sql_id` — já traz plano real e métricas do shared pool
2. Carregue o steering `analysis` e analise o relatório
3. Se precisar de mais contexto: `deep=True`, `expand_views=True`, `expand_functions=True`

### Sem sql_id
1. Chame `analyze_sql` (sem `execute` — rápido, plano estimado)
2. Carregue o steering `analysis` e analise o relatório
3. Se a análise precisa de mais assertividade, sugira ao usuário rodar com `execute=True` + binds para obter plano real com ALLSTATS
4. Se precisar de mais contexto: `deep=True`, `expand_views=True`, `expand_functions=True`

## Troubleshooting

Use `sqlmentor doctor` na CLI para verificar Python, oracledb, Instant Client e testar todas as conexões de uma vez.
