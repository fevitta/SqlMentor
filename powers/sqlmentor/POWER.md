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

- **analysis** — Prompt completo de análise SQL com metodologia de DBA sênior Oracle. Carregue este steering SEMPRE que for analisar um relatório gerado pelo sqlmentor. Contém: ordem de análise, critérios de evidência, formato de resposta, e regras críticas.

## Pré-requisitos

- Python 3.12+
- `sqlmentor` instalado (`pip install -e .` no repo do projeto)
- Pelo menos um profile de conexão Oracle configurado via `sqlmentor config add`
- O usuário Oracle precisa de permissões de leitura em `ALL_*` views e `DBMS_XPLAN`

## Tools Disponíveis

### list_connections
Lista os profiles de conexão Oracle configurados. Mostra qual conexão é a padrão (campo `default: true`). Use primeiro para saber qual `conn` passar para `analyze_sql`, ou omita `conn` para usar a conexão padrão.

### test_connection
Testa se um profile de conexão funciona. Retorna versão do banco e schema.

### parse_sql
Parse offline — extrai tabelas, colunas, joins, subqueries sem conectar no banco. Útil para entender a query antes de decidir o que coletar.

Auto-detecta SQL normalizado (Datadog, OEM, pg_stat_statements) e desnormaliza automaticamente antes do parse.

### analyze_sql
A tool principal. Conecta no Oracle, coleta contexto completo e retorna relatório estruturado.

Parâmetros importantes:
- `sql_text`: O SQL a ser analisado
- `conn`: Nome do profile de conexão. Se omitido, usa a conexão padrão definida via `sqlmentor config set-default`
- `deep`: Coleta histogramas e partições (mais lento, mais completo)
- `expand_views`: Detalha views referenciadas (DDL, colunas)
- `expand_functions`: Coleta DDL de funções PL/SQL
- `execute`: Executa a query real e coleta plano com ALLSTATS LAST + métricas de runtime
- `binds`: Bind variables no formato "nome=valor,nome2=valor2". Use `null` ou `none` para parâmetros que devem ser NULL no Oracle (ex: `binds="id=123,filtro=null"`)
- `timeout`: Timeout em segundos para operações no banco (0 = usa default do profile, 180s)
- `normalized`: Se True, força tratamento como SQL normalizado. Na maioria dos casos não é necessário — a auto-detecção identifica `?` placeholders automaticamente. Incompatível com `execute=True`.
- `denorm_mode`: Estratégia de desnormalização: `"literal"` (default, `?` → `'1'`) ou `"bind"` (`?` → `:dn1`, `:dn2`...). Modo bind gera plano com seletividade padrão do otimizador, sem depender de valores concretos.
- `verbosity`: Nível de compressão do plano: `"compact"` (default — todas as podas ativas), `"full"` (sem compressão, comportamento legado), `"minimal"` (só hotspots + runtime stats + parâmetros do otimizador, sem plano nem DDL).
- `no_cache`: Se True, ignora cache e força re-coleta de metadata. Útil quando tabelas/índices foram alterados.
- `show_sql`: Se True, inclui texto SQL completo no relatório. Omitido por padrão no compact para economizar tokens.
- `show_all_indexes`: Se True, mostra todos os índices. Por padrão, só mostra índices cujas colunas são relevantes ao SQL.

### inspect_sql
Coleta contexto de um SQL já executado via sql_id, sem re-executar. Puxa plano real e métricas do shared pool Oracle. Ideal para queries longas que já rodaram.

Parâmetros importantes:
- `sql_id`: SQL_ID da query no shared pool Oracle
- `conn`: Nome do profile de conexão. Se omitido, usa a conexão padrão
- `deep`, `expand_views`, `expand_functions`, `timeout`: mesmos do analyze_sql
- `verbosity`, `no_cache`, `show_sql`, `show_all_indexes`: mesmos do analyze_sql

## Conexão Padrão

Defina uma conexão como padrão para não precisar passar `conn` toda vez:

```bash
sqlmentor config set-default -n prod
```

Depois disso, `analyze_sql`, `inspect_sql` e os comandos CLI usam essa conexão automaticamente quando `conn` é omitido. O `list_connections` mostra qual é a padrão.

## Workflow Recomendado

1. Chame `list_connections` para ver os profiles disponíveis e qual é o default
2. Chame `parse_sql` para entender a estrutura da query
3. Chame `analyze_sql` com as flags adequadas:
   - Primeira rodada: sem `execute`, sem `deep` (rápido, plano estimado)
   - Se precisar de mais dados: adicione `deep=True` para histogramas
   - Se precisar de plano real: adicione `execute=True` com os binds necessários
   - Se a query referencia views: adicione `expand_views=True`
   - Se a query usa funções PL/SQL no WHERE: adicione `expand_functions=True`
4. Se a query já foi executada (pelo dev, pelo sistema), use `inspect_sql` com o `sql_id` em vez de re-executar
5. Carregue o steering `analysis` para obter as instruções de análise
6. Analise o relatório seguindo a metodologia do steering
7. Se faltam dados, chame `analyze_sql` ou `inspect_sql` novamente com flags adicionais

## SQL Normalizado (Datadog, OEM, pg_stat_statements)

Ferramentas de monitoramento normalizam SQL substituindo literais por `?`. Isso é comum em exports do Datadog, Oracle Enterprise Manager, e pg_stat_statements.

O SqlMentor detecta automaticamente SQL normalizado (2+ `?` fora de strings) e desnormaliza antes do parse e EXPLAIN PLAN. Não é necessário passar `normalized=True` na maioria dos casos.

Duas estratégias de desnormalização (`denorm_mode`):
- `"literal"` (default): substitui `?` por `'1'` (string literal). Funciona na maioria dos casos por conversão implícita do Oracle. O plano estimado pode divergir do real se o valor influenciar a cardinalidade.
- `"bind"`: substitui `?` por bind variables Oracle (`:dn1`, `:dn2`...). O otimizador usa seletividade padrão sem depender de valores concretos — mais fiel ao comportamento de prepared statements.

Limitações:
- SQL normalizado não pode ser executado com `execute=True` — os literais originais foram perdidos
- O plano estimado (sem execute) funciona normalmente com os literais dummy
- Bind variables Oracle (`:param`) são preservadas e não são afetadas

## Troubleshooting

### Diagnóstico do ambiente
- Use `sqlmentor doctor` na CLI para verificar Python, oracledb, Instant Client e testar todas as conexões de uma vez.

### Erro de conexão
- Verifique se o profile existe: `list_connections`
- Teste a conexão: `test_connection`
- Para Oracle < 12c, é necessário o Oracle Instant Client (modo thick). O `config add` da CLI já detecta isso automaticamente.

### Binds faltantes com execute=True
- A tool retorna quais binds estão faltando
- Passe no formato: `binds="id=123,status=A"`
- Para binds que podem ser NULL: `binds="id=123,filtro=null"` (aceita `null` ou `none`, case-insensitive)

### Plano sem ALLSTATS
- Use `execute=True` para obter plano real com A-Rows, Buffers, A-Time
- Sem execute, o plano é apenas estimado (E-Rows)

## Best Practices

- Sempre comece com plano estimado (sem execute) — é rápido e já mostra problemas óbvios
- Use `deep=True` quando suspeitar de problemas de cardinalidade (histogramas ajudam)
- Use `execute=True` quando precisar confirmar hipóteses com dados reais
- Sempre carregue o steering `analysis` antes de analisar — ele contém a metodologia completa
- Não sugira índices sem ter ALLSTATS — o steering explica por quê
