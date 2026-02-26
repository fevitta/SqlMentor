"""
Queries Oracle 11g para coleta de metadata e estatísticas.

Cada função retorna uma tuple (sql, params) pronta pra executar.
"""

import re

_SQL_ID_RE = re.compile(r"^[a-z0-9]{8,16}$")


def _validate_sql_id(sql_id: str) -> None:
    """Valida formato do sql_id Oracle (alfanumérico lowercase, 8-16 chars)."""
    if not _SQL_ID_RE.match(sql_id):
        raise ValueError(
            f"sql_id inválido: {sql_id!r}. "
            "Formato esperado: 8-16 caracteres alfanuméricos lowercase (ex: 'abc123def4567')."
        )


def explain_plan(sql_text: str) -> list[tuple[str, dict]]:
    """Gera EXPLAIN PLAN e recupera o resultado."""
    # EXPLAIN PLAN não aceita bind variables no STATEMENT_ID — usa literal.
    stmt_id = "SQLMENTOR_PLAN"
    return [
        (
            f"EXPLAIN PLAN SET STATEMENT_ID = '{stmt_id}' FOR {sql_text}",
            {},
        ),
        (
            f"""
            SELECT plan_table_output
            FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '{stmt_id}', 'ALL'))
            """,  # noqa: S608
            {},
        ),
        (
            "DELETE FROM PLAN_TABLE WHERE statement_id = :stmt_id",
            {"stmt_id": stmt_id},
        ),
    ]


def runtime_plan(sql_id: str, child_number: int = 0) -> tuple[str, dict]:
    """Plano real com ALLSTATS LAST via sql_id explícito (sem Outline pra reduzir ruído).

    Nota: DBMS_XPLAN.DISPLAY_CURSOR não aceita bind variables nos parâmetros
    sql_id e child_number — usa f-string com validação prévia do formato.
    """
    _validate_sql_id(sql_id)
    return (
        f"""
        SELECT plan_table_output
        FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(
            '{sql_id}', {child_number}, 'ALLSTATS LAST +PEEKED_BINDS -OUTLINE'
        ))
        """,  # noqa: S608
        {},
    )


def sql_runtime_stats(sql_id: str) -> tuple[str, dict]:
    """Métricas de execução reais de V$SQL (compatível com 11g)."""
    return (
        """
        SELECT s.sql_id, s.child_number, s.plan_hash_value,
               s.executions, s.elapsed_time, s.cpu_time,
               s.buffer_gets, s.disk_reads, s.rows_processed,
               s.sorts, s.fetches,
               s.parse_calls, s.loads, s.invalidations,
               (SELECT COUNT(*) FROM v$sql c WHERE c.sql_id = s.sql_id) AS version_count,
               ROUND(s.elapsed_time / GREATEST(s.executions, 1) / 1000, 2) AS avg_elapsed_ms,
               ROUND(s.cpu_time / GREATEST(s.executions, 1) / 1000, 2) AS avg_cpu_ms,
               ROUND(s.buffer_gets / GREATEST(s.executions, 1), 0) AS avg_buffer_gets,
               ROUND(s.rows_processed / GREATEST(s.executions, 1), 0) AS avg_rows_per_exec
        FROM v$sql s
        WHERE s.sql_id = :sql_id
        AND ROWNUM = 1
        ORDER BY s.child_number DESC
        """,
        {"sql_id": sql_id},
    )


def sql_text_by_id(sql_id: str) -> tuple[str, dict]:
    """Recupera o texto completo do SQL a partir do sql_id via V$SQL."""
    return (
        """
        SELECT sql_fulltext
        FROM v$sql
        WHERE sql_id = :sql_id
        AND ROWNUM = 1
        """,
        {"sql_id": sql_id},
    )


def session_wait_events(sid: int) -> tuple[str, dict]:
    """Wait events da sessão atual (top 10 por tempo)."""
    return (
        """
        SELECT * FROM (
            SELECT event, total_waits, time_waited_micro,
                   ROUND(time_waited_micro / 1000, 2) AS time_waited_ms,
                   average_wait
            FROM v$session_event
            WHERE sid = :sid
            AND event NOT LIKE 'SQL*Net%'
            ORDER BY time_waited_micro DESC
        ) WHERE ROWNUM <= 10
        """,
        {"sid": sid},
    )


def db_version() -> tuple[str, dict]:
    """Versão do banco Oracle."""
    return (
        "SELECT banner FROM v$version WHERE ROWNUM = 1",
        {},
    )


def object_type(owner: str, object_name: str) -> tuple[str, dict]:
    """Tipo do objeto (TABLE, VIEW, etc.)."""
    return (
        """
        SELECT object_type FROM all_objects
        WHERE owner = :owner AND object_name = :object_name
        AND object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
        AND ROWNUM = 1
        """,
        {"owner": owner.upper(), "object_name": object_name.upper()},
    )


def index_to_table_map(owner: str) -> tuple[str, dict]:
    """Mapa index_name → table_name para um schema."""
    return (
        """
        SELECT index_name, table_name
        FROM all_indexes
        WHERE owner = :owner
        """,
        {"owner": owner.upper()},
    )


def table_ddl(owner: str, table_name: str) -> tuple[str, dict]:
    """DDL completo da tabela ou view via DBMS_METADATA."""
    # Tenta detectar se é view ou tabela pra usar o tipo correto.
    # Chamador deve tratar exceção e tentar com 'VIEW' se 'TABLE' falhar.
    return (
        """
        SELECT DBMS_METADATA.GET_DDL(
            CASE
                WHEN (SELECT object_type FROM all_objects
                      WHERE owner = :owner AND object_name = :table_name
                      AND object_type IN ('TABLE', 'VIEW')
                      AND ROWNUM = 1) = 'VIEW'
                THEN 'VIEW'
                ELSE 'TABLE'
            END,
            :table_name, :owner
        ) AS ddl
        FROM DUAL
        """,
        {"owner": owner.upper(), "table_name": table_name.upper()},
    )


def function_ddl(owner: str, function_name: str) -> tuple[str, dict]:
    """DDL de uma função ou procedure PL/SQL via DBMS_METADATA."""
    return (
        """
        SELECT DBMS_METADATA.GET_DDL(
            CASE
                WHEN (SELECT object_type FROM all_objects
                      WHERE owner = :owner AND object_name = :function_name
                      AND object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')
                      AND ROWNUM = 1) = 'PROCEDURE'
                THEN 'PROCEDURE'
                WHEN (SELECT object_type FROM all_objects
                      WHERE owner = :owner AND object_name = :function_name
                      AND object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')
                      AND ROWNUM = 1) = 'PACKAGE'
                THEN 'PACKAGE_SPEC'
                ELSE 'FUNCTION'
            END,
            :function_name, :owner
        ) AS ddl
        FROM DUAL
        """,
        {"owner": owner.upper(), "function_name": function_name.upper()},
    )


def table_stats(owner: str, table_name: str) -> tuple[str, dict]:
    """Estatísticas gerais da tabela."""
    return (
        """
        SELECT table_name, num_rows, blocks, avg_row_len,
               last_analyzed, sample_size, partitioned, temporary,
               degree, compression
        FROM all_tables
        WHERE owner = :owner AND table_name = :table_name
        """,
        {"owner": owner.upper(), "table_name": table_name.upper()},
    )


def column_stats(owner: str, table_name: str) -> tuple[str, dict]:
    """Estatísticas de colunas (cardinalidade, nulls, histogramas)."""
    return (
        """
        SELECT c.column_name, c.data_type, c.data_length, c.nullable,
               s.num_distinct, s.num_nulls, s.density, s.histogram,
               s.num_buckets, s.last_analyzed, s.sample_size,
               c.data_default
        FROM all_tab_columns c
        LEFT JOIN all_tab_col_statistics s
            ON s.owner = c.owner
            AND s.table_name = c.table_name
            AND s.column_name = c.column_name
        WHERE c.owner = :owner AND c.table_name = :table_name
        ORDER BY c.column_id
        """,
        {"owner": owner.upper(), "table_name": table_name.upper()},
    )


def indexes(owner: str, table_name: str) -> tuple[str, dict]:
    """Índices da tabela com colunas."""
    return (
        """
        SELECT i.index_name, i.index_type, i.uniqueness, i.status,
               i.num_rows, i.distinct_keys, i.clustering_factor,
               i.last_analyzed, i.blevel, i.leaf_blocks,
               LISTAGG(ic.column_name, ', ')
                   WITHIN GROUP (ORDER BY ic.column_position) AS columns
        FROM all_indexes i
        JOIN all_ind_columns ic
            ON ic.index_owner = i.owner
            AND ic.index_name = i.index_name
        WHERE i.table_owner = :owner AND i.table_name = :table_name
        GROUP BY i.index_name, i.index_type, i.uniqueness, i.status,
                 i.num_rows, i.distinct_keys, i.clustering_factor,
                 i.last_analyzed, i.blevel, i.leaf_blocks
        ORDER BY i.index_name
        """,
        {"owner": owner.upper(), "table_name": table_name.upper()},
    )


def constraints(owner: str, table_name: str) -> tuple[str, dict]:
    """Constraints (PK, FK, unique, check). FK inclui tabela referenciada."""
    return (
        """
        SELECT c.constraint_name, c.constraint_type, c.status,
               c.validated, c.r_constraint_name,
               r.table_name AS r_table_name,
               r.owner AS r_owner,
               LISTAGG(cc.column_name, ', ')
                   WITHIN GROUP (ORDER BY cc.position) AS columns
        FROM all_constraints c
        LEFT JOIN all_cons_columns cc
            ON cc.owner = c.owner
            AND cc.constraint_name = c.constraint_name
        LEFT JOIN all_constraints r
            ON r.owner = c.r_owner
            AND r.constraint_name = c.r_constraint_name
        WHERE c.owner = :owner AND c.table_name = :table_name
        GROUP BY c.constraint_name, c.constraint_type, c.status,
                 c.validated, c.r_constraint_name, r.table_name, r.owner
        ORDER BY c.constraint_type, c.constraint_name
        """,
        {"owner": owner.upper(), "table_name": table_name.upper()},
    )


def histograms(owner: str, table_name: str, column_name: str) -> tuple[str, dict]:
    """Histograma detalhado de uma coluna específica."""
    return (
        """
        SELECT endpoint_number, endpoint_value,
               endpoint_actual_value, endpoint_repeat_count
        FROM all_tab_histograms
        WHERE owner = :owner
            AND table_name = :table_name
            AND column_name = :column_name
        ORDER BY endpoint_number
        """,
        {
            "owner": owner.upper(),
            "table_name": table_name.upper(),
            "column_name": column_name.upper(),
        },
    )


def optimizer_params() -> tuple[str, dict]:
    """Parâmetros relevantes do otimizador (valor efetivo da sessão via V$PARAMETER)."""
    return (
        """
        SELECT name, value
        FROM v$parameter
        WHERE name IN (
            'optimizer_mode',
            'optimizer_features_enable',
            'optimizer_index_cost_adj',
            'optimizer_index_caching',
            'optimizer_dynamic_sampling',
            'db_file_multiblock_read_count',
            'cursor_sharing',
            'statistics_level',
            'workarea_size_policy',
            'result_cache_mode',
            'star_transformation_enabled',
            'parallel_degree_policy',
            'pga_aggregate_target',
            'sga_target',
            'nls_sort',
            'nls_comp'
        )
        ORDER BY name
        """,
        {},
    )


def table_partitions(owner: str, table_name: str) -> tuple[str, dict]:
    """Info de particionamento, se existir."""
    return (
        """
        SELECT partition_name, partition_position, high_value,
               num_rows, blocks, last_analyzed
        FROM all_tab_partitions
        WHERE table_owner = :owner AND table_name = :table_name
        ORDER BY partition_position
        """,
        {"owner": owner.upper(), "table_name": table_name.upper()},
    )


def dangerous_privileges() -> tuple[str, dict]:
    """Privilégios de sistema perigosos (escrita/DDL) que o user do sqlmentor NÃO deveria ter."""
    return (
        """
        SELECT privilege
        FROM session_privs
        WHERE privilege IN (
            'INSERT ANY TABLE', 'UPDATE ANY TABLE', 'DELETE ANY TABLE',
            'ALTER ANY TABLE', 'DROP ANY TABLE', 'CREATE ANY TABLE',
            'ALTER ANY INDEX', 'DROP ANY INDEX', 'CREATE ANY INDEX',
            'GRANT ANY PRIVILEGE', 'GRANT ANY ROLE',
            'ALTER DATABASE', 'ALTER SYSTEM',
            'DROP USER', 'CREATE USER', 'ALTER USER',
            'SYSDBA', 'SYSOPER',
            'EXECUTE ANY PROCEDURE',
            'ALTER ANY PROCEDURE', 'DROP ANY PROCEDURE', 'CREATE ANY PROCEDURE',
            'CREATE ANY TRIGGER', 'ALTER ANY TRIGGER', 'DROP ANY TRIGGER'
        )
        """,
        {},
    )


def dangerous_roles() -> tuple[str, dict]:
    """Roles perigosas que o user do sqlmentor NÃO deveria ter."""
    return (
        """
        SELECT role
        FROM session_roles
        WHERE role IN (
            'DBA', 'IMP_FULL_DATABASE', 'EXP_FULL_DATABASE',
            'DATAPUMP_IMP_FULL_DATABASE', 'EXECUTE_CATALOG_ROLE',
            'DELETE_CATALOG_ROLE', 'RESOURCE'
        )
        """,
        {},
    )
