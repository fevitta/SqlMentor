"""
Adapter MariaDB — implementação concreta de DatabaseAdapter.

Encapsula toda lógica específica do PyMySQL: conexão, validação de privilégios,
diagnóstico. MariaDBQueryBuilder contém queries reais contra information_schema
e performance_schema. explain_plan/runtime_plan permanecem stubs (T20).
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, ClassVar

import pymysql  # type: ignore[import-untyped]

from sqlmentor.adapters import register_adapter
from sqlmentor.adapters.base import DatabaseAdapter, PlanParser, QueryBuilder

if TYPE_CHECKING:
    from sqlmentor.report import PlanBlock

logger = logging.getLogger(__name__)


# ── MariaDBQueryBuilder ──────────────────────────────────────────────


class MariaDBQueryBuilder(QueryBuilder):
    """Queries MariaDB parametrizadas (paramstyle pyformat: %(name)s).

    Queries contra information_schema e performance_schema.
    explain_plan e runtime_plan são stubs — serão implementados em T20.
    """

    @staticmethod
    def _sanitize_identifier(name: str) -> str:
        """Remove backticks de identificadores para prevenir injeção SQL."""
        return name.replace("`", "")

    @staticmethod
    def build_tuple_in_clause(
        pairs: list[tuple[str, str]],
    ) -> tuple[str, dict[str, str]]:
        """Constrói cláusula OR-chain para batch WHERE (pyformat).

        Args:
            pairs: Lista de (owner, table_name).

        Returns:
            (sql_fragment, params_dict).
        """
        parts: list[str] = []
        params: dict[str, str] = {}
        for i, (owner, table_name) in enumerate(pairs):
            parts.append(f"(TABLE_SCHEMA = %(o{i})s AND TABLE_NAME = %(t{i})s)")
            params[f"o{i}"] = owner
            params[f"t{i}"] = table_name
        return " OR ".join(parts), params

    # ── Plano de execução ────────────────────────────────────────────

    def explain_plan(self, sql_text: str) -> list[tuple[str, dict | None]]:
        """EXPLAIN FORMAT=JSON — 1 step (MariaDB não usa PLAN_TABLE).

        params=None evita que PyMySQL tente mogrify de % no SQL (ex: DATE_FORMAT('%Y')).
        """
        return [("EXPLAIN FORMAT=JSON " + sql_text, None)]

    def runtime_plan(self, sql_id: str, child_number: int = 0) -> tuple[str, dict]:
        """Stub — inspect é bloqueado para MariaDB. Mantido para satisfazer a interface abstrata."""
        return (
            """
            SELECT DIGEST_TEXT AS plan_table_output
            FROM performance_schema.events_statements_summary_by_digest
            WHERE DIGEST = %(sql_id)s
            """,
            {"sql_id": sql_id},
        )

    # ── Sessão / instância ───────────────────────────────────────────

    def db_version(self) -> tuple[str, dict]:
        """Versão do MariaDB."""
        return ("SELECT VERSION()", {})

    def session_sid(self) -> tuple[str, dict]:
        """ID da conexão atual (equivalente ao SID Oracle)."""
        return ("SELECT CONNECTION_ID() AS sid", {})

    def optimizer_params(self) -> tuple[str, dict]:
        """Parâmetros relevantes do otimizador via GLOBAL_VARIABLES."""
        return (
            """
            SELECT VARIABLE_NAME AS name, VARIABLE_VALUE AS value
            FROM information_schema.GLOBAL_VARIABLES
            WHERE VARIABLE_NAME IN (
                'OPTIMIZER_SWITCH',
                'OPTIMIZER_USE_CONDITION_SELECTIVITY',
                'OPTIMIZER_SEARCH_DEPTH',
                'JOIN_BUFFER_SIZE',
                'SORT_BUFFER_SIZE',
                'TMP_TABLE_SIZE',
                'MAX_HEAP_TABLE_SIZE',
                'READ_BUFFER_SIZE',
                'READ_RND_BUFFER_SIZE',
                'EQ_RANGE_INDEX_DIVE_LIMIT',
                'HISTOGRAM_SIZE',
                'HISTOGRAM_TYPE',
                'USE_STAT_TABLES'
            )
            ORDER BY VARIABLE_NAME
            """,
            {},
        )

    def set_statistics_level(self, level: str) -> tuple[str, dict]:
        """NOP — MariaDB não possui STATISTICS_LEVEL equivalente ao Oracle."""
        return ("SELECT 1", {})

    def prev_sql_id(self) -> tuple[str, dict]:
        """Digest da query anterior via performance_schema.events_statements_history."""
        return (
            """
            SELECT DIGEST AS sql_id
            FROM performance_schema.events_statements_history
            WHERE THREAD_ID = (
                SELECT THREAD_ID FROM performance_schema.threads
                WHERE PROCESSLIST_ID = CONNECTION_ID()
            )
            ORDER BY EVENT_ID DESC
            LIMIT 1 OFFSET 1
            """,
            {},
        )

    def last_analyze_stats(self) -> tuple[str, dict]:
        """Stats da última query executada (ignora ANALYZE wrapper e queries internas)."""
        return (
            """
            SELECT DIGEST AS sql_id,
                   1 AS executions,
                   TIMER_WAIT / 1000000000 AS avg_elapsed_ms,
                   0 AS avg_cpu_ms,
                   0 AS avg_buffer_gets,
                   ROWS_SENT AS avg_rows_per_exec,
                   ROWS_SENT AS rows_processed,
                   SUM_SORT_ROWS AS sorts,
                   ROWS_EXAMINED AS disk_reads
            FROM performance_schema.events_statements_history
            WHERE THREAD_ID = (
                SELECT THREAD_ID FROM performance_schema.threads
                WHERE PROCESSLIST_ID = CONNECTION_ID()
            )
            AND DIGEST IS NOT NULL
            AND SQL_TEXT NOT LIKE 'ANALYZE%%'
            AND SQL_TEXT NOT LIKE 'SELECT%%THREAD_ID%%'
            AND SQL_TEXT NOT LIKE 'SELECT%%CONNECTION_ID%%'
            ORDER BY EVENT_ID DESC
            LIMIT 1
            """,
            {},
        )

    def session_wait_events(self, session_id: int) -> tuple[str, dict]:
        """Wait events da sessão (top 10 por tempo) via performance_schema."""
        return (
            """
            SELECT EVENT_NAME AS event,
                   COUNT_STAR AS total_waits,
                   SUM_TIMER_WAIT / 1000000 AS time_waited_micro,
                   SUM_TIMER_WAIT / 1000000000 AS time_waited_ms,
                   CASE WHEN COUNT_STAR > 0
                        THEN SUM_TIMER_WAIT / COUNT_STAR / 1000000
                        ELSE 0 END AS average_wait
            FROM performance_schema.events_waits_summary_by_thread_by_event_name
            WHERE THREAD_ID = (
                SELECT THREAD_ID FROM performance_schema.threads
                WHERE PROCESSLIST_ID = %(session_id)s
            )
            AND COUNT_STAR > 0
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT 10
            """,
            {"session_id": session_id},
        )

    # ── Objetos (tabela/view) ────────────────────────────────────────

    def object_type(self, owner: str, object_name: str) -> tuple[str, dict]:
        """Tipo do objeto (TABLE ou VIEW) via information_schema.TABLES."""
        return (
            """
            SELECT CASE TABLE_TYPE
                       WHEN 'BASE TABLE' THEN 'TABLE'
                       WHEN 'VIEW' THEN 'VIEW'
                       WHEN 'SYSTEM VIEW' THEN 'VIEW'
                       ELSE TABLE_TYPE
                   END AS object_type
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %(owner)s
            AND TABLE_NAME = %(object_name)s
            """,
            {"owner": owner, "object_name": object_name},
        )

    def table_ddl(self, owner: str, table_name: str) -> tuple[str, dict]:
        """DDL da tabela via SHOW CREATE TABLE.

        Nota: SHOW CREATE TABLE retorna coluna 'Create Table' (não 'ddl').
        O mapeamento de colunas é resolvido no adapter/collector (T19).
        """
        safe_owner = self._sanitize_identifier(owner)
        safe_table = self._sanitize_identifier(table_name)
        return (f"SHOW CREATE TABLE `{safe_owner}`.`{safe_table}`", {})

    def function_ddl(self, owner: str, function_name: str) -> tuple[str, dict]:
        """DDL da função via SHOW CREATE FUNCTION.

        Nota: SHOW CREATE FUNCTION retorna coluna 'Create Function' (não 'ddl').
        O mapeamento de colunas é resolvido no adapter/collector (T19).
        """
        safe_owner = self._sanitize_identifier(owner)
        safe_func = self._sanitize_identifier(function_name)
        return (f"SHOW CREATE FUNCTION `{safe_owner}`.`{safe_func}`", {})

    def table_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Estatísticas gerais da tabela via information_schema.TABLES."""
        return (
            """
            SELECT TABLE_NAME AS table_name,
                   TABLE_ROWS AS num_rows,
                   DATA_LENGTH DIV 16384 AS blocks,
                   AVG_ROW_LENGTH AS avg_row_len,
                   UPDATE_TIME AS last_analyzed,
                   TABLE_ROWS AS sample_size,
                   CASE WHEN CREATE_OPTIONS LIKE '%%partitioned%%'
                        THEN 'YES' ELSE 'NO' END AS partitioned,
                   CASE ENGINE WHEN 'MEMORY' THEN 'Y' ELSE 'N' END AS temporary,
                   1 AS degree,
                   ROW_FORMAT AS compression
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %(owner)s
            AND TABLE_NAME = %(table_name)s
            """,
            {"owner": owner, "table_name": table_name},
        )

    def column_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Estatísticas de colunas via information_schema.COLUMNS."""
        return (
            """
            SELECT COLUMN_NAME AS column_name,
                   COLUMN_TYPE AS data_type,
                   IFNULL(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) AS data_length,
                   CASE IS_NULLABLE WHEN 'YES' THEN 'Y' ELSE 'N' END AS nullable,
                   NULL AS num_distinct,
                   NULL AS num_nulls,
                   NULL AS density,
                   'NONE' AS histogram,
                   0 AS num_buckets,
                   NULL AS last_analyzed,
                   NULL AS sample_size,
                   COLUMN_DEFAULT AS data_default
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %(owner)s
            AND TABLE_NAME = %(table_name)s
            ORDER BY ORDINAL_POSITION
            """,
            {"owner": owner, "table_name": table_name},
        )

    def indexes(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Índices da tabela via information_schema.STATISTICS."""
        return (
            """
            SELECT INDEX_NAME AS index_name,
                   CASE WHEN INDEX_TYPE = 'BTREE' THEN 'BTREE'
                        WHEN INDEX_TYPE = 'FULLTEXT' THEN 'FULLTEXT'
                        WHEN INDEX_TYPE = 'SPATIAL' THEN 'SPATIAL'
                        ELSE INDEX_TYPE END AS index_type,
                   CASE NON_UNIQUE WHEN 0 THEN 'UNIQUE' ELSE 'NONUNIQUE' END AS uniqueness,
                   'VALID' AS status,
                   CARDINALITY AS num_rows,
                   CARDINALITY AS distinct_keys,
                   NULL AS clustering_factor,
                   NULL AS last_analyzed,
                   NULL AS blevel,
                   NULL AS leaf_blocks,
                   GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %(owner)s
            AND TABLE_NAME = %(table_name)s
            GROUP BY INDEX_NAME, INDEX_TYPE, NON_UNIQUE, CARDINALITY
            ORDER BY INDEX_NAME
            """,
            {"owner": owner, "table_name": table_name},
        )

    def constraints(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Constraints (PK, FK, unique) via TABLE_CONSTRAINTS + KEY_COLUMN_USAGE."""
        return (
            """
            SELECT tc.CONSTRAINT_NAME AS constraint_name,
                   CASE tc.CONSTRAINT_TYPE
                       WHEN 'PRIMARY KEY' THEN 'P'
                       WHEN 'FOREIGN KEY' THEN 'R'
                       WHEN 'UNIQUE' THEN 'U'
                       WHEN 'CHECK' THEN 'C'
                       ELSE tc.CONSTRAINT_TYPE
                   END AS constraint_type,
                   'ENABLED' AS status,
                   'VALIDATED' AS validated,
                   rc.UNIQUE_CONSTRAINT_NAME AS r_constraint_name,
                   kcu2.TABLE_NAME AS r_table_name,
                   kcu2.TABLE_SCHEMA AS r_owner,
                   GROUP_CONCAT(kcu.COLUMN_NAME
                       ORDER BY kcu.ORDINAL_POSITION) AS columns
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE kcu
                ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND kcu.TABLE_NAME = tc.TABLE_NAME
            LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
                AND kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                AND kcu2.ORDINAL_POSITION = 1
            WHERE tc.TABLE_SCHEMA = %(owner)s
            AND tc.TABLE_NAME = %(table_name)s
            GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE,
                     rc.UNIQUE_CONSTRAINT_NAME, kcu2.TABLE_NAME, kcu2.TABLE_SCHEMA
            ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME
            """,
            {"owner": owner, "table_name": table_name},
        )

    def histograms(self, owner: str, table_name: str, column_name: str) -> tuple[str, dict]:
        """Histograma via information_schema.COLUMN_STATISTICS (MariaDB 10.8+)."""
        return (
            """
            SELECT COLUMN_NAME AS column_name,
                   HISTOGRAM AS histogram_data
            FROM information_schema.COLUMN_STATISTICS
            WHERE SCHEMA_NAME = %(owner)s
            AND TABLE_NAME = %(table_name)s
            AND COLUMN_NAME = %(column_name)s
            """,
            {
                "owner": owner,
                "table_name": table_name,
                "column_name": column_name,
            },
        )

    def table_partitions(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Info de particionamento via information_schema.PARTITIONS."""
        return (
            """
            SELECT PARTITION_NAME AS partition_name,
                   PARTITION_ORDINAL_POSITION AS partition_position,
                   PARTITION_DESCRIPTION AS high_value,
                   TABLE_ROWS AS num_rows,
                   DATA_LENGTH DIV 16384 AS blocks,
                   UPDATE_TIME AS last_analyzed
            FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = %(owner)s
            AND TABLE_NAME = %(table_name)s
            AND PARTITION_NAME IS NOT NULL
            ORDER BY PARTITION_ORDINAL_POSITION
            """,
            {"owner": owner, "table_name": table_name},
        )

    def index_to_table_map(self, owner: str) -> tuple[str, dict]:
        """Mapa index_name → table_name para um schema via STATISTICS."""
        return (
            """
            SELECT DISTINCT INDEX_NAME AS index_name,
                            TABLE_NAME AS table_name
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %(owner)s
            """,
            {"owner": owner},
        )

    # ── Runtime stats ────────────────────────────────────────────────

    def sql_runtime_stats(self, sql_id: str) -> tuple[str, dict]:
        """Métricas de execução via performance_schema.events_statements_summary_by_digest."""
        return (
            """
            SELECT DIGEST AS sql_id,
                   0 AS child_number,
                   0 AS plan_hash_value,
                   COUNT_STAR AS executions,
                   SUM_TIMER_WAIT / 1000000 AS elapsed_time,
                   0 AS cpu_time,
                   SUM_NO_INDEX_USED + SUM_NO_GOOD_INDEX_USED AS buffer_gets,
                   SUM_SORT_MERGE_PASSES AS disk_reads,
                   SUM_ROWS_SENT AS rows_processed,
                   SUM_SORT_ROWS AS sorts,
                   SUM_ROWS_SENT AS fetches,
                   0 AS parse_calls,
                   0 AS loads,
                   0 AS invalidations,
                   0 AS version_count,
                   CASE WHEN COUNT_STAR > 0
                        THEN SUM_TIMER_WAIT / COUNT_STAR / 1000000000
                        ELSE 0 END AS avg_elapsed_ms,
                   0 AS avg_cpu_ms,
                   CASE WHEN COUNT_STAR > 0
                        THEN (SUM_NO_INDEX_USED + SUM_NO_GOOD_INDEX_USED) / COUNT_STAR
                        ELSE 0 END AS avg_buffer_gets,
                   CASE WHEN COUNT_STAR > 0
                        THEN SUM_ROWS_SENT / COUNT_STAR
                        ELSE 0 END AS avg_rows_per_exec
            FROM performance_schema.events_statements_summary_by_digest
            WHERE DIGEST = %(sql_id)s
            """,
            {"sql_id": sql_id},
        )

    def sql_text_by_id(self, sql_id: str) -> tuple[str, dict]:
        """Texto do SQL via performance_schema (DIGEST_TEXT)."""
        return (
            """
            SELECT DIGEST_TEXT AS sql_fulltext
            FROM performance_schema.events_statements_summary_by_digest
            WHERE DIGEST = %(sql_id)s
            """,
            {"sql_id": sql_id},
        )

    def sql_text_original(self, digest: str) -> tuple[str, dict]:
        """Texto SQL original de events_statements_history_long."""
        return (
            """
            SELECT SQL_TEXT
            FROM performance_schema.events_statements_history_long
            WHERE DIGEST = %(digest)s
            ORDER BY EVENT_ID DESC
            LIMIT 1
            """,
            {"digest": digest},
        )

    def setup_consumers(self) -> tuple[str, dict]:
        """Estado dos consumers relevantes do performance_schema."""
        return (
            """
            SELECT NAME, ENABLED
            FROM performance_schema.setup_consumers
            WHERE NAME IN (
                'statements_digest',
                'events_statements_history',
                'events_statements_history_long'
            )
            """,
            {},
        )

    # ── Segurança ────────────────────────────────────────────────────

    def dangerous_privileges(self) -> tuple[str, dict]:
        """Privilégios perigosos do user atual via information_schema."""
        return (
            """
            SELECT PRIVILEGE_TYPE AS privilege
            FROM information_schema.USER_PRIVILEGES
            WHERE GRANTEE = CONCAT("'", CURRENT_USER(), "'")
            AND PRIVILEGE_TYPE IN (
                'INSERT', 'UPDATE', 'DELETE', 'DROP',
                'ALTER', 'CREATE', 'GRANT OPTION',
                'SUPER', 'SHUTDOWN', 'FILE',
                'RELOAD', 'PROCESS'
            )
            """,
            {},
        )

    def dangerous_roles(self) -> tuple[str, dict]:
        """Stub — MariaDB roles são menos comuns que Oracle."""
        return ("SELECT 1 WHERE 1=0", {})

    # ── Batch (múltiplas tabelas) ────────────────────────────────────

    def batch_table_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Estatísticas de múltiplas tabelas em uma query."""
        if not pairs:
            return ("SELECT 1 WHERE 1=0", {})
        where_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT TABLE_SCHEMA AS owner, TABLE_NAME AS table_name,
                   TABLE_ROWS AS num_rows,
                   DATA_LENGTH DIV 16384 AS blocks,
                   AVG_ROW_LENGTH AS avg_row_len,
                   UPDATE_TIME AS last_analyzed,
                   TABLE_ROWS AS sample_size,
                   CASE WHEN CREATE_OPTIONS LIKE '%%partitioned%%'
                        THEN 'YES' ELSE 'NO' END AS partitioned,
                   CASE ENGINE WHEN 'MEMORY' THEN 'Y' ELSE 'N' END AS temporary,
                   1 AS degree,
                   ROW_FORMAT AS compression
            FROM information_schema.TABLES
            WHERE {where_clause}
            """,  # noqa: S608
            params,
        )

    def batch_column_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Estatísticas de colunas de múltiplas tabelas em uma query."""
        if not pairs:
            return ("SELECT 1 WHERE 1=0", {})
        where_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT TABLE_SCHEMA AS owner, TABLE_NAME AS table_name,
                   COLUMN_NAME AS column_name,
                   COLUMN_TYPE AS data_type,
                   IFNULL(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) AS data_length,
                   CASE IS_NULLABLE WHEN 'YES' THEN 'Y' ELSE 'N' END AS nullable,
                   NULL AS num_distinct,
                   NULL AS num_nulls,
                   NULL AS density,
                   'NONE' AS histogram,
                   0 AS num_buckets,
                   NULL AS last_analyzed,
                   NULL AS sample_size,
                   COLUMN_DEFAULT AS data_default
            FROM information_schema.COLUMNS
            WHERE {where_clause}
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """,  # noqa: S608
            params,
        )

    def batch_indexes(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Índices de múltiplas tabelas em uma query."""
        if not pairs:
            return ("SELECT 1 WHERE 1=0", {})
        where_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT TABLE_SCHEMA AS owner, TABLE_NAME AS table_name,
                   INDEX_NAME AS index_name,
                   CASE WHEN INDEX_TYPE = 'BTREE' THEN 'BTREE'
                        WHEN INDEX_TYPE = 'FULLTEXT' THEN 'FULLTEXT'
                        WHEN INDEX_TYPE = 'SPATIAL' THEN 'SPATIAL'
                        ELSE INDEX_TYPE END AS index_type,
                   CASE NON_UNIQUE WHEN 0 THEN 'UNIQUE' ELSE 'NONUNIQUE' END AS uniqueness,
                   'VALID' AS status,
                   CARDINALITY AS num_rows,
                   CARDINALITY AS distinct_keys,
                   NULL AS clustering_factor,
                   NULL AS last_analyzed,
                   NULL AS blevel,
                   NULL AS leaf_blocks,
                   GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns
            FROM information_schema.STATISTICS
            WHERE {where_clause}
            GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, INDEX_TYPE,
                     NON_UNIQUE, CARDINALITY
            ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
            """,  # noqa: S608
            params,
        )

    def batch_constraints(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Constraints de múltiplas tabelas em uma query."""
        if not pairs:
            return ("SELECT 1 WHERE 1=0", {})
        # Qualified WHERE para evitar ambiguidade com JOINs (tc.TABLE_SCHEMA)
        parts: list[str] = []
        params: dict[str, str] = {}
        for i, (owner, table_name) in enumerate(pairs):
            parts.append(f"(tc.TABLE_SCHEMA = %(o{i})s AND tc.TABLE_NAME = %(t{i})s)")
            params[f"o{i}"] = owner
            params[f"t{i}"] = table_name
        where_clause = " OR ".join(parts)
        return (
            f"""
            SELECT tc.TABLE_SCHEMA AS owner, tc.TABLE_NAME AS table_name,
                   tc.CONSTRAINT_NAME AS constraint_name,
                   CASE tc.CONSTRAINT_TYPE
                       WHEN 'PRIMARY KEY' THEN 'P'
                       WHEN 'FOREIGN KEY' THEN 'R'
                       WHEN 'UNIQUE' THEN 'U'
                       WHEN 'CHECK' THEN 'C'
                       ELSE tc.CONSTRAINT_TYPE
                   END AS constraint_type,
                   'ENABLED' AS status,
                   'VALIDATED' AS validated,
                   rc.UNIQUE_CONSTRAINT_NAME AS r_constraint_name,
                   kcu2.TABLE_NAME AS r_table_name,
                   kcu2.TABLE_SCHEMA AS r_owner,
                   GROUP_CONCAT(kcu.COLUMN_NAME
                       ORDER BY kcu.ORDINAL_POSITION) AS columns
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE kcu
                ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND kcu.TABLE_NAME = tc.TABLE_NAME
            LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
                AND kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                AND kcu2.ORDINAL_POSITION = 1
            WHERE ({where_clause})
            GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME,
                     tc.CONSTRAINT_TYPE, rc.UNIQUE_CONSTRAINT_NAME,
                     kcu2.TABLE_NAME, kcu2.TABLE_SCHEMA
            ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME,
                     tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME
            """,  # noqa: S608
            params,
        )


# ── MariaDBPlanParser ────────────────────────────────────────────────


class MariaDBPlanParser(PlanParser):
    """Parser de planos MariaDB (EXPLAIN/ANALYZE FORMAT=JSON).

    Parseia JSON de plano MariaDB em lista plana de PlanBlock via DFS.
    MariaDB não reporta buffers/reads no plano — esses campos ficam None.
    """

    def parse_plan(self, plan_lines: list[str]) -> list[PlanBlock]:
        """Parseia JSON do plano MariaDB em lista de PlanBlock."""
        raw = "".join(plan_lines).strip()
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return []

        if not isinstance(data, dict):
            return []

        counter = [0]
        blocks: list[PlanBlock] = []
        query_block = data.get("query_block")
        if query_block:
            blocks.extend(self._walk_node(query_block, depth=0, counter=counter))
        return blocks

    def _walk_node(self, node: dict, depth: int, counter: list[int]) -> list[PlanBlock]:
        """Percorre recursivamente o JSON do plano MariaDB via DFS."""
        from sqlmentor.report import PlanBlock

        blocks: list[PlanBlock] = []
        if not isinstance(node, dict):
            return blocks

        # table node — operação concreta de acesso
        if "table" in node:
            tbl = node["table"]
            counter[0] += 1
            blocks.append(
                PlanBlock(
                    id=str(counter[0]),
                    operation=tbl.get("access_type", "UNKNOWN").upper(),
                    name=tbl.get("table_name", ""),
                    starts=tbl.get("r_loops", 0),
                    e_rows=tbl.get("rows_examined_per_scan", tbl.get("rows", 0)),
                    a_rows=tbl.get("r_rows", 0),
                    a_time_ms=tbl.get("r_total_time_ms", 0.0),
                    buffers=None,
                    reads=None,
                    indent=depth,
                )
            )
            # Recurse subqueries attached to this table
            for key in ("subqueries", "attached_subqueries"):
                for sq in tbl.get(key, []):
                    blocks.extend(self._walk_node(sq, depth + 1, counter))
            if "materialized_from_subquery" in tbl:
                mat = tbl["materialized_from_subquery"]
                qb = mat.get("query_block", mat)
                blocks.extend(self._walk_node(qb, depth + 1, counter))
            return blocks

        # nested_loop — array of child nodes
        if "nested_loop" in node:
            for item in node["nested_loop"]:
                blocks.extend(self._walk_node(item, depth, counter))
            # Also process other keys besides nested_loop
            for key in ("subqueries", "attached_subqueries"):
                for sq in node.get(key, []):
                    blocks.extend(self._walk_node(sq, depth + 1, counter))
            if "ordering_operation" in node:
                blocks.extend(self._walk_ordering(node["ordering_operation"], depth, counter))
            return blocks

        # ordering_operation — structural node
        if "ordering_operation" in node:
            blocks.extend(self._walk_ordering(node["ordering_operation"], depth, counter))
            # Process other children at this level
            for key in ("subqueries", "attached_subqueries"):
                for sq in node.get(key, []):
                    blocks.extend(self._walk_node(sq, depth + 1, counter))
            return blocks

        # grouping_operation — structural node
        if "grouping_operation" in node:
            grp = node["grouping_operation"]
            counter[0] += 1
            blocks.append(
                PlanBlock(
                    id=str(counter[0]),
                    operation="GROUPING",
                    name=grp.get("using_temporary_table", ""),
                    starts=grp.get("r_loops", 0),
                    e_rows=grp.get("rows", 0),
                    a_rows=grp.get("r_rows", 0),
                    a_time_ms=grp.get("r_total_time_ms", 0.0),
                    buffers=None,
                    reads=None,
                    indent=depth,
                )
            )
            # Recurse nested_loop or table inside grouping
            blocks.extend(self._walk_children(grp, depth + 1, counter))
            return blocks

        # duplicates_removal — structural node
        if "duplicates_removal" in node:
            dup = node["duplicates_removal"]
            counter[0] += 1
            blocks.append(
                PlanBlock(
                    id=str(counter[0]),
                    operation="DUPLICATES REMOVAL",
                    name="",
                    starts=dup.get("r_loops", 0),
                    e_rows=dup.get("rows", 0),
                    a_rows=dup.get("r_rows", 0),
                    a_time_ms=dup.get("r_total_time_ms", 0.0),
                    buffers=None,
                    reads=None,
                    indent=depth,
                )
            )
            blocks.extend(self._walk_children(dup, depth + 1, counter))
            return blocks

        # union_result — structural node
        if "union_result" in node:
            ur = node["union_result"]
            counter[0] += 1
            blocks.append(
                PlanBlock(
                    id=str(counter[0]),
                    operation="UNION RESULT",
                    name=ur.get("table_name", ""),
                    starts=ur.get("r_loops", 0),
                    e_rows=0,
                    a_rows=ur.get("r_rows", 0),
                    a_time_ms=ur.get("r_total_time_ms", 0.0),
                    buffers=None,
                    reads=None,
                    indent=depth,
                )
            )
            for qs in ur.get("query_specifications", []):
                blocks.extend(self._walk_node(qs, depth + 1, counter))
            return blocks

        # materialized — wraps a subquery materialized into a temp table
        if "materialized" in node:
            mat = node["materialized"]
            qb = mat.get("query_block", mat)
            blocks.extend(self._walk_node(qb, depth + 1, counter))
            return blocks

        # query_block — container, recurse children
        if "select_id" in node:
            blocks.extend(self._walk_children(node, depth, counter))
            return blocks

        # Generic: try to find known children
        blocks.extend(self._walk_children(node, depth, counter))
        return blocks

    def _walk_ordering(self, ord_node: dict, depth: int, counter: list[int]) -> list[PlanBlock]:
        """Processa nó ordering_operation."""
        from sqlmentor.report import PlanBlock

        blocks: list[PlanBlock] = []
        counter[0] += 1
        blocks.append(
            PlanBlock(
                id=str(counter[0]),
                operation="ORDERING",
                name=ord_node.get("using_filesort", ""),
                starts=ord_node.get("r_loops", 0),
                e_rows=ord_node.get("rows", 0),
                a_rows=ord_node.get("r_rows", 0),
                a_time_ms=ord_node.get("r_total_time_ms", 0.0),
                buffers=None,
                reads=None,
                indent=depth,
            )
        )
        blocks.extend(self._walk_children(ord_node, depth + 1, counter))
        return blocks

    def _walk_children(self, node: dict, depth: int, counter: list[int]) -> list[PlanBlock]:
        """Percorre filhos conhecidos de um nó genérico."""
        blocks: list[PlanBlock] = []
        if "nested_loop" in node:
            for item in node["nested_loop"]:
                blocks.extend(self._walk_node(item, depth, counter))
        if "table" in node:
            blocks.extend(self._walk_node({"table": node["table"]}, depth, counter))
        for key in ("subqueries", "attached_subqueries"):
            for sq in node.get(key, []):
                blocks.extend(self._walk_node(sq, depth + 1, counter))
        if "ordering_operation" in node:
            blocks.extend(self._walk_ordering(node["ordering_operation"], depth, counter))
        if "grouping_operation" in node:
            blocks.extend(
                self._walk_node({"grouping_operation": node["grouping_operation"]}, depth, counter)
            )
        if "duplicates_removal" in node:
            blocks.extend(
                self._walk_node({"duplicates_removal": node["duplicates_removal"]}, depth, counter)
            )
        if "union_result" in node:
            blocks.extend(self._walk_node({"union_result": node["union_result"]}, depth, counter))
        if "materialized" in node:
            blocks.extend(self._walk_node({"materialized": node["materialized"]}, depth, counter))
        if "materialized_from_subquery" in node:
            mat = node["materialized_from_subquery"]
            qb = mat.get("query_block", mat)
            blocks.extend(self._walk_node(qb, depth + 1, counter))
        if "query_block" in node:
            blocks.extend(self._walk_node(node["query_block"], depth + 1, counter))
        return blocks

    def is_runtime_plan(self, plan_lines: list[str]) -> bool:
        """Detecta se o plano contém estatísticas reais (r_rows) — indica ANALYZE."""
        raw = "".join(plan_lines).strip()
        if not raw:
            return False
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return False
        return self._has_runtime_key(data)

    @staticmethod
    def _has_runtime_key(obj: Any) -> bool:
        """Busca recursivamente a chave 'r_rows' no JSON."""
        if isinstance(obj, dict):
            if "r_rows" in obj:
                return True
            return any(MariaDBPlanParser._has_runtime_key(v) for v in obj.values())
        if isinstance(obj, list):
            return any(MariaDBPlanParser._has_runtime_key(item) for item in obj)
        return False


# ── MariaDBAdapter ───────────────────────────────────────────────────


class MariaDBAdapter(DatabaseAdapter):
    """Adapter concreto para MariaDB via PyMySQL."""

    def __init__(self) -> None:
        self._query_builder: MariaDBQueryBuilder | None = None
        self._plan_parser: MariaDBPlanParser | None = None

    @property
    def db_type(self) -> str:
        return "mariadb"

    @property
    def fold_case(self) -> bool:
        return False

    @property
    def query_builder(self) -> MariaDBQueryBuilder:
        if self._query_builder is None:
            self._query_builder = MariaDBQueryBuilder()
        return self._query_builder

    @property
    def plan_parser(self) -> MariaDBPlanParser:
        if self._plan_parser is None:
            self._plan_parser = MariaDBPlanParser()
        return self._plan_parser

    def connect(self, config: dict[str, Any], timeout: int | None = None) -> pymysql.Connection:
        """Abre conexão MariaDB via PyMySQL."""
        effective_timeout = timeout if timeout is not None else config.get("timeout", 180)

        conn = pymysql.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config.get("database", config.get("service")),
            connect_timeout=effective_timeout,
            charset="utf8mb4",
        )
        return conn

    def test_connection(self, config: dict[str, Any]) -> bool:
        """Testa conexão (connect + SELECT 1 + close)."""
        try:
            conn = self.connect(config)
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
            finally:
                conn.close()
        except Exception:
            return False

    def validate_privileges(self, conn: Any) -> dict[str, list[str]]:
        """Valida privilégios do user conectado.

        Retorna dict com listas (NÃO levanta PermissionError — quem decide é o connector).
        """
        qb = self.query_builder
        cursor = conn.cursor()
        result: dict[str, list[str]] = {
            "dangerous_privileges": [],
            "dangerous_roles": [],
        }

        try:
            sql, params = qb.dangerous_privileges()
            cursor.execute(sql, params or None)
            result["dangerous_privileges"] = [row[0] for row in cursor]

            sql, params = qb.dangerous_roles()
            cursor.execute(sql, params or None)
            result["dangerous_roles"] = [row[0] for row in cursor]
        finally:
            cursor.close()

        return result

    def diagnose_connection(self, config: dict[str, Any]) -> dict[str, Any]:
        """Diagnóstico: versão, performance_schema."""
        conn = pymysql.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config.get("database", config.get("service")),
            connect_timeout=config.get("timeout", 30),
            charset="utf8mb4",
        )

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            row = cursor.fetchone()
            version = row[0] if row else "unknown"

            cursor.execute("SELECT @@performance_schema")
            row = cursor.fetchone()
            perf_schema = bool(row[0]) if row else False

            cursor.execute("SELECT DATABASE()")
            row = cursor.fetchone()
            current_db = row[0] if row else "?"

            match = re.search(r"(\d+)", version)
            major = int(match.group(1)) if match else 0

            return {
                "status": "ok",
                "version": version,
                "major_version": str(major),
                "performance_schema": str(perf_schema),
                "schema": current_db,
            }
        finally:
            conn.close()

    # Mapeamento de colunas SHOW CREATE → "ddl" (compatível com Oracle DDL flow)
    _DDL_COL_REMAP: ClassVar[dict[str, str]] = {
        "create table": "ddl",
        "create view": "ddl",
        "create function": "ddl",
        "create procedure": "ddl",
    }

    def execute_query(self, cursor: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Executa query e retorna resultados como lista de dicts.

        Sem LOB handling — PyMySQL já retorna tipos Python nativos.
        Remapeia colunas de SHOW CREATE (Create Table → ddl) para compatibilidade.
        """
        cursor.execute(sql, params or None)
        if cursor.description is None:
            return []
        columns = [col[0].lower() for col in cursor.description]
        columns = [self._DDL_COL_REMAP.get(c, c) for c in columns]
        rows = []
        for row in cursor:
            rows.append(dict(zip(columns, row, strict=False)))
        return rows

    def close_connection(self, conn: Any) -> None:
        """Fecha a conexão MariaDB."""
        conn.close()

    def check_deps(self) -> list[dict[str, str]]:
        """Verifica se PyMySQL está instalado."""
        results: list[dict[str, str]] = []
        try:
            import importlib.metadata

            ver = importlib.metadata.version("PyMySQL")
            results.append({"name": "PyMySQL", "status": "ok", "detail": ver})
        except Exception:
            results.append(
                {
                    "name": "PyMySQL",
                    "status": "missing",
                    "detail": "pip install PyMySQL",
                }
            )
        return results


# ── Registro ────────────────────────────────────────────────────────

register_adapter("mariadb", MariaDBAdapter)
