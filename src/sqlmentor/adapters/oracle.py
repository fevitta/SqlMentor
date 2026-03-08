"""
Adapter Oracle — implementação concreta de DatabaseAdapter.

Encapsula toda lógica específica do oracledb: conexão thin/thick,
thick mode fallback (DPY-3010), validação de privilégios, diagnóstico.
OracleQueryBuilder contém todas as queries Oracle parametrizadas.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import oracledb

from sqlmentor.adapters import register_adapter
from sqlmentor.adapters.base import DatabaseAdapter, PlanParser, QueryBuilder

if TYPE_CHECKING:
    from sqlmentor.report import PlanBlock

logger = logging.getLogger(__name__)

# ── Thick mode state (módulo-level) ─────────────────────────────────

_thick_mode_initialized: bool = False


def _init_thick_mode_if_available() -> None:
    """Tenta ativar thick mode se o Oracle Instant Client estiver disponível.

    Não explode se não encontrar — apenas re-raise o erro original
    com uma mensagem útil sobre como resolver.
    """
    global _thick_mode_initialized
    if _thick_mode_initialized:
        return
    try:
        oracledb.init_oracle_client()
        _thick_mode_initialized = True
        logger.info("oracledb: thick mode ativado via Oracle Instant Client")
    except oracledb.ProgrammingError:
        raise RuntimeError(
            "Este banco Oracle é antigo demais para o modo thin do oracledb.\n"
            "Opções:\n"
            "  1. Instale o Oracle Instant Client e adicione ao PATH\n"
            "     https://www.oracle.com/database/technologies/instant-client.html\n"
            "  2. Atualize o banco para Oracle 12c+ (suporta thin mode nativo)"
        )


def check_thick_mode_available() -> dict[str, str]:
    """Verifica se o Oracle Instant Client está disponível no ambiente.

    Retorna dict com available (bool como str) e detalhes.
    """
    global _thick_mode_initialized
    if _thick_mode_initialized:
        return {"available": "True", "detail": "Thick mode já inicializado"}
    try:
        oracledb.init_oracle_client()
        _thick_mode_initialized = True
        return {"available": "True", "detail": "Oracle Instant Client encontrado"}
    except Exception as e:
        return {"available": "False", "detail": str(e)}


# ── OracleQueryBuilder ──────────────────────────────────────────────


_SQL_ID_RE = re.compile(r"^[a-z0-9]{8,16}$")


class OracleQueryBuilder(QueryBuilder):
    """Queries Oracle 11g+ parametrizadas.

    Cada método retorna tuple[str, dict] (sql, params) pronta para cursor.execute(),
    exceto explain_plan que retorna list[tuple[str, dict]] (múltiplos steps).
    """

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def validate_sql_id(sql_id: str) -> None:
        """Valida formato do sql_id Oracle (alfanumérico lowercase, 8-16 chars)."""
        if not _SQL_ID_RE.match(sql_id):
            raise ValueError(
                f"sql_id inválido: {sql_id!r}. "
                "Formato esperado: 8-16 caracteres alfanuméricos lowercase (ex: 'abc123def4567')."
            )

    @staticmethod
    def build_tuple_in_clause(
        pairs: list[tuple[str, str]],
    ) -> tuple[str, dict[str, str]]:
        """Constrói cláusula (owner, table_name) IN ((:o0,:t0), ...) com params.

        Args:
            pairs: Lista de (owner, table_name).

        Returns:
            (sql_fragment, params_dict).
        """
        parts = []
        params: dict[str, str] = {}
        for i, (owner, table_name) in enumerate(pairs):
            parts.append(f"(:o{i}, :t{i})")
            params[f"o{i}"] = owner.upper()
            params[f"t{i}"] = table_name.upper()
        return ", ".join(parts), params

    # ── Plano de execução ────────────────────────────────────────────

    def explain_plan(self, sql_text: str) -> list[tuple[str, dict]]:
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

    def runtime_plan(self, sql_id: str, child_number: int = 0) -> tuple[str, dict]:
        """Plano real com ALLSTATS LAST via sql_id explícito (sem Outline pra reduzir ruído).

        Nota: DBMS_XPLAN.DISPLAY_CURSOR não aceita bind variables nos parâmetros
        sql_id e child_number — usa f-string com validação prévia do formato.
        """
        self.validate_sql_id(sql_id)
        return (
            f"""
            SELECT plan_table_output
            FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(
                '{sql_id}', {child_number}, 'ALLSTATS LAST +PEEKED_BINDS -OUTLINE'
            ))
            """,  # noqa: S608
            {},
        )

    # ── Sessão / instância ───────────────────────────────────────────

    def db_version(self) -> tuple[str, dict]:
        """Versão do banco Oracle."""
        return (
            "SELECT banner FROM v$version WHERE ROWNUM = 1",
            {},
        )

    def optimizer_params(self) -> tuple[str, dict]:
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

    def session_wait_events(self, session_id: int) -> tuple[str, dict]:
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
            {"sid": session_id},
        )

    # ── Objetos (tabela/view) ────────────────────────────────────────

    def object_type(self, owner: str, object_name: str) -> tuple[str, dict]:
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

    def table_ddl(self, owner: str, table_name: str) -> tuple[str, dict]:
        """DDL completo da tabela ou view via DBMS_METADATA."""
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

    def function_ddl(self, owner: str, function_name: str) -> tuple[str, dict]:
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

    def table_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
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

    def column_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
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

    def indexes(self, owner: str, table_name: str) -> tuple[str, dict]:
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

    def constraints(self, owner: str, table_name: str) -> tuple[str, dict]:
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

    def histograms(self, owner: str, table_name: str, column_name: str) -> tuple[str, dict]:
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

    def table_partitions(self, owner: str, table_name: str) -> tuple[str, dict]:
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

    def index_to_table_map(self, owner: str) -> tuple[str, dict]:
        """Mapa index_name → table_name para um schema."""
        return (
            """
            SELECT index_name, table_name
            FROM all_indexes
            WHERE owner = :owner
            """,
            {"owner": owner.upper()},
        )

    # ── Runtime stats ────────────────────────────────────────────────

    def sql_runtime_stats(self, sql_id: str) -> tuple[str, dict]:
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

    def sql_text_by_id(self, sql_id: str) -> tuple[str, dict]:
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

    # ── Segurança ────────────────────────────────────────────────────

    def dangerous_privileges(self) -> tuple[str, dict]:
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

    def dangerous_roles(self) -> tuple[str, dict]:
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

    # ── Batch (múltiplas tabelas) ────────────────────────────────────

    def batch_table_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Estatísticas de múltiplas tabelas em uma query."""
        if not pairs:
            return "SELECT 1 FROM DUAL WHERE 1=0", {}
        in_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT owner, table_name, num_rows, blocks, avg_row_len,
                   last_analyzed, sample_size, partitioned, temporary,
                   degree, compression
            FROM all_tables
            WHERE (owner, table_name) IN ({in_clause})
            """,  # noqa: S608
            params,
        )

    def batch_column_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Estatísticas de colunas de múltiplas tabelas em uma query."""
        if not pairs:
            return "SELECT 1 FROM DUAL WHERE 1=0", {}
        in_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT c.owner, c.table_name,
                   c.column_name, c.data_type, c.data_length, c.nullable,
                   s.num_distinct, s.num_nulls, s.density, s.histogram,
                   s.num_buckets, s.last_analyzed, s.sample_size,
                   c.data_default
            FROM all_tab_columns c
            LEFT JOIN all_tab_col_statistics s
                ON s.owner = c.owner
                AND s.table_name = c.table_name
                AND s.column_name = c.column_name
            WHERE (c.owner, c.table_name) IN ({in_clause})
            ORDER BY c.owner, c.table_name, c.column_id
            """,  # noqa: S608
            params,
        )

    def batch_indexes(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Índices de múltiplas tabelas em uma query."""
        if not pairs:
            return "SELECT 1 FROM DUAL WHERE 1=0", {}
        in_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT i.table_owner AS owner, i.table_name,
                   i.index_name, i.index_type, i.uniqueness, i.status,
                   i.num_rows, i.distinct_keys, i.clustering_factor,
                   i.last_analyzed, i.blevel, i.leaf_blocks,
                   LISTAGG(ic.column_name, ', ')
                       WITHIN GROUP (ORDER BY ic.column_position) AS columns
            FROM all_indexes i
            JOIN all_ind_columns ic
                ON ic.index_owner = i.owner
                AND ic.index_name = i.index_name
            WHERE (i.table_owner, i.table_name) IN ({in_clause})
            GROUP BY i.table_owner, i.table_name,
                     i.index_name, i.index_type, i.uniqueness, i.status,
                     i.num_rows, i.distinct_keys, i.clustering_factor,
                     i.last_analyzed, i.blevel, i.leaf_blocks
            ORDER BY i.table_owner, i.table_name, i.index_name
            """,  # noqa: S608
            params,
        )

    def batch_constraints(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Constraints de múltiplas tabelas em uma query."""
        if not pairs:
            return "SELECT 1 FROM DUAL WHERE 1=0", {}
        in_clause, params = self.build_tuple_in_clause(pairs)
        return (
            f"""
            SELECT c.owner, c.table_name,
                   c.constraint_name, c.constraint_type, c.status,
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
            WHERE (c.owner, c.table_name) IN ({in_clause})
            GROUP BY c.owner, c.table_name,
                     c.constraint_name, c.constraint_type, c.status,
                     c.validated, c.r_constraint_name, r.table_name, r.owner
            ORDER BY c.owner, c.table_name, c.constraint_type, c.constraint_name
            """,  # noqa: S608
            params,
        )


# ── OraclePlanParser (stub — T5 substitui) ──────────────────────────


class OraclePlanParser(PlanParser):
    """Stub — parsing de plano Oracle será implementado em T5."""

    def parse_plan(self, plan_lines: list[str]) -> list[PlanBlock]:
        raise NotImplementedError("OraclePlanParser será implementado em T5")

    def is_runtime_plan(self, plan_lines: list[str]) -> bool:
        raise NotImplementedError("OraclePlanParser será implementado em T5")


# ── OracleAdapter ───────────────────────────────────────────────────


class OracleAdapter(DatabaseAdapter):
    """Adapter concreto para Oracle via oracledb (thin/thick)."""

    def __init__(self) -> None:
        self._query_builder: OracleQueryBuilder | None = None
        self._plan_parser: OraclePlanParser | None = None

    @property
    def db_type(self) -> str:
        return "oracle"

    @property
    def query_builder(self) -> OracleQueryBuilder:
        if self._query_builder is None:
            self._query_builder = OracleQueryBuilder()
        return self._query_builder

    @property
    def plan_parser(self) -> OraclePlanParser:
        if self._plan_parser is None:
            self._plan_parser = OraclePlanParser()
        return self._plan_parser

    def connect(self, config: dict[str, Any], timeout: int | None = None) -> oracledb.Connection:
        """Abre conexão Oracle (thin → thick fallback em DPY-3010).

        NÃO chama validate_privileges — isso é política do connector.
        """
        dsn = oracledb.makedsn(config["host"], config["port"], service_name=config["service"])

        effective_timeout = timeout if timeout is not None else config.get("timeout", 180)

        try:
            conn = oracledb.connect(
                user=config["user"],
                password=config["password"],
                dsn=dsn,
            )
        except oracledb.DatabaseError as e:
            if "DPY-3010" not in str(e):
                raise

            logger.info("Thin mode não suportado por este banco, tentando thick mode...")
            _init_thick_mode_if_available()

            conn = oracledb.connect(
                user=config["user"],
                password=config["password"],
                dsn=dsn,
            )

        if effective_timeout > 0:
            conn.call_timeout = effective_timeout * 1000

        return conn

    def test_connection(self, config: dict[str, Any]) -> bool:
        """Testa conexão (connect + query simples + close)."""
        try:
            conn = self.connect(config)
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
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
            cursor.execute(sql, params)
            result["dangerous_privileges"] = [row[0] for row in cursor]

            sql, params = qb.dangerous_roles()
            cursor.execute(sql, params)
            result["dangerous_roles"] = [row[0] for row in cursor]
        finally:
            cursor.close()

        return result

    def diagnose_connection(self, config: dict[str, Any]) -> dict[str, Any]:
        """Diagnóstico completo: versão, modo, schema, thick mode."""
        dsn = oracledb.makedsn(config["host"], config["port"], service_name=config["service"])
        needs_thick = False

        try:
            conn = oracledb.connect(
                user=config["user"],
                password=config["password"],
                dsn=dsn,
            )
        except oracledb.DatabaseError as e:
            if "DPY-3010" not in str(e):
                raise
            needs_thick = True
            _init_thick_mode_if_available()
            conn = oracledb.connect(
                user=config["user"],
                password=config["password"],
                dsn=dsn,
            )

        mode = "thick" if not oracledb.is_thin_mode() else "thin"

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
            row = cursor.fetchone()
            version = row[0] if row else "unknown"

            cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
            row = cursor.fetchone()
            current_schema = row[0] if row else "unknown"

            match = re.search(r"(\d+)", version)
            major = int(match.group(1)) if match else 0

            return {
                "status": "ok",
                "version": version,
                "major_version": str(major),
                "mode": mode,
                "schema": current_schema,
                "needs_thick": str(needs_thick),
            }
        finally:
            conn.close()

    def execute_query(self, cursor: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Executa query e retorna resultados como lista de dicts.

        Converte LOBs via .read() automaticamente.
        """
        cursor.execute(sql, params)
        if cursor.description is None:
            return []
        columns = [col[0] for col in cursor.description]
        rows = []
        for row in cursor:
            converted = []
            for val in row:
                if hasattr(val, "read"):
                    converted.append(val.read())
                else:
                    converted.append(val)
            rows.append(dict(zip(columns, converted, strict=False)))
        return rows

    def close_connection(self, conn: Any) -> None:
        """Fecha a conexão Oracle."""
        conn.close()


# ── Registro ────────────────────────────────────────────────────────

register_adapter("oracle", OracleAdapter)
