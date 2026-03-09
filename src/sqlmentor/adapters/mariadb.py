"""
Adapter MariaDB — implementação concreta de DatabaseAdapter.

Encapsula toda lógica específica do PyMySQL: conexão, validação de privilégios,
diagnóstico. MariaDBQueryBuilder contém stubs para T18/T20.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import pymysql  # type: ignore[import-untyped]

from sqlmentor.adapters import register_adapter
from sqlmentor.adapters.base import DatabaseAdapter, PlanParser, QueryBuilder

if TYPE_CHECKING:
    from sqlmentor.report import PlanBlock

logger = logging.getLogger(__name__)


# ── MariaDBQueryBuilder ──────────────────────────────────────────────


class MariaDBQueryBuilder(QueryBuilder):
    """Queries MariaDB parametrizadas (paramstyle pyformat: %(name)s).

    Apenas db_version e dangerous_privileges estão implementados.
    Demais métodos são stubs que retornam zero rows — serão implementados em T18/T20.
    """

    # ── Plano de execução ────────────────────────────────────────────

    def explain_plan(self, sql_text: str) -> list[tuple[str, dict]]:
        """Stub — será implementado em T18/T20."""
        return [("SELECT 1 WHERE 1=0", {})]

    def runtime_plan(self, sql_id: str, child_number: int = 0) -> tuple[str, dict]:
        """Stub — será implementado em T18/T20."""
        return ("SELECT 1 WHERE 1=0", {})

    # ── Sessão / instância ───────────────────────────────────────────

    def db_version(self) -> tuple[str, dict]:
        """Versão do MariaDB."""
        return ("SELECT VERSION()", {})

    def optimizer_params(self) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def set_statistics_level(self, level: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def session_sid(self) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def prev_sql_id(self) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def session_wait_events(self, session_id: int) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    # ── Objetos (tabela/view) ────────────────────────────────────────

    def object_type(self, owner: str, object_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def table_ddl(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def function_ddl(self, owner: str, function_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def table_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def column_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def indexes(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def constraints(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def histograms(self, owner: str, table_name: str, column_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def table_partitions(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def index_to_table_map(self, owner: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    # ── Runtime stats ────────────────────────────────────────────────

    def sql_runtime_stats(self, sql_id: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def sql_text_by_id(self, sql_id: str) -> tuple[str, dict]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

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
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def batch_column_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def batch_indexes(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})

    def batch_constraints(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Stub — será implementado em T18."""
        return ("SELECT 1 WHERE 1=0", {})


# ── MariaDBPlanParser ────────────────────────────────────────────────


class MariaDBPlanParser(PlanParser):
    """Parser de planos MariaDB — stub para T20."""

    def parse_plan(self, plan_lines: list[str]) -> list[PlanBlock]:
        """Stub — será implementado em T20."""
        return []

    def is_runtime_plan(self, plan_lines: list[str]) -> bool:
        """Stub — será implementado em T20."""
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
            database=config.get("service"),
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
            database=config.get("service"),
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

            match = re.search(r"(\d+)", version)
            major = int(match.group(1)) if match else 0

            return {
                "status": "ok",
                "version": version,
                "major_version": str(major),
                "performance_schema": str(perf_schema),
            }
        finally:
            conn.close()

    def execute_query(self, cursor: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Executa query e retorna resultados como lista de dicts.

        Sem LOB handling — PyMySQL já retorna tipos Python nativos.
        """
        cursor.execute(sql, params or None)
        if cursor.description is None:
            return []
        columns = [col[0].lower() for col in cursor.description]
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
                    "detail": "pip install sqlmentor[mariadb]",
                }
            )
        return results


# ── Registro ────────────────────────────────────────────────────────

register_adapter("mariadb", MariaDBAdapter)
