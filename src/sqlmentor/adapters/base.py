"""
Interfaces base (ABCs) para adapters de banco de dados.

Define os contratos que cada adapter (Oracle, PostgreSQL, MariaDB) deve implementar.
O collector e o report continuam orquestrando — os adapters fornecem queries e parsing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlmentor.report import PlanBlock


class QueryBuilder(ABC):
    """Gera queries parametrizadas para um banco específico.

    Cada método retorna tuple[str, dict] (sql, params) pronta para cursor.execute(),
    exceto explain_plan que retorna list[tuple[str, dict]] (múltiplos steps).
    """

    # ── Plano de execução ───────────────────────────────────────────

    @abstractmethod
    def explain_plan(self, sql_text: str) -> list[tuple[str, dict]]:
        """Gera EXPLAIN PLAN e recupera o resultado.

        Retorna lista de steps (Oracle=3, PG=1, etc.).
        """

    @abstractmethod
    def runtime_plan(self, sql_id: str, child_number: int = 0) -> tuple[str, dict]:
        """Plano real com estatísticas de execução via identificador do SQL."""

    # ── Sessão / instância ──────────────────────────────────────────

    @abstractmethod
    def db_version(self) -> tuple[str, dict]:
        """Versão do banco de dados."""

    @abstractmethod
    def optimizer_params(self) -> tuple[str, dict]:
        """Parâmetros relevantes do otimizador."""

    @abstractmethod
    def session_wait_events(self, session_id: int) -> tuple[str, dict]:
        """Wait events da sessão (top por tempo)."""

    # ── Objetos (tabela/view) ───────────────────────────────────────

    @abstractmethod
    def object_type(self, owner: str, object_name: str) -> tuple[str, dict]:
        """Tipo do objeto (TABLE, VIEW, etc.)."""

    @abstractmethod
    def table_ddl(self, owner: str, table_name: str) -> tuple[str, dict]:
        """DDL completo da tabela ou view."""

    @abstractmethod
    def function_ddl(self, owner: str, function_name: str) -> tuple[str, dict]:
        """DDL de uma função, procedure ou package."""

    @abstractmethod
    def table_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Estatísticas gerais da tabela."""

    @abstractmethod
    def column_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Estatísticas de colunas (cardinalidade, nulls, histogramas)."""

    @abstractmethod
    def indexes(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Índices da tabela com colunas."""

    @abstractmethod
    def constraints(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Constraints (PK, FK, unique, check)."""

    @abstractmethod
    def histograms(self, owner: str, table_name: str, column_name: str) -> tuple[str, dict]:
        """Histograma detalhado de uma coluna específica."""

    @abstractmethod
    def table_partitions(self, owner: str, table_name: str) -> tuple[str, dict]:
        """Info de particionamento."""

    @abstractmethod
    def index_to_table_map(self, owner: str) -> tuple[str, dict]:
        """Mapa index_name → table_name para um schema."""

    # ── Runtime stats ───────────────────────────────────────────────

    @abstractmethod
    def sql_runtime_stats(self, sql_id: str) -> tuple[str, dict]:
        """Métricas de execução reais via identificador do SQL."""

    @abstractmethod
    def sql_text_by_id(self, sql_id: str) -> tuple[str, dict]:
        """Recupera o texto completo do SQL a partir do identificador."""

    # ── Segurança ───────────────────────────────────────────────────

    @abstractmethod
    def dangerous_privileges(self) -> tuple[str, dict]:
        """Privilégios de sistema perigosos que o user não deveria ter."""

    @abstractmethod
    def dangerous_roles(self) -> tuple[str, dict]:
        """Roles perigosas que o user não deveria ter."""

    # ── Batch (múltiplas tabelas) ───────────────────────────────────

    @abstractmethod
    def batch_table_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Estatísticas de múltiplas tabelas em uma query."""

    @abstractmethod
    def batch_column_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Estatísticas de colunas de múltiplas tabelas em uma query."""

    @abstractmethod
    def batch_indexes(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Índices de múltiplas tabelas em uma query."""

    @abstractmethod
    def batch_constraints(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        """Constraints de múltiplas tabelas em uma query."""


class PlanParser(ABC):
    """Parseia linhas de plano de execução em PlanBlocks estruturados.

    Separado do QueryBuilder porque parsing é puro (sem I/O) e testável isoladamente.
    """

    @abstractmethod
    def parse_plan(self, plan_lines: list[str]) -> list[PlanBlock]:
        """Parseia linhas do plano de execução em lista de PlanBlock."""

    @abstractmethod
    def is_runtime_plan(self, plan_lines: list[str]) -> bool:
        """Detecta se o plano contém estatísticas reais (runtime) ou é apenas estimado."""


class DatabaseAdapter(ABC):
    """Adapter principal que agrupa conexão, queries e parsing para um banco específico.

    Cada banco de dados (Oracle, PostgreSQL, MariaDB) implementa um DatabaseAdapter
    concreto que fornece QueryBuilder e PlanParser específicos.
    """

    @property
    @abstractmethod
    def db_type(self) -> str:
        """Identificador do tipo de banco (ex: 'oracle', 'postgresql', 'mariadb')."""

    @property
    @abstractmethod
    def query_builder(self) -> QueryBuilder:
        """Instância do QueryBuilder específico deste banco."""

    @property
    @abstractmethod
    def plan_parser(self) -> PlanParser:
        """Instância do PlanParser específico deste banco."""

    @abstractmethod
    def connect(self, config: dict[str, Any], timeout: int | None = None) -> Any:
        """Abre conexão com o banco.

        Args:
            config: Configuração de conexão (chaves variam por DB).
            timeout: Timeout em segundos (None = sem limite).

        Returns:
            Objeto de conexão (tipo varia por driver).
        """

    @abstractmethod
    def test_connection(self, config: dict[str, Any]) -> bool:
        """Testa se a conexão funciona (connect + simple query + close).

        Returns:
            True se conectou com sucesso.
        """

    @abstractmethod
    def validate_privileges(self, conn: Any) -> dict[str, list[str]]:
        """Valida privilégios do user conectado.

        Returns:
            Dict com 'dangerous_privileges' e 'dangerous_roles' encontrados.
        """

    @abstractmethod
    def diagnose_connection(self, config: dict[str, Any]) -> dict[str, Any]:
        """Diagnóstico detalhado de problemas de conexão.

        Returns:
            Dict com informações de diagnóstico (versão, modo, erros, etc.).
        """

    @abstractmethod
    def execute_query(self, cursor: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Executa query e retorna resultados como lista de dicts.

        Args:
            cursor: Cursor do banco (tipo varia por driver).
            sql: SQL com placeholders.
            params: Parâmetros para bind.

        Returns:
            Lista de dicts (column_name → value) por row.
        """

    @abstractmethod
    def close_connection(self, conn: Any) -> None:
        """Fecha a conexão com o banco."""
