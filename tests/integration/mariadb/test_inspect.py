"""Testes de integração MariaDB: inspect flow (performance_schema → digest)."""

import pytest

pytestmark = pytest.mark.mariadb


class TestDigestLookup:
    """Testa busca de queries via DIGEST em performance_schema."""

    def test_seed_query_has_digest(self, seed_query_digest):
        """Fixture retorna DIGEST válido (hex string)."""
        assert seed_query_digest is not None
        assert len(seed_query_digest) >= 8
        # DIGEST é hex string
        assert all(c in "0123456789abcdef" for c in seed_query_digest.lower())

    def test_sql_text_by_id(self, mariadb_cursor, seed_query_digest, mariadb_adapter):
        """sql_text_by_id() retorna SQL reconhecível (DIGEST_TEXT é normalizado)."""
        qb = mariadb_adapter.query_builder
        sql, params = qb.sql_text_by_id(seed_query_digest)
        rows = mariadb_adapter.execute_query(mariadb_cursor, sql, params)
        assert len(rows) == 1
        text = str(rows[0]["sql_fulltext"])
        # DIGEST_TEXT é normalizado (sem literais), mas deve conter tokens da query
        assert "EMPLOYEES" in text.upper() or "SELECT" in text.upper()

    def test_runtime_stats(self, mariadb_cursor, seed_query_digest, mariadb_adapter):
        """sql_runtime_stats() retorna métricas com executions > 0."""
        qb = mariadb_adapter.query_builder
        sql, params = qb.sql_runtime_stats(seed_query_digest)
        rows = mariadb_adapter.execute_query(mariadb_cursor, sql, params)
        assert len(rows) == 1
        stats = rows[0]
        assert stats["executions"] >= 1
        assert stats["sql_id"] == seed_query_digest


class TestInspectFlow:
    """Testa o fluxo de inspect para MariaDB (estimado, não runtime)."""

    def test_inspect_produces_execution_plan(self, mariadb_conn, mariadb_adapter):
        """Inspect MariaDB produz execution_plan via collect_context."""
        from sqlmentor.collector import collect_context
        from sqlmentor.parser import ParsedSQL

        # Usa SQL direto (DIGEST_TEXT é normalizado e pode ser inválido pra re-explain)
        parsed = ParsedSQL(
            raw_sql=(
                "SELECT e.EMP_ID, e.FIRST_NAME, o.ORDER_ID, o.TOTAL "
                "FROM EMPLOYEES e "
                "JOIN ORDERS o ON e.EMP_ID = o.EMP_ID "
                "WHERE e.DEPT_ID = 10 AND o.STATUS = 'COMPLETED'"
            ),
            sql_type="SELECT",
            tables=[
                {"name": "EMPLOYEES", "schema": None, "alias": "e"},
                {"name": "ORDERS", "schema": None, "alias": "o"},
            ],
            where_columns=["DEPT_ID", "STATUS"],
        )

        from tests.integration.mariadb.conftest import MARIADB_SCHEMA

        ctx = collect_context(
            parsed=parsed,
            conn=mariadb_conn,
            default_schema=MARIADB_SCHEMA,
            use_cache=False,
            adapter=mariadb_adapter,
        )
        # MariaDB inspect produz execution_plan (estimado via EXPLAIN FORMAT=JSON)
        assert ctx.execution_plan is not None
        assert len(ctx.execution_plan) > 0


class TestWaitEvents:
    """Testa coleta de wait events da sessão."""

    def test_session_wait_events_returns_list(self, mariadb_conn, mariadb_adapter):
        """session_wait_events() retorna lista (pode ser vazia)."""
        cursor = mariadb_conn.cursor()
        cursor.execute("SELECT CONNECTION_ID()")
        row = cursor.fetchone()
        connection_id = row[0]

        qb = mariadb_adapter.query_builder
        sql, params = qb.session_wait_events(connection_id)
        rows = mariadb_adapter.execute_query(cursor, sql, params)
        cursor.close()
        assert isinstance(rows, list)
