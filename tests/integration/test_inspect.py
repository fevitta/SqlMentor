"""Testes de integração: inspect flow (V$SQL, DBMS_XPLAN.DISPLAY_CURSOR)."""

import pytest

from sqlmentor.collector import _execute_query
from sqlmentor.queries import runtime_plan, session_wait_events, sql_runtime_stats, sql_text_by_id

pytestmark = pytest.mark.oracle


class TestVSQLLookup:
    """Testa busca de queries em V$SQL."""

    def test_seed_query_has_sql_id(self, seed_query_sql_id):
        """Fixture retorna sql_id válido (alfanumérico, 13 chars)."""
        assert seed_query_sql_id is not None
        assert len(seed_query_sql_id) >= 8

    def test_sql_text_by_id(self, oracle_cursor, seed_query_sql_id):
        """sql_text_by_id() retorna SQL contendo o comment marker."""
        sql, params = sql_text_by_id(seed_query_sql_id)
        oracle_cursor.execute(sql, params)
        row = oracle_cursor.fetchone()
        assert row is not None
        text = str(row[0])
        assert "SQLMENTOR_SEED_QUERY" in text

    def test_runtime_stats(self, oracle_cursor, seed_query_sql_id):
        """sql_runtime_stats() retorna métricas com executions > 0."""
        sql, params = sql_runtime_stats(seed_query_sql_id)
        rows = _execute_query(oracle_cursor, sql, params)
        assert len(rows) == 1
        stats = rows[0]
        assert stats["executions"] >= 1
        assert stats["sql_id"] == seed_query_sql_id


class TestDisplayCursor:
    """Testa DBMS_XPLAN.DISPLAY_CURSOR via sql_id."""

    def test_runtime_plan_returns_lines(self, oracle_cursor, seed_query_sql_id):
        """runtime_plan() retorna linhas de plano ALLSTATS."""
        sql, params = runtime_plan(seed_query_sql_id)
        oracle_cursor.execute(sql, params)
        lines = [row[0] for row in oracle_cursor]
        assert len(lines) > 0
        plan_text = "\n".join(str(line) for line in lines)
        # Deve conter ao menos Id e Operation
        assert "Id" in plan_text or "PLAN_TABLE_OUTPUT" in plan_text

    def test_runtime_plan_has_allstats_columns(self, oracle_cursor, seed_query_sql_id):
        """Plano ALLSTATS deve ter colunas de estatísticas reais (A-Rows, Buffers)."""
        sql, params = runtime_plan(seed_query_sql_id)
        oracle_cursor.execute(sql, params)
        lines = [str(row[0]) for row in oracle_cursor]
        plan_text = "\n".join(lines)
        # A seed query foi executada com STATISTICS_LEVEL=ALL
        assert "A-Rows" in plan_text or "Starts" in plan_text


class TestWaitEvents:
    """Testa coleta de wait events da sessão."""

    def test_session_wait_events_returns_list(self, oracle_conn):
        """session_wait_events() retorna lista (pode ser vazia se sessão nova)."""
        cursor = oracle_conn.cursor()
        cursor.execute("SELECT sid FROM v$mystat WHERE ROWNUM = 1")
        row = cursor.fetchone()
        sid = row[0]

        sql, params = session_wait_events(sid)
        rows = _execute_query(cursor, sql, params)
        cursor.close()
        # Resultado é uma lista (pode ser vazia, mas não deve dar erro)
        assert isinstance(rows, list)
