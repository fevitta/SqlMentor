"""Testes das funções de análise de plano — parsing, extração de tabelas, conversões implícitas, hotspots."""

from sqlmentor.report import (
    _extract_implicit_conversions,
    _extract_plan_tables,
    _format_hotspots,
    _parse_plan_operations,
    _time_to_seconds,
)

# ─── Linha ALLSTATS reutilizável ─────────────────────────────────────────────

_ALLSTATS_LINE = "|*  3 |   INDEX RANGE SCAN           | IDX_ORD_USER   |    100 |     10 |    100 |00:00:00.01 |     300 |    50 |"
_ALLSTATS_LINE_EMPTY_EROWS = "|   0 | SELECT STATEMENT             |                |      1 |        |     50 |00:00:00.26 |   10919 |     0 |"


# ─── _parse_plan_operations ──────────────────────────────────────────────────


class TestParsePlanOperations:
    def test_allstats_line(self):
        ops = _parse_plan_operations([_ALLSTATS_LINE])
        assert len(ops) == 1
        op = ops[0]
        assert op["id"] == 3
        assert op["operation"] == "INDEX RANGE SCAN"
        assert op["name"] == "IDX_ORD_USER"
        assert op["starts"] == 100
        assert op["e_rows"] == 10
        assert op["a_rows"] == 100
        assert op["buffers"] == 300

    def test_empty_erows(self):
        ops = _parse_plan_operations([_ALLSTATS_LINE_EMPTY_EROWS])
        assert len(ops) == 1
        assert ops[0]["e_rows"] == 0

    def test_no_data_lines(self):
        lines = [
            "Plan hash value: 123456789",
            "------------------------------",
            "| Id  | Operation | Name |",
        ]
        ops = _parse_plan_operations(lines)
        assert ops == []

    def test_multiple_ops(self):
        lines = [
            "|   0 | SELECT STATEMENT             |                |      1 |        |     50 |00:00:00.26 |   10919 |     0 |",
            "|   1 |  HASH JOIN                   |                |      1 |     50 |     50 |00:00:00.26 |   10919 |     0 |",
            "|   2 |   TABLE ACCESS FULL          | USERS          |      1 |  10000 |  10000 |00:00:00.05 |     200 |     0 |",
        ]
        ops = _parse_plan_operations(lines)
        assert len(ops) == 3
        assert [o["id"] for o in ops] == [0, 1, 2]


# ─── _extract_plan_tables ───────────────────────────────────────────────────


class TestExtractPlanTables:
    def test_table_access(self):
        ops = [{"operation": "TABLE ACCESS FULL", "name": "USERS"}]
        result = _extract_plan_tables(ops, {})
        assert result == {"USERS"}

    def test_index_resolved(self):
        ops = [{"operation": "INDEX RANGE SCAN", "name": "IDX_ORD_USER"}]
        index_map = {"IDX_ORD_USER": "ORDERS"}
        result = _extract_plan_tables(ops, index_map)
        assert result == {"ORDERS"}

    def test_index_not_in_map(self):
        ops = [{"operation": "INDEX UNIQUE SCAN", "name": "IDX_UNKNOWN"}]
        result = _extract_plan_tables(ops, {})
        assert result == set()

    def test_mat_view(self):
        ops = [{"operation": "MAT_VIEW REWRITE ACCESS FULL", "name": "MV_SUMMARY"}]
        result = _extract_plan_tables(ops, {})
        assert result == {"MV_SUMMARY"}

    def test_empty_name_ignored(self):
        ops = [{"operation": "TABLE ACCESS FULL", "name": ""}]
        result = _extract_plan_tables(ops, {})
        assert result == set()


# ─── _extract_implicit_conversions ───────────────────────────────────────────


class TestExtractImplicitConversions:
    def test_to_number(self):
        lines = [
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            '   3 - filter(TO_NUMBER("STATUS")=1)',
        ]
        result = _extract_implicit_conversions(lines)
        assert len(result) == 1
        assert result[0]["id"] == "3"
        assert "TO_NUMBER" in result[0]["function"]

    def test_to_char_and_to_date(self):
        lines = [
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            "   1 - access(TO_CHAR(\"COL1\")='ABC')",
            '   2 - filter(TO_DATE("COL2")>SYSDATE)',
        ]
        result = _extract_implicit_conversions(lines)
        assert len(result) == 2
        funcs = {r["function"] for r in result}
        assert any("TO_CHAR" in f for f in funcs)
        assert any("TO_DATE" in f for f in funcs)

    def test_no_conversions(self):
        lines = [
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            '   3 - access("U"."ID"="O"."USER_ID")',
        ]
        result = _extract_implicit_conversions(lines)
        assert result == []

    def test_outside_predicate_section(self):
        lines = [
            '   3 - filter(TO_NUMBER("COL")=1)',  # before Predicate Information
            "Predicate Information (identified by operation id):",
            "---------------------------------------------------",
            '   1 - access("A"."ID"="B"."ID")',
        ]
        result = _extract_implicit_conversions(lines)
        assert result == []


# ─── _time_to_seconds ───────────────────────────────────────────────────────


class TestTimeToSeconds:
    def test_zero(self):
        assert _time_to_seconds("00:00:00.00") == 0.0

    def test_mixed(self):
        result = _time_to_seconds("01:02:03.45")
        assert abs(result - 3723.45) < 0.001

    def test_subseconds(self):
        result = _time_to_seconds("00:00:00.97")
        assert abs(result - 0.97) < 0.001


# ─── _format_hotspots ───────────────────────────────────────────────────────


class TestFormatHotspots:
    def test_high_starts(self, allstats_plan_lines):
        result = _format_hotspots(allstats_plan_lines)
        assert "efeito multiplicador" in result
        assert "Starts" in result

    def test_cardinality_deviation(self):
        lines = [
            "|   0 | SELECT STATEMENT             |                |      1 |        |     50 |00:00:00.26 |   10919 |     0 |",
            "|   1 |  TABLE ACCESS FULL           | BIG_TABLE      |      1 |     10 |  10000 |00:00:00.50 |   50000 |     0 |",
        ]
        result = _format_hotspots(lines)
        assert "cardinalidade" in result.lower() or "Ratio" in result

    def test_empty_plan(self):
        result = _format_hotspots(["Plan hash value: 123", "---"])
        assert result == ""
