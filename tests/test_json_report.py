"""Testes de to_json() e _table_to_dict() — serialização JSON do report."""

import json

from sqlmentor.collector import TableContext
from sqlmentor.report import _table_to_dict, to_json

# ─── _table_to_dict ─────────────────────────────────────────────────────────


class TestTableToDict:
    def test_all_fields(self, rich_table_context):
        result = _table_to_dict(rich_table_context)
        assert result["schema"] == "HR"
        assert result["name"] == "ORDERS"
        assert result["object_type"] == "TABLE"
        assert result["stats"]["num_rows"] == 500_000
        assert len(result["columns"]) == 3
        assert len(result["indexes"]) == 2
        assert len(result["constraints"]) == 2
        assert len(result["partitions"]) == 2
        assert "STATUS" in result["histograms"]

    def test_minimal(self):
        table = TableContext(name="SIMPLE", schema="HR")
        result = _table_to_dict(table)
        assert result["name"] == "SIMPLE"
        assert result["ddl"] is None
        assert result["stats"] is None
        assert result["columns"] == []

    def test_view_with_ddl(self, view_table_context):
        result = _table_to_dict(view_table_context)
        assert result["object_type"] == "VIEW"
        assert result["ddl"] is not None
        assert "CREATE" in result["ddl"]


# ─── to_json (extended) ─────────────────────────────────────────────────────


class TestToJsonExtended:
    def test_runtime_fields(self, rich_collected_context):
        data = json.loads(to_json(rich_collected_context))
        assert data["runtime_plan"] is not None
        assert data["runtime_stats"]["sql_id"] == "abc123def456"

    def test_wait_events(self, rich_collected_context):
        data = json.loads(to_json(rich_collected_context))
        assert len(data["wait_events"]) == 1
        assert data["wait_events"][0]["event"] == "db file sequential read"

    def test_view_expansions(self, rich_collected_context):
        data = json.loads(to_json(rich_collected_context))
        assert "V_ACTIVE_ORDERS" in data["view_expansions"]

    def test_function_ddls(self, rich_collected_context):
        data = json.loads(to_json(rich_collected_context))
        assert "HR.FN_CALC" in data["function_ddls"]

    def test_errors_in_json(self, rich_collected_context):
        data = json.loads(to_json(rich_collected_context))
        assert len(data["errors"]) == 1
        assert "Timeout" in data["errors"][0]

    def test_sql_section_complete(self, rich_collected_context):
        data = json.loads(to_json(rich_collected_context))
        sql = data["sql"]
        assert sql["type"] == "SELECT"
        assert len(sql["tables"]) == 2
        assert len(sql["join_columns"]) > 0
        assert len(sql["order_columns"]) > 0
        assert len(sql["group_columns"]) > 0
        assert sql["subqueries"] == 1
        assert len(sql["functions"]) == 1
