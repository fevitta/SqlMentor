"""Testes para o registry de adapters e ABCs."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlmentor.adapters import (
    DatabaseAdapter,
    PlanParser,
    QueryBuilder,
    get_adapter,
    list_adapters,
    register_adapter,
)


@pytest.fixture(autouse=True)
def _clean_registry(monkeypatch):
    """Isola o registry entre testes."""
    monkeypatch.setattr("sqlmentor.adapters._ADAPTER_REGISTRY", {})
    monkeypatch.setattr(
        "sqlmentor.adapters._LAZY_IMPORTS",
        {"oracle": "sqlmentor.adapters.oracle"},
    )


# ── Classe concreta mínima para testes ──────────────────────────────


class _StubAdapter(DatabaseAdapter):
    """Adapter stub que implementa todos os métodos abstratos."""

    @property
    def db_type(self) -> str:
        return "stub"

    @property
    def query_builder(self) -> QueryBuilder:
        return MagicMock(spec=QueryBuilder)

    @property
    def plan_parser(self) -> PlanParser:
        return MagicMock(spec=PlanParser)

    def connect(self, config: dict[str, Any], timeout: int | None = None) -> Any:
        return None

    def test_connection(self, config: dict[str, Any]) -> bool:
        return True

    def validate_privileges(self, conn: Any) -> dict[str, list[str]]:
        return {"dangerous_privileges": [], "dangerous_roles": []}

    def diagnose_connection(self, config: dict[str, Any]) -> dict[str, Any]:
        return {}

    def execute_query(self, cursor: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        return []

    def close_connection(self, conn: Any) -> None:
        pass


# ── register_adapter ────────────────────────────────────────────────


class TestRegisterAdapter:
    def test_registers_and_retrieves(self):
        register_adapter("stub", _StubAdapter)
        assert get_adapter("stub") is _StubAdapter

    def test_overwrite_replaces_silently(self):
        register_adapter("stub", _StubAdapter)

        class _OtherStub(_StubAdapter):
            pass

        register_adapter("stub", _OtherStub)
        assert get_adapter("stub") is _OtherStub

    def test_normalizes_db_type(self):
        register_adapter("  Oracle ", _StubAdapter)
        assert get_adapter("oracle") is _StubAdapter


# ── get_adapter ─────────────────────────────────────────────────────


class TestGetAdapter:
    def test_unknown_db_type_raises_value_error(self):
        with pytest.raises(ValueError, match=r"não suportado.*'mysql'"):
            get_adapter("mysql")

    def test_error_message_lists_available_types(self):
        register_adapter("stub", _StubAdapter)
        with pytest.raises(ValueError, match=r"oracle.*stub"):
            get_adapter("mysql")

    def test_lazy_import_triggers_module_load(self, monkeypatch):
        """Simula lazy import: importlib.import_module registra o adapter."""
        import sqlmentor.adapters as adapters_mod

        def fake_import(module_path):
            adapters_mod._ADAPTER_REGISTRY["oracle"] = _StubAdapter

        monkeypatch.setattr("importlib.import_module", fake_import)
        result = get_adapter("oracle")
        assert result is _StubAdapter

    def test_lazy_import_module_without_register_raises(self, monkeypatch):
        """Módulo importado mas não chama register_adapter."""
        monkeypatch.setattr("importlib.import_module", lambda _path: None)
        with pytest.raises(ValueError, match="não registrou"):
            get_adapter("oracle")

    def test_already_registered_skips_import(self):
        register_adapter("oracle", _StubAdapter)
        # Se tentasse importar, falharia (módulo oracle não existe ainda).
        # Mas como já está registrado, retorna direto.
        assert get_adapter("oracle") is _StubAdapter

    def test_normalizes_db_type_case(self):
        register_adapter("stub", _StubAdapter)
        assert get_adapter("STUB") is _StubAdapter

    def test_import_error_propagates(self, monkeypatch):
        """ImportError no lazy import propaga sem ser engolido."""
        monkeypatch.setattr(
            "importlib.import_module",
            lambda _path: (_ for _ in ()).throw(ImportError("No module named 'oracledb'")),
        )
        with pytest.raises(ImportError, match="oracledb"):
            get_adapter("oracle")

    def test_empty_registry_shows_nenhum(self, monkeypatch):
        """Sem adapters registrados nem lazy imports, mensagem diz 'nenhum'."""
        monkeypatch.setattr("sqlmentor.adapters._LAZY_IMPORTS", {})
        with pytest.raises(ValueError, match="nenhum"):
            get_adapter("anything")


# ── list_adapters ───────────────────────────────────────────────────


class TestListAdapters:
    def test_includes_lazy_imports(self):
        result = list_adapters()
        assert "oracle" in result

    def test_includes_registered_adapters(self):
        register_adapter("stub", _StubAdapter)
        result = list_adapters()
        assert "stub" in result
        assert "oracle" in result

    def test_deduplication(self):
        register_adapter("oracle", _StubAdapter)
        result = list_adapters()
        assert result.count("oracle") == 1

    def test_sorted_output(self):
        register_adapter("zebra", _StubAdapter)
        register_adapter("alpha", _StubAdapter)
        result = list_adapters()
        assert result == sorted(result)


# ── ABCs não instanciáveis ──────────────────────────────────────────


class TestABCsNotInstantiable:
    def test_database_adapter_cannot_instantiate(self):
        with pytest.raises(TypeError):
            DatabaseAdapter()  # type: ignore[abstract]

    def test_query_builder_cannot_instantiate(self):
        with pytest.raises(TypeError):
            QueryBuilder()  # type: ignore[abstract]

    def test_plan_parser_cannot_instantiate(self):
        with pytest.raises(TypeError):
            PlanParser()  # type: ignore[abstract]

    def test_stub_adapter_instantiates(self):
        adapter = _StubAdapter()
        assert adapter.db_type == "stub"


# ── Stubs concretos de QueryBuilder e PlanParser ────────────────────


class _StubQueryBuilder(QueryBuilder):
    """QueryBuilder concreto mínimo para validar o contrato ABC."""

    def explain_plan(self, sql_text: str) -> list[tuple[str, dict]]:
        return [("EXPLAIN ...", {})]

    def runtime_plan(self, sql_id: str, child_number: int = 0) -> tuple[str, dict]:
        return ("SELECT ...", {"sql_id": sql_id})

    def db_version(self) -> tuple[str, dict]:
        return ("SELECT version()", {})

    def optimizer_params(self) -> tuple[str, dict]:
        return ("SHOW ALL", {})

    def session_wait_events(self, session_id: int) -> tuple[str, dict]:
        return ("SELECT ...", {"sid": session_id})

    def object_type(self, owner: str, object_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {"owner": owner, "name": object_name})

    def table_ddl(self, owner: str, table_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def function_ddl(self, owner: str, function_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def table_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def column_stats(self, owner: str, table_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def indexes(self, owner: str, table_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def constraints(self, owner: str, table_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def histograms(self, owner: str, table_name: str, column_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def table_partitions(self, owner: str, table_name: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def index_to_table_map(self, owner: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def sql_runtime_stats(self, sql_id: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def sql_text_by_id(self, sql_id: str) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def dangerous_privileges(self) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def dangerous_roles(self) -> tuple[str, dict]:
        return ("SELECT ...", {})

    def batch_table_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        return ("SELECT ...", {})

    def batch_column_stats(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        return ("SELECT ...", {})

    def batch_indexes(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        return ("SELECT ...", {})

    def batch_constraints(self, pairs: list[tuple[str, str]]) -> tuple[str, dict[str, str]]:
        return ("SELECT ...", {})


class _StubPlanParser(PlanParser):
    """PlanParser concreto mínimo para validar o contrato ABC."""

    def parse_plan(self, plan_lines: list[str]) -> list:
        return []

    def is_runtime_plan(self, plan_lines: list[str]) -> bool:
        return False


class TestConcreteStubs:
    def test_query_builder_instantiates(self):
        qb = _StubQueryBuilder()
        steps = qb.explain_plan("SELECT 1")
        assert isinstance(steps, list)
        assert len(steps) == 1

    def test_plan_parser_instantiates(self):
        pp = _StubPlanParser()
        assert pp.is_runtime_plan([]) is False
        assert pp.parse_plan(["line"]) == []
