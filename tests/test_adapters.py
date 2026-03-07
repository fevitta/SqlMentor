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


# ── get_adapter ─────────────────────────────────────────────────────


class TestGetAdapter:
    def test_unknown_db_type_raises_value_error(self):
        with pytest.raises(ValueError, match="não suportado.*'mysql'"):
            get_adapter("mysql")

    def test_error_message_lists_available_types(self):
        register_adapter("stub", _StubAdapter)
        with pytest.raises(ValueError, match="oracle.*stub"):
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
