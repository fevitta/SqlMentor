"""
Registry de adapters de banco de dados.

Cada adapter se registra via register_adapter() ao ser importado.
O get_adapter() faz lazy import do módulo correspondente.
"""

from __future__ import annotations

from sqlmentor.adapters.base import DatabaseAdapter, PlanParser, QueryBuilder

__all__ = [
    "DatabaseAdapter",
    "PlanParser",
    "QueryBuilder",
    "get_adapter",
    "list_adapters",
    "register_adapter",
]

_ADAPTER_REGISTRY: dict[str, type[DatabaseAdapter]] = {}

# Mapa de lazy imports: db_type → módulo que contém o adapter.
# Ao chamar get_adapter(), se o adapter não está registrado ainda,
# importamos o módulo correspondente (que deve chamar register_adapter).
_LAZY_IMPORTS: dict[str, str] = {
    "oracle": "sqlmentor.adapters.oracle",
    # "postgresql": "sqlmentor.adapters.postgresql",
    # "mariadb": "sqlmentor.adapters.mariadb",
}


def register_adapter(db_type: str, adapter_class: type[DatabaseAdapter]) -> None:
    """Registra um adapter para um tipo de banco.

    Chamado pelo módulo do adapter ao ser importado.
    """
    _ADAPTER_REGISTRY[db_type] = adapter_class


def get_adapter(db_type: str) -> type[DatabaseAdapter]:
    """Retorna a classe do adapter para o tipo de banco.

    Faz lazy import do módulo se o adapter ainda não foi registrado.

    Raises:
        ValueError: Se o db_type não é suportado.
    """
    if db_type not in _ADAPTER_REGISTRY:
        module_path = _LAZY_IMPORTS.get(db_type)
        if module_path is None:
            supported = sorted(_ADAPTER_REGISTRY.keys() | _LAZY_IMPORTS.keys())
            raise ValueError(
                f"Tipo de banco não suportado: {db_type!r}. "
                f"Tipos disponíveis: {', '.join(supported) or 'nenhum'}"
            )
        import importlib

        importlib.import_module(module_path)

    if db_type not in _ADAPTER_REGISTRY:
        raise ValueError(
            f"Módulo para {db_type!r} foi importado mas não registrou um adapter. "
            f"O módulo deve chamar register_adapter() ao ser importado."
        )

    return _ADAPTER_REGISTRY[db_type]


def list_adapters() -> list[str]:
    """Lista tipos de banco disponíveis (registrados + lazy)."""
    return sorted(_ADAPTER_REGISTRY.keys() | _LAZY_IMPORTS.keys())
