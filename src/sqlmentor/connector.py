"""
Gerenciador de conexões de banco de dados.

Salva profiles em ~/.sqlmentor/connections.yaml.
Suporta Oracle (oracledb thin/thick), com extensibilidade para PostgreSQL e MariaDB.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

from sqlmentor.adapters.oracle import (
    _init_thick_mode_if_available,
    check_thick_mode_available,
)

logger = logging.getLogger(__name__)

CONFIG_DIR = Path.home() / ".sqlmentor"
CONNECTIONS_FILE = CONFIG_DIR / "connections.yaml"

# Re-export para backward compat (usados em test_connector.py e cli.py)
__all__ = [
    "_init_thick_mode_if_available",
    "check_thick_mode_available",
]


def _supported_db_types() -> list[str]:
    """Retorna tipos de banco suportados via adapters registry (lazy import)."""
    from sqlmentor.adapters import list_adapters

    return list_adapters()


def _validate_db_type(db_type: str) -> str:
    """Valida e normaliza o tipo de banco.

    Raises:
        ValueError: Se o db_type não é suportado.
    """
    normalized = db_type.lower().strip()
    supported = _supported_db_types()
    if normalized not in supported:
        raise ValueError(
            f"Tipo de banco não suportado: {db_type!r}. Tipos disponíveis: {', '.join(supported)}"
        )
    return normalized


def _ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def _load_connections() -> dict[str, dict]:
    if not CONNECTIONS_FILE.exists():
        return {}
    with open(CONNECTIONS_FILE) as f:
        data = yaml.safe_load(f)
    connections = data or {}
    # Backward compat: profiles sem 'type' recebem default 'oracle'
    for cfg in connections.values():
        cfg.setdefault("type", "oracle")
    return connections


def validate_privileges(conn: Any) -> None:
    """
    Verifica se o user conectado tem apenas privilégios de leitura.

    Delega ao adapter Oracle e levanta PermissionError se encontrar
    qualquer privilégio de escrita/DDL ou role perigosa.

    Raises:
        PermissionError: Se o user tiver privilégios além de leitura.
    """
    from sqlmentor.adapters import get_adapter

    adapter = get_adapter("oracle")()
    result = adapter.validate_privileges(conn)

    problems: list[str] = []
    if result["dangerous_privileges"]:
        problems.append(f"Privilégios perigosos: {', '.join(result['dangerous_privileges'])}")
    if result["dangerous_roles"]:
        problems.append(f"Roles perigosas: {', '.join(result['dangerous_roles'])}")

    if problems:
        user = conn.username or "desconhecido"
        raise PermissionError(
            f"Usuário '{user}' tem permissões além de leitura. "
            f"O sqlmentor recusa conexão por segurança.\n"
            f"  {'; '.join(problems)}\n"
            f"Use um usuário read-only (veja scripts/oracle_create_user.sql)."
        )


def _save_connections(connections: dict[str, dict]) -> None:
    _ensure_config_dir()
    with open(CONNECTIONS_FILE, "w") as f:
        yaml.dump(connections, f, default_flow_style=False, allow_unicode=True)


def add_connection(
    name: str,
    host: str,
    port: int,
    service: str,
    user: str,
    password: str,
    schema: str | None = None,
    timeout: int | None = None,
    db_type: str = "oracle",
) -> None:
    """Adiciona ou atualiza um profile de conexão."""
    validated_type = _validate_db_type(db_type)
    connections = _load_connections()
    connections[name] = {
        "type": validated_type,
        "host": host,
        "port": port,
        "service": service,
        "user": user,
        "password": password,
        "schema": schema or user.upper(),
        "timeout": timeout if timeout is not None else 180,
    }
    _save_connections(connections)


def remove_connection(name: str) -> bool:
    """Remove um profile. Retorna True se existia."""
    connections = _load_connections()
    if name in connections:
        del connections[name]
        _save_connections(connections)
        return True
    return False


def list_connections() -> dict[str, dict]:
    """Lista todos os profiles (sem senha)."""
    connections = _load_connections()
    safe = {}
    for name, cfg in connections.items():
        safe[name] = {k: v for k, v in cfg.items() if k != "password"}
        safe[name]["password"] = "****"  # noqa: S105
    return safe


def get_connection_config(name: str) -> dict[str, Any]:
    """Retorna config completa de um profile.

    Profiles sem 'type' recebem default 'oracle' via _load_connections() (backward compat).
    Validação de tipo acontece no write (add_connection) e no uso (connect/test/diagnose).
    """
    connections = _load_connections()
    if name not in connections:
        raise ValueError(f"Conexão '{name}' não encontrada. Use 'sqlmentor config list'.")
    return connections[name]


def set_default_connection(name: str) -> None:
    """Define um profile como conexão padrão."""
    connections = _load_connections()
    if name not in connections:
        raise ValueError(f"Conexão '{name}' não encontrada. Use 'sqlmentor config list'.")
    # Remove default anterior
    for cfg in connections.values():
        cfg.pop("default", None)
    connections[name]["default"] = True
    _save_connections(connections)


def get_default_connection() -> str | None:
    """Retorna o nome do profile marcado como padrão, ou None."""
    connections = _load_connections()
    for name, cfg in connections.items():
        if cfg.get("default"):
            return name
    return None


def resolve_connection(conn: str | None) -> str:
    """Resolve o nome da conexão: explícito > default > erro."""
    if conn:
        return conn
    default = get_default_connection()
    if default:
        return default
    raise ValueError(
        "Nenhuma conexão informada e nenhuma conexão padrão definida.\n"
        "Use --conn <profile> ou defina um default: sqlmentor config set-default -n <profile>"
    )


def connect_with_adapter(name: str, timeout: int | None = None) -> tuple[Any, Any]:
    """
    Abre uma conexão e retorna o adapter junto.

    Delega ao adapter correspondente ao tipo do profile.
    Após conectar, valida que o user não tem privilégios além de leitura.

    Args:
        name: Nome do profile de conexão.
        timeout: Timeout em segundos para operações no banco.
                 Se None, usa o valor do profile (default 180s).
                 Se 0, sem timeout.

    Returns:
        Tuple (adapter, connection).
    """
    from sqlmentor.adapters import get_adapter

    cfg = get_connection_config(name)
    adapter = get_adapter(cfg.get("type", "oracle"))()
    conn = adapter.connect(cfg, timeout)

    # Valida que o user não tem privilégios além de leitura
    try:
        validate_privileges(conn)
    except PermissionError:
        conn.close()
        raise

    return adapter, conn


def connect(name: str, timeout: int | None = None) -> Any:
    """
    Abre uma conexão a partir de um profile salvo.

    Delega ao adapter correspondente ao tipo do profile.
    Após conectar, valida que o user não tem privilégios além de leitura.

    Args:
        name: Nome do profile de conexão.
        timeout: Timeout em segundos para operações no banco.
                 Se None, usa o valor do profile (default 180s).
                 Se 0, sem timeout.
    """
    _adapter, conn = connect_with_adapter(name, timeout)
    return conn


def test_connection(name: str) -> dict[str, str]:
    """Testa conexão e retorna info do banco."""
    conn = connect(name)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
        row = cursor.fetchone()
        version = row[0] if row else "unknown"

        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
        row = cursor.fetchone()
        current_schema = row[0] if row else "unknown"

        return {"status": "ok", "version": version, "schema": current_schema}
    finally:
        conn.close()


def diagnose_connection(name: str) -> dict[str, Any]:
    """
    Diagnóstico completo de uma conexão: versão, modo, schema, thick mode.

    Delega ao adapter correspondente ao tipo do profile.
    """
    from sqlmentor.adapters import get_adapter

    cfg = get_connection_config(name)
    adapter = get_adapter(cfg.get("type", "oracle"))()
    return adapter.diagnose_connection(cfg)
