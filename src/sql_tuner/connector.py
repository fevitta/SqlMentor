"""
Gerenciador de conexões Oracle.

Salva profiles em ~/.sql-tuner/connections.yaml.
"""

import logging
from pathlib import Path
from typing import Any

import oracledb
import yaml

logger = logging.getLogger(__name__)

CONFIG_DIR = Path.home() / ".sql-tuner"
CONNECTIONS_FILE = CONFIG_DIR / "connections.yaml"

_thick_mode_initialized = False


def _ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def _load_connections() -> dict[str, dict]:
    if not CONNECTIONS_FILE.exists():
        return {}
    with open(CONNECTIONS_FILE) as f:
        data = yaml.safe_load(f)
    return data or {}


def _save_connections(connections: dict[str, dict]) -> None:
    _ensure_config_dir()
    with open(CONNECTIONS_FILE, "w") as f:
        yaml.dump(connections, f, default_flow_style=False, allow_unicode=True)


def _init_thick_mode() -> None:
    """Inicializa modo thick do oracledb (requer Oracle Instant Client)."""
    global _thick_mode_initialized
    if _thick_mode_initialized:
        return
    try:
        oracledb.init_oracle_client()
        _thick_mode_initialized = True
        logger.info("oracledb: modo thick ativado via Oracle Instant Client")
    except oracledb.ProgrammingError as e:
        raise RuntimeError(
            "Oracle Instant Client não encontrado. "
            "Instale o Instant Client e adicione ao PATH.\n"
            "Download: https://www.oracle.com/database/technologies/instant-client.html\n"
            f"Detalhe: {e}"
        ) from e


def add_connection(
    name: str,
    host: str,
    port: int,
    service: str,
    user: str,
    password: str,
    schema: str | None = None,
) -> None:
    """Adiciona ou atualiza um profile de conexão."""
    connections = _load_connections()
    connections[name] = {
        "type": "oracle",
        "host": host,
        "port": port,
        "service": service,
        "user": user,
        "password": password,
        "schema": schema or user.upper(),
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
        safe[name]["password"] = "****"
    return safe


def get_connection_config(name: str) -> dict[str, Any]:
    """Retorna config completa de um profile."""
    connections = _load_connections()
    if name not in connections:
        raise ValueError(f"Conexão '{name}' não encontrada. Use 'sql-tuner config list'.")
    return connections[name]


def connect(name: str) -> oracledb.Connection:
    """
    Abre uma conexão Oracle a partir de um profile salvo.

    Tenta modo thin primeiro. Se falhar com DPY-3010 (versão antiga do Oracle),
    tenta modo thick automaticamente (requer Oracle Instant Client).
    """
    cfg = get_connection_config(name)
    dsn = oracledb.makedsn(cfg["host"], cfg["port"], service_name=cfg["service"])

    try:
        return oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            dsn=dsn,
        )
    except oracledb.DatabaseError as e:
        if "DPY-3010" in str(e):
            logger.info("Thin mode falhou (Oracle antigo), tentando thick mode...")
            _init_thick_mode()
            return oracledb.connect(
                user=cfg["user"],
                password=cfg["password"],
                dsn=dsn,
            )
        raise


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
