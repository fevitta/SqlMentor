"""
Gerenciador de conexões Oracle.

Salva profiles em ~/.sqlmentor/connections.yaml.
Usa oracledb em modo thin (sem Oracle Instant Client).
"""

import logging
from pathlib import Path
from typing import Any

import oracledb
import yaml

logger = logging.getLogger(__name__)

CONFIG_DIR = Path.home() / ".sqlmentor"
CONNECTIONS_FILE = CONFIG_DIR / "connections.yaml"


def _ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def _load_connections() -> dict[str, dict]:
    if not CONNECTIONS_FILE.exists():
        return {}
    with open(CONNECTIONS_FILE) as f:
        data = yaml.safe_load(f)
    return data or {}


def validate_privileges(conn: oracledb.Connection) -> None:
    """
    Verifica se o user conectado tem apenas privilégios de leitura.

    Consulta SESSION_PRIVS e SESSION_ROLES e rejeita se encontrar
    qualquer privilégio de escrita/DDL ou role perigosa.

    Raises:
        PermissionError: Se o user tiver privilégios além de leitura.
    """
    from sqlmentor.queries import dangerous_privileges, dangerous_roles

    cursor = conn.cursor()
    problems: list[str] = []

    try:
        sql, params = dangerous_privileges()
        cursor.execute(sql, params)
        bad_privs = [row[0] for row in cursor]
        if bad_privs:
            problems.append(f"Privilégios perigosos: {', '.join(bad_privs)}")

        sql, params = dangerous_roles()
        cursor.execute(sql, params)
        bad_roles = [row[0] for row in cursor]
        if bad_roles:
            problems.append(f"Roles perigosas: {', '.join(bad_roles)}")
    finally:
        cursor.close()

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
    """Retorna config completa de um profile."""
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


_thick_mode_initialized = False


def _init_thick_mode_if_available() -> None:
    """
    Tenta ativar thick mode se o Oracle Instant Client estiver disponível.

    Não explode se não encontrar — apenas re-raise o erro original
    com uma mensagem útil sobre como resolver.
    """
    global _thick_mode_initialized
    if _thick_mode_initialized:
        return
    try:
        oracledb.init_oracle_client()
        _thick_mode_initialized = True
        logger.info("oracledb: thick mode ativado via Oracle Instant Client")
    except oracledb.ProgrammingError:
        raise RuntimeError(
            "Este banco Oracle é antigo demais para o modo thin do oracledb.\n"
            "Opções:\n"
            "  1. Instale o Oracle Instant Client e adicione ao PATH\n"
            "     https://www.oracle.com/database/technologies/instant-client.html\n"
            "  2. Atualize o banco para Oracle 12c+ (suporta thin mode nativo)"
        )


def connect(name: str, timeout: int | None = None) -> oracledb.Connection:
    """
    Abre uma conexão Oracle a partir de um profile salvo.

    Tenta modo thin primeiro (zero dependências externas).
    Se o banco for muito antigo (DPY-3010), tenta thick mode
    automaticamente caso o Oracle Instant Client esteja no PATH.

    Args:
        name: Nome do profile de conexão.
        timeout: Timeout em segundos para operações no banco.
                 Se None, usa o valor do profile (default 180s).
                 Se 0, sem timeout.
    """
    cfg = get_connection_config(name)
    dsn = oracledb.makedsn(cfg["host"], cfg["port"], service_name=cfg["service"])

    # Resolve timeout: parâmetro explícito > config do profile > 180s
    effective_timeout = timeout if timeout is not None else cfg.get("timeout", 180)

    try:
        conn = oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            dsn=dsn,
        )
    except oracledb.DatabaseError as e:
        if "DPY-3010" not in str(e):
            raise

        logger.info("Thin mode não suportado por este banco, tentando thick mode...")
        _init_thick_mode_if_available()

        conn = oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            dsn=dsn,
        )

    # Seta call_timeout (em milissegundos, 0 = sem timeout)
    if effective_timeout > 0:
        conn.call_timeout = effective_timeout * 1000

    # Valida que o user não tem privilégios além de leitura
    try:
        validate_privileges(conn)
    except PermissionError:
        conn.close()
        raise

    return conn


def check_thick_mode_available() -> dict[str, str]:
    """
    Verifica se o Oracle Instant Client está disponível no ambiente.

    Retorna dict com available (bool como str) e detalhes.
    """
    global _thick_mode_initialized
    if _thick_mode_initialized:
        return {"available": "True", "detail": "Thick mode já inicializado"}
    try:
        oracledb.init_oracle_client()
        _thick_mode_initialized = True
        return {"available": "True", "detail": "Oracle Instant Client encontrado"}
    except Exception as e:
        return {"available": "False", "detail": str(e)}


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


def diagnose_connection(name: str) -> dict[str, str]:
    """
    Diagnóstico completo de uma conexão: versão, modo, schema, thick mode.

    Retorna dict com status, version, major_version, mode (thin/thick),
    schema, e needs_thick (se o banco precisa de thick mode).
    """
    cfg = get_connection_config(name)
    dsn = oracledb.makedsn(cfg["host"], cfg["port"], service_name=cfg["service"])
    mode = "thin"
    needs_thick = False

    try:
        conn = oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            dsn=dsn,
        )
    except oracledb.DatabaseError as e:
        if "DPY-3010" not in str(e):
            raise
        needs_thick = True
        _init_thick_mode_if_available()
        conn = oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            dsn=dsn,
        )
        mode = "thick"

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
        row = cursor.fetchone()
        version = row[0] if row else "unknown"

        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
        row = cursor.fetchone()
        current_schema = row[0] if row else "unknown"

        # Extrai major version (ex: "Oracle Database 11g" → 11)
        import re

        match = re.search(r"(\d+)", version)
        major = int(match.group(1)) if match else 0

        return {
            "status": "ok",
            "version": version,
            "major_version": str(major),
            "mode": mode,
            "schema": current_schema,
            "needs_thick": str(needs_thick),
        }
    finally:
        conn.close()
