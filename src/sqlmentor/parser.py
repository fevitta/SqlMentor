"""
Parser de SQL usando sqlglot.

Extrai tabelas, colunas, joins e tipo de statement de qualquer SQL/procedure Oracle.
"""

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

# Tabelas de sistema Oracle que não devem ser coletadas como objetos do usuário
_ORACLE_SYSTEM_TABLES = frozenset({"DUAL"})


@dataclass
class ParsedSQL:
    """Resultado do parsing de um SQL."""

    raw_sql: str
    sql_type: str  # SELECT, INSERT, UPDATE, DELETE, CREATE, PROCEDURE, etc.
    tables: list[dict] = field(default_factory=list)  # [{schema, name, alias}]
    where_columns: list[str] = field(default_factory=list)
    join_columns: list[str] = field(default_factory=list)
    order_columns: list[str] = field(default_factory=list)
    group_columns: list[str] = field(default_factory=list)
    cte_names: set[str] = field(default_factory=set)  # nomes de CTEs (WITH ... AS)
    functions: list[dict] = field(
        default_factory=list
    )  # [{schema, name}] — funções PL/SQL chamadas
    subqueries: int = 0
    is_parseable: bool = True
    parse_errors: list[str] = field(default_factory=list)

    @property
    def table_names(self) -> list[str]:
        """Lista simplificada de nomes de tabelas (schema.table ou table), ordenada."""
        result = []
        for t in self.tables:
            if t.get("schema"):
                result.append(f"{t['schema']}.{t['name']}")
            else:
                result.append(t["name"])
        return sorted(set(result))


def is_normalized_sql(sql_text: str) -> bool:
    """
    Detecta se o SQL parece ser normalizado (literais substituídos por '?').

    Heurística: conta '?' fora de strings. Se houver 2+ ocorrências, é normalizado.
    SQL Oracle normal não usa '?' — bind variables usam ':param'.

    Args:
        sql_text: SQL a ser verificado.

    Returns:
        True se o SQL parece normalizado.
    """
    count = 0
    in_single_quote = False
    in_double_quote = False

    for ch in sql_text:
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            continue
        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            continue
        if in_single_quote or in_double_quote:
            continue
        if ch == "?":
            count += 1
            if count >= 2:
                return True

    return False


def parse_sql(sql_text: str, default_schema: str | None = None) -> ParsedSQL:
    """
    Faz parse do SQL e extrai metadata estrutural.

    Args:
        sql_text: SQL completo (pode ser query, procedure, trigger, etc.)
        default_schema: Schema padrão caso não esteja qualificado no SQL.

    Returns:
        ParsedSQL com tabelas, colunas e metadata extraídos.
    """
    result = ParsedSQL(raw_sql=sql_text.strip().rstrip(";").strip(), sql_type="UNKNOWN")

    # Limpa SQL — remove terminadores e comentários de header comuns
    cleaned = sql_text.strip().rstrip(";").strip()

    # Tenta detectar o tipo antes do parse
    upper = cleaned.upper().lstrip()
    if upper.startswith("CREATE OR REPLACE PROCEDURE") or upper.startswith("CREATE PROCEDURE"):
        result.sql_type = "PROCEDURE"
    elif upper.startswith("CREATE OR REPLACE TRIGGER") or upper.startswith("CREATE TRIGGER"):
        result.sql_type = "TRIGGER"
    elif upper.startswith("CREATE OR REPLACE FUNCTION") or upper.startswith("CREATE FUNCTION"):
        result.sql_type = "FUNCTION"
    elif upper.startswith("CREATE OR REPLACE PACKAGE"):
        result.sql_type = "PACKAGE"

    # Pra procedures/triggers/packages, tenta extrair tabelas via regex como fallback
    if result.sql_type in ("PROCEDURE", "TRIGGER", "FUNCTION", "PACKAGE"):
        _extract_from_plsql(cleaned, result, default_schema)
        return result

    # Parse com sqlglot
    try:
        statements = sqlglot.parse(cleaned, dialect="oracle")
    except sqlglot.errors.ParseError as e:
        result.is_parseable = False
        result.parse_errors.append(str(e))
        # Fallback: tenta extrair tabelas via regex
        _extract_from_plsql(cleaned, result, default_schema)
        return result

    for statement in statements:
        if statement is None:
            continue

        # Tipo do statement
        if result.sql_type == "UNKNOWN":
            result.sql_type = type(statement).__name__.upper()
            # Normaliza nomes comuns
            type_map = {
                "SELECT": "SELECT",
                "INSERT": "INSERT",
                "UPDATE": "UPDATE",
                "DELETE": "DELETE",
                "MERGE": "MERGE",
                "CREATE": "CREATE",
            }
            for key, val in type_map.items():
                if key in result.sql_type:
                    result.sql_type = val
                    break

        # Coleta nomes de CTEs (WITH ... AS) pra não confundir com tabelas reais
        for cte in statement.find_all(exp.CTE):
            cte_alias = cte.alias
            if cte_alias:
                result.cte_names.add(cte_alias.upper())

        # Extrai tabelas (ignorando referências a CTEs e tabelas de sistema)
        for table in statement.find_all(exp.Table):
            # CTE nunca tem schema; se o nome bate com uma CTE, pula
            if not table.db and table.name and table.name.upper() in result.cte_names:
                continue
            # Ignora tabelas de sistema Oracle (DUAL, etc.)
            if table.name and table.name.upper() in _ORACLE_SYSTEM_TABLES:
                continue
            table_info = {
                "name": table.name,
                "schema": table.db or default_schema,
                "alias": table.alias or None,
            }
            if table.name and table_info not in result.tables:
                result.tables.append(table_info)

        # Colunas em WHERE
        for where in statement.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                col_str = f"{col.table}.{col.name}" if col.table else col.name
                if col_str not in result.where_columns:
                    result.where_columns.append(col_str)

        # Colunas em JOIN ON
        for join in statement.find_all(exp.Join):
            on_clause = join.find(exp.Condition)
            if on_clause:
                for col in on_clause.find_all(exp.Column):
                    col_str = f"{col.table}.{col.name}" if col.table else col.name
                    if col_str not in result.join_columns:
                        result.join_columns.append(col_str)

        # Colunas em ORDER BY
        for order in statement.find_all(exp.Order):
            for col in order.find_all(exp.Column):
                col_str = f"{col.table}.{col.name}" if col.table else col.name
                if col_str not in result.order_columns:
                    result.order_columns.append(col_str)

        # Colunas em GROUP BY
        for group in statement.find_all(exp.Group):
            for col in group.find_all(exp.Column):
                col_str = f"{col.table}.{col.name}" if col.table else col.name
                if col_str not in result.group_columns:
                    result.group_columns.append(col_str)

        # Conta subqueries
        result.subqueries = len(list(statement.find_all(exp.Subquery)))

    # Extrai funções PL/SQL schema-qualificadas via regex (sqlglot não captura bem)
    _extract_functions(sql_text, result, default_schema)

    return result


def denormalize_sql(sql_text: str, mode: str = "literal") -> tuple[str, dict]:
    """
    Substitui placeholders '?' de SQL normalizado (Datadog, OEM, etc.).

    Ferramentas de monitoramento normalizam SQL substituindo literais por '?'.
    Isso quebra o parser e o EXPLAIN PLAN. Esta função restaura o SQL para uma
    forma sintaticamente válida.

    Dois modos disponíveis:
    - "literal": substitui '?' por '1' (string literal). Funciona pra parse e
      EXPLAIN PLAN na maioria dos casos. Pode divergir do plano real se o tipo
      do literal influenciar a estimativa de cardinalidade.
    - "bind": substitui '?' por bind variables Oracle (:dn1, :dn2, ...).
      O EXPLAIN PLAN usa seletividade padrão sem depender de valores concretos.
      Retorna dict com as binds geradas (chave→None) pra passar ao cursor.

    Não altera bind variables Oracle já existentes (:param, :B1) — preservadas.

    Args:
        sql_text: SQL com placeholders '?' de normalização.
        mode: "literal" (default) ou "bind".

    Returns:
        Tupla (sql_transformado, bind_dict). bind_dict é vazio no modo literal.
    """
    result = []
    binds: dict[str, None] = {}
    bind_counter = 0
    i = 0
    in_single_quote = False
    in_double_quote = False

    while i < len(sql_text):
        ch = sql_text[i]

        # Rastreia strings (ignora ? dentro de strings)
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            result.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            result.append(ch)
            i += 1
            continue

        # Dentro de string, copia literal
        if in_single_quote or in_double_quote:
            result.append(ch)
            i += 1
            continue

        # Encontrou ? fora de string — substitui conforme modo
        if ch == "?":
            if mode == "bind":
                bind_counter += 1
                bind_name = f"dn{bind_counter}"
                result.append(f":{bind_name}")
                binds[bind_name] = None
            else:
                result.append("'1'")
            i += 1
            continue

        result.append(ch)
        i += 1

    return "".join(result), binds


def parse_bind_values(raw_binds: dict[str, str]) -> dict[str, str | int | float | None]:
    """Converte dict de bind values string para tipos Python adequados.

    Trata "null"/"none" como None, converte números quando possível,
    mantém strings como string.
    """
    result: dict[str, str | int | float | None] = {}
    for key, val in raw_binds.items():
        if val.lower() in ("null", "none"):
            result[key] = None
        else:
            try:
                result[key] = int(val)
            except ValueError:
                try:
                    result[key] = float(val)
                except ValueError:
                    result[key] = val
    return result


def detect_sql_binds(sql_text: str) -> list[str]:
    """Detecta nomes de bind variables (:param) no SQL, deduplicados e case-preserving."""
    import re

    sql_bind_names = re.findall(r"(?<!:):([A-Za-z_]\w*)", sql_text)
    seen_upper: set[str] = set()
    unique: list[str] = []
    for name in sql_bind_names:
        if name.upper() not in seen_upper:
            seen_upper.add(name.upper())
            unique.append(name)
    return unique


def remap_bind_params(
    bind_params: dict[str, str | int | float | None],
    sql_binds: list[str],
) -> dict[str, str | int | float | None]:
    """Remapeia bind_params pro case exato dos bind names encontrados no SQL."""
    if not bind_params or not sql_binds:
        return bind_params
    provided_upper = {k.upper(): v for k, v in bind_params.items()}
    remapped: dict[str, str | int | float | None] = {}
    for sql_name in sql_binds:
        if sql_name.upper() in provided_upper:
            remapped[sql_name] = provided_upper[sql_name.upper()]
    return remapped


def _extract_from_plsql(sql_text: str, result: ParsedSQL, default_schema: str | None) -> None:
    """
    Fallback: extrai tabelas de PL/SQL via parsing parcial.

    Procura por padrões como FROM table, JOIN table, INTO table,
    UPDATE table, INSERT INTO table, DELETE FROM table.
    """
    import re

    # Padrões que precedem nomes de tabela
    # Nota: INTO sozinho pega variáveis PL/SQL (SELECT INTO v_var).
    # Usamos INSERT INTO e MERGE INTO explicitamente pra evitar falsos positivos.
    patterns = [
        r"\bFROM\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
        r"\bJOIN\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
        r"\bINSERT\s+INTO\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
        r"\bUPDATE\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
        r"\bDELETE\s+FROM\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
        r"\bTRUNCATE\s+TABLE\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
        r"\bMERGE\s+INTO\s+([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)",
    ]

    # Palavras reservadas que podem aparecer em posições de tabela
    reserved = {
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "IN",
        "EXISTS",
        "NULL",
        "IS",
        "SET",
        "VALUES",
        "AS",
        "ON",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "IF",
        "LOOP",
        "BEGIN",
        "DECLARE",
        "EXCEPTION",
        "CURSOR",
        "OPEN",
        "CLOSE",
        "FETCH",
        "INTO",
        "BULK",
        "COLLECT",
        "FORALL",
        "DUAL",
        "TABLE",
        "INDEX",
        "VIEW",
        "SEQUENCE",
    }

    # Detecta nomes de CTEs (WITH nome AS (...)) pra não confundir com tabelas
    cte_pattern = r"\bWITH\s+(\w+)\s+AS\s*\("
    for m in re.finditer(cte_pattern, sql_text, re.IGNORECASE):
        result.cte_names.add(m.group(1).upper())

    found_tables = set()
    for pattern in patterns:
        for match in re.finditer(pattern, sql_text, re.IGNORECASE):
            table_ref = match.group(1)
            if table_ref.upper() not in reserved and table_ref.upper() not in result.cte_names:
                found_tables.add(table_ref)

    for table_ref in sorted(found_tables):
        parts = table_ref.split(".")
        if len(parts) == 2:
            result.tables.append(
                {
                    "name": parts[1],
                    "schema": parts[0],
                    "alias": None,
                }
            )
        else:
            result.tables.append(
                {
                    "name": parts[0],
                    "schema": default_schema,
                    "alias": None,
                }
            )


def _extract_functions(sql_text: str, result: ParsedSQL, default_schema: str | None) -> None:
    """
    Extrai funções PL/SQL chamadas no SQL via regex.

    Captura padrões como SCHEMA.FUNCTION_NAME(...) — funções Oracle
    custom que podem impactar performance (ex: chamadas row-by-row).
    Ignora funções built-in do Oracle (NVL, TO_CHAR, COUNT, etc.).
    """
    import re

    # Funções built-in Oracle que não interessam
    builtins = {
        "NVL",
        "NVL2",
        "COALESCE",
        "DECODE",
        "CASE",
        "CAST",
        "TO_CHAR",
        "TO_DATE",
        "TO_NUMBER",
        "TO_TIMESTAMP",
        "TO_CLOB",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "UPPER",
        "LOWER",
        "INITCAP",
        "SUBSTR",
        "INSTR",
        "REPLACE",
        "TRANSLATE",
        "LENGTH",
        "LPAD",
        "RPAD",
        "ROUND",
        "TRUNC",
        "CEIL",
        "FLOOR",
        "MOD",
        "ABS",
        "SIGN",
        "POWER",
        "SQRT",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "LISTAGG",
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "LEAD",
        "LAG",
        "FIRST_VALUE",
        "LAST_VALUE",
        "OVER",
        "PARTITION",
        "WITHIN",
        "SYSDATE",
        "SYSTIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIMESTAMP",
        "EXTRACT",
        "ADD_MONTHS",
        "MONTHS_BETWEEN",
        "LAST_DAY",
        "NEXT_DAY",
        "GREATEST",
        "LEAST",
        "NULLIF",
        "SYS_CONTEXT",
        "USERENV",
        "USER",
        "UID",
        "ROWNUM",
        "ROWID",
        "LEVEL",
        "CONNECT_BY_ROOT",
        "EXISTS",
        "NOT",
        "IN",
        "BETWEEN",
        "LIKE",
        "DBMS_METADATA",
        "DBMS_XPLAN",
        "TABLE",
    }

    # Padrão: SCHEMA.FUNCTION_NAME( — schema-qualificado
    pattern = r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*\("
    seen = set()
    for match in re.finditer(pattern, sql_text, re.IGNORECASE):
        schema_part = match.group(1).upper()
        func_name = match.group(2).upper()

        # Ignora se o "schema" é na verdade um alias de tabela usado no SQL
        table_aliases = {(t.get("alias") or "").upper() for t in result.tables}
        table_names = {t["name"].upper() for t in result.tables}
        # Se schema_part é alias ou nome de tabela, é acesso a coluna, não função
        if schema_part in table_aliases or schema_part in table_names:
            continue
        # Ignora built-ins
        if func_name in builtins or schema_part in builtins:
            continue
        # Ignora CTEs
        if schema_part in result.cte_names:
            continue

        key = f"{schema_part}.{func_name}"
        if key not in seen:
            seen.add(key)
            result.functions.append({"schema": schema_part, "name": func_name})
