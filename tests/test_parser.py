"""Testes para o modulo parser (sqlglot + regex fallback)."""

import pytest

from sqlmentor.parser import (
    _BIND_STYLE,
    _BUILTIN_FUNCTIONS,
    _SQLGLOT_DIALECT,
    _SYSTEM_TABLES,
    SUPPORTED_DIALECTS,
    ParsedSQL,
    _validate_dialect,
    denormalize_sql,
    detect_sql_binds,
    is_normalized_sql,
    parse_bind_values,
    parse_sql,
    remap_bind_params,
)

# ─── parse_sql: SELECT simples ───────────────────────────────────────────────


class TestParseSimpleSelect:
    def test_single_table(self):
        result = parse_sql("SELECT id, name FROM users")
        assert result.sql_type == "SELECT"
        assert result.is_parseable
        assert any(t["name"].upper() == "USERS" for t in result.tables)

    def test_qualified_table(self):
        result = parse_sql("SELECT * FROM hr.employees")
        assert any(
            t["name"].upper() == "EMPLOYEES" and t["schema"].upper() == "HR" for t in result.tables
        )

    def test_default_schema(self):
        result = parse_sql("SELECT * FROM orders", default_schema="MYSCHEMA")
        assert any(t["schema"] == "MYSCHEMA" for t in result.tables)

    def test_table_with_alias(self):
        result = parse_sql("SELECT e.id FROM employees e")
        assert any(t["name"].upper() == "EMPLOYEES" for t in result.tables)

    def test_dual_filtered(self):
        result = parse_sql("SELECT SYSDATE FROM DUAL")
        assert not any(t["name"].upper() == "DUAL" for t in result.tables)

    def test_table_names_property(self):
        result = parse_sql("SELECT * FROM hr.employees e JOIN hr.departments d ON e.dept_id = d.id")
        names = result.table_names
        assert isinstance(names, list)
        assert len(names) >= 2
        # Deve ser ordenado e unique
        assert names == sorted(set(names))


# ─── parse_sql: JOINs ────────────────────────────────────────────────────────


class TestParseJoins:
    def test_inner_join(self):
        sql = "SELECT * FROM employees e JOIN departments d ON e.dept_id = d.id"
        result = parse_sql(sql)
        assert len(result.tables) >= 2
        assert len(result.join_columns) > 0

    def test_left_join(self):
        sql = "SELECT * FROM orders o LEFT JOIN customers c ON o.cust_id = c.id"
        result = parse_sql(sql)
        assert any(t["name"].upper() == "ORDERS" for t in result.tables)
        assert any(t["name"].upper() == "CUSTOMERS" for t in result.tables)

    def test_multiple_joins(self):
        sql = """
        SELECT e.name, d.name, l.city
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        JOIN locations l ON d.loc_id = l.id
        """
        result = parse_sql(sql)
        assert len(result.tables) >= 3


# ─── parse_sql: WHERE, ORDER BY, GROUP BY ────────────────────────────────────


class TestParseClauseColumns:
    def test_where_columns(self):
        result = parse_sql("SELECT * FROM users WHERE status = 'ACTIVE' AND age > 18")
        assert len(result.where_columns) > 0

    def test_order_columns(self):
        result = parse_sql("SELECT * FROM users ORDER BY name, created_at")
        assert len(result.order_columns) > 0

    def test_group_columns(self):
        result = parse_sql("SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id")
        assert len(result.group_columns) > 0


# ─── parse_sql: CTEs ─────────────────────────────────────────────────────────


class TestParseCTEs:
    def test_cte_not_in_tables(self):
        sql = """
        WITH active_users AS (
            SELECT * FROM users WHERE status = 'ACTIVE'
        )
        SELECT * FROM active_users
        """
        result = parse_sql(sql)
        assert "ACTIVE_USERS" in result.cte_names
        # CTE não deve aparecer como tabela real
        assert not any(t["name"].upper() == "ACTIVE_USERS" for t in result.tables)
        # Mas a tabela real sim
        assert any(t["name"].upper() == "USERS" for t in result.tables)

    def test_multiple_ctes(self):
        sql = """
        WITH cte1 AS (SELECT * FROM t1),
             cte2 AS (SELECT * FROM t2)
        SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id
        """
        result = parse_sql(sql)
        assert "CTE1" in result.cte_names
        assert "CTE2" in result.cte_names


# ─── parse_sql: subqueries ───────────────────────────────────────────────────


class TestParseSubqueries:
    def test_subquery_count(self):
        sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        result = parse_sql(sql)
        assert result.subqueries >= 1

    def test_no_subquery(self):
        result = parse_sql("SELECT * FROM users")
        assert result.subqueries == 0


# ─── parse_sql: PL/SQL fallback ──────────────────────────────────────────────


class TestParsePLSQL:
    def test_procedure_detected(self):
        sql = """
        CREATE OR REPLACE PROCEDURE update_salary AS
        BEGIN
            UPDATE employees SET salary = salary * 1.1;
            INSERT INTO audit_log (action) VALUES ('salary_update');
        END;
        """
        result = parse_sql(sql)
        assert result.sql_type == "PROCEDURE"
        assert any(t["name"].upper() == "EMPLOYEES" for t in result.tables)
        assert any(t["name"].upper() == "AUDIT_LOG" for t in result.tables)

    def test_trigger_detected(self):
        sql = "CREATE OR REPLACE TRIGGER trg_audit AFTER INSERT ON orders BEGIN NULL; END;"
        result = parse_sql(sql)
        assert result.sql_type == "TRIGGER"

    def test_function_detected(self):
        sql = "CREATE OR REPLACE FUNCTION get_name RETURN VARCHAR2 AS BEGIN RETURN 'x'; END;"
        result = parse_sql(sql)
        assert result.sql_type == "FUNCTION"


# ─── parse_sql: edge cases ───────────────────────────────────────────────────


class TestParseEdgeCases:
    def test_empty_sql(self):
        result = parse_sql("")
        assert result.sql_type in ("UNKNOWN",)

    def test_whitespace_only(self):
        result = parse_sql("   \n  \t  ")
        assert result.sql_type in ("UNKNOWN",)

    def test_insert(self):
        result = parse_sql("INSERT INTO users (name) VALUES ('test')")
        assert result.sql_type == "INSERT"
        assert any(t["name"].upper() == "USERS" for t in result.tables)

    def test_update(self):
        result = parse_sql("UPDATE users SET name = 'new' WHERE id = 1")
        assert result.sql_type == "UPDATE"

    def test_delete(self):
        result = parse_sql("DELETE FROM users WHERE id = 1")
        assert result.sql_type == "DELETE"

    def test_parse_errors_recorded(self):
        """SQL inválido deve registrar erros mas não explodir."""
        result = parse_sql("THIS IS NOT VALID SQL AT ALL @@!!")
        # Pode ou não parsear parcialmente, mas não deve levantar exceção
        assert isinstance(result, ParsedSQL)


# ─── parse_sql: functions extraction ──────────────────────────────────────────


class TestParseFunctions:
    def test_schema_qualified_function(self):
        sql = "SELECT pkg_utils.get_status(id) FROM orders"
        result = parse_sql(sql)
        assert any(
            f["schema"].upper() == "PKG_UTILS" and f["name"].upper() == "GET_STATUS"
            for f in result.functions
        )

    def test_builtin_ignored(self):
        sql = "SELECT NVL(name, 'N/A'), TO_CHAR(created_at, 'YYYY') FROM users"
        result = parse_sql(sql)
        # NVL e TO_CHAR são builtins, não devem aparecer
        assert not any(f["name"].upper() in ("NVL", "TO_CHAR") for f in result.functions)


# ─── is_normalized_sql ────────────────────────────────────────────────────────


class TestIsNormalized:
    def test_normalized_with_many_placeholders(self):
        sql = "SELECT * FROM users WHERE id = ? AND name = ? AND status = ?"
        assert is_normalized_sql(sql)

    def test_two_placeholders_is_normalized(self):
        sql = "SELECT * FROM t WHERE a = ? AND b = ?"
        assert is_normalized_sql(sql)

    def test_normal_oracle_sql(self):
        sql = "SELECT * FROM users WHERE id = :id AND name = :name"
        assert not is_normalized_sql(sql)

    def test_single_placeholder_not_normalized(self):
        sql = "SELECT * FROM users WHERE id = ?"
        assert not is_normalized_sql(sql)

    def test_placeholder_inside_string_ignored(self):
        sql = "SELECT * FROM users WHERE name = '? not a placeholder' AND id = 1"
        assert not is_normalized_sql(sql)

    def test_empty_sql(self):
        assert not is_normalized_sql("")


# ─── denormalize_sql ──────────────────────────────────────────────────────────


class TestDenormalize:
    def test_literal_mode(self):
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        result, binds = denormalize_sql(sql, mode="literal")
        assert "?" not in result
        assert "'1'" in result
        assert binds == {}

    def test_bind_mode(self):
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        result, binds = denormalize_sql(sql, mode="bind")
        assert "?" not in result
        assert ":dn1" in result
        assert ":dn2" in result
        assert "dn1" in binds
        assert "dn2" in binds

    def test_preserves_existing_binds(self):
        sql = "SELECT * FROM users WHERE id = :id AND name = ?"
        result, _ = denormalize_sql(sql, mode="literal")
        assert ":id" in result
        assert "?" not in result

    def test_placeholder_in_string_preserved(self):
        sql = "SELECT '?' FROM users WHERE id = ?"
        result, _ = denormalize_sql(sql, mode="literal")
        # O ? dentro da string deve ser preservado
        assert "'?'" in result

    def test_no_placeholders(self):
        sql = "SELECT * FROM users WHERE id = 1"
        result, binds = denormalize_sql(sql, mode="literal")
        assert result == sql
        assert binds == {}


# ─── parse_bind_values ────────────────────────────────────────────────────────


class TestParseBindValues:
    def test_null_value(self):
        result = parse_bind_values({"a": "null"})
        assert result["a"] is None

    def test_none_value(self):
        result = parse_bind_values({"a": "None"})
        assert result["a"] is None

    def test_int_value(self):
        result = parse_bind_values({"a": "123"})
        assert result["a"] == 123
        assert isinstance(result["a"], int)

    def test_float_value(self):
        result = parse_bind_values({"a": "3.14"})
        assert result["a"] == 3.14
        assert isinstance(result["a"], float)

    def test_string_value(self):
        result = parse_bind_values({"a": "hello"})
        assert result["a"] == "hello"

    def test_empty_dict(self):
        assert parse_bind_values({}) == {}

    def test_mixed_values(self):
        result = parse_bind_values({"a": "123", "b": "null", "c": "text"})
        assert result["a"] == 123
        assert result["b"] is None
        assert result["c"] == "text"


# ─── detect_sql_binds ────────────────────────────────────────────────────────


class TestDetectSqlBinds:
    def test_simple_binds(self):
        result = detect_sql_binds("SELECT * FROM t WHERE id = :id AND name = :name")
        assert "id" in result
        assert "name" in result

    def test_no_binds(self):
        assert detect_sql_binds("SELECT * FROM t WHERE id = 1") == []

    def test_deduplication(self):
        result = detect_sql_binds("SELECT * FROM t WHERE a = :id OR b = :ID")
        assert len(result) == 1

    def test_ignores_double_colon(self):
        result = detect_sql_binds("SELECT col::integer FROM t WHERE id = :id")
        assert result == ["id"]


# ─── remap_bind_params ───────────────────────────────────────────────────────


class TestRemapBindParams:
    def test_case_remapping(self):
        params = {"ID": 1, "NAME": "test"}
        sql_binds = ["id", "name"]
        result = remap_bind_params(params, sql_binds)
        assert result == {"id": 1, "name": "test"}

    def test_empty_params(self):
        assert remap_bind_params({}, ["id"]) == {}

    def test_empty_binds(self):
        assert remap_bind_params({"id": 1}, []) == {"id": 1}


# ─── dialect validation ────────────────────────────────────────────────────


class TestDialectValidation:
    def test_valid_dialects(self):
        for d in SUPPORTED_DIALECTS:
            assert _validate_dialect(d) == d

    def test_case_insensitive(self):
        assert _validate_dialect("Oracle") == "oracle"
        assert _validate_dialect("POSTGRESQL") == "postgresql"
        assert _validate_dialect("MariaDB") == "mariadb"

    def test_invalid_dialect_raises(self):
        with pytest.raises(ValueError, match="nao suportado"):
            _validate_dialect("sqlite")

    def test_mysql_is_not_valid(self):
        with pytest.raises(ValueError):
            _validate_dialect("mysql")

    def test_postgres_is_not_valid(self):
        with pytest.raises(ValueError):
            _validate_dialect("postgres")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _validate_dialect("")

    def test_parse_sql_invalid_dialect(self):
        with pytest.raises(ValueError, match="nao suportado"):
            parse_sql("SELECT 1", dialect="sqlite")

    def test_denormalize_invalid_dialect(self):
        with pytest.raises(ValueError, match="nao suportado"):
            denormalize_sql("SELECT ?", dialect="sqlite")

    def test_is_normalized_invalid_dialect(self):
        with pytest.raises(ValueError, match="nao suportado"):
            is_normalized_sql("SELECT ?", dialect="sqlite")


# ─── parse_sql: dialect-aware system tables ─────────────────────────────────


class TestParseDialectSystemTables:
    def test_oracle_filters_dual(self):
        result = parse_sql("SELECT SYSDATE FROM DUAL", dialect="oracle")
        assert not any(t["name"].upper() == "DUAL" for t in result.tables)

    def test_postgresql_does_not_filter_dual(self):
        """PostgreSQL nao tem DUAL como tabela de sistema."""
        result = parse_sql("SELECT 1 FROM DUAL", dialect="postgresql")
        # DUAL nao esta no _SYSTEM_TABLES do PostgreSQL, deve aparecer como tabela
        assert any(t["name"].upper() == "DUAL" for t in result.tables)

    def test_postgresql_filters_information_schema(self):
        """PostgreSQL filtra information_schema como tabela de sistema."""
        result = parse_sql("SELECT * FROM information_schema.tables", dialect="postgresql")
        # information_schema como nome de tabela deve ser filtrada
        # Nota: sqlglot pode interpretar como schema.table, entao o nome "tables" aparece
        assert not any(t["name"].upper() == "INFORMATION_SCHEMA" for t in result.tables)

    def test_mariadb_filters_dual(self):
        result = parse_sql("SELECT 1 FROM DUAL", dialect="mariadb")
        assert not any(t["name"].upper() == "DUAL" for t in result.tables)

    def test_mariadb_filters_information_schema(self):
        result = parse_sql("SELECT * FROM information_schema.tables", dialect="mariadb")
        assert not any(t["name"].upper() == "INFORMATION_SCHEMA" for t in result.tables)

    def test_oracle_select_preserves_real_tables(self):
        """Garante que tabelas reais nao sao filtradas em nenhum dialeto."""
        for d in SUPPORTED_DIALECTS:
            result = parse_sql("SELECT * FROM users WHERE id = 1", dialect=d)
            assert any(t["name"].upper() == "USERS" for t in result.tables)


# ─── parse_sql: dialect-aware sqlglot parsing ───────────────────────────────


class TestParseDialectSqlglot:
    def test_postgresql_basic_select(self):
        sql = "SELECT u.id, u.name FROM users u WHERE u.active = true"
        result = parse_sql(sql, dialect="postgresql")
        assert result.sql_type == "SELECT"
        assert result.is_parseable
        assert any(t["name"].upper() == "USERS" for t in result.tables)

    def test_mariadb_basic_select(self):
        sql = "SELECT id, name FROM users WHERE status = 'active'"
        result = parse_sql(sql, dialect="mariadb")
        assert result.sql_type == "SELECT"
        assert result.is_parseable
        assert any(t["name"].upper() == "USERS" for t in result.tables)

    def test_postgresql_with_join(self):
        sql = """
        SELECT e.name, d.name
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        WHERE e.active = true
        """
        result = parse_sql(sql, dialect="postgresql")
        assert len(result.tables) >= 2
        assert len(result.join_columns) > 0

    def test_mariadb_with_join(self):
        sql = """
        SELECT e.name, d.name
        FROM employees e
        INNER JOIN departments d ON e.dept_id = d.id
        """
        result = parse_sql(sql, dialect="mariadb")
        assert len(result.tables) >= 2

    def test_default_dialect_is_oracle(self):
        """Sem dialeto explicito, deve usar Oracle (backward compat)."""
        result = parse_sql("SELECT SYSDATE FROM DUAL")
        assert not any(t["name"].upper() == "DUAL" for t in result.tables)


# ─── _extract_functions: dialect-aware builtins ─────────────────────────────


class TestExtractFunctionsDialect:
    def test_oracle_ignores_nvl(self):
        sql = "SELECT pkg.custom_fn(id), NVL(name, 'N/A') FROM users"
        result = parse_sql(sql, dialect="oracle")
        assert any(f["name"].upper() == "CUSTOM_FN" for f in result.functions)
        assert not any(f["name"].upper() == "NVL" for f in result.functions)

    def test_postgresql_ignores_string_agg(self):
        sql = "SELECT pkg.custom_fn(id), STRING_AGG(name, ',') FROM users"
        result = parse_sql(sql, dialect="postgresql")
        assert any(f["name"].upper() == "CUSTOM_FN" for f in result.functions)
        assert not any(f["name"].upper() == "STRING_AGG" for f in result.functions)

    def test_mariadb_ignores_ifnull(self):
        sql = "SELECT pkg.custom_fn(id), IFNULL(name, 'N/A') FROM users"
        result = parse_sql(sql, dialect="mariadb")
        assert any(f["name"].upper() == "CUSTOM_FN" for f in result.functions)
        assert not any(f["name"].upper() == "IFNULL" for f in result.functions)

    def test_oracle_specific_builtin_not_ignored_in_pg(self):
        """NVL e Oracle-only — nao deve ser filtrada em PostgreSQL."""
        # NVL nao esta no builtins de PG, mas quando usada como schema.NVL(...)
        # o schema seria detectado como funcao. Testamos que o builtins set e correto.
        assert "NVL" in _BUILTIN_FUNCTIONS["oracle"]
        assert "NVL" not in _BUILTIN_FUNCTIONS["postgresql"]

    def test_pg_specific_builtin_not_in_oracle(self):
        assert "ILIKE" in _BUILTIN_FUNCTIONS["postgresql"]
        assert "ILIKE" not in _BUILTIN_FUNCTIONS["oracle"]

    def test_mariadb_specific_builtin_not_in_oracle(self):
        assert "IFNULL" in _BUILTIN_FUNCTIONS["mariadb"]
        assert "IFNULL" not in _BUILTIN_FUNCTIONS["oracle"]


# ─── denormalize_sql: dialect-aware bind styles ─────────────────────────────


class TestDenormalizeDialect:
    def test_oracle_bind_mode(self):
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        result, binds = denormalize_sql(sql, mode="bind", dialect="oracle")
        assert "?" not in result
        assert ":dn1" in result
        assert ":dn2" in result
        assert "dn1" in binds
        assert "dn2" in binds

    def test_postgresql_bind_mode(self):
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        result, binds = denormalize_sql(sql, mode="bind", dialect="postgresql")
        assert "?" not in result
        assert "%(dn1)s" in result
        assert "%(dn2)s" in result
        assert "dn1" in binds
        assert "dn2" in binds

    def test_mariadb_bind_mode_noop(self):
        """MariaDB usa '?' nativamente — bind mode nao altera o SQL."""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        result, binds = denormalize_sql(sql, mode="bind", dialect="mariadb")
        assert result == sql  # inalterado
        assert binds == {}  # sem binds gerados

    def test_literal_mode_same_for_all_dialects(self):
        """Modo literal deve se comportar igual em todos os dialetos."""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        for d in SUPPORTED_DIALECTS:
            result, binds = denormalize_sql(sql, mode="literal", dialect=d)
            assert "?" not in result
            assert "'1'" in result
            assert binds == {}

    def test_postgresql_bind_preserves_existing_colon_binds(self):
        sql = "SELECT * FROM users WHERE id = :id AND name = ?"
        result, binds = denormalize_sql(sql, mode="bind", dialect="postgresql")
        assert ":id" in result  # preservado
        assert "%(dn1)s" in result
        assert "dn1" in binds

    def test_default_dialect_is_oracle(self):
        """Sem dialeto explicito, deve usar Oracle bind style."""
        sql = "SELECT * FROM t WHERE a = ? AND b = ?"
        result, _binds = denormalize_sql(sql, mode="bind")
        assert ":dn1" in result
        assert ":dn2" in result


# ─── is_normalized_sql: dialect param ───────────────────────────────────────


class TestIsNormalizedDialect:
    def test_works_for_all_dialects(self):
        """A heuristica de '?' e universal — deve funcionar com qualquer dialeto."""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        for d in SUPPORTED_DIALECTS:
            assert is_normalized_sql(sql, dialect=d)

    def test_not_normalized_for_all_dialects(self):
        sql = "SELECT * FROM users WHERE id = 1"
        for d in SUPPORTED_DIALECTS:
            assert not is_normalized_sql(sql, dialect=d)


# ─── config dicts integrity ────────────────────────────────────────────────


class TestConfigDictsIntegrity:
    def test_all_dialects_have_system_tables(self):
        for d in SUPPORTED_DIALECTS:
            assert d in _SYSTEM_TABLES
            assert isinstance(_SYSTEM_TABLES[d], frozenset)

    def test_all_dialects_have_builtin_functions(self):
        for d in SUPPORTED_DIALECTS:
            assert d in _BUILTIN_FUNCTIONS
            assert isinstance(_BUILTIN_FUNCTIONS[d], frozenset)
            assert len(_BUILTIN_FUNCTIONS[d]) > 0

    def test_common_builtins_across_dialects(self):
        """Funcoes comuns a todos os dialetos devem estar em todos."""
        common = {"COUNT", "SUM", "AVG", "MIN", "MAX", "UPPER", "LOWER", "ROUND"}
        for d in SUPPORTED_DIALECTS:
            for fn in common:
                assert fn in _BUILTIN_FUNCTIONS[d], f"{fn} ausente em {d}"

    def test_sqlglot_dialect_has_all_dialects(self):
        for d in SUPPORTED_DIALECTS:
            assert d in _SQLGLOT_DIALECT

    def test_bind_style_has_all_dialects(self):
        for d in SUPPORTED_DIALECTS:
            assert d in _BIND_STYLE
