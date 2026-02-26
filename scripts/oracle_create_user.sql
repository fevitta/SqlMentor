-- =============================================================================
-- SqlMentor: Script de criação de usuário Oracle 11g
-- =============================================================================
--
-- Cria um usuário read-only para o sqlmentor coletar metadata e planos
-- de execução.
--
-- Uso:
--   sqlplus / as sysdba @oracle_create_user.sql SQL_TUNER MinhaSenha123 USERS TEMP
--
-- Parâmetros:
--   &1 - Nome do usuário       (ex: SQL_TUNER)
--   &2 - Senha                 (será solicitada se omitida)
--   &3 - Tablespace padrão     (ex: USERS)
--   &4 - Tablespace temporário (ex: TEMP)
--
-- =============================================================================

SET VERIFY OFF
SET FEEDBACK ON

DEFINE TUNER_USER = &1
DEFINE TUNER_PASS = &2
DEFINE DEF_TS     = &3
DEFINE TEMP_TS    = &4

PROMPT
PROMPT Criando usuário &&TUNER_USER ...
PROMPT

CREATE USER &&TUNER_USER
    IDENTIFIED BY "&&TUNER_PASS"
    DEFAULT TABLESPACE &&DEF_TS
    TEMPORARY TABLESPACE &&TEMP_TS
    QUOTA 50M ON &&DEF_TS;

-- Sessão + dicionário + packages (DBMS_XPLAN, DBMS_METADATA, V$*, ALL_*)
GRANT CREATE SESSION, SELECT_CATALOG_ROLE TO &&TUNER_USER;

-- Leitura em todas as tabelas (EXPLAIN PLAN, GET_DDL)
GRANT SELECT ANY TABLE TO &&TUNER_USER;

-- =============================================================================
-- ROLE PARA FUNÇÕES PL/SQL
-- =============================================================================
-- O EXPLAIN PLAN precisa de EXECUTE nas funções PL/SQL referenciadas nos SQLs.
-- Crie a role e adicione grants conforme os SQLs que serão analisados.
--
-- Exemplo:
--   GRANT EXECUTE ON SCHEMA.FNC_GET_DOC_TYPE TO SQLMENTOR_EXEC_ROLE;
--   GRANT EXECUTE ON SCHEMA.FNC_GET_STATUS_BY_CODE TO SQLMENTOR_EXEC_ROLE;
--
-- Se o EXPLAIN PLAN falhar com ORA-01031, o sqlmentor mostra a linha do SQL
-- com a função que precisa de grant.
-- =============================================================================

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_exists FROM dba_roles WHERE role = 'SQLMENTOR_EXEC_ROLE';
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE ROLE SQLMENTOR_EXEC_ROLE';
    END IF;
END;
/

GRANT SQLMENTOR_EXEC_ROLE TO &&TUNER_USER;

-- =============================================================================
-- VALIDAÇÃO
-- =============================================================================

SET SERVEROUTPUT ON
DECLARE
    v NUMBER;
    PROCEDURE chk(p_label VARCHAR2, p_ok BOOLEAN) IS
    BEGIN
        IF p_ok THEN
            DBMS_OUTPUT.PUT_LINE('[OK]   ' || p_label);
        ELSE
            DBMS_OUTPUT.PUT_LINE('[ERRO] ' || p_label);
        END IF;
    END;
BEGIN
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('============================================');

    SELECT COUNT(*) INTO v FROM dba_users
    WHERE username = UPPER('&&TUNER_USER');
    chk('Usuário &&TUNER_USER', v > 0);

    SELECT COUNT(*) INTO v FROM dba_sys_privs
    WHERE grantee = UPPER('&&TUNER_USER') AND privilege = 'CREATE SESSION';
    chk('CREATE SESSION', v > 0);

    SELECT COUNT(*) INTO v FROM dba_role_privs
    WHERE grantee = UPPER('&&TUNER_USER') AND granted_role = 'SELECT_CATALOG_ROLE';
    chk('SELECT_CATALOG_ROLE', v > 0);

    SELECT COUNT(*) INTO v FROM dba_sys_privs
    WHERE grantee = UPPER('&&TUNER_USER') AND privilege = 'SELECT ANY TABLE';
    chk('SELECT ANY TABLE', v > 0);

    SELECT COUNT(*) INTO v FROM dba_role_privs
    WHERE grantee = UPPER('&&TUNER_USER') AND granted_role = 'SQLMENTOR_EXEC_ROLE';
    chk('SQLMENTOR_EXEC_ROLE', v > 0);

    DBMS_OUTPUT.PUT_LINE('============================================');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Configurar no cli:');
    DBMS_OUTPUT.PUT_LINE('  sqlmentor config add \');
    DBMS_OUTPUT.PUT_LINE('    --name prod \');
    DBMS_OUTPUT.PUT_LINE('    --host <HOST> --port 1521 \');
    DBMS_OUTPUT.PUT_LINE('    --service <SERVICE> \');
    DBMS_OUTPUT.PUT_LINE('    --user &&TUNER_USER --schema <SCHEMA>');
    DBMS_OUTPUT.PUT_LINE('');
END;
/

SET VERIFY ON
