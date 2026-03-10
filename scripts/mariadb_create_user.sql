-- =============================================================================
-- SqlMentor: Script de criação de usuário MariaDB 10.6+
-- =============================================================================
--
-- Cria um usuário read-only para o sqlmentor coletar metadata e planos
-- de execução.
--
-- Pré-requisitos:
--   - MariaDB 10.6+ com performance_schema habilitado
--   - Executar como root ou usuário com GRANT OPTION
--
-- Uso:
--   mysql -u root -p < mariadb_create_user.sql
--
-- Para customizar usuário/senha, edite as variáveis abaixo ou use sed:
--   sed -e "s/SQLMENTOR/meu_user/g" \
--       -e "s/MinhaSenha123/minha_senha/g" \
--       scripts/mariadb_create_user.sql | mysql -u root -p
--
-- =============================================================================

-- =============================================
-- 1. CRIAR USUÁRIO
-- =============================================

CREATE USER IF NOT EXISTS 'SQLMENTOR'@'%'
    IDENTIFIED BY 'MinhaSenha123';

-- =============================================
-- 2. PERMISSÕES MÍNIMAS
-- =============================================

-- performance_schema: runtime stats, wait events, digest lookup (inspect)
-- Requer performance_schema = ON no my.cnf/my.ini
GRANT SELECT ON performance_schema.* TO 'SQLMENTOR'@'%';

-- Leitura em todos os schemas (EXPLAIN/ANALYZE, SHOW CREATE TABLE/VIEW)
-- Equivalente ao SELECT ANY TABLE do Oracle.
GRANT SELECT, SHOW VIEW ON *.* TO 'SQLMENTOR'@'%';

-- Para restringir a schemas específicos em vez de *.*, substitua por:
--   GRANT SELECT, SHOW VIEW ON `meu_schema`.* TO 'SQLMENTOR'@'%';
-- Repita para cada schema que será analisado.

-- Opcional: necessário apenas se usar --execute com SQLs que chamam funções.
-- EXPLAIN FORMAT=JSON (plano estimado) não requer EXECUTE.
-- ANALYZE FORMAT=JSON (plano real) executa o SQL e precisa de EXECUTE nas funções.
--   GRANT EXECUTE ON `meu_schema`.* TO 'SQLMENTOR'@'%';

FLUSH PRIVILEGES;

-- =============================================
-- 3. VALIDAÇÃO
-- =============================================

SELECT '============================================' AS '';
SELECT 'Validação de permissões do SQLMENTOR' AS '';
SELECT '============================================' AS '';

-- Verificar usuário criado
SELECT
    CASE WHEN COUNT(*) > 0 THEN '[OK]   Usuário SQLMENTOR criado'
         ELSE '[ERRO] Usuário SQLMENTOR não encontrado'
    END AS resultado
FROM mysql.user
WHERE User = 'SQLMENTOR';

-- Verificar SELECT global
SELECT
    CASE WHEN COUNT(*) > 0 THEN '[OK]   SELECT (global)'
         ELSE '[ERRO] SELECT não concedido'
    END AS resultado
FROM information_schema.USER_PRIVILEGES
WHERE GRANTEE LIKE "'SQLMENTOR'%"
  AND PRIVILEGE_TYPE = 'SELECT';

-- Verificar SHOW VIEW global
SELECT
    CASE WHEN COUNT(*) > 0 THEN '[OK]   SHOW VIEW (global)'
         ELSE '[ERRO] SHOW VIEW não concedido'
    END AS resultado
FROM information_schema.USER_PRIVILEGES
WHERE GRANTEE LIKE "'SQLMENTOR'%"
  AND PRIVILEGE_TYPE = 'SHOW VIEW';


-- Verificar performance_schema habilitado
SELECT
    CASE WHEN VARIABLE_VALUE = 'ON' THEN '[OK]   performance_schema habilitado'
         ELSE '[AVISO] performance_schema desabilitado — inspect não funcionará'
    END AS resultado
FROM information_schema.GLOBAL_VARIABLES
WHERE VARIABLE_NAME = 'performance_schema';

-- Verificar ausência de privilégios perigosos
SELECT
    CASE WHEN COUNT(*) = 0 THEN '[OK]   Sem privilégios perigosos (INSERT/UPDATE/DELETE/DROP/ALTER/SUPER)'
         ELSE CONCAT('[AVISO] Privilégio perigoso encontrado: ', GROUP_CONCAT(PRIVILEGE_TYPE))
    END AS resultado
FROM information_schema.USER_PRIVILEGES
WHERE GRANTEE LIKE "'SQLMENTOR'%"
  AND PRIVILEGE_TYPE IN ('INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER',
                         'CREATE', 'GRANT OPTION', 'SUPER', 'SHUTDOWN',
                         'FILE', 'RELOAD', 'PROCESS');

SELECT '============================================' AS '';
SELECT 'Configurar no CLI:' AS '';
SELECT '  sqlmentor config add mariadb \\' AS '';
SELECT '    --name prod \\' AS '';
SELECT '    --host <HOST> --port 3306 \\' AS '';
SELECT '    --database <DATABASE> \\' AS '';
SELECT '    --user SQLMENTOR --schema <SCHEMA>' AS '';
SELECT '' AS '';
