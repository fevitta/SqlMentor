#!/bin/bash
# =============================================================================
# SqlMentor Integration Tests: setup completo no PDB (XEPDB1)
# Sem APP_USER — script .sh roda como oracle OS user, conecta via sqlplus.
# =============================================================================

sqlplus -s "SYS/${ORACLE_PASSWORD}@XEPDB1 AS SYSDBA" <<'EOSQL'

-- =============================================================
-- 1. Criar usuario read-only (mesma estrutura do oracle_create_user.sql)
-- =============================================================
CREATE USER SQLMENTOR_TEST IDENTIFIED BY "TestPwd123"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

GRANT CREATE SESSION TO SQLMENTOR_TEST;
GRANT SELECT_CATALOG_ROLE TO SQLMENTOR_TEST;
GRANT SELECT ANY TABLE TO SQLMENTOR_TEST;

CREATE ROLE SQLMENTOR_EXEC_ROLE;
GRANT SQLMENTOR_EXEC_ROLE TO SQLMENTOR_TEST;

-- Grants para criar objetos de teste (schema proprio)
GRANT CREATE TABLE TO SQLMENTOR_TEST;
GRANT CREATE VIEW TO SQLMENTOR_TEST;
GRANT CREATE PROCEDURE TO SQLMENTOR_TEST;

-- =============================================================
-- 2. Criar schema de teste (conectado como SYS, objetos no schema SQLMENTOR_TEST)
-- =============================================================

CREATE TABLE SQLMENTOR_TEST.DEPARTMENTS (
    dept_id    NUMBER(4)    NOT NULL,
    dept_name  VARCHAR2(50) NOT NULL,
    location   VARCHAR2(100),
    CONSTRAINT pk_departments PRIMARY KEY (dept_id)
);

CREATE TABLE SQLMENTOR_TEST.EMPLOYEES (
    emp_id      NUMBER(10)    NOT NULL,
    first_name  VARCHAR2(50)  NOT NULL,
    last_name   VARCHAR2(50)  NOT NULL,
    email       VARCHAR2(100),
    hire_date   DATE          NOT NULL,
    salary      NUMBER(10,2),
    dept_id     NUMBER(4),
    status      VARCHAR2(10)  DEFAULT 'ACTIVE',
    CONSTRAINT pk_employees PRIMARY KEY (emp_id),
    CONSTRAINT fk_emp_dept FOREIGN KEY (dept_id)
        REFERENCES SQLMENTOR_TEST.DEPARTMENTS(dept_id),
    CONSTRAINT chk_emp_status CHECK (status IN ('ACTIVE','INACTIVE','TERMINATED'))
);

CREATE INDEX SQLMENTOR_TEST.idx_emp_dept ON SQLMENTOR_TEST.EMPLOYEES(dept_id);
CREATE INDEX SQLMENTOR_TEST.idx_emp_name ON SQLMENTOR_TEST.EMPLOYEES(last_name, first_name);
CREATE INDEX SQLMENTOR_TEST.idx_emp_hire ON SQLMENTOR_TEST.EMPLOYEES(hire_date);
CREATE UNIQUE INDEX SQLMENTOR_TEST.idx_emp_email ON SQLMENTOR_TEST.EMPLOYEES(email);

CREATE TABLE SQLMENTOR_TEST.ORDERS (
    order_id    NUMBER(10)    NOT NULL,
    emp_id      NUMBER(10)    NOT NULL,
    order_date  DATE          NOT NULL,
    total       NUMBER(12,2),
    status      VARCHAR2(20)  DEFAULT 'PENDING',
    notes       VARCHAR2(500),
    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_ord_emp FOREIGN KEY (emp_id)
        REFERENCES SQLMENTOR_TEST.EMPLOYEES(emp_id)
);

CREATE INDEX SQLMENTOR_TEST.idx_ord_emp ON SQLMENTOR_TEST.ORDERS(emp_id);
CREATE INDEX SQLMENTOR_TEST.idx_ord_date ON SQLMENTOR_TEST.ORDERS(order_date);
CREATE INDEX SQLMENTOR_TEST.idx_ord_status ON SQLMENTOR_TEST.ORDERS(status);

CREATE OR REPLACE VIEW SQLMENTOR_TEST.V_ACTIVE_EMPLOYEES AS
    SELECT e.emp_id, e.first_name, e.last_name, e.email,
           e.salary, d.dept_name
    FROM SQLMENTOR_TEST.EMPLOYEES e
    JOIN SQLMENTOR_TEST.DEPARTMENTS d ON e.dept_id = d.dept_id
    WHERE e.status = 'ACTIVE';

CREATE OR REPLACE FUNCTION SQLMENTOR_TEST.FN_ANNUAL_SALARY(
    p_salary NUMBER
) RETURN NUMBER IS
BEGIN
    RETURN NVL(p_salary, 0) * 12;
END;
/

-- =============================================================
-- 3. Seed data
-- =============================================================

INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (10, 'Engineering', 'San Francisco');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (20, 'Marketing', 'New York');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (30, 'Finance', 'Chicago');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (40, 'HR', 'Austin');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (50, 'Operations', 'Seattle');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (60, 'Legal', 'Boston');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (70, 'Sales', 'Denver');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (80, 'Support', 'Portland');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (90, 'Research', 'San Diego');
INSERT INTO SQLMENTOR_TEST.DEPARTMENTS VALUES (100, 'Admin', 'Remote');
COMMIT;

BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO SQLMENTOR_TEST.EMPLOYEES (
            emp_id, first_name, last_name, email,
            hire_date, salary, dept_id, status
        ) VALUES (
            i,
            'First' || TO_CHAR(i),
            'Last' || TO_CHAR(i),
            'emp' || TO_CHAR(i) || '@test.com',
            DATE '2020-01-01' + MOD(i, 1000),
            30000 + MOD(i * 137, 70000),
            (MOD(i - 1, 10) + 1) * 10,
            CASE MOD(i, 10)
                WHEN 0 THEN 'INACTIVE'
                WHEN 9 THEN 'TERMINATED'
                ELSE 'ACTIVE'
            END
        );
    END LOOP;
    COMMIT;
END;
/

BEGIN
    FOR i IN 1..5000 LOOP
        INSERT INTO SQLMENTOR_TEST.ORDERS (
            order_id, emp_id, order_date, total, status, notes
        ) VALUES (
            i,
            MOD(i - 1, 1000) + 1,
            DATE '2023-01-01' + MOD(i, 730),
            ROUND(DBMS_RANDOM.VALUE(10, 10000), 2),
            CASE MOD(i, 5)
                WHEN 0 THEN 'COMPLETED'
                WHEN 1 THEN 'SHIPPED'
                WHEN 2 THEN 'PENDING'
                WHEN 3 THEN 'CANCELLED'
                ELSE 'PROCESSING'
            END,
            CASE WHEN MOD(i, 3) = 0 THEN 'Note for order ' || TO_CHAR(i) ELSE NULL END
        );
    END LOOP;
    COMMIT;
END;
/

-- =============================================================
-- 4. Gather statistics
-- =============================================================
BEGIN
    DBMS_STATS.GATHER_SCHEMA_STATS('SQLMENTOR_TEST');
END;
/

-- =============================================================
-- 5. Revogar CREATE TABLE/VIEW/PROCEDURE (user deve ser read-only)
-- =============================================================
REVOKE CREATE TABLE FROM SQLMENTOR_TEST;
REVOKE CREATE VIEW FROM SQLMENTOR_TEST;
REVOKE CREATE PROCEDURE FROM SQLMENTOR_TEST;

EXIT;
EOSQL
