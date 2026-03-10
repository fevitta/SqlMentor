-- =============================================================================
-- SqlMentor MariaDB Integration Tests: schema + seed data
-- Roda automaticamente pelo entrypoint do container MariaDB.
-- Todos os nomes de objetos em UPPERCASE (lower_case_table_names=0).
-- Database criado UPPERCASE para match com collector (.upper()).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS SQLMENTOR_TEST;
USE SQLMENTOR_TEST;

-- =============================================================
-- 1. Tabelas
-- =============================================================

CREATE TABLE DEPARTMENTS (
    DEPT_ID    INT          NOT NULL,
    DEPT_NAME  VARCHAR(50)  NOT NULL,
    LOCATION   VARCHAR(100),
    CONSTRAINT PK_DEPARTMENTS PRIMARY KEY (DEPT_ID)
) ENGINE=InnoDB;

CREATE TABLE EMPLOYEES (
    EMP_ID      INT          NOT NULL,
    FIRST_NAME  VARCHAR(50)  NOT NULL,
    LAST_NAME   VARCHAR(50)  NOT NULL,
    EMAIL       VARCHAR(100),
    HIRE_DATE   DATE         NOT NULL,
    SALARY      DECIMAL(10,2),
    DEPT_ID     INT,
    STATUS      VARCHAR(10)  DEFAULT 'ACTIVE',
    CONSTRAINT PK_EMPLOYEES PRIMARY KEY (EMP_ID),
    CONSTRAINT FK_EMP_DEPT FOREIGN KEY (DEPT_ID)
        REFERENCES DEPARTMENTS(DEPT_ID),
    CONSTRAINT CHK_EMP_STATUS CHECK (STATUS IN ('ACTIVE','INACTIVE','TERMINATED'))
) ENGINE=InnoDB;

CREATE INDEX IDX_EMP_DEPT ON EMPLOYEES(DEPT_ID);
CREATE INDEX IDX_EMP_NAME ON EMPLOYEES(LAST_NAME, FIRST_NAME);
CREATE INDEX IDX_EMP_HIRE ON EMPLOYEES(HIRE_DATE);
CREATE UNIQUE INDEX IDX_EMP_EMAIL ON EMPLOYEES(EMAIL);

CREATE TABLE ORDERS (
    ORDER_ID    INT          NOT NULL,
    EMP_ID      INT          NOT NULL,
    ORDER_DATE  DATE         NOT NULL,
    TOTAL       DECIMAL(12,2),
    STATUS      VARCHAR(20)  DEFAULT 'PENDING',
    NOTES       VARCHAR(500),
    CONSTRAINT PK_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_ORD_EMP FOREIGN KEY (EMP_ID)
        REFERENCES EMPLOYEES(EMP_ID)
) ENGINE=InnoDB;

CREATE INDEX IDX_ORD_EMP ON ORDERS(EMP_ID);
CREATE INDEX IDX_ORD_DATE ON ORDERS(ORDER_DATE);
CREATE INDEX IDX_ORD_STATUS ON ORDERS(STATUS);

CREATE TABLE ORDER_ARCHIVE (
    ARCHIVE_ID   INT          NOT NULL,
    ORDER_ID     INT          NOT NULL,
    EMP_ID       INT          NOT NULL,
    ORDER_DATE   DATE         NOT NULL,
    TOTAL        DECIMAL(12,2),
    ARCHIVED_AT  DATETIME     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT PK_ORDER_ARCHIVE PRIMARY KEY (ARCHIVE_ID, ORDER_DATE)
) ENGINE=InnoDB
PARTITION BY RANGE (YEAR(ORDER_DATE)) (
    PARTITION P2022 VALUES LESS THAN (2023),
    PARTITION P2023 VALUES LESS THAN (2024),
    PARTITION P2024 VALUES LESS THAN (2025),
    PARTITION PMAX  VALUES LESS THAN MAXVALUE
);

-- =============================================================
-- 2. View e Function
-- =============================================================

CREATE VIEW V_ACTIVE_EMPLOYEES AS
    SELECT e.EMP_ID, e.FIRST_NAME, e.LAST_NAME, e.EMAIL,
           e.SALARY, d.DEPT_NAME
    FROM EMPLOYEES e
    JOIN DEPARTMENTS d ON e.DEPT_ID = d.DEPT_ID
    WHERE e.STATUS = 'ACTIVE';

DELIMITER //
CREATE FUNCTION FN_ANNUAL_SALARY(p_salary DECIMAL(10,2))
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    RETURN IFNULL(p_salary, 0) * 12;
END //
DELIMITER ;

-- =============================================================
-- 3. Seed data
-- =============================================================

INSERT INTO DEPARTMENTS VALUES (10, 'Engineering', 'San Francisco');
INSERT INTO DEPARTMENTS VALUES (20, 'Marketing', 'New York');
INSERT INTO DEPARTMENTS VALUES (30, 'Finance', 'Chicago');
INSERT INTO DEPARTMENTS VALUES (40, 'HR', 'Austin');
INSERT INTO DEPARTMENTS VALUES (50, 'Operations', 'Seattle');
INSERT INTO DEPARTMENTS VALUES (60, 'Legal', 'Boston');
INSERT INTO DEPARTMENTS VALUES (70, 'Sales', 'Denver');
INSERT INTO DEPARTMENTS VALUES (80, 'Support', 'Portland');
INSERT INTO DEPARTMENTS VALUES (90, 'Research', 'San Diego');
INSERT INTO DEPARTMENTS VALUES (100, 'Admin', 'Remote');

-- Employees: 1000 rows via stored procedure
DELIMITER //
CREATE PROCEDURE SEED_EMPLOYEES()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
        INSERT INTO EMPLOYEES (
            EMP_ID, FIRST_NAME, LAST_NAME, EMAIL,
            HIRE_DATE, SALARY, DEPT_ID, STATUS
        ) VALUES (
            i,
            CONCAT('First', i),
            CONCAT('Last', i),
            CONCAT('emp', i, '@test.com'),
            DATE_ADD('2020-01-01', INTERVAL MOD(i, 1000) DAY),
            30000 + MOD(i * 137, 70000),
            (MOD(i - 1, 10) + 1) * 10,
            CASE MOD(i, 10)
                WHEN 0 THEN 'INACTIVE'
                WHEN 9 THEN 'TERMINATED'
                ELSE 'ACTIVE'
            END
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL SEED_EMPLOYEES();
DROP PROCEDURE SEED_EMPLOYEES;

-- Orders: 5000 rows via stored procedure
DELIMITER //
CREATE PROCEDURE SEED_ORDERS()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 5000 DO
        INSERT INTO ORDERS (
            ORDER_ID, EMP_ID, ORDER_DATE, TOTAL, STATUS, NOTES
        ) VALUES (
            i,
            MOD(i - 1, 1000) + 1,
            DATE_ADD('2023-01-01', INTERVAL MOD(i, 730) DAY),
            ROUND(10 + RAND() * 9990, 2),
            CASE MOD(i, 5)
                WHEN 0 THEN 'COMPLETED'
                WHEN 1 THEN 'SHIPPED'
                WHEN 2 THEN 'PENDING'
                WHEN 3 THEN 'CANCELLED'
                ELSE 'PROCESSING'
            END,
            CASE WHEN MOD(i, 3) = 0 THEN CONCAT('Note for order ', i) ELSE NULL END
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL SEED_ORDERS();
DROP PROCEDURE SEED_ORDERS;

-- Order Archive: 200 rows
DELIMITER //
CREATE PROCEDURE SEED_ORDER_ARCHIVE()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 200 DO
        INSERT INTO ORDER_ARCHIVE (
            ARCHIVE_ID, ORDER_ID, EMP_ID, ORDER_DATE, TOTAL
        ) VALUES (
            i,
            i,
            MOD(i - 1, 100) + 1,
            DATE_ADD('2022-01-01', INTERVAL MOD(i * 7, 1095) DAY),
            ROUND(10 + RAND() * 9990, 2)
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
CALL SEED_ORDER_ARCHIVE();
DROP PROCEDURE SEED_ORDER_ARCHIVE;

-- =============================================================
-- 4. Analyze tables (InnoDB stats update)
-- =============================================================

ANALYZE TABLE DEPARTMENTS;
ANALYZE TABLE EMPLOYEES;
ANALYZE TABLE ORDERS;
ANALYZE TABLE ORDER_ARCHIVE;

-- =============================================================
-- 5. Habilitar consumers de performance_schema (runtime)
-- =============================================================

UPDATE performance_schema.setup_consumers
SET ENABLED = 'YES'
WHERE NAME = 'events_waits_summary_by_thread_by_event_name';

-- =============================================================
-- 6. Criar user read-only
-- =============================================================

CREATE USER IF NOT EXISTS 'sqlmentor_test'@'%' IDENTIFIED BY 'TestPwd123';
GRANT SELECT, REFERENCES, SHOW VIEW ON SQLMENTOR_TEST.* TO 'sqlmentor_test'@'%';
GRANT EXECUTE ON SQLMENTOR_TEST.* TO 'sqlmentor_test'@'%';
GRANT SELECT ON performance_schema.* TO 'sqlmentor_test'@'%';
FLUSH PRIVILEGES;
