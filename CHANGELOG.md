# CHANGELOG

<!-- version list -->

## v1.1.0 (2026-04-02)

### Bug Fixes

- Address review findings — improve tests and docs (T10, T2, T4)
  ([`4f50a6c`](https://github.com/fevitta/SqlMentor/commit/4f50a6c9ddb328466f7d6823fc885dfc5a7cef85))

- Handle 'materialized' key in MariaDB plan DFS traversal (T2)
  ([`a6b3106`](https://github.com/fevitta/SqlMentor/commit/a6b31063d47a6d9fe17367062249d2ba2e7c8a07))

- MariaDB report improvements (T1-T9)
  ([`2ecf8fe`](https://github.com/fevitta/SqlMentor/commit/2ecf8fe2caf0068c3d75c8480bada3805589cb1c))

- MariaDB schema defaults to empty instead of username (T7)
  ([`bbacbe0`](https://github.com/fevitta/SqlMentor/commit/bbacbe050bd89cdf8d1819d302d97cef31225e54))

- Normalize optimizer params keys to lowercase for MariaDB (T1)
  ([`9bfd19f`](https://github.com/fevitta/SqlMentor/commit/9bfd19fc969435c1ab207419420942a1b26bde98))

- Prevent EXPLAIN crash with % in SQL via PyMySQL mogrify (T10)
  ([`57d9c9f`](https://github.com/fevitta/SqlMentor/commit/57d9c9f6470abe6422ffa63c23eeb906ae7fbf48))

- Remove private database names from tests and docs
  ([`5897997`](https://github.com/fevitta/SqlMentor/commit/58979975e78fff6b2031790e926c34f335eb8b1e))

- Strip ANSI codes em testes de config add (CI Typer output)
  ([`cdb78a4`](https://github.com/fevitta/SqlMentor/commit/cdb78a4dabec9b9020ad131f7743ce1d8730dcf5))

- SUM_SORT_ROWS → SORT_ROWS em events_statements_history, adapter param em testes de integração
  ([`29e4ab2`](https://github.com/fevitta/SqlMentor/commit/29e4ab268b89f5b327b9d1230522165f90aa2435))

### Chores

- Add worktrees/ to .gitignore and clean up Phase 1 branches
  ([`7b94034`](https://github.com/fevitta/SqlMentor/commit/7b94034aa82387be648cceb36f1dbc147cf9c871))

- Skip mypy/pytest hooks for non-Python changes
  ([`bac192e`](https://github.com/fevitta/SqlMentor/commit/bac192e2f109ca2c09eac6952e56d5ea3add2904))

- Update gitignore, add tasks_mariadb and session logs
  ([`cf54c2b`](https://github.com/fevitta/SqlMentor/commit/cf54c2b305810e7f953d59c449b6bee2bc6b51c7))

- Update permissions and add T12-T17 to tasks_mariadb
  ([`7df4d5b`](https://github.com/fevitta/SqlMentor/commit/7df4d5be3c9f06abb2976fb197c018ec18607cde))

### Documentation

- MariaDB [BETA], timeout 600s sem limite, docs enxutas
  ([`eee21c1`](https://github.com/fevitta/SqlMentor/commit/eee21c1c0f7a7827c4eb06aa06c785a10f43e799))

- Mark Phase 1 complete, add MariaDB tasks for Phase 3
  ([`236e884`](https://github.com/fevitta/SqlMentor/commit/236e8844c09dd20dd56e4d21ba47543d34d15424))

- Update TASKS.md dependency references for T17/T18 completion
  ([`b90c897`](https://github.com/fevitta/SqlMentor/commit/b90c897e82918b5483f38f3b631fdba0573783c4))

### Features

- Add dialect parameter to MCP parse_sql tool (T6)
  ([`96b6a1e`](https://github.com/fevitta/SqlMentor/commit/96b6a1efad0e12087d6fce504466f8d991045d30))

- Add get-sql command, block inspect for MariaDB, update docs (T18-T21)
  ([`a4c50b5`](https://github.com/fevitta/SqlMentor/commit/a4c50b55cfbbe1444f2fe71327486b1efc62a98e))

- Add MariaDB integration tests with Docker + CI (T21)
  ([`3aef603`](https://github.com/fevitta/SqlMentor/commit/3aef603982367e5db02fd0431784bd9f9db015f8))

- Add MariaDBAdapter with PyMySQL driver (T17)
  ([`eb9342b`](https://github.com/fevitta/SqlMentor/commit/eb9342b827f8588e75d5e7f8a9d0566e5b7692df))

- Config add oracle/mariadb subcommands + case sensitivity fix + DBA script
  ([`081fcc6`](https://github.com/fevitta/SqlMentor/commit/081fcc6f4539c735b935d12e38dcceefd7861b17))

- Implement all remaining MariaDB tasks (T3, T5, T8, T9, T11-T17)
  ([`873dd78`](https://github.com/fevitta/SqlMentor/commit/873dd78b4606e6d3707ea2bb9f79edec473afa31))

- Implement MariaDB queries in MariaDBQueryBuilder (T18)
  ([`003e4f6`](https://github.com/fevitta/SqlMentor/commit/003e4f61646ea6bb957802ff9dc907753d62f8d1))

- Implement MariaDBPlanParser with JSON DFS traversal (T20)
  ([`83bb79a`](https://github.com/fevitta/SqlMentor/commit/83bb79a87cd8b5947024e6a32e1bc124d888ec3e))

- Integrate MariaDB metadata collection in collector/report pipeline (T19)
  ([`6caf0a6`](https://github.com/fevitta/SqlMentor/commit/6caf0a620517485319dfb96ece4a81122d6de5fd))

- Validate R1-R12 for MariaDB + implement inspect MariaDB (T22, T23)
  ([`f431a75`](https://github.com/fevitta/SqlMentor/commit/f431a75401118ff2acbaf0da43f05105c0b2bd3a))

### Testing

- Add coverage for MariaDB runtime plan labels (T4)
  ([`10e5526`](https://github.com/fevitta/SqlMentor/commit/10e55267dfa92c7750b44d3d582fa5d5604187bb))

- Add Oracle regression tests closing Phase 1 coverage gaps (T7)
  ([`6d254b6`](https://github.com/fevitta/SqlMentor/commit/6d254b665ce3ad9e0b23784631a03372d1a03ce2))

- Add tests for MariaDB report improvements (T1-T9)
  ([`cd7e0bd`](https://github.com/fevitta/SqlMentor/commit/cd7e0bd9e224c216861d837715a41805dd1e0d17))


## v1.0.2 (2026-03-09)

### Refactoring

- Make CLI/MCP dialect-aware with adapter-driven doctor and generic naming (T6)
  ([`5f3bb3d`](https://github.com/fevitta/SqlMentor/commit/5f3bb3da3b6e917c4b98b851f6b5d57dde3517e9))


## v1.0.1 (2026-03-09)

### Refactoring

- Implement OraclePlanParser and delegate plan parsing from report.py (T5)
  ([`8cd7bf6`](https://github.com/fevitta/SqlMentor/commit/8cd7bf6e379b4cc31509f914b25c8c2917ec7a17))


## v1.0.0 (2026-03-09)

- Initial Release
