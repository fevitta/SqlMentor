"""
Queries Oracle — re-export layer para backward compat.

O SQL vive em OracleQueryBuilder (sqlmentor.adapters.oracle).
Este módulo delega para uma instância singleton do builder,
mantendo a interface `from sqlmentor.queries import explain_plan` funcionando.
"""

from __future__ import annotations

from sqlmentor.adapters.oracle import OracleQueryBuilder

_qb = OracleQueryBuilder()

# ── Re-exports (delegam ao OracleQueryBuilder) ──────────────────────

# Helpers (acessíveis diretamente para testes e código legado)
_validate_sql_id = _qb.validate_sql_id
_build_tuple_in_clause = _qb.build_tuple_in_clause

# Plano de execução
explain_plan = _qb.explain_plan
runtime_plan = _qb.runtime_plan

# Runtime stats
sql_runtime_stats = _qb.sql_runtime_stats
sql_text_by_id = _qb.sql_text_by_id
session_wait_events = _qb.session_wait_events

# Sessão / instância
db_version = _qb.db_version
optimizer_params = _qb.optimizer_params

# Objetos
object_type = _qb.object_type
table_ddl = _qb.table_ddl
function_ddl = _qb.function_ddl
table_stats = _qb.table_stats
column_stats = _qb.column_stats
indexes = _qb.indexes
constraints = _qb.constraints
histograms = _qb.histograms
table_partitions = _qb.table_partitions
index_to_table_map = _qb.index_to_table_map

# Segurança
dangerous_privileges = _qb.dangerous_privileges
dangerous_roles = _qb.dangerous_roles

# Batch
batch_table_stats = _qb.batch_table_stats
batch_column_stats = _qb.batch_column_stats
batch_indexes = _qb.batch_indexes
batch_constraints = _qb.batch_constraints
