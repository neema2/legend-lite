/**
 * Phase I output &mdash; the SQL intermediate representation
 * (PHASE_HIJ_LOWERING.md): sealed, immutable, <strong>data-only</strong> records.
 * No {@code toSql()} here and no dialect knowledge &mdash; rendering lives
 * entirely in {@link com.legend.sql.dialect}. Function calls carry SEMANTIC
 * names ({@code "divide"}, {@code "roundHalfEven"}); the dialect maps them.
 *
 * <p>The design centre is {@link com.legend.sql.SqlSelect}: ONE node with every
 * clause slot, so a run of fold-compatible relational ops extends a single
 * SELECT (the lean-SQL tenet) and nesting exists only as an explicit
 * {@link com.legend.sql.SqlSource.Subselect}.
 */
package com.legend.sql;
