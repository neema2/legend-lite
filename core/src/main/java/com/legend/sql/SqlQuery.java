package com.legend.sql;

import java.util.List;

/**
 * A complete SQL query: a single {@link SqlSelect} or a {@link SqlUnion} of
 * queries. Modelling union at the QUERY level (not as an operator inside a
 * select) keeps a bare {@code a UNION ALL b} renderable without a wrapping
 * {@code SELECT *} &mdash; the lean-SQL tenet applied to set operations.
 */
public sealed interface SqlQuery permits SqlSelect, SqlUnion {

    /** The query's output columns, with Pure types (the typed-results contract). */
    List<OutputCol> outputs();
}
