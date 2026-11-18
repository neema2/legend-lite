// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.dialect.SqlDialect;

/**
 * legend-engine's own text for a database whose engine spelling is built (the engine's test database H2, DB2 and
 * Composite): the dialect the engine-text and plan-text paths take, named for what it writes rather than for one
 * database (the dialect program's Phase 3, 2026-10-16: until then those paths took {@link EngineStyleH2}).
 */
public interface EngineTextDialect extends SqlDialect {

    /** This printer with the plan's template parameters ({@link EngineParams}, the edge's table: an enumeration's
     *  mapping function, a wrapper spliced bare; the audit's J31): set before the plan is written. */
    void params(EngineParams params);

    /** A collection-valued plan parameter spliced as the engine's plan text writes it (its FreeMarker collection
     *  template). */
    String collectionSplice(com.legend.sql.SqlExpr.PlanParam cp);

    /** A relation alias as the engine's plan text writes it. */
    String renderedAlias(String alias);

    /** The select list of a query's (first) select as {@link #render} writes it, after the same rewrites and the same
     *  alias plan: the engine's {@code resultColumns} text for a scalar projection. Null when the query is no select
     *  (a {@code with}). */
    @com.legend.base.Nullable String selectList(com.legend.sql.SqlQuery query);

    /** The query as {@link #render} writes it but with the aliases it carries: how the engine prints a query it
     *  built with its own aliases (the test-data generator's plan: {@code "root"}, {@code <table>_0}), where
     *  {@link #render} renames them as the engine's reAliasQuery does a query it planned. */
    String renderAsAliased(com.legend.sql.SqlQuery query);

    /** The query that reads the in-collection temp table back, as the engine's
     *  {@code generateTempTableSelectSQLQuery} writes it: its one column, under the table's own alias. */
    String inTempTableSelect(String tempTable);
}
