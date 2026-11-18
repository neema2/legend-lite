// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * ONE STATEMENT'S WHERE MERGES, the style's (L10, the lens audit's L-A.unionAlias, 2026-11-18; the orchestrator's
 * ruling: the form gets the product's own facts, the zone order is the edge's): a filter's predicate merged into the
 * WHERE it folds onto, and the WHERE an EXISTS join's DISTINCT-key subselect is written with. The product's appends and knows no
 * zones ({@link ProductLoweringForms}); legend-engine's orders the conjuncts by zone -- user, correlation, temporal --
 * and keeps each split for its exists form (the edge's). A fresh one per lowering ({@link LoweringForms#whereZones}).
 */
public interface WhereZones {

    /** {@code predicate}, of zone {@code stamp} (the filter's own), merged into {@code existing} (null: none). */
    SqlExpr merge(@com.legend.base.Nullable SqlExpr existing, SqlExpr predicate,
            com.legend.compiler.spec.typed.TypedFilter.Stamp stamp);

    /** An EXISTS join's DISTINCT-key subselect's WHERE: its {@code local} conjuncts, written as this statement's merges
     *  wrote {@code where} (the subquery's own WHERE) -- the product's in encounter order, legend-engine's temporal
     *  conditions first ({@code sql.ExistsJoins.rewrite}). */
    @com.legend.base.Nullable SqlExpr keysWhere(java.util.List<SqlExpr> local, @com.legend.base.Nullable SqlExpr where);
}
