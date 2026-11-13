// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.lowering.Lowerer;
import com.legend.lowering.ObjectSites;
import com.legend.platform.Feature;
import com.legend.sql.SqlExpr;

import java.util.List;
import java.util.Set;

/**
 * A QUERY LOWERED FOR A TEST TO READ ITS TREE (the user's ruling of 2026-11-13, option (b)): what TypedQuery.lower and
 * TypedQuery.lowerer were, for the tests that inspect a lowered statement, kept in TEST sources -- not product code, so
 * not a door. The product lowers through the planner's own road ({@link ProductStatements}: its plans, and
 * {@link ProductStatements#lowerTestDataSeed}, the test-data seed's one entry); this reaches the same road the same way,
 * step by step, as the root's methods did: the raw-SQL refusal first ({@link QueryDoor#refuseRawSql}), then the plan's
 * own prepare, plan and lowering ({@link ProductStatements#prepare}, {@code planned}, {@code lower}: package-private to
 * the planner since 2026-11-13, so this helper sits in its package), and the planner's one lowering configuration
 * ({@link ProductStatements#configured}).
 */
public final class Lowered {

    private Lowered() {
    }

    /** {@code query} lowered for {@code runtime}: inlined (G½), resolved (H) against the runtime, every touched store
     *  checked against its connections, lowered (I) -- every planning step, no database. {@code streaming}: the
     *  streaming graph root (one json_object per row). */
    public static Compiler.LoweredQuery lower(TypedQuery query, String runtime, boolean streaming) {
        return lower(query, runtime, streaming, List.of(), Set.of());
    }

    /** {@link #lower(TypedQuery, String, boolean)} with the query's declared parameters as {@code slots}. */
    public static Compiler.LoweredQuery lower(TypedQuery query, String runtime, boolean streaming,
            List<SqlExpr.PlanParam> slots, Set<Feature> features) {
        // the product's raw-SQL refusal first, as its doors and the seed's entry refuse (the C4 audit's M5, 2026-11-13)
        QueryDoor.refuseRawSql(query);
        ProductStatements.Statement s = ProductStatements.prepare(query, query.body(), runtime, null, features);
        Prepared.Planned p = ProductStatements.planned(s);
        return new Compiler.LoweredQuery(ProductStatements.lower(query, p, s.temporal(), streaming, slots,
                new ObjectSites()), p.body().get(p.body().size() - 1), query.context());
    }

    /** The Lowerer a planned statement lowers with, the planner's own configuration
     *  ({@link ProductStatements#configured}). */
    public static Lowerer lowerer(TypedQuery query, boolean temporal, boolean streaming, List<SqlExpr.PlanParam> slots,
            Set<Feature> features) {
        return ProductStatements.configured(query, temporal, streaming, slots, features, new ObjectSites());
    }
}
