// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.compiler.element.ModelContext;

/** A view's own fetch for a test of the generator's planning: the planner's ({@code BodyPlanner.viewFetch}) over the
 *  model, on a runtime or none, no session -- what the test planner hands the generator (B2d.3). */
final class PlannerViews {

    private PlannerViews() {
    }

    /** No view to fetch: a model whose queries read none. */
    static TestDataPlanner.Views of(ModelContext ctx) {
        return (db, view, reads, tables) -> {
            throw new IllegalStateException("a view's fetch where the test reads no view: " + view);
        };
    }

    /** The planner's view fetch on {@code runtime}'s connections, written in {@code dialect} (a query over a view
     *  plans against its runtime, in the body's dialect). */
    static TestDataPlanner.Views of(ModelContext ctx, String runtime, com.legend.sql.dialect.SqlDialect dialect) {
        com.legend.compiler.spec.SpecCompiler specs = new com.legend.compiler.spec.SpecCompiler(ctx);
        com.legend.planner.PlanEnv env = com.legend.planner.PlanEnv.of(ctx, runtime, dialect, java.util.Set.of(),
                com.legend.planner.ProductStatements::noGenerator);
        return (db, view, reads, tables) -> com.legend.planner.BodyPlanner.viewFetch(db, view, reads, tables, specs,
                env);
    }
}
