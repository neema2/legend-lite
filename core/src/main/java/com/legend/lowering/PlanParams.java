// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import java.util.ArrayDeque;

/** Plan-template parameter helpers (the executionPlan printer's
 * vocabulary). */
public final class PlanParams {

    private PlanParams() {
    }

    /** The placeholder KIND for a parameter/field type — drives the
     * engine's freemarker spelling (h2New types its date placeholders;
     * optional parameters pick per-kind varPlaceHolderToString args). */
    public static SqlExpr.PlanParam.Kind kindOf(Type t) {
        return Fold.planKindOf(t);
    }

    /** {@code <param>.<a>.<b>.<field>} when {@code base} is a struct-get
     * chain rooted at a plan parameter (or the parameter itself); null
     * otherwise. */
    static @com.legend.base.Nullable String dottedPlanParam(SqlExpr base,
            String field) {
        ArrayDeque<String> path = new ArrayDeque<>();
        path.addFirst(field);
        SqlExpr cur = base;
        while (cur instanceof SqlExpr.StructGet g) {
            path.addFirst(g.field());
            cur = g.source();
        }
        return cur instanceof SqlExpr.PlanParam pp
                ? pp.name() + "." + String.join(".", path) : null;
    }
}
