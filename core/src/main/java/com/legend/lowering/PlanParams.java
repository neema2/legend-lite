// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;

/** A parameter's placeholder kind (the edge's dotted template reads resolve themselves: the audit's J31). */
public final class PlanParams {

    private PlanParams() {
    }

    /** The placeholder KIND for a parameter/field type — drives the
     * engine's freemarker spelling (h2New types its date placeholders;
     * optional parameters pick per-kind varPlaceHolderToString args). */
    public static SqlExpr.PlanParam.Kind kindOf(Type t) {
        return Fold.planKindOf(t);
    }
}
