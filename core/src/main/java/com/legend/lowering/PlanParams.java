// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;

/** A parameter's declared Pure type, as a value class (L-B.PlanParam, 2026-11-18: the template's placeholder kind
 *  until then; legend-engine's spellings of it are the edge's). */
public final class PlanParams {

    private PlanParams() {
    }

    /** The value class of a parameter declared {@code t} ({@link Fold#planValueTypeOf}). */
    public static SqlExpr.PlanParam.ValueType valueTypeOf(Type t) {
        return Fold.planValueTypeOf(t);
    }
}
