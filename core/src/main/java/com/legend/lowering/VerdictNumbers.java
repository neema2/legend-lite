// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;

/**
 * HOW THE DATABASE JUDGE PLANS TWO NUMBERS' EQUALITY, A TWO-FORM KIND (train 17, the orchestrator's item 4 of
 * 2026-11-18: until then the plan's judging rules' {@code floatUlps} and {@code decimalScale}, which only the
 * compatibility preset switched on): the product's form is lite's exact rule ({@link LiteVerdictNumbers}), the edge's
 * compiled legend-pure's. The host judge asks the run hook the same question ({@code testexec.HostForms}).
 * Signatures only (HookRulingTest).
 */
public interface VerdictNumbers {

    /** Whether a Float side's cells are walked for a tolerance at all (the 2-ULP leniency's cell CTEs). */
    boolean floatTolerance();

    /** Two Floats {@code x} and {@code y} whose canons differ, equal all the same: the predicate. Asked only where
     *  {@link #floatTolerance} holds. */
    SqlExpr floatsWithin(SqlExpr x, SqlExpr y);

    /** Whether a side of kind {@code kind} is compared by its value, its scale dropped from its canon. */
    boolean decimalByValue(Type kind);
}
