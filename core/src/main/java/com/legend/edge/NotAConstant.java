// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/** Raised while an arm is PLANNED for the program road where a text side is not a constant of the plan (it would
 *  send a statement at planning): the edge's own refusal, raised by its text collector (train 17, 2026-11-18: moved
 *  from the test planner, which now asks the collector and never names the refusal), the body refused by name ({@code SqlTextVerdicts.plan} answers it as
 *  {@code SqlTextVerdicts.notAConstant}'s check, its message the side's kind and planned root for the unplanned-assert census). */
public final class NotAConstant extends RuntimeException implements com.legend.testexecutionplan.Failure.Kinded {
    /** The message's label; the side's kind and planned root follow it. */
    public static final String LABEL = "a text side that is not a constant of the plan: ";

    /** {@code what}: the side's kind and planned root, after {@link #LABEL}. */
    public NotAConstant(String what) {
        super(LABEL + what);
    }

    @Override
    public com.legend.testexecutionplan.Failure.Kind failureKind() {
        return com.legend.testexecutionplan.Failure.Kind.NOT_A_CONSTANT;
    }
}
