// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

/**
 * THE RAW-SQL REFUSAL: a statement's own SQL text (an {@code executeInDb}'s read) refused by a product door and by the
 * root's {@code Compiler.lowerResolved} ({@code planner.QueryDoor.refuseRawSql}). A NotImplementedException of its own
 * class, so a catch answering "no rows" for a lowering's failure ({@code planner.PlanAllocations.noLineageRows}) tells
 * it apart by its class, never by its words (the C4 final fix round's nit, 2026-11-14: until then
 * {@code QueryDoor.isRawSqlRefusal} matched the message text, which another refusal could share). In
 * {@code com.legend.error} so a test plan can record it as itself and raise it at its verdict's turn
 * ({@code testexecutionplan.Failure.Kind.RAW_SQL_REFUSAL}).
 */
public final class RawSqlRefusal extends NotImplementedException {

    private static final long serialVersionUID = 1L;

    public RawSqlRefusal(String message) {
        super(message);
    }
}
