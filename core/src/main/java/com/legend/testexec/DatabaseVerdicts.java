// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.testexecutionplan.TypedSide;
import java.util.ArrayList;
import java.util.List;

final class DatabaseVerdicts {

    private DatabaseVerdicts() {
    }


    /** The verdict of one row ({@code verdict, expected, actual, unjudged, lenient, holds, message}): unjudged
     * (counted, the assert fails with the reason), or the row's own answer -- holds, or its message raised (the
     * test-is-the-product plan's M8b: the polarity and the text are the statement's; this reads two columns). */
    static TypedSide verdictOf(String name, List<Object> row) {
        if (com.legend.diagnostics.Diagnostics.dumpSql()) {
            // the SQL dump's companion: the verdict row the statement returned
            System.err.println("[verdict] " + name + " verdict=" + row.get(com.legend.testexecutionplan.RootLoop.VERDICT_COLUMN)
                    + " expected=" + row.get(com.legend.testexecutionplan.RootLoop.EXPECTED_COLUMN) + " actual=" + row.get(com.legend.testexecutionplan.RootLoop.ACTUAL_COLUMN)
                    + " unjudged=" + row.get(com.legend.testexecutionplan.RootLoop.UNJUDGED_COLUMN) + " holds=" + row.get(com.legend.testexecutionplan.RootLoop.HOLDS_COLUMN));
        }
        Object unjudged = row.get(com.legend.testexecutionplan.RootLoop.UNJUDGED_COLUMN);
        if (unjudged != null) {
            // counted UNJUDGED here, at the row's turn: the run's own census (the slice 3 follow-up audit, 2026-11-08:
            // moving the count off planning left the row's UNJUDGED uncounted)
            com.legend.diagnostics.VerdictCensus.sqlUnjudged(name, String.valueOf(unjudged));
            // the evidence columns ride the message (bounded): an accepted
            // divergence matches its witness here, and a row is diagnosable
            // without a re-run
            return unjudged(name, String.valueOf(unjudged),
                    "\nexpected: " + com.legend.testexec.AssertRuns.excerpt(row.get(com.legend.testexecutionplan.RootLoop.EXPECTED_COLUMN))
                    + "\nactual:   " + com.legend.testexec.AssertRuns.excerpt(row.get(com.legend.testexecutionplan.RootLoop.ACTUAL_COLUMN)));
        }
        if (!(row.get(com.legend.testexecutionplan.RootLoop.HOLDS_COLUMN) instanceof Boolean holds)) {
            throw new IllegalStateException(name + ": the holds column is not a boolean: " + row.get(com.legend.testexecutionplan.RootLoop.HOLDS_COLUMN));
        }
        com.legend.diagnostics.VerdictCensus.sqlJudgedInDatabase(name);
        if (Boolean.TRUE.equals(row.get(com.legend.testexecutionplan.RootLoop.LENIENT_COLUMN))) {
            // the declared 2-ULP Float leniency decided it — counted as
            // host mode counts its own firings
            com.legend.testexec.CanonicalDivergence.sqlUlpPolicy("database " + name);
            com.legend.diagnostics.Diagnostics.judgeSend("ulp-tolerance", "database " + name);
        }
        if (holds) {
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        return com.legend.testexecutionplan.Verdicts.fail(String.valueOf(row.get(com.legend.testexecutionplan.RootLoop.MESSAGE_COLUMN)));
    }

    /** A verdict row's UNJUDGED reason raised as the assert's failure, its evidence in the message (the run's copy of
     *  the planner's own three lines). */
    private static TypedSide unjudged(String name, String why, String detail) {
        throw com.legend.error.AssertFailed.unjudged(why,
                name + ": UNJUDGED in database mode — " + why + detail);
    }
}
