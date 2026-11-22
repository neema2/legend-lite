// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.RootLoop;


import com.legend.testexecutionplan.TypedSide;
import com.legend.testexecutionplan.Verdicts;
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
            // (the evidence's words were HostWords#unjudged's until item 3, 2026-11-21; this decline goes with S4)
            return unjudged(name, String.valueOf(unjudged), " (expected " + AssertRuns.excerpt(row.get(RootLoop.EXPECTED_COLUMN))
                    + ", got " + AssertRuns.excerpt(row.get(RootLoop.ACTUAL_COLUMN)) + ")");
        }
        if (!(row.get(com.legend.testexecutionplan.RootLoop.HOLDS_COLUMN) instanceof Boolean holds)) {
            throw new IllegalStateException(name + ": the holds column is not a boolean: " + row.get(com.legend.testexecutionplan.RootLoop.HOLDS_COLUMN));
        }
        com.legend.diagnostics.VerdictCensus.sqlJudgedInDatabase(name);
        // (the 2-ULP leniency's census, read off the row's lenient column: deleted 2026-11-22, item 3's S4 -- every
        // verdict row plans the column false since A4 of the compatibility program, stage A ratified by the user
        // 2026-11-19, so it never fired)
        return holds ? Verdicts.ok() : Verdicts.fail(String.valueOf(row.get(RootLoop.MESSAGE_COLUMN)));
    }

    /**
     * The verdict of one TYPED row (item 3's S4, 2026-11-21, the user's Q-A): {@code [verdict, expected, actual]}, the
     * verdict the database's over the typed values. Where it holds, nothing; where it fails, the failure is the record
     * {@link AssertRecords#ofTypedRow} chooses from the values the database returned -- the one choice the host judge
     * makes over the values it read (the user, 2026-11-22: one translation from a failure to its words).
     */
    static TypedSide typedVerdictOf(String name, List<Object> row, RootLoop.Report report) {
        com.legend.diagnostics.VerdictCensus.sqlJudgedInDatabase(name);
        return Boolean.TRUE.equals(row.get(0)) ? Verdicts.ok() : Verdicts.fail(AssertRecords.ofTypedRow(report, row));
    }

    /** A verdict row's UNJUDGED reason raised as the assert's failure, its evidence in the message (the run's copy of
     *  the planner's own three lines). */
    private static TypedSide unjudged(String name, String why, String detail) {
        throw com.legend.error.AssertFailed.unjudged(why,
                name + ": UNJUDGED in database mode — " + why + detail);
    }
}
