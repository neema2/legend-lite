// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.RootLoop;


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
            // (the evidence's words were HostWords#unjudged's until item 3, 2026-11-21; this decline goes with S4)
            return unjudged(name, String.valueOf(unjudged), " (expected " + AssertRuns.excerpt(row.get(RootLoop.EXPECTED_COLUMN))
                    + ", got " + AssertRuns.excerpt(row.get(RootLoop.ACTUAL_COLUMN)) + ")");
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

    /**
     * The verdict of one TYPED row (item 3's S4, 2026-11-21, the user's Q-A): {@code [verdict, expected, actual]}, the
     * verdict the database's over the typed values, each side a list in its own order, written where the verdict
     * fails. Where it holds, nothing; where it fails, the failure is recorded by the HOST judge's own builders
     * ({@link AssertRecords}) over the values the database computed -- one record for one failure on both judges.
     */
    static TypedSide typedVerdictOf(String name, List<Object> row, RootLoop.Report report) {
        if (!(row.get(0) instanceof Boolean holds)) {
            throw new IllegalStateException(name + ": a typed row's verdict is not a boolean: " + row.get(0));
        }
        com.legend.diagnostics.VerdictCensus.sqlJudgedInDatabase(name);
        if (holds) {
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        com.legend.error.ErrorRecord failure = report.fixed() != null ? report.fixed() : switch (report.family()) {
            case EQUALS -> report.want()
                    ? AssertRecords.sidesNotEqual(side(row, 1), side(row, 2), side(row, 1), side(row, 2),
                            classOf(report.e()), classOf(report.a()))
                    : AssertRecords.notEqual(side(row, 1), side(row, 2), classOf(report.a()));
            case SAME_ELEMENTS -> report.want()
                    ? AssertRecords.sameElements(side(row, 1), side(row, 2), classOf(report.e()), classOf(report.a()))
                    : AssertRecords.notEqual(side(row, 1), side(row, 2), classOf(report.a()));
            case IS_NOT -> AssertRecords.negated("IS_NOT", side(row, 1), side(row, 2));
        };
        return com.legend.testexecutionplan.Verdicts.fail(report.index() >= 0
                ? AssertRecords.atIndex(failure, report.index()) : failure);
    }

    /** A typed row's side at {@code column}: the list the database wrote (an empty side's none, written NULL). */
    private static List<Object> side(List<Object> row, int column) {
        Object v = row.get(column);
        if (v == null) {
            return List.of();
        }
        if (!(v instanceof List<?> l)) {
            throw new IllegalStateException("a typed row's side is a list, not " + v.getClass().getSimpleName());
        }
        return java.util.Collections.unmodifiableList(new java.util.ArrayList<>(l));
    }

    private static @com.legend.base.Nullable String classOf(
            com.legend.testexecutionplan.HostJudgment.@com.legend.base.Nullable SideFacts f) {
        return f == null ? null : f.classFqn();
    }

    /** A verdict row's UNJUDGED reason raised as the assert's failure, its evidence in the message (the run's copy of
     *  the planner's own three lines). */
    private static TypedSide unjudged(String name, String why, String detail) {
        throw com.legend.error.AssertFailed.unjudged(why,
                name + ": UNJUDGED in database mode — " + why + detail);
    }
}
