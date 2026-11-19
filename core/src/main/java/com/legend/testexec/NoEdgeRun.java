// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.error.NotImplementedException;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TypedSide;

import java.util.List;
import java.util.function.Function;

/**
 * THE PRODUCT'S ANSWER TO THE EDGE'S RUN HOOK (census C2, 2026-11-17; G7, no null): a run hears no compatibility
 * appeal. Its hearing tells the caller's listener as it is, and refuses an edge-judged comparison and an appeal BY NAME,
 * pointing to the edge -- as a plan naming an appeal is refused when the run opens. Every other implementation of
 * {@link EdgeRun} and {@link EdgeHearing} is the edge's.
 */
public final class NoEdgeRun implements JudgeRun {

    /** Lite's runs: no appeal is heard. */
    public static final NoEdgeRun NONE = new NoEdgeRun();

    private static final String WHY = "needs the compatibility edge's run (com.legend.edge): lite's own rules hear no"
            + " appeal and judge no engine text";

    private NoEdgeRun() {
    }

    @Override
    public JudgeRun.Hearing open(TestPlan plan, @Nullable AssertListener callers, java.util.UUID execution) {
        if (!plan.extensionsByVerdict().isEmpty()) {
            throw new NotImplementedException("a test plan naming the compatibility edge's appeals " + WHY);
        }
        return new Hearing(callers);
    }

    @Override
    public boolean wordsAlike() {
        // lite's own rules: both judges write lite's sentences
        return true;
    }

    /** A run that hears no appeal: the caller's listener as it is. */
    private record Hearing(@Nullable AssertListener listener) implements JudgeRun.Hearing {

        @Override
        public String wordsName() {
            return LiteHostWords.NAME;
        }

        @Override
        public String present(VerdictRecord failure) {
            return LiteHostWords.present(failure);
        }

        @Override
        public String caughtMessage(RuntimeException error) {
            return LiteHostWords.caught(error);
        }

        @Override
        public TypedSide judged(HostJudgment.EdgeJudged j, List<TypedSide> sides, boolean messaged) {
            throw new NotImplementedException("an edge-judged comparison " + WHY);
        }

        @Override
        public TypedSide rowAppealed(int number, List<Object> row, boolean messaged) {
            throw new NotImplementedException("appeal " + number + " " + WHY);
        }


        @Override
        public com.legend.testexecutionplan.JsonGrammar json() {
            // lite's own runs read strict JSON (register S144)
            return com.legend.testexecutionplan.JudgeJson.STRICT;
        }

        @Override
        public RenderedTexts renderedTexts() {
            // lite's own runs: the two strings are the verdict
            return LiteRenderedTexts.STRINGS;
        }


        @Override
        public void writing() {
            // no appeal is held: nothing a write could stale
        }

        @Override
        public void decide(Sessions.Session session, com.legend.exec.SystemDatabase.Leases systemSessions,
                Function<ExecutionPlan.Target, Sessions.Session> sessions) {
            // nothing held
        }

        @Override
        public RuntimeException afterFailure(RuntimeException runFailed, Sessions.Session session,
                com.legend.exec.SystemDatabase.Leases systemSessions,
                Function<ExecutionPlan.Target, Sessions.Session> sessions) {
            return runFailed;
        }
    }
}
