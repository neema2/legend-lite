// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TypedSide;

import java.util.List;
import java.util.function.Function;

/**
 * H10 SPIKE (claude/compat-judges-spike, never for merge; CP-14 (iii), ruled 2026-11-19): THE ONE RUN INTERFACE of a
 * test body's judge, handed to the test door whole: the product's ({@link NoEdgeRun#NONE}, lite's) or the compatibility
 * edge's ({@code edge.AppealRun}, legend-pure's, the SQL-text appeal included). It replaces the run hook
 * ({@code EdgeRun}), its hearing ({@code EdgeHearing}), the host judge's words ({@code HostWords}) and behaviours
 * ({@code HostForms}): the host judge keeps lite's rules inline and asks this run only to present a failure record,
 * to say what an assertError matches a raise by, to judge the two kinds the edge owns whole (an edge-judged comparison,
 * a rendered text), and to hear the appeals' run events.
 */
public interface JudgeRun {

    /** This run's hearing of {@code plan}, its reports to {@code callers}; the run's execution id {@code execution}. */
    Hearing open(TestPlan plan, @Nullable AssertListener callers, java.util.UUID execution);

    /** Whether both judges of an isolated run present failures alike (2.2a deletes it with JUDGES-3's branch). */
    boolean wordsAlike();

    /** One run's hearing. */
    interface Hearing {

        /** The words' name, the plan's data ({@code TestPlan.words}; 1.2 deletes it with the words in the plan). */
        String wordsName();

        /** The listener the run's judges tell: the caller's, its reports kept back while an appeal is held. */
        @Nullable AssertListener listener();

        /** A failed verdict, worded in this run's audience's words: the judge's ONE question about words. */
        String present(VerdictRecord failure);

        /** What an assertError matches the raise {@code error} by: the audience's words of it (lite's: its own
         *  message; legend-pure's: the planned raise site's words, from the plan's records). */
        String caughtMessage(RuntimeException error);

        /** The edge's comparison {@code j} judged over its {@code sides}: the SQL-text appeal's verdict step. */
        TypedSide judged(HostJudgment.EdgeJudged j, List<TypedSide> sides, boolean messaged);

        /** The database judge's failed {@code row}, whose appeal is numbered {@code number} (2.2a deletes it). */
        TypedSide rowAppealed(int number, List<Object> row, boolean messaged);

        /** How the host judge reads a JSON text (SPIKE STUB: kept as the run's; R-S144 at the door replaces it). */
        com.legend.testexecutionplan.JsonGrammar json();

        /** The rendered-text kind's rule, the edge's own for that kind (CP-14 (iii)). */
        RenderedTexts renderedTexts();

        /** A step of the body that writes is about to run. */
        void writing();

        /** The held appeals decided after the run. */
        void decide(Sessions.Session session, com.legend.exec.SystemDatabase.Leases systemSessions,
                Function<ExecutionPlan.Target, Sessions.Session> sessions);

        /** The held appeals of a run that failed with {@code runFailed}: the error the body raises. */
        RuntimeException afterFailure(RuntimeException runFailed, Sessions.Session session,
                com.legend.exec.SystemDatabase.Leases systemSessions,
                Function<ExecutionPlan.Target, Sessions.Session> sessions);
    }
}
