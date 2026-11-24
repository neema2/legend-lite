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
 * ONE RUN'S HEARING OF ITS COMPATIBILITY APPEALS (census C2, the orchestrator's P2 of 2026-11-17): opened by an
 * {@link EdgeRun} for one run of a plan, told what the run's judges hear and asked, after the run, to decide what it
 * held. A ruled hook (HookRulingTest): signatures only; the edge implements it, the product's {@link NoEdgeRun}
 * refusing an edge-judged comparison and an appeal by name.
 */
public interface EdgeHearing {

    /** The listener the run's judges tell: the caller's, its reports kept back while an appeal is held. */
    @Nullable AssertListener listener();

    /** The edge's comparison {@code j} judged at its verdict step over its two {@code sides}, under {@code rules}
     *  ({@code messaged}: its assert carries a caller's message): equal holds, else the appeal heard (census C3,
     *  2026-11-17: its facts the edge's own record). */
    TypedSide judged(HostJudgment.EdgeJudged j, List<TypedSide> sides, boolean messaged);

    // (rowAppealed, the database judge's failed row's appeal heard: deleted with the database judge's appeal path,
    // A4 of the compatibility program, stage A ratified by the user 2026-11-19)

    // (raisedWords, legend-pure's words of a planned site looked up in the plan's records (K10b): deleted 2026-11-20
    // with stage B's B1 -- the edge's words of a raise read its site's record, HostForms#caught)

    // (json, how the run's host judge read a JSON text, census C1, 2026-11-17: deleted 2026-11-24, B3 step 2 of the
    // compatibility program -- the judge reads strict JSON on every route, and the two goldens json-simple read past are
    // cleaned at the edge's test-body door before planning, edge.UpstreamTestBodies, register S144)

    /** Whether a rendered text that differs from its golden is held (census C1, 2026-11-17): the product's
     *  {@link LiteRenderedTexts#STRINGS}, upstream's ruled comparison at the edge. */
    RenderedTexts renderedTexts();

    /** The host judge's behaviours (census C1's step 2c, 2026-11-17): the product's {@link LiteHostForms#STRICT},
     *  compiled legend-pure's at the edge. */
    HostForms hostForms();

    /** A step of the body that writes is about to run. */
    void writing();

    /** The held appeals decided after the run, reading on {@code session}, {@code systemSessions} and the run's
     *  {@code sessions} by target. */
    void decide(Sessions.Session session, com.legend.exec.SystemDatabase.Leases systemSessions,
            Function<ExecutionPlan.Target, Sessions.Session> sessions);

    /** The held appeals of a run that failed with {@code runFailed}: the error the body raises. */
    RuntimeException afterFailure(RuntimeException runFailed, Sessions.Session session,
            com.legend.exec.SystemDatabase.Leases systemSessions,
            Function<ExecutionPlan.Target, Sessions.Session> sessions);
}
