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
 * refusing an engine-text judgment and an appeal by name.
 */
public interface EdgeHearing {

    /** The listener the run's judges tell: the caller's, its reports kept back while an appeal is held. */
    @Nullable AssertListener listener();

    /** The edge's judgment {@code j} (an engine text's) judged at its verdict step over its two text {@code sides},
     *  under {@code rules} ({@code messaged}: its assert carries a caller's message): equal holds, else the appeal
     *  heard. */
    TypedSide engineText(HostJudgment j, List<TypedSide> sides, TestPlan.JudgingRules rules, boolean messaged);

    /** The database judge's failed {@code row}, whose appeal is numbered {@code number}, heard. */
    TypedSide rowAppealed(int number, List<Object> row, boolean messaged);

    /** How the run's host judge reads a JSON text (census C1, 2026-11-17): the product's strict form, upstream's
     *  json-simple grammar at the edge. */
    com.legend.testexecutionplan.JsonGrammar json();

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
