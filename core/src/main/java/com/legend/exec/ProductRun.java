// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.ResultSink;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * A PRODUCT QUERY'S PLAN RUN (the plan-as-data design's slice 5, 2026-11-09: the door's {@code runPlan} until then,
 * moved here so the door holds no logic): what the plan says alone decides where it runs. A plan that writes runs on
 * its statements' own sessions (decision A, docs/WRITES_DESIGN_2026_10_18.md §8: a write runs on a database of its own,
 * a database the user names or a URL reaches keeps it); one that reads runs on the session {@code sessions} opens for
 * its answer's target, established as {@link Establish} says; a constant answer sends nothing and opens no session (the
 * test-is-the-product plan's M2, B11: its value is the plan's). The rows stream to the sink in batches.
 */
public final class ProductRun {

    private ProductRun() {
    }

    /** How a plan that reads takes its session: established as legend-engine establishes a connection on every
     *  acquisition, the target's declared setup run on it first ({@code pure/v1} execute), or as the caller set it up
     *  (its own tables, its own connection). The door's {@code Execution.Establish} until slice 5. */
    public enum Establish { DECLARED_SETUP, AS_GIVEN }

    /** {@code plan} run, its rows to the sink {@code sinkOf} makes from it -- made before any statement runs, so a sink
     *  that refuses the plan (an answer its format has no form for) refuses it untouched: a program's writes never land
     *  behind a refused answer. Returns the plan that ran. */
    public static ExecutionPlan run(ExecutionPlan plan, SessionSource sessions, PlanSessions.Source planSessions,
            Establish establish, Function<ExecutionPlan, ResultSink> sinkOf, int batchRows) throws IOException {
        // the caller's sink and sessions are called back here, before the plan's run opens: they plan nothing
        try (var callbacks = com.legend.base.RunScope.callingBack()) {
            run(plan, sessions, planSessions, establish, sinkOf.apply(plan), batchRows);
        }
        return plan;
    }

    /** {@code plan} run as the server runs it (legend-engine's {@code pure/v1/execution/execute}): a read on its
     *  answer's target's project database, established first as legend-engine establishes a connection on every
     *  acquisition (its declared test data run on it); a program that writes on the plan's own shared sessions. Its rows
     *  to the sink {@code sinkOf} makes from it, in the stream's default batches. */
    public static ExecutionPlan onProjectDatabases(ExecutionPlan plan, Function<ExecutionPlan, ResultSink> sinkOf)
            throws IOException {
        return run(plan, PlanSessions.PROJECT_DATABASES, PlanSessions.shared(), Establish.DECLARED_SETUP, sinkOf,
                com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@code plan} run, its rows to {@code sink} in batches of up to {@code batchRows}. */
    public static void run(ExecutionPlan plan, SessionSource sessions, PlanSessions.Source planSessions,
            Establish establish, ResultSink sink, int batchRows) throws IOException {
        try (var callbacks = com.legend.base.RunScope.callingBack()) {
            runCalledBack(plan, sessions, planSessions, establish, sink, batchRows);
        }
    }

    /** {@link #run(ExecutionPlan, SessionSource, PlanSessions.Source, Establish, ResultSink, int)} inside the scope
     *  that refuses planning while the caller's sessions are opened. */
    private static void runCalledBack(ExecutionPlan plan, SessionSource sessions, PlanSessions.Source planSessions,
            Establish establish, ResultSink sink, int batchRows) throws IOException {
        if (plan.writes()) {
            PlanRunner.stream(plan, Map.of(), planSessions, sink, batchRows);
            return;
        }
        if (!plan.sendsTheAnswer()) {
            PlanRunner.stream(plan, Map.of(), NOTHING_SENT, sink, batchRows);
            return;
        }
        ExecutionPlan.Target target = plan.answerSql().target();
        try (Sessions.Session session = sessions.open(target)) {
            PlanSessions.Source on = switch (establish) {
                case DECLARED_SETUP -> PlanSessions.setUp(session, target);
                case AS_GIVEN -> PlanSessions.of(session);
            };
            PlanRunner.stream(plan, Map.of(), on, sink, batchRows);
        }
    }

    /** A constant answer's sessions: it sends no statement, so none is opened (until slice 5 the door opened the
     *  runtime's session for it and sent nothing on it). Asked for one, it refuses by name. */
    private static final PlanSessions.Source NOTHING_SENT = target -> {
        throw new IllegalStateException("a constant answer sends no statement: no session is opened for it");
    };
}
