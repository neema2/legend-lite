// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;

/**
 * THE EXECUTION FRONT DOOR (C2a, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): a query planned by
 * {@link Compiler} (the planner, which never touches a database — its library has no {@code exec}) run on a
 * session — handed by the caller and checked, or opened from the target the planner decided
 * ({@code Compiler.executesOn}, {@code exec.Sessions}). Execution reaches the planner only through its public API:
 * the compiled model, the resolved query, the target, and the lowered query ({@link Compiler.LoweredQuery}).
 */
public final class Execution {

    private Execution() {
    }

    /** How a query that only reads takes its session: established as legend-engine establishes a connection on every
     *  acquisition, the runtime's declared test data run on it first ({@code pure/v1} execute), or as the caller set it
     *  up (its own tables, its own connection). */
    public enum Establish { DECLARED_SETUP, AS_GIVEN }

    /**
     * THE PRODUCTION EXECUTION (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state recorded 2026-10-27): a query
     * typed once, planned as typed rows, run by the plan's runner, its rows streamed in batches to the sink the caller
     * makes from the plan that runs. {@code sinkOf} is called before any statement runs, so a sink that refuses the plan
     * (an answer its format has no form for) refuses it untouched -- a program's writes never land behind a refused
     * answer. A query that only reads runs on the session {@code sessions} opens for its runtime's target, the plan
     * written for that session's server; a program that writes runs on {@code planSessions} (decision A: a write runs
     * on a database of its own, a database the user names or a URL reaches keeps it). Returns the plan that ran.
     */
    public static com.legend.executionplan.ExecutionPlan run(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, com.legend.exec.Sessions.Source sessions,
            com.legend.exec.PlanSessions.Source planSessions, Establish establish,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
            throws java.io.IOException {
        return run(typed, runtimeFqn, sessions, planSessions, establish, sinkOf,
                com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run(TypedQuery, String, com.legend.exec.Sessions.Source, com.legend.exec.PlanSessions.Source,
     *  Establish, java.util.function.Function)} with the rows handed to the sink in batches of up to
     *  {@code batchRows} (the stream's granularity: a writer writes and flushes each batch as it arrives). */
    public static com.legend.executionplan.ExecutionPlan run(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, com.legend.exec.Sessions.Source sessions,
            com.legend.exec.PlanSessions.Source planSessions, Establish establish,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf,
            int batchRows) throws java.io.IOException {
        if (typed.writes()) {
            com.legend.executionplan.ExecutionPlan plan = typed.executionPlan(runtimeFqn);
            com.legend.result.ResultSink sink = sinkOf.apply(plan);
            com.legend.exec.PlanRunner.stream(plan, java.util.Map.of(), planSessions, sink, batchRows);
            return plan;
        }
        ModelContext ctx = typed.context();
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(ctx, runtimeFqn), ctx)) {
            java.sql.Connection connection = session.connection();
            com.legend.executionplan.ExecutionPlan plan = typed.executionPlan(runtimeFqn, java.util.Set.of(),
                    com.legend.exec.Sessions.version(connection));
            com.legend.result.ResultSink sink = sinkOf.apply(plan);
            com.legend.exec.PlanSessions.Source on = switch (establish) {
                case DECLARED_SETUP -> com.legend.exec.PlanSessions.setUp(connection, plan.answerSql().target());
                case AS_GIVEN -> com.legend.exec.PlanSessions.given(connection);
            };
            com.legend.exec.PlanRunner.stream(plan, java.util.Map.of(), on, sink, batchRows);
            return plan;
        }
    }

    /** {@link #run(TypedQuery, String, com.legend.exec.Sessions.Source, com.legend.exec.PlanSessions.Source,
     *  Establish, java.util.function.Function)} for a model's text and a query's text, the session as the caller set
     *  it up. */
    public static com.legend.executionplan.ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, com.legend.exec.Sessions.Source sessions,
            com.legend.exec.PlanSessions.Source planSessions,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
            throws java.io.IOException {
        return run(model, query, runtimeFqn, sessions, planSessions, sinkOf, com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run(String, String, String, com.legend.exec.Sessions.Source, com.legend.exec.PlanSessions.Source,
     *  java.util.function.Function)} in batches of up to {@code batchRows}. */
    public static com.legend.executionplan.ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, com.legend.exec.Sessions.Source sessions,
            com.legend.exec.PlanSessions.Source planSessions,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf,
            int batchRows) throws java.io.IOException {
        return run(Compiler.query(Compiler.compileModel(model), query), runtimeFqn, sessions, planSessions,
                Establish.AS_GIVEN, sinkOf, batchRows);
    }

    /**
     * THE dialect of a program that runs on {@code session}: the database its runtime executes on
     * ({@link Compiler#executesOn}, upstream's {@code createDbConfig(connection.type)}), refined by the server's
     * version ({@code SqlDialect.forServer}), with its session setup run once. The session is only
     * CHECKED ({@code Sessions.check}): connected to another database than the one declared is refused,
     * never reinterpreted.
     */
    static com.legend.sql.dialect.SqlDialect dialectOf(ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            com.legend.exec.Sessions.Session session) {
        com.legend.database.Target decided = Compiler.executesOn(ctx, runtimeFqn);
        com.legend.exec.Sessions.check(session.connection(), decided.type());
        String version = session.version();
        com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(decided.type())
                .forServer(version);
        // B6: session setup rides the connection-dialect resolution -- the ONE seam every connection-bearing entry
        // passes through; the dialect states the FACTS, the plan runner runs them as the program's setup steps on
        // its target (the doors, 2026-10-29; until then SetupRunner sent them on the session's statements)
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.SESSION)) {
            com.legend.exec.PlanRunner.setUp(dialect.sessionSetup().stream()
                    .<com.legend.executionplan.ExecutionPlan.SetupStep>map(
                            com.legend.executionplan.ExecutionPlan.SetupStep.Statement::new).toList(),
                    JudgePlans.target(decided, version), com.legend.exec.PlanSessions.of(session, null), null);
        }
        return dialect;
    }

    /**
     * THE ONE ENTRY FOR A PROGRAM (track B's B2, 2026-10-29; docs/RESULT_BOUNDARY_START_HERE.md, the track B block;
     * the plan path program's step 8): a test body -- lets, effects, asserts, a value -- typed once
     * ({@link Compiler#query}), run statement by statement by the body driver on the session {@code sessions} opens
     * for the target its runtime declares (checked against the declared database, the dialect refined by the server's
     * version, the session's setup run once: {@link #dialectOf}), every assert's verdict reported to {@code listener}
     * as it is judged (one event each, in statement order; a failure is raised after its report), the SQL-text
     * referee {@code referee} consulted where an assert carries a golden text (the harnesses' registration seam;
     * production passes neither), nothing materialized returned: the last statement's answer, where a caller wants
     * it, streams to the sink the caller hands for its shape ({@link Answer}; the doors, 2026-10-29: until then the
     * last side came back materialized, {@code ProgramReport.last}). The session is closed when the program ends
     * (the source's lease: nothing for a caller's own, {@link com.legend.exec.Sessions#given}).
     */
    public static void runProgram(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            com.legend.exec.Sessions.Source sessions, com.legend.exec.@com.legend.base.Nullable AssertListener listener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayReferee referee, ExecuteOptions options,
            @com.legend.base.Nullable Answer answer) {
        ModelContext ctx = typed.context();
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(ctx, runtimeFqn), ctx)) {
            runProgram(typed, runtimeFqn, session, listener, referee, options, answer);
        }
    }

    /** {@link #runProgram(TypedQuery, String, com.legend.exec.Sessions.Source, com.legend.exec.AssertListener,
     *  com.legend.exec.SqlReplayReferee, ExecuteOptions, Answer)} with no answer wanted: the body's last statement
     *  runs as every other and answers nothing to anyone (the corpus's bodies, whose verdicts are the listener's). */
    public static void runProgram(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            com.legend.exec.Sessions.Source sessions, com.legend.exec.@com.legend.base.Nullable AssertListener listener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayReferee referee, ExecuteOptions options) {
        runProgram(typed, runtimeFqn, sessions, listener, referee, options, null);
    }

    /** Where a program's last statement answers: the sink for its shape, made once the shape is known (a relation,
     *  a value of one column, a graph fetch's objects) -- a writer at the boundary, as a product answer's
     *  ({@link #run}'s {@code sinkOf}). The body driver streams a planned statement's typed rows through the plan
     *  runner to it; an answer it holds in Java (a host constant the prelude gave, a verdict, a staged let's rows, a
     *  graph fetch's objects read as one cell) is written through a batcher as a host's rows are. */
    @FunctionalInterface
    public interface Answer {
        com.legend.result.ResultSink of(com.legend.executionplan.ExecutionPlan.TdsResult.Shape shape);
    }

    /** {@link #runProgram(TypedQuery, String, com.legend.exec.Sessions.Source, com.legend.exec.AssertListener,
     *  com.legend.exec.SqlReplayReferee, ExecuteOptions, Answer)} on a session the caller holds and keeps (the old
     *  door's connection): checked, its dialect decided, the body driver run on it; the one place a program's
     *  session meets its JDBC connection. A program leaves nothing behind: its verdicts are the listener's, one
     *  event each, its last statement's answer the sink's ({@code ProgramReport}, the one materialized side a public
     *  entry returned, went 2026-10-29 with the doors). */
    static void runProgram(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            com.legend.exec.Sessions.Session session, com.legend.exec.@com.legend.base.Nullable AssertListener listener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayReferee referee, ExecuteOptions options,
            @com.legend.base.Nullable Answer answer) {
        ModelContext ctx = typed.context();
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(ctx, runtimeFqn, session);
        BodyDriver.execute(typed, runtimeFqn, dialect, session, listener, referee, options, answer);
    }
}
