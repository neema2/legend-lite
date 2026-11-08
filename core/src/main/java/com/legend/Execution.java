// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.driver.BodyRouter;

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

    /** Where execution gets the session for the target the compiler decided (the server's connection resolver; a
     *  caller's own session, {@link #given}). {@code ctx} is the compiled model the target was decided from. Exec's
     *  {@code Sessions.Source} until D72 (2026-10-30): a contract spelled in the compiler's types is the driver's,
     *  never the runtime's. */
    @FunctionalInterface
    public interface Source {
        com.legend.exec.Sessions.Session open(com.legend.database.Target target, ModelContext ctx);

        /** A source of {@code session} as the caller holds it: a non-closing view of it for every target (D68,
         *  2026-10-30: the test runners hold sessions, not connections). It checks nothing at open: its callers are
         *  the program entry, whose {@code dialectOf} checks the session against the database the compiler decided,
         *  and the query entry, whose runner checks each statement's target (D70's audit: a check here was a second
         *  metadata read per body). */
        static Source given(com.legend.exec.Sessions.Session session) {
            return (target, ctx) -> com.legend.exec.Sessions.view(session);
        }
    }

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
            @com.legend.base.Nullable String runtimeFqn, Source sessions,
            com.legend.exec.PlanSessions.Source planSessions, Establish establish,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
            throws java.io.IOException {
        return run(typed, runtimeFqn, sessions, planSessions, establish, sinkOf,
                com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run(TypedQuery, String, Source, com.legend.exec.PlanSessions.Source,
     *  Establish, java.util.function.Function)} with the rows handed to the sink in batches of up to
     *  {@code batchRows} (the stream's granularity: a writer writes and flushes each batch as it arrives). */
    public static com.legend.executionplan.ExecutionPlan run(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, Source sessions,
            com.legend.exec.PlanSessions.Source planSessions, Establish establish,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf,
            int batchRows) throws java.io.IOException {
        return run(typed, runtimeFqn, sessions, planSessions, establish, java.util.Set.of(), sinkOf, batchRows);
    }

    /** {@link #run(TypedQuery, String, Source, com.legend.exec.PlanSessions.Source, Establish,
     *  java.util.function.Function, int)} under the query's FEATURE FLAGS: upstream's execution context carries them
     *  per query (its {@code ExecutionOptionContext}'s {@code FeatureFlagOption}), and the lowering reads them (2026-11-03:
     *  the one door's own parameter, so a caller that sets a flag -- the PCT, the user's ruling of 2026-09-12 -- runs
     *  through the door, never around it). */
    public static com.legend.executionplan.ExecutionPlan run(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, Source sessions,
            com.legend.exec.PlanSessions.Source planSessions, Establish establish,
            java.util.Set<com.legend.platform.Feature> features,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf,
            int batchRows) throws java.io.IOException {
        if (typed.writes()) {
            com.legend.executionplan.ExecutionPlan plan = planned(typed, runtimeFqn, features);
            com.legend.result.ResultSink sink = sinkOf.apply(plan);
            com.legend.exec.PlanRunner.stream(plan, java.util.Map.of(), planSessions, sink, batchRows);
            return plan;
        }
        ModelContext ctx = typed.context();
        com.legend.database.Target decided = Compiler.executesOn(ctx, runtimeFqn);
        try (com.legend.exec.Sessions.Session session = sessions.open(decided, ctx)) {
            com.legend.executionplan.ExecutionPlan plan = planned(typed, runtimeFqn, features);
            com.legend.result.ResultSink sink = sinkOf.apply(plan);
            com.legend.exec.PlanSessions.Source on = switch (establish) {
                // a constant answer sends nothing: no setup to run (the test-is-the-product plan's M2, B11)
                case DECLARED_SETUP -> plan.sendsTheAnswer()
                        ? com.legend.exec.PlanSessions.setUp(session, plan.answerSql().target())
                        : com.legend.exec.PlanSessions.of(session);
                case AS_GIVEN -> com.legend.exec.PlanSessions.of(session);
            };
            com.legend.exec.PlanRunner.stream(plan, java.util.Map.of(), on, sink, batchRows);
            return plan;
        }
    }

    /** {@code typed}'s plan, made under the wall's planning scope (THE WALL's rule 2, the final audit, 2026-11-06):
     *  no session opens and no run starts while the product's query is planned. */
    private static com.legend.executionplan.ExecutionPlan planned(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, java.util.Set<com.legend.platform.Feature> features) {
        try (var planning = com.legend.base.RunScope.plans("a query")) {
            com.legend.executionplan.ExecutionPlan plan = typed.executionPlan(runtimeFqn, features);
            roundTrip(plan);
            return plan;
        }
    }

    /** G11's guard on the product door: a plan that does not survive its JSON is refused. Checked where the test lanes
     *  ask for it ({@code -Dlegend.plan.roundtrip=true}, every junit lane: tools/junit/defs.bzl), so a product query pays
     *  nothing; the test-body door checks every plan it makes (BodyRouter). */
    static void roundTrip(com.legend.executionplan.ExecutionPlan plan) {
        if (Boolean.getBoolean("legend.plan.roundtrip")) {
            com.legend.executionplan.PlanJson.requireRoundTrip(plan);
        }
    }

    /** {@link #run(TypedQuery, String, Source, com.legend.exec.PlanSessions.Source,
     *  Establish, java.util.function.Function)} for a model's text and a query's text, the session as the caller set
     *  it up. */
    public static com.legend.executionplan.ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, Source sessions,
            com.legend.exec.PlanSessions.Source planSessions,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf)
            throws java.io.IOException {
        return run(model, query, runtimeFqn, sessions, planSessions, sinkOf, com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run(String, String, String, Source, com.legend.exec.PlanSessions.Source,
     *  java.util.function.Function)} in batches of up to {@code batchRows}. */
    public static com.legend.executionplan.ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, Source sessions,
            com.legend.exec.PlanSessions.Source planSessions,
            java.util.function.Function<com.legend.executionplan.ExecutionPlan, com.legend.result.ResultSink> sinkOf,
            int batchRows) throws java.io.IOException {
        return run(Compiler.query(Compiler.compileModel(model), query), runtimeFqn, sessions, planSessions,
                Establish.AS_GIVEN, sinkOf, batchRows);
    }

    /**
     * THE dialect of a program: the database its runtime executes on ({@link Compiler#executesOn}, upstream's
     * {@code createDbConfig(connection.type)}), read from the model alone. Its session setup is the body's target's,
     * which the plan runner runs on the connection once (D66, 2026-10-30; until then this entry ran it as setup
     * steps, once per program). The session is not read here (THE WALL, 2026-11-06: until then this entry CHECKED it
     * outside every run): the runner checks every session it is handed against each statement's target before the
     * statement runs ({@code JdbcStatements.check}), so a session on another database than the one declared is
     * refused there, never reinterpreted.
     */
    static com.legend.sql.dialect.SqlDialect dialectOf(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn) {
        // B6: the dialect states the session's FACTS (sessionSetup); the plan runner runs them, as the body's
        // target's session statements, before the connection's first statement and not again (JudgePlans.target)
        return Compiler.dialectOf(ctx, runtimeFqn);
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
     * (the source's lease: nothing for a caller's own, {@link Source#given}). It is exactly {@link #runBody} of
     * {@link #planBody}, nothing between them: it provides no fixture for the stores the plan's appeals name
     * ({@code PlannedAppeals.stores}) -- a caller whose appeals read another package's store plans and runs the two
     * halves itself, providing them between (the test runner does).
     */
    public static void runProgram(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            Source sessions, com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            com.legend.judge.@com.legend.base.Nullable SqlReplayReferee referee, ExecuteOptions options,
            @com.legend.base.Nullable Answer answer) {
        ModelContext ctx = typed.context();
        // planned before any session is opened: planning names targets, never sessions
        PlannedProgram planned = planBody(typed, runtimeFqn, options);
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(ctx, runtimeFqn), ctx)) {
            runBody(planned, session, ctx, listener, referee, options, answer);
        }
    }

    /** {@link #runProgram(TypedQuery, String, Source, com.legend.judge.AssertListener,
     *  com.legend.judge.SqlReplayReferee, ExecuteOptions, Answer)} with no answer wanted: the body's last statement
     *  runs as every other and answers nothing to anyone (the corpus's bodies, whose verdicts are the listener's). */
    public static void runProgram(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            Source sessions, com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            com.legend.judge.@com.legend.base.Nullable SqlReplayReferee referee, ExecuteOptions options) {
        runProgram(typed, runtimeFqn, sessions, listener, referee, options, null);
    }

    /** A test body or program PLANNED before it runs ({@link #planBody}, {@link #planProgram}): its test plan, data
     *  (the plan-as-data design's step 8c, 2026-11-08: until then a closure); run by {@link #runBody}, on a session of
     *  the database it was planned for, which plans nothing. */
    public record PlannedProgram(com.legend.testexecutionplan.TestPlan plan) {
        public PlannedProgram {
            java.util.Objects.requireNonNull(plan, "plan");
        }
    }

    /**
     * A test body PLANNED WHOLE, with no session and nothing told to anyone (hole 4, 2026-11-08: the planning half of
     * {@link #runProgram}): its unread lets made statements, its asserts planned by its judge's planner, the whole
     * planned by the test door; a body the program road does not plan is refused by name. Run by {@link #runBody}.
     */
    public static PlannedProgram planBody(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            ExecuteOptions options) {
        ModelContext ctx = typed.context();
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(ctx, runtimeFqn);
        // an unread let IS the statement it binds (the user's ruling, 2026-11-02): it runs at its turn, on every road
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> body = typed.body();
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> program =
                com.legend.planner.LetPlanner.unreadAsStatements(body, typed.resolved(), ctx);
        return new PlannedProgram(BodyRouter.plan(program != body ? typed.withBody(program) : typed, runtimeFqn,
                dialect, options));
    }

    /**
     * {@code planned} RUN on {@code session} (the run half of {@link #runProgram}): its plan's data alone decides what
     * is sent and judged; each verdict is told to {@code listener} at its turn -- the listener is the run's output and
     * is asked for nothing (hole 4, 2026-11-08: the fixtures a plan's appeals name, {@code PlannedAppeals.stores}, are
     * the caller's to provide before this); a held appeal is decided after the run by the {@code referee}; the last
     * statement's answer, where wanted, streams to {@code answer}'s sink. {@code ctx} is the graph it was planned
     * over: the run still reads it for the system store's sessions and an appeal's enum decode, until slice 4.
     */
    public static void runBody(PlannedProgram planned, com.legend.exec.Sessions.Session session, ModelContext ctx,
            com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            com.legend.judge.@com.legend.base.Nullable SqlReplayReferee referee, ExecuteOptions options,
            @com.legend.base.Nullable Answer answer) {
        // a plan naming an appeal it does not carry is refused before anything runs, whoever made it (the slice 3
        // invariant audit, 2026-11-08: the planning door checked it, a plan handed in did not)
        com.legend.testexecutionplan.PlannedAppeals.requireCarried(planned.plan());
        BodyRouter.run(planned.plan(), session, ctx, listener, referee, options, answer);
    }

    /** {@code typed} PLANNED NOW for a session of its runtime's database, to run later (zero exemptions, the
     *  owner, 2026-11-05: a test runner's fixture program, planned and run before the body whose compatibility appeal
     *  names its store, {@link #runBody}): as {@link #runProgram} plans it, with
     *  no session at all (THE WALL, 2026-11-06: until then a session was opened and checked to plan it); null for a
     *  body that cannot be planned ahead -- one holding an assert, or of no moved class (the walk's). */
    public static @com.legend.base.Nullable PlannedProgram planProgram(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, ExecuteOptions options) {
        ModelContext ctx = typed.context();
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(ctx, runtimeFqn);
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> body = typed.body();
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> program =
                com.legend.planner.LetPlanner.unreadAsStatements(body, typed.resolved(), ctx);
        return BodyRouter.planned(program != body ? typed.withBody(program) : typed, runtimeFqn, dialect, options);
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
}
