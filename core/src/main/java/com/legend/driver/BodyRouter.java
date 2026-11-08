// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.Execution;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.DataError;
import com.legend.diagnostics.Census;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.TypedSide;
import com.legend.judge.SqlReplayReferee;
import com.legend.planner.ProductStatements;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testplan.BodyClass;
import com.legend.testplan.BodyKinds;
import com.legend.testplan.TestDoor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * THE BODY ROUTER at the test door (the test-is-the-product plan's R16, brief B1, M5): a body of a class that moves
 * ({@link BodyClass}, a named rule over its typed shape) is planned WHOLE by the test door and run as one plan on its
 * session; every other body is walked by the body driver, until M10 moves the last class and deletes the walk and
 * this router. A moved body the test door refuses fails (R14), never falls back to the walk.
 */
public final class BodyRouter {

    private BodyRouter() {
    }

    /** {@code typed} run on {@code session}: moved or walked ({@link BodyDriver#execute}'s contract either way). */
    public static void execute(TypedQuery given, @Nullable String runtimeFqn, SqlDialect dialect,
            Sessions.Session session, @Nullable AssertListener callers, @Nullable SqlReplayReferee referee,
            ExecuteOptions options, Execution.@Nullable Answer answer) {
        ExecuteOptions.JudgeMode mode = options.judgeMode();
        // the body's compatibility appeals, held at their asserts and decided after its run (P1.2, 2026-11-07); the
        // judges tell the caller's listener through it, its reports kept back while an appeal is held
        Appeals appeals = new Appeals(callers);
        AssertListener listener = appeals.listener();
        TypedQuery typed;
        BodyClass bodyClass;
        boolean systemRouted;
        HostAsserts host = null;
        DatabaseAsserts database = null;
        AgreementAsserts agreement = null;
        // the body's engine-text appeals, held beside its plan by number until the design's step 8 (EngineTextAppeals)
        EngineTextAppeals textAppeals = null;
        // THE WALL's rule 2 (2026-11-06): the body is PLANNED -- lifted, classed, its environment made, its asserts
        // planned -- with no run started, no session opened and no statement sent, each refused by name (until then
        // only the asserts' planning was scoped, and the system database's session was opened just before it)
        try (var planning = com.legend.exec.StatementOrigin.planning("a test body")) {
            typed = lifted(given, options);
            // every judge plans its asserts on the program road: the agreement judge too since the walker-deletion
            // program (2026-11-02; AgreementAsserts), so the classes are one judge's rules whatever the mode
            bodyClass = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn, true);
            // an assert program's asserts, planned by its judge before the program is (M6 the host's, M7 the
            // database's): a body holding an assert of a shape the judge's planner does not plan yet has none, and the
            // walk runs it (a named rule over its shape: AssertVerdicts.planHost; the database arms answering null)
            boolean assertsPlanned = bodyClass == BodyClass.ASSERT_PROGRAM || bodyClass == BodyClass.FRAME_PROGRAM;
            // a SYSTEM-ROUTED assert program (M10a): every store it reads is the graph's system metamodel store, so
            // its asserts are planned on the system database's TARGET and its program's target is that database, whose
            // session the run's sessions source opens -- one per graph, which the plan's own seed step writes
            // (SystemRoutes.seeds; nothing is sent before the plan runs, and planning names no session: I3)
            systemRouted = bodyClass == BodyClass.ASSERT_PROGRAM
                    && BodyClass.systemRouted(typed, java.util.Objects.requireNonNull(runtimeFqn));
            BodyDriver.ExecEnv bodyEnv = runtimeFqn == null ? null
                    : BodyDriver.bodyEnv(typed, runtimeFqn, dialect, listener, referee, options);
            BodyDriver.ExecEnv env = bodyEnv;
            textAppeals = bodyEnv == null ? null : bodyEnv.appeals();
            if (systemRouted) {
                BodyDriver.ExecEnv on = java.util.Objects.requireNonNull(bodyEnv);
                com.legend.planner.SystemRoutes.Route route = BodyDriver.systemRouteOf(typed.context(),
                        java.util.Objects.requireNonNull(runtimeFqn));
                // named, never opened at planning: the run opens the system database's session, and the program's
                // seed step writes the store (a server engine is refused by the class rule before this, so the tables
                // keep their names)
                env = on.withTarget(BodyDriver.systemTarget(route, dialect));
            }
            if (assertsPlanned) {
                // the body's own environment, the walk's: an engine-text assert of a system-routed body has its ARM
                // planned on it -- its appeal reads the user's tables on the body's session -- and its sides on the
                // program's (M10c item 10)
                BodyDriver.ExecEnv on = java.util.Objects.requireNonNull(env);
                String runtime = java.util.Objects.requireNonNull(runtimeFqn);
                // a frame program with no assert judges nothing (2026-11-04, the core program): its frames run and its
                // last statement answers as the host's planner plans them, whatever the judge -- the database judge's
                // planner composes frames into verdict statements and splices none into an answer (ExecuteFrameTest
                // under the database judge alone)
                boolean judgesNothing = bodyClass == BodyClass.FRAME_PROGRAM && !BodyClass.anAssert(typed, runtime);
                switch (judgesNothing ? ExecuteOptions.JudgeMode.HOST : mode) {
                    case HOST -> host = HostAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null);
                    case DATABASE -> database = DatabaseAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null);
                    // both judges in one plan: each assert's step carries the host's sides and the database's
                    // statement
                    case BOTH -> agreement = AgreementAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null,
                            systemRouted);
                }
            }
        }
        boolean assertsPlanned = bodyClass == BodyClass.ASSERT_PROGRAM || bodyClass == BodyClass.FRAME_PROGRAM;
        TestDoor.Asserts asserts = host != null ? host : database != null ? database : agreement;
        if (runtimeFqn == null || bodyClass == null || assertsPlanned && asserts == null) {
            String why = (runtimeFqn == null ? "no runtime" : bodyClass == null ? "no class admits the body"
                    : mode.name().toLowerCase(java.util.Locale.ROOT) + " judge's planner refused an assert")
                    + (runtimeFqn == null ? "" : " [" + BodyKinds.of(typed, runtimeFqn, null) + "]")
                    + (bodyClass == null && BodyClass.lastRefusal() != null
                            ? " {query rule: " + BodyClass.lastRefusal() + "}" : "");
            // the walk is deleted (2026-11-06): a body the program road does not plan is refused by name
            throw new com.legend.error.NotImplementedException("the program road does not plan this body: " + why);
        }
        Census.inc(Census.Key.BODIES_MOVED);
        TestDoor.Planned planned;
        try (var p = com.legend.exec.StatementOrigin.planning("a test body's program")) {
            planned = TestDoor.planned(typed, runtimeFqn, options.features(),
                    asserts == null ? TestDoor.Asserts.NONE : asserts, systemRouted, options.resources());
        }
        // the caller's sink for an answer a statement sends is asked for before anything is sent, as the walk asks
        // for it before its last statement runs (M5f's audit S1: a caller may refuse the shape); a constant's when the
        // program has run, as the walk answers a folded value at the body's end
        TestPlan plan = planned.plan();
        // G11 (the goals of record, 2026-11-07): every test body's plan is portable data -- it survives its JSON
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(plan);
        if (com.legend.diagnostics.Diagnostics.on("decoded-plan")) {
            // guard 3 in report mode (docs/PLAN_AS_DATA_2026_11_07.md 2.10, from the design's step 3, 2026-11-08; hard
            // from step 9): the run reads the plan READ BACK from its JSON, never the planner's objects -- a lookup by
            // a planned object's identity, not its data, shows as a verdict that changes on the lane run so
            plan = com.legend.testexecutionplan.TestPlanJson.read(com.legend.testexecutionplan.TestPlanJson.write(plan));
            planned = new TestDoor.Planned(plan, planned.effects(), planned.seeds());
        }
        if (com.legend.diagnostics.Diagnostics.on("plan-json")) {
            // the plan as data, printed whole (the plan-as-data design, 2026-11-07: what the run reads)
            System.out.println("[plan-json] " + com.legend.testexecutionplan.TestPlanJson.write(plan));
        }
        ResultSink early = answer != null && plan.answers() && plan.sendsTheAnswer() ? answer.of(plan.answerShape())
                : null;
        try {
            // the graph's system database for a step that names it -- the body's own target when the body is
            // system-routed, else the second, read-only session a statement reading the system store alone runs on
            // (per-statement routing, 2026-11-03) -- one session per graph and engine (SystemDatabase), opened when a
            // step first asks
            java.util.function.Function<ExecutionPlan.Database.System, Sessions.Session> systemSessions =
                    target -> SystemDatabase.of(typed.context()).sessionFor(target.type(), options.systemConnections());
            Moved moved = new Moved(planned, options.judgingRules(), dialect, options.recorder(), answer, early, host,
                    database, agreement, listener, sessionsByTarget(session, systemSessions));
            if (listener != null) {
                // the fixtures the planned appeals asked for, provided before the body's run (each a run of its own)
                listener.beforeRun();
            }
            try (var open = appeals.open();
                    var published = EngineTextAppeals.during(java.util.Objects.requireNonNull(textAppeals))) {
                TestRun.run(plan, Map.of(), PlanSessions.of(session, null, systemSessions), moved::textOrigin,
                        moved, moved);
            } catch (RuntimeException runFailed) {
                // the run failed after the appeals it held: an appeal precedes the failure, so a failed appeal is the
                // body's failure (the run's suppressed in it); else the run's failure stands (an Error -- a wall's
                // refusal, the JVM's -- passes untouched: nothing is decided after it)
                throw appeals.afterFailure(runFailed, session, systemSessions,
                        sessionsByTarget(session, systemSessions));
            }
            // the run is over: its held appeals decided, each its own plans at the edge -- the first that fails is the
            // body's failure (an answer the run streamed as it ran -- a sent one -- has already gone to the caller)
            appeals.decide(session, systemSessions, sessionsByTarget(session, systemSessions));
            if (answer != null && asserts != null && asserts.at(typed.body().size() - 1) != null) {
                // a body whose last statement is an assert answers its verdict as host rows, as the walk does
                JudgePlans.host(moved.lastVerdict(), answer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code given} as every road sees it: the lift (the asserts-as-plan-nodes program, step 4: an assert in a value's
     *  position is a statement of its own at its place, true where it stood), and register S137 (the user's ruling,
     *  2026-11-05: a relation passed to assertSize, assertEmpty or assertNotEmpty is ONE value upstream -- lite's own
     *  rules refuse it, the compatibility rules count it as upstream does). */
    private static TypedQuery lifted(TypedQuery given, ExecuteOptions options) {
        List<com.legend.compiler.spec.typed.TypedSpec> lifted = com.legend.compiler.spec.AssertLift.lift(given.body());
        if (options.judgingRules() == com.legend.testexecutionplan.TestPlan.JudgingRules.LITE) {
            com.legend.compiler.spec.RelationCountAsserts.refuse(lifted);
        } else {
            lifted = com.legend.compiler.spec.RelationCountAsserts.countedAsOne(lifted);
        }
        return lifted == given.body() ? given : given.withBody(lifted);
    }

    /**
     * {@code given} PLANNED NOW, to run later on a session of the same database (zero exemptions, the owner,
     * 2026-11-05): a runner's fixture program planned with the assert whose compatibility appeal provides it, and run
     * where the appeal provides it -- inside the body's run, which plans nothing. A body that judges nothing (an effect
     * or query program, no assert) planned whole by the test door as {@link #execute} plans it; null for any other
     * (the walk's, an assert's, a system-routed body's), which the caller cannot plan ahead.
     */
    public static @Nullable Execution.PlannedProgram planned(TypedQuery given, @Nullable String runtimeFqn,
            SqlDialect dialect, ExecuteOptions options) {
        TypedQuery typed = lifted(given, options);
        BodyClass bodyClass = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn, true);
        if (runtimeFqn == null || bodyClass == null || bodyClass == BodyClass.ASSERT_PROGRAM
                || BodyClass.anAssert(typed, runtimeFqn)) {
            return null;
        }
        TestDoor.Planned planned;
        try (var p = com.legend.exec.StatementOrigin.planning("a program planned ahead")) {
            planned = TestDoor.planned(typed, runtimeFqn, options.features(), TestDoor.Asserts.NONE, false,
                    options.resources());
        }
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(planned.plan());
        TestDoor.Planned ahead = com.legend.diagnostics.Diagnostics.on("decoded-plan")
                // guard 3 in report mode on a program planned ahead too (the slice 1 audit, 2026-11-08)
                ? new TestDoor.Planned(com.legend.testexecutionplan.TestPlanJson.read(
                        com.legend.testexecutionplan.TestPlanJson.write(planned.plan())), planned.effects(),
                        planned.seeds())
                : planned;
        return target -> {
            Census.inc(Census.Key.BODIES_MOVED);
            java.util.function.Function<ExecutionPlan.Database.System, Sessions.Session> systemSessions =
                    system -> SystemDatabase.of(typed.context()).sessionFor(system.type(), options.systemConnections());
            Moved moved = new Moved(ahead, options.judgingRules(), dialect, options.recorder(), null, null,
                    null, null, null, null, sessionsByTarget(target, systemSessions));
            try {
                TestRun.run(ahead.plan(), Map.of(), PlanSessions.of(target, null, systemSessions),
                        moved::textOrigin, moved, moved);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /** A moved body's sink: its answer to the caller's, by the shape of the plan's answer; its effect and seed
     *  statements to the raw-SQL ledger as each runs, the texts the walk records (the referee mirrors the ledger to
     *  replay the golden SQL of later bodies), and an effect's failure named as the walk names it; each assert's
     *  sides judged by the host judge at its verdict step, counted and reported to the listener as the walk does
     *  (M6). The database judge's verdicts reach no moved body: its bodies stay on the walk until M7. */
    /** The run's session for a target: the graph's system database for a target naming it, the body's own session for
     *  every other -- as the run's sessions source maps them ({@link PlanSessions#of}), unviewed: a judgment that needs
     *  the body's session (the referee's replay) asks the run for it ({@link RunReader#session}). */
    private static java.util.function.Function<ExecutionPlan.Target, Sessions.Session> sessionsByTarget(
            Sessions.Session session,
            java.util.function.Function<ExecutionPlan.Database.System, Sessions.Session> systemSessions) {
        return target -> target.database() instanceof ExecutionPlan.Database.System s ? systemSessions.apply(s)
                : session;
    }

    private static final class Moved implements com.legend.exec.ProgramRunner.ProgramSink, TestRun.Judges {
        private final TestDoor.Planned planned;
        private final SqlDialect dialect;
        private final RawSqlBoundary.@Nullable Recorder recorder;
        private final Execution.@Nullable Answer answer;
        /** The caller's sink, asked for before the program ran (a sent answer), or null (none, or a constant's). */
        private final @Nullable ResultSink early;
        /** The body's asserts as the host judge planned them (an assert program's under the host judge), or null. */
        private final @Nullable HostAsserts asserts;
        /** The database judge's segments, read from the plan (step 5, 2026-11-08), or null under another judge. */
        private final com.legend.judge.RootLoops.@Nullable Runs databaseRuns;
        /** The body's asserts as the database judge planned them (under the database judge), or null. */
        private final @Nullable DatabaseAsserts database;
        /** The body's asserts as both judges planned them (under the agreement judge), or null. */
        private final @Nullable AgreementAsserts agreement;
        /** The verdict steps whose engine-text check reads OUR text as a side the step runs, by the origin the walk's
         *  arm marked for it (M10c item 8). */
        private final java.util.Map<Integer, com.legend.exec.StatementOrigin> deferredTextOrigins;
        private final @Nullable AssertListener listener;
        /** The run's sessions by target, published to the judgments it runs ({@link RunReader#session}). */
        private final java.util.function.Function<ExecutionPlan.Target, Sessions.Session> sessions;
        /** The caller's sink holding the walk's contract, once the answer's columns arrive. */
        private @Nullable ResultSink opened;
        /** {@code node}'s verdict steps' rules recorded by verdict, each checked against the run's: a planner that
         *  stamped another run's rules is refused by name, never judged under them. */
        private void rulesOf(TestPlan.TestStep node, com.legend.testexecutionplan.TestPlan.JudgingRules runRules) {
            switch (node) {
                case TestPlan.Branch b -> {
                    b.then().forEach(step -> rulesOf(step, runRules));
                    b.otherwise().forEach(step -> rulesOf(step, runRules));
                }
                case TestPlan.Verdicts v -> {
                    if (v.rules() != runRules) {
                        throw new IllegalStateException("a verdict step planned under " + v.rules()
                                + " in a run judging by " + runRules);
                    }
                    v.verdicts().forEach(each -> rulesOf.put(each.id(), v.rules()));
                }
                default -> {
                }
            }
        }

        /** The rules {@code verdict}'s step names. */
        private com.legend.testexecutionplan.TestPlan.JudgingRules rules(TestPlan.Verdict verdict) {
            return java.util.Objects.requireNonNull(rulesOf.get(verdict.id()),
                    () -> "a verdict no step of the plan holds: " + verdict.name());
        }

        /** The last assert's verdict, once judged: a body ending on an assert answers it. */
        private @Nullable TypedSide lastVerdict;
        /** The rules each verdict is judged by, read off the plan's verdict steps (the explicit rule, 2026-11-04): the
         *  plan is the one carrier; the host judge is handed them at the verdict's step. */
        private final java.util.Map<Integer, com.legend.testexecutionplan.TestPlan.JudgingRules> rulesOf = new java.util.HashMap<>();

        Moved(TestDoor.Planned planned, com.legend.testexecutionplan.TestPlan.JudgingRules runRules, SqlDialect dialect,
                RawSqlBoundary.@Nullable Recorder recorder,
                Execution.@Nullable Answer answer, @Nullable ResultSink early,
                @Nullable HostAsserts asserts, @Nullable DatabaseAsserts database,
                @Nullable AgreementAsserts agreement, @Nullable AssertListener listener,
                java.util.function.Function<ExecutionPlan.Target, Sessions.Session> sessions) {
            this.planned = planned;
            this.sessions = sessions;
            this.dialect = dialect;
            this.recorder = recorder;
            this.answer = answer;
            this.early = early;
            this.asserts = asserts;
            this.database = database;
            this.agreement = agreement;
            this.listener = listener;
            this.databaseRuns = database != null ? new com.legend.judge.RootLoops.Runs(planned.plan().steps()) : null;
            planned.plan().steps().forEach(step -> rulesOf(step, runRules));
            java.util.Map<Integer, com.legend.exec.StatementOrigin> textOrigins = new java.util.HashMap<>();
            List<TestPlan.TestStep> steps = planned.plan().steps();
            for (int i = 0; i < steps.size(); i++) {
                // a verdict group's or a branch's statements under the database judge go under BODY, as the walk's
                // flush sent them (M7): the plan's judge says so (TestRun.program)
                if (!(steps.get(i) instanceof TestPlan.Verdicts group)) {
                    continue;
                }
                // the host's facts the plan carries on the group's first verdict (step 4, 2026-11-08): an agreement
                // step's or a host judge's one assert's
                TestPlan.Host p = agreement != null || asserts != null && group.verdicts().size() == 1
                        ? group.verdicts().get(0).host() : null;
                if (p != null) {
                    // an engine-text assert whose text is a side the step runs: sent under the origin the walk's arm
                    // marked for it (M10c item 8)
                    com.legend.exec.StatementOrigin o = AssertVerdicts.deferredTextOrigin(p.judgment());
                    if (o != null) {
                        textOrigins.put(i, o);
                    }
                }
            }
            this.deferredTextOrigins = textOrigins;
        }

        TypedSide lastVerdict() {
            return java.util.Objects.requireNonNull(lastVerdict, "a body ending on an assert judged none");
        }

        /** The database judge's row for {@code verdict} (M7): to its segment's batch, judged at the segment's last
         *  verdict by the batch's root loop; a body ending on an assert answers the verdict the walk answers. */
        @Override
        public void verdict(TestPlan.Verdict verdict, @Nullable List<Object> row) {
            if (agreement != null) {
                // both judges have read once the step's last row is in: the agreement rule decides
                TypedSide v;
                try (var judging = RunReader.judging(reader, sessions)) {
                    v = agreement.received(verdict, row, listener, reader);
                }
                if (v != null) {
                    lastVerdict = v;
                }
                return;
            }
            com.legend.judge.RootLoops.Runs runs = databaseRuns;
            if (runs == null) {
                throw new IllegalStateException("a database verdict in a body the host judge planned");
            }
            TestPlan.Host hostStep = verdict.host();
            if (hostStep != null && verdict.wrapper() != null) {
                // an assertError's verdict the database judges (S135): its row is its statement's, judged alone at its
                // step -- the host's facts name the assert it reports under
                try (var judging = RunReader.judging(reader, sessions)) {
                    AssertVerdicts.judgedRowAtItsStep(hostStep.assertion(), java.util.Objects.requireNonNull(row,
                            "an assertError's verdict statement answered no row"), listener);
                }
                lastVerdict = AssertVerdicts.ok();
                return;
            }
            if (hostStep != null) {
                // an assertError's step under the database judge: judged at its sides (ruling N3) -- or here, at its
                // turn, where it has no side to read (its error caught at planning, or its inner assert decided there):
                // the runner hands no sides for an empty base, and until 2026-11-04 such a step was never judged (a
                // body ending on it failed on its missing verdict, any other passed it silently)
                if (verdict.base().isEmpty()) {
                    judged(hostStep, List::of, null, rules(verdict));
                }
                return;
            }
            // a row of a segment: to its root loop, the plan's, judged once the segment's last verdict is in (inside
            // the program's run: a row the loop planned ahead is read through the program's reader, THE WALL)
            com.legend.judge.RootLoops.Run run = runs.of(verdict);
            try (var judging = RunReader.judging(reader, sessions)) {
                run.received(verdict, row);
                if (runs.closes(verdict)) {
                    run.judge(listener, reader, true, DatabaseJudge::verdictOf, EngineTextAppeals::rowAppeal);
                }
            }
            lastVerdict = AssertVerdicts.ok();
        }

        @Override
        public void fused(TestPlan.Verdicts group, @Nullable DataError rejected) {
            if (agreement != null) {
                agreement.fused(group, rejected);
                return;
            }
            com.legend.judge.RootLoops.Runs runs = databaseRuns;
            if (runs == null) {
                throw new IllegalStateException("a fused verdict group in a body the host judge planned");
            }
            runs.of(group.verdicts().get(0)).fused(rejected);
        }

        /** The assert {@code verdict} planned, judged over the rows the runner read for it -- counted and reported
         *  as the walk's {@code AssertVerdicts.tryAdjudicate} does: the assert entered, a verdict reported, a
         *  failure (an assert that does not hold) counted as raised and reported; a side the runner could not read
         *  comes to {@link #sidesFailed}, judged by the same path. */
        @Override
        public void sides(TestPlan.Verdict verdict, List<PlanRunner.Rows> rows) {
            if (agreement != null) {
                // the host's verdict held until the database half's rows are in
                agreement.hostSides(verdict, () -> rows, messageOf(verdict), listener, rules(verdict));
                return;
            }
            TestPlan.Host p = hostOf(verdict);
            java.util.function.Supplier<PlanRunner.Rows> read = messageOf(verdict);
            try (var judging = RunReader.judging(reader, sessions)) {
                judged(p, () -> HostSides.shapedBase(p.assertion().name(), p.shapes(), rows),
                        read == null ? null : HostSides.message(p.shapes(), read), rules(verdict));
            }
        }

        /** The program's reader, while it runs (PlanRunner.ProgramSink#reader). */
        private com.legend.judge.@Nullable SideReader reader;

        @Override
        public void reader(com.legend.judge.@Nullable SideReader reader) {
            this.reader = reader;
        }

        /** {@code verdict}'s caller's message read on demand (the judge asks only where it fails), or null where the
         *  verdict carries none. */
        private java.util.function.@Nullable Supplier<PlanRunner.Rows> messageOf(TestPlan.Verdict verdict) {
            TestPlan.Side message = verdict.message();
            if (message == null) {
                return null;
            }
            com.legend.judge.SideReader r = java.util.Objects.requireNonNull(reader,
                    "a verdict's message asked for with no program running");
            return () -> {
                try {
                    return r.read(message);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            };
        }

        /** A side of {@code verdict}'s that could not be read (the database refused it, or the read raised): judged as
         *  rows are, the judgment deciding the error (an assertError's answer; any other assert's failure, entered,
         *  counted and reported as the walk counts and reports an assert whose side raised). */
        @Override
        public void sidesFailed(TestPlan.Verdict verdict, RuntimeException failure) {
            if (agreement != null) {
                agreement.hostSides(verdict, () -> {
                    throw failure;
                }, null, listener, rules(verdict));
                return;
            }
            judged(hostOf(verdict), () -> {
                throw failure;
            }, null, rules(verdict));
        }

        /** The host's facts {@code verdict} carries in the plan (step 4, 2026-11-08): the host judge's assert, or,
         *  under the database judge, an assertError's step (ruling N3: Java compares the error in both judge modes). */
        private TestPlan.Host hostOf(TestPlan.Verdict verdict) {
            TestPlan.Host h = verdict.host();
            if (h == null) {
                throw new IllegalStateException(database != null
                        ? "host sides for a verdict the database judge planned: " + verdict.name()
                        : "a verdict's sides in a moved body whose plan carries no host facts for it: "
                                + verdict.name());
            }
            return h;
        }

        private void judged(TestPlan.Host p, java.util.function.Supplier<List<TypedSide>> sides,
                java.util.function.@Nullable Supplier<TypedSide> message,
                com.legend.testexecutionplan.TestPlan.JudgingRules rules) {
            com.legend.testexecutionplan.Assertion a = p.assertion();
            TypedSide v = AssertVerdicts.judgedAtItsStep(p, listener, sides, rules, message);
            if (v == null) {
                throw new com.legend.error.NotImplementedException(a.name() + " on the program road: the host"
                        + " judge declined the sides it read (the walk's generic path has no program form)");
            }
            lastVerdict = v;
        }

        /** The origin the verdict step at {@code step} reads our text under: an engine-text verdict step reading our
         *  text as a side, the origin the arm marked for it (M10c item 8); else null. (A frame let's eager run goes
         *  under LET and the database judge's steps under BODY by the plan's own facts: TestRun.program.) */
        com.legend.exec.@Nullable StatementOrigin textOrigin(int step) {
            return deferredTextOrigins.get(step);
        }

        /** Whether the program's step {@code step} is the graph's system store's seed step (the planner's). */
        private boolean systemStore(int step) {
            return planned.plan().steps().get(step) instanceof TestPlan.Run r
                    && r.node() instanceof ExecutionPlan.Setup u && u.key().equals(ExecutionPlan.Setup.SYSTEM_STORE);
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) throws IOException {
            if (answer != null) {
                ResultSink asked = early != null ? early : answer.of(planned.plan().answerShape());
                ResultSink sink = holdingTheWalksContract(asked, columns);
                opened = sink;
                sink.columns(columns);
            }
        }

        /** {@code sink} holding the cardinality the walk holds on a statement's answer (JudgePlans.streamResult): a
         *  value of at most one row answers no more, a required one at least one, a to-one reader exactly one; a
         *  constant the walk folds and answers as it is. */
        private ResultSink holdingTheWalksContract(ResultSink sink, List<ExecutionPlan.Column> columns) {
            if (!planned.plan().sendsTheAnswer()) {
                return sink;
            }
            return switch (planned.plan().answerShape()) {
                case VALUE -> {
                    ExecutionPlan.Multiplicity m = columns.get(0).multiplicity();
                    Integer upper = m.upper();
                    yield JudgePlans.holding(sink, upper != null && upper <= 1, m.lower(),
                            new Multiplicity.Bounded(m.lower(), upper).text());
                }
                // a relation's toOne() is the database's to check (D2, 2026-11-07: ToOneGuard in the plan's SQL)
                case RELATION -> sink;
                // a graph fetch's objects, one per row (the query program's answer since 2026-11-06, the stress
                // lanes' service queries): the product's streaming graph root, no cardinality the walk held on it
                case OBJECTS -> sink;
            };
        }

        @Override
        public void batch(Batch batch) throws IOException {
            if (opened != null) {
                opened.batch(batch);
            }
        }

        @Override
        public void end() throws IOException {
            if (opened != null) {
                opened.end();
            }
        }

        @Override
        public void seeded(int step, List<Boolean> answeredRows) {
            List<String> texts = planned.seeds().get(step);
            if (texts == null && systemStore(step)) {
                // the graph's system store's seeds are no body's: the walk's ledger recorded none of them
                return;
            }
            if (texts == null || answeredRows.size() > texts.size()) {
                throw new IllegalStateException("seed step " + step + " of a moved body ran "
                        + answeredRows.size() + " statement(s) with " + (texts == null ? "no" : texts.size())
                        + " ledger text(s) beside it");
            }
            for (int i = 0; i < answeredRows.size(); i++) {
                record(texts.get(i), answeredRows.get(i));
            }
        }

        @Override
        public void writing(int step) {
            Appeals.writing();
        }

        @Override
        public void effectRan(int step) {
            for (TestDoor.Recorded r : effect(step).statements()) {
                Census.inc(Census.Key.EFFECTS_IN_SCRIPT);
                record(r.text(), r.query());
            }
        }

        private TestDoor.Effect effect(int step) {
            TestDoor.Effect effect = planned.effects().get(step);
            if (effect == null) {
                throw new IllegalStateException("effect step " + step + " of a moved body ran with no ledger texts"
                        + " beside it");
            }
            return effect;
        }

        private void record(String text, boolean query) {
            if (recorder != null) {
                recorder.recordExecuted(text, query);
            }
        }
    }
}
