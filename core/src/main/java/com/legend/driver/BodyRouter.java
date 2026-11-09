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

    /**
     * {@code given} PLANNED WHOLE by the test door, its asserts by its judge's planner: lifted, classed, its environment
     * made -- with no run started, no session opened, no statement sent, and nothing told to anyone (hole 4,
     * 2026-11-08: planning is handed no listener and no referee, so what it decides reaches the caller only as the
     * plan's data, at its turn in the run). A body the program road does not plan is refused by name.
     */
    public static TestPlan plan(TypedQuery given, @Nullable String runtimeFqn, SqlDialect dialect,
            ExecuteOptions options) {
        ExecuteOptions.JudgeMode mode = options.judgeMode();
        TypedQuery typed;
        BodyClass bodyClass;
        boolean systemRouted;
        HostAsserts host = null;
        DatabaseAsserts database = null;
        AgreementAsserts agreement = null;
        // the body's engine-text appeals as its arms planned them, by number: the plan carries those its verdicts name
        // (step 8b, 2026-11-08)
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
                    : BodyDriver.bodyEnv(typed, runtimeFqn, dialect, options);
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
        // the census of the bodies the test door plans whole (a measurement: counted where the class admits the body)
        Census.inc(Census.Key.BODIES_MOVED);
        TestPlan plan;
        try (var p = com.legend.exec.StatementOrigin.planning("a test body's program")) {
            plan = TestDoor.planned(typed, runtimeFqn, options.features(),
                    asserts == null ? TestDoor.Asserts.NONE : asserts, systemRouted, options.resources(),
                    options.edge());
        }
        // the caller's sink for an answer a statement sends is asked for before anything is sent, as the walk asks
        // for it before its last statement runs (M5f's audit S1: a caller may refuse the shape); a constant's when the
        // program has run, as the walk answers a folded value at the body's end
        // G11 (the goals of record, 2026-11-07): every test body's plan is portable data -- it survives its JSON
        if (textAppeals != null) {
            // the appeals the plan's verdicts name, the plan's own data (the plan-as-data design's step 8b, 2026-11-08):
            // heard at their verdicts' steps, decided after the run from these records. Under lite's own rules the
            // verdicts name none, so the plan carries none; whatever the rules, a plan naming an appeal it does not
            // carry is refused (PlannedAppeals.requireCarried, the slice 2 invariant audit's D2)
            plan = plan.withExtensionsByVerdict(AppealsByVerdict.of(plan, textAppeals));
        }
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(plan);
        if (com.legend.diagnostics.Diagnostics.on("decoded-plan")) {
            // guard 3 in report mode (docs/PLAN_AS_DATA_2026_11_07.md 2.10, from the design's step 3, 2026-11-08; hard
            // from step 9): the run reads the plan READ BACK from its JSON, never the planner's objects -- a lookup by
            // a planned object's identity, not its data, shows as a verdict that changes on the lane run so
            plan = com.legend.testexecutionplan.TestPlanJson.read(com.legend.testexecutionplan.TestPlanJson.write(plan));
        }
        if (com.legend.diagnostics.Diagnostics.on("plan-json")) {
            // the plan as data, printed whole (the plan-as-data design, 2026-11-07: what the run reads)
            System.out.println("[plan-json] " + com.legend.testexecutionplan.TestPlanJson.write(plan));
        }
        return plan;
    }

    /**
     * A body's plan ({@link #plan}, or a program planned ahead, {@link #planned}) RUN on {@code session}: the plan's
     * data alone decides what is sent and judged, each verdict told to {@code callers} at its turn (the listener is the
     * run's output, never asked for anything: hole 4, 2026-11-08 -- the fixtures the plan's appeals name are the
     * caller's to provide before this, {@code PlannedAppeals.stores}); the appeals held are decided after the run by
     * the {@code referee}. No compiled model: the system database's sessions are found by the plan's own targets
     * (hole 2, 2026-11-08) and an appeal's enums are the plan's (hole 1).
     */
    public static void run(TestPlan plan, Sessions.Session session,
            @Nullable AssertListener callers, @Nullable SqlReplayReferee referee, ExecuteOptions options,
            Execution.@Nullable Answer answer) {
        // the body's compatibility appeals, held at their asserts and decided after its run (P1.2, 2026-11-07); the
        // judges tell the caller's listener through it, its reports kept back while an appeal is held
        Appeals appeals = new Appeals(callers);
        AssertListener listener = appeals.listener();
        ResultSink early = answer != null && plan.answers() && plan.sendsTheAnswer() ? answer.of(plan.answerShape())
                : null;
        // the graph's system database for a step that names it -- the body's own target when the body is
        // system-routed, else the second, read-only session a statement reading the system store alone runs on
        // (per-statement routing, 2026-11-03) -- found by the plan's target (SystemDatabase: the engine's current
        // one, leased for the run), opened when a step first asks
        try (SystemDatabase.Leases leases = SystemDatabase.leases(options.systemConnections())) {
            java.util.function.Function<ExecutionPlan.Database.System, Sessions.Session> systemSessions =
                    leases::sessionFor;
            Moved moved = new Moved(plan, options.edge(), answer, early,
                    listener, sessionsByTarget(session, systemSessions));
            // the appeals the run hears, read from the plan's own data (step 8b), and how each is decided after the run
            java.util.Map<Integer, com.legend.testexecutionplan.Appeal> planned = com.legend.testexecutionplan.PlannedAppeals.byNumber(plan);
            Appeals.Decider decider = (n, golden, ours, textEqual, l) -> SqlTextVerdicts.decideAppeal(
                    com.legend.testexecutionplan.PlannedAppeals.of(planned, n), golden, ours, textEqual, referee, l);
            try (var open = appeals.open()) {
                TestRun.run(plan, Map.of(), PlanSessions.of(session, null, systemSessions), moved::textOrigin,
                        moved, moved);
            } catch (RuntimeException runFailed) {
                // the run failed after the appeals it held: an appeal precedes the failure, so a failed appeal is the
                // body's failure (the run's suppressed in it); else the run's failure stands (an Error -- a wall's
                // refusal, the JVM's -- passes untouched: nothing is decided after it)
                throw appeals.afterFailure(runFailed, session, systemSessions,
                        sessionsByTarget(session, systemSessions), decider);
            }
            // the run is over: its held appeals decided, each its own plans at the edge -- the first that fails is the
            // body's failure (an answer the run streamed as it ran -- a sent one -- has already gone to the caller)
            appeals.decide(session, systemSessions, sessionsByTarget(session, systemSessions), decider);
            if (answer != null && plan.answersLastVerdict()) {
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
     * or query program, no assert) planned whole by the test door as {@link #plan} plans it; null for any other
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
        // counted where it is planned, as a body is (plan; until hole 4 a program planned ahead counted when it ran)
        Census.inc(Census.Key.BODIES_MOVED);
        TestPlan planned;
        try (var p = com.legend.exec.StatementOrigin.planning("a program planned ahead")) {
            planned = TestDoor.planned(typed, runtimeFqn, options.features(), TestDoor.Asserts.NONE, false,
                    options.resources(), options.edge());
        }
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(planned);
        TestPlan ahead = com.legend.diagnostics.Diagnostics.on("decoded-plan")
                // guard 3 in report mode on a program planned ahead too (the slice 1 audit, 2026-11-08)
                ? com.legend.testexecutionplan.TestPlanJson.read(com.legend.testexecutionplan.TestPlanJson.write(planned))
                : planned;
        return new Execution.PlannedProgram(ahead);
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
        private final TestPlan planned;
        /** The compatibility edge the run was handed, which records the referee's ledger, or null (lite's rules). */
        private final com.legend.testexecutionplan.@Nullable Edge edge;
        private final Execution.@Nullable Answer answer;
        /** The caller's sink, asked for before the program ran (a sent answer), or null (none, or a constant's). */
        private final @Nullable ResultSink early;
        /** The database judge's segments, read from the plan (step 5, 2026-11-08), or null under another judge. */
        private final com.legend.judge.RootLoops.@Nullable Runs databaseRuns;
        /** A database root's failed row's appeal, by its number in the plan (step 8b, 2026-11-08). */
        private final java.util.function.IntFunction<com.legend.judge.VerdictBatch.Appeal> rowAppeals;
        /** The agreement judge's asserts, read from the plan (step 6, 2026-11-08), or null under another judge. */
        private final @Nullable AgreementRuns agreementRuns;
        /** The verdict steps whose engine-text check reads OUR text as a side the step runs, by the origin the walk's
         *  arm marked for it (M10c item 8). */
        private final java.util.Map<Integer, com.legend.exec.StatementOrigin> deferredTextOrigins;
        private final @Nullable AssertListener listener;
        /** The run's sessions by target, published to the judgments it runs ({@link RunReader#session}). */
        private final java.util.function.Function<ExecutionPlan.Target, Sessions.Session> sessions;
        /** The caller's sink holding the walk's contract, once the answer's columns arrive. */
        private @Nullable ResultSink opened;
        /** {@code node}'s verdict steps' rules recorded by verdict: the plan's rules stand, the run reads them and no
         *  option of its own (the slice 2 invariant audit's hole 8, 2026-11-08: the run checked them against the
         *  caller's options). */
        private void rulesOf(TestPlan.TestStep node) {
            switch (node) {
                case TestPlan.Branch b -> {
                    b.then().forEach(this::rulesOf);
                    b.otherwise().forEach(this::rulesOf);
                }
                case TestPlan.Verdicts v -> {
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

        Moved(TestPlan planned, com.legend.testexecutionplan.@Nullable Edge edge,
                Execution.@Nullable Answer answer, @Nullable ResultSink early, @Nullable AssertListener listener,
                java.util.function.Function<ExecutionPlan.Target, Sessions.Session> sessions) {
            if (edge == null && !planned.extensionsByStep().isEmpty()) {
                // records planned for the edge (the compatibility rules' ledger texts) with no edge to hand them: the
                // plan was made for another run, refused by name, never run with its records dropped (the slice 3
                // invariant audit, 2026-11-08)
                throw new IllegalStateException("a test plan carrying the edge's records at steps "
                        + planned.extensionsByStep().keySet() + " run with no edge");
            }
            this.planned = planned;
            this.sessions = sessions;
            this.edge = edge;
            this.answer = answer;
            this.early = early;
            this.listener = listener;
            // each judge's run read from the plan: its judge, and the segments or pairings its verdict groups carry
            // (the plan-as-data design's steps 5 and 6, 2026-11-08)
            TestPlan.Judge judge = planned.judge();
            this.databaseRuns = judge == TestPlan.Judge.DATABASE
                    ? new com.legend.judge.RootLoops.Runs(planned.steps()) : null;
            // a database row's appeal heard by its number, the plan's data (step 8b): the texts its row compared recorded
            java.util.Map<Integer, com.legend.testexecutionplan.Appeal> appealsPlanned = com.legend.testexecutionplan.PlannedAppeals.byNumber(planned);
            this.rowAppeals = n -> row -> SqlTextVerdicts.rowAppealed(com.legend.testexecutionplan.PlannedAppeals.of(appealsPlanned, n), row);
            this.agreementRuns = judge == TestPlan.Judge.AGREEMENT ? new AgreementRuns(planned.steps(), rowAppeals)
                    : null;
            planned.steps().forEach(this::rulesOf);
            java.util.Map<Integer, com.legend.exec.StatementOrigin> textOrigins = new java.util.HashMap<>();
            List<TestPlan.TestStep> steps = planned.steps();
            for (int i = 0; i < steps.size(); i++) {
                // a verdict group's or a branch's statements under the database judge go under BODY, as the walk's
                // flush sent them (M7): the plan's judge says so (TestRun.program)
                if (!(steps.get(i) instanceof TestPlan.Verdicts group)) {
                    continue;
                }
                // the host's facts the plan carries on the group's first verdict (step 4, 2026-11-08): an agreement
                // step's or a host judge's one assert's
                TestPlan.Host p = judge == TestPlan.Judge.AGREEMENT
                        || judge == TestPlan.Judge.HOST && group.verdicts().size() == 1
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
            if (agreementRuns != null) {
                // both judges have read once the step's last row is in: the agreement rule decides
                TypedSide v;
                try (var judging = RunReader.judging(reader, sessions)) {
                    v = agreementRuns.of(verdict).received(verdict, row, listener, reader);
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
                    run.judge(listener, reader, true, DatabaseJudge::verdictOf, rowAppeals);
                }
            }
            lastVerdict = AssertVerdicts.ok();
        }

        @Override
        public void fused(TestPlan.Verdicts group, @Nullable DataError rejected) {
            if (agreementRuns != null) {
                agreementRuns.of(group.verdicts().get(0)).fused(rejected);
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
            if (agreementRuns != null) {
                // the host's verdict held until the database half's rows are in
                agreementRuns.of(verdict).hostSides(verdict, () -> rows, messageOf(verdict), listener,
                        rules(verdict));
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
            if (agreementRuns != null) {
                agreementRuns.of(verdict).hostSides(verdict, () -> {
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
                throw new IllegalStateException(databaseRuns != null
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
            return planned.steps().get(step) instanceof TestPlan.Run r
                    && r.node() instanceof ExecutionPlan.Setup u && u.key().equals(ExecutionPlan.Setup.SYSTEM_STORE);
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) throws IOException {
            if (answer != null) {
                ResultSink asked = early != null ? early : answer.of(planned.answerShape());
                ResultSink sink = holdingTheWalksContract(asked, columns);
                opened = sink;
                sink.columns(columns);
            }
        }

        /** {@code sink} holding the cardinality the walk holds on a statement's answer (JudgePlans.streamResult): a
         *  value of at most one row answers no more, a required one at least one, a to-one reader exactly one; a
         *  constant the walk folds and answers as it is. */
        private ResultSink holdingTheWalksContract(ResultSink sink, List<ExecutionPlan.Column> columns) {
            if (!planned.sendsTheAnswer()) {
                return sink;
            }
            return switch (planned.answerShape()) {
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
            com.legend.testexecutionplan.Edge e = edge;
            if (e == null) {
                return;   // lite's own rules: nothing planned for the edge, no ledger
            }
            List<com.legend.testexecutionplan.Extension> texts = planned.extensionsByStep().get(step);
            if (texts == null && systemStore(step)) {
                // the graph's system store's seeds are no body's: the walk's ledger recorded none of them
                return;
            }
            if (texts == null) {
                throw new IllegalStateException("seed step " + step + " of a moved body ran " + answeredRows.size()
                        + " statement(s) with no ledger texts planned at it");
            }
            e.seeded(texts, answeredRows);
        }

        @Override
        public void writing(int step) {
            Appeals.writing();
        }

        @Override
        public void effectRan(int step) {
            // the census counts the effect's statements from the plan's own step (its script), whatever the rules
            if (!(planned.steps().get(step) instanceof TestPlan.Run r
                    && r.node() instanceof ExecutionPlan.Effect effect && effect.script() != null)) {
                throw new IllegalStateException("effect step " + step + " of a moved body is no scripted effect of the"
                        + " plan");
            }
            for (int i = 0; i < effect.script().statements().size(); i++) {
                Census.inc(Census.Key.EFFECTS_IN_SCRIPT);
            }
            com.legend.testexecutionplan.Edge e = edge;
            if (e != null) {
                List<com.legend.testexecutionplan.Extension> texts = planned.extensionsByStep().get(step);
                if (texts == null) {
                    throw new IllegalStateException("effect step " + step + " of a moved body ran with no ledger texts"
                            + " planned at it");
                }
                e.effectRan(texts);
            }
        }
    }
}
