// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.Edge;

import com.legend.exec.StatementOrigin;
import com.legend.exec.SystemDatabase;
import com.legend.base.Nullable;
import com.legend.error.DataError;
import com.legend.diagnostics.Census;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.AssertListener;
import com.legend.testexecutionplan.TypedSide;
import com.legend.result.Batch;
import com.legend.result.Output;
import com.legend.result.ResultSink;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

public final class TestRunner {

    private TestRunner() {
    }


    /** A test body's answer is written in lite's formats, refused before anything is opened or sent otherwise (the
     *  follow-up audit, 2026-11-09: the engine's answer, which needs a plan's statement and columns, was refused only
     *  when its writer was first asked for -- after the body's writes, where the answer was not sent early). */
    private static void lites(@Nullable Output answer) {
        if (answer instanceof Output.Engine || answer instanceof Output.Arrow) {
            throw new IllegalArgumentException("a test body's answer is written in lite's formats (JSON rows, CSV, or "
                    + "the caller's receiver): the engine's answer needs a product plan's statement and columns");
        }
    }

    /**
     * A body's test plan RUN on the session {@code sessions} opens for the plan's own target ({@link TestPlan#session},
     * planned: slice 5, 2026-11-09 -- the door opened it from the model until then), closed when the run ends (a
     * caller's own session, {@code SessionSource.given}, is a view: closing it closes nothing). The test door's
     * {@code runProgram}: no fixture is provided between the body's plan and its run.
     */
    public static void runOpening(TestPlan plan, com.legend.exec.SessionSource sessions,
            Map<com.legend.executionplan.DatabaseType, Sessions.Opener> systemConnections, @Nullable Edge edge,
            @Nullable AssertListener callers, @Nullable SqlReplayReferee referee, @Nullable Output answer) {
        lites(answer);
        // the caller's session source is called back before the plan's run opens: it plans nothing
        try (var callbacks = com.legend.base.RunScope.callingBack();
                Sessions.Session session = sessions.open(plan.session())) {
            run(plan, session, systemConnections, edge, callers, referee, answer, StatementOrigin.OTHER);
        }
    }

    /**
     * A body's test plan (planned whole, or a program planned ahead: the test planner's) RUN on {@code session}: the
     * plan's data alone decides what is sent and judged, each verdict told to {@code callers} at its turn (the listener
     * is the run's output, never asked for anything: hole 4, 2026-11-08 -- the fixtures the plan's appeals name are
     * the caller's to provide before this, {@code PlannedAppeals.stores}); the appeals held are decided after the run
     * by the {@code referee}; its statements counted under {@code origin} (a fixture's seeding: SEED). No compiled
     * model: the system database's sessions are found by the plan's own targets (hole 2, 2026-11-08) and an appeal's
     * enums are the plan's (hole 1). A plan naming an appeal it does not carry is refused before anything runs,
     * whoever made it (the slice 3 invariant audit, 2026-11-08; the door checked it until slice 5).
     */
    public static void run(TestPlan plan, Sessions.Session session,
            Map<com.legend.executionplan.DatabaseType, Sessions.Opener> systemConnections,
            com.legend.testexecutionplan.@Nullable Edge edge,
            @Nullable AssertListener callers, @Nullable SqlReplayReferee referee,
            @Nullable Output answer, com.legend.exec.StatementOrigin origin) {
        com.legend.testexecutionplan.PlannedAppeals.requireCarried(plan);
        lites(answer);
        // the caller's answer sink and listener are called back during the run, and before its plan's run scope opens
        // (the answer's sink is made first): they plan nothing (the full audit's must-fix, 2026-11-09)
        try (var callbacks = com.legend.base.RunScope.callingBack()) {
            // the body's compatibility appeals, held at their asserts and decided after its run (P1.2, 2026-11-07); the
            // judges tell the caller's listener through it, its reports kept back while an appeal is held
            Appeals appeals = new Appeals(callers);
            // the run record (hole 6, 2026-11-09): what the judges share while the plan runs, handed to each explicitly
            BodyRun run = new BodyRun(appeals);
            AssertListener listener = appeals.listener();
            ResultSink early = answer != null && plan.answers() && plan.sendsTheAnswer() ? answer.writerFor(plan.answerShape())
                    : null;
            // the graph's system database for a step that names it -- the body's own target when the body is
            // system-routed, else the second, read-only session a statement reading the system store alone runs on
            // (per-statement routing, 2026-11-03) -- found by the plan's target (SystemDatabase: the engine's current
            // one, leased for the run), opened when a step first asks
            try (SystemDatabase.Leases leases = SystemDatabase.leases(systemConnections)) {
                SystemDatabase.Leases systemSessions = leases;
                Moved moved = new Moved(plan, edge, answer, early, listener, run);
                // the appeals the run hears, read from the plan's own data (step 8b), and how each is decided after the run
                java.util.Map<Integer, com.legend.testexecutionplan.Appeal> planned = com.legend.testexecutionplan.PlannedAppeals.byNumber(plan);
                Appeals.Decider decider = (n, golden, ours, textEqual, l, reader, sessions) ->
                        com.legend.testexec.SqlTextRuns.decideAppeal(com.legend.testexecutionplan.PlannedAppeals.of(planned, n),
                                golden, ours, textEqual, referee, l, reader, sessions);
                try {
                    TestRun.run(plan, Map.of(), PlanSessions.marked(PlanSessions.of(session, null, systemSessions), origin),
                            moved::textOrigin,
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
                    com.legend.testexec.SideShapes.host(moved.lastVerdict(), answer);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The run's session for a target: the graph's system database for a target naming it, the body's own session for
     *  every other -- as the run's sessions source maps them ({@link PlanSessions#of}), unviewed: the held appeals'
     *  decision after the run is handed it (the referee's replay; {@link Appeals#decide}). */
    private static java.util.function.Function<ExecutionPlan.Target, Sessions.Session> sessionsByTarget(
            Sessions.Session session,
            SystemDatabase.Leases systemSessions) {
        return target -> target.database() instanceof ExecutionPlan.Database.System s ? systemSessions.sessionFor(s)
                : session;
    }

    /** The run's sink: its answer to the caller's, by the shape of the plan's answer; its effect and seed statements'
     *  ledger texts to the edge as each runs (the referee mirrors the ledger to replay the golden SQL of later bodies),
     *  and an effect's failure named; each assert's verdict judged at its step by the judge the plan names and reported
     *  to the listener. */
    private static final class Moved implements com.legend.exec.ProgramRunner.ProgramSink, TestRun.Judges {
        private final TestPlan planned;
        /** The compatibility edge the run was handed, which records the referee's ledger, or null (lite's rules). */
        private final com.legend.testexecutionplan.@Nullable Edge edge;
        private final @Nullable Output answer;
        /** The caller's sink, asked for before the program ran (a sent answer), or null (none, or a constant's). */
        private final @Nullable ResultSink early;
        /** The database judge's segments, read from the plan (step 5, 2026-11-08), or null under another judge. */
        private final com.legend.testexec.RootLoops.@Nullable Runs databaseRuns;
        /** A database root's failed row's appeal, by its number in the plan (step 8b, 2026-11-08). */
        private final java.util.function.IntFunction<com.legend.testexec.RootLoops.Appeal> rowAppeals;
        /** The agreement judge's asserts, read from the plan (step 6, 2026-11-08), or null under another judge. */
        private final @Nullable AgreementRuns agreementRuns;
        /** The verdict steps whose engine-text check reads OUR text as a side the step runs, by the origin the walk's
         *  arm marked for it (M10c item 8). */
        private final java.util.Map<Integer, com.legend.exec.StatementOrigin> deferredTextOrigins;
        private final @Nullable AssertListener listener;
        /** The run record (hole 6, 2026-11-09): the held appeals and the caller's-message depth the judges share. */
        private final BodyRun run;
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
                @Nullable Output answer, @Nullable ResultSink early, @Nullable AssertListener listener,
                BodyRun run) {
            if (edge == null && !planned.extensionsByStep().isEmpty()) {
                // records planned for the edge (the compatibility rules' ledger texts) with no edge to hand them: the
                // plan was made for another run, refused by name, never run with its records dropped (the slice 3
                // invariant audit, 2026-11-08)
                throw new IllegalStateException("a test plan carrying the edge's records at steps "
                        + planned.extensionsByStep().keySet() + " run with no edge");
            }
            this.planned = planned;
            this.run = run;
            this.edge = edge;
            this.answer = answer;
            this.early = early;
            this.listener = listener;
            // each judge's run read from the plan: its judge, and the segments or pairings its verdict groups carry
            // (the plan-as-data design's steps 5 and 6, 2026-11-08)
            TestPlan.Judge judge = planned.judge();
            this.databaseRuns = judge == TestPlan.Judge.DATABASE
                    ? new com.legend.testexec.RootLoops.Runs(planned.steps()) : null;
            // a database row's appeal heard by its number, the plan's data (step 8b): the texts its row compared recorded
            java.util.Map<Integer, com.legend.testexecutionplan.Appeal> appealsPlanned = com.legend.testexecutionplan.PlannedAppeals.byNumber(planned);
            this.rowAppeals = n -> row -> com.legend.testexec.SqlTextRuns.rowAppealed(
                    com.legend.testexecutionplan.PlannedAppeals.of(appealsPlanned, n), row, run);
            this.agreementRuns = judge == TestPlan.Judge.AGREEMENT ? new AgreementRuns(planned.steps(), rowAppeals, run)
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
                    String o = com.legend.testexecutionplan.Verdicts.deferredTextOrigin(p.judgment());
                    if (o != null) {
                        textOrigins.put(i, com.legend.exec.StatementOrigin.valueOf(o));
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
                v = agreementRuns.of(verdict).received(verdict, row, listener, reader);
                if (v != null) {
                    lastVerdict = v;
                }
                return;
            }
            com.legend.testexec.RootLoops.Runs runs = databaseRuns;
            if (runs == null) {
                throw new IllegalStateException("a database verdict in a body the host judge planned");
            }
            TestPlan.Host hostStep = verdict.host();
            if (hostStep != null && verdict.wrapper() != null) {
                // an assertError's verdict the database judges (S135): its row is its statement's, judged alone at its
                // step -- the host's facts name the assert it reports under
                com.legend.testexec.AssertRuns.judgedRowAtItsStep(hostStep.assertion(), java.util.Objects.requireNonNull(row,
                        "an assertError's verdict statement answered no row"), listener);
                lastVerdict = com.legend.testexecutionplan.Verdicts.ok();
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
            com.legend.testexec.RootLoops.Run run = runs.of(verdict);
            run.received(verdict, row);
            if (runs.closes(verdict)) {
                run.judge(listener, reader, true, com.legend.testexec.DatabaseVerdicts::verdictOf, rowAppeals, this.run);
            }
            lastVerdict = com.legend.testexecutionplan.Verdicts.ok();
        }

        @Override
        public void fused(TestPlan.Verdicts group, @Nullable DataError rejected) {
            if (agreementRuns != null) {
                agreementRuns.of(group.verdicts().get(0)).fused(rejected);
                return;
            }
            com.legend.testexec.RootLoops.Runs runs = databaseRuns;
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
            judged(p, () -> com.legend.testexec.HostSideShapes.shapedBase(p.assertion().name(), p.shapes(), rows),
                    read == null ? null : com.legend.testexec.HostSideShapes.message(p.shapes(), read), rules(verdict));
        }

        /** The program's reader, while it runs (PlanRunner.ProgramSink#reader). */
        private com.legend.testexec.@Nullable SideReader reader;

        @Override
        public void reader(com.legend.testexec.@Nullable SideReader reader) {
            this.reader = reader;
        }

        /** {@code verdict}'s caller's message read on demand (the judge asks only where it fails), or null where the
         *  verdict carries none. */
        private java.util.function.@Nullable Supplier<PlanRunner.Rows> messageOf(TestPlan.Verdict verdict) {
            TestPlan.Side message = verdict.message();
            if (message == null) {
                return null;
            }
            com.legend.testexec.SideReader r = java.util.Objects.requireNonNull(reader,
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
            TypedSide v = com.legend.testexec.AssertRuns.judgedAtItsStep(p, listener, sides, rules, message, run);
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
                ResultSink asked = early != null ? early : answer.writerFor(planned.answerShape());
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
                    yield com.legend.testexec.SideShapes.holding(sink, upper != null && upper <= 1, m.lower(),
                            m.text());
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
            run.writing();
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
