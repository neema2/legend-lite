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
import com.legend.exec.Census;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.TypedSide;
import com.legend.judge.SqlReplayReferee;
import com.legend.planner.ProductStatements;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testplan.BodyClass;
import com.legend.testplan.EffectSegment;
import com.legend.testplan.TestDoor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

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
    public static void execute(TypedQuery typed, @Nullable String runtimeFqn, SqlDialect dialect,
            Sessions.Session session, @Nullable AssertListener listener, @Nullable SqlReplayReferee referee,
            ExecuteOptions options, Execution.@Nullable Answer answer) {
        boolean hostJudge = options.judgeMode() == ExecuteOptions.JudgeMode.HOST;
        BodyClass bodyClass = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn, hostJudge);
        // an assert program's asserts, planned by the host judge before the program is (M6): a body holding an assert
        // of a shape the host checks do not plan yet has none, and the walk runs it (a named rule over its shape:
        // AssertVerdicts.planHost)
        HostAsserts asserts = bodyClass != BodyClass.ASSERT_PROGRAM ? null : HostAsserts.plan(typed,
                java.util.Objects.requireNonNull(runtimeFqn), BodyDriver.bodyEnv(typed, runtimeFqn, dialect, session,
                        listener, referee, options));
        if (runtimeFqn == null || bodyClass == null || bodyClass == BodyClass.ASSERT_PROGRAM && asserts == null) {
            BodyDriver.execute(typed, runtimeFqn, dialect, session, listener, referee, options, answer);
            return;
        }
        Census.inc(Census.Key.BODIES_MOVED);
        TestDoor.Planned planned = TestDoor.planned(typed, runtimeFqn, options.features(), session.version(),
                asserts == null ? TestDoor.Asserts.NONE : asserts);
        // the caller's sink for an answer a statement sends is asked for before anything is sent, as the walk asks
        // for it before its last statement runs (M5f's audit S1: a caller may refuse the shape); a constant's when the
        // program has run, as the walk answers a folded value at the body's end
        ExecutionPlan plan = planned.plan();
        ResultSink early = answer != null && plan.answers() && plan.sendsTheAnswer() ? answer.of(plan.answerShape())
                : null;
        try {
            Moved moved = new Moved(planned, dialect, options.recorder(), answer, early, toOneReader(typed), asserts,
                    listener);
            PlanRunner.program(plan, Map.of(), PlanSessions.of(session), PlanRunner.Judging.HOST, moved);
            if (answer != null && asserts != null && asserts.at(typed.body().size() - 1) != null) {
                // a body whose last statement is an assert answers its verdict as host rows, as the walk does
                JudgePlans.host(moved.lastVerdict(), answer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Whether {@code typed}'s last statement is a to-one reader ({@code toOne()} over a relation): the walk's
     *  exactly-one contract on its answer (a moved class admits one only as the answer's own root). */
    private static boolean toOneReader(TypedQuery typed) {
        List<TypedSpec> body = typed.body();
        return !body.isEmpty() && ProductStatements.toOneReader(Lets.bare(body.get(body.size() - 1)));
    }

    /** A moved body's sink: its answer to the caller's, by the shape of the plan's answer; its effect and seed
     *  statements to the raw-SQL ledger as each runs, the texts the walk records (the referee mirrors the ledger to
     *  replay the golden SQL of later bodies), and an effect's failure named as the walk names it; each assert's
     *  sides judged by the host judge at its verdict step, counted and reported to the listener as the walk does
     *  (M6). The database judge's verdicts reach no moved body: its bodies stay on the walk until M7. */
    private static final class Moved implements PlanRunner.ProgramSink {
        private final TestDoor.Planned planned;
        private final SqlDialect dialect;
        private final RawSqlBoundary.@Nullable Recorder recorder;
        private final Execution.@Nullable Answer answer;
        /** The caller's sink, asked for before the program ran (a sent answer), or null (none, or a constant's). */
        private final @Nullable ResultSink early;
        private final boolean toOneReader;
        /** The body's asserts as the host judge planned them (an assert program's), or null. */
        private final @Nullable HostAsserts asserts;
        private final @Nullable AssertListener listener;
        /** The caller's sink holding the walk's contract, once the answer's columns arrive. */
        private @Nullable ResultSink opened;
        /** The last assert's verdict, once judged: a body ending on an assert answers it. */
        private @Nullable TypedSide lastVerdict;

        Moved(TestDoor.Planned planned, SqlDialect dialect, RawSqlBoundary.@Nullable Recorder recorder,
                Execution.@Nullable Answer answer, @Nullable ResultSink early, boolean toOneReader,
                @Nullable HostAsserts asserts, @Nullable AssertListener listener) {
            this.planned = planned;
            this.dialect = dialect;
            this.recorder = recorder;
            this.answer = answer;
            this.early = early;
            this.toOneReader = toOneReader;
            this.asserts = asserts;
            this.listener = listener;
        }

        TypedSide lastVerdict() {
            return java.util.Objects.requireNonNull(lastVerdict, "a body ending on an assert judged none");
        }

        @Override
        public void verdict(ExecutionPlan.Verdict verdict, @Nullable List<Object> row) {
            throw new IllegalStateException("a database verdict in a moved body: the database judge's bodies stay"
                    + " on the walk until M7");
        }

        @Override
        public void fused(ExecutionPlan.Verdicts group, @Nullable DataError rejected) {
            throw new IllegalStateException("a fused verdict group in a moved body: the database judge's bodies stay"
                    + " on the walk until M7");
        }

        /** The assert {@code verdict} planned, judged over the rows the runner read for it -- counted and reported
         *  as the walk's {@code AssertVerdicts.tryAdjudicate} does: the assert entered, a verdict reported, a
         *  failure (an assert that does not hold, a side the database refused) counted as raised and reported. */
        @Override
        public void sides(ExecutionPlan.Verdict verdict, List<PlanRunner.Rows> rows) {
            HostAsserts.Planned p = assertOf(verdict);
            AssertVerdicts.HostAssert a = p.assertion();
            TypedSide v = AssertVerdicts.judgedAtItsStep(a, listener,
                    () -> HostSides.shaped(a.name(), p.sides(), rows));
            if (v == null) {
                throw new com.legend.error.NotImplementedException(a.name() + " on the program road: the host"
                        + " judge declined the sides it read (the walk's generic path has no program form)");
            }
            lastVerdict = v;
        }

        /** A side of {@code verdict}'s that could not be read (the database refused it, or the read raised): the
         *  assert entered and counted, a data error reported, as the walk counts and reports an assert whose side
         *  raised. */
        @Override
        public RuntimeException sidesFailed(ExecutionPlan.Verdict verdict, RuntimeException failure) {
            return AssertVerdicts.sideRefused(assertOf(verdict).assertion(), listener, failure);
        }

        private HostAsserts.Planned assertOf(ExecutionPlan.Verdict verdict) {
            if (asserts == null) {
                throw new IllegalStateException("a verdict's sides in a moved body whose asserts no judge planned");
            }
            return asserts.of(verdict);
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
                            new Multiplicity.Bounded(m.lower(), upper).text(), false);
                }
                case RELATION -> JudgePlans.holding(sink, toOneReader, 0, "", toOneReader);
                case OBJECTS -> throw new IllegalStateException("a moved body answered objects: no moved class"
                        + " answers a graph");
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
        public void effectRan(int step) {
            for (TestDoor.Recorded r : effect(step).statements()) {
                Census.inc(Census.Key.EFFECTS_IN_SCRIPT);
                record(r.text(), r.query());
            }
        }

        @Override
        public DataError effectFailed(int step, DataError failure) {
            // what the database applied before the failing statement (H2 names it), or nothing when the script rolled
            // back as a unit (DuckDB): the walk's ledger rule and message (BodyDriver.sendScript)
            TestDoor.Effect effect = effect(step);
            List<TestDoor.Recorded> texts = effect.statements();
            texts.forEach(r -> Census.inc(Census.Key.EFFECTS_IN_SCRIPT));
            OptionalInt failing = dialect.failingStatement(String.valueOf(failure.getMessage()), executed(step));
            for (int i = 0; i < failing.orElse(0); i++) {
                record(texts.get(i).text(), texts.get(i).query());
            }
            return EffectSegment.failed(failure, effect.where(), failing, texts.size());
        }

        /** The statements effect step {@code step} sent, as executed (the dialect names a failing one by them). */
        private List<String> executed(int step) {
            ExecutionPlan.Node node = planned.plan().root() instanceof ExecutionPlan.Sequence s ? s.steps().get(step)
                    : planned.plan().root();
            if (!(node instanceof ExecutionPlan.Effect e) || e.script() == null) {
                throw new IllegalStateException("step " + step + " of a moved body failed as an effect, and is a "
                        + node.getClass().getSimpleName());
            }
            return e.script().statements();
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
