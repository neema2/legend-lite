// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.Execution;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.error.DataError;
import com.legend.exec.Census;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.SqlReplayReferee;
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
        if (runtimeFqn == null || BodyClass.of(typed, runtimeFqn) == null) {
            BodyDriver.execute(typed, runtimeFqn, dialect, session, listener, referee, options, answer);
            return;
        }
        TestDoor.Planned planned = TestDoor.planned(typed, runtimeFqn, options.features(), session.version());
        Census.inc(Census.Key.BODIES_MOVED);
        try {
            PlanRunner.program(planned.plan(), Map.of(), PlanSessions.of(session), PlanRunner.Judging.HOST,
                    new Moved(planned, dialect, options.recorder(), answer));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A moved body's sink: its answer to the caller's, by the shape of the plan's answer; its effect and seed
     *  statements to the raw-SQL ledger as each runs, the texts the walk records (the referee mirrors the ledger to
     *  replay the golden SQL of later bodies), and an effect's failure named as the walk names it. No verdict: no
     *  moved class holds an assert yet. */
    private static final class Moved implements PlanRunner.ProgramSink {
        private final TestDoor.Planned planned;
        private final SqlDialect dialect;
        private final RawSqlBoundary.@Nullable Recorder recorder;
        private final Execution.@Nullable Answer answer;
        /** The caller's sink, opened when the answer's columns arrive (as the walk opens it: only to answer). */
        private @Nullable ResultSink opened;

        Moved(TestDoor.Planned planned, SqlDialect dialect, RawSqlBoundary.@Nullable Recorder recorder,
                Execution.@Nullable Answer answer) {
            this.planned = planned;
            this.dialect = dialect;
            this.recorder = recorder;
            this.answer = answer;
        }

        @Override
        public void verdict(ExecutionPlan.Verdict verdict, @Nullable List<Object> row) {
            throw new IllegalStateException("a verdict in a moved body: no moved class holds an assert");
        }

        @Override
        public void fused(ExecutionPlan.Verdicts group, @Nullable DataError rejected) {
            throw new IllegalStateException("a verdict group in a moved body: no moved class holds an assert");
        }

        @Override
        public void sides(ExecutionPlan.Verdict verdict, List<PlanRunner.Rows> sides) {
            throw new IllegalStateException("a verdict's sides in a moved body: no moved class holds an assert");
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) throws IOException {
            if (answer != null) {
                ResultSink sink = answer.of(planned.plan().answerShape());
                opened = sink;
                sink.columns(columns);
            }
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
