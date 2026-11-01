// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.Execution;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.error.DataError;
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
    public static void execute(TypedQuery typed, @Nullable String runtimeFqn, SqlDialect dialect,
            Sessions.Session session, @Nullable AssertListener listener, @Nullable SqlReplayReferee referee,
            ExecuteOptions options, Execution.@Nullable Answer answer) {
        BodyClass moved = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn);
        if (moved == null || runtimeFqn == null) {
            BodyDriver.execute(typed, runtimeFqn, dialect, session, listener, referee, options, answer);
            return;
        }
        TestDoor.Planned planned = TestDoor.planned(typed, runtimeFqn, options.features(), session.version());
        try {
            PlanRunner.program(planned.plan(), Map.of(), PlanSessions.of(session), PlanRunner.Judging.HOST,
                    new Moved(planned, dialect, options.recorder(), answer));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A moved body's sink: its answer to the caller's, by the shape of the plan's answer; its effect and seed
     *  statements to the raw-SQL ledger as each runs, the texts the walk records (the referee mirrors the ledger to
     *  replay the golden SQL of later bodies). No verdict: no moved class holds an assert yet. */
    private static final class Moved implements PlanRunner.ProgramSink {
        private final TestDoor.Planned planned;
        private final SqlDialect dialect;
        private final RawSqlBoundary.@Nullable Recorder recorder;
        private final Execution.@Nullable Answer answer;
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
                opened = answer.of(answerShape(planned.plan()));
                opened.columns(columns);
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
            List<TestDoor.Recorded> texts = recorded(step);
            for (int i = 0; i < texts.size(); i++) {
                record(texts.get(i).text(), answeredRows.get(i));
            }
        }

        @Override
        public void effectRan(int step) {
            for (TestDoor.Recorded r : recorded(step)) {
                record(r.text(), r.query());
            }
        }

        @Override
        public void effectFailed(int step, DataError failure) {
            // what the database applied before the failing statement (H2 names it), or nothing when the script rolled
            // back as a unit (DuckDB): the walk's ledger rule (BodyDriver.sendScript)
            List<String> statements = effectStatements(step);
            int applied = dialect.failingStatement(String.valueOf(failure.getMessage()), statements).orElse(0);
            List<TestDoor.Recorded> texts = recorded(step);
            for (int i = 0; i < applied; i++) {
                record(texts.get(i).text(), texts.get(i).query());
            }
        }

        private List<TestDoor.Recorded> recorded(int step) {
            List<TestDoor.Recorded> texts = planned.recorded().get(step);
            if (texts == null) {
                throw new IllegalStateException("step " + step + " of a moved body ran with no ledger texts beside it");
            }
            return texts;
        }

        private List<String> effectStatements(int step) {
            ExecutionPlan.Node node = steps(planned.plan()).get(step);
            if (!(node instanceof ExecutionPlan.Effect e) || e.script() == null) {
                throw new IllegalStateException("step " + step + " of a moved body failed as an effect, and is a "
                        + node.getClass().getSimpleName());
            }
            return e.script().statements();
        }

        private void record(String text, boolean query) {
            if (recorder != null) {
                recorder.recordExecuted(text, query);
            }
        }
    }

    private static List<ExecutionPlan.Node> steps(ExecutionPlan plan) {
        return plan.root() instanceof ExecutionPlan.Sequence s ? s.steps() : List.of(plan.root());
    }

    /** The shape of the plan's answer: its answering step's (a constant answers a value). */
    private static ExecutionPlan.TdsResult.Shape answerShape(ExecutionPlan plan) {
        ExecutionPlan.TdsResult.Shape shape = null;
        for (ExecutionPlan.Node step : steps(plan)) {
            switch (step) {
                case ExecutionPlan.TdsResult t -> shape = t.shape();
                case ExecutionPlan.Keyed k -> shape = k.shape();
                case ExecutionPlan.Constant c -> shape = ExecutionPlan.TdsResult.Shape.VALUE;
                default -> {
                    // a step that answers nothing
                }
            }
        }
        if (shape == null) {
            throw new IllegalStateException("a moved body's answer streamed with no answering step in its plan");
        }
        return shape;
    }
}
