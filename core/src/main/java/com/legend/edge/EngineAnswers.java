// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.ArrowWriter;
import com.legend.result.Batch;
import com.legend.result.ResultSink;

import java.io.IOException;
import java.util.List;

/**
 * LEGEND-ENGINE'S ANSWERS OF A RUN, AS RECEIVERS (W8 of docs/records/d-plan-2026-11-16.md, D1.8, 2026-11-16): the
 * {@code pure/v1} execute answer ({@link PureV1Writer}) and its Arrow answer, written by the compatibility edge through
 * the run's {@code Output.Rows}, where the result library's own engine variants made them until then. The run tells the
 * receiver the answer's shape, its execution and the plan's stated answer before any statement runs
 * ({@link ResultSink#planned}), so a relation's envelope is made -- and a column with no builder entry, a value, a graph
 * fetch under Arrow refused -- before a write program changes anything, as before. A receiver no plan was told (a test
 * body's answer) refuses by name ({@link ResultSink#needsAPlan}).
 */
public final class EngineAnswers {

    private EngineAnswers() {
    }

    /** legend-engine's {@code pure/v1} execute answer onto {@code out}, its relation's envelope from {@code facts}. */
    public static ResultSink engine(java.io.Writer out, PureV1Writer.Facts facts) {
        return new Planned((plan, execution) -> switch (plan.answerShape()) {
            case OBJECTS -> PureV1Writer.objects(out);
            case RELATION -> {
                String sql = plan.answerSql().statement();
                List<ExecutionPlan.Column> stated = plan.answerColumns();
                // the envelope from the plan's stated columns BEFORE any statement runs, so a column with no builder
                // entry refuses before a write program changes anything; made from the announced columns only where
                // the plan states none (a dynamic pivot's: the phase 3 audit, 2026-10-27)
                if (stated == null) {
                    yield PureV1Writer.relation(out, columns -> PureV1Writer.envelope(facts, sql, columns), execution);
                }
                PureV1Writer.Envelope made = PureV1Writer.envelope(facts, sql, stated);
                yield PureV1Writer.relation(out, columns -> made, execution);
            }
            case VALUE -> throw new com.legend.error.NotImplementedException(facts.valueRefusal());
        }, "the engine's answer");
    }

    /** legend-engine's Arrow answer onto {@code out}: a relation under the envelope's metadata, a date's text in
     *  legend-pure's form ({@link Presenter#LEGEND_PURE}); a graph fetch and a value refused. */
    public static ResultSink arrow(java.io.OutputStream out, PureV1Writer.Facts facts) {
        return new Planned((plan, execution) -> switch (plan.answerShape()) {
            case OBJECTS -> throw new IllegalArgumentException("ARROW_IPC answers a relation query; a graph fetch "
                    + "under it is refused (upstream's answer unprobed)");
            case RELATION -> {
                String sql = plan.answerSql().statement();
                List<ExecutionPlan.Column> stated = plan.answerColumns();
                // the JSON route's rule (engine above); the metadata's activity names the execution (the door's id)
                java.util.UUID ran = PureV1Writer.ran(execution);
                if (stated == null) {
                    yield new ArrowWriter(out, columns -> PureV1Writer.arrowMetadata(facts, sql, columns, ran),
                            Presenter.LEGEND_PURE);
                }
                PureV1Writer.envelope(facts, sql, stated);
                yield new ArrowWriter(out, columns -> PureV1Writer.arrowMetadata(facts, sql, stated, ran),
                        Presenter.LEGEND_PURE);
            }
            case VALUE -> throw new com.legend.error.NotImplementedException(facts.valueRefusal());
        }, "the engine's Arrow answer");
    }

    /** A writer made from the plan's stated answer and the execution it is for. */
    @FunctionalInterface
    private interface Maker {
        ResultSink make(ExecutionPlan plan, java.util.@com.legend.base.Nullable UUID execution);
    }

    /** A receiver that makes its writer when the run tells it the plan, and passes every call on to it. */
    private static final class Planned implements ResultSink {
        private final Maker maker;
        private final String what;
        private java.util.@com.legend.base.Nullable UUID execution;
        private @com.legend.base.Nullable ResultSink writer;

        Planned(Maker maker, String what) {
            this.maker = maker;
            this.what = what;
        }

        @Override
        public boolean needsAPlan() {
            return true;
        }

        @Override
        public void execution(java.util.UUID id) {
            this.execution = id;
            if (writer != null) {
                writer.execution(id);   // passed on as every other call is (the P2 checkpoint audit's A-S3, 2026-11-16)
            }
        }

        @Override
        public void planned(ExecutionPlan plan) {
            writer = maker.make(plan, execution);
            if (execution != null) {
                writer.execution(execution);   // the writer is told the execution it is for, whichever came first (A-S3)
            }
            writer.planned(plan);
        }

        private ResultSink writer() {
            if (writer == null) {
                throw new IllegalArgumentException(what + " needs a plan's statement and columns: a test body's answer "
                        + "is written in lite's formats");
            }
            return writer;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) throws IOException {
            writer().columns(columns);
        }

        @Override
        public void batch(Batch batch) throws IOException {
            writer().batch(batch);
        }

        @Override
        public void end() throws IOException {
            writer().end();
        }
    }
}
