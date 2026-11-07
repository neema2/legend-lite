// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.type.ExprType;
import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;

/**
 * Channel A through THE PRODUCT DOOR (2026-11-03, the user's order of fixes, step 6: one planning door, one execution
 * door): the case's expression and its injected model compiled by the one compiler and run by {@code Execution.run},
 * exactly as a product query runs -- its session the lane's for its plan ({@link PctBackend#executionSource}: as the
 * plan route's sessions chose it), its plan made for that session's server, its declared setup established, under the
 * PCT's feature flag
 * (CORRECT_SQL_SUBSTRING_INDEXING: the user's ruling of 2026-09-12, a written deviation from upstream's PCT adapter,
 * which sets none -- the engine's testable runner sets it for relation tests). Its answer is the plan's TYPED rows, and, for a relation, the engine's JSON
 * answer the server writes from them ({@code PureV1Api.engineWriter}), which the adapter reads as legend-engine's own
 * PCT adapter reads it ({@link ValueBridge#engineTDS}); a value's elements cross as the reference's own instances
 * ({@link ValueBridge}).
 */
final class PctPlanRoute {

    private PctPlanRoute() {
    }

    /** A case's answer: what the plan answers ({@code shape}), its typed rows, the query's result type, which a
     *  value's elements are built by, and for a relation the engine's JSON answer the server writes ({@code
     *  engineJson}; null for a value). */
    record Answer(ExecutionPlan.TdsResult.Shape shape, PlanRunner.Rows rows, ExprType resultType,
            @com.legend.base.Nullable String engineJson) {
    }

    static Answer execute(String model, String expression) throws IOException {
        // the tests' planning options: as-of joins answer the same row on every database (PlanOption, off by default;
        // the user, 2026-11-07: "our tests should use the option")
        TypedQuery query = Compiler.query(Compiler.compileModel(PctBackend.withStorelessRuntime(model)), expression)
                .withOptions(java.util.Set.of(com.legend.platform.PlanOption.DETERMINISTIC_AS_OF));
        com.legend.result.Collector collector = new com.legend.result.Collector();
        java.io.StringWriter json = new java.io.StringWriter();
        boolean[] relation = new boolean[1];
        java.util.Set<com.legend.platform.Feature> features =
                java.util.Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING);
        // the lane's session for the query, as its sessions choose it for the query's plan (PctBackend.executionSource):
        // a lane holding a session (a cloud's) asks the plan's answering target first
        ExecutionPlan plan = com.legend.Execution.run(query, com.legend.test.StorelessRuntime.RUNTIME,
                PctBackend.executionSource(() -> {
                    ExecutionPlan planned = query.executionPlan(com.legend.test.StorelessRuntime.RUNTIME, features);
                    return planned.sendsTheAnswer() ? planned.answerSql().target() : null;
                }),
                PctBackend.planSessions(), com.legend.Execution.Establish.DECLARED_SETUP, features, p -> {
                    if (p.answerShape() != ExecutionPlan.TdsResult.Shape.RELATION) {
                        return collector;
                    }
                    // a relation crosses as legend-engine's PCT adapter reads one (the result boundary program's
                    // stage 1, step 3): the server's own writer of the engine's answer, fed the stream the rows
                    // are collected from
                    relation[0] = true;
                    com.legend.result.ResultSink engine = com.legend.server.PureV1Api.engineWriter(query, p, json);
                    return new com.legend.result.ResultSink() {
                        @Override
                        public void columns(java.util.List<ExecutionPlan.Column> columns) throws IOException {
                            collector.columns(columns);
                            engine.columns(columns);
                        }

                        @Override
                        public void batch(com.legend.result.Batch batch) throws IOException {
                            collector.batch(batch);
                            engine.batch(batch);
                        }

                        @Override
                        public void end() throws IOException {
                            collector.end();
                            engine.end();
                        }
                    };
                }, com.legend.result.Batcher.DEFAULT_CAPACITY);
        com.legend.result.Collector.Collected c = collector.collected();
        return new Answer(plan.answerShape(), new PlanRunner.Rows(c.columns(), c.rows()), query.resultType(),
                relation[0] ? json.toString() : null);
    }
}
