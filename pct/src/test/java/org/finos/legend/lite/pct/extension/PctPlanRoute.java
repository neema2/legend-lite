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
 * answer the server writes from them ({@code Output.Engine}, after the run, from the rows it collected), which the adapter reads as legend-engine's own
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
        TypedQuery query = Compiler.query(Compiler.compileModel(PctBackend.withStorelessRuntime(model)), expression);
        com.legend.result.Collector collector = new com.legend.result.Collector();
        java.io.StringWriter json = new java.io.StringWriter();
        java.util.Set<com.legend.platform.Feature> features =
                java.util.Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING);
        // the lane's session for the query, as its sessions choose it for the query's plan (PctBackend.executionSource):
        // a lane holding a session (a cloud's) asks the plan's answering target first
        ExecutionPlan plan = com.legend.Execution.run(query, com.legend.testbackend.StorelessRuntime.RUNTIME,
                // the session for the target the plan answers on, handed to the source by the run (2026-11-09, the full
                // audit's must-fix: a supplier here planned the query again inside the run)
                PctBackend.executionSource(),
                PctBackend.planSessions(), com.legend.exec.ProductRun.Establish.DECLARED_SETUP, features,
                new com.legend.result.Output.Rows(collector), com.legend.result.Batcher.DEFAULT_CAPACITY);
        com.legend.result.Collector.Collected c = collector.collected();
        boolean relation = plan.answerShape() == ExecutionPlan.TdsResult.Shape.RELATION;
        if (relation) {
            // a relation crosses as legend-engine's PCT adapter reads one (the result boundary program's stage 1, step
            // 3): the server's own writer of the engine's answer, written from the rows the run collected, after it
            // (2026-11-09: a tee the run called back until then)
            // made after the run for the execution that ran the SQL, its door's id as the run named it to the collector
            com.legend.result.ResultSink engine = new com.legend.result.Output.Engine(json,
                    com.legend.server.PureV1Api.facts(query)).writerFor(plan, collector.execution());
            com.legend.result.Batcher batcher = new com.legend.result.Batcher(c.columns(), engine,
                    com.legend.result.Batcher.DEFAULT_CAPACITY);
            batcher.start();
            for (java.util.List<Object> row : c.rows()) {
                batcher.row(row);
            }
            batcher.end();
        }
        return new Answer(plan.answerShape(), new PlanRunner.Rows(c.columns(), c.rows()), query.resultType(),
                relation ? json.toString() : null);
    }
}
