// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE HOLDER HANDS ON WHAT THE RUN NAMES (B3's check of 2026-11-12, the exec-id item): the body router's holder of the
 * walk's cardinality ({@link SideShapes#holding}) passes the answer's shape and the execution's id -- its door's -- to
 * the sink it holds, as it passes the columns, rows and end; until then the id stopped at it.
 */
class SideShapesHoldingTest {

    @Test
    void theShapeAndTheExecutionReachTheSinkItHolds() throws Exception {
        List<Object> told = new ArrayList<>();
        ResultSink receiver = new ResultSink() {
            @Override
            public void answers(ExecutionPlan.TdsResult.Shape shape) {
                told.add(shape);
            }

            @Override
            public void execution(UUID id) {
                told.add(id);
            }

            @Override
            public void columns(List<ExecutionPlan.Column> columns) {
                told.add("columns");
            }

            @Override
            public void batch(Batch batch) {
                told.add("batch");
            }

            @Override
            public void end() {
                told.add("end");
            }
        };
        UUID id = UUID.fromString("00000000-0000-4000-8000-000000000003");
        ResultSink held = SideShapes.holding(receiver, true, 0, "[0..1]");
        held.answers(ExecutionPlan.TdsResult.Shape.VALUE);
        held.execution(id);
        held.columns(List.of());
        held.end();
        assertEquals(List.of(ExecutionPlan.TdsResult.Shape.VALUE, id, "columns", "end"), told);
    }
}
