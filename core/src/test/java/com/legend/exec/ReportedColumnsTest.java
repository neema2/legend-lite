// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestSessions;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A relation of NO declared column (the planner-driver seam program's S3c, 2026-10-30): a raw grid's plan, whose
 * columns the program never named. The runner reads the columns its result reports from the real run -- each named,
 * typed by the wire table, slotted by its SQL type -- hands them to the sink before any row, and reads each cell by
 * them; nothing was read at planning.
 */
final class ReportedColumnsTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void theColumnsAreTheRealRunsAndTheCellsTyped(TestBackend backend) throws Exception {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(backend.type()),
                new ExecutionPlan.Servers.Every(), List.of(), List.of());
        ExecutionPlan plan = new ExecutionPlan(List.of(), new ExecutionPlan.TdsResult(
                ExecutionPlan.TdsResult.Shape.RELATION, List.of(),
                ExecutionPlan.Sql.of("SELECT CAST(7 AS BIGINT) AS N, CAST('a' AS VARCHAR(10)) AS S", target)));
        List<ExecutionPlan.Column> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        try (Connection c = backend.open()) {
            PlanRunner.stream(plan, Map.of(), PlanSessions.setUp(TestSessions.over(c), target), new ResultSink() {
                @Override
                public void columns(List<ExecutionPlan.Column> reported) {
                    columns.addAll(reported);
                }

                @Override
                public void batch(Batch batch) {
                    for (int r = 0; r < batch.size(); r++) {
                        List<Object> row = new ArrayList<>();
                        for (int k = 0; k < columns.size(); k++) {
                            row.add(batch.get(r, k));
                        }
                        rows.add(row);
                    }
                }

                @Override
                public void end() {
                }
            });
        }
        assertEquals(List.of("N", "S"), columns.stream().map(ExecutionPlan.Column::name).map(String::toUpperCase)
                .toList());
        assertEquals(List.of("Integer", "String"), columns.stream().map(ExecutionPlan.Column::type).toList());
        assertNull(columns.get(0).store(), "a reported column is no store's");
        assertEquals(List.of(List.of(7L, "a")), rows);
    }
}
