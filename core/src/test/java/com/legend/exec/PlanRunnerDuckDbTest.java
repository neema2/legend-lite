// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.legend.exec.PlanRunnerTest.inMemory;
import static com.legend.exec.PlanRunnerTest.kRows;
import static com.legend.exec.PlanRunnerTest.plan;
import static com.legend.exec.PlanRunnerTest.run;
import static com.legend.exec.PlanRunnerTest.statement;
import static com.legend.exec.PlanRunnerTest.target;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The runner on DuckDB ({@link PlanRunnerTest}'s case that is DuckDB's own; moved here from it by the dialect program's
 * Phase 6, 2026-10-16): rows set up through its bulk loader, the Appender.
 */
class PlanRunnerDuckDbTest {

    /** Two targets are one database only when their content is equal, cell for cell: a cell holding a comma and a
     *  null cell are spelled apart from their look-alikes (the audit's S4: a key of the targets' printed text gave
     *  {@code ["a, b", "c"]} and {@code ["a", "b, c"]} one database). */
    @Test
    void aTargetIsKeyedByItsWholeContent_soLookAlikeCellsNeverShareADatabase() throws Exception {
        var duck = new ExecutionPlan.Database.Declared(inMemory("s::Duck", DatabaseType.DuckDB,
                new ConnectionSpecification.InMemory()));
        String read = "SELECT string_agg(COALESCE(A, '<none>') || '/' || COALESCE(B, '<none>'), ';') FROM K";
        Map<List<String>, String> rows = new java.util.LinkedHashMap<>();
        rows.put(List.of("a, b", "c"), "a, b/c");
        rows.put(List.of("a", "b, c"), "a/b, c");
        rows.put(Arrays.asList(null, "x"), "<none>/x");
        rows.put(List.of("null", "x"), "null/x");
        for (var e : rows.entrySet()) {
            ExecutionPlan p = plan(read, target(duck, new ExecutionPlan.Servers.Every(),
                    statement("CREATE TABLE K (A VARCHAR, B VARCHAR)"), kRows(e.getKey())));
            assertEquals(e.getValue(), run(p, PlanSessions.shared()), String.valueOf(e.getKey()));
        }
    }
}
