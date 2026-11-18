// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testexec;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * A MIXED RUN IS REFUSED BY NAME (census C1, the orchestrator's ruling of 2026-11-17: one choice per run -- the plan's
 * style and the run's hook word a failure together, never the rules): each mismatch planted, the product's style heard
 * by the edge's hook and the edge's style by the product's. The defect this closes: a lite run handed the edge's hook
 * was judged by compiled legend-pure's equality and words on the host while its database judge wrote lite's.
 */
class MixedRunRefusalTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";
    private static final String BODY = "|assertEquals(1, 1)";

    private static void run(ExecuteOptions options, EdgeRun hook) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            var ctx = Compiler.compileModel(StorelessRuntime.with(MODEL, Databases.PLATFORM));
            TestExecution.runProgram(Compiler.query(ctx, BODY), StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null, hook,
                    options);
        }
    }

    @Test
    void theProductsStyleHeardByTheEdgesHookIsRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run(ExecuteOptions.NONE, com.legend.edge.AppealRun.with(null)));
        assertTrue(e.getMessage().contains("the product's style") && e.getMessage().contains("go together"),
                e.getMessage());
    }

    @Test
    void theEdgesStyleHeardByTheProductsHookIsRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run(ExecuteOptions.NONE.withStyle(com.legend.edge.PureText.UPSTREAM), NoEdgeRun.NONE));
        assertTrue(e.getMessage().contains("the compatibility edge's style") && e.getMessage().contains("go together"),
                e.getMessage());
    }

    @Test
    void eachStyleWithItsOwnHookRuns() throws Exception {
        run(ExecuteOptions.NONE, NoEdgeRun.NONE);
        run(ExecuteOptions.NONE.withStyle(com.legend.edge.PureText.UPSTREAM), com.legend.edge.AppealRun.with(null));
    }
}
