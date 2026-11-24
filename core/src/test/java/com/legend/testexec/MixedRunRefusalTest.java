// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testexec;


import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * EACH STYLE RUNS WITH ITS OWN HOOK. Until item 3's S2 (2026-11-21) a mixed run was refused by name (census C1, the
 * orchestrator's ruling of 2026-11-17: one choice per run -- the plan's style and the run's hook word a failure together,
 * never the rules); one catalog now words every assert failure, so the refusal had no subject left. The defect this closes: a lite run handed the edge's hook
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

    // (theProductsStyleHeardByTheEdgesHookIsRefused and theEdgesStyleHeardByTheProductsHookIsRefused: gone 2026-11-21,
    // item 3's S2 -- the refusal pinned census C1's "one choice per run: the plan's style and the run's hook word a failure
    // together, never the rules"; one catalog words every assert failure, so a run has no words of its own to match)

    @Test
    void eachStyleWithItsOwnHookRuns() throws Exception {
        run(ExecuteOptions.NONE, NoEdgeRun.NONE);
        run(ExecuteOptions.NONE.withStyle(com.legend.edge.PureText.UPSTREAM), com.legend.edge.AppealRun.with(null));
    }
}
