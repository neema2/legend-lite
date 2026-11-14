// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P7 OF docs/DATA_IN_THE_PLAN_2026_11_09.md 6, EVERY RUNNER READS THE SAME PLAN (2026-11-09): the host's plan runner
 * (engine-client's {@code plan-runner.ts}, the browser tab's and DataCube's) reads exactly the plan version the Java
 * planner writes ({@code PlanJson.VERSION}): a version bump on one side alone fails here, never at a user's first plan.
 */
@Tag("census")
class PlanVersionsAgreeTest {

    @Test
    void theHostRunnerReadsThePlannersVersion() throws Exception {
        List<Path> files = SourceFiles.under("engine-client/src");
        Path runner = files.stream().filter(f -> f.toString().endsWith("plan-runner.ts")).findFirst()
                .orElseThrow(() -> new AssertionError("engine-client/src/plan-runner.ts is not among the census's"
                        + " sources: " + files));
        String text = Files.readString(runner, StandardCharsets.UTF_8);
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?m)^const PLAN_VERSION = (\\d+);$").matcher(text);
        assertTrue(m.find(), "plan-runner.ts declares its plan version");
        assertEquals(com.legend.executionplan.PlanJson.VERSION, Integer.parseInt(m.group(1)),
                "the host's plan runner reads another plan version than the planner writes");
        assertTrue(!m.find(), "plan-runner.ts declares its plan version once");
    }
}
