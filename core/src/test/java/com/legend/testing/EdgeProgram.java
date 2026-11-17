// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.ExecuteOptions;

/**
 * THE EDGE'S TEST HARNESS (Phase 7's build half, 2026-11-17): what an edge test runs a body in. The product's harness
 * ({@link Program}) names no edge class, so a product test cannot reach the edge; an edge test, in the edge's test
 * library, reaches it here.
 */
public final class EdgeProgram {

    private EdgeProgram() {
    }

    /** The platform's default options in the compatibility edge's plan style ({@code edge.PureText.UPSTREAM}): a body
     *  whose answer is legend-pure's or legend-engine's exact text -- the engine-text natives ({@code toSQLString},
     *  {@code planToString}, the DDL generators), an activity's SQL -- which the product's style refuses by name
     *  (phase D's O3, 2026-11-13: legend-engine's text is the edge's). */
    public static ExecuteOptions upstream() {
        return Program.defaults().withStyle(com.legend.edge.PureText.UPSTREAM);
    }

    /** The compatibility rules' options under {@code mode} (census C1's K14, the orchestrator's ruling of 2026-11-17: a
     *  test of upstream's behaviour is an edge test, never a compatibility run on the product's door): upstream's
     *  leniencies and the edge's plan style. */
    public static ExecuteOptions compat(com.legend.testplan.PlanningOptions.JudgeMode mode) {
        return ExecuteOptions.NONE.withJudgeMode(mode).withEngineCompat(true)
                .withStyle(com.legend.edge.PureText.UPSTREAM);
    }

    /** {@link #compat(com.legend.testplan.PlanningOptions.JudgeMode)} under the lane's judge. */
    public static ExecuteOptions compat() {
        return compat(Program.laneJudge());
    }

    /** {@code body} run on the edge's door: the compatibility rules' {@code options}, heard by the edge's run. */
    public static Program.Report run(String model, String body, @com.legend.base.Nullable String runtime,
            java.sql.Connection c, ExecuteOptions options) {
        return Program.run(model, body, runtime, c, options, com.legend.edge.AppealRun.with(null));
    }

    /** {@link #run} under {@link #compat()}. */
    public static Program.Report run(String model, String body, @com.legend.base.Nullable String runtime,
            java.sql.Connection c) {
        return run(model, body, runtime, c, compat());
    }
}
