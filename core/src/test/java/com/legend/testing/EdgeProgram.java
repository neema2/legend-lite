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
        // the compatibility rules' runs carry the compatibility edge, which names its rules (census C1, the orchestrator's
        // ruling of 2026-11-17)
        return ExecuteOptions.recording(new com.legend.edge.CompatEdge(new com.legend.edge.RawLedger.Recorder(),
                com.legend.executionplan.DataStores.none()), com.legend.executionplan.DataStores.none())
                .withJudgeMode(mode).withStyle(com.legend.edge.PureText.UPSTREAM);
    }

    /** {@link #compat(com.legend.testplan.PlanningOptions.JudgeMode)} under the host judge, the only judge in
     *  compatibility (rule 4 of the compatibility program's rules of record; A4 of the compatibility program, stage A
     *  ratified by the user 2026-11-19: until A4 the lane's judge, and the edge's tests ran under each). */
    public static ExecuteOptions compat() {
        return compat(com.legend.testplan.PlanningOptions.JudgeMode.HOST);
    }

    /** {@code body} run on the edge's door: typed, then admitted by the edge's test-body door (upstream's reading of a
     *  body, {@code edge.UpstreamTestBodies}: B3 step 2, 2026-11-24), planned under the compatibility rules'
     *  {@code options} and heard by the edge's run. */
    public static Program.Report run(String model, String body, @com.legend.base.Nullable String runtime,
            java.sql.Connection c, ExecuteOptions options) {
        return Program.run(model, body, runtime, c, upstream(options), com.legend.edge.AppealRun.with(null),
                com.legend.edge.UpstreamTestBodies::admitted);
    }

    /** {@code options} if they plan in upstream's style, else refused by name: a body the edge's door admitted is
     *  upstream's reading, never planned by the product's rules (the review of B3 step 2's M1, 2026-11-24). */
    private static ExecuteOptions upstream(ExecuteOptions options) {
        if (options.planning().style() != com.legend.edge.PureText.UPSTREAM) {
            throw new IllegalArgumentException("the edge's door admits a body for upstream's reading; its options plan"
                    + " in " + options.planning().style() + ", not upstream's style (EdgeProgram.compat or upstream)");
        }
        return options;
    }

    /** The last statement's side of {@code body} run on the edge's door with {@code options} (the edge's style and its
     *  run hook together: census C1, the orchestrator's ruling of 2026-11-17, a mixed run is refused by name). */
    public static Lean.Result last(String model, String body, @com.legend.base.Nullable String runtime,
            java.sql.Connection c, ExecuteOptions options) {
        return run(model, body, runtime, c, options).side();
    }

    /** {@code body} run on the edge's door with a session of its own per use ({@link Program#runFresh}). */
    public static Program.Report runFresh(String model, String body, @com.legend.base.Nullable String runtime,
            ExecuteOptions options) {
        return Program.runFresh(model, body, runtime, upstream(options), com.legend.edge.AppealRun.with(null),
                com.legend.edge.UpstreamTestBodies::admitted);
    }

    /** {@link #run} under {@link #compat()}. */
    public static Program.Report run(String model, String body, @com.legend.base.Nullable String runtime,
            java.sql.Connection c) {
        return run(model, body, runtime, c, compat());
    }
}
