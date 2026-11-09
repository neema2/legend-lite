// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.exec.PlanSessions;
import com.legend.exec.ProductRun;
import com.legend.exec.SessionSource;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batcher;
import com.legend.result.ResultSink;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

/**
 * THE PRODUCT'S DOOR (C2a, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md; thin since the plan-as-data
 * design's slice 5, 2026-11-09): a query planned by its one planning entry ({@code TypedQuery.executionPlan}: the
 * planner never touches a database) and the plan run by exec's product run ({@link ProductRun}: the plan alone says
 * where it runs). Each method is one statement: its run call, its planning calls the run's argument (a query's
 * text compiled, then planned), or a call of another of them -- no logic, no state
 * ({@code ArchitectureTest.theDoorsOnlyCompose}, {@code JavaEvalLedgerTest.theDoorsHoldNoLogic}). Its own library ({@code //core:execution}); a test body's door
 * is {@link TestExecution}, its own.
 */
public final class Execution {

    private Execution() {
    }

    /**
     * THE PRODUCTION EXECUTION (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state recorded 2026-10-27): a query
     * typed once, planned as typed rows, run by the plan's runner, its rows streamed in batches to the sink the caller
     * makes from the plan that runs ({@code sinkOf}, called before any statement runs). A plan that reads runs on the
     * session {@code sessions} opens for its answer's target, established as {@code establish} says; a program that
     * writes runs on {@code planSessions} (decision A). Returns the plan that ran.
     */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Function<ExecutionPlan, ResultSink> sinkOf) throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn), sessions, planSessions, establish, sinkOf,
                Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Function)} with the
     *  rows handed to the sink in batches of up to {@code batchRows} (a writer writes and flushes each as it arrives). */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Function<ExecutionPlan, ResultSink> sinkOf, int batchRows) throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn), sessions, planSessions, establish, sinkOf, batchRows);
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Function, int)} under
     *  the query's FEATURE FLAGS: upstream's execution context carries them per query (its
     *  {@code ExecutionOptionContext}'s {@code FeatureFlagOption}), and the lowering reads them (2026-11-03: the door's
     *  own parameter, so a caller that sets a flag -- the PCT, the user's ruling of 2026-09-12 -- runs through it). */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Set<com.legend.platform.Feature> features, Function<ExecutionPlan, ResultSink> sinkOf, int batchRows)
            throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn, features), sessions, planSessions, establish, sinkOf,
                batchRows);
    }

    /** A query run as legend-lite's server runs it (legend-engine's {@code pure/v1/execution/execute}): planned, then
     *  run on its project's databases ({@link ProductRun#onProjectDatabases}). pure/v1's execute reaches it through the
     *  runner the server hands the API (slice 5, 2026-11-09: pure/v1 compiles, the door plans and runs -- the API's own
     *  plan-then-run is gone). */
    public static ExecutionPlan runOnProjectDatabases(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, Function<ExecutionPlan, ResultSink> sinkOf) throws IOException {
        return ProductRun.onProjectDatabases(typed.projectPlan(runtimeFqn), sinkOf);
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Function)} for a
     *  model's text and a query's text, the session as the caller set it up. */
    public static ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, SessionSource sessions,
            PlanSessions.Source planSessions, Function<ExecutionPlan, ResultSink> sinkOf) throws IOException {
        return ProductRun.run(Compiler.query(Compiler.compileModel(model), query).executionPlan(runtimeFqn), sessions,
                planSessions, ProductRun.Establish.AS_GIVEN, sinkOf, Batcher.DEFAULT_CAPACITY);
    }

    /** {@link #run(String, String, String, SessionSource, PlanSessions.Source, Function)} in batches of up to
     *  {@code batchRows}. */
    public static ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, SessionSource sessions,
            PlanSessions.Source planSessions, Function<ExecutionPlan, ResultSink> sinkOf, int batchRows)
            throws IOException {
        return ProductRun.run(Compiler.query(Compiler.compileModel(model), query).executionPlan(runtimeFqn), sessions,
                planSessions, ProductRun.Establish.AS_GIVEN, sinkOf, batchRows);
    }
}
