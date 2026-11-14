// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.exec.PlanSessions;
import com.legend.exec.ProductRun;
import com.legend.exec.SessionSource;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batcher;
import com.legend.result.Output;

import java.io.IOException;
import java.util.Set;

/**
 * THE PRODUCT'S DOOR (C2a, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md; thin since the plan-as-data
 * design's slice 5, 2026-11-09): a query planned by its one planning entry ({@code TypedQuery.executionPlan}: the
 * planner never touches a database) and the plan run by exec's product run ({@link ProductRun}: the plan alone says
 * where it runs). Each method is one statement: its run call, its planning calls the run's argument (a query's
 * text compiled, then planned), or a call of another of them -- no logic, no state
 * ({@code ArchitectureTest.theDoorsOnlyCompose}, {@code JavaEvalLedgerTest.theDoorsHoldNoLogic}). Its own library ({@code //core:execution}); a test body's door
 * is {@link TestExecution}, its own.
 *
 * <p>EACH CALL IS ONE EXECUTION (a query run; the user's ruling of 2026-11-12, "Do the exec id as part
 * of C"): the door mints its id ({@code java.util.UUID.randomUUID}) and hands it to exec, which binds it to the
 * statements' stamps and tells it to the listener, and makes no value of its own
 * ({@code ArchitectureTest.execMakesNoValueOfItsOwn}). No caller supplies an id.
 */
public final class Execution {

    private Execution() {
    }

    /**
     * THE PRODUCTION EXECUTION (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state recorded 2026-10-27): a query
     * typed once, planned as typed rows, run by the plan's runner, its rows streamed in batches in the caller's
     * {@code output} (a format and a stream: the result library makes its writer from the plan before any statement
     * runs). A plan that reads runs on the
     * session {@code sessions} opens for its answer's target, established as {@code establish} says; a program that
     * writes runs on {@code planSessions} (decision A). Returns the plan that ran.
     */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Output output) throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn), sessions, planSessions, establish, output,
                Batcher.DEFAULT_CAPACITY, java.util.UUID.randomUUID());
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Output)} with the
     *  rows handed to the sink in batches of up to {@code batchRows} (a writer writes and flushes each as it arrives). */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Output output, int batchRows) throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn), sessions, planSessions, establish, output, batchRows,
                java.util.UUID.randomUUID());
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Output, int)} under
     *  the query's FEATURE FLAGS: upstream's execution context carries them per query (its
     *  {@code ExecutionOptionContext}'s {@code FeatureFlagOption}), and the lowering reads them (2026-11-03: the door's
     *  own parameter, so a caller that sets a flag -- the PCT, the user's ruling of 2026-09-12 -- runs through it). */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Set<com.legend.platform.Feature> features, Output output, int batchRows)
            throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn, features), sessions, planSessions, establish, output,
                batchRows, java.util.UUID.randomUUID());
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Set, Output, int)}
     *  with its statements written in {@code dialects}' dialects: THE SPELLING HOOK (phase D's D1.0), the compatibility
     *  edge's ({@code com.legend.edge.PureText}) for the harnesses that compare against upstream's texts (PCT's
     *  channel A). */
    public static ExecutionPlan run(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SessionSource sessions, PlanSessions.Source planSessions, ProductRun.Establish establish,
            Set<com.legend.platform.Feature> features, com.legend.database.PlanStyle style, Output output,
            int batchRows) throws IOException {
        return ProductRun.run(typed.executionPlan(runtimeFqn, features, style), sessions, planSessions, establish,
                output, batchRows, java.util.UUID.randomUUID());
    }

    /** {@link #runOnProjectDatabases(TypedQuery, String, String, Output)} with its statements written in
     *  {@code dialects}' dialects (the spelling hook, D1.0: pure/v1's upstream routes, at the edge). */
    public static ExecutionPlan runOnProjectDatabases(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, @com.legend.base.Nullable String admittedSources,
            com.legend.database.PlanStyle style, Output output) throws IOException {
        return ProductRun.onProjectDatabases(typed.projectPlan(runtimeFqn, style), admittedSources, output,
                java.util.UUID.randomUUID());
    }

    /** A query run as legend-lite's server runs it (legend-engine's {@code pure/v1/execution/execute}): planned, then
     *  run on its project's databases ({@link ProductRun#onProjectDatabases}). pure/v1's execute reaches it through the
     *  runner the server hands the API (slice 5, 2026-11-09: pure/v1 compiles, the door plans and runs -- the API's own
     *  plan-then-run is gone). */
    public static ExecutionPlan runOnProjectDatabases(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, Output output) throws IOException {
        return ProductRun.onProjectDatabases(typed.projectPlan(runtimeFqn), output, java.util.UUID.randomUUID());
    }

    /** {@link #runOnProjectDatabases(TypedQuery, String, Output)}, its live sources read under the fence the host's
     *  configuration names, {@code admittedSources} ({@code SourceAdmission.configured}'s text: null, any file; the A
     *  checkpoint's tenets audit, 11). */
    public static ExecutionPlan runOnProjectDatabases(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn, @com.legend.base.Nullable String admittedSources,
            Output output) throws IOException {
        return ProductRun.onProjectDatabases(typed.projectPlan(runtimeFqn), admittedSources, output,
                java.util.UUID.randomUUID());
    }

    /** {@link #run(TypedQuery, String, SessionSource, PlanSessions.Source, ProductRun.Establish, Output)} for a
     *  model's text and a query's text, the target's loads established on the session it acquires. */
    public static ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, SessionSource sessions,
            PlanSessions.Source planSessions, Output output) throws IOException {
        return ProductRun.run(Compiler.query(Compiler.compileModel(model), query).executionPlan(runtimeFqn), sessions,
                planSessions, ProductRun.Establish.DECLARED_SETUP, output, Batcher.DEFAULT_CAPACITY,
                java.util.UUID.randomUUID());
    }

    /** {@link #run(String, String, String, SessionSource, PlanSessions.Source, Output)} in batches of up to
     *  {@code batchRows}. */
    public static ExecutionPlan run(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, SessionSource sessions,
            PlanSessions.Source planSessions, Output output, int batchRows)
            throws IOException {
        return ProductRun.run(Compiler.query(Compiler.compileModel(model), query).executionPlan(runtimeFqn), sessions,
                planSessions, ProductRun.Establish.DECLARED_SETUP, output, batchRows, java.util.UUID.randomUUID());
    }
}
