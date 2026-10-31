// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * The PLANNING half of a body's environment (the planner-driver seam program's S2g, 2026-10-31,
 * docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md §3.3): what a statement is planned against and nothing it runs on --
 * the compiled model, the driver runtime, the dialect the plan is written in, the query's lets and the handles' rows
 * the resolver reads, the frame's table renames and bound context, the run's feature flags, the body's one site-id
 * minter (the program's, B9: every statement and both sides of every verdict share it). No session, no trace, no
 * verdict batch: those are the driver's half ({@code com.legend.driver.BodyDriver.ExecEnv}), which composes this one. Until S2g both halves were one record of
 * the driver's, so no planning helper could leave the driver.
 *
 * <p>{@code queryLets} and {@code planRows} are the body's run-scoped accumulators (the inliner's consumed lets, the
 * handles' lineage rows), shared by every environment derived from the body's: a derived environment changes what
 * it names, never the maps it was handed.
 */
public record PlanEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
        com.legend.sql.dialect.SqlDialect dialect,
        java.util.Map<String, TypedSpec> queryLets,
        java.util.Map<String, String> tableReplace,
        com.legend.lowering.ObjectSites instanceIds,
        java.util.Map<String, java.util.Map<String, java.util.List<java.util.List<String>>>> planRows,
        java.util.List<com.legend.protocol.spec.ValueSpecification> protocolBody,
        com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext frame,
        java.util.Set<com.legend.platform.Feature> features,
        java.util.function.Function<com.legend.compiler.spec.typed.TypedTestDataGen, String> generatorPlanText) {

    /** A body's planning environment at its start: empty accumulators, no renames, no frame, no protocol body.
     *  {@code generatorPlanText}: a test-data generator's plan text ({@code planToString} over the generator's
     *  carrier), which is the generator library's -- a library the planner does not reach, since it also runs the
     *  generator's statements -- handed in by the driver until the generator's planning moves (the seam program's
     *  S2j: the generator's plan a plan of its own). */
    public static PlanEnv of(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect, java.util.Set<com.legend.platform.Feature> features,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedTestDataGen, String> generatorPlanText) {
        return new PlanEnv(ctx, runtimeFqn, dialect, new java.util.LinkedHashMap<>(), java.util.Map.of(),
                // the body is one program: one minter for its objects (B9); the test road's ids keep their own
                // spelling until its layouts converge on the product's (the plan's M5)
                new com.legend.lowering.ObjectSites("i"), new java.util.LinkedHashMap<>(), java.util.List.of(), null,
                features,
                generatorPlanText);
    }

    /** The run's feature flags (the caller's execute options'). */
    public PlanEnv withFeatures(java.util.Set<com.legend.platform.Feature> f) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, planRows, protocolBody,
                frame, f, generatorPlanText);
    }

    /** The executing frame's bound context (post-processors, time zone, options) -- set where an execute frame is
     *  entered. */
    public PlanEnv withFrame(com.legend.compiler.spec.typed.ExecutionContext f) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, planRows, protocolBody, f,
                features, generatorPlanText);
    }

    public PlanEnv withTableReplace(java.util.Map<String, String> tr) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tr, instanceIds, planRows, protocolBody, frame,
                features, generatorPlanText);
    }

    /** The query's PROTOCOL statements (the source-shaped lets a handle's rows are built from -- the lineage scan
     *  walks the raw query lambda). */
    public PlanEnv withProtocolBody(java.util.List<com.legend.protocol.spec.ValueSpecification> body) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, planRows, body, frame,
                features, generatorPlanText);
    }

    public com.legend.compiler.spec.typed.ExecutionContext.PostProcessors postProcessors() {
        return frame == null ? com.legend.compiler.spec.typed.ExecutionContext.PostProcessors.NONE
                : frame.postProcessors();
    }

    /** The frame connection's time zone (every DateTime literal spells in it). */
    public @com.legend.base.Nullable String timeZone() {
        return frame == null ? null : frame.timeZone();
    }
}
