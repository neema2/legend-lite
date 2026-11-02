// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.lowering.ObjectSites;
import com.legend.platform.Feature;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.sql.dialect.SqlDialect;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

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
public record PlanEnv(ModelContext ctx, @Nullable String runtimeFqn,
        com.legend.sql.dialect.@Nullable SqlDialect dialect,
        Map<String, TypedSpec> queryLets,
        Map<String, String> tableReplace,
        ObjectSites instanceIds,
        Map<String, Map<String, List<List<String>>>> planRows,
        List<ValueSpecification> protocolBody,
        com.legend.compiler.spec.typed.@Nullable ExecutionContext frame,
        Set<Feature> features,
        Function<TypedTestDataGen, String> generatorPlanText,
        List<String> stamps) {

    /** A body's planning environment at its start: empty accumulators, no renames, no frame, no protocol body.
     *  {@code stamps} (the third run-scoped accumulator, the test-is-the-product plan's M9c): the execution-trace stamp
     *  parameters a frame's pre-planned activity rows carry as cells ({@code LazyRows.Parameter}), which every
     *  statement lowered on this environment binds as a String plan parameter ({@code BodyPlanner.lowererFor}) -- the
     *  comment of a frame run by the program's check step at its let, read by a side planned before the program ran.
     *  {@code generatorPlanText}: a test-data generator's plan text ({@code planToString} over the generator's
     *  carrier), which is the generator library's -- a library the planner does not reach, since it also runs the
     *  generator's statements -- handed in by the driver until the generator's planning moves (the seam program's
     *  S2j: the generator's plan a plan of its own). */
    public static PlanEnv of(ModelContext ctx, @Nullable String runtimeFqn,
            com.legend.sql.dialect.@Nullable SqlDialect dialect,
            Set<Feature> features,
            Function<TypedTestDataGen, String> generatorPlanText) {
        return new PlanEnv(ctx, runtimeFqn, dialect, new LinkedHashMap<>(), Map.of(),
                // the body is one program: one minter for its objects (B9); the test road's ids keep their own
                // spelling until its layouts converge on the product's (the plan's M5)
                new ObjectSites("i"), new LinkedHashMap<>(), List.of(), null,
                features,
                generatorPlanText, new java.util.ArrayList<>());
    }

    /** This environment reading and registering the plan rows {@code rows} (a program's, shared by its statements:
     *  the test-is-the-product plan's M3f). */
    public PlanEnv withPlanRows(Map<String, Map<String, List<List<String>>>> rows) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, rows, protocolBody, frame,
                features, generatorPlanText, stamps);
    }

    /** This environment writing its statements in {@code d}: a test's program on the product's road, its runtime
     *  decided (the test door's effects, the test-is-the-product plan's M3d). */
    public PlanEnv withDialect(SqlDialect d) {
        return new PlanEnv(ctx, runtimeFqn, d, queryLets, tableReplace, instanceIds, planRows, protocolBody, frame,
                features, generatorPlanText, stamps);
    }

    /** The run's feature flags (the caller's execute options'). */
    /** {@code stamp} carried by the statements lowered on this environment (the stamps' accumulator), once: both halves
     *  of the agreement judge plan a frame under the host's stamp (the walker-deletion program, 2026-11-02). */
    public void stamp(String stamp) {
        if (!stamps.contains(stamp)) {
            stamps.add(stamp);
        }
    }

    public PlanEnv withFeatures(Set<Feature> f) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, planRows, protocolBody,
                frame, f, generatorPlanText, stamps);
    }

    /** The dialect this environment's statements are written in: a test body's environment always names one; the
     *  product's statement road ({@code ProductStatements}) names none, since its caller renders in the runtime's
     *  dialect and nothing on that road writes a statement itself. */
    public SqlDialect writtenIn() {
        return Objects.requireNonNull(dialect, "a statement written on an environment that names no"
                + " dialect: the product's statement road writes none");
    }

    /** The executing frame's bound context (post-processors, time zone, options) -- set where an execute frame is
     *  entered. */
    public PlanEnv withFrame(ExecutionContext f) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, planRows, protocolBody, f,
                features, generatorPlanText, stamps);
    }

    public PlanEnv withTableReplace(Map<String, String> tr) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tr, instanceIds, planRows, protocolBody, frame,
                features, generatorPlanText, stamps);
    }

    /** The query's PROTOCOL statements (the source-shaped lets a handle's rows are built from -- the lineage scan
     *  walks the raw query lambda). */
    public PlanEnv withProtocolBody(List<ValueSpecification> body) {
        return new PlanEnv(ctx, runtimeFqn, dialect, queryLets, tableReplace, instanceIds, planRows, body, frame,
                features, generatorPlanText, stamps);
    }

    public ExecutionContext.PostProcessors postProcessors() {
        return frame == null ? ExecutionContext.PostProcessors.NONE
                : frame.postProcessors();
    }

    /** The frame connection's time zone (every DateTime literal spells in it). */
    public @Nullable String timeZone() {
        return frame == null ? null : frame.timeZone();
    }
}
