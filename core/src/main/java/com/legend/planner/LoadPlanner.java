// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.cache.Hash;
import com.legend.executionplan.ExecutionPlan;

import java.util.List;

/**
 * THE LOAD PLANNER (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.2 and P2, L2, 2026-11-09): the one place a plan's
 * {@link ExecutionPlan.Load} is made, and the one identity scheme every load takes -- the SHA-256 of a canonical
 * encoding of its steps (each field length-prefixed, a cell's absence marked, a file by its reference's digest, size
 * and format and its delivery), carried in the plan and never recomputed at the run. Equal steps name one load; steps
 * that load differently never do. A target's declared test data ({@link ExecutionPlan.Load.Cadence#PER_ACQUISITION}),
 * a program's seeds ({@code BEFORE_STEP}) and its own loads ({@code AT_STEP}) are all made here.
 */
public final class LoadPlanner {

    private LoadPlanner() {
    }

    /** {@code steps} as a load of {@code cadence} into {@code target}, named by its content ({@link #identity}). */
    public static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target) {
        return new ExecutionPlan.Load(identity(cadence, steps), cadence, steps, target, java.util.Map.of());
    }

    /** {@code steps} as a load of {@code cadence} into {@code target} whose identity was digested already
     *  ({@link #identity}, memoized by its caller: a project's fill, per model). */
    static ExecutionPlan.Load load(String identity, ExecutionPlan.Load.Cadence cadence,
            List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target) {
        return new ExecutionPlan.Load(identity, cadence, steps, target, java.util.Map.of());
    }

    /** {@link #load(String, ExecutionPlan.Load.Cadence, List, ExecutionPlan.Target)} reading {@code content}, the
     *  bundle its CONTENT file steps name (L6c). */
    static ExecutionPlan.Load load(String identity, ExecutionPlan.Load.Cadence cadence,
            List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target, java.util.Map<String, String> content) {
        return new ExecutionPlan.Load(identity, cadence, steps, target, content);
    }

    /** {@code steps} as a load of {@code cadence} into {@code target} reading {@code content}, named by its steps'
     *  content (the digests stand for the bundle's bytes). */
    static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target, java.util.Map<String, String> content) {
        return new ExecutionPlan.Load(identity(cadence, steps), cadence, steps, target, content);
    }

    /** {@code steps} as the loads a target establishes on acquisition: none for no step, else one load into
     *  {@code target}'s own database and session. */
    public static List<ExecutionPlan.Load> targetLoads(List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target) {
        return steps.isEmpty() ? List.of()
                : List.of(load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, steps, target.bare()));
    }

    /** {@code target} establishing {@code steps} on acquisition (none: the target as it is). */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, List<ExecutionPlan.LoadStep> steps) {
        return new ExecutionPlan.Target(target.database(), target.session(), targetLoads(steps, target));
    }

    /** A load's name: the SHA-256 of its canonical content ({@link ExecutionPlan.Load#canonical}: its cadence and its
     *  steps' content), so equal loads share it and loads that run differently never do. Exec recomputes it before it
     *  runs a load and refuses one whose identity is not its content's (the M1 tenets audit's note, 2026-11-09). */
    public static String identity(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps) {
        return ExecutionPlan.Load.IDENTITY_PREFIX + Hash.ofUtf8(ExecutionPlan.Load.canonical(cadence, steps)).hex();
    }
}
