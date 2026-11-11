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

    /** {@code steps} as a load of {@code cadence} into {@code target}, reading their bundle (A2: a table's rows are a
     *  content blob of inserts), named by their content. */
    public static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, com.legend.setup.LoadSteps steps,
            ExecutionPlan.Target target) {
        return load(cadence, steps.steps(), target, steps.content());
    }

    /** {@code steps} as the loads a target establishes on acquisition: none for no step, else one load into
     *  {@code target}'s own database and session. */
    public static List<ExecutionPlan.Load> targetLoads(com.legend.setup.LoadSteps steps,
            ExecutionPlan.Target target) {
        return steps.isEmpty() ? List.of()
                : List.of(load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, steps, target.bare()));
    }

    /** {@code target} establishing {@code steps} on acquisition (none: the target as it is). */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, com.legend.setup.LoadSteps steps) {
        return new ExecutionPlan.Target(target.database(), target.session(), targetLoads(steps, target));
    }

    /** {@link #withLoads(ExecutionPlan.Target, com.legend.setup.LoadSteps)} for steps that read no blob. */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, List<ExecutionPlan.LoadStep> steps) {
        return withLoads(target, com.legend.setup.LoadSteps.of(steps));
    }

    /** A load's name: the SHA-256 of its canonical content ({@link ExecutionPlan.Load#canonical}: its cadence and its
     *  steps' content), so equal loads share it and loads that run differently never do. Exec recomputes it before it
     *  runs a load and refuses one whose identity is not its content's (the M1 tenets audit's note, 2026-11-09). */
    public static String identity(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps) {
        return ExecutionPlan.Load.IDENTITY_PREFIX + Hash.ofUtf8(ExecutionPlan.Load.canonical(cadence, steps)).hex();
    }

    /** The test data {@code runtime}'s connections declare, as load steps final for {@code dialect}'s database: a plan's
     *  target's acquisition load. Its steps are {@code CsvSeed.declaredSteps}', which {@link #seeds} plans too, beside a
     *  statement's other seeds, in one load (B2a, 2026-11-11: until then the target's in {@code PlanMaker}, the seeds'
     *  in the test planner's {@code Seeds.planned}; the B2a audit's note: the javadoc said seeds called this). */
    public static com.legend.setup.LoadSteps declared(String runtime,
            com.legend.compiler.element.ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        return com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect), dialect);
    }

    /**
     * {@code statement}'s SEEDS as a program's load steps, final for {@code dialect}'s database (B2a, 2026-11-11: the
     * load planner's, moved from the test planner's {@code Seeds.planned}), with the referee's own copy of them, written
     * in {@code referee}'s dialect (A2; the edge's, until B2b replays the plan's loads): each {@code from()}'s inline test
     * data and named runtime's declared data, then the program's {@code runtime}'s -- each runtime's once, and each
     * from()'s inline data once (the correctness audit's C3, 2026-11-10: a from()'s runtime that is the program's too was
     * planned twice, its setup SQL failing "already exists", its rows doubled; the A delta audit's note 10, 2026-11-11:
     * two from()s of one instance runtime too).
     */
    public static com.legend.setup.CsvSeed.Planned seeds(com.legend.compiler.spec.typed.TypedSpec statement,
            @com.legend.base.Nullable String runtime, com.legend.compiler.element.ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, com.legend.sql.dialect.SqlDialect referee) {
        List<com.legend.setup.CsvSeed.Step> setups = new java.util.ArrayList<>();
        java.util.Set<String> declared = new java.util.HashSet<>();
        java.util.Set<List<Object>> inline = new java.util.HashSet<>();
        for (com.legend.compiler.spec.typed.TypedFrom fr : com.legend.compiler.spec.typed.ExecutionContext.froms(
                statement)) {
            // (the setup SQL's language asked only where there is setup SQL: a connection with none may declare none)
            if (inline.add(List.of(fr.sqlSetups(), fr.csvSetups(),
                    fr.sqlSetups().isEmpty() ? "" : String.valueOf(fr.context().rawLanguage())))) {
                for (String sql : fr.sqlSetups()) {
                    // written in the database the from()'s connection declares (decision D2)
                    setups.add(new com.legend.setup.CsvSeed.Step.Sql(sql, fr.context().rawLanguage()));
                }
                setups.addAll(com.legend.setup.CsvSeed.setupSteps(fr, ctx, dialect));
            }
            // an ELEMENT runtime named by the from(): its declared connections' test data (the instance form above
            // carries the same facts on the instance)
            fr.context().runtime().ifPresent(ref -> {
                if (declared.add(ref.fullPath())) {
                    setups.addAll(com.legend.setup.CsvSeed.declaredSteps(ref.fullPath(), ctx, dialect));
                }
            });
        }
        // the program's runtime: its declared connections' test data seeds the session the same way
        if (runtime != null && declared.add(runtime)) {
            setups.addAll(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect));
        }
        return com.legend.setup.CsvSeed.planned(setups, dialect, referee);
    }
}
