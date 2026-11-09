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
        return new ExecutionPlan.Load(identity(steps), cadence, steps, target);
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

    /** A load's name: the content of its steps (a statement's text, a rows step's statements and cells, a file's
     *  reference and delivery), so equal loads share it and loads that run differently never do (the M3 audit; the
     *  design audit's H1, 2026-11-02: each field length-prefixed, since a record's text joins fields with no escape). */
    public static String identity(List<ExecutionPlan.LoadStep> steps) {
        StringBuilder sb = new StringBuilder();
        for (ExecutionPlan.LoadStep step : steps) {
            switch (step) {
                case ExecutionPlan.LoadStep.Statement st -> field(sb.append('S'), st.sql());
                case ExecutionPlan.LoadStep.Rows r -> {
                    field(field(field(field(sb.append('R'), r.stagingTable()), r.createStaging()), r.copy()),
                            r.dropStaging());
                    sb.append(r.rows().size()).append(':');
                    for (List<String> row : r.rows()) {
                        sb.append(row.size()).append(':');
                        for (String cell : row) {
                            if (cell == null) {
                                sb.append('N');
                            } else {
                                field(sb.append('V'), cell);
                            }
                        }
                    }
                }
                case ExecutionPlan.LoadStep.File f -> {
                    field(field(field(sb.append('F'), f.ref().namespace().name()), f.ref().name()), f.ref().digest());
                    sb.append(f.ref().size()).append(':').append(f.sizeLimitMegabytes()).append(':');
                    switch (f.ref().format()) {
                        case com.legend.executionplan.DataRef.Csv csv ->
                                sb.append("csv").append(csv.delimiter()).append(csv.quote());
                    }
                    switch (f.delivery()) {
                        case ExecutionPlan.LoadStep.Delivery.Streamed st ->
                                field(sb.append("streamed").append(st.columns()).append(':'), st.insert());
                    }
                }
            }
        }
        return "load:" + Hash.ofUtf8(sb.toString()).hex();
    }

    /** {@code text} into {@code sb} prefixed by its length: no two field sequences spell one string. */
    private static StringBuilder field(StringBuilder sb, String text) {
        return sb.append(text.length()).append(':').append(text);
    }
}
