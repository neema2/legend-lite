// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * ONE PROGRAM'S PLANNING STATE (the test-is-the-product plan's M3, brief B12 item 2): what {@code PlanMaker.program}
 * holds across a program's statements, where each statement on the product's road was prepared alone -- the door it
 * came through, the runtime decided once and its target, the one object-id minter (B9), the lets the statements after
 * read, the steps planned so far, the staged tables to drop. M3's later sub-steps add the frames and the plan rows a
 * frame needs across statements, the last seed key emitted, and the steps waiting to be placed before a statement.
 * The test-planning library reads it and adds its steps through {@link ProgramKinds}.
 */
public final class ProgramState {

    /** The door a program came through: the product's query door ({@code PlanMaker.plan}, a program that writes a
     *  store), or the test door (the test-planning library's, {@code PlanMaker.testProgram}). */
    public enum Door { QUERY, TEST }

    private final TypedQuery query;
    private final String runtime;
    private final Door door;
    private final SqlDialect dialect;
    private final ExecutionPlan.Target target;
    final List<com.legend.sql.SqlExpr.PlanParam> slots;
    final java.util.Set<com.legend.platform.Feature> features;
    /** One minter for the program's objects (B9): a let's construction read by two statements is one object. */
    final com.legend.lowering.ObjectSites sites = new com.legend.lowering.ObjectSites();
    final List<TypedSpec> lets = new ArrayList<>();
    final List<ExecutionPlan.Node> steps = new ArrayList<>();
    final List<String> staged = new ArrayList<>();

    ProgramState(TypedQuery query, String runtime, Door door, SqlDialect dialect, ExecutionPlan.Target target,
            List<com.legend.sql.SqlExpr.PlanParam> slots, java.util.Set<com.legend.platform.Feature> features) {
        this.query = query;
        this.runtime = runtime;
        this.door = door;
        this.dialect = dialect;
        this.target = target;
        this.slots = slots;
        this.features = features;
    }

    /** The program's query: its body, its model, its compiler. */
    public TypedQuery query() {
        return query;
    }

    /** The runtime the program runs on, decided once. */
    public String runtime() {
        return runtime;
    }

    /** The door the program came through. */
    public Door door() {
        return door;
    }

    /** The dialect every statement of the program is written in (the runtime's). */
    public SqlDialect dialect() {
        return dialect;
    }

    /** The program's target. */
    public ExecutionPlan.Target target() {
        return target;
    }

    /** Places {@code step} after the steps planned so far. */
    public void add(ExecutionPlan.Node step) {
        steps.add(step);
    }
}
