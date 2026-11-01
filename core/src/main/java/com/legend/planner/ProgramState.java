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
 * read, the steps planned so far, the staged tables to drop, the frames its statements read with their plan rows and
 * stamps ({@link ProgramFrames}, M3f). The test-planning library reads it and adds its steps through
 * {@link ProgramKinds} (its own memo of the seeds placed and the open script is its own, M3d-M3e).
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
    /** The program's frames (M3f): the execute(...) results its statements read, their plan rows and stamps. */
    private final ProgramFrames frames = new ProgramFrames();

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

    /** The lets the statements after read (the lets planned so far, in order). */
    public List<TypedSpec> lets() {
        return java.util.Collections.unmodifiableList(lets);
    }

    /** The program's feature flags. */
    public java.util.Set<com.legend.platform.Feature> features() {
        return features;
    }

    /** The program's frames. */
    public ProgramFrames frames() {
        return frames;
    }

    /** {@code st}, prepared on the product's road, lowered with the product's configuration: the declared parameters'
     *  slots and the frames' stamps bound, the program's minter; {@code last} the program's last statement (the
     *  streaming graph root). */
    public com.legend.sql.SqlQuery lowered(ProductStatements.Statement st, boolean last) {
        // the frames it reads as their CTE defined ahead of it (M3g)
        return com.legend.sql.FrameCtes.attach(com.legend.sql.EnumValueTables.apply(ProductStatements.lower(query,
                ProductStatements.planned(st), st.temporal(), last, PlanMaker.withStamps(slots, frames), sites)),
                frames.definitions());
    }

    /** {@code q} written in the program's dialect for its target. */
    public ExecutionPlan.Sql statement(com.legend.sql.SqlQuery q) {
        return PlanMaker.sql(dialect.renderStatement(q), target, q);
    }

    /** Places {@code step} after the steps planned so far. */
    public void add(ExecutionPlan.Node step) {
        steps.add(step);
    }
}
