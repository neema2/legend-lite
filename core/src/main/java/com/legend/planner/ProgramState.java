// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.ObjectSites;
import com.legend.platform.Feature;
import com.legend.sql.EnumValueTables;
import com.legend.sql.FrameCtes;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * ONE PROGRAM'S PLANNING STATE (the test-is-the-product plan's M3, brief B12 item 2): what {@code PlanMaker.program}
 * holds across a program's statements, where each statement on the product's road was prepared alone -- the runtime
 * decided once and its target, the one object-id minter (B9), the lets the statements after
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
    private final SqlDialect dialect;
    private final ExecutionPlan.Target target;
    private final List<SqlExpr.PlanParam> slots;
    private final Set<Feature> features;
    /** One minter for the program's objects (B9): a let's construction read by two statements is one object. The
     *  lowering calls it deep inside (an object's site named at its first sight), so it is a minter with one owner,
     *  this program, never a value threaded through. */
    private final ObjectSites sites = new ObjectSites();
    private final List<TypedSpec> lets = new ArrayList<>();
    private final List<ExecutionPlan.Node> steps = new ArrayList<>();
    private final List<String> staged = new ArrayList<>();
    /** The program's frames (M3f): the execute(...) results its statements read, their plan rows and stamps. */
    private final ProgramFrames frames = new ProgramFrames();

    ProgramState(TypedQuery query, String runtime, SqlDialect dialect, ExecutionPlan.Target target,
            List<SqlExpr.PlanParam> slots, Set<Feature> features) {
        this.query = query;
        this.runtime = runtime;
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

    /** The dialect every statement of the program is written in (the runtime's). */
    public SqlDialect dialect() {
        return dialect;
    }

    /** The program's target. */
    public ExecutionPlan.Target target() {
        return target;
    }

    /** The lets planned so far, in order: what the statement being planned reads (a copy -- a list handed out never
     *  changes under its reader). */
    public List<TypedSpec> lets() {
        return List.copyOf(lets);
    }

    /** The program's feature flags. */
    public Set<Feature> features() {
        return features;
    }

    /** The program's frames. */
    public ProgramFrames frames() {
        return frames;
    }

    /** {@code st}, prepared on the product's road, lowered with the product's configuration: the declared parameters'
     *  slots and the frames' stamps bound, the program's minter; {@code last} the program's last statement (the
     *  streaming graph root). */
    public SqlQuery lowered(ProductStatements.Statement st, boolean last) {
        // the frames it reads as their CTE defined ahead of it (M3g)
        return FrameCtes.attach(EnumValueTables.apply(ProductStatements.lower(query,
                ProductStatements.planned(st), st.temporal(), last, PlanMaker.withStamps(slots, frames), sites)),
                st.reads().definitions());
    }

    /** {@code q} written in the program's dialect for its target. */
    public ExecutionPlan.Sql statement(SqlQuery q) {
        return PlanMaker.sql(dialect.renderStatement(q), target, q);
    }

    /** Places {@code step} after the steps planned so far. */
    public void add(ExecutionPlan.Node step) {
        steps.add(step);
    }

    /** Adds {@code let} to the lets the statements after read. */
    void let(TypedSpec let) {
        lets.add(let);
    }

    /** Records {@code table}, a staged let's temporary table, to drop once the program has answered. */
    void stage(String table) {
        staged.add(table);
    }

    /** How many steps the program holds so far: the place the next {@link #add} takes in the sequence (the steps
     *  keep their places when the finished plan drops the stamps nothing reads: {@code withoutUnreadStamps} maps them
     *  one to one, {@code finished} only appends). */
    public int size() {
        return steps.size();
    }

    /** The steps planned so far, in order (a copy). */
    List<ExecutionPlan.Node> steps() {
        return List.copyOf(steps);
    }

    /** The staged tables, in the order they were made (a copy). */
    List<String> staged() {
        return List.copyOf(staged);
    }

    /** The declared parameters' slots every statement binds. */
    List<SqlExpr.PlanParam> slots() {
        return slots;
    }

    /** The program's one object-id minter (B9). */
    ObjectSites sites() {
        return sites;
    }
}
