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
    private final List<PlannedStep> steps = new ArrayList<>();
    private final List<String> staged = new ArrayList<>();
    /** The program's frames (M3f): the execute(...) results its statements read, their plan rows and stamps. */
    private final ProgramFrames frames = new ProgramFrames();
    /** The program's {@code data:} tables (A5): each content's made by the program's own load before its first
     *  reader, and its drops, run after the answer. */
    private final DataSources.Tables data;
    private final List<String> dataReleases = new ArrayList<>();
    /** The loads that make the program's {@code data:} tables, as placed (B2a: the test door marks each for the edge,
     *  which records none of them). */
    private final List<ExecutionPlan.Load> dataLoads = new ArrayList<>();
    /** What the program's statements read (A5): the system metamodel store's tables, another source -- the product's
     *  door runs a program reading the store alone on the process's catalog, as it runs such a query. */
    private final java.util.EnumSet<Reads> reads = java.util.EnumSet.noneOf(Reads.class);

    /** What a statement of the program reads: the system metamodel store's tables, another source. */
    private enum Reads { SYSTEM, OTHER }

    ProgramState(TypedQuery query, String runtime, SqlDialect dialect, ExecutionPlan.Target target,
            List<SqlExpr.PlanParam> slots, Set<Feature> features, DataSources.Tables data) {
        this.query = query;
        this.runtime = runtime;
        this.dialect = dialect;
        this.target = target;
        this.slots = slots;
        this.features = features;
        if (data.dialect().getClass() != dialect.getClass()) {
            throw new IllegalStateException("the data: tables planned ahead of the program are written in "
                    + data.dialect().getClass().getSimpleName() + ", the program in " + dialect.getClass().getSimpleName());
        }
        this.data = data;
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
        return read(FrameCtes.attach(EnumValueTables.apply(ProductStatements.lower(query,
                ProductStatements.planned(st), st.temporal(), last, PlanMaker.withStamps(slots, frames), sites)),
                st.reads().definitions()));
    }

    /**
     * {@code q} reading each {@code data:} source from its table (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §4, A5: data
     * written in the model is the plan's load on every road, never a literal): a content not read before is made by
     * the program's own load ({@code AT_STEP}), placed now -- ahead of the statement that reads it -- on the program's
     * session, and dropped after the program's answer ({@link #dataReleases}). A program's statements run in one
     * transaction, so a failed program's tables go with its writes where the database's DDL is transactional.
     */
    public SqlQuery read(SqlQuery q) {
        reads(PlanMaker.tableReads(q, query.context(),
                java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(staged))));
        SqlQuery read = data.read(q);
        placeMade();
        return read;
    }

    /** {@code dml}'s rows and conditions reading each {@code data:} source from its table, as {@link #read(SqlQuery)}:
     *  a store write writes another source than the metamodel's. */
    public com.legend.sql.SqlDml read(com.legend.sql.SqlDml dml) {
        reads.add(Reads.OTHER);
        com.legend.sql.SqlDml read = data.read(dml);
        placeMade();
        return read;
    }

    private void reads(PlanMaker.TableReads read) {
        if (read.system()) {
            reads.add(Reads.SYSTEM);
        }
        if (read.other()) {
            reads.add(Reads.OTHER);
        }
    }

    /** Whether a statement of the program reads the system metamodel store's tables. */
    boolean readsSystem() {
        return reads.contains(Reads.SYSTEM);
    }

    /** Whether a statement of the program reads, or writes, a source other than the system metamodel store. */
    boolean readsOther() {
        return reads.contains(Reads.OTHER);
    }

    /** The tables made since the last placement -- a statement's, or the steps planned ahead of the program -- placed
     *  now, as the program's own load ({@code AT_STEP}) after the steps planned so far. */
    void placeMade() {
        DataSources.Tables.Made made = data.take();
        if (!made.steps().isEmpty()) {
            ExecutionPlan.Load load = LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_STEP, made.steps(), target,
                    made.content());
            add(load);
            dataLoads.add(load);
            dataReleases.addAll(made.drops());
        }
    }

    /** The drops of the program's {@code data:} tables, run after its answer. */
    List<String> dataReleases() {
        return List.copyOf(dataReleases);
    }

    /** The loads that make the program's {@code data:} tables, as placed. */
    List<ExecutionPlan.Load> dataLoads() {
        return List.copyOf(dataLoads);
    }

    /** {@code q} written in the program's dialect for its target. */
    public ExecutionPlan.Sql statement(SqlQuery q) {
        return PlanMaker.sql(dialect.renderStatement(q), target, q);
    }

    /** Places {@code step} after the steps planned so far. */
    public void add(ExecutionPlan.Node step) {
        steps.add(new PlannedStep.Product(step));
    }

    /** Places the extension's step {@code handle} after the steps planned so far, reading {@code declared} (the
     *  plan-as-data design's step 3, 2026-11-08: a test's verdict group or branch, which the product's plan never
     *  holds). */
    public void addExtension(int handle, List<ExecutionPlan.Node> declared) {
        // the tables its statements made when they were planned ahead, made before it (B2a)
        placeMade();
        steps.add(new PlannedStep.Extension(handle, declared));
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
    List<PlannedStep> steps() {
        return new ArrayList<>(steps);
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
