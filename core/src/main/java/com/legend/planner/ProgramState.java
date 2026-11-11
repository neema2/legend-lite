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
    /** Each distinct drop once, in the order the tables were made (H2's release makes its schema first, once). */
    private final java.util.Set<String> dataReleases = new java.util.LinkedHashSet<>();
    /** What the tables' database sends before the program's first statement (H2's schema: the B2a audit's note). */
    private final java.util.Set<String> dataPrepare = new java.util.LinkedHashSet<>();
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
     * the program's own load ({@code SOURCE}: a live source's session table, no write of the body's), placed now --
     * ahead of the statement that reads it -- on the program's session, and dropped by the plan's release once the run
     * is over, whatever its outcome ({@link #dataReleases}; on the graph's system catalog, by its turn's rollback).
     */
    public SqlQuery read(SqlQuery q) {
        reads(PlanMaker.tableReads(q, query.context(),
                java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(staged))));
        SqlQuery read = data.read(q, target);
        placeMade();
        return read;
    }

    /** {@code dml}'s rows and conditions reading each {@code data:} source from its table, as {@link #read(SqlQuery)}:
     *  a store write writes another source than the metamodel's. */
    public com.legend.sql.SqlDml read(com.legend.sql.SqlDml dml) {
        reads.add(Reads.OTHER);
        com.legend.sql.SqlDml read = data.read(dml, target);
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
     *  now, as the program's own load ({@code SOURCE}) after the steps planned so far. */
    void placeMade() {
        DataSources.Tables.Made made = data.take();
        // a session table's prepare (H2's schema statement) commits an open transaction, and on the graph's system
        // catalog the run's transaction holds the model's registration, which the run rolls back (measured
        // 2026-11-11: committed, the next run's registration collided): the catalog made it ONCE with its own tables
        // (SystemCatalog.target), so a body routed there sends none (the user's veto, 2026-11-11, of the refusal the
        // B2a audit's S2 had placed). Nor any DROP: a release on the catalog runs inside the run's turn, which is held
        // until the run's sessions close, and H2's DROP commits it (measured 2026-11-11: the next run's registration
        // collided); the turn is rolled back whole when the run ends (SystemDatabase), taking the table's rows -- and,
        // where the database's DDL is transactional (DuckDB, SQLite), the table -- with it. A table H2 keeps is the
        // catalog session's own, empty, and named by its content: the next make of it empties and fills it again.
        boolean catalog = target.database() instanceof ExecutionPlan.Database.System;
        if (catalog && !SystemCatalog.madeOnce(target, made.prepare())) {
            throw new IllegalStateException("a data: session table's schema statements " + made.prepare()
                    + " are not the system catalog's ONCE load's on " + target.database());
        }
        if (!made.steps().isEmpty()) {
            ExecutionPlan.Load load = LoadPlanner.load(ExecutionPlan.Load.Cadence.SOURCE, made.steps(), target,
                    made.content());
            add(load);
            dataLoads.add(load);
            if (!catalog) {
                dataReleases.addAll(made.drops());
                dataPrepare.addAll(made.prepare());
            }
        }
    }

    /** What the program's tables' database sends before its first statement, where it made any (H2's schema). */
    List<String> dataPrepare() {
        return List.copyOf(dataPrepare);
    }

    /** The drops of the program's {@code data:} tables, run after its answer. */
    List<String> dataReleases() {
        return List.copyOf(dataReleases);
    }

    /** The loads that make the program's {@code data:} tables, as placed. */
    List<ExecutionPlan.Load> dataLoads() {
        return List.copyOf(dataLoads);
    }

    /** Records {@code load}, the tables' prepare the planner places first, among the program's data: loads. */
    void dataLoad(ExecutionPlan.Load load) {
        dataLoads.add(load);
    }

    /** {@code q} written in the program's dialect for its target. */
    public ExecutionPlan.Sql statement(SqlQuery q) {
        return PlanMaker.sql(dialect.renderStatement(q), target, q);
    }

    /** Places {@code step} after the steps planned so far. */
    public void add(ExecutionPlan.Node step) {
        steps.add(new PlannedStep.Product(step));
    }

    /** Places {@code run}, a frame let's eager run (M9), after the steps planned so far, marked as one
     *  ({@link PlannedStep.FrameRun}). */
    public void addFrameRun(ExecutionPlan.Check run) {
        steps.add(new PlannedStep.FrameRun(run));
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
