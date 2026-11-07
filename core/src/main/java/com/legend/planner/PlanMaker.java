// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.SystemMetamodel;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.database.Databases;
import com.legend.database.Target;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ColumnFacts;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.KeyedPivot;
import com.legend.lowering.Lowerer;
import com.legend.lowering.ObjectSites;
import com.legend.lowering.SqlPostProcessors;
import com.legend.lowering.TypedRows;
import com.legend.model.ConnectionDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.FunctionId;
import com.legend.model.RelationalDataType;
import com.legend.plan.PlanColumns;
import com.legend.plan.ResultShape;
import com.legend.platform.Feature;
import com.legend.resolver.StagedRows;
import com.legend.setup.CsvSeed;
import com.legend.setup.Ddl;
import com.legend.sql.EnumValueTables;
import com.legend.sql.FrameCtes;
import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlWith;
import com.legend.sql.ValueKind;
import com.legend.sql.dialect.RenderedStatement;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * THE PLANNER MAKES LITE PLANS (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): a typed query planned
 * for its runtime as an {@link ExecutionPlan} that a model-free runner executes. Its answer is TYPED ROWS
 * (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state of 2026-10-27: a relation's rows, a value's elements, a
 * graph fetch's objects), read by the plan's column facts and spelled by a writer at the edge: no plan asks its
 * database to write a text (the last, Pure's TDS text, went with the program's phase 4). Every statement is final
 * for its target: written in the dialect of the target's database, with the server versions that spelling is for; and
 * the target is whole — the database (a declared connection, or the platform's own engine), the statements each
 * connection runs first, and the setup that establishes it, written at plan time.
 *
 * <p>Public since step C (D73, 2026-10-30): the planner's machinery is its own package, so the root's door (TypedQuery.plan) and the driver (the body driver's and the judges' plans) reach it across a package line; the product API is the door.
 */
public final class PlanMaker {

    /** The query door's: no statement is a test's kind (its test-only kinds were refused before, QueryDoor). */
    private static final ProgramKinds NO_TEST_KINDS = (program, index, statement, last) -> false;

    /** The plan's declared parameters, as the plan carries them. */
    private static List<ExecutionPlan.Parameter> declarations(List<QueryParameters.Declared> declared,
            ModelContext ctx) {
        return declared.stream().map(p -> p.declaration(ctx)).toList();
    }

    private PlanMaker() {
    }

    /** {@code query}'s TYPED plan on {@code runtime}, lowered under the feature flags {@code features}, each declared
     *  parameter a value the statement binds where it is used. No server version: a database's statements are spelled
     *  alike for every version lite runs (2026-11-05, one H2; until then H2's plans were written per version, read off
     *  the caller's session). */
    public static ExecutionPlan plan(TypedQuery query, @Nullable String runtime, Set<Feature> features) {
        // THE QUERY DOOR (B6, B12 item 1): a test's kinds refused by name before anything is planned
        QueryDoor.refuseTestOnly(query, runtime);
        List<QueryParameters.Declared> declared = query.parameters();
        List<SqlExpr.PlanParam> slots = slots(declared);
        if (query.writes()) {
            // a program that changes a store: main's write plans (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7; merged
            // 2026-10-26), a sequence of steps on one session
            return new ExecutionPlan(declarations(declared, query.context()),
                    program(query, runtime, slots, features, ProgramState.Door.QUERY,
                            NO_TEST_KINDS, false));
        }
        // the product's statement road (M2, B11); a value the program wrote is answered as a constant, nothing sent
        // a query's inline execute(...) reads are frames too (D15, M3f): their activities' stamps the statement's own
        ProgramFrames frames = new ProgramFrames();
        ProductStatements.Statement st = ProductStatements.prepare(query, query.body(), runtime, null, features, frames);
        ExecutionPlan.Constant constant = ProductStatements.constant(st);
        if (constant != null) {
            return new ExecutionPlan(declarations(declared, query.context()), constant);
        }
        // a typed plan answers a graph fetch's objects one per row: the streaming graph root
        Prepared.Planned planned = ProductStatements.planned(st);
        Compiler.LoweredQuery l = new Compiler.LoweredQuery(ProductStatements.lower(query, planned, st.temporal(), true,
                withStamps(slots, frames), new ObjectSites()),
                planned.body().get(planned.body().size() - 1), query.context());
        Decided decided = decide(l.ctx(), runtime);
        SqlDialect dialect = decided.dialect();
        Metamodel metamodel = metamodel(l, dialect);
        ExecutionPlan.Target target = target(decided.target(), Objects.requireNonNull(runtime), l.ctx(), dialect,
                metamodel);
        // an enumeration parameter compared with a mapped column: through that place's value table; a system
        // metamodel table on a server read by its session name (Metamodel.renames)
        SqlQuery lowered = metamodel.read(EnumValueTables.apply(l.plan()));
        Staged staged = stagedShared(dialect, lowered, target, 0);
        ExecutionPlan.Node answer = stampedIfRead(typed(l.root(), staged.statement(), dialect, target),
                st.reads().ownStamp());
        if (staged.tables().isEmpty()) {
            return new ExecutionPlan(declarations(declared, l.ctx()), answer);
        }
        // the shared relations' tables made ahead of the answer and dropped after it (stagedShared): effects, a
        // session's own tables, never a change of the database's (no transaction is opened for them: ClickHouse's
        // driver takes none, "setAutoCommit(false) is not supported", measured 2026-11-07)
        List<ExecutionPlan.Node> steps = new ArrayList<>();
        for (String table : staged.tables()) {
            // a table a failed run left in the session (its drop never ran) is dropped before it is made again
            steps.add(new ExecutionPlan.Effect(dropping(dialect, table, target)));
            steps.add(new ExecutionPlan.Effect(staging(dialect, staged, table, target)));
        }
        steps.add(answer);
        if (!dialect.dropsStagedTablesWithTheirTransaction()) {
            for (String table : staged.tables()) {
                steps.add(new ExecutionPlan.Effect(dropping(dialect, table, target)));
            }
        }
        return new ExecutionPlan(declarations(declared, l.ctx()), new ExecutionPlan.Sequence(steps));
    }

    /** {@code slots} and the stamp parameters of {@code frames}, which every statement's lowering binds. */
    static List<SqlExpr.PlanParam> withStamps(List<SqlExpr.PlanParam> slots,
            ProgramFrames frames) {
        List<SqlExpr.PlanParam> out = new ArrayList<>(slots);
        out.addAll(frames.stampSlots());
        return out;
    }

    /**
     * {@code step}, binding its own execution-trace stamp under {@code stamp} where its statement reads it (an inline
     * frame's activity comment it records: the statement that runs the frame is this one, R17 C4); unchanged where
     * {@code stamp} is null or nothing in it reads it. A keyed result reading it is refused by name: its two
     * statements' stamps are not designed.
     */
    static ExecutionPlan.Node stampedIfRead(ExecutionPlan.Node step, @Nullable String stamp) {
        if (stamp == null) {
            return step;
        }
        return switch (step) {
            case ExecutionPlan.TdsResult t -> reads(t.sql(), stamp)
                    ? new ExecutionPlan.TdsResult(t.shape(), t.columns(), t.sql().stamped(stamp)) : t;
            case ExecutionPlan.Check c -> reads(c.sql(), stamp) ? new ExecutionPlan.Check(c.sql().stamped(stamp)) : c;
            case ExecutionPlan.Keyed k -> {
                if (reads(k.keys(), stamp) || reads(k.body(), stamp)) {
                    throw new NotImplementedException("a keyed result reading an inline execute(...)'s"
                            + " activity comment: the stamp of a keyed plan's statements is not designed");
                }
                yield k;
            }
            default -> throw new IllegalStateException("a stamped " + step.getClass().getSimpleName()
                    + ": only a query's or a check's statement reads its own stamp");
        };
    }

    /** {@code steps}' checks with their stamp dropped where no step binds it. */
    private static List<ExecutionPlan.Node> withoutUnreadStamps(List<ExecutionPlan.Node> steps) {
        Set<String> read = new HashSet<>();
        for (ExecutionPlan.Node step : steps) {
            for (ExecutionPlan.Sql sql : sqls(step)) {
                sql.slots().forEach(slot -> read.add(slot.parameter()));
            }
        }
        List<ExecutionPlan.Node> out = new ArrayList<>(steps.size());
        for (ExecutionPlan.Node step : steps) {
            out.add(step instanceof ExecutionPlan.Check c && c.sql().stamp() != null && !read.contains(c.sql().stamp())
                    ? new ExecutionPlan.Check(new ExecutionPlan.Sql(c.sql().statement(), c.sql().slots(),
                            c.sql().target(), c.sql().tree(), c.sql().regions()))
                    : step);
        }
        return out;
    }

    /** The test door's refusal of a statement its library does not plan and the product road prepares as no query
     *  (the M3 audit: the query door's words misled there). */
    private static void unplannedOnTheTestDoor(ProductStatements.Statement st) {
        switch (st.prepared()) {
            case Prepared.Effect e -> throw new NotImplementedException("an effect ("
                    + e.call().callee().qualifiedName() + ") on the test door's program road: not planned yet (the"
                    + " test-is-the-product plan's M5; setUpDataSQLs' texts as a constant, B12 item 4, deferred there)");
            case Prepared.EffectfulMap m -> throw new NotImplementedException("a map over an effect on"
                    + " the test door's program road: the T9 kind the test-is-the-product plan's M9 plans");
            default -> {
                // planned, or answered above (a store write, a constant, a handle)
            }
        }
    }

    /** Every execution-trace stamp a step's statement reads is bound by a step of the program (a frame's run, or the
     *  statement itself): a stamp nothing binds -- the activity comment of an execute(...) run inside a frame let's
     *  own query, which the let's run does not stamp -- is refused by name (the MR inventory's latent defect 3, where
     *  it failed unbound when the plan ran; MR's audit S2: only where it is read). */
    private static void everyReadStampBound(List<ExecutionPlan.Node> steps) {
        Set<String> bound = new HashSet<>();
        for (ExecutionPlan.Node step : steps) {
            for (ExecutionPlan.Sql sql : sqls(step)) {
                if (sql.stamp() != null) {
                    bound.add(sql.stamp());
                }
            }
        }
        for (ExecutionPlan.Node step : steps) {
            for (ExecutionPlan.Sql sql : sqls(step)) {
                for (ExecutionPlan.Slot slot : sql.slots()) {
                    if (slot.parameter().startsWith(ProgramFrames.STAMP) && !bound.contains(slot.parameter())) {
                        throw new NotImplementedException("a statement reads the trace stamp '" + slot.parameter()
                                + "' of an execute(...) run that no step of the program binds (a run inside a frame"
                                + " let's own query): not designed");
                    }
                }
            }
        }
    }

    /** The statements a program's step sends. */
    private static List<ExecutionPlan.Sql> sqls(ExecutionPlan.Node step) {
        return switch (step) {
            case ExecutionPlan.TdsResult t -> List.of(t.sql());
            case ExecutionPlan.Check c -> List.of(c.sql());
            case ExecutionPlan.Update u -> List.of(u.sql());
            case ExecutionPlan.Effect e -> List.of(e.sql());
            case ExecutionPlan.Keyed k -> List.of(k.keys(), k.body());
            case ExecutionPlan.Setup u -> List.of();
            case ExecutionPlan.Constant c -> List.of();
            // a verdict group (the test-is-the-product plan's M6): its sides' statements, its wrappers', its fused one
            case ExecutionPlan.Verdicts v -> {
                List<ExecutionPlan.Sql> out = new ArrayList<>();
                for (ExecutionPlan.Verdict verdict : v.verdicts()) {
                    for (ExecutionPlan.Side side : verdict.base()) {
                        out.addAll(sideSqls(side));
                    }
                    if (verdict.wrapper() != null) {
                        out.addAll(sqls(verdict.wrapper()));
                    }
                    if (verdict.message() != null) {
                        out.addAll(sideSqls(verdict.message()));
                    }
                }
                if (v.fused() != null) {
                    out.add(v.fused().sql());
                }
                yield out;
            }
            // a branch (the asserts-as-plan-nodes program, step 4): its condition's statement, then both sides' -- each
            // side's statements are the program's whichever side runs
            case ExecutionPlan.Branch b -> {
                List<ExecutionPlan.Sql> out = new ArrayList<>(sideSqls(b.condition()));
                for (ExecutionPlan.Node n : b.then()) {
                    out.addAll(sqls(n));
                }
                for (ExecutionPlan.Node n : b.otherwise()) {
                    out.addAll(sqls(n));
                }
                yield out;
            }
            case ExecutionPlan.Sequence q -> throw new IllegalStateException("a sequence inside a sequence");
        };
    }

    /** The statements a side sends: a constant's none. */
    private static List<ExecutionPlan.Sql> sideSqls(ExecutionPlan.Side side) {
        return switch (side) {
            case ExecutionPlan.Side.Rows r -> List.of(r.result().sql());
            case ExecutionPlan.Side.KeyedRows k -> List.of(k.keyed().keys(), k.keyed().body());
            case ExecutionPlan.Side.Constant c -> List.of();
        };
    }

    /** Whether {@code sql} binds the parameter {@code name}. */
    private static boolean reads(ExecutionPlan.Sql sql, String name) {
        return sql.slots().stream().anyMatch(slot -> slot.parameter().equals(name));
    }

    /**
     * THE TEST DOOR's plan (the test-is-the-product plan's M3, B12 item 1): {@code query}'s body planned as a program
     * whatever it holds, each statement holding a test's kind planned by {@code kinds} -- the test-planning
     * library's, its one caller (ArchitectureTest pins it) -- on {@code runtime}. Its target carries no setup (R17
     * C6): a test's seeds are steps of its own program.
     * {@code systemRouted}: the body's statements read the graph's system metamodel store and no user store, so the
     * program's target is the SYSTEM database on the runtime's engine (the test-is-the-product plan's M10a).
     */
    public static ExecutionPlan testProgram(TypedQuery query, String runtime, Set<Feature> features,
            ProgramKinds kinds, boolean systemRouted) {
        List<QueryParameters.Declared> declared = query.parameters();
        return new ExecutionPlan(declarations(declared, query.context()),
                program(query, runtime, slots(declared), features, ProgramState.Door.TEST, kinds,
                        systemRouted));
    }

    /** Each declared parameter as the value its statement binds: one value, an optional one's absence, or a list as
     *  one array ({@link QueryParameters.Declared#slot}). */
    private static List<SqlExpr.PlanParam> slots(List<QueryParameters.Declared> declared) {
        List<SqlExpr.PlanParam> slots = new ArrayList<>(declared.size());
        for (QueryParameters.Declared p : declared) {
            slots.add(p.slot());
        }
        return slots;
    }

    /**
     * A program that changes a store, planned as the steps of one {@link ExecutionPlan.Sequence}
     * (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7): each statement at its turn with the lets before it, as the
     * execution's body compiler takes them, every one inlined, resolved and lowered as a query is
     * (the product's statement road, {@link ProductStatements}), each declared parameter a value its statement binds. A staged let
     * ({@code let s = rows->stage()}) creates its temporary table, which the statements after read, and is dropped
     * after the result; another let rides the lets of the statements after; a write into a store table is an update;
     * any other statement before the last a check (its rows dropped: a Cypher write's guard raises). The last
     * statement's value is the plan's answer: a query's typed rows, or a write's count, which the database answers as a
     * value bound in a query of its own.
     */
    private static ExecutionPlan.Node program(TypedQuery query, @Nullable String runtime,
            List<SqlExpr.PlanParam> slots, Set<Feature> features,
            ProgramState.Door door, ProgramKinds kinds, boolean systemRouted) {
        Decided decided = decide(query.context(), runtime);
        // a product program reads no system metamodel table (Class.all() inside a write program is not designed); the
        // test door's target carries no setup (R17 C6), and names the graph's SYSTEM database where the body reads the
        // metamodel store (M10a: the system tables keep their names in process; the program's own seed step writes
        // them, once per graph, below)
        ExecutionPlan.Target target = door == ProgramState.Door.TEST
                ? new ExecutionPlan.Target(systemRouted
                        ? new ExecutionPlan.Database.System(decided.target().type()) : database(decided.target()),
                        decided.dialect().sessionSetup(), List.of())
                : target(decided.target(), Objects.requireNonNull(runtime), query.context(), decided.dialect(),
                        Metamodel.NONE);
        List<TypedSpec> body = query.body();
        StagedRows.refuseLetsTheWritesChange(body, query.specs());
        // ONE PROGRAM STATE (B12 item 2), its one minter for the program's objects (B9, 2026-10-31): a let's
        // construction read by two statements is one object in both, and every statement names its sites by the one
        // counter
        ProgramState state = new ProgramState(query, Objects.requireNonNull(runtime), decided.dialect(), target,
                slots, features);
        // the let kinds' effect scan, memoized over the program (LetPlanner.kind)
        Map<FunctionId, Boolean> effectMemo = new HashMap<>();
        // the graph's system store, seeded by the plan's own step ahead of the first statement that reads it (the
        // walker-deletion program, 2026-11-03: planning sends nothing) -- the program's own target, or the system
        // database beside it (per-statement routing); the runner runs it once per system session
        boolean systemSeeded = false;
        if (target.database() instanceof ExecutionPlan.Database.System) {
            state.add(SystemRoutes.seeds(query.context(), target, decided.dialect()));
            systemSeeded = true;
        } else {
            // a step planned ahead of the program that reads the store -- an assert's side, its message, a branch's
            // (2026-11-05, planning sends nothing: the side's route had seeded the store while the side was planned)
            ExecutionPlan.Target ahead = systemTargetOf(kinds.plannedAhead());
            if (ahead != null) {
                state.add(SystemRoutes.seeds(query.context(), ahead, decided.dialect()));
                systemSeeded = true;
            }
        }
        for (int i = 0; i < body.size(); i++) {
            TypedSpec stmt = body.get(i);
            boolean last = i == body.size() - 1;
            if (kinds.plan(state, i, stmt, last)) {
                // a test's kind, the test-planning library's (the test door only)
                continue;
            }
            if (stmt instanceof TypedLet let && !last) {
                planLet(state, body, i, let, effectMemo);
            } else {
                ExecutionPlan.Target on = kinds.targetOf(state, i, stmt);
                if (!systemSeeded && on != null && on.database() instanceof ExecutionPlan.Database.System) {
                    state.add(SystemRoutes.seeds(query.context(), on, decided.dialect()));
                    systemSeeded = true;
                }
                planStatement(state, i, stmt, last, door, on);
            }
        }
        List<ExecutionPlan.Node> steps = finished(state);
        if (!systemSeeded && systemTargetOf(steps) != null) {
            throw new IllegalStateException("a step of the program reads the graph's system store and no seed step is"
                    + " ahead of it (the program's planning seeds the store before the first step that reads it)");
        }
        return new ExecutionPlan.Sequence(steps);
    }

    /** The graph's system database's target, where a statement of {@code steps} names it; else null. */
    private static ExecutionPlan.@Nullable Target systemTargetOf(List<ExecutionPlan.Node> steps) {
        for (ExecutionPlan.Node step : steps) {
            for (ExecutionPlan.Sql sql : sqls(step)) {
                if (sql.target().database() instanceof ExecutionPlan.Database.System) {
                    return sql.target();
                }
            }
        }
        return null;
    }

    /** Where a plan runs: the runtime's database ({@code Compiler.executesOn}: a null runtime refused there, by name)
     *  and its dialect. */
    private record Decided(Target target, SqlDialect dialect) {
    }

    /** {@link Decided} for {@code runtime} of {@code ctx}. */
    private static Decided decide(ModelContext ctx, @Nullable String runtime) {
        Target decided = Compiler.executesOn(ctx, runtime);
        return new Decided(decided, Databases.dialect(decided.type()));
    }

    /** A let before the program's last statement, planned by its KIND (the planner's {@link LetPlanner}, the walk's
     *  own rule): where its rows live. */
    private static void planLet(ProgramState state, List<TypedSpec> body, int i, TypedLet let,
            Map<FunctionId, Boolean> effectMemo) {
        switch (LetPlanner.kind(let, body, i, state.query().specs(), effectMemo, state.frames()::relationFrame)) {
            // $r.values(->at(0)) over a relation frame: the same frame
            case ALIAS -> state.frames().alias(let.name(), Objects.requireNonNull(
                    LetPlanner.aliasOf(let.value(), state.frames()::relationFrame)));
            case FRAME -> throw new IllegalStateException("a let-bound execute(...) reached the planner: the"
                    + " query door refuses it, the test door's library plans it");
            case STAGED_GRID -> throw new NotImplementedException("a raw read bound by a let"
                    + " and read after a later effect: its rows held at the let's turn are the T12 kind the"
                    + " test-is-the-product plan's M9 plans");
            case VALUE -> state.let(let);
            case STAGED -> stagedLet(state, i, let);
        }
    }

    /** STAGED ROWS: a let's rows computed at its statement's turn into a temporary table of the session, which the
     *  statements after read by name (the writes between read the rows as they were). */
    private static void stagedLet(ProgramState state, int i, TypedLet let) {
        TypedQuery query = state.query();
        ModelContext ctx = query.context();
        TypedSpec rows = Objects.requireNonNull(StagedRows.rowsOf(let));
        String table = StagedRows.table(let.name());
        // the product's statement road (M2, B11): the test road's front half, the product's lowering
        ProductStatements.Statement st = ProductStatements.prepare(query, with(state.lets(), rows), state.runtime(), i,
                state.features(), state.frames());
        Prepared.Planned sp = ProductStatements.planned(st);
        // a relation read more than once written once, but where the engine's CTE extraction names the statement's
        // subqueries itself (as ProductStatements.lower: the third review's finding 6, 2026-11-07)
        Lowerer lowerer = ProductStatements.lowerer(query, sp, st.temporal(), false,
                withStamps(state.slots(), state.frames()), state.sites())
                .sharingRelations(!sp.env().postProcessors().extractCtes());
        SqlQuery q = FrameCtes.attach(EnumValueTables.apply(lowerer.namedColumns(
                ProductStatements.postProcessed(lowerer.lower(ProductStatements.seeded(sp.body(), sp.env(), ctx)),
                        sp.env()))), st.reads().definitions());
        TypedSpec rowsRoot = sp.body().get(sp.body().size() - 1);
        ExecutionPlan.Sql stagedSql = sql(state.dialect().renderStatement(SqlDml.CreateTempTableAs
                .staged(table, q, StagedRows.columns(rowsRoot.info()))), state.target(), null);
        String own = st.reads().ownStamp();
        if (own != null && reads(stagedSql, own)) {
            // a write is sent with no trace comment (the M3 audit's B7c)
            throw new NotImplementedException("a staged let's rows read an inline"
                    + " execute(...)'s activity comment: a write is sent with no trace comment to record");
        }
        state.add(new ExecutionPlan.Update(stagedSql, null));
        state.stage(table);
        state.let(StagedRows.boundTo(let.name(), table, rows.info(),
                StagedRows.storeOf(sp.body(), ctx, state.runtime()), let.info()));
    }

    /** A statement of the program (any but a let before the last): a value the program wrote (a constant, the answer
     *  when last), an orchestration handle (no step; the test door answers a last one as no value), a store write (an
     *  update, its count the answer when last), or a query (a check, its rows dropped; the typed answer when last). */
    private static void planStatement(ProgramState state, int i, TypedSpec stmt, boolean last,
            ProgramState.Door door, ExecutionPlan.@Nullable Target routed) {
        TypedQuery query = state.query();
        SqlDialect dialect = state.dialect();
        ExecutionPlan.Target target = routed != null ? routed : state.target();
        ProductStatements.Statement st = ProductStatements.prepare(query, with(state.lets(), stmt), state.runtime(), i,
                state.features(), state.frames(), Map.of(), last && door == ProgramState.Door.TEST);
        ExecutionPlan.Constant constant = ProductStatements.constant(st);
        if (constant != null) {
            // a value the program wrote: the answer where it is the last statement (a test's as the walk answers
            // it, M5); before it, nothing to run
            if (last) {
                state.add(door == ProgramState.Door.TEST ? ProductStatements.asATestAnswers(constant) : constant);
            }
            return;
        }
        if (last && door == ProgramState.Door.TEST && ProductStatements.handle(st)) {
            // a test's program ending on a handle (a setup that returns its runtime): answered as the walk answers
            // it, no value, which no judge compares; the query door refuses a handle as an answer (planned())
            state.add(ProductStatements.asATestAnswers(ProductStatements.scalar(ProductStatements.handleType(st),
                    null)));
            return;
        }
        if (!last && ProductStatements.handle(st)) {
            // an orchestration handle (a connection, a runtime) before the last statement: nothing to run
            return;
        }
        if (st.prepared() instanceof Prepared.StoreWrite w) {
            SqlDml dml = ProductStatements.storeWrite(query, w, st.temporal(), state.slots(), state.sites());
            state.add(new ExecutionPlan.Update(sql(dialect.renderStatement(dml), target, null),
                    last ? ExecutionPlan.COUNT : null));
            if (last) {
                state.add(count(query, w.write(), dialect, target, state.features(), state.sites()));
            }
            return;
        }
        if (door == ProgramState.Door.TEST) {
            unplannedOnTheTestDoor(st);
        }
        Prepared.Planned p = ProductStatements.planned(st);
        Staged staged = stagedShared(dialect, state.lowered(st, last), target, state.staged().size());
        for (String table : staged.tables()) {
            state.add(new ExecutionPlan.Effect(dropping(dialect, table, target)));
            state.add(new ExecutionPlan.Effect(staging(dialect, staged, table, target)));
            state.stage(table);
        }
        SqlQuery q = staged.statement();
        TypedSpec answer = p.body().get(p.body().size() - 1);
        state.add(stampedIfRead(last ? typed(answer, q, dialect, target)
                : new ExecutionPlan.Check(sql(dialect.renderStatement(q), target, q)), st.reads().ownStamp()));
    }

    /**
     * SHARED RELATIONS STAGED (docs/TS_DECISIONS_2026_11_07.md item 9b, decision 16 (b), 2026-11-07): where the
     * database writes each read of a common table expression again ({@link SqlDialect#writesEachReadOfAWithAgain}:
     * ClickHouse), each relation {@code q} reads more than once -- the lowering's shared ones, {@code __let<n>} at its
     * head ({@code lowering.SharedRelations}) -- is computed once, in the WITH's order, into a temporary table of the
     * session (named {@code __stage<n>__let<m>} from {@code first}, so a program's statements never share one), and
     * the statement and the relations after read it by name. Elsewhere {@code q} as it is, and no table.
     */
    static Staged stagedShared(SqlDialect dialect, SqlQuery q, ExecutionPlan.Target target, int first) {
        if (!dialect.writesEachReadOfAWithAgain() || !(q instanceof SqlWith w)
                || w.ctes().stream().noneMatch(c -> c.name().startsWith(SqlWith.SHARED_PREFIX))) {
            return new Staged(q, List.of(), Map.of());
        }
        Map<String, String> tables = new HashMap<>();
        List<String> order = new ArrayList<>();
        Map<String, SqlQuery> bodies = new LinkedHashMap<>();
        List<SqlWith.Cte> kept = new ArrayList<>();
        for (SqlWith.Cte c : w.ctes()) {
            SqlQuery body = readingTables(c.query(), tables);
            if (!c.name().startsWith(SqlWith.SHARED_PREFIX)) {
                kept.add(new SqlWith.Cte(c.name(), body, c.materialized()));
                continue;
            }
            String table = "__stage" + (first + order.size()) + c.name();
            order.add(table);
            bodies.put(table, body);
            tables.put(c.name(), table);
        }
        SqlQuery body = readingTables(w.body(), tables);
        return new Staged(kept.isEmpty() ? body : w.with(kept, body), List.copyOf(order),
                java.util.Collections.unmodifiableMap(bodies));
    }

    /** A statement with its shared relations staged: the statement reading the tables, the tables in the order they
     *  are made, and each one's query. */
    record Staged(SqlQuery statement, List<String> tables, Map<String, SqlQuery> queries) {
    }

    /** The statement that drops {@code table} where it is ({@code DROP TABLE IF EXISTS}). */
    static ExecutionPlan.Sql dropping(SqlDialect dialect, String table, ExecutionPlan.Target target) {
        return new ExecutionPlan.Sql(dialect.render(new SqlDdl.DropTable(null, table, NameQuoting.BARE)), List.of(),
                target, null, List.of());
    }

    /** The statement that makes {@code table} of {@code staged}: a temporary table of the session holding its rows. */
    static ExecutionPlan.Sql staging(SqlDialect dialect, Staged staged, String table, ExecutionPlan.Target target) {
        return sql(dialect.renderStatement(new SqlDml.CreateTempTableAs(table,
                Objects.requireNonNull(staged.queries().get(table), table))), target, null);
    }

    /** {@code q}, each read of a staged relation ({@code tables}' keys) a read of its table. */
    private static SqlQuery readingTables(SqlQuery q, Map<String, String> tables) {
        if (tables.isEmpty()) {
            return q;
        }
        return new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Cte c && tables.containsKey(c.name())) {
                    return new SqlSource.Table(tables.get(c.name()), c.alias(), c.outputs(), false, Map.of(),
                            NameQuoting.BARE);
                }
                return s;
            }
        }.rewrite(q);
    }

    /** The graph's SYSTEM database beside {@code home} (a program's own target): a statement reading the system
     *  metamodel store alone runs there (per-statement routing, 2026-11-03) -- the engine and the session statements
     *  {@code home}'s, as the system database is the runtime's engine in this process. */
    public static ExecutionPlan.Target systemBeside(ExecutionPlan.Target home) {
        ConnectionDefinition.DatabaseType type = switch (home.database()) {
            case ExecutionPlan.Database.Declared d -> d.connection().databaseType();
            case ExecutionPlan.Database.Platform p -> p.type();
            case ExecutionPlan.Database.System s -> s.type();
        };
        return new ExecutionPlan.Target(new ExecutionPlan.Database.System(type), home.session(), List.of());
    }

    /** The program's steps, finished: a frame's run binds its stamp only where a later statement reads it (the M3
     *  audit's B7a: an unread stamp would still be minted, and refused on a session whose statements carry no
     *  comment), and the staged tables dropped after the answer (a failed program's transaction takes them back with
     *  its writes where the dialect drops them with it). */
    private static List<ExecutionPlan.Node> finished(ProgramState state) {
        List<ExecutionPlan.Node> steps = withoutUnreadStamps(state.steps());
        everyReadStampBound(steps);
        SqlDialect dialect = state.dialect();
        for (String table : dialect.dropsStagedTablesWithTheirTransaction() ? List.<String>of() : state.staged()) {
            steps.add(new ExecutionPlan.Update(new ExecutionPlan.Sql(dialect.render(new SqlDdl.DropTable(
                    null, table, NameQuoting.BARE)), List.of(), state.target(), null, List.of()), null));
        }
        return steps;
    }

    /** {@code statement} after {@code lets}. */
    private static List<TypedSpec> with(
            List<TypedSpec> lets, TypedSpec statement) {
        List<TypedSpec> out = new ArrayList<>(lets);
        out.add(statement);
        return out;
    }

    /** The answer of a program whose last statement is the write {@code write}: its count, which the step before
     *  binds ({@link ExecutionPlan#COUNT}), as an {@code Integer[1]} value: one row, one column {@code value}. */
    private static ExecutionPlan.Node count(TypedQuery query, TypedSpec write,
            SqlDialect dialect, ExecutionPlan.Target target, Set<Feature> features,
            ObjectSites sites) {
        ExprType type = new ExprType(write.info().type(), Multiplicity.Bounded.ONE);
        QueryParameters.Declared count = new QueryParameters.Declared(ExecutionPlan.COUNT, type.type(),
                type.multiplicity());
        SqlQuery q = query.lowerer(false, false, List.of(count.slot()), features, sites)
                .lowerPlanParam(ExecutionPlan.COUNT, type);
        return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.VALUE,
                PlanColumns.of(TypedRows.schema(type), q, ExecutionPlan.TdsResult.Shape.VALUE),
                sql(q, dialect, target));
    }

    /**
     * The plan's TYPED answer to {@code rootSpec}, lowered to {@code lowered} (the result boundary program's step 1,
     * docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md §5): the statement as the query's own, its rows read by the session
     * from the driver by the plan's columns and their three facts, no output text in it. A relation answers its rows;
     * a value its elements in the one column {@code value}; a graph fetch its objects, one per row, each the object's
     * JSON as the database built it (the streaming graph root). A sort key that is no output rides as a hidden column
     * ({@link TypedRows#statement}). Two answers are KEYED, their columns the data's (the test-is-the-product plan's
     * M2, B11 step 7, D29: the test road's rule, now the product's for a query and a program's last statement alike):
     * a pivot no cast names (its keys read first, the probe plan of 2026-10-20,
     * {@link com.legend.lowering.KeyedPivot}), and a statement that reads a raw grid's report ({@link #rowsOrGridKeyed}).
     */
    private static ExecutionPlan.Node typed(TypedSpec rootSpec, SqlQuery lowered,
            SqlDialect dialect, ExecutionPlan.Target target) {
        ExprType root = rootSpec.info();
        return switch (ResultShape.of(rootSpec)) {
            case GRAPH -> {
                List<OutputCol> outs = lowered.outputs();
                if (outs.size() != 1) {
                    throw new IllegalStateException("a graph fetch's statement answers " + outs.size() + " columns "
                            + outs.stream().map(OutputCol::name).toList()
                            + ": the streaming graph root answers one object per row");
                }
                yield new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.OBJECTS, List.of(
                        new ExecutionPlan.Column(outs.get(0).name(), root.type().typeName(),
                                PlanColumns.multiplicity(root.multiplicity()),
                                ColumnFacts.slot(outs.get(0).type()), null,
                                ColumnFacts.precision(root.type().typeName(),
                                        outs.get(0).type(), ExecutionPlan.TdsResult.Shape.OBJECTS))),
                        sql(lowered, dialect, target));
            }
            case TABULAR -> {
                // a relation root's schema with any row-struct column flattened (TypedRows.flat; its dynamic columns
                // kept)
                Type.RelationType rowSchema = TypedRows.schema(root);
                SqlSource.Pivot dynamic = KeyedPivot.dynamic(lowered);
                if (dynamic != null && !rowSchema.dynamicColumns().isEmpty()) {
                    yield keyed(lowered, dynamic, rowSchema, dialect, target);
                }
                yield rowsOrGridKeyed(ExecutionPlan.TdsResult.Shape.RELATION, PlanColumns.of(
                        rowSchema, lowered, ExecutionPlan.TdsResult.Shape.RELATION), TypedRows.statement(lowered),
                        dialect, target);
            }
            // a value's one column is the value's: through a class's chain the map binder's cell carries it, typed by
            // the value and the root's multiplicity (ResultShape.valueInfo, as the test road answers it), never the
            // binder's own to-one column (the test-is-the-product plan's §9 item 3; the user, 2026-11-01: "match
            // upstream engine/pure"; the engine answers the value's rows, upstream-api/m5-answers.json)
            case SCALAR, COLLECTION -> rowsOrGridKeyed(ExecutionPlan.TdsResult.Shape.VALUE,
                    PlanColumns.of(TypedRows.schema(ResultShape.valueInfo(root)), lowered,
                            ExecutionPlan.TdsResult.Shape.VALUE),
                    TypedRows.statement(lowered), dialect, target);
        };
    }

    /** A rendered statement as the plan's, on {@code target}. */
    static ExecutionPlan.Sql sql(RenderedStatement rendered, ExecutionPlan.Target target,
            @Nullable SqlQuery tree) {
        List<ExecutionPlan.Slot> slots = rendered.binds().stream()
                .map(b -> new ExecutionPlan.Slot(b.parameter(), binding(b.binding()))).toList();
        List<ExecutionPlan.Region> regions = rendered.regions().stream()
                .map(r -> new ExecutionPlan.Region(r.start(), r.end(), r.lead(), r.separator(), r.empty(),
                        r.firstBind(), r.bindCount(), r.counters())).toList();
        return new ExecutionPlan.Sql(rendered.sql(), slots, target, tree, regions);
    }

    /**
     * A dynamic pivot's probe plan ({@link com.legend.lowering.KeyedPivot}, 2026-10-20): the keys probe, the body with
     * its per-key regions, the outputs with their three facts (a fixed column's from the typed schema and the lowered
     * query, a per-key one's from its aggregate's template and slot), the database's snapshot and column limit; its
     * answer typed rows.
     */
    public static ExecutionPlan.Keyed keyed(SqlQuery lowered, SqlSource.Pivot pivot, Type.RelationType schema,
            SqlDialect dialect, ExecutionPlan.Target target) {
        // the database's snapshot first: a database with no probe plan (MongoDB) refuses by its own name before any
        // statement is written
        SqlDialect.KeysSnapshot keysSnapshot = dialect.keysSnapshot();
        KeyedPivot.Parts parts = KeyedPivot.of(lowered, pivot, schema);
        Map<String, ExecutionPlan.Column> facts = new HashMap<>();
        for (ExecutionPlan.Column c : PlanColumns.of(schema, lowered,
                ExecutionPlan.TdsResult.Shape.RELATION)) {
            facts.put(c.name(), c);
        }
        // each statement's cells by the names it writes them under (the body's outputs in the outputs' order)
        List<OutputCol> bodyFields = parts.body().outputs();
        if (bodyFields.size() != parts.outputs().size()) {
            throw new IllegalStateException("a keyed body answers " + bodyFields.size() + " columns for "
                    + parts.outputs().size() + " outputs");
        }
        List<ExecutionPlan.Keyed.Output> outputs = new ArrayList<>();
        for (int i = 0; i < parts.outputs().size(); i++) {
            String field = bodyFields.get(i).name();
            outputs.add(switch (parts.outputs().get(i)) {
                case KeyedPivot.Fixed f -> new ExecutionPlan.Keyed.Fixed(Objects
                        .requireNonNull(facts.get(f.column().name()), () -> "the fixed column '" + f.column().name()
                                + "' has no facts in the typed schema " + facts.keySet()), field);
                case KeyedPivot.PerKey k -> new ExecutionPlan.Keyed.PerKey(
                        new ExecutionPlan.Column(k.template().name(), k.template().type().typeName(),
                                PlanColumns.multiplicity(k.template().multiplicity()),
                                ColumnFacts.slot(k.slot()), null,
                                ColumnFacts.precision(k.template().type().typeName(),
                                        k.slot(), ExecutionPlan.TdsResult.Shape.RELATION)),
                        k.nameCell(), field);
            });
        }
        List<String> keyFields = parts.keys().outputs().stream().map(OutputCol::name).toList();
        return new ExecutionPlan.Keyed(sql(parts.keys(), dialect, target), keyFields, outputs, parts.perKeyAt(),
                sql(parts.body(), dialect, target), snapshotOf(keysSnapshot), dialect.resultColumnLimit());
    }

    /** The plan's snapshot for a keyed result's two reads, as its database declares it ({@link
     *  SqlDialect#keysSnapshot}). */
    private static ExecutionPlan.Keyed.Snapshot snapshotOf(SqlDialect.KeysSnapshot keysSnapshot) {
        return switch (keysSnapshot) {
            case TRANSACTION -> ExecutionPlan.Keyed.Snapshot.TRANSACTION;
            case REPEATABLE_READ -> ExecutionPlan.Keyed.Snapshot.REPEATABLE_READ;
            case SERIALIZABLE -> ExecutionPlan.Keyed.Snapshot.SERIALIZABLE;
            case NONE -> ExecutionPlan.Keyed.Snapshot.NONE;
        };
    }

    /**
     * A typed result of {@code columns} over {@code statement} -- or, where the statement holds a RAW GRID's report
     * ({@link com.legend.sql.SqlSource.GridReport}: its column names or its cells, the planner-driver seam program's
     * S3c, 2026-10-30), the KEYED result whose keys are the grid's reported columns ({@code KeysOf.COLUMNS}), read when
     * the plan runs by a {@code LIMIT 0} of the grid, its body the statement written with a region per column. The
     * kinds its cells may be in: the cells' JSON keeps an integer's, a text's, a boolean's and a double's; the
     * database's text is the engine's for an integer and a text; any other column is refused when the plan runs, by
     * name. One grid per statement (two are refused by name: one keys step).
     */
    public static ExecutionPlan.Node rowsOrGridKeyed(ExecutionPlan.TdsResult.Shape shape,
            List<ExecutionPlan.Column> columns, SqlQuery statement, SqlDialect dialect, ExecutionPlan.Target target) {
        List<SqlSource.GridReport> reports = gridReports(statement);
        if (reports.isEmpty()) {
            return new ExecutionPlan.TdsResult(shape, columns, sql(statement, dialect, target));
        }
        // the database's snapshot first, as the pivot's keyed result takes it: a database with no keyed result
        // refuses by its own name before any statement is written
        SqlDialect.KeysSnapshot keysSnapshot = dialect.keysSnapshot();
        SqlSource.RawSql grid = rawGridOf(reports.get(0));
        boolean texts = false;
        boolean cells = false;
        for (SqlSource.GridReport r : reports) {
            SqlSource.RawSql each = rawGridOf(r);
            if (!each.sql().equals(grid.sql()) || !each.writtenFor().equals(grid.writtenFor())) {
                throw new NotImplementedException("one statement reads two raw grids' reports: a"
                        + " keyed result reads one grid's columns");
            }
            texts |= r.part() == SqlSource.GridReport.Part.TEXTS;
            cells |= r.part() == SqlSource.GridReport.Part.CELLS
                    || r.part() == SqlSource.GridReport.Part.ROWS;
        }
        List<String> admitted = texts ? List.of("INTEGER", "BIGINT", "VARCHAR")
                : cells ? List.of("INTEGER", "BIGINT", "VARCHAR", "BOOLEAN", "DOUBLE") : List.of();
        SqlQuery keys = SqlSelect.starOf(
                new SqlSource.Subselect(reports.get(0).grid(), "grid", null)).withLimit(0L);
        List<ExecutionPlan.Keyed.Output> outputs = new ArrayList<>(columns.size());
        for (ExecutionPlan.Column c : columns) {
            outputs.add(new ExecutionPlan.Keyed.Fixed(c, c.name()));
        }
        return new ExecutionPlan.Keyed(sql(keys, dialect, target), ExecutionPlan.Keyed.KeysOf.COLUMNS, admitted,
                List.of("name"), outputs, outputs.size(), sql(statement, dialect, target),
                snapshotOf(keysSnapshot), dialect.resultColumnLimit(), shape);
    }

    /** The raw grid a report reads: its grid is a plain read of one authored text (the lowering's raw grid, or its
     *  rows as their cells over it); a report over anything else is refused by name, its grid's identity being that
     *  text and the language it is written in. */
    private static SqlSource.RawSql rawGridOf(SqlSource.GridReport report) {
        SqlQuery q = report.grid();
        while (q instanceof SqlSelect s && s.projections().isEmpty() && s.where() == null
                && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null && s.orderBy().isEmpty()
                && s.limit() == null && s.offset() == null && !s.distinct()) {
            switch (s.from()) {
                case SqlSource.RawSql raw -> {
                    return raw;
                }
                case SqlSource.GridReport rows when rows.part()
                        == SqlSource.GridReport.Part.ROWS -> q = rows.grid();
                default -> q = null;
            }
        }
        throw new NotImplementedException("a raw grid's report over a relation derived from it (a"
                + " filter, a projection, a sort): a keyed result reads the columns of the grid as it is written");
    }

    /** Whether {@code statement} reads a raw grid's report (its plan is keyed, {@link #rowsOrGridKeyed}). */
    public static boolean readsGridReport(SqlQuery statement) {
        return !gridReports(statement).isEmpty();
    }

    private static List<SqlSource.GridReport> gridReports(SqlQuery statement) {
        List<SqlSource.GridReport> reports = new ArrayList<>();
        new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.GridReport g) {
                    reports.add(g);
                }
                return s;
            }
        }.rewrite(statement);
        return reports;
    }

    /** {@code statement} final for {@code target}: rendered in {@code dialect}, each placeholder's binding as the
     *  dialect decided it, the typed tree kept (the judges' plans take it too, JudgePlans, step 6). */
    public static ExecutionPlan.Sql sql(SqlQuery statement, SqlDialect dialect, ExecutionPlan.Target target) {
        return sql(dialect.renderStatement(statement), target, statement);
    }

    /** A placeholder's binding, as the dialect decided it, in the plan's own terms. */
    private static ExecutionPlan.Binding binding(RenderedStatement.Binding b) {
        return switch (b) {
            case RenderedStatement.Binding.One one -> {
                RenderedStatement.TypeHole hole = one.hole();
                yield new ExecutionPlan.Binding.One(one.nullType(), hole == null ? null
                        : new ExecutionPlan.TypeHole(hole.at(), spellings(hole.types()), kind(hole.absent())));
            }
            case RenderedStatement.Binding.Array array -> new ExecutionPlan.Binding.Array(array.elementSqlType());
            case RenderedStatement.Binding.Key key -> new ExecutionPlan.Binding.Key(key.cell());
        };
    }

    private static Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> spellings(
            Map<ValueKind, RenderedStatement.TypeSpelling> types) {
        Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> out =
                new EnumMap<>(ExecutionPlan.ValueKind.class);
        types.forEach((k, t) -> out.put(kind(k), new ExecutionPlan.TypeSpelling(t.name(), switch (t.digits()) {
            case NONE -> ExecutionPlan.Digits.NONE;
            case PRECISION -> ExecutionPlan.Digits.PRECISION;
            case PRECISION_AND_SCALE -> ExecutionPlan.Digits.PRECISION_AND_SCALE;
        }, t.fraction())));
        return out;
    }

    private static ExecutionPlan.ValueKind kind(ValueKind k) {
        return switch (k) {
            case INTEGER -> ExecutionPlan.ValueKind.INTEGER;
            case DECIMAL -> ExecutionPlan.ValueKind.DECIMAL;
            case FLOATING -> ExecutionPlan.ValueKind.FLOATING;
            case DATE -> ExecutionPlan.ValueKind.DATE;
            case DATE_TIME -> ExecutionPlan.ValueKind.DATE_TIME;
            case DATE_TIME_NANOS -> ExecutionPlan.ValueKind.DATE_TIME_NANOS;
        };
    }

    /** Where the plan runs: the database {@code decided}, the session statements of its dialect,
     *  and the setup its runtime's connections declare, every statement final for that database. */
    private static ExecutionPlan.Target target(Target decided, String runtime,
            ModelContext ctx, SqlDialect dialect,
            Metamodel metamodel) {
        ExecutionPlan.Database database = database(decided);
        List<ExecutionPlan.SetupStep> steps = new ArrayList<>(
                setup(CsvSeed.declaredSteps(runtime, ctx, dialect), dialect));
        steps.addAll(metamodel.setup());
        List<String> session = new ArrayList<>(dialect.sessionSetup());
        session.addAll(metamodel.session());
        return new ExecutionPlan.Target(database, session, steps);
    }

    /** The database the compiler {@code decided}: the platform's own engine, or the declared connection's. */
    private static ExecutionPlan.Database database(Target decided) {
        return switch (decided) {
            case Target.Platform p -> new ExecutionPlan.Database.Platform(p.type());
            case Target.Declared d -> new ExecutionPlan.Database.Declared(oneDefinition(d));
        };
    }

    /**
     * The system metamodel store's tables a plan carries, in the form its database takes: {@code setup}, the target's
     * setup steps (an engine in this process); {@code session}, statements of the session's own, run on every session
     * the plan opens (a server engine); {@code renames}, the query's references to read a session table by its name;
     * {@code inline}, on a database with no temporary table, each table referenced as the query's own rows (the
     * relation for an alias).
     */
    record Metamodel(List<ExecutionPlan.SetupStep> setup, List<String> session, Map<String, String> renames,
            Map<String, Function<String, SqlSource>> inline) {
        static final Metamodel NONE = new Metamodel(List.of(), List.of(), Map.of(), Map.of());

        /** {@code q} reading the tables as they are held: a session table by its name, an inline table as its rows. */
        SqlQuery read(SqlQuery q) {
            SqlQuery renamed = SqlPostProcessors.apply(q, renames);
            if (inline.isEmpty()) {
                return renamed;
            }
            return new SqlRewriter() {
                @Override
                protected SqlSource source(SqlSource s) {
                    if (s instanceof SqlSource.Table t && inline.containsKey(t.name())) {
                        return inline.get(t.name()).apply(t.alias());
                    }
                    return s;
                }
            }.rewrite(renamed);
        }
    }

    /**
     * The system metamodel store's tables a query reads ({@code Class.all()}: the typed tree's references to
     * {@link com.legend.builtin.SystemMetamodel#STORE_FQN}), carried by the plan as the legacy route seeded them on
     * the graph's own system database ({@code SystemDatabase}; the plan path program's step 9): each referenced
     * table with the graph's facts ({@link MetamodelSeeds}, written at plan time), so a model-free runner answers the
     * query on the plan's session. On an engine in this process (2026-10-18) the tables are the target's setup, in
     * their schema, as the private instance held them. On a SERVER engine (2026-10-19; the user's design of
     * 2026-10-11 for the legacy route) they are the session's own TEMPORARY tables, made by the session's statements
     * on every session the plan opens and gone with it (no other session sees them, nothing permanent is made in the
     * user's database, so a plan with them runs on a database reached by a URL), each named
     * {@link com.legend.setup.Ddl#systemTable} and read by that name through the query's renames; on a server with no
     * temporary table (Trino, MongoDB; 2026-10-20) each is the query's own rows, inline, each cell its column's typed
     * literal. A query reading no such table adds nothing; a table of no rows is created empty (inline: a relation of
     * its columns with no row).
     */
    private static Metamodel metamodel(Compiler.LoweredQuery l, SqlDialect dialect) {
        Set<String> tables = new java.util.TreeSet<>();
        collectSystemTables(l.root(), tables);
        if (tables.isEmpty()) {
            return Metamodel.NONE;
        }
        DatabaseDefinition store = l.ctx()
                .findDatabase(SystemMetamodel.STORE_FQN)
                .orElseThrow(() -> new IllegalStateException("the system metamodel store is not in the model"));
        boolean inProcess = dialect.inProcess();
        List<ExecutionPlan.SetupStep> setup = new ArrayList<>();
        List<String> session = new ArrayList<>();
        Map<String, String> renames = new LinkedHashMap<>();
        Map<String, Function<String, SqlSource>> inline =
                new LinkedHashMap<>();
        for (DatabaseDefinition.SchemaDefinition schema : store.schemas()) {
            for (DatabaseDefinition.TableDefinition def : schema.tables()) {
                // a reference names its table schema-qualified (metamodel.classes)
                if (!tables.contains(schema.name() + "." + def.name()) && !tables.contains(def.name())) {
                    continue;
                }
                List<List<String>> rows = MetamodelSeeds.rows(def.name(), l.ctx());
                if (inProcess) {
                    setup.addAll(Ddl.metamodelSteps(def, schema.name(), rows, dialect));
                } else if (!dialect.temporaryTables()) {
                    inline.put(schema.name() + "." + def.name(), inlineRows(def, rows));
                } else {
                    session.addAll(Ddl.metamodelTemporary(def, schema.name(), rows, dialect));
                    renames.put(schema.name() + "." + def.name(),
                            Ddl.systemTable(schema.name(), def.name()));
                }
            }
        }
        return new Metamodel(List.copyOf(setup), List.copyOf(session),
                Collections.unmodifiableMap(new LinkedHashMap<>(renames)),
                Collections.unmodifiableMap(new LinkedHashMap<>(inline)));
    }

    /**
     * A system table's rows as a query's own relation, for an alias: each cell its column's typed literal (the graph's
     * facts, plan-time constants: the store declares VARCHAR, INTEGER and BIT), a NULL cast to the column's type; a
     * table of no rows a select of typed NULLs that holds no row.
     */
    private static Function<String, SqlSource> inlineRows(
            DatabaseDefinition.TableDefinition def, List<List<String>> rows) {
        List<String> columns = new ArrayList<>();
        List<SqlType> types = new ArrayList<>();
        List<OutputCol> outputs = new ArrayList<>();
        for (DatabaseDefinition.ColumnDefinition col : def.columns()) {
            SqlType type = switch (col.dataType()) {
                case RelationalDataType.Varchar ignored -> SqlType.Scalar.VARCHAR;
                case RelationalDataType.Integer_ ignored -> SqlType.Scalar.INTEGER;
                case RelationalDataType.Bit ignored -> SqlType.Scalar.BOOLEAN;
                default -> throw new IllegalStateException("system table " + def.name() + "." + col.name()
                        + ": no literal for its type " + col.dataType() + " (the store declares VARCHAR, INTEGER and"
                        + " BIT)");
            };
            columns.add(col.name());
            types.add(type);
            outputs.add(new OutputCol(col.name(), type, !col.notNull() && !col.primaryKey()));
        }
        List<List<SqlExpr>> cells = new ArrayList<>();
        for (List<String> row : rows.isEmpty() ? List.of(Collections.<String>nCopies(columns.size(), null))
                : rows) {
            List<SqlExpr> out = new ArrayList<>();
            for (int c = 0; c < row.size(); c++) {
                out.add(literal(def.name() + "." + columns.get(c), row.get(c), types.get(c)));
            }
            cells.add(out);
        }
        List<List<SqlExpr>> values = List.copyOf(cells);
        if (!rows.isEmpty()) {
            return alias -> new SqlSource.Values(values, columns, alias, outputs);
        }
        return alias -> new SqlSource.Subselect(SqlSelect.starOf(
                new SqlSource.Values(values, columns, alias + "_none", outputs))
                .withWhere(new SqlExpr.BoolLit(false)), alias, null);
    }

    /** A system table's cell as its column's typed literal; a NULL cast to the type. */
    private static SqlExpr literal(String column, @Nullable String text,
            SqlType type) {
        if (text == null) {
            return new SqlExpr.Cast(new SqlExpr.NullLit(), type);
        }
        if (type == SqlType.Scalar.VARCHAR) {
            return new SqlExpr.StringLit(text);
        }
        if (type == SqlType.Scalar.INTEGER) {
            return new SqlExpr.Cast(new SqlExpr.IntLit(Long.parseLong(text)), type);
        }
        return switch (text) {
            case "true" -> new SqlExpr.BoolLit(true);
            case "false" -> new SqlExpr.BoolLit(false);
            default -> throw new IllegalStateException("system table column " + column + ": '" + text + "' is no BIT"
                    + " fact (true or false)");
        };
    }

    private static void collectSystemTables(TypedSpec n, Set<String> out) {
        if (n instanceof TypedTableReference tr
                && tr.store().equals(SystemMetamodel.STORE_FQN)) {
            out.add(tr.table());
        }
        for (TypedSpec c : n.children()) {
            collectSystemTables(c, out);
        }
    }

    /** The one definition a declared target opens: its connections may differ in name, not in database —
     *  a query runs on one session. */
    private static ConnectionDefinition oneDefinition(Target.Declared d) {
        ConnectionDefinition first = d.connections().get(0);
        for (ConnectionDefinition c : d.connections()) {
            if (c.databaseType() != first.databaseType() || !c.specification().equals(first.specification())
                    || !c.authentication().equals(first.authentication())) {
                throw new NotImplementedException("the runtime binds "
                        + d.connections().stream().map(ConnectionDefinition::qualifiedName).toList()
                        + ", different connections: a query runs on one session");
            }
        }
        return first;
    }

    /**
     * A target's setup as the plan's steps, final for {@code type}: each statement of a connection's SQL (written in
     * H2's spelling, as legend-engine's test data is) adapted to the database, and each table's rows either for the
     * database's bulk loader ({@link SqlDialect#loadsRowsInBulk}) with the statements that stage them, or as the one
     * INSERT the database runs. A table of no rows is no step.
     */
    private static List<ExecutionPlan.SetupStep> setup(List<CsvSeed.Step> steps, SqlDialect dialect) {
        // the one owner of the steps' form (the doors, 2026-10-29): the body driver's seeds take the same
        return CsvSeed.planned(steps, dialect).stream().map(CsvSeed.Planned::step).toList();
    }
}
