// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.SystemMetamodel;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Temporal;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedCsvCensus;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRawSqlRelation;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.FunctionId;
import com.legend.model.MappingDefinition;
import com.legend.planner.BodyPlanner;
import com.legend.planner.ConnectionLets;
import com.legend.planner.LetPlanner;
import com.legend.planner.SystemRoutes;
import com.legend.planner.FramePlanner;
import com.legend.planner.ProductStatements;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.planner.QueryDoor;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE CLASSES OF BODIES THAT RUN WHOLE ON THE PROGRAM ROAD (the test-is-the-product plan's R16, brief B1, M5): a body
 * is planned whole by the test door and run as one plan when it falls in a class here, by a named rule over its typed
 * shape -- never by trying the test door and falling back on a failure. A body of a moved class the test door refuses
 * FAILS (R14). The classes are one closed list that only grows ({@code BodyClassTest} pins it); the walk runs every
 * other body until M10 moves the last class.
 */
public enum BodyClass {

    /**
     * An EFFECT PROGRAM (M5, the corpus's setup programs): every statement is one of
     * <ul>
     *   <li>a database effect whose String arguments are literals (each folded over the lets before it), or a let of
     *       one that later statements read only as a connection;</li>
     *   <li>an orchestration handle (a connection, a runtime: {@code BodyPlanner.handleType}), a let of one or its
     *       value;</li>
     *   <li>a String or Boolean literal, a let of one or its value;</li>
     *   <li>a {@code print} whose arguments hold no test kind and read no store (never evaluated, on both roads);</li>
     * </ul>
     * with at least one effect, no assert, and nothing that seeds: no statement names test data (a {@code from()}),
     * and the runtime declares none (the walk keys its seed memo by the seed's sources, the runner by the seeds'
     * content, and orders a statement's seeds against an open script the other way round: the M5 audit's B1). The
     * test door plans each as the walk sends it (M3d: every script byte-identical; {@code BodyRouterParityTest}).
     */
    EFFECT_PROGRAM,

    /**
     * A QUERY PROGRAM (M5f, narrowed by its audit): a body whose one statement that runs is its last, a query the
     * product's query door plans, answered as the walk answers it. Before the last, a statement plans no step: a let of
     * a literal or of a handle, a String or Boolean literal, an inert {@code print}, a handle. No statement holds a
     * test kind (no effect, no seeds, no let-bound {@code execute(...)}, no assert) or a kind the program road plans
     * another way, or with a movement not yet ruled: an {@code execute(...)} read where it stands (M9), staged rows, a
     * raw grid (T8, T12), a store write, test-data generation, a read of the system metamodel store (M4), an object
     * built or a graph answered (their ids and layouts, MV6), a temporal class read (the exists-join form, D5, MV6).
     * The answer is a relation or a value that reads no class's extent (a value through a class's chain is answered
     * in the product's column facts, D27, MV6); not an object (a class-typed answer is a graph on the product's road);
     * a to-one reader ({@code toOne()} over a relation) only as the answer's own root, where the router holds the
     * walk's exactly-one contract. The runtime declares no test data (M5's audit B1).
     */
    QUERY_PROGRAM,

    /**
     * An ASSERT PROGRAM under the HOST judge (M6): statement-root asserts -- at least one -- among lets the program
     * rides (a let's kind {@code VALUE}: no frame, staged rows, raw grid or alias) and statements that plan no step;
     * the last statement an assert or a statement that sends nothing (a query planned after the asserts could fail its
     * planning before they ran, where the walk fails it at its turn). No statement holds another test kind, a read
     * of the system metamodel store (M10a) or the eager inline execute ({@link #sideKind}; since M9c every other
     * kind is admitted here: the sides are the walk's plans on both roads). Each assert is planned by its judge before the
     * program is ({@link TestDoor.Asserts}); the router leaves on the walk a body holding an assert of a shape the host
     * checks do not plan yet (a named rule of the driver's: an engine-text assert whose text side is no constant of
     * the plan, a nested quantification, the vector contract's row-by-row families, the cell pool; the flat
     * quantified forms are planned since M10c item 5). The database judge's bodies stay on the walk (its fused group is composed at planning from M7).
     */
    ASSERT_PROGRAM,

    /**
     * A FRAME PROGRAM under the HOST judge (the test-is-the-product plan's M9, 2026-11-01): an assert program whose
     * lets may also bind FRAMES -- {@code let r = execute(...)} ({@link LetPlanner.Kind#FRAME}), the corpus's own
     * idiom, its query holding no kind the product's lowering plans another way than the walk's (its eager run is
     * the product's statement: {@link #anotherWay}) -- read by the asserts and the value lets after it (their
     * reads spliced into the sides, as the walk splices them). The frame's eager run is the test door's check step at
     * the let; the asserts' sides are planned over the walk's own frames, so a side is the walk's statement. Admitted
     * since M9b: an alias of a relation frame ({@code let v = $r.values->at(0)}, the same frame), an inline
     * {@code execute(...)} read where it stands (the product's lazy splice on both roads) but the one shape the splice
     * runs at planning ({@code size(execute(...))}), and a database effect as a statement with literal arguments (the
     * effect program's rule). A frame's query reads no earlier frame, by name or through a value let tainted by one
     * (the walk resolves its run chain with no splice, where the program road's recorder splices such a read: the
     * M9a audit's N1, the M9b audit's F2). Admitted since M9c: a read of a frame's activities ({@code $r.activities}:
     * the run's SQL, a planning-time render on both roads, and its comment, the trace stamp of the eager run -- a
     * plan parameter the run's check step binds and the sides carry, where the walk's frame holds the run's comment
     * as a literal). Admitted since M10c item 11: a from()'s inline test data in a let BEFORE anything the body sends
     * (an effect, a frame let, an assert), which the test door seeds before the let as the walk establishes it; a
     * frame's let-bound query is read through the let that binds it. Not admitted, each a later sub-step: an effect
     * let, staged rows and raw grids, the test-data generator, {@code assertError}, a seeded runtime.
     */
    FRAME_PROGRAM;

    /** {@code body}'s class on {@code runtime} under a run that judges with BOTH judges (channel B), or null when the
     *  walk runs it. */
    public static @Nullable BodyClass of(TypedQuery body, String runtime) {
        return of(body, runtime, false);
    }

    /** {@code body}'s class on {@code runtime}, {@code oneJudge} whether the run judges with one judge (the host's,
     *  M6 and M9, or the database's, M7: the assert and frame classes are theirs; a run judging with both, channel B,
     *  keeps every assert body on the walk until M8), or null when the walk runs it. */
    public static @Nullable BodyClass of(TypedQuery body, String runtime, boolean oneJudge) {
        // a runtime that declares test data: the test door seeds it as the walk does, a keyed setup step before the
        // first statement that names it, once per session (TestKinds.seed, Seeds.planned: "the program's runtime";
        // M3e) -- the M5 audit's B1 refusal predated that seed (2026-11-04, the core program: JudgesOnPlansTest,
        // ServiceTestProvisionKeyTest, CreateClassTest measured on the program road)
        QueryDoor.Scan scan = new QueryDoor.Scan(body, runtime);
        if (computedEffectText(body, scan)) {
            // a database effect's text computed at run time (the user's ruling, 2026-11-04, the core program: lite has
            // no text holes, D75 -- a value is written with a store write): the test door refuses it by name when it
            // plans the effect (TestKinds.effect), before anything runs
            return EFFECT_PROGRAM;
        }
        if (effectProgram(body, scan)) {
            return EFFECT_PROGRAM;
        }
        if (queryProgram(body, scan)) {
            return QUERY_PROGRAM;
        }
        if (!oneJudge || writesAStore(body)) {
            // a body a store write reaches (a side's, a value let's, a helper's) runs on the walk inside the
            // transaction the walk opens for it and rolls back on a raise; the program road opens one for an Update
            // step alone (the M9c audit's F2)
            return null;
        }
        if (assertProgram(body, scan, systemRouted(body, runtime))) {
            return ASSERT_PROGRAM;
        }
        return frameProgram(body, runtime, scan) ? FRAME_PROGRAM : null;
    }

    /**
     * Whether {@code query}'s body is SYSTEM-ROUTED (the test-is-the-product plan's M10a, the narrow M4): some
     * statement reads the system metamodel store (a system class's {@code all()}, the store's tables, a system
     * function, a tracked element's row) and no statement reads a user store (a mapped class's {@code all()}, a
     * user store's table, a {@code from()}, an {@code execute} or a handle over a runtime) -- each through the user
     * functions it calls, a lambda literal's body left aside (a lambda is a value: the native that runs one against
     * a store, an execute or a handle, is itself the read the scan names; a lambda handed to a SQL-text generator,
     * {@code toSQLString(|Product.all()...)}, runs nowhere) -- on a runtime whose engine the platform runs in
     * process (the system tables keep their names there). The frame, effect and store-write exclusions are the
     * class's ({@link #of}, {@code assertProgram}), not this predicate's. Such a body's program runs on the graph's
     * system database, the one session the walk routes each of its sides to; a body mixing the two stores has two
     * targets and stays on the walk, by name.
     */
    public static boolean systemRouted(TypedQuery query, String runtime) {
        ModelContext ctx = query.context();
        if (!SystemRoutes.inProcess(ctx, runtime)) {
            return false;
        }
        Set<String> systemClasses = systemClasses(ctx);
        Map<FunctionId, Boolean> systemMemo = new HashMap<>();
        Map<FunctionId, Boolean> userMemo = new HashMap<>();
        boolean system = false;
        List<TypedSpec> body = query.body();
        ElementConsumption consumption = new ElementConsumption(body, systemClasses, ctx, query.specs());
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (readsStore(statement, n -> userStoreRead(n, systemClasses, ctx), query.specs(), userMemo)) {
                return false;
            }
            if (consumption.elementOnlyLet(i)) {
                // a let binding system elements alone (M10b; through helpers, constructions and chains: items 7, 7b):
                // a read of the store only where the body reads its variable as a value
                system |= !consumption.consumed(i);
                continue;
            }
            system |= readsStore(statement, n -> anotherWay(n, systemClasses, ctx) == Kind.SYSTEM_READ, query.specs(),
                    systemMemo);
        }
        return system;
    }

    /** The stores {@code statement} of {@code query} reads, through the user functions it calls: none, the system
     *  metamodel store alone, a user store alone, or both (per-statement routing, the walker-deletion program,
     *  2026-11-03: a statement reading the system store alone runs on the graph's system database beside the body's own
     *  target; one reading both stays the walk's until the metamodel's rows ride the statement). */
    public enum Stores { NONE, SYSTEM, USER, BOTH }

    /** {@code statement}'s {@link Stores}, statement {@code index} of {@code query}'s body: a variable read through
     *  the let that binds it, as the planners splice it. */
    public static Stores storesOf(TypedSpec statement, TypedQuery query, int index) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        // a frame read as its query (the execute call is a handle the scan stops at): a frame RUNS its query, so the
        // scan descends into that lambda alone -- a lambda handed anywhere else (a SQL-text helper, a lineage scan, a
        // plan handle) runs nowhere, as the system-routing rule leaves it aside
        java.util.Set<TypedSpec> frameQueries = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        TypedSpec read = framesAsTheirQueries(
                throughLets(statement, query.body().subList(0, Math.min(index, query.body().size())), 0),
                frameQueries);
        boolean user = readsStore(read, n -> userStoreRead(n, systemClasses, ctx), query.specs(), new HashMap<>(),
                frameQueries::contains);
        boolean system = readsStore(read, n -> anotherWay(n, systemClasses, ctx) == Kind.SYSTEM_READ,
                query.specs(), new HashMap<>(), frameQueries::contains);
        return user && system ? Stores.BOTH : user ? Stores.USER : system ? Stores.SYSTEM : Stores.NONE;
    }

    /** {@code n} with each frame call ({@code execute(...)}: {@code NativeFn.Handle.runsAFrame}) replaced by its query
     *  argument, for the store scan alone. */
    private static TypedSpec framesAsTheirQueries(TypedSpec n, java.util.Set<TypedSpec> frameQueries) {
        if (n instanceof TypedNativeCall call && !call.args().isEmpty()
                && NativeFn.Handle.runsAFrame(call.callee().id())) {
            TypedSpec q = framesAsTheirQueries(call.args().get(0), frameQueries);
            lambdasUnder(q, frameQueries);
            return q;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = framesAsTheirQueries(k, frameQueries);
            changed |= r != k;
            out.add(r);
        }
        return changed ? n.withChildren(out) : n;
    }

    /** Every lambda literal at or under {@code n} (a frame's query: its predicates run where it runs) into {@code out}. */
    private static void lambdasUnder(TypedSpec n, java.util.Set<TypedSpec> out) {
        if (n instanceof TypedLambda) {
            out.add(n);
        }
        for (TypedSpec c : n.children()) {
            lambdasUnder(c, out);
        }
    }

    /** {@code n} with each variable a let of {@code lets} binds replaced by the let's value (itself read through the
     *  lets before it), for the store scan alone; bounded, a cycle impossible in a body's lexical lets. */
    private static TypedSpec throughLets(TypedSpec n, List<TypedSpec> lets, int depth) {
        if (depth > 32) {
            return n;
        }
        if (n instanceof TypedVariable v) {
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof TypedLet let && let.name().equals(v.name())) {
                    return throughLets(let.value(), lets.subList(0, i), depth + 1);
                }
            }
            return n;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = throughLets(k, lets, depth);
            changed |= r != k;
            out.add(r);
        }
        return changed ? n.withChildren(out) : n;
    }

    /** A property read over a tracked system element ({@code x::DB.schemas}, {@code $db.schemas.tables.name} inside a
     *  helper): a read of the element's row, which the walk routes to the system session -- never an element reference
     *  alone (item 7's audit: the kinds name the reference inside such a navigation, not the navigation). */
    static boolean elementNavigation(TypedSpec n, Set<String> systemClasses) {
        return n instanceof TypedPropertyAccess pa && pa.source().info().type() instanceof Type.ClassType ct
                && systemClasses.contains(ct.fqn());
    }


    /**
     * Whether {@code n} reaches a node {@code read} names (a store's read) through the user functions it calls
     * ({@code StatementEffects.reaches}' walk, one memo per callee), with three places left aside: a lambda literal's
     * body (a lambda is a value; the native that runs one against a store -- an execute, a handle, a generator -- is
     * itself the read {@link #userStoreRead} names) and the arguments of a planning-time native (a handle's, a Java
     * routine's, a planning-time effect's: {@code NativeFn.Handle}, {@code NativeFn.JavaRoutine},
     * {@code NativeFn.Effect.readsArgumentsAtPlanning}: a mapping or a runtime handed to {@code executionPlan(...)},
     * {@code toSQLString(...)} or {@code connectionByElement(...)} is consumed when the plan, the text or the target is
     * made, never read as a row of the system store); a {@code from()}'s mapping and runtime are its context, read at
     * planning, so the walk reads its source alone (as {@link #framesAndHandlesAsTheirQueries} does).
     */
    static boolean readsStore(TypedSpec n, java.util.function.Predicate<TypedSpec> read, SpecCompiler specs,
            Map<FunctionId, Boolean> memo) {
        return readsStore(n, read, specs, memo, false);
    }

    /** {@link #readsStore(TypedSpec, java.util.function.Predicate, SpecCompiler, Map)}; {@code throughLambdas}: a lambda
     *  literal's body walked too (the element-consumption analysis's "no other read" half, item 7b's audit: a lambda a
     *  collection native RUNS evaluates where the let is spliced). */
    static boolean readsStore(TypedSpec n, java.util.function.Predicate<TypedSpec> read, SpecCompiler specs,
            Map<FunctionId, Boolean> memo, boolean throughLambdas) {
        return readsStore(n, read, specs, memo, lambda -> throughLambdas);
    }

    /** {@link #readsStore(TypedSpec, java.util.function.Predicate, SpecCompiler, Map, boolean)}, a lambda literal
     *  walked where {@code descend} says (a frame's query, which runs where the statement runs). */
    static boolean readsStore(TypedSpec n, java.util.function.Predicate<TypedSpec> read, SpecCompiler specs,
            Map<FunctionId, Boolean> memo, java.util.function.Predicate<TypedSpec> descend) {
        boolean throughLambdas = !(n instanceof TypedLambda) || descend.test(n);
        if (n instanceof TypedLambda && !throughLambdas) {
            return false;
        }
        if (read.test(n)) {
            return true;
        }
        if (n instanceof TypedNativeCall call) {
            FunctionId id = call.callee().id();
            if (NativeFn.Handle.of(call.callee().id()).isPresent()
                    || NativeFn.JavaRoutine.of(call.callee().id()).isPresent()) {
                return false;
            }
            if (NativeFn.Effect.readsArgumentsAtPlanning(id)) {
                // the seed forms' data argument is a value; the store, the config and connectionByElement's arguments
                // are read at planning
                for (int k = 0; k < call.args().size(); k++) {
                    if (!NativeFn.Effect.argumentReadAtPlanning(id, k)
                            && readsStore(call.args().get(k), read, specs, memo, descend)) {
                        return true;
                    }
                }
                return false;
            }
        }
        if (n instanceof TypedFrom from) {
            return readsStore(from.source(), read, specs, memo, descend);
        }
        if (n instanceof TypedUserCall uc) {
            FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in progress: a cycle scores false
                boolean reaches = false;
                try {
                    for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
                        if (readsStore(stmt, read, specs, memo, descend)) {
                            reaches = true;
                            break;
                        }
                    }
                } catch (com.legend.compiler.spec.TypeInferenceException e) {
                    // an un-typeable callee executes in no channel; the scan does not decide on it (StatementEffects')
                }
                memo.put(key, reaches);
                known = reaches;
            }
            if (known) {
                return true;
            }
        }
        for (TypedSpec c : n.children()) {
            if (readsStore(c, read, specs, memo, descend)) {
                return true;
            }
        }
        return false;
    }

    /** A read of a USER store: a mapped class's extent that is no system class's, a table of a store that is not the
     *  system store, a {@code from()} (its runtime's), an {@code execute} or a handle over a runtime, a test-data
     *  generator (it runs against the runtime's database: {@code StatementEffects.containsTdgGenerator}'s natives). */
    static boolean userStoreRead(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        if (n instanceof TypedGetAll all) {
            return !systemClasses.contains(all.classFqn()) && !SystemMetamodel.isSystemElement(all.classFqn());
        }
        if (n instanceof TypedTableReference table) {
            return !SystemMetamodel.STORE_FQN.equals(table.store());
        }
        return n instanceof TypedFrom
                || n instanceof TypedNativeCall call && (NativeFn.Handle.isExecute(call.callee().id())
                        || NativeFn.Handle.runsAFrame(call.callee().id())
                        || StatementEffects.containsTdgGenerator(call));
    }

    /** Whether any statement of {@code query} reaches a store write (through the user functions it calls). */
    static boolean writesAStore(TypedQuery query) {
        Map<FunctionId, Boolean> memo = new HashMap<>();
        for (TypedSpec statement : query.body()) {
            if (StatementEffects.writesStore(statement, query.specs(), memo)) {
                return true;
            }
        }
        return false;
    }

    private static boolean frameProgram(TypedQuery query, String runtime, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        // one memo per reach predicate (StatementEffects.reaches caches "the callee's body reaches p" by callee, so
        // a memo shared by two predicates blinds the second to every callee the first walked: the M9b audit's F1)
        Map<FunctionId, Boolean> frameQueryMemo = new HashMap<>();
        Map<FunctionId, Boolean> sideKindMemo = new HashMap<>();
        Map<FunctionId, Boolean> taintMemo = new HashMap<>();
        Map<FunctionId, Boolean> runsAFrameMemo = new HashMap<>();
        Map<FunctionId, Boolean> effectMemo = new HashMap<>();
        Map<FunctionId, Boolean> checkMemo = new HashMap<>();
        List<TypedSpec> body = query.body();
        // the frames bound so far, each whether relation-rooted (the planner's own fact, FramePlanner.relationRooted:
        // the alias test and the splice read it); the value lets before, as the walk's prefix; the value lets TAINTED
        // by a frame (a read of a frame, an alias, a tainted let or an inline execute), which a later frame's query
        // may not read: the walk's run chain is resolved with no splice, so such a read fails there where the
        // program road's recorder splices it (the M9b audit's F2; the M9a audit's N1 for a frame read by name)
        Map<String, Boolean> frames = new LinkedHashMap<>();
        Set<String> tainted = new HashSet<>();
        java.util.function.Predicate<String> relationFrame = n -> Boolean.TRUE.equals(frames.get(n));
        java.util.function.Predicate<TypedSpec> readsAFrame = n -> n instanceof TypedVariable v
                && (frames.containsKey(v.name()) || tainted.contains(v.name()))
                || n instanceof TypedNativeCall call && NativeFn.Handle.runsAFrame(call.callee().id());
        List<TypedSpec> lets = new ArrayList<>();
        boolean anAssert = false;
        // a frame bound by a let, or an execute(...) read where it stands (M9b): what makes the body a frame program
        boolean aFrame = false;
        boolean statementFrame = false;
        ElementConsumption consumption = new ElementConsumption(body, systemClasses, ctx, query.specs());
        boolean sentBefore = false;
        // the seed sources established so far with no write since (the test door's own established set, TestKinds.seed:
        // a let naming them again plans no seed step)
        Set<Seeds.Sources> seeded = new HashSet<>();
        // the names the frames' query lambdas bind themselves (their parameters, their top-level lets), so far
        Set<String> frameNames = new HashSet<>();
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            boolean last = i == body.size() - 1;
            if (statement instanceof TypedLet && !last && !sentBefore) {
                // a from()'s inline test data in a let (the corpus's `let func = {|...->from(mapping, $runtimeWithTestData)}`
                // a frame then runs; M10c item 11): the test door seeds the let's sources before it, as the walk
                // establishes them before the statement (TestKinds.seed) -- no kind of the let's own. Only before
                // anything the body sends (an effect, a frame let, an assert): the walk orders a statement's seeds
                // before closing an open effect script where the door closes it first, and under the database judge
                // the segment's fused statement is placed at its last assert where the walk flushes it later, so a
                // seed after either would land on the other side of it (the M5 audit's B1, the item's audit F2/F3).
                // An assert holding inline data stays refused: its planner plans an assert whose kinds are its alone
                if (kinds.remove(QueryDoor.TestOnly.INLINE_SEEDS)) {
                    seeded.add(Seeds.sources(statement, runtime, ctx));
                }
            } else if (statement instanceof TypedLet && !last && kinds.contains(QueryDoor.TestOnly.INLINE_SEEDS)
                    && seeded.contains(Seeds.sources(statement, runtime, ctx))) {
                // the SAME sources a let established earlier, with no write since: the test door plans no seed step for
                // it (TestKinds.seed's established set), so nothing lands between the asserts -- a second frame over
                // the first's inline data (the corpus burn-down, 2026-11-06: paginate::testPaginated,
                // union::rootLevel::testSpecialUnion_m2m2r)
                kinds.remove(QueryDoor.TestOnly.INLINE_SEEDS);
            }
            // whether this statement sends (an effect, a frame let, an assert): no later let's seeds are admitted
            sentBefore |= kinds.contains(QueryDoor.TestOnly.EFFECT) || kinds.contains(QueryDoor.TestOnly.ASSERT)
                    || kinds.contains(QueryDoor.TestOnly.LET_EXECUTE);
            if (StatementEffects.containsTdgGenerator(statement)) {
                return false;
            }
            TypedSpec value = statement instanceof TypedLet l ? l.value() : statement;
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && value instanceof TypedNativeCall call
                    && NativeFn.Effect.isDbEffect(call.callee().id())) {
                // a database effect as a statement (M9b): the effect program's rule -- literal arguments, not a let
                // (an effect let's value is a raw grid: later)
                if (statement instanceof TypedLet || !literalEffect(call, body.subList(0, i))) {
                    return false;
                }
                seeded.clear();   // a write: the sources established before it are established no longer
                continue;
            }
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && !(statement instanceof TypedLet)
                    && seedFormEffectMap(value, body.subList(0, i))) {
                // a seed form mapped over an effect (M10c item 7c): the test door plans its texts as one script
                seeded.clear();
                continue;
            }
            if (statement instanceof TypedLet let && !last) {
                if (LetPlanner.badAliasIndex(let.value(), relationFrame)) {
                    // $r.values->at(k>0) over a relation frame: the walk raises it at the let's turn (the M9a audit's
                    // S1: a rule decides by the shape, never by raising)
                    return false;
                }
                LetPlanner.Kind kind = LetPlanner.kind(let, body, i, query.specs(), effectMemo, relationFrame);
                if (kind == LetPlanner.Kind.FRAME && kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE))
                        && storesOf(statement, query, i) == Stores.BOTH) {
                    // a frame whose query reads both stores once its lets are spliced (a let of a system read read by
                    // a query over a user store): one statement over two databases, the walk's until the metamodel's
                    // rows ride the statement
                    return false;
                }
                if (kind == LetPlanner.Kind.FRAME && kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE))) {
                    // the frame's query holds no kind planned another way and reads no earlier frame, by name or
                    // through a tainted let (N1, F2); its mapping and runtime arguments are the frame's own references
                    TypedNativeCall call = (TypedNativeCall) LetPlanner.rhs(let);
                    // the query read through the let that binds it (`let func = {|...}; let r = execute($func, ...)`:
                    // the corpus's spelling) -- a variable has no children, so the scan saw no kind in a let-bound
                    // query until M10c item 11's lanes found a temporal graph fetch through it
                    TypedSpec boundQuery = com.legend.compiler.spec.typed.Lets.bound(call.args().get(0),
                            body.subList(0, i));
                    if (boundQuery instanceof com.legend.compiler.spec.typed.TypedLambda lam
                            && !java.util.Collections.disjoint(frameNames, ownNames(lam))) {
                        // the query lambda binds a name an earlier frame's lambda bound (an executeLegendQuery
                        // parameter, a top-level let): the walk's frames write ONE environment's query lets, so the
                        // earlier frame's readers read the later binding (`$r1` over the pairs of `$r2`: the walk's
                        // defect, upstream reads each frame's own) -- the program road cannot agree with it and must
                        // not copy it: the walk's, by name, until per-frame bindings (the M10c item 12 audit's F1)
                        return false;
                    }
                    if (boundQuery instanceof com.legend.compiler.spec.typed.TypedLambda lam) {
                        frameNames.addAll(ownNames(lam));
                    }
                    TypedSpec frameQuery = framesAndHandlesAsTheirQueries(boundQuery);
                    if (StatementEffects.reaches(frameQuery, n -> frameQueryKind(n, systemClasses, ctx) != null
                            || readsAFrame.test(n), query.specs(), frameQueryMemo)) {
                        return false;
                    }
                    // relation-rooted by the query's typed root (the string entry never); a shape the chain's build
                    // refuses is left to the walk's refusal at the let (the M9b audit's F5: no chain built here)
                    Boolean relationRooted = FramePlanner.relationRooted(call, lets, query.specs());
                    if (relationRooted == null) {
                        return false;
                    }
                    frames.put(let.name(), relationRooted);
                    aFrame = true;
                    continue;
                }
                if (kind == LetPlanner.Kind.ALIAS && kinds.isEmpty()) {
                    // an alias of a relation frame ({@code let v = $r.values->at(0)}, M9b): the same frame
                    frames.put(let.name(), true);
                    continue;
                }
                if (kind == LetPlanner.Kind.VALUE && kinds.isEmpty() && BodyPlanner.handleType(let.value(), ctx) != null) {
                    // an orchestration handle bound by a let (a connection for the effects, M9b): the effect program's
                    // rule, a value both roads answer with no statement
                    lets.add(let);
                    continue;
                }
                // a let binding system elements the body never reads as a value: a value let the program splices,
                // no system read (M10b, as the assert program's rule; through helpers, constructions and chains: items
                // 7, 7b)
                boolean consumedBinding = consumption.consumedElementLet(i);
                if (kind != LetPlanner.Kind.VALUE || !kinds.isEmpty()
                        || !consumedBinding && storesOf(statement, query, i) == Stores.BOTH
                        || !consumedBinding && StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                                n -> sideKind(n, systemClasses, ctx) != null, query.specs(), sideKindMemo)) {
                    return false;
                }
                if (StatementEffects.reaches(let.value(), readsAFrame, query.specs(), taintMemo)) {
                    tainted.add(let.name());
                }
                lets.add(let);
                continue;
            }
            // an assert at a statement's root, or an assertError (its function's body the step's side, 2026-11-03)
            boolean assertRoot = kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) && !(statement instanceof TypedLet)
                    || QueryDoor.assertErrorRoot(statement, query.specs());
            if (!assertRoot && !kinds.isEmpty() || storesOf(statement, query, i) == Stores.BOTH
                    || StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                            n -> sideKind(n, systemClasses, ctx) != null, query.specs(), sideKindMemo)) {
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (!last && !plansNoStep(statement, body.subList(0, i), ctx)
                    && !check(statement, systemClasses, ctx, query, checkMemo)) {
                return false;
            }
            boolean runs = StatementEffects.reaches(statement, n -> n instanceof TypedNativeCall call
                    && NativeFn.Handle.runsAFrame(call.callee().id()), query.specs(), runsAFrameMemo);
            aFrame |= runs;
            // a frame run by a statement of its own (an unread frame let runs as its statement): a check step the
            // program plans, which no-assert bodies may not hold (below)
            statementFrame |= runs && !last;
        }
        if (!aFrame) {
            return false;
        }
        TypedSpec last = body.get(body.size() - 1);
        if (!anAssert) {
            // a frame program with no assert (2026-11-03, the core program: ExecuteFrameTest's `let r = execute(...);
            // $r.values`): its last statement is its answer, by the query program's answer rule -- the guard below (a
            // query planned after the asserts could fail before they ran) has no assert to guard.
            // Only let-bound frames, no frame run as a statement of its own: the corpus's
            // otherwiseTestQualifierPropertyConstantExpression (`let result = execute(|Product.all()->filter(...));
            // true;`, its unread let a statement over an object query) the walk runs and the store resolver refuses.
            // A bare execute(...) of a relation-rooted query as the answer answers its frame's rows, the walk's forced
            // frame (2026-11-04, the core program: ExecuteFrameTest's executeInResultPosition; the test door's
            // ProductStatements.prepare, frameAnswered); any other execute(...) answered whole stays the walk's
            if (bareExecute(last)) {
                return !statementFrame && relationFrameAnswer(query, last, lets);
            }
            // a frame run as a statement of its own (an unread frame let's execute(...), the user's ruling of
            // 2026-11-02): run where it stands, its rows dropped, as the walk runs it (the user, 2026-11-06)
            return (statementFrame || !frames.isEmpty()) && answered(query, Lets.bare(last));
        }
        return scan.kinds(last).contains(QueryDoor.TestOnly.ASSERT) || QueryDoor.assertErrorRoot(last, query.specs())
                || plansNoStep(last, body.subList(0, body.size() - 1), ctx);
    }

    /** Whether {@code statement} is an {@code execute(...)} at its root (no let): a frame answered whole. */
    private static boolean bareExecute(TypedSpec statement) {
        return statement instanceof TypedNativeCall call
                && NativeFn.Handle.of(call.callee().id()).orElse(null) == NativeFn.Handle.EXECUTE;
    }

    /** Whether {@code last}, a bare {@code execute(...)}, runs a relation-rooted frame (its answer the frame's rows). */
    private static boolean relationFrameAnswer(TypedQuery query, TypedSpec last, List<TypedSpec> lets) {
        Boolean relationRooted = FramePlanner.relationRooted((TypedNativeCall) last, lets, query.specs());
        return Boolean.TRUE.equals(relationRooted);
    }

    /** The names {@code lam}, a frame's query, binds itself: its parameters (an executeLegendQuery's, bound from its
     *  pairs) and its top-level lets -- the lets its chain's inliner records as the frame's query lets. */
    private static Set<String> ownNames(com.legend.compiler.spec.typed.TypedLambda lam) {
        Set<String> names = new HashSet<>(lam.parameters());
        for (TypedSpec st : lam.body()) {
            if (st instanceof TypedLet let) {
                names.add(let.name());
            }
        }
        return names;
    }

    /** A read of a frame's activities ({@code $r.activities...}: the run's SQL and comment). Admitted since M9c (the
     *  pre-planned frame's comment is the stamp parameter its eager run binds); named for the kind census. */
    static boolean readsActivities(TypedSpec n) {
        return n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa && "activities".equals(pa.property());
    }

    /** {@code statement} with each inline {@code execute(...)} read where it stands, and each orchestration HANDLE
     *  native ({@code executionPlan(...)}, the lineage scans: {@code PlatformTypes.handleRowClass}), replaced by its
     *  query argument (M9b, M9d: the product's lazy splice, D15, on both roads; a handle's rows registered from its
     *  arguments by {@code PlanAllocations.registerHandlesIn} on both), and each {@code from()} by its source (its
     *  mapping and runtime are its context): the scan for the kinds that keep a body on the walk then reads the
     *  frame's or the handle's query and not its mapping and runtime references, as the frame let's branch reads
     *  them. The eager shape {@code size(execute(...))} is refused first
     *  ({@link #eagerInlineExecute}: it is the call the scan sees, so the size call keeps its argument). */
    static TypedSpec framesAndHandlesAsTheirQueries(TypedSpec statement) {
        if (eagerInlineExecute(statement)) {
            return statement;
        }
        if (statement instanceof TypedNativeCall call && !call.args().isEmpty()
                && (NativeFn.Handle.runsAFrame(call.callee().id())
                        || PlatformTypes.handleRowClass(call.callee().id(), call.callee().returnType()) != null
                        // a Java routine (toSQLString, planToString: a planning-time text) reads as its first argument,
                        // the query or the plan under test; the mapping, the runtime and the dialect it is handed are
                        // consumed when the text is made, never read as a row (M10b)
                        || NativeFn.JavaRoutine.of(call.callee().id()).isPresent())) {
            return framesAndHandlesAsTheirQueries(call.args().get(0));
        }
        if (statement instanceof TypedFrom from) {
            // a from()'s mapping and runtime are its execution context, never values read (the resolver binds them as
            // the context, not as element rows): the scan reads the source alone, the envelope itself no kind
            return framesAndHandlesAsTheirQueries(from.source());
        }
        List<TypedSpec> kids = statement.children();
        if (kids.isEmpty()) {
            return statement;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = framesAndHandlesAsTheirQueries(k);
            changed |= r != k;
            out.add(r);
        }
        return changed ? statement.withChildren(out) : statement;
    }

    /** {@code size(execute(...))}: the one inline shape the splice RUNS at the side's planning
     *  ({@code ResultEnvelopeSplice}: nothing downstream consumes the chain), which a body planned before it runs
     *  cannot carry. */
    static boolean eagerInlineExecute(TypedSpec n) {
        if (!(n instanceof TypedNativeCall sz && ResultEnvelopeSplice.SIZE_FQNS.contains(sz.callee().qualifiedName())
                && sz.args().size() == 1)) {
            return false;
        }
        TypedSpec arg = sz.args().get(0);
        while (arg instanceof TypedFrom f) {
            arg = f.source();
        }
        return arg instanceof TypedNativeCall ec && NativeFn.Handle.isExecute(ec.callee().id());
    }

    /** {@code -Dlegend.diagnostics=body-class}: the assert program's rule that refused a body, by statement (the walk
     *  deletion's measurement, 2026-11-06; prints, decides nothing). */
    private static void assertRefusal(int statement, String why) {
        if (java.util.Arrays.asList(System.getProperty("legend.diagnostics", "").split(",")).contains("body-class")) {
            System.err.println("[body-class] assert rule " + System.getProperty("legend.test.current", "?")
                    + " statement " + statement + ": " + why);
        }
    }

    private static boolean assertProgram(TypedQuery query, QueryDoor.Scan scan, boolean systemRouted) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        Map<FunctionId, Boolean> memo = new HashMap<>();
        Map<FunctionId, Boolean> checkMemo = new HashMap<>();
        Map<FunctionId, Boolean> effectMemo = new HashMap<>();
        List<TypedSpec> body = query.body();
        boolean anAssert = false;
        ElementConsumption consumption = new ElementConsumption(body, systemClasses, ctx, query.specs());
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            // an assert at a statement's root, or an assertError (its function's body the step's side, 2026-11-03)
            boolean assertRoot = kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) && !(statement instanceof TypedLet)
                    || QueryDoor.assertErrorRoot(statement, query.specs());
            // a let binding system elements the body never reads as a value (consumed by toSQLString, by
            // executionPlan, by an execute's runtime argument): a value let the program splices, no system read (M10b;
            // the same test routes the body; through helpers, constructions and chains: items 7, 7b)
            boolean consumedBinding = consumption.consumedElementLet(i);
            if (!systemRouted && kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT))
                    && plannedEffect(statement, body.subList(0, i))) {
                // a database effect the test door writes as a statement of its own, as the frame and effect programs
                // admit it (a literal effect, a text form mapped over one): the corpus's setup helpers unfolded before
                // the asserts (the corpus burn-down, 2026-11-06; the walk-deletion analysis's group A). A system-routed
                // body's effects would target the user's connection beside the program's own: not admitted
                continue;
            }
            if (!assertRoot && !kinds.isEmpty() || StatementEffects.containsTdgGenerator(statement)
                    || !consumedBinding && storesOf(statement, query, i) == Stores.BOTH
                    || !consumedBinding && StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                            n -> sideKind(n, systemClasses, ctx, systemRouted) != null, query.specs(), memo)) {
                assertRefusal(i, !assertRoot && !kinds.isEmpty() ? "kinds " + kinds + " ("
                        + (statement instanceof TypedNativeCall nc ? "native " + nc.callee().qualifiedName()
                                : statement instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                                        ? "user call " + uc.callee().qualifiedName()
                                        : statement.getClass().getSimpleName())
                        + (systemRouted ? ", system-routed" : "") + ")"
                        : StatementEffects.containsTdgGenerator(statement) ? "the test data generator"
                        : !consumedBinding && storesOf(statement, query, i) == Stores.BOTH ? "both stores"
                        : "a side of another kind");
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (i < body.size() - 1 && !plansNoStep(statement, body.subList(0, i), ctx)
                    && !(statement instanceof TypedLet let && LetPlanner.kind(let, body, i, query.specs(),
                            effectMemo, name -> false) == LetPlanner.Kind.VALUE)
                    && !check(statement, systemClasses, ctx, query, checkMemo)) {
                assertRefusal(i, "a statement before the last that plans a step and is no check");
                return false;
            }
        }
        if (!anAssert) {
            return false;
        }
        TypedSpec last = body.get(body.size() - 1);
        // an assert as the last statement answers its verdict, as the walk answers it; any other last statement sends
        // nothing (a literal, a handle, an inert print) -- or is a QUERY the query program's own rule answers
        // (answersAsTheLastStatement: no test-only kind, no generator, one store, nothing planned another way, an
        // answer the query door gives). Admitting ANY last statement (2026-11-04) lost the corpus's 55 test-data
        // generation bodies, whose last statement the program cannot plan (a class query under an if() decided at
        // run time) and was reverted; the query rule refuses those. Readmitted 2026-11-05: its revert at 25facf2e was
        // a wrong diagnosis -- the 55 were lost by the walk reading a caller's message first (HostSides.fetchChecked)
        return scan.kinds(last).contains(QueryDoor.TestOnly.ASSERT) || QueryDoor.assertErrorRoot(last, query.specs())
                || plansNoStep(last, body.subList(0, body.size() - 1), ctx)
                || answersAsTheLastStatement(query, scan, body.size() - 1, systemClasses, ctx);
    }

    /** Whether the statement at {@code i}, a program's last, is a query the query program's rule answers as its last
     *  statement ({@link #queryProgram}'s own checks: no test-only kind, no test-data generator, one store, nothing
     *  that plans another way but a system read, a raw grid or an object built inside it, and an answer the query door
     *  gives -- {@link #answered}). */
    private static boolean answersAsTheLastStatement(TypedQuery query, QueryDoor.Scan scan, int i,
            Set<String> systemClasses, ModelContext ctx) {
        TypedSpec statement = query.body().get(i);
        if (!scan.kinds(statement).isEmpty() || StatementEffects.containsTdgGenerator(statement)
                || storesOf(statement, query, i) == Stores.BOTH) {
            return false;
        }
        if (StatementEffects.reaches(statement, n -> {
                    Kind kind = anotherWay(n, systemClasses, ctx);
                    return kind != null && kind != Kind.SYSTEM_READ && kind != Kind.RAW_GRID && kind != Kind.OBJECT;
                }, query.specs(), new HashMap<>())) {
            return false;
        }
        return answered(query, Lets.bare(statement));
    }

    /** Whether {@code body} holds an assert at a statement's root, or an assertError: what a judge plans. */
    public static boolean anAssert(TypedQuery body, String runtime) {
        QueryDoor.Scan scan = new QueryDoor.Scan(body, runtime);
        for (TypedSpec statement : body.body()) {
            if (scan.kinds(statement).contains(QueryDoor.TestOnly.ASSERT)
                    || QueryDoor.assertErrorRoot(statement, body.specs())) {
                return true;
            }
        }
        return false;
    }

    /** Why the query program's rule last refused a body on this thread (a measurement for the walk's door message,
     *  2026-11-03: the core program reads it to move the walking bodies); null where it admitted the body. */
    private static final ThreadLocal<String> REFUSAL = new ThreadLocal<>();

    /** The query program rule's last refusal on this thread, or null. */
    public static @Nullable String lastRefusal() {
        return REFUSAL.get();
    }

    private static boolean refused(String why) {
        REFUSAL.set(why);
        return false;
    }

    private static boolean queryProgram(TypedQuery query, QueryDoor.Scan scan) {
        REFUSAL.remove();
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        Map<FunctionId, Boolean> memo = new HashMap<>();
        List<TypedSpec> body = query.body();
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            // a read of the system store is no kind planned another way: a statement reading it alone runs on the
            // graph's system database (per-statement routing, 2026-11-03); one reading both stores stays the walk's
            if (!scan.kinds(statement).isEmpty()) {
                return refused("statement " + i + " kinds " + scan.kinds(statement));
            }
            if (StatementEffects.containsTdgGenerator(statement)) {
                return refused("statement " + i + " tdg");
            }
            if (storesOf(statement, query, i) == Stores.BOTH) {
                if (body.size() == 1) {
                    // a lone statement reading both stores (2026-11-04, the core program: MetamodelStoreTest's
                    // mixedChainKeepsTheWall): planning raises what the walk raises at its turn, nothing before it --
                    // the store resolver's wall where a user class has no context, else the system route's refusal
                    // (no single session: SystemRoutes.refuseMixed, the walk's and the program road's)
                    return true;
                }
                return refused("statement " + i + " reads both stores");
            }
            if (i < body.size() - 1 && statement instanceof TypedLet let
                    && BodyPlanner.handleType(let.value(), ctx) != null) {
                // an orchestration handle bound by a let (a connection for executeInDb, a runtime): the frame
                // program's rule (M9b), a value both roads answer with no statement -- its constructor builds no object
                // a statement reads (2026-11-03, the core program: Phase1AuditTest's `let c = ^TestDatabaseConnection`)
                continue;
            }
            Kind[] found = new Kind[1];
            if (StatementEffects.reaches(statement, n -> {
                        Kind kind = anotherWay(n, systemClasses, ctx);
                        // the product's statement plans these too (2026-11-03, the core program, each measured on the
                        // program road by core's tests): a raw grid (executeInDb's rows, the product's keyed result,
                        // ProductStatements), an object built inside the statement (InstanceIdentityTest's keyed
                        // equality and membership in value position)
                        // a store write as the body's ONE statement (2026-11-04, the core program): one update step,
                        // atomic in the database as the walk's transaction around it is (StagedRowsTest's delete)
                        if (kind == Kind.STORE_WRITE || kind == Kind.STAGED) {
                            return false;
                        }
                        if (kind != null && kind != Kind.SYSTEM_READ && kind != Kind.RAW_GRID && kind != Kind.OBJECT) {
                            found[0] = kind;
                            return true;
                        }
                        return false;
                    }, query.specs(), memo)) {
                return refused("statement " + i + " another way: " + found[0]);
            }
            if (i < body.size() - 1 && !plansNoStep(statement, body.subList(0, i), ctx)
                    && !(!(statement instanceof TypedLet) && StatementEffects.isStoreWrite(Lets.bare(statement)))
                    // a CHECK before the last (an unread let's value is one, the user's ruling of 2026-11-02; a no-assert
                    // program's run as it is, the user, 2026-11-06: "do all including last row"): a value statement run
                    // for what it raises, its rows dropped, as the walk runs it
                    && !check(statement, systemClasses, ctx, query, memo)
                    && !(statement instanceof TypedLet let && LetPlanner.kind(let, body, i, query.specs(),
                            new HashMap<>(), n -> false) == LetPlanner.Kind.STAGED)
                    && !(statement instanceof TypedLet let && LetPlanner.kind(let, body, i, query.specs(),
                            new HashMap<>(), n -> false) == LetPlanner.Kind.VALUE
                            && !StatementEffects.reaches(let.value(), ProductStatements::toOneReader, query.specs(),
                                    new HashMap<>()))) {
                // a VALUE let (read by a later statement: an unread one runs as its own statement before this rule,
                // LetPlanner.unreadAsStatements) folds into its readers as the product planner folds a query's let
                // (2026-11-03, the core program: `let r = executeInDb(...); $r.rows`); not one holding a to-one reader,
                // whose exactly-one contract the walk holds at the answer's root alone (M5f's audit B4)
                return refused("statement " + i + " plans a step before the last");
            }
        }
        if (body.isEmpty()) {
            return refused("empty body");
        }
        return answered(query, Lets.bare(body.get(body.size() - 1))) || refused("the answer: " + answerRefusal(query,
                Lets.bare(body.get(body.size() - 1))));
    }

    /** Why {@link #answered} refuses {@code answer} (the measurement's text). */
    private static String answerRefusal(TypedQuery query, TypedSpec answer) {
        Type type = answer.info().type();
        if (type instanceof Type.ClassType ct) {
            return "an object (" + ct.fqn() + ")";
        }
        if (!Type.isRelation(type) && StatementEffects.reaches(answer, n -> n instanceof TypedGetAll, query.specs(),
                new HashMap<>())) {
            return "a value through a user class's extent";
        }
        return "a to-one reader below the root";
    }

    /** Whether {@code answer}, a program's last statement, is answered on the program road as the walk answers it:
     *  a relation or a value that reads no class's extent, not an object, a to-one reader only as its own root. */
    private static boolean answered(TypedQuery query, TypedSpec answer) {
        Type type = answer.info().type();
        if (type instanceof Type.ClassType ct && !PlatformTypes.isVariant(ct) && !PlatformTypes.isNil(ct)
                && !PlatformTypes.ANY.equals(ct.fqn())
                && !(answer instanceof com.legend.compiler.spec.typed.TypedNewInstance)) {
            // an Any-typed answer (a raw grid's cells) and a constructed instance are the product's answers too
            // (2026-11-03, the core program: ExecuteInDbProbeCountTest, InstanceIdentityTest#plainLaneUnperturbed)
            return false;
        }
        // a value through a USER class's extent stays the walk's (M5f's audit B1: the corpus's
        // otherwiseTestQualifierPropertyConstantExpression, a filter over an extent no mapping resolves, which the walk
        // runs as a value statement and the store resolver refuses); one through the system metamodel store's
        // (Class.all()->size(), a mapping's set implementations) is the product's statement, routed to the system
        // database (2026-11-03, the core program: the metamodel stores' tests, measured on the program road)
        Set<String> systemClasses = systemClasses(query.context());
        if (!Type.isRelation(type) && StatementEffects.reaches(answer, n -> n instanceof TypedGetAll all
                && !systemClasses.contains(all.classFqn()) && !SystemMetamodel.isSystemElement(all.classFqn()),
                query.specs(), new HashMap<>())) {
            return false;
        }
        // a to-one reader only as the answer's root: one reached through a let or a user function is not the walk's
        // planned root the router reads (M5f's audit B4)
        List<TypedSpec> below = ProductStatements.toOneReader(answer) ? answer.children() : List.of(answer);
        Map<FunctionId, Boolean> readers = new HashMap<>();
        for (TypedSpec n : below) {
            if (StatementEffects.reaches(n, ProductStatements::toOneReader, query.specs(), readers)) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code statement}, before a body's last, is a CHECK: a value statement run for what it raises, its rows
     *  dropped -- the program road's check step, the walk's value statement (an unread let's value is one, the user's
     *  ruling of 2026-11-02): not a let, holding no kind the program road plans another way ({@link #anotherWay}). The
     *  caller has refused a test kind already. */
    private static boolean check(TypedSpec statement, Set<String> systemClasses, ModelContext ctx, TypedQuery query,
            Map<FunctionId, Boolean> memo) {
        return !(statement instanceof TypedLet)
                && !StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                        n -> anotherWay(n, systemClasses, ctx) != null, query.specs(), memo);
    }

    /** Whether {@code statement}, before a body's last, plans no step on the program road and sends nothing on the
     *  walk: a let of a literal (folded over {@code lets}) or of a handle, a String or Boolean literal, an inert
     *  {@code print}, a handle (M5f's audit B3: any other value statement is a check the walk holds and labels). */
    static boolean plansNoStep(TypedSpec statement, List<TypedSpec> lets, ModelContext ctx) {
        if (statement instanceof TypedLet let) {
            return Literals.fold(let.value(), lets) != null || BodyPlanner.handleType(let.value(), ctx) != null;
        }
        return literal(statement) || BodyPlanner.handleType(statement, ctx) != null
                || statement instanceof TypedNativeCall call && NativeFn.Effect.isInertDiagnostic(call.callee().id());
    }

    /** The kinds the program road plans another way than the walk yet, each with the kind census's name. */
    enum Kind {
        EXECUTE("execute"), STAGED("staged"), RAW_GRID("raw-grid"), STORE_WRITE("store-write"),
        SYSTEM_READ("system-read"), TEMPORAL("temporal"), OBJECT("object"), GRAPH("graph"),
        /** The one inline shape the splice runs at planning ({@link #eagerInlineExecute}). */
        EAGER_INLINE_EXECUTE("eager-inline-execute"),
        /** The test-data-generation lane's census carrier ({@code getRelationalCSVDataFromQuery}, the checker's
         *  {@link TypedCsvCensus}): the walk folds it to instance literals in Java from the model
         *  ({@code TestDataGenerationNatives.foldCensus}) before the statement is planned; the program road has no
         *  such fold (M10a's third cut, caught by the corpus: its three census tests moved on the strength of their
         *  system reads alone and walled at lowering). */
        CSV_CENSUS("csv-census"),
        /** The test-data generator's carrier ({@code generateTestData}, {@code generateSeedDataString}: the checker's
         *  {@link TypedTestDataGen} of the {@code generate} and {@code seedString} flavours): the walk folds it by
         *  running the generator's fetches through the database ({@code TestDataGenerationNatives.foldCensus}); the
         *  program road has no such step yet (T8, the plan's M10c/M10d). The {@code plan} flavour is a value the
         *  planner renders ({@code generatorPlanText}) and no kind (M10b). */
        TDG_GENERATOR("tdg-generator");

        private final String tag;

        Kind(String tag) {
            this.tag = tag;
        }

        /** The kind as the census names it. */
        String tag() {
            return tag;
        }
    }

    /** The kind {@code n} is that the program road plans another way than the walk yet, or null: an
     *  {@code execute(...)}, staged rows, a raw grid ({@code executeInDb}'s rows, typed as a raw-SQL relation: T8,
     *  T12), a store write; a read of the system metamodel store (M4) -- the extent of a class its mapping binds
     *  ({@code systemClasses}), a table of it, one of its functions, an element used as a value (a mapping, a store:
     *  its instance is the store's row: {@link #systemElementReference}); an object built, a graph answered, a
     *  temporal class read (their ids, layouts and the exists-join form converge on the product's with MV6); the
     *  test-data-generation lane's census carrier, folded in Java on the walk ({@link Kind#CSV_CENSUS}). */
    static @Nullable Kind anotherWay(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        if (n instanceof TypedCsvCensus) {
            return Kind.CSV_CENSUS;
        }
        if (n instanceof TypedTestDataGen g && !"plan".equals(g.flavor())) {
            return Kind.TDG_GENERATOR;
        }
        if (n instanceof TypedNativeCall call) {
            if (NativeFn.Handle.runsAFrame(call.callee().id())) {
                return Kind.EXECUTE;
            }
            if (PlatformTypes.SCAN_RELATIONS.equals(call.callee().qualifiedName())
                    || PlatformTypes.SCAN_COLUMNS.equals(call.callee().qualifiedName())) {
                // a lineage scan: a handle whose rows ride the query and join the metamodel tables -- a read of the
                // system store (the walk routes its sides to the system session), so a body whose only store read is
                // the scan is system-routed and its program runs there (the test-is-the-product plan's M10c item 3)
                return Kind.SYSTEM_READ;
            }
            if (Pure.Lite.STAGE.equals(call.callee().qualifiedName())) {
                return Kind.STAGED;
            }
        }
        if (n instanceof TypedRawSqlRelation) {
            return Kind.RAW_GRID;
        }
        if (StatementEffects.isStoreWrite(n)) {
            return Kind.STORE_WRITE;
        }
        if (n instanceof TypedGetAll all) {
            if (systemClasses.contains(all.classFqn()) || SystemMetamodel.isSystemElement(all.classFqn())) {
                return Kind.SYSTEM_READ;
            }
            if (Temporal.strategyOf(ctx, all.classFqn()) != null) {
                return Kind.TEMPORAL;
            }
        }
        if (n instanceof TypedTableReference table && SystemMetamodel.STORE_FQN.equals(table.store())
                || n instanceof TypedUserCall call && SystemMetamodel.functionIds().contains(call.callee().id())
                || systemElementReference(n, systemClasses, ctx)) {
            return Kind.SYSTEM_READ;
        }
        if (n instanceof TypedNewInstance || n instanceof TypedCopyInstance) {
            return Kind.OBJECT;
        }
        if (n instanceof TypedGraphFetch || n instanceof TypedSerialize || n instanceof TypedSerializeGraph) {
            return Kind.GRAPH;
        }
        return null;
    }

    /** An element used as a VALUE whose row is the system store's (the resolver's own criterion,
     *  {@code ElementReferences.trackedElementClass}): a reference typed by a bare tracked class the system mapping
     *  binds -- a mapping, a store, a function. A class reference ({@code Class<X>}: {@code instanceOf}'s type
     *  argument, {@code cast}'s) is a value on every road and reads no store. */
    static boolean systemElementReference(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        return n instanceof TypedPackageableRef pr && pr.info().type() instanceof Type.ClassType ct
                && ctx.tracksClassifier(ct.fqn()) && systemClasses.contains(ct.fqn());
    }

    /** The kind {@code n} is that keeps an assert statement or a value let of a HOST-judged program off the program
     *  road, named, or null: a read of the system metamodel store (its session is the system database's, which a
     *  moved body's program does not open until M10a), a store write (run where its side is planned), the census
     *  carrier the walk folds in Java ({@link Kind#CSV_CENSUS}), or the eager inline execute (a run at planning).
     *  Every other
     *  kind in such a statement is admitted since M9c: the asserts' sides are the walk's own plans on both roads
     *  ({@code HostAsserts} plans them with the walk's planner, let prefix and frames) and a value let sends nothing
     *  on either road, so an object, a graph, a temporal read or a class reference inside one diverges nowhere. */
    static @Nullable Kind sideKind(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        return sideKind(n, systemClasses, ctx, false);
    }

    /** {@link #sideKind(TypedSpec, Set, ModelContext)}; {@code systemRouted}: the body's program runs on the graph's
     *  system database ({@link #systemRouted}), so a system read is no longer a kind that keeps it on the walk
     *  (M10a). */
    static @Nullable Kind sideKind(TypedSpec n, Set<String> systemClasses, ModelContext ctx, boolean systemRouted) {
        Kind kind = anotherWay(n, systemClasses, ctx);
        if (kind == Kind.SYSTEM_READ) {
            // a side or a value let reading the system store: its side is routed to the graph's system database and
            // runs there beside the body's own (per-statement routing, 2026-11-03; a system-routed body's one target
            // since M10a) -- a statement reading BOTH stores is refused by the class rules (Stores.BOTH)
            return eagerInlineExecute(n) ? Kind.EAGER_INLINE_EXECUTE : null;
        }
        if (kind == Kind.STORE_WRITE || kind == Kind.CSV_CENSUS || kind == Kind.TDG_GENERATOR) {
            // a store write inside a side or a value let: the side planner runs it where it plans it (the walk at
            // the assert's turn, inside the body's transaction; the program road before the program ran), so it is
            // the walk's (the M9c audit's F2); the census and generator carriers: folded by the walk alone (M10a, M10b)
            return kind;
        }
        return eagerInlineExecute(n) ? Kind.EAGER_INLINE_EXECUTE : null;
    }

    /** The kind {@code n} is that keeps a FRAME's query off the program road, named, or null: the kinds the product's
     *  lowering plans another way than the walk's ({@link #anotherWay}) but a temporal class read and a graph fetch,
     *  whose eager runs the two lowerings spell alike (M9d for a temporal read; M10c item 12 for a graph fetch and its
     *  serialize -- the frame's one JSON value, the same statement on both roads -- pinned byte-identical by
     *  {@code BodyRouterParityTest} on every in-process database, a tree's qualifier arguments bound on the product's
     *  road since that item). */
    static @Nullable Kind frameQueryKind(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        Kind kind = anotherWay(n, systemClasses, ctx);
        return kind == Kind.TEMPORAL || kind == Kind.GRAPH ? null : kind;
    }

    /** The classes the system metamodel's mapping binds: their extents are the system store's rows. */
    static Set<String> systemClasses(ModelContext ctx) {
        MappingDefinition system = ctx.findMapping(SystemMetamodel.MAPPING_FQN).orElseThrow(() ->
                new IllegalStateException("the system metamodel's mapping is not in the model -- injection"
                        + " regressed"));
        Set<String> classes = new HashSet<>();
        for (MappingDefinition.ClassBinding binding : system.classBindings()) {
            classes.add(binding.classFqn());
        }
        return classes;
    }

    private static boolean effectProgram(TypedQuery query, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        List<TypedSpec> body = query.body();
        Map<FunctionId, Boolean> storeMemo = new HashMap<>();
        ElementConsumption consumption = new ElementConsumption(body, systemClasses(ctx), ctx, query.specs());
        boolean anEffect = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && value instanceof TypedNativeCall call
                    && NativeFn.Effect.isDbEffect(call.callee().id())) {
                if (!literalEffect(call, body.subList(0, i))) {
                    return false;
                }
                if (statement instanceof TypedLet let && !ConnectionLets.onlyConnectionReads(body, i + 1, let.name())) {
                    // an effect let a later statement reads: the test door raises the walk's own refusal at this
                    // statement ("reading an executeInDb result binding ... is not supported", TestKinds.effect), as the
                    // walk raises it at the let's turn -- nothing after it is reached on either road (2026-11-04, the core
                    // program: ExecuteInDbDuckDbTest's effectfulLetReadIsLoud)
                    return true;
                }
                anEffect = true;
                continue;
            }
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && !(statement instanceof TypedLet)
                    && seedFormEffectMap(value, body.subList(0, i))) {
                // a seed form mapped over an effect (M10c item 7c)
                anEffect = true;
                continue;
            }
            // every other statement holds no test kind (no seeds, no assert, no effect inside it) and reads no store
            if (!kinds.isEmpty() || readsAStore(statement, query, storeMemo)) {
                return false;
            }
            if (statement instanceof TypedLet && i < body.size() - 1
                    && (consumption.consumedElementLet(i) || Seeds.seedFormCall(value, body.subList(0, i)) != null)) {
                // the corpus's setup helpers unfolded (M10c item 7c): the store and the runtime bound and consumed (the
                // element-consumption analysis: no read on either road), and the seed form bound (planning-time texts
                // the map after it spells) -- value lets the program splices
                continue;
            }
            if (!(statement instanceof TypedLet) && i < body.size() - 1
                    && statement instanceof com.legend.compiler.spec.typed.TypedRawSqlRelation) {
                // a RAW READ as a statement (executeInDb('select ...', $c): what an unread let of one is, the user's
                // ruling of 2026-11-02, LetPlanner.unreadAsStatements) -- a check at its turn on both roads, its rows
                // dropped: upstream runs it there, and what it raises fails the test
                continue;
            }
            if (statement instanceof TypedLet && i < body.size() - 1
                    && Literals.fold(value, body.subList(0, i)) instanceof String) {
                // a text the planner folds ('insert into ... ' + '(' + ...: a literal concatenation), which a later
                // effect reads as its text -- the effect's own rule folds it the same way (literalEffect); the corpus
                // burn-down, 2026-11-06: the aggregation-aware setUp's calendar insert
                continue;
            }
            boolean inert = value instanceof TypedNativeCall call
                    && NativeFn.Effect.isInertDiagnostic(call.callee().id());
            if (!inert && !literal(value) && BodyPlanner.handleType(value, ctx) == null) {
                // a computed value: the product's lowering would answer it where the walk sends its own statement
                // (the M5 audit's S3), a class of its own
                return false;
            }
        }
        return anEffect;
    }

    /** Whether {@code value} is a SEED FORM mapped over one db effect ({@code $sqls->map(sql | executeInDb($sql,
     *  $connection))} with {@code sqls} a {@code setUpDataSQLs} form whose data folds over {@code lets}: the corpus's
     *  setup helpers, unfolded into the body by the resolver's statement inliner): the test door plans its texts as the
     *  effect script the walk sends (M10c item 7c; {@code Seeds.seedForm}, one reader for both roads). */
    /** Whether {@code statement} is a database effect the test door writes as a statement of its own: not a let, and
     *  either a literal effect ({@link #literalEffect}) or a text form mapped over one ({@link #seedFormEffectMap}). */
    private static boolean plannedEffect(TypedSpec statement, List<TypedSpec> lets) {
        if (statement instanceof TypedLet) {
            return false;
        }
        if (statement instanceof TypedNativeCall call && NativeFn.Effect.isDbEffect(call.callee().id())) {
            return literalEffect(call, lets);
        }
        return seedFormEffectMap(statement, lets);
    }

    static boolean seedFormEffectMap(TypedSpec value, List<TypedSpec> lets) {
        // a text form whose texts the planner spells -- a seed form's (item 7c) or a DDL text collection's (the corpus
        // burn-down, 2026-11-06) -- mapped over the one effect the door writes
        return value instanceof com.legend.compiler.spec.typed.TypedMap tm
                && (Seeds.seedForm(tm, lets) != null || Seeds.ddlTextSource(tm, lets) != null)
                && Seeds.oneEffectMapper(tm, lets);
    }

    /** Whether {@code query}'s body, holding no assert, has a statement that is a database effect whose String
     *  arguments are not all literals ({@link #literalEffect}): a text the database would compute before the effect is
     *  written -- a text hole, which lite does not have. */
    private static boolean computedEffectText(TypedQuery query, QueryDoor.Scan scan) {
        List<TypedSpec> body = query.body();
        boolean computed = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (scan.kinds(statement).contains(QueryDoor.TestOnly.ASSERT)) {
                return false;
            }
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            computed |= value instanceof TypedNativeCall call && NativeFn.Effect.isDbEffect(call.callee().id())
                    && !literalEffect(call, body.subList(0, i));
        }
        return computed;
    }

    /** Whether {@code value} is a String or Boolean literal: a value both roads answer with no statement sent. */
    static boolean literal(TypedSpec value) {
        return value instanceof TypedCString || value instanceof TypedCBoolean;
    }

    /** Whether {@code call}, a database effect, is one the test door writes: each String argument a literal (folded
     *  over {@code lets}, the statements before it) -- a {@code loadCsvToDbTable}'s, the CSV file's resource path, which
     *  its statement names for the database to read (the user's ruling, 2026-11-06). */
    static boolean literalEffect(TypedNativeCall call, List<TypedSpec> lets) {
        for (TypedSpec arg : EffectPlanner.stringArgs(call)) {
            if (!(Literals.fold(arg, lets) instanceof String)) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code statement} reaches a store read or write: a class's extent, a table, a {@code from()}, a store
     *  write, staged rows (through the user functions it calls). */
    private static boolean readsAStore(TypedSpec statement, TypedQuery query, Map<FunctionId, Boolean> memo) {
        return StatementEffects.reaches(statement, n -> n instanceof TypedGetAll || n instanceof TypedTableReference
                || n instanceof TypedFrom || StatementEffects.isStoreWrite(n)
                || n instanceof TypedNativeCall stage && Pure.Lite.STAGE.equals(stage.callee().qualifiedName()),
                query.specs(), memo);
    }
}
