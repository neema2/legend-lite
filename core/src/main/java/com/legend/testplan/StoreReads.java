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
 * What a test body's statements READ and WRITE, for its class ({@link BodyClass}): the stores a statement reads (none,
 * the system metamodel's, the user's, both), a read of the system metamodel's elements, a side's kind -- facts read from
 * the typed tree, nothing evaluated. Split out of {@code BodyClass} (v2 item 8, 2026-11-16: no judge class over 1,000
 * lines), the code moved unchanged.
 */
final class StoreReads {

    private StoreReads() {
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
    static TypedSpec framesAsTheirQueries(TypedSpec n, java.util.Set<TypedSpec> frameQueries) {
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
    static void lambdasUnder(TypedSpec n, java.util.Set<TypedSpec> out) {
        if (n instanceof TypedLambda) {
            out.add(n);
        }
        for (TypedSpec c : n.children()) {
            lambdasUnder(c, out);
        }
    }

    /** {@code n} with each variable a let of {@code lets} binds replaced by the let's value (itself read through the
     *  lets before it), for the store scan alone; bounded, a cycle impossible in a body's lexical lets. */
    static TypedSpec throughLets(TypedSpec n, List<TypedSpec> lets, int depth) {
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
     * planning, so the walk reads its source alone (as {@link FramePrograms#framesAndHandlesAsTheirQueries} does).
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
                } catch (com.legend.error.TypeInferenceException e) {
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
        // a raw SQL read, a JDBC catalog grid and a database effect act on the CONNECTION's database, the user's (the
        // walk deletion, 2026-11-06: a body whose metamodel read is the connection let alone was classed system-routed,
        // and its effects and raw reads would have run on the system database)
        return n instanceof TypedFrom
                || n instanceof TypedRawSqlRelation
                // a planned statement's value (B2d.2: the test-data generator's text, a comparison over its slice) reads
                // the body's database
                || n instanceof com.legend.compiler.spec.typed.TypedPlannedValue
                || n instanceof com.legend.compiler.spec.typed.TypedCatalogGrid
                || n instanceof TypedNativeCall call && (NativeFn.Handle.isExecute(call.callee().id())
                        || NativeFn.Handle.runsAFrame(call.callee().id())
                        || NativeFn.Effect.isDbEffect(call.callee().id())
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

    /** The kinds the program road plans another way than the walk yet, each with the kind census's name. */
    enum Kind {
        EXECUTE("execute"), STAGED("staged"), RAW_GRID("raw-grid"), STORE_WRITE("store-write"),
        SYSTEM_READ("system-read"), TEMPORAL("temporal"), OBJECT("object"), GRAPH("graph"),
        /** The one inline shape the splice runs at planning ({@link FramePrograms#eagerInlineExecute}). */
        EAGER_INLINE_EXECUTE("eager-inline-execute"),
        /** The test-data-generation lane's census carrier ({@code getRelationalCSVDataFromQuery}, the checker's
         *  {@link TypedCsvCensus}): the program road has no fold for it, so a body holding it is refused by name (its
         *  Java fold went with the walk, 2026-11-06; the tests skipped by a ruled register row). */
        CSV_CENSUS("csv-census"),
        /** A test-data generator's carrier the program road does not plan (the checker's {@link TypedTestDataGen}): a
         *  body holding one is refused by name. Since B2d.2 (2026-11-11) a {@code generateTestData} read by its
         *  {@code sqls} or its {@code dataCsvString} is planned before the body is classed ({@code GeneratedTestData}:
         *  none reaches the class rules); what remains is {@code generateSeedDataString} (the {@code seedString}
         *  flavour) and a generator read another way. The {@code plan} flavour is a value the planner renders
         *  ({@code generatorPlanText}) and no kind (M10b). */
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
     *  test-data-generation lane's census carrier ({@link Kind#CSV_CENSUS}). */
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
     *  carrier ({@link Kind#CSV_CENSUS}), or the eager inline execute (a run at planning).
     *  Every other
     *  kind in such a statement is admitted since M9c: the asserts' sides are the walk's own plans on both roads
     *  ({@code HostAsserts} plans them with the walk's planner, let prefix and frames) and a value let sends nothing
     *  on either road, so an object, a graph, a temporal read or a class reference inside one diverges nowhere. */
    static @Nullable Kind sideKind(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        return sideKind(n, systemClasses, ctx, false);
    }

    /** {@link #sideKind(TypedSpec, Set, ModelContext)}; {@code systemRouted}: the body's program runs on the graph's
     *  system database ({@link BodyClass#systemRouted}), so a system read is no longer a kind that keeps it on the walk
     *  (M10a). */
    static @Nullable Kind sideKind(TypedSpec n, Set<String> systemClasses, ModelContext ctx, boolean systemRouted) {
        Kind kind = anotherWay(n, systemClasses, ctx);
        if (kind == Kind.SYSTEM_READ) {
            // a side or a value let reading the system store: its side is routed to the graph's system database and
            // runs there beside the body's own (per-statement routing, 2026-11-03; a system-routed body's one target
            // since M10a) -- a statement reading BOTH stores is refused by the class rules (Stores.BOTH)
            // an eager inline execute (size(execute(...))) is the test door's check step at its assert's turn and the
            // splice's folded 1 (2026-11-06): no kind that keeps the body on the walk
            return null;
        }
        if (kind == Kind.STORE_WRITE || kind == Kind.CSV_CENSUS || kind == Kind.TDG_GENERATOR) {
            // a store write inside a side or a value let: the side planner runs it where it plans it (the walk at
            // the assert's turn, inside the body's transaction; the program road before the program ran), so it is
            // the walk's (the M9c audit's F2); the census and generator carriers: folded by the walk alone (M10a, M10b)
            return kind;
        }
        return null;
    }

    /** The kind {@code n} is that keeps a FRAME's query off the program road, named, or null: the kinds the product's
     *  lowering plans another way than the walk's ({@link #anotherWay}) but a temporal class read and a graph fetch,
     *  whose eager runs the two lowerings spell alike (M9d for a temporal read; M10c item 12 for a graph fetch and its
     *  serialize -- the frame's one JSON value, the same statement on both roads -- pinned byte-identical by
     *  {@code TestPlannerParityTest} on every in-process database, a tree's qualifier arguments bound on the product's
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

    /** Whether {@code statement} reaches a store read or write: a class's extent, a table, a {@code from()}, a store
     *  write, staged rows (through the user functions it calls). */
    static boolean readsAStore(TypedSpec statement, TypedQuery query, Map<FunctionId, Boolean> memo) {
        return StatementEffects.reaches(statement, n -> n instanceof TypedGetAll || n instanceof TypedTableReference
                || n instanceof TypedFrom || StatementEffects.isStoreWrite(n)
                || n instanceof TypedNativeCall stage && Pure.Lite.STAGE.equals(stage.callee().qualifiedName()),
                query.specs(), memo);
    }
}
