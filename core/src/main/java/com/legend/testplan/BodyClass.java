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
     * the plan, the quantified forms, the cell
     * pool). The database judge's bodies stay on the walk (its fused group is composed at planning from M7).
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
     * as a literal). Not admitted, each a later sub-step: an effect let, staged rows and raw grids, the test-data
     * generator, {@code assertError}, a seeded runtime.
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
        if (QueryDoor.declaresTestData(runtime, body.context())) {
            return null;
        }
        QueryDoor.Scan scan = new QueryDoor.Scan(body, runtime);
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
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (readsStore(statement, n -> userStoreRead(n, systemClasses, ctx), query.specs(), userMemo)) {
                return false;
            }
            if (bindsASystemElement(statement, i, body, systemClasses, ctx)) {
                system |= !bindingConsumed((TypedLet) statement, i, body, query.specs());
                continue;
            }
            system |= readsStore(statement, n -> anotherWay(n, systemClasses, ctx) == Kind.SYSTEM_READ, query.specs(),
                    systemMemo);
        }
        return system;
    }

    /** Whether {@code statement}, at {@code i} in {@code body}, is a let binding a system element as a value
     *  ({@code let m = my::Mapping}: {@link #systemElementReference}), before the last statement. */
    static boolean bindsASystemElement(TypedSpec statement, int i, List<TypedSpec> body, Set<String> systemClasses,
            ModelContext ctx) {
        return statement instanceof TypedLet let && i < body.size() - 1
                && let.value() instanceof TypedPackageableRef ref && systemElementReference(ref, systemClasses, ctx);
    }

    /**
     * Whether the element {@code let} binds is CONSUMED (M10b): no later statement reads the variable as a value --
     * {@code $m.name}, {@code $m} handed to a helper -- outside a lambda literal and outside a planning-time native's
     * arguments ({@link #readsStore}'s two exclusions: a mapping handed to {@code toSQLString} or
     * {@code executionPlan} is consumed when the text or the plan is made, never read as a row). A consumed binding
     * reads no store: the body is not system-routed by it, and the class rules see no system read in it.
     */
    static boolean bindingConsumed(TypedLet let, int i, List<TypedSpec> body, SpecCompiler specs) {
        for (int j = i + 1; j < body.size(); j++) {
            if (readsStore(body.get(j), n -> n instanceof TypedVariable v && let.name().equals(v.name()), specs,
                    new HashMap<>())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code n} reaches a node {@code read} names (a store's read) through the user functions it calls
     * ({@code StatementEffects.reaches}' walk, one memo per callee), with two places left aside: a lambda literal's
     * body (a lambda is a value; the native that runs one against a store -- an execute, a handle, a generator -- is
     * itself the read {@link #userStoreRead} names) and the arguments of a planning-time native (a handle's or a Java
     * routine's, {@code NativeFn.Handle}, {@code NativeFn.JavaRoutine}: a mapping or a runtime handed to
     * {@code executionPlan(...)} or {@code toSQLString(...)} is consumed when the plan or the text is made, never read
     * as a row of the system store).
     */
    static boolean readsStore(TypedSpec n, java.util.function.Predicate<TypedSpec> read, SpecCompiler specs,
            Map<FunctionId, Boolean> memo) {
        if (n instanceof TypedLambda) {
            return false;
        }
        if (read.test(n)) {
            return true;
        }
        if (n instanceof TypedNativeCall call && (NativeFn.Handle.of(call.callee().id()).isPresent()
                || NativeFn.JavaRoutine.of(call.callee().id()).isPresent())) {
            return false;
        }
        if (n instanceof TypedUserCall uc) {
            FunctionId key = uc.callee().id();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in progress: a cycle scores false
                boolean reaches = false;
                try {
                    for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
                        if (readsStore(stmt, read, specs, memo)) {
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
            if (readsStore(c, read, specs, memo)) {
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
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            boolean last = i == body.size() - 1;
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
                continue;
            }
            if (statement instanceof TypedLet let && !last) {
                if (LetPlanner.badAliasIndex(let.value(), relationFrame)) {
                    // $r.values->at(k>0) over a relation frame: the walk raises it at the let's turn (the M9a audit's
                    // S1: a rule decides by the shape, never by raising)
                    return false;
                }
                LetPlanner.Kind kind = LetPlanner.kind(let, body, i, query.specs(), effectMemo, relationFrame);
                if (kind == LetPlanner.Kind.FRAME && kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE))) {
                    // the frame's query holds no kind planned another way and reads no earlier frame, by name or
                    // through a tainted let (N1, F2); its mapping and runtime arguments are the frame's own references
                    TypedNativeCall call = (TypedNativeCall) LetPlanner.rhs(let);
                    if (StatementEffects.reaches(call.args().get(0), n -> frameQueryKind(n, systemClasses, ctx) != null
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
                // a let binding a system element the body never reads as a value: a value let the program splices,
                // no system read (M10b, as the assert program's rule)
                boolean consumedBinding = bindsASystemElement(statement, i, body, systemClasses, ctx)
                        && bindingConsumed(let, i, body, query.specs());
                if (kind != LetPlanner.Kind.VALUE || !kinds.isEmpty()
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
            boolean assertRoot = kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT))
                    && !(statement instanceof TypedLet);
            if (!assertRoot && !kinds.isEmpty()
                    || StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                            n -> sideKind(n, systemClasses, ctx) != null, query.specs(), sideKindMemo)) {
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (!last && !plansNoStep(statement, body.subList(0, i), ctx)) {
                return false;
            }
            aFrame |= StatementEffects.reaches(statement, n -> n instanceof TypedNativeCall call
                    && NativeFn.Handle.runsAFrame(call.callee().id()), query.specs(), runsAFrameMemo);
        }
        if (!anAssert || !aFrame) {
            return false;
        }
        TypedSpec last = body.get(body.size() - 1);
        return scan.kinds(last).contains(QueryDoor.TestOnly.ASSERT)
                || plansNoStep(last, body.subList(0, body.size() - 1), ctx);
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

    private static boolean assertProgram(TypedQuery query, QueryDoor.Scan scan, boolean systemRouted) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        Map<FunctionId, Boolean> memo = new HashMap<>();
        Map<FunctionId, Boolean> effectMemo = new HashMap<>();
        List<TypedSpec> body = query.body();
        boolean anAssert = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            boolean assertRoot = kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT))
                    && !(statement instanceof TypedLet);
            // a let binding a system element the body never reads as a value (consumed by toSQLString, by
            // executionPlan): a value let the program splices, no system read (M10b; the same test routes the body)
            boolean consumedBinding = bindsASystemElement(statement, i, body, systemClasses, ctx)
                    && bindingConsumed((TypedLet) statement, i, body, query.specs());
            if (!assertRoot && !kinds.isEmpty() || StatementEffects.containsTdgGenerator(statement)
                    || !consumedBinding && StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                            n -> sideKind(n, systemClasses, ctx, systemRouted) != null, query.specs(), memo)) {
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (i < body.size() - 1 && !plansNoStep(statement, body.subList(0, i), ctx)
                    && !(statement instanceof TypedLet let && LetPlanner.kind(let, body, i, query.specs(),
                            effectMemo, name -> false) == LetPlanner.Kind.VALUE)) {
                return false;
            }
        }
        if (!anAssert) {
            return false;
        }
        TypedSpec last = body.get(body.size() - 1);
        // an assert as the last statement answers its verdict, as the walk answers it; any other last statement sends
        // nothing (a literal, a handle, an inert print): a query planned after the asserts could fail planning before
        // they ran and reported, where the walk fails it at its turn
        return scan.kinds(last).contains(QueryDoor.TestOnly.ASSERT)
                || plansNoStep(last, body.subList(0, body.size() - 1), ctx);
    }

    private static boolean queryProgram(TypedQuery query, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        Map<FunctionId, Boolean> memo = new HashMap<>();
        List<TypedSpec> body = query.body();
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (!scan.kinds(statement).isEmpty() || StatementEffects.containsTdgGenerator(statement)
                    || StatementEffects.reaches(statement, n -> plannedAnotherWay(n, systemClasses, ctx),
                            query.specs(), memo)) {
                return false;
            }
            if (i < body.size() - 1 && !plansNoStep(statement, body.subList(0, i), ctx)) {
                return false;
            }
        }
        if (body.isEmpty()) {
            return false;
        }
        return answered(query, Lets.bare(body.get(body.size() - 1)));
    }

    /** Whether {@code answer}, a program's last statement, is answered on the program road as the walk answers it:
     *  a relation or a value that reads no class's extent, not an object, a to-one reader only as its own root. */
    private static boolean answered(TypedQuery query, TypedSpec answer) {
        Type type = answer.info().type();
        if (type instanceof Type.ClassType ct && !PlatformTypes.isVariant(ct) && !PlatformTypes.isNil(ct)) {
            return false;
        }
        if (!Type.isRelation(type) && StatementEffects.reaches(answer, n -> n instanceof TypedGetAll, query.specs(),
                new HashMap<>())) {
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

    /** Whether {@code n} is a kind the program road plans another way than the walk yet ({@link #anotherWay}): the
     *  test for a statement the PRODUCT road plans (a query program's answer, a frame's eager run). */
    private static boolean plannedAnotherWay(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        return anotherWay(n, systemClasses, ctx) != null;
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
        if (kind == Kind.SYSTEM_READ && systemRouted) {
            return eagerInlineExecute(n) ? Kind.EAGER_INLINE_EXECUTE : null;
        }
        if (kind == Kind.SYSTEM_READ || kind == Kind.STORE_WRITE || kind == Kind.CSV_CENSUS
                || kind == Kind.TDG_GENERATOR) {
            // a store write inside a side or a value let: the side planner runs it where it plans it (the walk at
            // the assert's turn, inside the body's transaction; the program road before the program ran), so it is
            // the walk's (the M9c audit's F2); the census and generator carriers: folded by the walk alone (M10a, M10b)
            return kind;
        }
        return eagerInlineExecute(n) ? Kind.EAGER_INLINE_EXECUTE : null;
    }

    /** The kind {@code n} is that keeps a FRAME's query off the program road, named, or null: the kinds the product's
     *  lowering plans another way than the walk's ({@link #anotherWay}) but a temporal class read, whose eager run
     *  the two lowerings spell alike (M9d: pinned byte-identical by {@code BodyRouterParityTest} on every in-process
     *  database; the walk's exists-join form is the product's since D5). */
    static @Nullable Kind frameQueryKind(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        Kind kind = anotherWay(n, systemClasses, ctx);
        return kind == Kind.TEMPORAL ? null : kind;
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
        boolean anEffect = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && value instanceof TypedNativeCall call
                    && NativeFn.Effect.isDbEffect(call.callee().id())) {
                if (!literalEffect(call, body.subList(0, i))
                        || statement instanceof TypedLet let
                                && !ConnectionLets.onlyConnectionReads(body, i + 1, let.name())) {
                    return false;
                }
                anEffect = true;
                continue;
            }
            // every other statement holds no test kind (no seeds, no assert, no effect inside it) and reads no store
            if (!kinds.isEmpty() || readsAStore(statement, query, storeMemo)) {
                return false;
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

    /** Whether {@code value} is a String or Boolean literal: a value both roads answer with no statement sent. */
    static boolean literal(TypedSpec value) {
        return value instanceof TypedCString || value instanceof TypedCBoolean;
    }

    /** Whether {@code call}, a database effect, is one the test door writes: each String argument a literal (folded
     *  over {@code lets}, the statements before it), and not {@code loadCsvToDbTable} (its CSV a test resource). */
    static boolean literalEffect(TypedNativeCall call, List<TypedSpec> lets) {
        if (NativeFn.Effect.of(call.callee().id()).orElse(null) == NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
            return false;
        }
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
