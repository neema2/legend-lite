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
import com.legend.compiler.spec.typed.TypedFrom;
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
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.FunctionId;
import com.legend.model.MappingDefinition;
import com.legend.planner.BodyPlanner;
import com.legend.planner.ConnectionLets;
import com.legend.planner.LetPlanner;
import com.legend.planner.FramePlanner;
import com.legend.planner.PlanEnv;
import com.legend.planner.ProductStatements;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.typed.TypedFrom;
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
     * planning before they ran, where the walk fails it at its turn). No statement holds another test kind
     * or a kind planned another way (the query program's list). Each assert is planned by its judge before the
     * program is ({@link TestDoor.Asserts}); the router leaves on the walk a body holding an assert of a shape the host
     * checks do not plan yet (a named rule of the driver's: the engine-text arms, the quantified forms, the cell
     * pool). The database judge's bodies stay on the walk (its fused group is composed at planning from M7).
     */
    ASSERT_PROGRAM,

    /**
     * A FRAME PROGRAM under the HOST judge (the test-is-the-product plan's M9, 2026-11-01): an assert program whose
     * lets may also bind FRAMES -- {@code let r = execute(...)} ({@link LetPlanner.Kind#FRAME}), the corpus's own
     * idiom, its query holding no kind planned another way -- read by the asserts and the value lets after it (their
     * reads spliced into the sides, as the walk splices them). The frame's eager run is the test door's check step at
     * the let; the asserts' sides are planned over the walk's own frames, so a side is the walk's statement. Admitted
     * since M9b: an alias of a relation frame ({@code let v = $r.values->at(0)}, the same frame), an inline
     * {@code execute(...)} read where it stands (the product's lazy splice on both roads) but the one shape the splice
     * runs at planning ({@code size(execute(...))}), and a database effect as a statement with literal arguments (the
     * effect program's rule). Not admitted, each a later sub-step: a read of a frame's activities (the run's comment:
     * the pre-planned frame registers none), an effect let, staged rows and raw grids, the test-data generator,
     * {@code assertError}, a seeded runtime.
     */
    FRAME_PROGRAM;

    /** {@code body}'s class on {@code runtime} under a run that does not judge on the host alone (the database judge,
     *  or both judges: channel B), or null when the walk runs it. */
    public static @Nullable BodyClass of(TypedQuery body, String runtime) {
        return of(body, runtime, false);
    }

    /** {@code body}'s class on {@code runtime}, {@code hostJudge} whether the run judges on the host, or null when
     *  the walk runs it. */
    public static @Nullable BodyClass of(TypedQuery body, String runtime, boolean hostJudge) {
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
        if (!hostJudge) {
            return null;
        }
        if (assertProgram(body, scan)) {
            return ASSERT_PROGRAM;
        }
        return frameProgram(body, runtime, scan) ? FRAME_PROGRAM : null;
    }

    private static boolean frameProgram(TypedQuery query, String runtime, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = systemClasses(ctx);
        Map<FunctionId, Boolean> memo = new HashMap<>();
        Map<FunctionId, Boolean> effectMemo = new HashMap<>();
        List<TypedSpec> body = query.body();
        // the frames bound so far, each whether relation-rooted (the planner's own fact, FramePlanner.chain: the
        // alias test and the splice read it); the value lets before, as the walk's prefix
        Map<String, Boolean> frames = new LinkedHashMap<>();
        java.util.function.Predicate<String> relationFrame = n -> Boolean.TRUE.equals(frames.get(n));
        List<TypedSpec> lets = new ArrayList<>();
        PlanEnv env = ProductStatements.env(ctx, runtime, Set.of(), Map.of());
        boolean anAssert = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            boolean last = i == body.size() - 1;
            if (StatementEffects.containsTdgGenerator(statement)
                    || StatementEffects.reaches(statement, BodyClass::readsActivities, query.specs(), memo)) {
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
                LetPlanner.Kind kind = LetPlanner.kind(let, body, i, query.specs(), effectMemo, relationFrame);
                if (kind == LetPlanner.Kind.FRAME && kinds.equals(EnumSet.of(QueryDoor.TestOnly.LET_EXECUTE))) {
                    // the frame's query holds no kind planned another way; its mapping and runtime arguments are the
                    // frame's own references
                    TypedNativeCall call = (TypedNativeCall) LetPlanner.rhs(let);
                    if (StatementEffects.reaches(call.args().get(0), n -> plannedAnotherWay(n, systemClasses, ctx),
                            query.specs(), memo)) {
                        return false;
                    }
                    FramePlanner.FrameChain fc = FramePlanner.chain(call, lets, query.specs(), env);
                    frames.put(let.name(), !fc.legendQuery() && fc.relationRooted());
                    continue;
                }
                if (kind == LetPlanner.Kind.ALIAS && kinds.isEmpty()) {
                    // an alias of a relation frame ({@code let v = $r.values->at(0)}, M9b): the same frame
                    frames.put(let.name(), true);
                    continue;
                }
                if (kind != LetPlanner.Kind.VALUE || !kinds.isEmpty()
                        || StatementEffects.reaches(statement, n -> plannedAnotherWayButLazyFrames(n, systemClasses,
                                ctx), query.specs(), memo)) {
                    return false;
                }
                lets.add(let);
                continue;
            }
            boolean assertRoot = kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT))
                    && !(statement instanceof TypedLet);
            if (!assertRoot && !kinds.isEmpty()
                    || StatementEffects.reaches(statement, n -> plannedAnotherWayButLazyFrames(n, systemClasses, ctx),
                            query.specs(), memo)) {
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (!last && !plansNoStep(statement, body.subList(0, i), ctx)) {
                return false;
            }
        }
        if (!anAssert || frames.isEmpty()) {
            return false;
        }
        TypedSpec last = body.get(body.size() - 1);
        return scan.kinds(last).contains(QueryDoor.TestOnly.ASSERT)
                || plansNoStep(last, body.subList(0, body.size() - 1), ctx);
    }

    /** A read of a frame's activities ({@code $r.activities...}: the run's comment and SQL), which the pre-planned
     *  frame of a moved body does not register (M9: left on the walk). */
    private static boolean readsActivities(TypedSpec n) {
        return n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa && "activities".equals(pa.property());
    }

    /** {@link #plannedAnotherWay}, but an inline {@code execute(...)} read where it stands is admitted (M9b: the
     *  product's lazy splice, D15, on both roads) -- except the one shape the splice RUNS at the side's planning,
     *  {@code size(execute(...))} ({@code ResultEnvelopeSplice}: nothing downstream consumes the chain), which a body
     *  planned before it runs cannot carry. */
    private static boolean plannedAnotherWayButLazyFrames(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        if (n instanceof TypedNativeCall call && NativeFn.Handle.runsAFrame(call.callee().id())) {
            return false;
        }
        return plannedAnotherWay(n, systemClasses, ctx) || eagerInlineExecute(n);
    }

    private static boolean eagerInlineExecute(TypedSpec n) {
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

    private static boolean assertProgram(TypedQuery query, QueryDoor.Scan scan) {
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
            if (!assertRoot && !kinds.isEmpty() || StatementEffects.containsTdgGenerator(statement)
                    || StatementEffects.reaches(statement, n -> plannedAnotherWay(n, systemClasses, ctx),
                            query.specs(), memo)) {
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
    private static boolean plansNoStep(TypedSpec statement, List<TypedSpec> lets, ModelContext ctx) {
        if (statement instanceof TypedLet let) {
            return Literals.fold(let.value(), lets) != null || BodyPlanner.handleType(let.value(), ctx) != null;
        }
        return literal(statement) || BodyPlanner.handleType(statement, ctx) != null
                || statement instanceof TypedNativeCall call && NativeFn.Effect.isInertDiagnostic(call.callee().id());
    }

    /** Whether {@code n} is a kind the program road plans another way than the walk yet: an {@code execute(...)},
     *  staged rows, a raw grid ({@code executeInDb}'s rows, typed as a raw-SQL relation: T8, T12), a store write; a
     *  read of the system metamodel store (M4) -- the extent of a class its mapping binds ({@code systemClasses}), a
     *  table of it, one of its functions, an element used as a value (a mapping, a store: its instance is the
     *  store's row); an object built, a graph answered, a temporal class read (their ids, layouts and the
     *  exists-join form converge on the product's with MV6). */
    private static boolean plannedAnotherWay(TypedSpec n, Set<String> systemClasses, ModelContext ctx) {
        return n instanceof TypedNativeCall call && (NativeFn.Handle.runsAFrame(call.callee().id())
                        || Pure.Lite.STAGE.equals(call.callee().qualifiedName()))
                || n instanceof TypedRawSqlRelation
                || StatementEffects.isStoreWrite(n)
                || n instanceof TypedGetAll all && (systemClasses.contains(all.classFqn())
                        || SystemMetamodel.isSystemElement(all.classFqn())
                        || Temporal.strategyOf(ctx, all.classFqn()) != null)
                || n instanceof TypedTableReference table && SystemMetamodel.STORE_FQN.equals(table.store())
                || n instanceof TypedUserCall call && SystemMetamodel.functionIds().contains(call.callee().id())
                || n instanceof TypedPackageableRef
                || n instanceof TypedNewInstance || n instanceof TypedCopyInstance
                || n instanceof TypedGraphFetch || n instanceof TypedSerialize || n instanceof TypedSerializeGraph;
    }

    /** The classes the system metamodel's mapping binds: their extents are the system store's rows. */
    private static Set<String> systemClasses(ModelContext ctx) {
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
    private static boolean literal(TypedSpec value) {
        return value instanceof TypedCString || value instanceof TypedCBoolean;
    }

    /** Whether {@code call}, a database effect, is one the test door writes: each String argument a literal (folded
     *  over {@code lets}, the statements before it), and not {@code loadCsvToDbTable} (its CSV a test resource). */
    private static boolean literalEffect(TypedNativeCall call, List<TypedSpec> lets) {
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
