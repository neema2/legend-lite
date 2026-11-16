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
     * test door plans each as the walk sends it (M3d: every script byte-identical; {@code TestPlannerParityTest}).
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
     * of the system metamodel store (M10a) or the eager inline execute ({@link StoreReads#sideKind}; since M9c every other
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
     * the product's statement: {@link StoreReads#anotherWay}) -- read by the asserts and the value lets after it (their
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
        // first statement that names it, once per session (TestKinds.seed, LoadPlanner.seeds: "the program's runtime";
        // M3e) -- the M5 audit's B1 refusal predated that seed (2026-11-04, the core program: JudgesOnPlansTest,
        // ServiceTestProvisionKeyTest, CreateClassTest measured on the program road)
        QueryDoor.Scan scan = new QueryDoor.Scan(body, runtime);
        if (EffectPrograms.computedEffectText(body, scan)) {
            // a database effect's text computed at run time (the user's ruling, 2026-11-04, the core program: lite has
            // no text holes, D75 -- a value is written with a store write): the test door refuses it by name when it
            // plans the effect (TestKinds.effect), before anything runs
            return EFFECT_PROGRAM;
        }
        if (EffectPrograms.effectProgram(body, scan)) {
            return EFFECT_PROGRAM;
        }
        if (queryProgram(body, scan) == null) {
            return QUERY_PROGRAM;
        }
        if (!oneJudge || StoreReads.writesAStore(body)) {
            // a body a store write reaches (a side's, a value let's, a helper's) runs on the walk inside the
            // transaction the walk opens for it and rolls back on a raise; the program road opens one for an Update
            // step alone (the M9c audit's F2)
            return null;
        }
        if (assertProgram(body, scan, systemRouted(body, runtime))) {
            return ASSERT_PROGRAM;
        }
        return FramePrograms.frameProgram(body, runtime, scan) ? FRAME_PROGRAM : null;
    }

    /**
     * Whether {@code query}'s body is SYSTEM-ROUTED (the test-is-the-product plan's M10a, the narrow M4): some
     * statement reads the system metamodel store (a system class's {@code all()}, the store's tables, a system
     * function, a tracked element's row) and no statement reads a user store (a mapped class's {@code all()}, a
     * user store's table, a {@code from()}, an {@code execute} or a handle over a runtime) -- each through the user
     * functions it calls, a lambda literal's body left aside (a lambda is a value: the native that runs one against
     * a store, an execute or a handle, is itself the read the scan names; a lambda handed to a SQL-text generator,
     * {@code toSQLString(|Product.all()...)}, runs nowhere) -- on any runtime (B4, 2026-11-11: until then only one
     * whose engine the platform runs in process; the store is the process's catalog whatever database holds the
     * user's data). The frame, effect and store-write exclusions are the class's ({@link #of}, {@code assertProgram}),
     * not this predicate's. Such a body's program runs on the process's catalog, the one session each of its sides is
     * routed to; a body mixing the two stores has two targets, each statement routed by its own stores.
     */
    public static boolean systemRouted(TypedQuery query, String runtime) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = StoreReads.systemClasses(ctx);
        Map<FunctionId, Boolean> systemMemo = new HashMap<>();
        Map<FunctionId, Boolean> userMemo = new HashMap<>();
        boolean system = false;
        List<TypedSpec> body = query.body();
        ElementConsumption consumption = new ElementConsumption(body, systemClasses, ctx, query.specs());
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (StoreReads.readsStore(statement, n -> StoreReads.userStoreRead(n, systemClasses, ctx), query.specs(), userMemo)) {
                return false;
            }
            if (consumption.elementOnlyLet(i)) {
                // a let binding system elements alone (M10b; through helpers, constructions and chains: items 7, 7b):
                // a read of the store only where the body reads its variable as a value
                system |= !consumption.consumed(i);
                continue;
            }
            system |= StoreReads.readsStore(statement, n -> StoreReads.anotherWay(n, systemClasses, ctx) == StoreReads.Kind.SYSTEM_READ, query.specs(),
                    systemMemo);
        }
        return system;
    }

    /** {@code -Dlegend.diagnostics=body-class}: the assert program's rule that refused a body, by statement (the walk
     *  deletion's measurement, 2026-11-06; prints, decides nothing). */
    static void assertRefusal(int statement, String why) {
        if (java.util.Arrays.asList(System.getProperty("legend.diagnostics", "").split(",")).contains("body-class")) {
            System.err.println("[body-class] assert rule " + System.getProperty("legend.test.current", "?")
                    + " statement " + statement + ": " + why);
        }
    }

    static boolean assertProgram(TypedQuery query, QueryDoor.Scan scan, boolean systemRouted) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = StoreReads.systemClasses(ctx);
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
                    && EffectPrograms.plannedEffect(statement, body.subList(0, i))) {
                // a database effect the test door writes as a statement of its own, as the frame and effect programs
                // admit it (a literal effect, a text form mapped over one): the corpus's setup helpers unfolded before
                // the asserts (the corpus burn-down, 2026-11-06; the walk-deletion analysis's group A). A system-routed
                // body's effects would target the user's connection beside the program's own: not admitted
                continue;
            }
            // a let whose value holds asserts only below its root -- a helper's own guards (the lift made every assert in
            // a value's position a statement of its own: AssertLift) -- is a value let, spliced into its readers as the
            // walk splices it (the walk deletion, 2026-11-06: testConvertAlias's newState(), whose converter map
            // validates itself with asserts)
            boolean guardedLet = statement instanceof TypedLet && kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT));
            if (!assertRoot && !kinds.isEmpty() && !guardedLet || StatementEffects.containsTdgGenerator(statement)
                    || !consumedBinding && StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH
                    || !consumedBinding && StatementEffects.reaches(FramePrograms.framesAndHandlesAsTheirQueries(statement),
                            n -> StoreReads.sideKind(n, systemClasses, ctx, systemRouted) != null, query.specs(), memo)) {
                assertRefusal(i, !assertRoot && !kinds.isEmpty() ? "kinds " + kinds + " ("
                        + (statement instanceof TypedNativeCall nc ? "native " + nc.callee().qualifiedName()
                                : statement instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                                        ? "user call " + uc.callee().qualifiedName()
                                        : statement.getClass().getSimpleName())
                        + (systemRouted ? ", system-routed" : "") + ")"
                        : StatementEffects.containsTdgGenerator(statement) ? "the test data generator"
                        : !consumedBinding && StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH ? "both stores"
                        : "a side of another kind");
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (i < body.size() - 1 && !plansNoStep(statement, body.subList(0, i), ctx)
                    && !(statement instanceof TypedLet let && FramePrograms.admittedLet(LetPlanner.kind(let, body, i, query.specs(),
                            effectMemo, name -> false)))
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
    static boolean answersAsTheLastStatement(TypedQuery query, QueryDoor.Scan scan, int i,
            Set<String> systemClasses, ModelContext ctx) {
        TypedSpec statement = query.body().get(i);
        if (!scan.kinds(statement).isEmpty() || StatementEffects.containsTdgGenerator(statement)
                || StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH) {
            return false;
        }
        if (StatementEffects.reaches(statement, n -> {
                    StoreReads.Kind kind = StoreReads.anotherWay(n, systemClasses, ctx);
                    return kind != null && kind != StoreReads.Kind.SYSTEM_READ && kind != StoreReads.Kind.RAW_GRID && kind != StoreReads.Kind.OBJECT;
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

    /** Why the query program's rule refuses {@code body} on {@code runtime}, or null where it admits it (a measurement
     *  for the door's refusal message, 2026-11-03; returned, no longer left on the thread for a later read: slice 5,
     *  2026-11-09). */
    public static @Nullable String queryRuleRefusal(TypedQuery body, String runtime) {
        return queryProgram(body, new QueryDoor.Scan(body, runtime));
    }

    /** The query program rule's refusal, {@code why}. */
    static String refused(String why) {
        return why;
    }

    /** The query program rule over {@code query}: null where it admits the body, else why it refuses it. */
    static @Nullable String queryProgram(TypedQuery query, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = StoreReads.systemClasses(ctx);
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
            if (StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH) {
                if (body.size() == 1) {
                    // a lone statement reading both stores (2026-11-04, the core program: MetamodelStoreTest's
                    // mixedChainKeepsTheWall): planning raises what the walk raises at its turn, nothing before it --
                    // the store resolver's wall where a user class has no context, else the system route's refusal
                    // (no single session: SystemRoutes.refuseMixed, the walk's and the program road's)
                    return null;
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
            StoreReads.Kind[] found = new StoreReads.Kind[1];
            boolean last = i == body.size() - 1;
            if (StatementEffects.reaches(statement, n -> {
                        StoreReads.Kind kind = StoreReads.anotherWay(n, systemClasses, ctx);
                        // a graph fetch and a temporal class read as the body's ANSWER (2026-11-06, the walk deletion's
                        // stress lanes: a service's query, 444 graph fetches and 17 temporal reads): the product's own
                        // statement, the one a frame's query plans (frameQueryKind) -- what the service serves
                        if (last && (kind == StoreReads.Kind.GRAPH || kind == StoreReads.Kind.TEMPORAL)) {
                            return false;
                        }
                        // the product's statement plans these too (2026-11-03, the core program, each measured on the
                        // program road by core's tests): a raw grid (executeInDb's rows, the product's keyed result,
                        // ProductStatements), an object built inside the statement (InstanceIdentityTest's keyed
                        // equality and membership in value position)
                        // a store write as the body's ONE statement (2026-11-04, the core program): one update step,
                        // atomic in the database as the walk's transaction around it is (StagedRowsTest's delete)
                        if (kind == StoreReads.Kind.STORE_WRITE || kind == StoreReads.Kind.STAGED) {
                            return false;
                        }
                        if (kind != null && kind != StoreReads.Kind.SYSTEM_READ && kind != StoreReads.Kind.RAW_GRID && kind != StoreReads.Kind.OBJECT) {
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
        return answered(query, Lets.bare(body.get(body.size() - 1))) ? null : refused("the answer: "
                + answerRefusal(query, Lets.bare(body.get(body.size() - 1))));
    }

    /** Why {@link #answered} refuses {@code answer} (the measurement's text). */
    static String answerRefusal(TypedQuery query, TypedSpec answer) {
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
    static boolean answered(TypedQuery query, TypedSpec answer) {
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
        Set<String> systemClasses = StoreReads.systemClasses(query.context());
        // a graph fetch's serialized answer reads its class's extent as the product's statement does (2026-11-06, the
        // stress lanes' service queries): the one JSON value a frame's graph fetch answers too
        boolean graph = answer instanceof TypedSerialize || answer instanceof TypedSerializeGraph;
        if (!graph && !Type.isRelation(type) && StatementEffects.reaches(answer, n -> n instanceof TypedGetAll all
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
     *  ruling of 2026-11-02): not a let, holding no kind the program road plans another way ({@link StoreReads#anotherWay}). The
     *  caller has refused a test kind already. */
    static boolean check(TypedSpec statement, Set<String> systemClasses, ModelContext ctx, TypedQuery query,
            Map<FunctionId, Boolean> memo) {
        return !(statement instanceof TypedLet)
                && !StatementEffects.reaches(FramePrograms.framesAndHandlesAsTheirQueries(statement),
                        n -> StoreReads.anotherWay(n, systemClasses, ctx) != null, query.specs(), memo);
    }

    /** Whether {@code statement}, before a body's last, plans no step on the program road and sends nothing on the
     *  walk: a let of a literal (folded over {@code lets}) or of a handle, a String or Boolean literal, an inert
     *  {@code print}, a handle (M5f's audit B3: any other value statement is a check the walk holds and labels). */
    static boolean plansNoStep(TypedSpec statement, List<TypedSpec> lets, ModelContext ctx) {
        if (statement instanceof TypedLet let) {
            return Literals.fold(let.value(), lets) != null || BodyPlanner.handleType(let.value(), ctx) != null;
        }
        return EffectPrograms.literal(statement) || BodyPlanner.handleType(statement, ctx) != null
                || statement instanceof TypedNativeCall call && NativeFn.Effect.isInertDiagnostic(call.callee().id());
    }

}
