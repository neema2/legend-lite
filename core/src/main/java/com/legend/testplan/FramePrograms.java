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
 * The FRAME PROGRAM's rule ({@link BodyClass#FRAME_PROGRAM}): whether a body's lets may bind frames the asserts read,
 * and the frame-reading shapes it admits. Split out of {@code BodyClass} (v2 item 8, 2026-11-16), the code moved
 * unchanged.
 */
final class FramePrograms {

    private FramePrograms() {
    }

    static boolean frameProgram(TypedQuery query, String runtime, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        Set<String> systemClasses = StoreReads.systemClasses(ctx);
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
        Map<String, Set<String>> framesBinding = new HashMap<>();
        // the frames whose lambda's names a LATER frame rebound (by the let name that denotes them): a statement that
        // reads one after the rebinding is the walk's -- the frames write one environment's query lets, so it would read
        // the later binding (the walk's defect, upstream reads each frame's own; the M10c item 12 audit's F1). A frame
        // read only BEFORE a later frame rebinds its names reads its own (the walk deletion, 2026-11-06: the corpus's
        // paginate and slice tests run one executeLegendQuery per lambda, each binding `$i`, each read at once)
        Set<String> stale = new HashSet<>();
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            boolean last = i == body.size() - 1;
            if (!stale.isEmpty() && !java.util.Collections.disjoint(
                    com.legend.compiler.spec.typed.FreeVars.of(statement instanceof TypedLet sl ? sl.value() : statement),
                    stale)) {
                // a read of a frame a later frame rebound (above)
                return false;
            }
            if (statement instanceof TypedLet rebinding) {
                // the name denotes this let's value from here: no longer a stale frame
                stale.remove(rebinding.name());
            }
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
                if (statement instanceof TypedLet || !EffectPrograms.literalEffect(call, body.subList(0, i))) {
                    return false;
                }
                seeded.clear();   // a write: the sources established before it are established no longer
                continue;
            }
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && !(statement instanceof TypedLet)
                    && EffectPrograms.seedFormEffectMap(value, body.subList(0, i))) {
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
                        && StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH) {
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
                    if (boundQuery instanceof com.legend.compiler.spec.typed.TypedLambda lam) {
                        // the query lambda binds a name an earlier frame's lambda bound (an executeLegendQuery
                        // parameter, a top-level let): the earlier frame is STALE from here (above)
                        for (String n : ownNames(lam)) {
                            Set<String> earlier = framesBinding.computeIfAbsent(n, k -> new HashSet<>());
                            stale.addAll(earlier);
                            earlier.add(let.name());
                        }
                        // this let's name denotes this frame now, even where an earlier frame bore it
                        stale.remove(let.name());
                    }
                    TypedSpec frameQuery = framesAndHandlesAsTheirQueries(boundQuery);
                    if (StatementEffects.reaches(frameQuery, n -> StoreReads.frameQueryKind(n, systemClasses, ctx) != null
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
                        || !consumedBinding && StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH
                        || !consumedBinding && StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                                n -> StoreReads.sideKind(n, systemClasses, ctx) != null, query.specs(), sideKindMemo)) {
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
            if (!assertRoot && !kinds.isEmpty() || StoreReads.storesOf(statement, query, i) == StoreReads.Stores.BOTH
                    || StatementEffects.reaches(framesAndHandlesAsTheirQueries(statement),
                            n -> StoreReads.sideKind(n, systemClasses, ctx) != null, query.specs(), sideKindMemo)) {
                return false;
            }
            if (assertRoot) {
                anAssert = true;
            } else if (!last && !BodyClass.plansNoStep(statement, body.subList(0, i), ctx)
                    && !BodyClass.check(statement, systemClasses, ctx, query, checkMemo)) {
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
            // A bare execute(...) of a relation-rooted query as the answer answers its frame's rows, the walk's forced
            // frame (2026-11-04, the core program: ExecuteFrameTest's executeInResultPosition; the test door's
            // ProductStatements.prepare, frameAnswered); any other execute(...) answered whole stays the walk's
            if (bareExecute(last)) {
                return !statementFrame && relationFrameAnswer(query, last, lets);
            }
            // a frame run as a statement of its own (an unread frame let's execute(...), the user's ruling of
            // 2026-11-02): run where it stands, its rows dropped, as the walk runs it (the user, 2026-11-06)
            return (statementFrame || !frames.isEmpty()) && BodyClass.answered(query, Lets.bare(last));
        }
        // a query as the last statement, the query program's answer rule (answersAsTheLastStatement), as the assert
        // program admits it since 2026-11-05 (B2d.2, 2026-11-11: upstream's loadAndTestExecution ends the test-data
        // generator's bodies with its frame's non-empty check, `$result.values->isNotEmpty() && ...`, after the
        // asserts)
        return scan.kinds(last).contains(QueryDoor.TestOnly.ASSERT) || QueryDoor.assertErrorRoot(last, query.specs())
                || BodyClass.plansNoStep(last, body.subList(0, body.size() - 1), ctx)
                || BodyClass.answersAsTheLastStatement(query, scan, body.size() - 1, systemClasses, ctx);
    }

    /** Whether {@code statement} is an {@code execute(...)} at its root (no let): a frame answered whole. */
    static boolean bareExecute(TypedSpec statement) {
        return statement instanceof TypedNativeCall call
                && NativeFn.Handle.of(call.callee().id()).orElse(null) == NativeFn.Handle.EXECUTE;
    }

    /** Whether {@code last}, a bare {@code execute(...)}, runs a relation-rooted frame (its answer the frame's rows). */
    static boolean relationFrameAnswer(TypedQuery query, TypedSpec last, List<TypedSpec> lets) {
        Boolean relationRooted = FramePlanner.relationRooted((TypedNativeCall) last, lets, query.specs());
        return Boolean.TRUE.equals(relationRooted);
    }

    /** The names {@code lam}, a frame's query, binds itself: its parameters (an executeLegendQuery's, bound from its
     *  pairs) and its top-level lets -- the lets its chain's inliner records as the frame's query lets. */
    static Set<String> ownNames(com.legend.compiler.spec.typed.TypedLambda lam) {
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

    /** Whether an assert program holds a let of {@code kind} before its last statement: a VALUE let (spliced into its
     *  readers), or a raw grid STAGED across a later effect (the test door stages it at its turn: PlanMaker's
     *  stagedGridLet, the walk deletion, 2026-11-06). */
    static boolean admittedLet(LetPlanner.Kind kind) {
        return kind == LetPlanner.Kind.VALUE || kind == LetPlanner.Kind.STAGED_GRID;
    }
}
