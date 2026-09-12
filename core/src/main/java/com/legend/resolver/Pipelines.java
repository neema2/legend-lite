package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
/**
 * Pipeline surgery: DEMANDED {@code TypedJoinSlot}s convert to prefixed
 * LEFT {@link TypedJoin}s; un-demanded slots are STRIPPED (the join
 * cancellation absence pins). Demand arrives from the resolver's scan of
 * the op-chain's consumed bindings (plus transitive predecessors: a
 * demanded slot's condition may read an earlier slot's sub-row).
 *
 * <p>Sub-row reads {@code $row.alias.COL} rewrite to the prefixed flat
 * column {@code alias_COL} through {@link #rewriteRowReads} — THE single
 * rewriter shared by slot conditions (here) and binding expressions
 * ({@link Substitution#renameRowVar} delegates to it), so the demand scan
 * and the rewrite cannot drift. A mapping ~filter reading through a slot
 * stays loud (join-mediated mapping filters: later in H3).
 */
public final class Pipelines {

    /** Rotate the given navigate steps to the BOTTOM of the step spine
     * (just above the base segment): a NAV-READ temporal date's chain
     * must materialize BELOW every head join whose window reads its
     * composed column (#32 — the engine's frame exposes the date the
     * same way). Steps are independent hops off the parent row, so the
     * rotation is row-neutral. Returns the pipe unchanged when none of
     * the aliases sit on the spine. */
    static TypedSpec sinkNavSteps(TypedSpec pipe, java.util.Set<String> aliases) {
        List<TypedSpec> steps = new ArrayList<>();   // top-first
        TypedSpec cur = pipe;
        while (true) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedNavigate nv) {
                steps.add(cur);
                cur = nv.source();
            } else if (cur instanceof
                    com.legend.compiler.spec.typed.TypedJoinSlot js) {
                steps.add(cur);
                cur = js.source();
            } else {
                break;
            }
        }
        List<TypedSpec> sunk = steps.stream()
                .filter(s -> s instanceof
                                com.legend.compiler.spec.typed.TypedNavigate nv
                        && nv.alias().map(aliases::contains).orElse(false))
                .toList();
        if (sunk.isEmpty() || sunk.size() == steps.size()) {
            return pipe;
        }
        List<TypedSpec> rest = new ArrayList<>(steps);
        rest.removeAll(sunk);
        List<TypedSpec> topFirst = new ArrayList<>(rest);
        topFirst.addAll(sunk);           // sunk steps end up DEEPEST
        TypedSpec p = cur;
        for (int i = topFirst.size() - 1; i >= 0; i--) {
            TypedSpec s = topFirst.get(i);
            p = s instanceof com.legend.compiler.spec.typed.TypedNavigate nv
                    ? new com.legend.compiler.spec.typed.TypedNavigate(p,
                            nv.alias(), nv.target(), nv.predicate(),
                            nv.form(), nv.info())
                    : new com.legend.compiler.spec.typed.TypedJoinSlot(p,
                            ((com.legend.compiler.spec.typed.TypedJoinSlot) s)
                                    .alias(),
                            ((com.legend.compiler.spec.typed.TypedJoinSlot) s)
                                    .target(),
                            ((com.legend.compiler.spec.typed.TypedJoinSlot) s)
                                    .condition(),
                            ((com.legend.compiler.spec.typed.TypedJoinSlot) s)
                                    .frameName(),
                            s.info());
        }
        return p;
    }

    /** One {@code toOne()} look-through — the multiplicity coercion is
     * transparent to structure (audit 23: this idiom had ~12 hand copies;
     * new code calls THIS). Returns the argument when {@code n} is
     * {@code toOne(x)}, else {@code n} unchanged. */
    static TypedSpec unwrapToOne(TypedSpec n) {
        return n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                ? c.args().get(0) : n;
    }

    private Pipelines() {
    }

    private static final String JOIN_KIND_FQN = "meta::pure::functions::relation::JoinKind";

    /**
     * @param pipeline    the materialized pipeline (joins in, strips done)
     * @param slotPrefixes converted alias -> column prefix ("alias_")
     * @param stripped    aliases whose joins were elided
     */
    record Materialized(TypedSpec pipeline, Map<String, String> slotPrefixes,
                        Set<String> stripped) {}

    /** Resolves a navigate step's target class to its (slot-stripped) pipeline. */
    interface TargetResolver {
        TypedSpec pipelineFor(String alias, String targetClassFqn);

        /** The navigate step's join condition for {@code alias} after the
         * resolver's own rewrites — a resolver that parks material on the
         * step (a correlated filter predicate whose outer reads are the
         * PARENT row's plain properties) composes it here; the engine's
         * nested join carries such a filter in its ON clause. Default:
         * the condition unchanged. */
        default TypedLambda conditionFor(String alias, TypedLambda cond) {
            return cond;
        }
    }

    /** All navigate-step aliases in {@code pipeline} (class-typed Join PMs). */
    static Map<String, TypedNavigate> navSteps(
            TypedSpec pipeline) {
        Map<String, TypedNavigate> out = new LinkedHashMap<>();
        collectNavSteps(pipeline, out);
        return out;
    }

    /** Navigate steps with the OUTERMOST same-named step winning: a union
     * pipeline carries the LIFTED navigate above the concatenate AND the
     * member threads' own same-named navigates inside — only the lifted
     * one is addressable from the union frame's row (its condition reads
     * the member-suffixed keys; a thread-internal navigate's raw reads are
     * meaningless outside its thread). GRAPH children correlate through
     * this view; the deep last-wins {@link #navSteps} stays the resolver's
     * per-thread addressing. */
    static Map<String, TypedNavigate> outerNavSteps(TypedSpec pipeline) {
        Map<String, TypedNavigate> out = new LinkedHashMap<>();
        collectOuterNavSteps(pipeline, out);
        return out;
    }

    private static void collectOuterNavSteps(TypedSpec n,
            Map<String, TypedNavigate> out) {
        if (n instanceof TypedNavigate nav && nav.alias().isPresent()) {
            out.putIfAbsent(nav.alias().get(), nav);
        }
        for (TypedSpec c : n.children()) {
            if (!(n instanceof TypedNavigate nav) || c == nav.source()) {
                collectOuterNavSteps(c, out);
            }
        }
    }

    private static void collectNavSteps(TypedSpec n,
            Map<String, TypedNavigate> out) {
        if (n instanceof TypedNavigate nav
                && nav.alias().isPresent()) {
            out.put(nav.alias().get(), nav);
        }
        for (TypedSpec c : n.children()) {
            if (!(n instanceof TypedNavigate nav)
                    || c == nav.source()) {   // only the chain spine
                collectNavSteps(c, out);
            }
        }
    }

    /** All slot aliases present in {@code pipeline}, in source order. */
    /** The JOIN SLOTS of a pipeline by alias (target + condition ride the slot). */
    /** SPINE-only by design (audit 23 #75, reviewed): the normalizer
     * emits join slots as a LINEAR first-child chain (source -> slot* ->
     * filter -> map); an off-spine slot would be a normalizer contract
     * change, and {@link #slotAliases} (full-tree) catching aliases this
     * walk misses surfaces exactly that as a demand/materialize mismatch
     * — loud downstream, never silently resolved. */
    static Map<String, com.legend.compiler.spec.typed.TypedJoinSlot> joinSlots(
            TypedSpec pipeline) {
        Map<String, com.legend.compiler.spec.typed.TypedJoinSlot> out =
                new java.util.LinkedHashMap<>();
        TypedSpec cur = pipeline;
        while (cur != null) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedJoinSlot js) {
                out.putIfAbsent(js.alias(), js);
            }
            cur = cur.children().isEmpty() ? null : cur.children().get(0);
        }
        return out;
    }

    static Set<String> slotAliases(TypedSpec pipeline) {
        Set<String> out = new LinkedHashSet<>();
        collectSlotAliases(pipeline, out);
        return out;
    }

    /**
     * Close {@code demanded} over slot-condition references: a demanded
     * slot whose condition reads an earlier slot's sub-row demands that
     * slot too (fixpoint; slot conditions are normalizer emissions).
     */
    static Set<String> closeOverConditions(TypedSpec pipeline, Set<String> demanded) {
        Map<String, TypedJoinSlot> byAlias = new LinkedHashMap<>();
        indexSlots(pipeline, byAlias);
        Set<String> closed = new LinkedHashSet<>(demanded);
        boolean grew = true;
        while (grew) {
            grew = false;
            for (String alias : List.copyOf(closed)) {
                TypedJoinSlot slot = byAlias.get(alias);
                if (slot == null) {
                    continue;
                }
                // Pass the condition's BODY — the lambda's own left param
                // IS the scoped var; entering via the lambda would trip the
                // shadow stop and silently disable the closure.
                String leftParam = slot.condition().parameters().get(0);
                for (String other : byAlias.keySet()) {
                    if (closed.contains(other)) {
                        continue;
                    }
                    for (TypedSpec b : slot.condition().body()) {
                        if (referencesAliasOn(b, leftParam, Set.of(other))) {
                            closed.add(other);
                            grew = true;
                            break;
                        }
                    }
                }
            }
        }
        return closed;
    }

    /** View-join pruning entry: narrow {@code cs}'s frame project to the
     * columns the demanded heads' bindings read — the macro path's demand
     * gating, restored for Leg 4 frames. Identity when nothing narrows. */
    static ClassSource narrowFrameSource(ClassSource cs,
            Set<List<String>> paths) {
        Set<String> frameReads = new LinkedHashSet<>();
        for (List<String> path : paths) {
            TypedSpec hb = cs.bindings().get(
                    SyntheticHeads.realHead(path.get(0)));
            if (hb != null) {
                collectVarReads(hb, cs.rowVar(), frameReads);
            }
        }
        TypedSpec narrowed = narrowViewFrame(cs.pipeline(), frameReads);
        return narrowed == cs.pipeline() ? cs
                : new ClassSource(cs.mappingFqn(), cs.classFqn(), cs.setId(),
                        narrowed, cs.rowVar(), cs.bindings(), cs.rowType(),
                        cs.sourceClass());
    }

    /**
     * View-join pruning on the FRAME path (Leg 4): narrow a view frame's
     * project to the columns the query READS, so the frame's internal
     * join slots behind un-read join-navigating view columns strip at
     * materialization (walkJoinSlot's JOIN CANCELLED arm) — the same
     * access-path demand gating the macro path always had. Only
     * SLOT-READING projections drop (local columns stay available for
     * later join-key reads); a read of a dropped column fails LOUDLY at
     * resolution, never a silent NULL. Row-set-DEFINING frames are left
     * whole: descent stops at anything but a filter (a ~distinct or
     * ~groupBy frame reads every declared column by definition).
     */
    static TypedSpec narrowViewFrame(TypedSpec pipeline, Set<String> readCols) {
        return switch (pipeline) {
            case TypedFilter f -> {
                // filters above the frame read frame columns too
                Set<String> reads = new LinkedHashSet<>(readCols);
                String rv = f.predicate().parameters().get(0);
                for (TypedSpec b : f.predicate().body()) {
                    collectVarReads(b, rv, reads);
                }
                TypedSpec src = narrowViewFrame(f.source(), reads);
                yield src == f.source() ? pipeline
                        : new TypedFilter(src, f.predicate(),
                                new ExprType(src.info().type(),
                                        f.info().multiplicity()),
                                f.stamp());
            }
            case TypedProject pr when containsSlot(pr.source()) -> {
                Set<String> slots = slotAliases(pr.source());
                List<TypedFuncCol> kept = new ArrayList<>(pr.columns().size());
                for (TypedFuncCol col : pr.columns()) {
                    boolean readsSlot = false;
                    String rv = col.fn().parameters().get(0);
                    for (TypedSpec b : col.fn().body()) {
                        if (referencesAliasOn(b, rv, slots)) {
                            readsSlot = true;
                            break;
                        }
                    }
                    if (!readsSlot || readCols.contains(col.name())) {
                        kept.add(col);
                    }
                }
                if (kept.size() == pr.columns().size()) {
                    yield pipeline;
                }
                Set<String> keptNames = new LinkedHashSet<>();
                for (TypedFuncCol col : kept) {
                    keptNames.add(col.name());
                }
                Type.RelationType rt = Type.requireRelationSchema(pr.info().type());
                List<Type.Column> cols = rt.columns().stream()
                        .filter(c -> keptNames.contains(c.name())).toList();
                yield new TypedProject(pr.source(), kept,
                        new ExprType(Type.relation(new Type.RelationType(cols)),
                                pr.info().multiplicity()));
            }
            default -> pipeline;
        };
    }

    static Materialized materialize(TypedSpec pipeline, Set<String> demanded,
                                    String classFqn) {
        return materialize(pipeline, demanded, Set.of(), classFqn, null);
    }

    /** §4AD P1 placement bit on the SLOT channel: joins materialized
     * for VALUE-position heads flip LEFT -> INNER (row-dropping — the
     * AssocJoin channel carries the same fact as
     * {@code AssocJoin.rowDropping}). A lone '#' synthetic CLAIMS the
     * plain slot under its REAL property name (slot-claim ordering), so
     * aliases translate through navHeadByAlias before the fact lookup.
     * Spine walk only; join targets' internals are their own frames. */
    static Materialized innerizeValueSlots(Materialized m,
            Map<String, String> navHeadByAlias, SyntheticHeads synthetics) {
        Set<String> innerPrefixes = new LinkedHashSet<>();
        for (var pe : m.slotPrefixes().entrySet()) {
            if (synthetics.isInnerValueHead(
                    navHeadByAlias.getOrDefault(pe.getKey(), pe.getKey()))) {
                innerPrefixes.add(pe.getValue());
            }
        }
        if (innerPrefixes.isEmpty()) {
            return m;
        }
        return new Materialized(
                innerizeValueJoins(m.pipeline(), innerPrefixes),
                m.slotPrefixes(), m.stripped());
    }

    private static TypedSpec innerizeValueJoins(TypedSpec pipe,
            Set<String> innerPrefixes) {
        if (pipe instanceof TypedJoin j) {
            TypedSpec l = innerizeValueJoins(j.left(), innerPrefixes);
            boolean flip = j.prefix().map(innerPrefixes::contains)
                    .orElse(false);
            if (!flip && l == j.left()) {
                return pipe;
            }
            return new TypedJoin(l, j.right(),
                    flip ? new TypedEnumValue(JOIN_KIND_FQN, "INNER",
                            new ExprType(new Type.EnumType(JOIN_KIND_FQN),
                                    Multiplicity.Bounded.ONE))
                          : j.kind(),
                    j.condition(), j.prefix(), j.frameName(), j.info(),
                    j.userCondition());
        }
        if (pipe instanceof TypedFilter f) {
            TypedSpec s = innerizeValueJoins(f.source(), innerPrefixes);
            return s == f.source() ? pipe
                    : new TypedFilter(s, f.predicate(), f.info(), f.stamp());
        }
        return pipe;
    }

    static Materialized materialize(TypedSpec pipeline, Set<String> demanded,
                                    Set<String> demandedNavs, String classFqn,
                                    @com.legend.Nullable TargetResolver targets) {
        Set<String> all = slotAliases(pipeline);
        Map<String, TypedNavigate> navs = navSteps(pipeline);
        if (all.isEmpty() && navs.isEmpty()) {
            return new Materialized(pipeline, Map.of(), Set.of());
        }
        // Row-set-defining demand (engine: the class-mapping ~filter applies
        // DURING getAll — a join the filter reads through is never cancelled,
        // it is exempt from demand gating). Slot reads inside any pipeline
        // TypedFilter predicate join the demand set; nav-step reads join it
        // only when a target resolver exists to serve them — without one the
        // stripped-read wall below stays the honest failure.
        Set<String> filterSlots = new LinkedHashSet<>();
        Set<String> filterNavs = new LinkedHashSet<>();
        collectFilterDemand(pipeline, all, navs.keySet(), filterSlots, filterNavs);
        if (!filterSlots.isEmpty()) {
            Set<String> withFilters = new LinkedHashSet<>(demanded);
            withFilters.addAll(filterSlots);
            demanded = closeOverConditions(pipeline, withFilters);
        }
        if (!filterNavs.isEmpty() && targets != null) {
            Set<String> withFilters = new LinkedHashSet<>(demandedNavs);
            withFilters.addAll(filterNavs);
            demandedNavs = withFilters;
        }
        Map<String, String> prefixes = new LinkedHashMap<>();
        Set<String> stripped = new LinkedHashSet<>();
        TypedSpec out = walk(pipeline, demanded, demandedNavs, targets,
                prefixes, stripped, classFqn);
        return new Materialized(out, prefixes, stripped);
    }


    private static TypedSpec walkJoinSlot(TypedJoinSlot js, Set<String> demanded,
            Set<String> demandedNavs, @com.legend.Nullable TargetResolver targets,
            Map<String, String> prefixes, Set<String> stripped,
            String classFqn) {

                TypedSpec target = js.target();
                if (containsSlot(target)) {
                    // a FRAMED VIEW target (Leg 4) carries its own internal
                    // slots — join-navigating view columns hoisted inside
                    // the frame's project. The frame is a self-contained
                    // row-defining subselect: walk it in its OWN scope (the
                    // TypedProject arm self-demands from its columns).
                    target = walk(target, Set.of(), Set.of(), targets,
                            new LinkedHashMap<>(), new LinkedHashSet<>(),
                            classFqn);
                    if (containsSlot(target)) {
                        throw new IllegalStateException("resolver bug: join"
                                + " slot '" + js.alias() + "' target still"
                                + " carries a slot after frame"
                                + " materialization");
                    }
                }
                final TypedSpec tgt = target;
                TypedSpec left = walk(js.source(), demanded, demandedNavs, targets, prefixes, stripped, classFqn);
                if (!demanded.contains(js.alias())) {
                    stripped.add(js.alias());
                    return left;   // JOIN CANCELLED: nothing reads through it
                }
                String prefix = slotPrefix(js.alias(),
                        Type.requireRelationSchema(left.info().type()),
                        Type.requireRelationSchema(tgt.info().type()));
                prefixes.put(js.alias(), prefix);
                // Condition: rewrite reads of PRIOR converted slots' sub-rows
                // to their prefixed columns (multi-hop chains). The BODY is
                // rewritten and the lambda rebuilt — entering via the lambda
                // itself would conflate its own param with shadowing.
                TypedLambda condLam = js.condition();
                String leftParam = condLam.parameters().get(0);
                TypedLambda cond = new TypedLambda(condLam.parameters(),
                        condLam.body().stream().map(b -> rewriteRowReads(
                                b, leftParam, prefixes, stripped,
                                UnaryOperator.identity())).toList(),
                        condLam.info());
                Type.RelationType leftRow = Type.requireRelationSchema(left.info().type());
                Type.RelationType rightRow = Type.requireRelationSchema(tgt.info().type());
                List<Type.Column> cols = new ArrayList<>(leftRow.columns());
                for (Type.Column c : rightRow.columns()) {
                    cols.add(new Type.Column(prefix + c.name(), c.type(), c.multiplicity()));
                }
                return new TypedJoin(left, tgt,
                        new TypedEnumValue(JOIN_KIND_FQN, "LEFT",
                                new ExprType(new Type.EnumType(JOIN_KIND_FQN),
                                        Multiplicity.Bounded.ONE)),
                        cond, Optional.of(prefix), js.frameName(),
                        new ExprType(Type.relation(new Type.RelationType(cols)), Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
                }

    /** ENGINE ON-FORM (memory milestoning-onclause-seam): the temporal
     * STAMP layers (STAMP_ROW_VAR-marked filters) on a join target
     * relocate onto the join condition — window in the ON, pipe raw.
     * The resolved AND callee arrives from the caller (the post-pass
     * has the ModelContext). Returns {target, cond}. */
    static Object[] onFormRelocate(TypedSpec target,
            TypedLambda cond,
            com.legend.compiler.element.TypedFunction andFn) {
        java.util.List<TypedLambda> stamps = new ArrayList<>();
        TypedSpec cur = target;
        while (cur instanceof TypedFilter twf
                && TemporalFrame.STAMP_ROW_VAR.equals(
                        twf.predicate().parameters().get(0))) {
            stamps.add(twf.predicate());
            cur = twf.source();
        }
        if (stamps.isEmpty()) {
            return new Object[] {target, cond};
        }
        String tv = cond.parameters().get(1);
        var rowInfo = new ExprType(cur.info().type(),
                Multiplicity.Bounded.ONE);
        var boolT = new ExprType(Type.Primitive.BOOLEAN,
                Multiplicity.Bounded.ONE);
        TypedSpec merged = cond.body().get(0);
        for (int i = stamps.size() - 1; i >= 0; i--) {
            TypedLambda st = stamps.get(i);
            TypedSpec pb = renameStampVar(st.body().get(0),
                    st.parameters().get(0), tv, rowInfo);
            merged = new com.legend.compiler.spec.typed.TypedNativeCall(
                    andFn, List.of(merged, pb), boolT);
        }
        return new Object[] {cur, new TypedLambda(cond.parameters(),
                List.of(merged), cond.info())};
    }

    private static TypedSpec renameStampVar(TypedSpec n, String from,
            String to, ExprType rowInfo) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(from)) {
            return new com.legend.compiler.spec.typed.TypedVariable(to, rowInfo);
        }
        return n.mapChildren(c -> renameStampVar(c, from, to, rowInfo));
    }

    /** Slot/nav aliases read by any {@link TypedFilter} predicate in the
     * pipeline (mapping ~filters and spliced below-hop filters alike). */
    private static void collectFilterDemand(TypedSpec n, Set<String> slotUniverse,
            Set<String> navUniverse, Set<String> outSlots, Set<String> outNavs) {
        if (n instanceof TypedFilter f) {
            String rv = f.predicate().parameters().get(0);
            for (TypedSpec b : f.predicate().body()) {
                collectSlotReads(b, rv, slotUniverse, outSlots);
                collectSlotReads(b, rv, navUniverse, outNavs);
            }
        }
        for (TypedSpec c : n.children()) {
            collectFilterDemand(c, slotUniverse, navUniverse, outSlots, outNavs);
        }
    }

    /** A slot's column prefix, minted clear of the LEFT row's own column
     * names (AssociationJoins.prefixFor's rule): a slot {@code schema} over
     * a row carrying {@code schema_name} must not compose its target's
     * {@code name} onto that same spelling. Readers take the prefix from
     * the materialization's map, never from the alias. */
    static String slotPrefix(String alias, Type.RelationType leftRow,
            Type.RelationType rightRow) {
        Set<String> taken = new LinkedHashSet<>();
        for (Type.Column c : leftRow.columns()) {
            taken.add(c.name());
        }
        String prefix = alias + "_";
        int ordinal = 2;
        while (composesDuplicate(prefix, rightRow, taken)) {
            prefix = alias + "_" + ordinal++ + "_";
        }
        return prefix;
    }

    private static boolean composesDuplicate(String prefix, Type.RelationType rightRow,
            Set<String> taken) {
        for (Type.Column c : rightRow.columns()) {
            if (taken.contains(prefix + c.name())) {
                return true;
            }
        }
        return false;
    }

    private static TypedSpec walk(TypedSpec n, Set<String> demanded,
                                  Set<String> demandedNavs, @com.legend.Nullable TargetResolver targets,
                                  Map<String, String> prefixes, Set<String> stripped,
                                  String classFqn) {
        return switch (n) {
            case TypedJoinSlot js -> walkJoinSlot(js, demanded, demandedNavs,
                    targets, prefixes, stripped, classFqn);
            case TypedNavigate nav
                    when nav.alias().isPresent() -> {
                TypedSpec left = walk(nav.source(), demanded, demandedNavs, targets,
                        prefixes, stripped, classFqn);
                String alias = nav.alias().get();
                if (!demandedNavs.contains(alias)) {
                    stripped.add(alias);
                    yield left;   // CLASS-SLOT JOIN CANCELLED
                }
                if (targets == null) {
                    throw new IllegalStateException(
                            "resolver bug: demanded navigate step without a target resolver");
                }
                if (!(nav.target() instanceof TypedGetAll ga)) {
                    throw new IllegalStateException("resolver bug: navigate step '"
                            + alias + "' target is "
                            + nav.target().getClass().getSimpleName()
                            + ", expected the class extent");
                }
                TypedSpec targetPipeline = targets.pipelineFor(alias, ga.classFqn());
                String prefix = slotPrefix(alias,
                        Type.requireRelationSchema(left.info().type()),
                        Type.requireRelationSchema(targetPipeline.info().type()));
                prefixes.put(alias, prefix);
                // TARGET-SIDE join-key collection (engine L5135's other
                // half): a distinct-narrowed target must expose the key
                // columns this navigation binds on.
                if (nav.predicate().parameters().size() == 2) {
                    Set<String> tgtReads = new LinkedHashSet<>();
                    for (TypedSpec b : nav.predicate().body()) {
                        collectVarReads(b, nav.predicate().parameters().get(1),
                                tgtReads);
                    }
                    targetPipeline = widenDistinctForKeys(targetPipeline, tgtReads);
                    // UNION target: member threads carry the key columns
                    // this navigation binds on (engine partial-union goldens)
                    targetPipeline = widenConcatenateForKeys(targetPipeline, tgtReads);
                }
                // The condition speaks (parent row, target TABLE row) — the
                // 4-arg emission; prior joinslot sub-row reads prefix.
                TypedLambda condLam = nav.predicate();
                String leftParam = condLam.parameters().get(0);
                TypedLambda cond = new TypedLambda(condLam.parameters(),
                        condLam.body().stream().map(b -> rewriteRowReads(
                                b, leftParam, prefixes, stripped,
                                UnaryOperator.identity())).toList(),
                        condLam.info());
                // GROUPED target (C1.7): the grouped derived table
                // projects its KEY OUTPUT names (k<i>__<base> / claimed
                // PM names), never the physical columns — target-side
                // condition reads rename through the key map, symmetric
                // to the normalizer's source-side renameGroupedNavCond.
                cond = renameGroupedTargetReads(cond, targetPipeline);
                cond = targets.conditionFor(alias, cond);
                Type.RelationType leftRow = Type.requireRelationSchema(left.info().type());
                Type.RelationType rightRow =
                        Type.requireRelationSchema(targetPipeline.info().type());
                List<Type.Column> cols = new ArrayList<>(leftRow.columns());
                for (Type.Column c : rightRow.columns()) {
                    cols.add(new Type.Column(prefix + c.name(), c.type(), c.multiplicity()));
                }
                yield new TypedJoin(left, targetPipeline,
                        new TypedEnumValue(JOIN_KIND_FQN, "LEFT",
                                new ExprType(new Type.EnumType(JOIN_KIND_FQN),
                                        Multiplicity.Bounded.ONE)),
                        cond, Optional.of(prefix), nav.frameName(),
                        new ExprType(Type.relation(new Type.RelationType(cols)), Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
            }
            case TypedFilter f -> {
                TypedSpec src = walk(f.source(), demanded, demandedNavs, targets, prefixes, stripped, classFqn);
                // BODY, not the lambda — same shadow-stop conflation as the
                // closure above; via the lambda this check silently never
                // fires (the un-loud direction, worse than over-firing).
                String rv = f.predicate().parameters().get(0);
                // Reads of CONVERTED (demanded) slots rewrite to their
                // prefixed columns — the (INNER) mapping-filter subselect
                // filters through its own materialized chain. Reads of
                // STRIPPED (undemanded) slots stay the loud wall.
                boolean readsStripped = false;
                boolean readsConverted = false;
                for (TypedSpec b : f.predicate().body()) {
                    readsStripped |= referencesAliasOn(b, rv, stripped);
                    readsConverted |= referencesAliasOn(b, rv, prefixes.keySet());
                }
                if (readsStripped) {
                    throw new NotImplementedException("mapping ~filter for '"
                            + classFqn + "' reads through a join slot;"
                            + " join-mediated mapping filters are H3-pending");
                }
                TypedLambda pred = f.predicate();
                if (readsConverted) {
                    pred = new TypedLambda(pred.parameters(),
                            pred.body().stream().map(b -> rewriteRowReads(
                                    b, rv, prefixes, stripped,
                                    UnaryOperator.identity())).toList(),
                            pred.info());
                }
                yield new TypedFilter(src, pred, src.info());
            }
            // UNION pipelines: each concatenate branch materializes
            // INDEPENDENTLY (its projection's own slot reads are its demand)
            case TypedConcatenate cc ->
                    new TypedConcatenate(
                            walk(cc.left(), demanded, demandedNavs, targets,
                                    prefixes, stripped, classFqn),
                            walk(cc.right(), demanded, demandedNavs, targets,
                                    prefixes, stripped, classFqn),
                            cc.info());
            // a PROJECT over a slotted member pipeline: the colspec lambdas
            // demand their own slot reads; materialize the source with that
            // demand and rewrite the reads to the prefixed columns
            case TypedProject pr
                    when containsSlot(pr.source()) || !navSteps(pr.source()).isEmpty() -> {
                Set<String> slotAliases = slotAliases(pr.source());
                Set<String> ownDemand = new LinkedHashSet<>();
                for (var col : pr.columns()) {
                    String rv = col.fn().parameters().get(0);
                    for (TypedSpec b : col.fn().body()) {
                        collectSlotReads(b, rv, slotAliases, ownDemand);
                    }
                }
                // FILTERs between the project and the slots demand their
                // own reads too — the (INNER) mapping-filter subselect and
                // join-navigating view ~filters filter THROUGH their chain
                // (project(filter(slotted)) shape; the predicate is the
                // only consumer of those slots)
                for (TypedSpec cur = pr.source();
                        cur instanceof TypedFilter tf; cur = tf.source()) {
                    String rv = tf.predicate().parameters().get(0);
                    for (TypedSpec b : tf.predicate().body()) {
                        collectSlotReads(b, rv, slotAliases, ownDemand);
                    }
                }
                ownDemand = closeOverConditions(pr.source(), ownDemand);
                Map<String, String> branchPrefixes = new LinkedHashMap<>();
                Set<String> branchStripped = new LinkedHashSet<>();
                TypedSpec src = walk(pr.source(), ownDemand, Set.of(), targets,
                        branchPrefixes, branchStripped, classFqn);
                List<TypedFuncCol> cols =
                        new ArrayList<>(pr.columns().size());
                for (var col : pr.columns()) {
                    String rv = col.fn().parameters().get(0);
                    List<TypedSpec> body = col.fn().body().stream()
                            .map(b -> rewriteRowReads(b, rv, branchPrefixes,
                                    branchStripped,
                                    UnaryOperator.identity()))
                            .toList();
                    cols.add(new TypedFuncCol(
                            col.name(), new TypedLambda(col.fn().parameters(),
                                    body, col.fn().info()),
                            col.documentation()));
                }
                yield new TypedProject(src, cols, pr.info());
            }
            // View ~groupBy above slots — see groupByOverSlots
            case TypedGroupBy g when containsSlot(g.source()) ->
                    groupByOverSlots(g, targets, classFqn);
            // Mapping ~distinct above slots: the engine FORCES all-property
            // materialization under a distinct (§A.6) — every slot joins and
            // the distinct tuple is the FULL materialized row. Column list
            // rebuilt from the widened row.
            case TypedDistinct d
                    when containsSlot(d.source())
                            || !navSteps(d.source()).isEmpty() -> {
                Set<String> prefixesBefore = new LinkedHashSet<>(prefixes.values());
                TypedSpec src = walk(d.source(), scalarSlotAliases(d.source()),
                        demandedNavs, targets, prefixes, stripped, classFqn);
                Type.RelationType row = Type.requireRelationSchema(src.info().type());
                if (!d.columns().isEmpty()) {
                    // MAPPED-COLUMN distinct (slot-carrying ~distinct): the
                    // tuple is the mapped main columns plus each newly
                    // materialized slot's prefixed columns (join-equality
                    // makes them dependent — dedup-neutral, engine-equal).
                    // physical mapped cols survive; slot pseudo-columns in
                    // the declared list are REPLACED by the materialized
                    // slots' prefixed columns (undemanded slots just drop)
                    List<String> cols = new ArrayList<>();
                    List<Type.Column> outCols = new ArrayList<>();
                    for (Type.Column c : row.columns()) {
                        if (d.columns().contains(c.name())) {
                            cols.add(c.name());
                            outCols.add(c);
                        }
                    }
                    for (String pfx : prefixes.values()) {
                        if (prefixesBefore.contains(pfx)) {
                            continue;
                        }
                        for (Type.Column c : row.columns()) {
                            if (c.name().startsWith(pfx) && !cols.contains(c.name())) {
                                cols.add(c.name());
                                outCols.add(c);
                            }
                        }
                    }
                    yield new TypedDistinct(src,
                            cols, new ExprType(Type.relation(new Type.RelationType(outCols)),
                                    Multiplicity.Bounded.ONE));
                }
                // WHOLE-ROW distinct (empty column list -> DISTINCT *):
                // dedup exactly what the source PROJECTS — naming the row
                // type's columns would reference ones a milestoned scan
                // does not project.
                yield new TypedDistinct(src,
                        List.of(), new ExprType(Type.relation(row), Multiplicity.Bounded.ONE));
            }
            // a resolver-synth JOIN above the slots (a nested scope's
            // association material joined onto the row-defining pipe): the
            // LEFT side carries the slots; the right is a self-contained
            // target pipeline. Condition reads of converted slots re-point
            // like a join slot's; the schema keeps the join's own right
            // columns after the walked left row.
            case TypedJoin j when containsSlot(j.left())
                    || !navSteps(j.left()).isEmpty() ->
                    walkJoinAboveSlots(j, demanded, demandedNavs, targets,
                            prefixes, stripped, classFqn);
            // ROW-SET wrappers a below-op splice leaves above the slots
            // (first()/take → limit, drop, slice): the source materializes
            // beneath them unchanged — they read no column
            case TypedLimit l -> new TypedLimit(
                    walk(l.source(), demanded, demandedNavs, targets,
                            prefixes, stripped, classFqn), l.count(), l.info());
            case com.legend.compiler.spec.typed.TypedDrop d -> walkRowSetWrapper(
                    d, demanded, demandedNavs, targets, prefixes, stripped, classFqn);
            case com.legend.compiler.spec.typed.TypedSlice sl -> walkRowSetWrapper(
                    sl, demanded, demandedNavs, targets, prefixes, stripped, classFqn);
            // a sort key reads the row like a filter predicate: converted
            // slot reads re-point to their prefixed columns, stripped
            // ones stay the loud wall
            case com.legend.compiler.spec.typed.TypedSortBy sb ->
                    walkSortBy(sb, demanded, demandedNavs, targets, prefixes,
                            stripped, classFqn);
            // the union-scan marker: the merged projection beneath it
            // materializes like any projection; the marker rides on top
            case TypedNativeCall nc when isUnionScan(nc) -> new TypedNativeCall(
                    nc.callee(), List.of(walk(nc.args().get(0), demanded, demandedNavs,
                            targets, prefixes, stripped, classFqn)), nc.info());
            default -> walkOpaque(n, classFqn);
        };
    }

    /** An unknown node above the slots is loud; a slot-free node passes. */
    private static TypedSpec walkOpaque(TypedSpec n, String classFqn) {
        if (containsSlot(n) || !navSteps(n).isEmpty()) {
            throw new NotImplementedException("mapping pipeline for '"
                    + classFqn + "' has " + n.getClass().getSimpleName()
                    + " above join slot(s); H3-pending");
        }
        return n;
    }

    /** drop/slice above the slots: the source materializes beneath. */
    private static TypedSpec walkRowSetWrapper(TypedSpec n, Set<String> demanded,
            Set<String> demandedNavs, @com.legend.Nullable TargetResolver targets,
            Map<String, String> prefixes, Set<String> stripped, String classFqn) {
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedDrop d -> new com.legend
                    .compiler.spec.typed.TypedDrop(
                    walk(d.source(), demanded, demandedNavs, targets,
                            prefixes, stripped, classFqn), d.count(), d.info());
            case com.legend.compiler.spec.typed.TypedSlice sl -> new com.legend
                    .compiler.spec.typed.TypedSlice(
                    walk(sl.source(), demanded, demandedNavs, targets,
                            prefixes, stripped, classFqn), sl.start(), sl.stop(),
                    sl.info());
            default -> throw new IllegalStateException("not a row-set wrapper: "
                    + n.getClass().getSimpleName());
        };
    }

    /** A resolver-synth JOIN above the slots (a nested scope's
     * association material joined onto the row-defining pipe): the LEFT
     * side carries the slots; the right is a self-contained target
     * pipeline. Condition reads of converted slots re-point like a join
     * slot's; the schema keeps the join's own right columns after the
     * walked left row. */
    private static TypedSpec walkJoinAboveSlots(TypedJoin j, Set<String> demanded,
            Set<String> demandedNavs, @com.legend.Nullable TargetResolver targets,
            Map<String, String> prefixes, Set<String> stripped, String classFqn) {
        int oldLeft = Type.requireRelationSchema(j.left().info().type())
                .columns().size();
        TypedSpec left = walk(j.left(), demanded, demandedNavs, targets,
                prefixes, stripped, classFqn);
        TypedLambda condLam = j.condition();
        String leftParam = condLam.parameters().get(0);
        TypedLambda cond = new TypedLambda(condLam.parameters(),
                condLam.body().stream().map(b -> rewriteRowReads(
                        b, leftParam, prefixes, stripped,
                        UnaryOperator.identity())).toList(),
                condLam.info());
        List<Type.Column> old = Type.requireRelationSchema(j.info().type())
                .columns();
        List<Type.Column> cols = new ArrayList<>(
                Type.requireRelationSchema(left.info().type()).columns());
        cols.addAll(old.subList(oldLeft, old.size()));
        return new TypedJoin(left, j.right(), j.kind(), cond, j.prefix(),
                j.frameName(),
                new ExprType(Type.relation(new Type.RelationType(cols)),
                        Multiplicity.Bounded.ONE),
                false /* resolver-synth */);
    }

    /** A sort key reads the row like a filter predicate: converted slot
     * reads re-point to their prefixed columns, stripped ones stay the
     * loud wall. */
    private static TypedSpec walkSortBy(com.legend.compiler.spec.typed.TypedSortBy sb,
            Set<String> demanded, Set<String> demandedNavs,
            @com.legend.Nullable TargetResolver targets,
            Map<String, String> prefixes, Set<String> stripped, String classFqn) {
        TypedSpec src = walk(sb.source(), demanded, demandedNavs,
                targets, prefixes, stripped, classFqn);
        String rv = sb.key().parameters().get(0);
        boolean readsStripped = false;
        boolean readsConverted = false;
        for (TypedSpec b : sb.key().body()) {
            readsStripped |= referencesAliasOn(b, rv, stripped);
            readsConverted |= referencesAliasOn(b, rv, prefixes.keySet());
        }
        if (readsStripped) {
            throw new NotImplementedException("sort key over '"
                    + classFqn + "' reads through a stripped join slot");
        }
        TypedLambda key = sb.key();
        if (readsConverted) {
            key = new TypedLambda(key.parameters(),
                    key.body().stream().map(b -> rewriteRowReads(
                            b, rv, prefixes, stripped,
                            UnaryOperator.identity())).toList(),
                    key.info());
        }
        return new com.legend.compiler.spec.typed.TypedSortBy(src, key, sb.ascending(), sb.keyAlias(),
                src.info());
    }

    /**
     * View ~groupBy above slots (AccountPnl): the groupBy's key/agg
     * lambdas are the only consumers of those slots — demand their own
     * reads, materialize the source in a branch scope, rewrite the reads
     * to the prefixed columns (mirror of the project-over-slots arm).
     */
    private static TypedSpec groupByOverSlots(TypedGroupBy g,
            @com.legend.Nullable TargetResolver targets, String classFqn) {
        Set<String> gSlots = slotAliases(g.source());
        Set<String> gDemand = new LinkedHashSet<>();
        for (var k : g.keys()) {
            k.fn().ifPresent(kf -> {
                for (TypedSpec b : kf.body()) {
                    collectSlotReads(b, kf.parameters().get(0),
                            gSlots, gDemand);
                }
            });
        }
        for (var a : g.aggs()) {
            for (TypedSpec b : a.map().body()) {
                collectSlotReads(b, a.map().parameters().get(0),
                        gSlots, gDemand);
            }
        }
        Set<String> gClosed = closeOverConditions(g.source(), gDemand);
        Map<String, String> gPrefixes = new LinkedHashMap<>();
        Set<String> gStripped = new LinkedHashSet<>();
        TypedSpec gSrc = walk(g.source(), gClosed, Set.of(), targets,
                gPrefixes, gStripped, classFqn);
        List<TypedGroupBy.GroupKey> gKeys = new ArrayList<>(g.keys().size());
        for (var k : g.keys()) {
            gKeys.add(new TypedGroupBy.GroupKey(k.column(),
                    k.fn().map(kf -> new TypedLambda(kf.parameters(),
                            kf.body().stream().map(b -> rewriteRowReads(
                                    b, kf.parameters().get(0), gPrefixes,
                                    gStripped, UnaryOperator.identity()))
                                    .toList(),
                            kf.info()))));
        }
        List<com.legend.compiler.spec.typed.TypedAggCol> gAggs =
                new ArrayList<>(g.aggs().size());
        for (var a : g.aggs()) {
            gAggs.add(new com.legend.compiler.spec.typed.TypedAggCol(
                    a.name(),
                    new TypedLambda(a.map().parameters(),
                            a.map().body().stream().map(b ->
                                    rewriteRowReads(b,
                                            a.map().parameters().get(0),
                                            gPrefixes, gStripped,
                                            UnaryOperator.identity()))
                                    .toList(),
                            a.map().info()),
                    a.reduce(), a.order()));
        }
        return new TypedGroupBy(gSrc, gKeys, gAggs, g.info());
    }

    /**
     * TARGET-SIDE grouped rename (C1.7): a navigation INTO a ~groupBy
     * class binds on the grouped pipeline's OUTPUT columns. The key map
     * recovers from the grouped keys' extraction lambdas (a bare
     * physical read named to the key's output column); a condition read
     * of a NON-KEY physical column is loud — a grouped set cannot be
     * navigated on a non-key (the same rule the normalizer's source-side
     * renameGroupedNavCond enforces). Non-grouped targets pass through.
     */
    private static TypedLambda renameGroupedTargetReads(TypedLambda cond,
            TypedSpec targetPipeline) {
        if (cond.parameters().size() != 2) {
            return cond;
        }
        TypedSpec top = targetPipeline;
        while (top instanceof TypedFilter f) {
            top = f.source();
        }
        if (!(top instanceof TypedGroupBy g)) {
            return cond;
        }
        Map<String, String> names = new LinkedHashMap<>();
        Set<String> outputs = new LinkedHashSet<>();
        for (TypedGroupBy.GroupKey k : g.keys()) {
            outputs.add(k.column());
            if (k.fn().isPresent() && k.fn().get().body().size() == 1
                    && k.fn().get().body().get(0)
                            instanceof TypedPropertyAccess pa
                    && pa.source() instanceof
                            com.legend.compiler.spec.typed.TypedVariable) {
                names.put(pa.property(), k.column());
            }
        }
        String tv = cond.parameters().get(1);
        return new TypedLambda(cond.parameters(),
                cond.body().stream().map(b ->
                        renameTargetReads(b, tv, names, outputs)).toList(),
                cond.info());
    }

    private static TypedSpec renameTargetReads(TypedSpec n, String tv,
            Map<String, String> names, Set<String> outputs) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(tv)
                && !outputs.contains(pa.property())) {
            String renamed = names.get(pa.property());
            if (renamed == null) {
                throw new com.legend.error.NotImplementedException(
                        "navigation into a ~groupBy class binds on '"
                        + pa.property() + "', which is not a ~groupBy key"
                        + " — a grouped set cannot be navigated on a"
                        + " non-key");
            }
            return new TypedPropertyAccess(pa.source(), renamed, pa.info());
        }
        // a nested lambda re-binding the target var shadows it — stop
        if (n instanceof TypedLambda l && l.parameters().contains(tv)) {
            return n;
        }
        return n.mapChildren(c -> renameTargetReads(c, tv, names, outputs));
    }

    /**
     * JOIN-KEY COLLECTION under mapping ~distinct (engine: a demanded join
     * widens the distinct tuple with its keys — pureToSQLQuery L5135): when
     * a join condition reads source columns the ~distinct NARROWING SELECT
     * dropped, re-add them to the select (and the distinct dedups over the
     * widened row). No distinct-over-select at the head: unchanged.
     */
    static TypedSpec widenDistinctForKeys(TypedSpec pipeline, Set<String> cols) {
        TypedSpec top = pipeline;
        UnaryOperator<TypedSpec> rewrap =
                UnaryOperator.identity();
        // WHILE, not if (audit 23 #75): stacked filters above the distinct
        // silently skipped key widening entirely
        while (top instanceof TypedFilter f) {
            TypedSpec inner = f.source();
            UnaryOperator<TypedSpec> prev = rewrap;
            rewrap = d -> prev.apply(new TypedFilter(d, f.predicate(),
                    new ExprType(d.info().type(), Multiplicity.Bounded.ONE)));
            top = inner;
        }
        if (!(top instanceof TypedDistinct d)) {
            return pipeline;
        }
        // A COLUMN-LIST distinct (slot-carrying ~distinct, already
        // materialized): widen the tuple with the missing key columns
        // present on its source row.
        if (!d.columns().isEmpty()
                && !(d.source() instanceof TypedSelect)) {
            Type.RelationType srow = Type.requireRelationSchema(d.source().info().type());
            List<String> dcols = new ArrayList<>(d.columns());
            List<Type.Column> outCols = new ArrayList<>();
            for (Type.Column c : srow.columns()) {
                if (dcols.contains(c.name())) {
                    outCols.add(c);
                }
            }
            boolean grew = false;
            for (String c : cols) {
                if (dcols.contains(c)) {
                    continue;
                }
                for (Type.Column sc : srow.columns()) {
                    if (sc.name().equals(c)) {
                        dcols.add(c);
                        outCols.add(sc);
                        grew = true;
                        break;
                    }
                }
            }
            if (!grew) {
                return pipeline;
            }
            return rewrap.apply(new TypedDistinct(
                    d.source(), dcols,
                    new ExprType(Type.relation(new Type.RelationType(outCols)),
                            Multiplicity.Bounded.ONE)));
        }
        if (!(d.source() instanceof TypedSelect sel)) {
            return pipeline;
        }
        Type.RelationType selRow = Type.requireRelationSchema(sel.info().type());
        Set<String> have = new LinkedHashSet<>();
        for (Type.Column c : selRow.columns()) {
            have.add(c.name());
        }
        Type.RelationType srcRow = Type.requireRelationSchema(sel.source().info().type());
        List<String> newCols = new ArrayList<>(sel.columns());
        List<Type.Column> newRowCols = new ArrayList<>(selRow.columns());
        boolean widened = false;
        for (String c : cols) {
            if (have.contains(c)) {
                continue;
            }
            for (Type.Column sc : srcRow.columns()) {
                if (sc.name().equals(c)) {
                    newCols.add(c);
                    newRowCols.add(sc);
                    widened = true;
                    break;
                }
            }
        }
        if (!widened) {
            return pipeline;
        }
        ExprType row = new ExprType(Type.relation(new Type.RelationType(newRowCols)),
                Multiplicity.Bounded.ONE);
        TypedSpec ns = new TypedSelect(
                sel.source(), newCols, row);
        return rewrap.apply(new TypedDistinct(
                ns, d.columns().isEmpty() ? d.columns() : newCols, row));
    }

    /**
     * JOIN-KEY COLLECTION over a UNION pipeline (engine: each member thread
     * of a union subselect carries the demanded join-key columns — the
     * {@code FirmID_0}-family columns in the partial-union goldens; this is
     * the shared-name form): a navigation join over a concatenate reads
     * source key columns the member projections dropped — re-add each key
     * to EVERY member projection, reading the member's own physical column.
     * No concatenate in the pipeline: unchanged. A member whose row lacks
     * the column is LOUD (the per-member suffixed/NULL-filled form is the
     * union-to-union rung).
     */
    /** {@link #widenConcatenateForKeys} applied to the concatenate BENEATH
     * a pipeline's navigate / join-slot / filter steps (a union source
     * carrying hoisted steps): the steps rebuild over the widened union. */
    static TypedSpec widenConcatenateBelow(TypedSpec pipeline, Set<String> cols) {
        if (cols.isEmpty()) {
            return pipeline;
        }
        return switch (pipeline) {
            case TypedConcatenate cat -> widenConcatenateForKeys(cat, cols);
            case TypedNativeCall nc when isUnionScan(nc) -> widenConcatenateForKeys(nc, cols);
            case TypedFilter f -> {
                TypedSpec inner = widenConcatenateBelow(f.source(), cols);
                yield inner == f.source() ? pipeline
                        : new TypedFilter(inner, f.predicate(),
                                new ExprType(inner.info().type(), Multiplicity.Bounded.ONE));
            }
            case TypedNavigate nav -> {
                TypedSpec inner = widenConcatenateBelow(nav.source(), cols);
                yield inner == nav.source() ? pipeline
                        : new TypedNavigate(inner, nav.alias(), nav.target(),
                                nav.predicate(), nav.pairedPredicate(), nav.frameName(),
                                nav.form(), nav.info());
            }
            case TypedJoinSlot js -> {
                TypedSpec inner = widenConcatenateBelow(js.source(), cols);
                yield inner == js.source() ? pipeline
                        : new TypedJoinSlot(inner, js.alias(), js.target(),
                                js.condition(), js.frameName(), js.info());
            }
            default -> pipeline;
        };
    }

    static TypedSpec widenConcatenateForKeys(TypedSpec pipeline, Set<String> cols) {
        if (pipeline instanceof TypedFilter f) {
            TypedSpec inner = widenConcatenateForKeys(f.source(), cols);
            if (inner == f.source()) {
                return pipeline;
            }
            return new TypedFilter(inner, f.predicate(),
                    new ExprType(inner.info().type(), Multiplicity.Bounded.ONE));
        }
        // a ONE-thread union: every member of a filtered single-table
        // hierarchy merged into one scan (UnionSynthesis single-scan
        // groups, the unionScan marker) — the projection hides the
        // physical keys exactly as a concatenate's threads do, so it
        // widens as the one member and keeps its marker
        if (isUnionScan(pipeline)) {
            TypedNativeCall mark = (TypedNativeCall) pipeline;
            TypedSpec inner = widenConcatenateForKeys(mark.args().get(0), cols);
            return inner == mark.args().get(0) ? pipeline
                    : new TypedNativeCall(mark.callee(), List.of(inner), inner.info());
        }
        if (pipeline instanceof TypedProject lone) {
            Type.RelationType lrow = Type.requireRelationSchema(lone.info().type());
            List<String> lmissing = new ArrayList<>();
            for (String c : cols) {
                if (lrow.columns().stream().noneMatch(x -> x.name().equals(c))) {
                    lmissing.add(c);
                }
            }
            return lmissing.isEmpty() ? pipeline
                    : widenUnionMember(lone, 0, List.of(lone), lmissing);
        }
        if (!(pipeline instanceof TypedConcatenate cat)) {
            return pipeline;
        }
        Type.RelationType row = Type.requireRelationSchema(cat.info().type());
        Set<String> have = new LinkedHashSet<>();
        for (Type.Column c : row.columns()) {
            have.add(c.name());
        }
        List<String> missing = new ArrayList<>();
        for (String c : cols) {
            if (!have.contains(c)) {
                missing.add(c);
            }
        }
        if (missing.isEmpty()) {
            return pipeline;
        }
        // Flatten the left-deep concatenate: member ordinal i = the i-th
        // thread = the engine's `<col>_<i>` key-column suffix.
        List<TypedSpec> members = new ArrayList<>();
        flattenConcatenate(cat, members);
        List<TypedSpec> widened = new ArrayList<>(members.size());
        for (int i = 0; i < members.size(); i++) {
            widened.add(widenUnionMember(members.get(i), i, members, missing));
        }
        TypedSpec out = widened.get(0);
        for (int i = 1; i < widened.size(); i++) {
            out = new TypedConcatenate(out,
                    widened.get(i),
                    new ExprType(out.info().type(), Multiplicity.Bounded.ONE));
        }
        return out;
    }

    private static void flattenConcatenate(TypedSpec n, List<TypedSpec> out) {
        if (n instanceof TypedConcatenate cat) {
            flattenConcatenate(cat.left(), out);
            flattenConcatenate(cat.right(), out);
        } else {
            out.add(n);
        }
    }

    /**
     * Append {@code missing} key columns to member {@code ordinal}'s
     * projection. Two spellings:
     * <ul>
     *   <li>a PLAIN column name — every member reads its own physical
     *       column (the shared-key form; a member lacking it is loud);</li>
     *   <li>{@code <col>_<i>} — a PARTIAL-route key (engine suffix): member
     *       {@code i} reads its physical {@code <col>}, every other member
     *       contributes a typed NULL (un-routed threads must not match).</li>
     * </ul>
     */
    private static TypedSpec widenUnionMember(TypedSpec side, int ordinal,
            List<TypedSpec> members, List<String> missing) {
        if (isUnionScan(side)) {
            // a merged single-scan member of a mixed union keeps its marker
            TypedNativeCall mark = (TypedNativeCall) side;
            TypedSpec inner = widenUnionMember(mark.args().get(0), ordinal, members, missing);
            return new TypedNativeCall(mark.callee(), List.of(inner), inner.info());
        }
        if (!(side instanceof TypedProject p)) {
            throw new NotImplementedException(
                    "a navigation join over this union demands key columns "
                    + missing + ", but a union member is a "
                    + side.getClass().getSimpleName()
                    + " — only projected members widen");
        }
        Type.RelationType srcRow = Type.requireRelationSchema(p.source().info().type());
        List<TypedFuncCol> newCols =
                new ArrayList<>(p.columns());
        List<Type.Column> outCols = new ArrayList<>(
                (Type.requireRelationSchema(p.info().type())).columns());
        for (String c : missing) {
            Type.Column src = columnOf(srcRow, c);
            if (src == null
                    && Pattern.matches("^.*_\\d+$", c)) {
                // ROUTED (member-suffixed) keys carry full provenance: the
                // normalizer projects them INTO the union body (lift source
                // keys + inbound route scan). A suffixed demand reaching
                // this widening means that projection was missed — never
                // re-derive meaning from the name pattern (audit 11: a real
                // column spelled like a suffix hijacked the NULL thread).
                // honest both ways (audit 23 #75): the demand may also be
                // a REAL physical column that happens to end in _<digits>
                throw new IllegalStateException("column '" + c + "' is"
                        + " demanded but absent from the union body: either"
                        + " a routed union key the normalizer's inbound-"
                        + "route scan failed to project (resolver bug), or"
                        + " a physical column named like a member suffix"
                        + " that the mapping never exposes");
            }
            String v = "u_k";
            TypedSpec body;
            Type colDeclType;
            if (src != null) {
                ExprType colType = new ExprType(src.type(), src.multiplicity());
                body = new TypedPropertyAccess(
                        new TypedVariable(v,
                                new ExprType(srcRow, Multiplicity.Bounded.ONE)),
                        src.name(), colType);
                colDeclType = src.type();
            } else {
                // HETEROGENEOUS member key (engine SQLNull padding,
                // pureToSQLQuery_union.pure:682-691): a member that does
                // not carry the demanded key contributes a TYPED NULL —
                // its rows can never match the navigation join, exactly
                // the engine's un-routed-thread semantics. The type comes
                // from a SIBLING that does carry the column.
                // — read off the sibling's SOURCE row (its projection is
                // the aligned union schema, which is exactly what lacks
                // the column; group F burn 2026-09-02)
                Type sibling = null;
                for (TypedSpec m : members) {
                    TypedSpec msrc = m instanceof TypedProject mp ? mp.source() : m;
                    if (Type.relationSchema(msrc.info().type()) instanceof Type.RelationType mr) {
                        Type.Column mc = columnOf(mr, c);
                        if (mc != null) {
                            sibling = mc.type();
                            break;
                        }
                    }
                }
                if (sibling == null) {
                    throw new NotImplementedException(
                            "a navigation join over this union demands key column '"
                            + c + "', which NO union member carries");
                }
                body = new TypedCollection(List.of(),
                        new ExprType(sibling, Multiplicity.Bounded.ZERO_ONE));
                colDeclType = sibling;
            }
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(srcRow, Multiplicity.Bounded.ONE)),
                    new Type.Param(colDeclType,
                            Multiplicity.Bounded.ZERO_ONE));
            newCols.add(new TypedFuncCol(c,
                    new TypedLambda(List.of(v),
                            List.of(body),
                            new ExprType(fnType, Multiplicity.Bounded.ONE))));
            outCols.add(new Type.Column(c, colDeclType,
                    Multiplicity.Bounded.ZERO_ONE));
        }
        return new TypedProject(p.source(), newCols,
                new ExprType(Type.relation(new Type.RelationType(outCols)),
                        Multiplicity.Bounded.ONE));
    }

    private static Type.@com.legend.Nullable Column columnOf(Type.RelationType row, String name) {
        for (Type.Column c : row.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    /** Join-slot aliases whose targets are CLASS-EXTENT-free — the slots
     * the engine's all-properties-under-distinct materialization may
     * demand (a class-typed navigation is not a property column). */
    private static Set<String> scalarSlotAliases(TypedSpec pipeline) {
        Set<String> out = new LinkedHashSet<>();
        TypedSpec cur = pipeline;
        while (cur != null) {
            if (cur instanceof TypedJoinSlot js) {
                if (!containsClassExtent(js.target())) {
                    out.add(js.alias());
                }
                cur = js.source();
            } else if (cur instanceof TypedNavigate nv) {
                cur = nv.source();
            } else if (cur.children().isEmpty()) {
                cur = null;
            } else {
                cur = cur.children().get(0);
            }
        }
        return out;
    }

    private static boolean containsClassExtent(TypedSpec n) {
        if (n instanceof TypedGetAll
                || n instanceof TypedNavigate) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsClassExtent(c)) {
                return true;
            }
        }
        return false;
    }

    /** Column names read on {@code var} anywhere in {@code n}. */
    static void collectVarReads(TypedSpec n, String var, Set<String> out) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(var)) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            collectVarReads(c, var, out);
        }
    }

    /** Slot aliases read through {@code rowVar} in {@code n} ($row.slot...). */
    private static void collectSlotReads(TypedSpec n, String rowVar,
            Set<String> slotAliases, Set<String> out) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(rowVar)
                && slotAliases.contains(pa.property())) {
            out.add(pa.property());
        }
        if (n instanceof TypedLambda l && l.parameters().contains(rowVar)) {
            return;     // shadowing lambda: its $rowVar is NOT our row
        }
        for (TypedSpec c : n.children()) {
            collectSlotReads(c, rowVar, slotAliases, out);
        }
    }

    /**
     * THE single row-read rewriter — shared by slot conditions (via
     * {@link #materialize}) and binding expressions
     * ({@link Substitution#renameRowVar}); the two sites CANNOT drift.
     * Closed vocabulary (the normalizer's emission set) with a LOUD
     * default; recognized shapes:
     * <ul>
     *   <li>{@code $var.alias.COL} of a CONVERTED slot &rArr; the prefixed
     *       flat column on {@code varRewrite($var)};</li>
     *   <li>any OTHER read of a converted or stripped slot &rArr;
     *       {@code IllegalStateException} — the demand scan and the rewrite
     *       disagreed, never silent;</li>
     *   <li>{@code $var} itself &rArr; {@code varRewrite} (identity for
     *       slot conditions; the fresh row var for bindings).</li>
     * </ul>
     */
    static TypedSpec rewriteRowReads(TypedSpec n, String rowVar,
                                     Map<String, String> prefixes, Set<String> stripped,
                                     UnaryOperator<TypedSpec> varRewrite) {
        if (n instanceof TypedPropertyAccess outer
                && outer.source() instanceof TypedPropertyAccess inner
                && inner.source() instanceof TypedVariable v
                && v.name().equals(rowVar)
                && prefixes.containsKey(inner.property())) {
            return new TypedPropertyAccess(varRewrite.apply(v),
                    prefixes.get(inner.property()) + outer.property(), outer.info());
        }
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(rowVar)) {
            if (prefixes.containsKey(pa.property())) {
                throw new IllegalStateException("resolver bug: converted-slot read"
                        + " in unrecognized shape — $" + rowVar + "." + pa.property()
                        + " consumed other than as a sub-row column read");
            }
            if (stripped.contains(pa.property())) {
                throw new IllegalStateException("resolver bug: undemanded navigation —"
                        + " consumed expression reads STRIPPED join slot '"
                        + pa.property() + "' (the demand scan and the rewrite disagreed)");
            }
        }
        return switch (n) {
            case TypedVariable v when v.name().equals(rowVar) -> varRewrite.apply(v);
            case TypedVariable v -> v;
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    rewriteRowReads(pa.source(), rowVar, prefixes, stripped, varRewrite),
                    pa.property(), pa.info());
            case TypedNativeCall c -> c.withChildren(c.args().stream().map(a ->
                            rewriteRowReads(a, rowVar, prefixes, stripped, varRewrite))
                            .toList());
            case TypedCollection c -> c.withChildren(c.elements().stream().map(e ->
                                    rewriteRowReads(e, rowVar, prefixes, stripped, varRewrite))
                                    .toList());
            case TypedIf i ->
                    new TypedIf(
                            rewriteRowReads(i.condition(), rowVar, prefixes, stripped, varRewrite),
                            rewriteRowReads(i.thenBranch(), rowVar, prefixes, stripped, varRewrite),
                            i.elseBranch().map(e ->
                                    rewriteRowReads(e, rowVar, prefixes, stripped, varRewrite)),
                            i.info());
            case TypedLambda l -> l.parameters().contains(rowVar)
                    ? l   // shadowing stops the rewrite (plain capture rule)
                    : new TypedLambda(l.parameters(),
                            l.body().stream().map(b ->
                                    rewriteRowReads(b, rowVar, prefixes, stripped, varRewrite))
                                    .toList(), l.info());
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            // JSON/variant-source bindings are casts over variant reads:
            // to(get($row.data, 'k'), @T) — the cast rides, the reads
            // rewrite (plan §F12: substitution doesn't care).
            case TypedCast c ->
                    new TypedCast(
                            rewriteRowReads(c.source(), rowVar, prefixes, stripped, varRewrite),
                            c.target(), c.info(), c.wire());
            case TypedTypeRef ignored -> n;
            // CORRELATED SCALAR SUBQUERY in condition position (the
            // parentNavCondReads emission: LIMIT-1 over project over
            // corr-filter): the correlation lambda reads THIS row var and
            // re-points with it; relation SOURCES below the filter are
            // self-contained resolved material and pass through untouched.
            case TypedLimit tl -> new TypedLimit(
                    rewriteRowReads(tl.source(), rowVar, prefixes, stripped,
                            varRewrite),
                    tl.count(), tl.info());
            // D6a: the graph-leaf scalar subquery dedups (TypedDistinct)
            // where it used to LIMIT 1 — same source-carrying rewrite
            case com.legend.compiler.spec.typed.TypedDistinct td ->
                    new com.legend.compiler.spec.typed.TypedDistinct(
                            rewriteRowReads(td.source(), rowVar, prefixes,
                                    stripped, varRewrite),
                            td.columns(), td.info());
            case TypedProject tp -> new TypedProject(
                    rewriteRowReads(tp.source(), rowVar, prefixes, stripped,
                            varRewrite),
                    tp.columns().stream().map(fc -> new TypedFuncCol(
                            fc.name(),
                            (TypedLambda) rewriteRowReads(fc.fn(), rowVar,
                                    prefixes, stripped, varRewrite),
                            fc.documentation()))
                            .toList(),
                    tp.info());
            case TypedFilter tf -> new TypedFilter(tf.source(),
                    (TypedLambda) rewriteRowReads(tf.predicate(), rowVar,
                            prefixes, stripped, varRewrite),
                    tf.info());
            // EMBEDDED ctor (same-row instance): every property value is
            // an ordinary row read — rewrite each (#71 ctor transplants)
            case com.legend.compiler.spec.typed.TypedNewInstance ni -> {
                java.util.LinkedHashMap<String, TypedSpec> props =
                        new java.util.LinkedHashMap<>();
                for (var pe : ni.properties().entrySet()) {
                    props.put(pe.getKey(), rewriteRowReads(pe.getValue(),
                            rowVar, prefixes, stripped, varRewrite));
                }
                yield new com.legend.compiler.spec.typed.TypedNewInstance(
                        ni.classFqn(), props, ni.info());
            }
            default -> throw new IllegalStateException(
                    "resolver bug: row-read rewrite hit "
                            + n.getClass().getSimpleName()
                            + ", outside the normalizer's emission vocabulary");
        };
    }

    /**
     * Rewrite a TARGET-class binding for use on the JOINED row: every
     * {@code $targetRow.COL} read becomes {@code varRewrite($targetRow)}
     * {@code .prefixCOL} (the prefixed flat column the association join
     * exposes). Closed vocabulary, loud default — the same discipline as
     * {@link #rewriteRowReads}.
     */
    static TypedSpec prefixColumns(TypedSpec n, String rowVar, String colPrefix,
                                   UnaryOperator<TypedSpec> varRewrite) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(rowVar)) {
            return new TypedPropertyAccess(varRewrite.apply(v),
                    colPrefix + pa.property(), pa.info());
        }
        return switch (n) {
            case TypedVariable v when v.name().equals(rowVar) ->
                    throw new IllegalStateException("resolver bug: bare target row var"
                            + " in an association-leaf binding");
            case TypedVariable v -> v;
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    prefixColumns(pa.source(), rowVar, colPrefix, varRewrite),
                    pa.property(), pa.info());
            case TypedNativeCall c -> c.withChildren(c.args().stream().map(a -> prefixColumns(a, rowVar, colPrefix, varRewrite))
                            .toList());
            case TypedCollection c -> c.withChildren(c.elements().stream().map(e ->
                                    prefixColumns(e, rowVar, colPrefix, varRewrite)).toList());
            case TypedIf i ->
                    new TypedIf(
                            prefixColumns(i.condition(), rowVar, colPrefix, varRewrite),
                            prefixColumns(i.thenBranch(), rowVar, colPrefix, varRewrite),
                            i.elseBranch().map(e -> prefixColumns(e, rowVar, colPrefix, varRewrite)),
                            i.info());
            case TypedLambda l -> l.parameters().contains(rowVar)
                    ? l
                    : new TypedLambda(l.parameters(),
                            l.body().stream().map(b ->
                                    prefixColumns(b, rowVar, colPrefix, varRewrite)).toList(),
                            l.info());
            // to(get($row.DATA, 'k'), @T) — JSON/variant-source bindings wrap
            // reads in a CAST; the substitution rides through it (the same
            // arm rewriteRowReads has — plan §F12: substitution doesn't care).
            case TypedCast c ->
                    new TypedCast(
                            prefixColumns(c.source(), rowVar, colPrefix, varRewrite),
                            c.target(), c.info(), c.wire());
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            default -> throw new IllegalStateException(
                    "resolver bug: association-leaf rewrite hit "
                            + n.getClass().getSimpleName()
                            + ", outside the normalizer's emission vocabulary");
        };
    }

    /** The pipeline's join-slot steps by alias (the chained-agg key chase
     * reads a mid slot's own join condition). */
    static Map<String, TypedJoinSlot> slotSteps(TypedSpec pipeline) {
        Map<String, TypedJoinSlot> out = new LinkedHashMap<>();
        indexSlots(pipeline, out);
        return out;
    }

    private static void indexSlots(TypedSpec n, Map<String, TypedJoinSlot> out) {
        if (n instanceof TypedJoinSlot js) {
            out.put(js.alias(), js);
        }
        for (TypedSpec c : n.children()) {
            indexSlots(c, out);
        }
    }

    private static void collectSlotAliases(TypedSpec n, Set<String> out) {
        if (n instanceof TypedJoinSlot js) {
            out.add(js.alias());
        }
        for (TypedSpec c : n.children()) {
            collectSlotAliases(c, out);
        }
    }

    /** Collection distinct/removeDuplicates over instances (no comparator). */
    static boolean isClassDistinct(com.legend.compiler.spec.typed.TypedNativeCall c) {
        return c.args().size() == 1
                && (c.callee().qualifiedName().equals(
                                "meta::pure::functions::collection::distinct")
                        || c.callee().qualifiedName().equals(
                                "meta::pure::functions::collection::removeDuplicates"));
    }

    static boolean containsSlot(TypedSpec n) {
        if (n instanceof TypedJoinSlot) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsSlot(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Any {@code $varName.alias} read where alias is in {@code aliases} —
     * scoped to ONE variable (a right-side param or base column whose name
     * collides with a slot alias must not over-demand or false-loud;
     * audit finding). Shadowing lambdas stop the walk.
     */
    static boolean referencesAliasOn(TypedSpec n, String varName, Set<String> aliases) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(varName)
                && aliases.contains(pa.property())) {
            return true;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(varName)) {
            return false;
        }
        for (TypedSpec c : n.children()) {
            if (referencesAliasOn(c, varName, aliases)) {
                return true;
            }
        }
        return false;
    }

    /**
     * BARE property read over a CLASS chain in value position — the
     * auto-map sugar: {@code chain.p1.p2 ≡ chain->map(v|$v.p1.p2)}.
     * Returns the map spelling for the resolver to RE-ENTER (one value
     * funnel), or null when the shape is not the plain sugar (milestoned
     * hops and class/relation-typed results keep their walls).
     */
    /** isEmpty/isNotEmpty over a CLASS chain rewrites to the
     * constant-project relation form (lowerer EXISTS; map §2 rule) —
     * identity otherwise. */
    static TypedNativeCall classEmptinessRewrite(TypedNativeCall nc,
            java.util.function.Predicate<TypedSpec> objectSpace) {
        boolean empt = com.legend.builtin.Pure.nativeNamed("isEmpty",
                nc.callee().signatureKey())
                || com.legend.builtin.Pure.nativeNamed("isNotEmpty",
                        nc.callee().signatureKey());
        if (!empt || nc.args().size() != 1
                || !objectSpace.test(nc.args().get(0))
                || !(Type.asClassType(nc.args().get(0).info().type())
                        instanceof Type.ClassType ct)) {
            return nc;
        }
        return (TypedNativeCall) nc.withChildren(
                List.of(constantProjectOver(nc.args().get(0), ct)));
    }

    /** {@code chain->project([_e|1],['c'])} — the RELATION form of a class
     * chain for scalar consumers (emptiness-as-EXISTS; map §2 rule). */
    static TypedSpec constantProjectOver(TypedSpec chain,
            Type.ClassType elementType) {
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        ExprType intT = new ExprType(Type.Primitive.INTEGER, one);
        TypedLambda fn = new TypedLambda(java.util.List.of("_e"),
                java.util.List.of(new com.legend.compiler.spec.typed
                        .TypedCInteger(1, intT)),
                new ExprType(new Type.FunctionType(
                        java.util.List.of(new Type.Param(elementType, one)),
                        new Type.Param(Type.Primitive.INTEGER, one)), one));
        return new TypedProject(chain,
                java.util.List.of(new com.legend.compiler.spec.typed
                        .TypedFuncCol("c", fn)),
                new ExprType(Type.relation(new Type.RelationType(java.util.List.of(
                        new Type.Column("c", Type.Primitive.INTEGER, one)))),
                        one));
    }

    /** The dot-rule DESUGAR — pure's own definition applied once at
     * the resolution boundary (map.pure grammarDoc: "map is auto
     * generated when the . operator is used"): a class-rooted sugar
     * chain becomes the canonical {@code map(base, v|$v.a.b)} the
     * resolution machinery consumes. NOT a duplicated matcher — the
     * READ side never forks because {@link Substitution#pathOf} (THE
     * path view) reads both spellings; this converter exists so the
     * RESOLUTION side has one canonical form, exactly as the language
     * defines it. (Path-view unification, closed by measurement.) */
    /** Property read over a {@code ^Class(...)} INSTANCE LITERAL folds
     * to its property value — compiler CONSTANT FOLDING on structural
     * shapes (the eq-nodes idiom; TDG lane S1: checker-fold results are
     * instance literals and their navigation must LOWER, never
     * store-resolve). Falls to the auto-map sugar read otherwise. */
    static @com.legend.Nullable TypedSpec literalOrAutoMapRead(
            TypedPropertyAccess pa) {
        TypedSpec lit = instanceLiteralProp(pa);
        return lit != null ? lit : autoMapRead(pa);
    }

    /** THE ONE literal-prop rule (both the resolver and the TDG fold
     * walk read it — never a second copy). */
    public static @com.legend.Nullable TypedSpec instanceLiteralProp(
            TypedPropertyAccess pa) {
        if (pa.source() instanceof com.legend.compiler.spec.typed
                .TypedNewInstance ni) {
            return ni.properties().get(pa.property());
        }
        return null;
    }

    static @com.legend.Nullable TypedSpec autoMapRead(TypedPropertyAccess pa) {
        // class-typed = bare or parameterized class (Type.classFqn: the
        // spec's PropertyMapping.property is Property<Nil,Any|*>)
        if (Type.classFqn(pa.info().type()) != null
                || Type.schemaView(pa.info().type()) != null) {
            return null;
        }
        List<TypedPropertyAccess> hops = new ArrayList<>();
        TypedSpec base = pa;
        while (base instanceof TypedPropertyAccess p
                && Type.classFqn(p.source().info().type()) != null) {
            hops.add(0, p);
            base = p.source();
        }
        if (hops.isEmpty() || base instanceof TypedPropertyAccess
                || Type.classFqn(base.info().type()) == null) {
            return null;
        }
        Type ct = base.info().type();
        String v = "v_amr";
        TypedSpec body = new TypedVariable(v,
                new ExprType(ct, Multiplicity.Bounded.ONE));
        for (TypedPropertyAccess h : hops) {
            body = new TypedPropertyAccess(body, h.property(), h.info());
        }
        ExprType lamInfo = new ExprType(new Type.FunctionType(
                List.of(new Type.Param(ct, Multiplicity.Bounded.ONE)),
                new Type.Param(pa.info().type(), pa.info().multiplicity())),
                Multiplicity.Bounded.ONE);
        return new com.legend.compiler.spec.typed.TypedMap(base,
                new TypedLambda(List.of(v), List.of(body), lamInfo),
                pa.info());
    }

    /** Whether the pipeline carries a union (TypedConcatenate) anywhere. */
    /** The union-scan marker (Pure.Lite.UNION_SCAN) around a merged
     * single-table-hierarchy projection: "this relation is a union body". */
    static boolean isUnionScan(TypedSpec n) {
        return n instanceof TypedNativeCall nc
                && com.legend.builtin.Pure.Lite.UNION_SCAN.equals(nc.callee().qualifiedName())
                && nc.args().size() == 1;
    }

    /** Whether the pipeline carries a UNION body: a concatenate of member
     * threads, or the single-scan marker of a merged hierarchy. */
    static boolean containsConcatenate(TypedSpec pipeline) {
        if (pipeline instanceof com.legend.compiler.spec.typed.TypedConcatenate
                || isUnionScan(pipeline)) {
            return true;
        }
        for (TypedSpec c : pipeline.children()) {
            if (containsConcatenate(c)) {
                return true;
            }
        }
        return false;
    }


    /** 2a' JOIN-KEY WIDENING body (extracted from resolveObject): every
     * demanded join/exists condition's source-side key columns must
     * survive the mapping ~distinct narrowing select and the union
     * projection (engine L5135 / partial-union goldens). */
    static TypedSpec widenPipeForJoinKeys(TypedSpec materializedPipe,
            List<AssociationJoins.AssocJoin> assocJoins,
            Map<String, AssociationJoins.AssocJoin> aggMaterials,
            Map<String, Substitution.ExistsSub> existsSubs) {
        Set<String> joinKeyReads = new LinkedHashSet<>();
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            var ajCondR = aj.condition();
            if (ajCondR != null) { CorrelatedSubselects.collectParamColumnReads(ajCondR, joinKeyReads); }
        }
        for (AssociationJoins.AssocJoin aj : aggMaterials.values()) {
            var ajCondR = aj.condition();
            if (ajCondR != null) { CorrelatedSubselects.collectParamColumnReads(ajCondR, joinKeyReads); }
        }
        for (Substitution.ExistsSub ex : existsSubs.values()) {
            CorrelatedSubselects.collectParamColumnReads(ex.orientedCond(), joinKeyReads);
        }
        if (joinKeyReads.isEmpty()) {
            return materializedPipe;
        }
        // UNION root: member threads carry the demanded join keys
        // through the union projection (engine partial-union goldens)
        return Pipelines.widenConcatenateForKeys(
                Pipelines.widenDistinctForKeys(materializedPipe, joinKeyReads),
                joinKeyReads);
    }


    /** {@code $chain.prop->map(v | f($v))} over an object chain: the mapper
     * composed over the read — {@code map(chain, x | f($x.prop))} — so the
     * object-space map arm serves it (per element, the auto-map reading). */
    static TypedMap composeScalarReadMap(com.legend.compiler.spec.SpecCompiler specs,
            TypedMap m, com.legend.compiler.spec.typed.TypedPropertyAccess pa,
            Type.ClassType ec) {
        var elemOne = new com.legend.compiler.element.type.ExprType(ec,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        var read = new com.legend.compiler.spec.typed.TypedPropertyAccess(
                new com.legend.compiler.spec.typed.TypedVariable("_mx", elemOne),
                pa.property(), new com.legend.compiler.element.type.ExprType(
                        pa.info().type(),
                        m.mapper().functionType().params().get(0).multiplicity()));
        TypedSpec body = substituteParam(specs, m.mapper(), read);
        var fnT = new Type.FunctionType(
                List.of(new Type.Param(ec,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                m.mapper().functionType().result());
        return new TypedMap(pa.source(), new TypedLambda(List.of("_mx"), List.of(body),
                com.legend.compiler.element.type.ExprType.one(fnT)), m.info());
    }

    /** &beta;-substitute a one-param lambda's variable with {@code read} —
     * via the inliner's LET reduction (one substitution engine, no second
     * walker). */
    static TypedSpec substituteParam(com.legend.compiler.spec.SpecCompiler specs,
            TypedLambda lam, TypedSpec read) {
        // audit 21b F6 (named wall): the let-reduction splices `read` at
        // EVERY param read. For a row-rooted read that is plain multi-eval;
        // for a source CHAIN it is DECORRELATION — a second fresh extent
        // replaces the row-correlated value. Refusing here is by design,
        // not an accident of downstream vocabulary walls.
        if (!rowRootedRead(read)) {
            int reads = 0;
            for (TypedSpec b : lam.body()) {
                reads += countParamReads(b, lam.parameters().get(0));
            }
            if (reads > 1) {
                throw new NotImplementedException("class-result mapper reads"
                        + " its parameter " + reads + " times; splicing the"
                        + " source chain at each read would decorrelate the"
                        + " later reads (a fresh extent replaces the"
                        + " row-correlated value)");
            }
            if (reads == 0) {
                // audit 22a L7: a mapper that IGNORES its parameter has
                // per-element semantics in pure (one result per source
                // element); the let-splice collapses that to ONE evaluation
                // — wrong cardinality, silently.
                throw new NotImplementedException("class-result mapper"
                        + " ignores its parameter — the splice would collapse"
                        + " per-element semantics to one evaluation");
            }
        }
        java.util.List<TypedSpec> body = new java.util.ArrayList<>();
        body.add(new com.legend.compiler.spec.typed.TypedLet(
                lam.parameters().get(0), read, read.info()));
        body.addAll(lam.body());
        return new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(body).get(0);
    }


    /** A read rooted at a VARIABLE (row-correlated): duplication is
     * multi-eval of the same row's value, never decorrelation. */
    static boolean rowRootedRead(TypedSpec read) {
        return switch (read) {
            case TypedVariable ignored -> true;
            case TypedPropertyAccess pa -> rowRootedRead(pa.source());
            default -> false;
        };
    }

    /** Shadow-aware count of {@code $var} reads beneath {@code n}. */
    static int countParamReads(TypedSpec n, String var) {
        if (n instanceof TypedVariable v) {
            return v.name().equals(var) ? 1 : 0;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(var)) {
            return 0;
        }
        int c = 0;
        for (TypedSpec ch : n.children()) {
            c += countParamReads(ch, var);
        }
        return c;
    }

}
