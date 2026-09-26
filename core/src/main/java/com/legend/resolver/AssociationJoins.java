// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * ASSOCIATION-ROUTE join material: the demanded association end's
 * target pipeline (materialized, temporally stamped, synthetic-pred
 * wrapped), the ORIENTED (parent, target) condition from the mapping's
 * legacyAssocPredicate emission, and the deterministic column prefix.
 * A stateless service over the model + sources; the per-resolution
 * frames (TemporalFrame) pass per call.
 */
final class AssociationJoins {

    private final ModelContext ctx;
    private final ClassSources sources;
    private final SpecCompiler specs;
    private final SyntheticHeads synthetics;

    SyntheticHeads synthetics() {
        return synthetics;
    }

    /** Join-kind vocabulary (one owner — §4AD placement bit consumers). */
    static com.legend.compiler.spec.typed.TypedEnumValue leftKind() {
        String fqn = "meta::pure::functions::relation::JoinKind";
        return new com.legend.compiler.spec.typed.TypedEnumValue(fqn, "LEFT",
                new ExprType(new Type.EnumType(fqn),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
    }

    /** INNER — the flatten's null-row guard by construction: the engine
     * spells LEFT and its READER skips null-pk rows; the row sets are
     * identical (deliberate documented emission divergence). Also the
     * §4AD P1 value-position placement bit (row-dropping). */
    static com.legend.compiler.spec.typed.TypedEnumValue innerKind() {
        String fqn = "meta::pure::functions::relation::JoinKind";
        return new com.legend.compiler.spec.typed.TypedEnumValue(fqn, "INNER",
                new ExprType(new Type.EnumType(fqn),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
    }

    /** The #69 exploding-reroute trigger for a nav-slot chain: the
     * head's OWN correlated pred (mid==1), or a TAIL seg's — a sub-hop
     * pred applies in the exploding sub's WHERE (the tail-pred loop)
     * and can never compose on the flat step's ON. Null when the chain
     * composes on the slot spine. */
    @com.legend.base.Nullable TypedLambda explodingReroutePred(
            List<String> path, int mid) {
        if (mid != 1) {
            return null;
        }
        TypedLambda cp = synthetics.correlatedPred(path.get(0));
        if (cp == null) {
            // an ELEMENT-scoped tail pred (re-based onto the head's
            // element, batch 106) applies inside the head's target — it
            // never triggers the ROOT's parent-copy reroute
            cp = path.subList(mid, path.size()).stream()
                    .filter(seg -> !synthetics.isElementScoped(seg))
                    .map(synthetics::correlatedPred)
                    .filter(java.util.Objects::nonNull)
                    .findFirst().orElse(null);
        }
        return cp != null && corrPredDemandsParentNav(cp) ? cp : null;
    }

    AssociationJoins(ModelContext ctx, ClassSources sources,
            SpecCompiler specs, SyntheticHeads synthetics) {
        this.ctx = ctx;
        this.sources = sources;
        this.specs = specs;
        this.synthetics = synthetics;
    }

    /** EMBEDDED PASS-THROUGH ({@code $p.firm.organizations.name} where
     * {@code firm} is an embedded/inline ctor binding): drill the ctor
     * fields along the path; when the chain leaves ctor territory at hop
     * {@code k} and the current ctor's class carries an ASSOCIATION for
     * {@code path[k]}, hop resolution re-roots at that class on the SAME
     * ROW (no join — engine per-hop findPropertyMapping through embedded
     * PMs). Null when the ctor drill serves the whole path (substitution
     * side) or the stop is not an association (today's loud wall). */
    record PassThrough(ClassSource root, int startHop) {}

    @com.legend.base.Nullable PassThrough embeddedPassThrough(ClassSource cs, java.util.List<String> path) {
        TypedSpec cur = cs.bindings().get(SyntheticHeads.realHead(path.get(0)));
        int hop = 1;
        while (hop + 1 <= path.size() - 1) {
            TypedSpec inner = cur;
            if (inner instanceof TypedNativeCall c && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                inner = c.args().get(0);
            }
            if (!(inner instanceof com.legend.compiler.spec.typed
                    .TypedNewInstance ni)) {
                return null;
            }
            if (ni.properties().containsKey(path.get(hop))) {
                cur = ni.properties().get(path.get(hop));
                hop++;
                continue;
            }
            // drill stops here: the hop must be an association of the
            // CURRENT ctor's class, mapped in this mapping
            String stopCls = ni.classFqn();
            if (ctx.findAssociationOf(stopCls,
                            SyntheticHeads.realHead(path.get(hop))).isEmpty()) {
                return null;
            }
            if (!sources.binds(cs.mappingFqn(), stopCls)) {
                // EMBEDDED-ONLY class (the firm block on the owner's row,
                // no set of its own): re-root as a SYNTHETIC view — the
                // owner's pipeline/row with the ctor's fields as bindings
                // (engine: the embedded set implementation shares its
                // owner's table; the assoc join anchors there).
                return new PassThrough(new ClassSource(cs.mappingFqn(),
                        stopCls, null, cs.pipeline(), cs.rowVar(),
                        ni.properties(), cs.rowType()), hop);
            }
            return new PassThrough(
                    sources.get(cs.mappingFqn(), stopCls, cs.scope()), hop);
        }
        return null;   // the whole path is ctor-drillable (or a scalar leaf)
    }

    /** The join material for an aggregated to-many head: the association
     * route, or the navigate-slot route (class-typed Join PM). */
    AssocJoin aggJoinMaterial(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      Set<String> leaves,
                                      Set<List<String>> tgtNavPaths) {
        return aggJoinMaterial(temporal, cs, head, context, leaves, tgtNavPaths, head);
    }

    /** {@code chainKey}: the association route's chain key — the DOTTED
     *  chain when the head is a nested end of a join's target (F-M): the
     *  temporal specs (an explicit hop date) key by it. */
    AssocJoin aggJoinMaterial(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      Set<String> leaves,
                                      Set<List<String>> tgtNavPaths, String chainKey) {
        // synthetic identities (#fN/#cN) bind by their REAL property — the
        // raw lookup missed the navigate-slot route and fell into the
        // association route, which errors when the property is PM-mapped
        TypedSpec binding = cs.bindings().get(SyntheticHeads.realHead(head));
        if (binding == null) {
            return associationJoin(temporal, cs, head, context, false, leaves,
                    chainKey, tgtNavPaths);
        }
        var navSteps = Pipelines.navSteps(cs.pipeline());
        // EMBEDDED head whose leaves ride ONE join slot (employeesExt
        // (birthdate: @J | col) — engine testDateAggregationWithMax): the
        // embedded hop is a NAMESPACE; the slot's target is the aggregated
        // relation and the ctor leaves REBASE onto its row
        if (binding instanceof com.legend.compiler.spec.typed
                .TypedNewInstance ctor) {
            AssocJoin ea = embeddedAggJoin(temporal, cs, head, ctor, navSteps);
            if (ea != null) {
                return ea;
            }
            throw new NotImplementedException("aggregate over embedded"
                    + " property '" + head + "' of class '" + cs.classFqn()
                    + "': the embedded leaves do not ride exactly one"
                    + " navigate slot — only single-slot embedded"
                    + " aggregation is built");
        }
        String alias = InnerDemand.navSlotAlias(binding, cs.rowVar(), navSteps.keySet());
        var nav = java.util.Objects.requireNonNull(
                navSteps.get(alias));
        String targetClass = ((TypedGetAll)
                nav.target()).classFqn();
        ClassSource t = sources.navTarget(cs, targetClass, nav, java.util.Objects.requireNonNull(alias));
        Set<String> targetSlots = Pipelines.slotAliases(t.pipeline());
        Set<String> targetDemand = new LinkedHashSet<>();
        if (!targetSlots.isEmpty()) {
            for (String leaf : leaves) {
                TypedSpec b = t.bindings().get(leaf);
                if (b != null) {
                    CorrelatedSubselects.collectAliasReads(b, t.rowVar(), targetSlots, targetDemand);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(t.pipeline(), targetDemand);
        // #69: the correlated pred's / computed mapper's TARGET-side reads
        // may hop the target's OWN class-typed nav steps ($e.address.name
        // over the aggregated rows) — demand those steps and build depth-1
        // SubNavs for the composition's pass-1 dispatch (mirrors the
        // exists route). Deeper hops stay loud (empty children).
        TypedLambda corrN = synthetics.correlatedPred(head);
        Set<List<String>> predPaths = new LinkedHashSet<>(tgtNavPaths);
        if (corrN != null) {
            for (TypedSpec b : corrN.body()) {
                FlattenOps.consumedPaths(b,
                        corrN.parameters().get(0), predPaths);
            }
        }
        // CLOSED parked preds contribute nav demand too — the applyToPipe
        // application below rewrites them against this target, and a pred
        // hopping a class-typed nav step ($e.address.name == 'New York',
        // the chained-unions agg family) needs its SubNav registered
        // exactly like a correlated pred's reads (task #78)
        for (TypedLambda sp : synthetics.allPreds(head)) {
            for (TypedSpec b : sp.body()) {
                FlattenOps.consumedPaths(b,
                        sp.parameters().get(0), predPaths);
            }
        }
        var tNavSteps = Pipelines.navSteps(t.pipeline());
        Map<String, String> predNavAliases = new java.util.LinkedHashMap<>();
        Set<String> tNavDemand = new LinkedHashSet<>();
        for (List<String> pp : predPaths) {
            if (pp.size() < 2) {
                continue;
            }
            TypedSpec hb = t.bindings().get(pp.get(0));
            String al = hb == null ? null
                    : InnerDemand.navSlotAlias(hb, t.rowVar(),
                            tNavSteps.keySet());
            if (al != null) {
                tNavDemand.add(al);
                predNavAliases.put(pp.get(0), al);
            }
        }
        // the navigate's target-side key reads ride the target's union
        // arms: a routed key is a MEMBER COLUMN the union body does not
        // project (B3.1) — widen beneath the materialization
        TypedSpec tPipeline = Pipelines.containsConcatenate(t.pipeline())
                ? StackBuilder.demandForCondition(t.pipeline(), nav.predicate(), 1) : t.pipeline();
        Pipelines.Materialized tMat = tNavDemand.isEmpty()
                ? Pipelines.materialize(
                        tPipeline, targetDemand, t.classFqn())
                : Pipelines.materialize(
                        tPipeline, targetDemand, tNavDemand, t.classFqn(),
                        (al2, tc2) -> Pipelines.materialize(
                                sources.navTarget(t, tc2, ClassSources.stepOf(t, al2), al2).pipeline(),
                                java.util.Set.of(), tc2).pipeline());
        Map<String, Substitution.SubNav> tSubNavs = new java.util.LinkedHashMap<>();
        for (var pne : predNavAliases.entrySet()) {
            String pfx = tMat.slotPrefixes().get(pne.getValue());
            var stepN = java.util.Objects.requireNonNull(tNavSteps.get(pne.getValue()));
            var stepT = stepN.target();
            if (pfx == null || !(stepT instanceof TypedGetAll stg)) {
                continue;
            }
            ClassSource sub = sources.navTarget(t, stg.classFqn(), stepN, pne.getValue());
            tSubNavs.put(pne.getKey(), new Substitution.SubNav(
                    pfx, sub.rowVar(), sub.bindings()));
        }
        TypedSpec tPipe0 = temporal.temporalTargetPipe(cs, t, head,
                temporal.applyJoinTemporalFilters(tMat.pipeline(), t, Map.of()));
        // a lifted head's parked material (filter / union branches)
        // applies to the aggregated target exactly like the plain routes.
        // A CORRELATED pred does NOT apply here — it composes into the
        // aggregated subselect's WHERE at the FOLD (parent-copy
        // architecture, #69); the fold re-checks the head and is loud if
        // it cannot compose (applyToPipe passes corr-only heads through
        // unchanged, never silently filtering).
        tPipe0 = synthetics.applyToPipe(head, tPipe0, (p, pred) ->
                CorrelatedSubselects.predFilteredPipe(p, t, tMat.slotPrefixes(),
                        tSubNavs, pred, cs.mappingFqn()));
        TypedLambda cond0 = withOuterDatedWindow(temporal, cs, t, head,
                nav.predicate(), tPipe0);
        return new AssocJoin(prefixFor(head, cs), t, tPipe0,
                Type.requireRelationSchema(tPipe0.info().type()), cond0,
                tMat.slotPrefixes(), tSubNavs, null,
                onFormOf(tPipe0, cond0), synthetics.isInnerValueHead(head));
    }

    /** The ONE navigate-slot alias an embedded ctor's leaves read through,
     * or null (no alias read / more than one — those heads keep their
     * plain routes). Shared by the head recognizer (StoreResolver) and the
     * material builder so routing and building cannot drift. */
    /** {@code head} navigates: an unbound association end or a
     * navigate-slot binding, ANY multiplicity — the bare-count route
     * (count(rows) is row-correct regardless of the declared bound; the
     * modelJoin corpus declares [1] ends whose join conditions fan out,
     * and the engine counts ROWS). */
    /** {@code head} is a to-many navigation: an unbound association end, or
     * a navigate-slot binding (class-typed Join PM), with to-many
     * multiplicity on the class property. Synthetic identities (#fN/#cN/#dN)
     * route by their REAL property — an aggregate over a lifted head must take
     * the grouped-subselect route, never bare-explode. findProperty misses
     * ASSOCIATION-DECLARED ends (modelJoin/XStore declare them on the
     * Association only): fall through to the end's own multiplicity. */
    boolean isToManyAssocHead(ClassSource cs, String head) {
        String real = SyntheticHeads.realHead(head);
        boolean toMany = ctx.findProperty(cs.classFqn(), real)
                .map(pr -> !(pr.multiplicity()
                        instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper())))
                .orElseGet(() -> ctx.findAssociationOf(cs.classFqn(), real)
                        .map(a -> !(a.property1().propertyName().equals(real)
                                ? a.property1() : a.property2()).isToOne())
                        .orElse(false));
        return toMany && isAssocOrNavHead(cs, real);
    }

    boolean isAssocOrNavHead(ClassSource cs, String head) {
        String real = SyntheticHeads.realHead(head);
        TypedSpec binding = cs.bindings().get(real);
        if (binding != null) {
            var navSteps = Pipelines.navSteps(cs.pipeline());
            // single-join-slot EMBEDDED heads aggregate (embeddedAggJoin;
            // unrouted, the explode+reduce pair silently drops the reducer)
            if (binding instanceof com.legend.compiler.spec.typed
                    .TypedNewInstance ctor) {
                return embeddedAggAlias(cs, ctor) != null;
            }
            String alias = InnerDemand.navSlotAlias(binding, cs.rowVar(),
                    navSteps.keySet());
            if (alias == null) {
                return false;   // otherwise heads keep their routes
            }
            var nav = java.util.Objects.requireNonNull(
                    navSteps.get(alias));
            return nav.target() instanceof com.legend.compiler.spec.typed
                    .TypedGetAll tg
                    && sources.binds(cs.mappingFqn(), tg.classFqn());
        }
        return ctx.findAssociationOf(cs.classFqn(), real).isPresent();
    }

    static @com.legend.base.Nullable String embeddedAggAlias(ClassSource cs,
            com.legend.compiler.spec.typed.TypedNewInstance ctor) {
        Set<String> aliases = new LinkedHashSet<>(
                Pipelines.navSteps(cs.pipeline()).keySet());
        aliases.addAll(Pipelines.slotAliases(cs.pipeline()));
        return embeddedAggAlias(cs, ctor, aliases);
    }

    private static @com.legend.base.Nullable String embeddedAggAlias(ClassSource cs,
            com.legend.compiler.spec.typed.TypedNewInstance ctor,
            java.util.Set<String> navAliases) {
        Set<String> reads = new LinkedHashSet<>();
        for (TypedSpec v : ctor.properties().values()) {
            CorrelatedSubselects.collectAliasReads(v, cs.rowVar(),
                    navAliases, reads);
        }
        return reads.size() == 1 ? reads.iterator().next() : null;
    }

    /** Aggregation material for an EMBEDDED head: pseudo target source =
     * the slot's raw target pipeline; bindings = the ctor leaves REBASED
     * from {@code $row.alias.col} onto the target row (leaves reading the
     * parent row directly are omitted — loud at aggColFor if demanded);
     * condition = the slot predicate, unchanged. */
    private @com.legend.base.Nullable AssocJoin embeddedAggJoin(TemporalFrame temporal, ClassSource cs,
            String head, com.legend.compiler.spec.typed.TypedNewInstance ctor,
            java.util.Map<String, com.legend.compiler.spec.typed
                    .TypedNavigate> navSteps) {
        var joinSlots = Pipelines.joinSlots(cs.pipeline());
        String alias = embeddedAggAlias(cs, ctor);
        if (alias == null) {
            return null;
        }
        TypedSpec tPipe;
        TypedLambda stepCond;
        if (navSteps.containsKey(alias)) {
            var nav = navSteps.get(alias);
            tPipe = nav.target();
            stepCond = (TypedLambda) nav.predicate();
        } else {
            var slot = java.util.Objects.requireNonNull(
                    joinSlots.get(alias));
            tPipe = slot.target();
            stepCond = slot.condition();
        }
        Type.RelationType tRow = Type.relationSchema(tPipe.info().type());
        if (tRow == null) {
            return null;
        }
        String tv = "_embt";
        while (cs.rowVar().equals(tv)) {
            tv = "_" + tv;
        }
        var tVarInfo = new ExprType(tRow,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        Map<String, TypedSpec> bnd = new java.util.LinkedHashMap<>();
        for (var e : ctor.properties().entrySet()) {
            TypedSpec r = rebaseAliasReads(e.getValue(), cs.rowVar(), alias,
                    tv, tVarInfo);
            if (r != null) {
                bnd.put(e.getKey(), r);
            }
        }
        if (bnd.isEmpty()) {
            return null;
        }
        ClassSource t = new ClassSource(cs.mappingFqn(), ctor.classFqn(),
                null, tPipe, tv, bnd, tRow);
        TypedSpec tPipe0 = temporal.temporalTargetPipe(cs, t, head,
                temporal.applyJoinTemporalFilters(tPipe, t, Map.of()));
        tPipe0 = synthetics.applyToPipe(head, tPipe0, (p, pred) ->
                CorrelatedSubselects.predFilteredPipe(p, t, Map.of(),
                        Map.of(), pred, cs.mappingFqn()));
        TypedLambda cond0 = withOuterDatedWindow(temporal, cs, t, head,
                stepCond, tPipe0);
        return new AssocJoin(prefixFor(head, cs), t, tPipe0,
                Type.requireRelationSchema(tPipe0.info().type()), cond0,
                Map.of(), Map.of(), null, onFormOf(tPipe0, cond0),
                synthetics.isInnerValueHead(head));
    }

    /** {@code $row.alias.col} reads become {@code $tv.col}; any OTHER
     * read rooted at {@code rowVar} (a parent column, a different alias)
     * makes the leaf unrebasable — null. */
    private static @com.legend.base.Nullable TypedSpec rebaseAliasReads(TypedSpec n, String rowVar,
            String alias, String tv, ExprType tVarInfo) {
        boolean[] bad = {false};
        TypedSpec out = rebaseWalk(n, rowVar, alias, tv, tVarInfo, bad);
        return bad[0] ? null : out;
    }

    private static TypedSpec rebaseWalk(TypedSpec n, String rowVar,
            String alias, String tv, ExprType tVarInfo, boolean[] bad) {
        if (n instanceof com.legend.compiler.spec.typed
                .TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed
                        .TypedPropertyAccess inner
                && inner.source() instanceof com.legend.compiler.spec.typed
                        .TypedVariable v
                && v.name().equals(rowVar)
                && inner.property().equals(alias)) {
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    new com.legend.compiler.spec.typed.TypedVariable(tv,
                            tVarInfo), pa.property(), pa.info());
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v2
                && v2.name().equals(rowVar)) {
            bad[0] = true;
            return n;
        }
        return n.mapChildren(c -> rebaseWalk(c, rowVar, alias, tv,
                tVarInfo, bad));
    }

    /** The ON form when the stamped pipe is exactly FILTER layers over a
     * filter-free scan pipe (only temporalTargetPipe's own layers move —
     * scanColumns testQualifier pins that ordinary filters never do);
     * null otherwise (hybrid replaceScan stamps, pre-filtered pipes,
     * views keep the in-pipe form for every consumer). */
    private @com.legend.base.Nullable OnForm onFormOf(TypedSpec stamped, TypedLambda cond) {
        java.util.List<TypedLambda> stamps = new java.util.ArrayList<>();
        TypedSpec cur = stamped;
        while (cur instanceof com.legend.compiler.spec.typed.TypedFilter tf0
                && TemporalFrame.STAMP_ROW_VAR.equals(
                        tf0.predicate().parameters().get(0))) {
            stamps.add(tf0.predicate());
            cur = tf0.source();
        }
        if (stamps.isEmpty() || Pipelines.containsConcatenate(cur)) {
            return null;
        }
        var boolT = ExprType.one(Type.Primitive.BOOLEAN);
        var tRowInfo = new ExprType(cur.info().type(),
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        String tv = cond.parameters().get(1);
        TypedSpec merged = cond.body().get(0);
        // outside-in: the OUTER stamp's conds append last (bitemp keeps
        // processing-then-business, the engine's spelling order)
        for (int i = stamps.size() - 1; i >= 0; i--) {
            var st = stamps.get(i);
            TypedSpec pb = renameCondVar(st.body().get(0),
                    st.parameters().get(0), tv, tRowInfo);
            merged = boolCall("and", merged, pb, boolT);
        }
        return new OnForm(cur, new TypedLambda(cond.parameters(),
                List.of(merged), cond.info()));
    }

    private static TypedSpec renameCondVar(TypedSpec n, String from,
            String to, ExprType rowInfo) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(from)) {
            return new com.legend.compiler.spec.typed.TypedVariable(to, rowInfo);
        }
        return SyntheticHeads.rebuildChildren(n,
                c -> renameCondVar(c, from, to, rowInfo));
    }

    /** TARGET-SIDE join-key widening: a distinct-narrowed or UNION target
     * must expose the key columns the association condition binds on
     * (engine partial-union goldens); the union wall names its head. */
    private static TypedSpec widenForConditionKeys(TypedLambda oriented,
            TypedSpec pipeline, String head, ClassSource cs) {
        Set<String> tgtReads = new LinkedHashSet<>();
        for (TypedSpec b : oriented.body()) {
            Pipelines.collectVarReads(b, oriented.parameters().get(1), tgtReads);
        }
        TypedSpec tPipe = Pipelines.widenDistinctForKeys(pipeline, tgtReads);
        try {
            return StackBuilder.demandForKeys(tPipe, tgtReads);
        } catch (NotImplementedException e) {
            throw new NotImplementedException(e.getMessage()
                    + " [association head '" + head + "' on "
                    + cs.classFqn() + "]");
        }
    }

    private TypedSpec boolCall(String op, TypedSpec a, TypedSpec b,
            ExprType boolT) {
        var fns = ctx.findFunction("meta::pure::functions::boolean::" + op)
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (fns.size() != 1) {
            throw new IllegalStateException(
                    "resolver bug: expected one 2-arg boolean::" + op);
        }
        return new TypedNativeCall(fns.get(0), List.of(a, b), boolT);
    }

    /** OUTER-ROW date ($o.product($o.orderDate)): the temporal window
     * composes into the NAV/ASSOC CONDITION — both rows in scope (engine:
     * window in the join ON against "root".orderDate; temporalTargetPipe
     * left the pipe unstamped for such specs). Identity otherwise. */
    static TypedLambda withOuterDatedWindow(TemporalFrame temporal,
            ClassSource cs, ClassSource target, String head,
            TypedLambda cond, TypedSpec targetPipe) {
        String odc = temporal.outerDateColumn(head, cs);
        if (odc != null) {
            return temporal.withDeferredOuterSubWindows(
                    temporal.outerDatedJoinCond(cond, cs.pipeline(), targetPipe,
                            target.classFqn(), odc),
                    cs.pipeline(), targetPipe, head, odc, target.classFqn());
        }
        TypedLambda bi = temporal.outerBiDatedJoinCond(cond, cs.pipeline(),
                targetPipe, cs, target, head);
        return bi != null ? bi : cond;
    }

    /** A demanded association navigation, ready to emit as a prefixed LEFT
     * join. {@code targetSubNavs}: the target's OWN demanded class-typed
     * nav steps (#69 — the correlated pred / computed mapper read through
     * them inside the aggregated subselect). */
    /** The ENGINE ON-FORM of a temporal hop (memory milestoning-onclause-
     * seam): the SAME window predicates temporalTargetPipe stamped onto the
     * pipe, relocated onto the join condition — pipe raw, cond ∧ window.
     * CONSUMER-SIDE choice: plain LEFT-join emitters opt in for engine
     * parity; grouped/materialize routes (which ignore the condition
     * channel) keep the stamped form. */
    record OnForm(TypedSpec pipeline, TypedLambda condition) {
    }

    /** {@code rowDropping}: the §4AD placement bit as a CONSTRUCTION-
     * TIME fact — value-position heads join INNER (fold sites read it,
     * never re-derive). */
    record AssocJoin(String prefix, ClassSource target,
                             TypedSpec targetPipeline,
                             Type.RelationType targetRow,
                             @com.legend.base.Nullable TypedLambda condition,
                             Map<String, String> targetSlotPrefixes,
                             Map<String, Substitution.SubNav> targetSubNavs,
                             @com.legend.base.Nullable TypedLambda corrSubPred,
                             @com.legend.base.Nullable OnForm onForm,
                             boolean rowDropping) {

        AssocJoin withCondition(TypedLambda cond) {
            // a rewritten condition invalidates the ON form (its window
            // was composed against the ORIGINAL condition body)
            return new AssocJoin(prefix, target, targetPipeline, targetRow,
                    cond, targetSlotPrefixes, targetSubNavs, corrSubPred,
                    null, rowDropping);
        }

        AssocJoin withTargetPipeline(TypedSpec pipe) {
            return new AssocJoin(prefix, target, pipe,
                    Type.requireRelationSchema(pipe.info().type()), condition,
                    targetSlotPrefixes, targetSubNavs, corrSubPred, null,
                    rowDropping);
        }
    }

    /**
     * A chained hop's prefix, ordinal-bumped against the ACCUMULATED column
     * set: the source row plus every already-registered join's prefixed
     * columns — the same guard {@link #prefixFor} gives hop 0.
     */
    static String chainedPrefix(String base, ClassSource cs,
                                        Map<String, AssocJoin> joinsByChain) {
        Set<String> taken = new LinkedHashSet<>();
        for (Type.Column c : cs.rowType().columns()) {
            taken.add(c.name());
        }
        for (AssocJoin aj : joinsByChain.values()) {
            for (Type.Column c : aj.targetRow().columns()) {
                taken.add(aj.prefix() + c.name());
            }
        }
        String prefix = base + "_";
        int ordinal = 2;
        while (hasPrefixCollision(prefix, taken)) {
            prefix = base + "_" + ordinal++ + "_";
        }
        return prefix;
    }

    /** Deterministic prefix with ordinal bump on collision against the
     * parent row (plan §2.3). PER-IDENTITY (§4AD batch 7): a SYNTHETIC
     * head spells its identity suffix into the prefix
     * ({@code employees#f0 → employees_f0_}) — identity ⇒ name,
     * collision-free against the PLAIN head's material BY CONSTRUCTION
     * (the batch-1 duplicate-{@code employees_ID} wall, fixed
     * structurally, never by registration-order probing). */
    static String prefixFor(String head, ClassSource cs) {
        return prefixFor(head, cs.rowType());
    }

    /** {@link #prefixFor(String, ClassSource)} against an explicit row —
     * a join folded onto an already-materialized pipeline bumps against
     * THAT row (the nested element reroute, batch 106). */
    static String prefixFor(String head, Type.RelationType row) {
        Set<String> taken = new LinkedHashSet<>();
        for (Type.Column c : row.columns()) {
            taken.add(c.name());
        }
        String base = head.replace('#', '_');
        String prefix = base + "_";
        int ordinal = 2;
        while (hasPrefixCollision(prefix, taken)) {
            prefix = base + "_" + ordinal++ + "_";
        }
        return prefix;
    }

    private static boolean hasPrefixCollision(String prefix, Set<String> taken) {
        for (String t : taken) {
            if (t.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /** The mapping's AssociationBinding for {@code assocFqn}, searching the
     * include closure transitively (own definitions win, first match by
     * declaration order — the class-binding resolution's exact rule). */
    private java.util.Optional<com.legend.model.MappingDefinition.AssociationBinding>
            associationBindingInClosure(String mappingFqn, String assocFqn) {
        java.util.ArrayDeque<String> queue = new java.util.ArrayDeque<>();
        Set<String> seen = new LinkedHashSet<>();
        queue.add(mappingFqn);
        while (!queue.isEmpty()) {
            String fqn = queue.poll();
            if (!seen.add(fqn)) {
                continue;
            }
            var m = ctx.findMapping(fqn);
            if (m.isEmpty()) {
                continue;
            }
            var hit = m.get().associationBinding(assocFqn);
            if (hit != null) {
                return java.util.Optional.of(hit);
            }
            for (var inc : m.get().includes()) {
                queue.add(inc.mappingPath());
            }
        }
        return java.util.Optional.empty();
    }

    /** The recursive nav materializer (set once by the resolver after
     * construction — it needs THIS instance): a tail continuing through a
     * target's navigate slot materializes that slot's target too, at any
     * depth (the depth leg, 2026-09-02). */
    private @com.legend.base.Nullable NavMaterializer navMaterializer;
    private @com.legend.base.Nullable UnionHeads unionHeads;

    private UnionHeads unionHeads() {
        if (unionHeads == null) {
            unionHeads = new UnionHeads(ctx, sources, synthetics, this,
                    navMaterializer);
        }
        return unionHeads;
    }

    void setNavMaterializer(NavMaterializer nm) {
        this.navMaterializer = nm;
    }

    /** The SOURCE-side key columns an association end mapped on the MEMBER
     * sets of a union / inheritance target reads: the union target itself
     * carries no hoisted binding for {@code head} (each member routes it
     * on its own), so the outer association join's condition reads the
     * members' key columns off the composed row — they must be demanded
     * through the union projection (group F burn 2026-09-02: a silent
     * miss surfaced as a SQL binder error, never a wall). Empty when no
     * member maps the end. */
    Set<String> memberAssocKeyReads(String mappingFqn, ClassSource unionTarget,
            String head) {
        Set<String> reads = new LinkedHashSet<>();
        String real = SyntheticHeads.realHead(head);
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return reads;
        }
        Set<String> members = new LinkedHashSet<>();
        List<String> declared = ctx.unionMemberClasses(mappingFqn, unionTarget.classFqn());
        if (declared != null) {
            members.addAll(declared);
        }
        for (var cb : md.classBindingsWithIncludes(
                ctx.subtree(unionTarget.classFqn()), ctx::findMapping)) {
            members.add(cb.classFqn());
        }
        for (String m : members) {
            ClassSource ms;
            try {
                ms = sources.get(mappingFqn, m, unionTarget.scope());
            } catch (com.legend.error.MappingResolutionException e) {
                continue;
            }
            TypedSpec b = ms.bindings().get(real);
            if (b == null) {
                continue;
            }
            // the end is a NAV SLOT in the member pipeline: the key columns
            // are the slot's join-condition reads on the member row
            var steps = Pipelines.navSteps(ms.pipeline());
            String alias = InnerDemand.navSlotAlias(b, ms.rowVar(), steps.keySet());
            var step = alias == null ? null : steps.get(alias);
            if (step != null && step.predicate().parameters().size() == 2) {
                for (TypedSpec pb : step.predicate().body()) {
                    Pipelines.collectVarReads(pb, step.predicate().parameters().get(0), reads);
                }
            } else {
                CorrelatedSubselects.collectAliasReads(b, ms.rowVar(),
                        Pipelines.slotAliases(ms.pipeline()), reads);
            }
        }
        return reads;
    }

    /** The class an ASSOCIATION end named {@code prop} on {@code classFqn}
     * navigates to, if an association realizes it (the assoc-sub probe —
     * union V3). */
    java.util.Optional<String> assocTargetClassOf(String classFqn, String prop) {
        return ctx.findAssociationOf(classFqn, prop).map(a ->
                (a.property1().propertyName().equals(prop)
                        ? a.property1() : a.property2()).targetClassFqn());
    }

    AssocJoin associationJoin(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      boolean forExists) {
        return associationJoin(temporal, cs, head, context, forExists, Set.of());
    }

    AssocJoin associationJoin(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      boolean forExists, Set<String> demandedLeaves) {
        return associationJoin(temporal, cs, head, context, forExists, demandedLeaves, head);
    }

    /** {@code chainKey}: the dotted path prefix this hop sits at — the
     * temporal-spec registry key (= {@code head} for hop 0). */
    AssocJoin associationJoin(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      boolean forExists, Set<String> demandedLeaves,
                                      String chainKey) {
        return associationJoin(temporal, cs, head, context, forExists,
                demandedLeaves, chainKey, Set.of());
    }

    /** TARGET-side nav tails per chain key: for each consumed path, the
     * segments past each hop. A BARE (size-1) class-typed tail head
     * demands its target join only when TO-ONE — an unread to-many
     * step would explode the hop's rows; deeper tails are genuinely
     * read (the qualifier-truncated demand shape, testJoinThroughView:
     * the qualifier body's leaves never reach the demand scan, so the
     * bare class hop must still expose its SubNav). */
    Map<String, java.util.Set<List<String>>> chainNavTails(ClassSource cs,
            java.util.Set<List<String>> paths) {
        Map<String, java.util.Set<List<String>>> out =
                new java.util.LinkedHashMap<>();
        for (List<String> path : paths) {
            String cur = cs.classFqn();
            for (int i = 0; i + 1 < path.size(); i++) {
                cur = hopTargetClass(cur, path.get(i));
                if (cur == null) {
                    // pseudo-hops (.milestoning, stc_ synthetics) have no
                    // target class — the resolvable chain ends here (the
                    // pre-gate code walked on with null, registering
                    // nothing; gate-caught: a loud wrap broke 12 tests)
                    break;
                }
                List<String> tail = path.subList(i + 1, path.size());
                if (tail.size() >= 2 || toOneClassProp(cur, tail.get(0))) {
                    out.computeIfAbsent(
                            String.join(".", path.subList(0, i + 1)),
                            k -> new java.util.LinkedHashSet<>()).add(tail);
                }
            }
        }
        return out;
    }

    /** TO-ONE class-typed property test — the bare-tail demand gate: a
     * to-one hop joins row-safely even when the demand scan saw no leaf
     * (qualifier-truncated reads); a to-many hop would explode rows. */
    boolean toOneClassProp(String clsFqn, String prop) {
        return clsFqn != null && ctx.findProperty(clsFqn,
                SyntheticHeads.realHead(prop))
                .map(p2 -> Type.asClassType(p2.type()) instanceof Type.ClassType
                        && p2.multiplicity() instanceof
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded bb
                        && Integer.valueOf(1).equals(bb.upper()))
                .orElse(false);
    }

    /** The class a hop lands on: a declared class-typed property, or an
     * association end (the walk's own dispatch — findProperty alone
     * misses association-declared ends). */
    @com.legend.base.Nullable String hopTargetClass(String clsFqn, String prop) {
        if (clsFqn == null) {
            return null;
        }
        String real = SyntheticHeads.realHead(prop);
        var pr = ctx.findProperty(clsFqn, real).orElse(null);
        if (pr != null && Type.asClassType(pr.type()) instanceof Type.ClassType ct) {
            return ct.fqn();
        }
        return ctx.findAssociationOf(clsFqn, real)
                .map(a -> (a.property1().propertyName().equals(real)
                        ? a.property1() : a.property2()).targetClassFqn())
                .orElse(null);
    }

    /** {@code navTails}: TARGET-side nav paths past this head
     * (placeOfInterest.name on Location for
     * $e.locations.placeOfInterest.name) — each tail's first segment
     * demands the target's navigate slot and exposes a SubNav so the
     * multi-hop walk resolves through it (task #70/#78). */
    AssocJoin associationJoin(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      boolean forExists, Set<String> demandedLeaves,
                                      String chainKey, Set<List<String>> navTails) {
        // a UNION head ({@code #uN}) has no underlying property: its join
        // target is the UNION ALL of its branch chains (UnionHeads)
        if (SyntheticHeads.isUnion(head)) {
            return unionHeads().material(temporal, cs, head, context,
                    demandedLeaves);
        }
        // A SYNTHETIC head resolves by its underlying property; its parked
        // predicate joins the leaf demand (the pred's own reads pull the
        // target's slots) and wraps the finished target pipeline below.
        String real = SyntheticHeads.realHead(head);
        List<TypedLambda> synthPreds = synthetics.allPreds(head);
        if (!synthPreds.isEmpty()) {
            Set<String> withPredLeaves = new LinkedHashSet<>(demandedLeaves);
            for (TypedLambda sp : synthPreds) {
                for (TypedSpec b : sp.body()) {
                    InnerDemand.collectParamPathHeads(b, sp.parameters().get(0),
                            withPredLeaves);
                }
            }
            demandedLeaves = withPredLeaves;
        }
        var assoc = ctx.findAssociationOf(cs.classFqn(), real).orElseThrow(() ->
                new MappingResolutionException("property '"
                        + SyntheticHeads.displayName(real) + "' of class '"
                        + cs.classFqn() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'", cs.classFqn()));
        // The end from the SAME association object — a separate index lookup
        // was a split-brain with findAssociationOf (audit blocker).
        var end = assoc.property1().propertyName().equals(real)
                ? assoc.property1() : assoc.property2();
        // A CONCRETE end joins: to-one flat, to-many with ROW EXPLOSION
        // (projection semantics — engine/V1/plangen unanimous). A
        // Parameter-multiplicity end stays denied (unknown cardinality).
        if (!forExists && !end.isConcrete()) {
            throw new NotImplementedException("navigation of association end '$"
                    + head + "' with non-concrete multiplicity "
                    + end.multiplicity() + " is not supported");
        }
        String targetClass = end.targetClassFqn();
        // predicate material FIRST (route A, XSTORE_LEG brief §3): a
        // property-space emission pins the target SET — its id must reach
        // this get call, which historically never passed one
        PredMaterial pm = predicateMaterial(cs, assoc, real, targetClass);
        ClassSource target = pm.targetSetId() != null
                ? sources.get(cs.mappingFqn(), targetClass, pm.targetSetId(), null, "", cs.scope())
                : sources.get(cs.mappingFqn(), targetClass, cs.scope());
        // The TARGET's own join slots materialize on demand too: a demanded
        // leaf whose binding reads a slot ($p.firm.country where country is
        // @FirmCountry-mapped) pulls that slot's LEFT join into the target
        // pipeline — nested navigation joins, the W4 slice.
        Set<String> targetSlots = Pipelines.slotAliases(target.pipeline());
        Set<String> targetDemand = new LinkedHashSet<>();
        if (!targetSlots.isEmpty()) {
            for (String leaf : demandedLeaves) {
                TypedSpec b = target.bindings().get(leaf);
                if (b != null) {
                    CorrelatedSubselects.collectAliasReads(b, target.rowVar(), targetSlots, targetDemand);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(target.pipeline(), targetDemand);
        var tNavSteps3 = Pipelines.navSteps(target.pipeline());
        // the target's tails: slot tails (recursive materialization) and
        // nested ASSOCIATION tails (the navigate() rule) — extracted at the
        // numbered seam (CodeShapeGuardrailTest)
        TargetTails tt = collectTargetTails(temporal, target, navTails, tNavSteps3, chainKey);
        Set<String> tNavDemand3 = tt.tNavDemand();
        Map<String, String> tailNavAliases = tt.tailNavAliases();
        Map<String, List<List<String>>> subTailsByAlias = tt.subTailsByAlias();
        Map<String, Set<String>> nestedAssocReads = tt.nestedAssocReads();
        Map<String, Set<List<String>>> nestedAssocTails = tt.nestedAssocTails();
        // EARLY predicate scan (before materialization): the association
        // CONDITION's target-side slot reads demand the target's navigate
        // steps — a ModelJoin/XStore condition navigating another
        // association ($employees.address.city) reads the nested nav
        // slot, and the stock slot machinery materializes it (user rule:
        // both are just navigate()). Target-side param ONLY — demanding
        // an unread to-many step would explode target rows.
        String condTgtVar = scanCondTargetReads(cs, assoc, real, targetClass,
                target, targetSlots, targetDemand, tNavSteps3.keySet(),
                tNavDemand3, nestedAssocReads);
        // the SYNTHETIC PREDICATE's nested-association reads (L1, 2026-09-05:
        // `employees->filter(e | $e.address.city == 'NYC')` — the filter
        // reads an association OF THE TARGET): the same navigate() rule
        // widens the target pipe with the nested join; the pred then reads
        // the nested target through a SubNav registered on the widened row
        for (TypedLambda sp : synthPreds) {
            if (!sp.parameters().isEmpty()) {
                for (TypedSpec b : sp.body()) {
                    collectNestedAssocReads(b, sp.parameters().get(0),
                            targetClass, nestedAssocReads);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(
                target.pipeline(), targetDemand);
        Map<String, NavMaterializer.NavMat> tailMats = new java.util.LinkedHashMap<>();
        final String chainKey0 = chainKey;
        Pipelines.Materialized tMat0 = tNavDemand3.isEmpty()
                ? Pipelines.materialize(
                        target.pipeline(), targetDemand, target.classFqn())
                : Pipelines.materialize(
                        target.pipeline(), targetDemand, tNavDemand3,
                        target.classFqn(),
                        (aln, tcn) -> {
                            List<List<String>> deeper = subTailsByAlias.get(aln);
                            if (deeper != null && navMaterializer != null) {
                                NavMaterializer.NavMat nm = navMaterializer
                                        .navTargetMaterialized(temporal,
                                                sources.navTarget(target, tcn, ClassSources.stepOf(target, aln), aln),
                                                cs.mappingFqn(), tcn, cs.scope(), deeper,
                                                chainKey0 + "." + aln,
                                                TemporalContext.NONE);
                                tailMats.put(aln, nm);
                                return nm.pipeline();
                            }
                            return Pipelines.materialize(
                                    sources.navTarget(target, tcn, ClassSources.stepOf(target, aln), aln).pipeline(),
                                    java.util.Set.of(), tcn).pipeline();
                        });
        TypedSpec basePipe = temporal.temporalTargetPipe(cs, target, chainKey,
                temporal.applyJoinTemporalFilters(tMat0.pipeline(), target,
                        Map.of()));
        // NESTED-ASSOCIATION widening (the navigate() rule): a condition or
        // the synthetic predicate reading $tgt.<assocProp>.<col> joins the
        // nested association's target into the materialized pipe, prefixed
        NestedWidening nw = widenNestedAssocs(temporal, target, context,
                nestedAssocReads, nestedAssocTails, basePipe, chainKey);
        basePipe = nw.pipe();
        Map<String, String> nestedPrefixByProp = nw.prefixByProp();
        Map<String, Substitution.SubNav> nestedSubNavs = nw.subNavs();
        final Pipelines.Materialized tMat = new Pipelines.Materialized(
                basePipe, tMat0.slotPrefixes(), tMat0.stripped());

        TypedLambda oriented;
        if (pm.propertySpace()) {
            // route A: the cond's params are END-CLASS instances —
            // substitute each side through its set's composed bindings
            // onto fresh relation-row binders; output contract identical
            // to the column-space emission (leftRow=PARENT, rightRow=TARGET)
            oriented = propertyCondToColumns(pm.cond(), pm.reverse(), cs,
                    target, tMat.slotPrefixes(),
                    Type.requireRelationSchema(tMat.pipeline().info().type()),
                    temporal, context);
        } else {
            TypedLambda cond = pm.cond();
            oriented = rewriteNestedAssocCondReads(cond, condTgtVar,
                    nestedPrefixByProp,
                    Type.requireRelationSchema(tMat.pipeline().info().type()));
            if (pm.reverse()) {
                var ft = cond.functionType();
                var swapped = new Type.FunctionType(
                        List.of(ft.params().get(1), ft.params().get(0)),
                        ft.result());
                oriented = new TypedLambda(List.of(cond.parameters().get(1),
                        cond.parameters().get(0)), cond.body(),
                        new ExprType(swapped,
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
            }
        }
        // A CORRELATED lifted predicate ANDs into the CONDITION — the one
        // place both rows are in scope: its own param substitutes against
        // the TARGET bindings over the condition's target row; the residual
        // OUTER-variable reads substitute against the PARENT's bindings
        // over the condition's source row (audit 14 B-F1's correlation
        // pass). Runs BEFORE key collection so the pred's target reads
        // widen distinct/union keys too.
        TypedLambda corr = synthetics.correlatedPred(head);
        TypedLambda corrSub = null;
        if (corr != null) {
            // ROUTE RULE (engine parity, testFunctionVariables goldens):
            // a pred whose OUTER reads are plain parent properties
            // composes into the flat join ON (both rows in scope); a pred
            // demanding a parent NAV hop ($f.address.name) can never
            // resolve there — it rides the parent-copy subselect at the
            // fold (#69, the exploding variant of the aggregated shape).
            if (corrPredDemandsParentNav(corr)) {
                corrSub = corr;
            } else {
                oriented = andCorrelatedIntoCondition(oriented, corr, cs,
                        target, tMat.slotPrefixes());
            }
        }
        TypedSpec tPipe = widenForConditionKeys(oriented, tMat.pipeline(),
                head, cs);
        // audit 10: the target pipeline's OWN materialized slot joins to
        // milestoned tables filter by the temporal context too (every
        // milestoned table alias filters — the dead wall this replaces)
        tPipe = temporal.applyJoinTemporalFilters(tPipe, target, Map.of());
        tPipe = synthetics.applyToPipe(head, tPipe, (p, pred) ->
                CorrelatedSubselects.predFilteredPipe(p, target, tMat.slotPrefixes(),
                        nestedSubNavs, pred, cs.mappingFqn()));
        Map<String, Substitution.SubNav> tailSubNavs =
                new java.util.LinkedHashMap<>();
        for (var tne : tailNavAliases.entrySet()) {
            String pfx3 = tMat.slotPrefixes().get(tne.getValue());
            var stepN3 = java.util.Objects.requireNonNull(tNavSteps3.get(tne.getValue()));
            var stepT3 = stepN3.target();
            if (pfx3 == null || !(stepT3 instanceof TypedGetAll stg3)) {
                continue;
            }
            ClassSource sub3 = sources.navTarget(target, stg3.classFqn(), stepN3, tne.getValue());
            NavMaterializer.NavMat deeper = tailMats.get(tne.getValue());
            Map<String, TypedSpec> subBindings = sub3.bindings();
            if (deeper != null && !deeper.slotPrefixes().isEmpty()) {
                // the sub-target's own slot-backed leaves flatten onto its
                // materialized row (its nav slots ride the SubNav children)
                Map<String, String> slotOnly3 = new java.util.LinkedHashMap<>(
                        deeper.slotPrefixes());
                slotOnly3.keySet().removeAll(deeper.subNavs().keySet());
                Map<String, TypedSpec> rebound = new java.util.LinkedHashMap<>();
                for (var be : sub3.bindings().entrySet()) {
                    TypedSpec bv = be.getValue();
                    if (!slotOnly3.isEmpty()
                            && !(Pipelines.unwrapToOne(bv) instanceof com.legend.compiler.spec.typed.TypedNewInstance)) {
                        bv = Pipelines.rewriteRowReads(bv, sub3.rowVar(),
                                slotOnly3, Set.of(),
                                java.util.function.UnaryOperator.identity());
                    }
                    rebound.put(be.getKey(), bv);
                }
                subBindings = rebound;
            }
            tailSubNavs.put(tne.getKey(), new Substitution.SubNav(
                    pfx3, sub3.rowVar(), subBindings,
                    deeper == null ? Map.of()
                            : NavMaterializer.composeSubNavPrefixes(pfx3, deeper.subNavs())));
        }
        // nested association ends the tails demanded are part of the
        // target's sub-tree too: the read walk descends them like slots
        for (var nse : nestedSubNavs.entrySet()) {
            tailSubNavs.putIfAbsent(nse.getKey(), nse.getValue());
        }
        return new AssocJoin(prefixFor(head, cs), target, tPipe,
                Type.requireRelationSchema(tPipe.info().type()),
                withOuterDatedWindow(temporal, cs, target, chainKey, oriented, tPipe),
                tMat.slotPrefixes(), tailSubNavs, corrSub, null,
                synthetics.isInnerValueHead(head));
    }

    /** The target-side tails of one association join, sorted by kind. */
    record TargetTails(Set<String> tNavDemand, Map<String, String> tailNavAliases,
            Map<String, List<List<String>>> subTailsByAlias,
            Map<String, Set<String>> nestedAssocReads,
            Map<String, Set<List<String>>> nestedAssocTails) {
    }

    /** Sort a join's target-side tails: a tail through the target's own
     * navigate SLOT demands the slot (materialized recursively with the
     * tail past it); a tail through an ASSOCIATION end of the target (no
     * binding) joins the nested widening with its deeper tail. */
    private TargetTails collectTargetTails(TemporalFrame temporal, ClassSource target,
            Set<List<String>> navTails,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> tNavSteps3,
            String chainKey) {
        Set<String> tNavDemand3 = new LinkedHashSet<>();
        Map<String, String> tailNavAliases = new java.util.LinkedHashMap<>();
        Map<String, List<List<String>>> subTailsByAlias = new java.util.LinkedHashMap<>();
        Map<String, Set<String>> nestedAssocReads =
                new java.util.LinkedHashMap<>();
        // the DEEPER tail past a nested association end (F-M, 2026-09-16:
        // $x.book.desk.businessUnit.legalEntity.jurisdiction — 'businessUnit'
        // and 'legalEntity' are association ends, not slots): each rides
        // its own nested join as THAT join's nav tails — the navigate()
        // rule recursing, one owner per level
        Map<String, Set<List<String>>> nestedAssocTails =
                new java.util.LinkedHashMap<>();
        for (List<String> tail : navTails) {
            String seg0 = tail.get(0);
            TypedSpec b3 = target.bindings().get(SyntheticHeads.realHead(seg0));
            String al3 = b3 == null ? null
                    : InnerDemand.navSlotAlias(b3, target.rowVar(),
                            tNavSteps3.keySet());
            var nestedCls = b3 == null && tail.size() >= 2
                    ? assocTargetClassOf(target.classFqn(), SyntheticHeads.realHead(seg0))
                    : java.util.Optional.<String>empty();
            // a TEMPORAL nested target needs a date it can stamp (an explicit
            // hop spec or a propagated context) — the same gate the
            // materializer's one-hop rule applies; otherwise the read stays
            // on its loud wall (corpus testDerivedPropertyOnNonTemporalClass-
            // WithMilestonedChain: 'leaves' to a temporal LeafEntity)
            String nestedChain = chainKey + "." + seg0;   // specs key by the RAW head (identity suffix included)
            boolean nestedStampable = nestedCls.isPresent()
                    && (temporal.temporalStrategy(nestedCls.get()) == null
                        || temporal.spec(nestedChain) != null
                        || !temporal.contextAt(nestedChain, nestedCls.get(),
                                TemporalContext.NONE).isEmpty());
            if (nestedCls.isPresent() && nestedStampable) {
                // an ASSOCIATION end of the target: nested widening below
                nestedAssocReads.computeIfAbsent(seg0, k -> new LinkedHashSet<>())
                        .add(tail.get(1));
                if (tail.size() > 2) {
                    nestedAssocTails.computeIfAbsent(seg0, k -> new LinkedHashSet<>())
                            .add(tail.subList(1, tail.size()));
                }
                continue;
            }
            if (al3 != null) {
                tNavDemand3.add(al3);
                tailNavAliases.put(seg0, al3);
                if (tail.size() > 1) {
                    subTailsByAlias.computeIfAbsent(al3, k -> new java.util.ArrayList<>())
                            .add(tail.subList(1, tail.size()));
                }
            }
        }
        return new TargetTails(tNavDemand3, tailNavAliases, subTailsByAlias,
                nestedAssocReads, nestedAssocTails);
    }

    /** The compiled association predicate's material: the raw condition,
     * the orientation ({@code reverse} = the parent rides the cond's
     * SECOND param), whether the emission is the PROPERTY-SPACE route-A
     * form (String set-id args, class-typed cond params), and — for that
     * form — the pinned TARGET set id. */
    record PredMaterial(TypedLambda cond, boolean reverse,
            boolean propertySpace, @com.legend.base.Nullable String targetSetId) {}

    /** The property that walks a self-association's join AS WRITTEN: the
     *  synthesized predicate's provenance names it (the mapping's first
     *  property mapping — FunctionDefinition.Synthesized.forwardProperty);
     *  the association's own first property is the fallback. */
    private static String forwardPropertyOf(com.legend.compiler.element.TypedFunction predicate,
            com.legend.model.AssociationDefinition assoc) {
        if (predicate.definition() instanceof com.legend.model.FunctionDefinition fd
                && fd.synthesizedFrom() != null && fd.synthesizedFrom().forwardProperty() != null) {
            return fd.synthesizedFrom().forwardProperty();
        }
        return assoc.property1().propertyName();
    }

    /** The association condition's PARENT side: the column-space
     *  condition and which of its two params is the parent's row. Null
     *  when {@code prop} is not a bound association end of {@code cs}'s
     *  class or the condition is property-space (route A substitutes it
     *  through the bindings — no physical key to demand). */
    record ParentSide(TypedLambda cond, int parentParam) {}

    @com.legend.base.Nullable ParentSide associationParentSide(ClassSource cs, String prop) {
        var assoc = ctx.findAssociationOf(cs.classFqn(), prop).orElse(null);
        var targetClass = assoc == null ? java.util.Optional.<String>empty()
                : assocTargetClassOf(cs.classFqn(), prop);
        if (assoc == null || targetClass.isEmpty()
                || associationBindingInClosure(cs.mappingFqn(), assoc.qualifiedName()).isEmpty()) {
            return null;
        }
        PredMaterial pm = predicateMaterial(cs, assoc, prop, targetClass.get());
        return pm.propertySpace() ? null : new ParentSide(pm.cond(), pm.reverse() ? 1 : 0);
    }

    private PredMaterial predicateMaterial(ClassSource cs,
            com.legend.model.AssociationDefinition assoc, String real,
            String targetClass) {
        // The predicate function: the AssociationBinding for the assoc,
        // searched across the INCLUDE CLOSURE (own mapping wins; audit V3:
        // the qualified YZ entries live in an included assoc mapping)
        var binding = associationBindingInClosure(cs.mappingFqn(),
                assoc.qualifiedName())
                .orElseThrow(() -> new MappingResolutionException("association '"
                        + assoc.qualifiedName() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'"
                        // a dropped/poisoned property route often lands here
                        // (the assoc fallback) — surface the recorded reason
                        // (class-keyed, or the per-ASSOCIATION poison)
                        + ctx.mappingPoison(cs.mappingFqn(), cs.classFqn())
                                .or(() -> ctx.mappingAssociationPoison(cs.mappingFqn(),
                                        assoc.qualifiedName()))
                                .map(r -> " (" + r + ")").orElse(""),
                        assoc.qualifiedName()));
        var fns = ctx.findFunction(binding.predicateFunctionFqn());
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: association predicate '"
                    + binding.predicateFunctionFqn() + "' has " + fns.size() + " overloads");
        }
        var cf = specs.compile(fns.get(0));
        TypedSpec last = cf.body().get(cf.body().size() - 1);
        if (!(last instanceof TypedNativeCall call)
                || !call.callee().qualifiedName().equals(com.legend.builtin.Pure.LEGACY_ASSOC_PREDICATE_FQN)
                || call.args().size() != 5
                || !(call.args().get(4) instanceof TypedLambda cond)) {
            throw new IllegalStateException("resolver bug: association predicate body"
                    + " for '" + assoc.qualifiedName() + "' is not the"
                    + " legacyAssocPredicate(a,b,src,tgt,cond) emission: "
                    + last.getClass().getSimpleName());
        }
        // ORIENTATION: the predicate fn's params are (a: classA, b: classB)
        // and the cond's (srcRow, tgtRow) are their tables' rows in that
        // order (H1's emission). The TypedJoin condition binds
        // (leftRow=PARENT, rightRow=TARGET): if the parent is classB the
        // params reverse. Self-associations (parent == target) cannot
        // orient by class — the emission convention puts {target} (the
        // navigated destination when traversing property1) on tgtRow, so
        // property1 keeps the order and property2 reverses (pinned by the
        // executing self-association fixture).
        String classAFqn = ((Type.ClassType)
                fns.get(0).parameters().get(0).type()).fqn();
        // SUBTYPE-aware orientation (extends family): the navigating
        // parent may be a SUBCLASS of the association's declared end
        // class (B extends A navigating AE's 'e' end)
        boolean parentIsA = ctx.isSubtype(cs.classFqn(), classAFqn);
        boolean targetIsA = ctx.isSubtype(targetClass, classAFqn);
        if (!parentIsA && !targetIsA) {
            throw new IllegalStateException("resolver bug: association predicate '"
                    + binding.predicateFunctionFqn() + "' first param class '"
                    + classAFqn + "' is neither parent '" + cs.classFqn()
                    + "' nor target '" + targetClass + "'");
        }
        // a SELF-association: the mapping's forward end walks the join as
        // written, the other end backwards (AssociationBinding.forwardProperty;
        // the association's own property order is the fallback)
        String forward = forwardPropertyOf(fns.get(0), assoc);
        boolean reverse = cs.classFqn().equals(targetClass)
                ? !forward.equals(real)
                : !parentIsA;
        boolean propSpace = call.args().get(2)
                instanceof com.legend.compiler.spec.typed.TypedCString;
        String targetSetId = null;
        if (propSpace) {
            // args 2/3 pin (setA, setB); the TARGET is the B side unless
            // orientation reversed
            String setA = ((com.legend.compiler.spec.typed.TypedCString)
                    call.args().get(2)).value();
            String setB = ((com.legend.compiler.spec.typed.TypedCString)
                    call.args().get(3)).value();
            targetSetId = reverse ? setA : setB;
        }
        return new PredMaterial(cond, reverse, propSpace, targetSetId);
    }

    /**
     * Route A conversion: a PROPERTY-SPACE condition ({@code srcRow},
     * {@code tgtRow} typed as the END CLASSES) becomes the column-space
     * join condition by substituting each side through its set's composed
     * bindings — the target side over the MATERIALIZED target row (slot
     * prefixes honored), the parent side over the parent's row. Set-local
     * reads arrive as {@code legacyLocalProperty} markers and dispatch
     * through the same binding tables (Substitution's marker arm).
     */
    private TypedLambda propertyCondToColumns(TypedLambda cond,
            boolean reverse, ClassSource parent, ClassSource target,
            Map<String, String> targetSlotPrefixes, Type.RelationType tgtRow,
            TemporalFrame temporal, StoreResolver.Context context) {
        String parentParam = cond.parameters().get(reverse ? 1 : 0);
        String targetParam = cond.parameters().get(reverse ? 0 : 1);
        Set<String> taken = new LinkedHashSet<>(cond.parameters());
        for (TypedSpec b : cond.body()) {
            collectVarNames(b, taken);
        }
        String srcVar = freshName("srcRow", taken);
        taken.add(srcVar);
        String tgtVar = freshName("tgtRow", taken);
        Type.RelationType srcRow = parent.rowType();
        Set<String> unconvertedTgt = new LinkedHashSet<>(
                Pipelines.slotAliases(target.pipeline()));
        unconvertedTgt.removeAll(targetSlotPrefixes.keySet());
        TypedSpec body = cond.body().get(cond.body().size() - 1);
        // DERIVED-property calls ($srcRow.productId()) β-inline first —
        // the cond then reads plain or navigation properties
        body = inlineDerivedCondCalls(body, 0);
        // PARENT-side navigation reads ($srcRow.product.productId after
        // inlining) become CORRELATED SCALAR SUBQUERIES over the parent's
        // OWN association join, correlated on the fresh source binder —
        // the engine's qualifier-in-condition emission
        body = parentNavCondReads(body, parentParam, parent, temporal,
                context, srcVar, srcRow);
        Substitution tgtSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(targetParam, tgtVar,
                        target.classFqn(), target.mappingFqn(),
                        target.rowVar(), target.bindings(), tgtRow,
                        unconvertedTgt, targetSlotPrefixes, Map.of()),
                new Substitution.Registries(Map.of(), java.util.Set.of(),
                        Map.of(), Map.of(), null, null),
                Substitution.TemporalView.NONE, true, true));
        body = tgtSub.rewriteLambda(new TypedLambda(List.of(targetParam),
                List.of(body), cond.info())).body().get(0);
        Substitution srcSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(parentParam, srcVar,
                        parent.classFqn(), parent.mappingFqn(),
                        parent.rowVar(), parent.bindings(), srcRow,
                        new LinkedHashSet<>(
                                Pipelines.slotAliases(parent.pipeline())),
                        Map.of(), Map.of()),
                new Substitution.Registries(Map.of(), java.util.Set.of(),
                        Map.of(), Map.of(), null, null),
                Substitution.TemporalView.NONE, true, true));
        body = srcSub.rewriteLambda(new TypedLambda(List.of(parentParam),
                List.of(body), cond.info())).body().get(0);
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var ft = new Type.FunctionType(
                List.of(new Type.Param(srcRow, one),
                        new Type.Param(tgtRow, one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        return new TypedLambda(List.of(srcVar, tgtVar), List.of(body),
                new ExprType(ft, one));
    }

    /** β-inline SYNTHESIZED derived-property calls ({@code $row.productId()})
     * inside a property-space condition — recognized by the callee's
     * {@code synthesizedFrom} PROP hat (exact, never name-parsed); params
     * substitute positionally ({@code $this} ← the receiver). Recursive:
     * a derived body calling another derived inlines too, depth-guarded. */
    private TypedSpec inlineDerivedCondCalls(TypedSpec n, int depth) {
        if (depth > 16) {
            throw new IllegalStateException("resolver bug: derived-property"
                    + " inlining in an XStore condition exceeded depth 16");
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && uc.callee().definition()
                        instanceof com.legend.model.FunctionDefinition fd
                && fd.synthesizedFrom() != null
                && fd.synthesizedFrom().hat() == com.legend.model.SynthHat.PROP) {
            var cf = specs.compile(uc.callee());
            if (cf.body().size() != 1
                    || cf.signature().parameters().size() != uc.args().size()) {
                throw new NotImplementedException("derived property '"
                        + uc.callee().qualifiedName() + "' in an XStore"
                        + " condition has an uninlinable body shape ("
                        + cf.body().size() + " statements, "
                        + cf.signature().parameters().size() + " params for "
                        + uc.args().size() + " args)");
            }
            TypedSpec b = cf.body().get(0);
            for (int i = 0; i < uc.args().size(); i++) {
                b = substCondVar(b,
                        cf.signature().parameters().get(i).name(),
                        uc.args().get(i));
            }
            return inlineDerivedCondCalls(b, depth + 1);
        }
        return SyntheticHeads.rebuildChildren(n,
                c -> inlineDerivedCondCalls(c, depth));
    }

    private static TypedSpec substCondVar(TypedSpec n, String name,
            TypedSpec value) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(name)) {
            return value;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(name)) {
            return l;   // shadowed: substitution stops
        }
        return SyntheticHeads.rebuildChildren(n,
                c -> substCondVar(c, name, value));
    }

    private static TypedSpec unwrapOnes(TypedSpec n) {
        while (n instanceof TypedNativeCall w && w.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::multiplicity::toOneMany")
                    || w.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first"))) {
            n = w.args().get(0);
        }
        return n;
    }

    /** Parent-nav reads in a property-space condition → a [0..1]
     * CORRELATED SCALAR SUBQUERY (the navLeafSubquery discipline,
     * condition position). Two spellings of the same read:
     * {@code $p.<assocProp>.<leaf>} and the auto-map desugar
     * {@code $p.<assocProp>->map(x|$x.<leaf>)->toOne()} (toOne/toOneMany/
     * first wrappers tolerated everywhere). Deeper chains keep their loud
     * substitution wall. */
    private TypedSpec parentNavCondReads(TypedSpec n, String parentParam,
            ClassSource parent, TemporalFrame temporal,
            StoreResolver.Context context, String srcVar,
            Type.RelationType srcRow) {
        if (n instanceof TypedPropertyAccess leaf
                && unwrapOnes(leaf.source()) instanceof TypedPropertyAccess hop
                && unwrapOnes(hop.source())
                        instanceof com.legend.compiler.spec.typed.TypedVariable pv
                && pv.name().equals(parentParam)
                && ctx.findAssociationOf(parent.classFqn(), hop.property())
                        .isPresent()) {
            return parentNavSubquery(parentParam, parent, temporal, context,
                    srcVar, srcRow, hop.property(), leaf.property(), false);
        }
        if (n instanceof TypedNativeCall oneW && oneW.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(oneW.callee().qualifiedName())
                    || oneW.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first"))
                && oneW.args().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedMap tm
                && tm.mapper().parameters().size() == 1
                && tm.mapper().body().size() == 1
                && unwrapOnes(tm.mapper().body().get(0))
                        instanceof TypedPropertyAccess mLeaf
                && mLeaf.source()
                        instanceof com.legend.compiler.spec.typed.TypedVariable mv
                && mv.name().equals(tm.mapper().parameters().get(0))
                && unwrapOnes(tm.source()) instanceof TypedPropertyAccess mHop
                && unwrapOnes(mHop.source())
                        instanceof com.legend.compiler.spec.typed.TypedVariable mpv
                && mpv.name().equals(parentParam)
                && ctx.findAssociationOf(parent.classFqn(), mHop.property())
                        .isPresent()) {
            // the ->toOne() around the map IS the declared semantics —
            // LIMIT 1 is faithful even over a to-many hop
            return parentNavSubquery(parentParam, parent, temporal, context,
                    srcVar, srcRow, mHop.property(), mLeaf.property(), true);
        }
        return SyntheticHeads.rebuildChildren(n, c -> parentNavCondReads(
                c, parentParam, parent, temporal, context, srcVar, srcRow));
    }

    private TypedSpec parentNavSubquery(String parentParam, ClassSource parent,
            TemporalFrame temporal, StoreResolver.Context context,
            String srcVar, Type.RelationType srcRow, String hopProp,
            String leafProp, boolean explicitToOne) {
        // audit 24 F2: LIMIT-1 over a TO-MANY hop would silently compare
        // ONE arbitrary element where pure compares the collection — only
        // an EXPLICIT ->toOne() (the map-arm spelling) declares that
        // semantics; a bare to-many read stays a loud wall
        if (!explicitToOne) {
            var pe = ctx.findAssociationOf(parent.classFqn(), hopProp)
                    .map(a -> a.property1().propertyName().equals(hopProp)
                            ? a.property1() : a.property2())
                    .orElse(null);
            if (pe != null && !pe.isToOne()) {
                throw new NotImplementedException("XStore condition read '$"
                        + parentParam + "." + hopProp + "." + leafProp
                        + "' navigates a TO-MANY end — collection-valued"
                        + " condition reads are not supported (wrap in"
                        + " ->toOne() to declare single-element semantics)");
            }
        }
        AssocJoin aj = associationJoin(temporal, parent, hopProp,
                context, /*forExists*/ true,
                java.util.Set.of(leafProp), hopProp,
                java.util.Set.of());
        TypedSpec leafBind = aj.target().bindings().get(leafProp);
        if (leafBind == null
                || Type.asClassType(leafBind.info().type()) instanceof Type.ClassType) {
            throw new NotImplementedException("XStore condition read '$"
                    + parentParam + "." + hopProp + "."
                    + leafProp + "' has no scalar binding on the"
                    + " hop target '" + aj.target().classFqn() + "'");
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        String pVar = java.util.Objects.requireNonNull(aj.condition()).parameters().get(0);
        String tVar = aj.condition().parameters().get(1);
        List<TypedSpec> corrBody = aj.condition().body().stream()
                .map(cb -> Pipelines.rewriteRowReads(cb, pVar, Map.of(),
                        java.util.Set.of(),
                        v -> new com.legend.compiler.spec.typed
                                .TypedVariable(srcVar,
                                        new ExprType(srcRow, one))))
                .toList();
        var boolFn = new Type.FunctionType(
                List.of(new Type.Param(aj.targetRow(), one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                new ExprType(boolFn, one));
        TypedSpec rel = new com.legend.compiler.spec.typed.TypedFilter(
                aj.targetPipeline(), corr, aj.targetPipeline().info());
        return GraphEmission.scalarLeafSubquery(rel,
                aj.target().rowVar(), aj.targetRow(), leafProp, leafBind);
    }

    /** The correlation pass: two sequential substitutions over the lifted
     * predicate — own param via TARGET bindings onto the condition's
     * target row; each residual FREE variable via the PARENT's bindings
     * onto the condition's source row — then AND into the condition body. */
    /** True when a correlated pred's OUTER reads hop a parent NAVIGATION
     * ($f.address.name — depth >= 2): those can never resolve on the flat
     * join ON's source row; the pred rides the parent-copy subselect. */
    boolean corrPredDemandsParentNav(TypedLambda pred) {
        Set<String> free = new LinkedHashSet<>();
        for (TypedSpec b : pred.body()) {
            collectFreeVars(b, new LinkedHashSet<>(pred.parameters()), free);
        }
        for (String outer : free) {
            Set<List<String>> paths = new LinkedHashSet<>();
            for (TypedSpec b : pred.body()) {
                FlattenOps.consumedPaths(b, outer, paths);
            }
            for (List<String> p : paths) {
                if (p.size() >= 2) {
                    return true;
                }
            }
        }
        return false;
    }

    TypedLambda andCorrelatedIntoCondition(TypedLambda cond,
            TypedLambda pred, ClassSource parent, ClassSource target,
            Map<String, String> targetSlotPrefixes) {
        return andCorrelatedIntoCondition(cond, pred, parent, target,
                targetSlotPrefixes, Map.of(), Map.of());
    }

    TypedLambda andCorrelatedIntoCondition(TypedLambda cond,
            TypedLambda pred, ClassSource parent, ClassSource target,
            Map<String, String> targetSlotPrefixes,
            Map<String, Substitution.AssocSub> parentAssocs,
            Map<String, Substitution.SubNav> targetSubNavs) {
        // F2 (audit 21b): the condition's binders are emission-literal
        // names ({s,t} on the navigate route, {srcRow,tgtRow} on the
        // association route). A user variable sharing a name would be
        // CAPTURED — deleted from the free set and lowered as a
        // condition-row column read ("Table t1 has no column named
        // date"). Alpha-freshen both binders collision-driven (audit
        // 18's tRenamed discipline), and take the free set from the
        // ORIGINAL pred: after pass 1, introduced target-param reads
        // and same-named user vars are indistinguishable.
        Set<String> taken = new LinkedHashSet<>(pred.parameters());
        for (TypedSpec b : pred.body()) {
            collectVarNames(b, taken);
        }
        for (TypedSpec b : cond.body()) {
            collectVarNames(b, taken);
        }
        cond = freshenBinders(cond, taken);
        String srcParam = cond.parameters().get(0);
        String tgtParam = cond.parameters().get(1);
        Set<String> free = new LinkedHashSet<>();
        for (TypedSpec b : pred.body()) {
            collectFreeVars(b, new LinkedHashSet<>(pred.parameters()), free);
        }
        Set<String> unconvertedTgt = new LinkedHashSet<>(
                Pipelines.slotAliases(target.pipeline()));
        unconvertedTgt.removeAll(targetSlotPrefixes.keySet());
        var ft = cond.functionType();
        // the assoc-route cond declares concrete relation params; the
        // navigate-step emission is GENERIC (TypeVars) — the actual
        // pipelines carry the row shapes either way
        Type.RelationType srcRow = rowOr(ft.params().get(0).type(),
                parent.rowType());
        Type.RelationType tgtRow = rowOr(ft.params().get(1).type(),
                target.rowType());
        // pass 1: the pred's own param -> target bindings over tgtParam.
        // #69: the pred's TARGET-side reads may navigate the target's OWN
        // class-typed heads ($e.address.name over the navigated rows) —
        // the target's materialized SubNav tree dispatches them, with the
        // read landing prefixed on the condition's target row.
        Map<String, Substitution.AssocSub> tgtAssocs = new java.util.LinkedHashMap<>();
        for (var sne : targetSubNavs.entrySet()) {
            var sn = sne.getValue();
            tgtAssocs.put(sne.getKey(), new Substitution.AssocSub(
                    sn.prefix(), sn.rowVar(), sn.bindings(),
                    target.classFqn() + "." + sne.getKey(),
                    java.util.Set.of(), Map.of(), tgtParam, tgtRow,
                    Map.of(), sn.children()));
        }
        Substitution tgtSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(pred.parameters().get(0), tgtParam,
                        target.classFqn(), target.mappingFqn(),
                        target.rowVar(), target.bindings(), tgtRow,
                        unconvertedTgt, targetSlotPrefixes, Map.of()),
                new Substitution.Registries(tgtAssocs, java.util.Set.of(),
                        Map.of(), Map.of(), null, null),
                Substitution.TemporalView.NONE,
                true, true));
        TypedLambda pass1 = tgtSub.rewriteLambda(pred);
        // pass 2: each residual free variable (collected pre-pass-1,
        // shadow-aware) -> parent bindings over srcParam
        TypedSpec body = pass1.body().get(pass1.body().size() - 1);
        for (String outer : free) {
            // #69: OUTER reads may navigate the PARENT's class-typed
            // heads — the parent's registered AssocSubs dispatch them
            // (Registries.NONE walled every such read).
            Substitution srcSub = new Substitution(new Substitution.Target(
                    new Substitution.RowScope(outer, srcParam,
                            parent.classFqn(), parent.mappingFqn(),
                            parent.rowVar(), parent.bindings(),
                            srcRow,
                            new LinkedHashSet<>(
                                    Pipelines.slotAliases(parent.pipeline())),
                            Map.of(), Map.of()),
                    new Substitution.Registries(parentAssocs, java.util.Set.of(),
                            Map.of(), Map.of(), null, null),
                    Substitution.TemporalView.NONE, true, true));
            body = srcSub.rewriteLambda(new com.legend.compiler.spec.typed
                    .TypedLambda(List.of(outer), List.of(body), pred.info()))
                    .body().get(0);
        }
        var andFns = ctx.findFunction("meta::pure::functions::boolean::and")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (andFns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected one 2-arg"
                    + " boolean::and, found " + andFns.size());
        }
        TypedSpec existing = cond.body().get(cond.body().size() - 1);
        TypedSpec anded = new com.legend.compiler.spec.typed.TypedNativeCall(
                andFns.get(0), List.of(existing, body), existing.info());
        return new TypedLambda(cond.parameters(), List.of(anded), cond.info());
    }

    /**
     * The correlated predicate rewritten onto the SINGLE joined row of the
     * aggregated subselect (the engine's parent-copy architecture, #69):
     * the pred's own param reads TARGET bindings prefixed with
     * {@code targetPrefix}; each free OUTER variable reads the
     * PARENT-COPY's bindings (parent columns unprefixed on the joined
     * row, the copy's own demanded nav columns via its slot prefixes and
     * depth-1 SubNavs). Same alpha/capture discipline as the ON-clause
     * composition (audit 22a).
     */
    TypedLambda corrPredOnJoinedRow(@com.legend.base.Nullable TypedLambda pred, ClassSource parent,
            ClassSource target, String targetPrefix,
            Map<String, String> targetSlotPrefixes,
            Map<String, Substitution.SubNav> targetSubNavs,
            Map<String, String> parentCopySlotPrefixes,
            Map<String, Substitution.SubNav> parentCopySubNavs,
            String rowVar, Type.RelationType rowType) {
        java.util.Objects.requireNonNull(pred,
                "corrPredOnJoinedRow requires a predicate");
        return corrPredOnJoinedRowCore(pred, parent, target, targetPrefix,
                targetSlotPrefixes, targetSubNavs, parentCopySlotPrefixes,
                parentCopySubNavs, rowVar, rowType,
                target.classFqn(), target.rowVar(), target.bindings(),
                targetPrefix);
    }

    /** {@link #corrPredOnJoinedRow} for a pred whose PARAM binds a target
     * SUB-NAVIGATION row (#69 tail filter, firm#f0.address#f1): the
     * param's reads land under {@code targetPrefix + the sub-nav's own
     * prefix}; OUTER reads ride the parent-copy channel unchanged. */
    TypedLambda corrPredOnJoinedRowForSubNav(TypedLambda pred,
            ClassSource parent, ClassSource target, String targetPrefix,
            Substitution.SubNav sn,
            Map<String, String> parentCopySlotPrefixes,
            Map<String, Substitution.SubNav> parentCopySubNavs,
            String rowVar, Type.RelationType rowType) {
        String paramClass = Type.asClassType(pred.functionType().params().get(0).type())
                        instanceof Type.ClassType pc
                ? pc.fqn() : target.classFqn();
        return corrPredOnJoinedRowCore(pred, parent, target, targetPrefix,
                Map.of(), sn.children(), parentCopySlotPrefixes,
                parentCopySubNavs, rowVar, rowType,
                paramClass, sn.rowVar(), sn.bindings(),
                targetPrefix + sn.prefix());
    }

    private TypedLambda corrPredOnJoinedRowCore(TypedLambda pred,
            ClassSource parent,
            ClassSource target, String targetPrefix,
            Map<String, String> targetSlotPrefixes,
            Map<String, Substitution.SubNav> targetSubNavs,
            Map<String, String> parentCopySlotPrefixes,
            Map<String, Substitution.SubNav> parentCopySubNavs,
            String rowVar, Type.RelationType rowType,
            String paramClassFqn, String paramRowVar,
            Map<String, TypedSpec> paramBindings, String landPrefix) {
        Set<String> taken = new LinkedHashSet<>(pred.parameters());
        for (TypedSpec b : pred.body()) {
            collectVarNames(b, taken);
        }
        taken.add(rowVar);
        String ct = freshName("_ct", taken);
        String csv = freshName("_cs", taken);
        Set<String> free = new LinkedHashSet<>();
        for (TypedSpec b : pred.body()) {
            collectFreeVars(b, new LinkedHashSet<>(pred.parameters()), free);
        }
        var rowInfo = new ExprType(rowType,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        Set<String> unconvertedTgt = new LinkedHashSet<>(
                Pipelines.slotAliases(target.pipeline()));
        unconvertedTgt.removeAll(targetSlotPrefixes.keySet());
        // the param's NESTED-navigation reads land DIRECTLY on the joined
        // row: the sub-nav tree's prefixes compose absolute to the target
        // row (NavMaterializer.composeSubNavPrefixes), so the landing is
        // targetPrefix + that prefix — registered on the joined row var,
        // never re-prefixed by the param landing below (batch 106: routed
        // through the param var, `$c.coveredProduct.name` under a sub-nav
        // param composed the sub-nav's prefix twice)
        Map<String, Substitution.AssocSub> tgtAssocs = new java.util.LinkedHashMap<>();
        for (var e : targetSubNavs.entrySet()) {
            var sn = e.getValue();
            tgtAssocs.put(e.getKey(), new Substitution.AssocSub(
                    targetPrefix + sn.prefix(), sn.rowVar(), sn.bindings(),
                    target.classFqn() + "." + e.getKey(),
                    java.util.Set.of(), Map.of(), rowVar, rowType,
                    Map.of(), NavMaterializer.composeSubNavPrefixes(
                            targetPrefix, sn.children())));
        }
        Substitution tgtSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(pred.parameters().get(0), ct,
                        paramClassFqn, target.mappingFqn(),
                        paramRowVar, paramBindings, rowType,
                        unconvertedTgt, targetSlotPrefixes, Map.of()),
                new Substitution.Registries(tgtAssocs, java.util.Set.of(),
                        Map.of(), Map.of(), null, null),
                Substitution.TemporalView.NONE,
                true, true));
        TypedLambda pass1 = tgtSub.rewriteLambda(pred);
        TypedSpec body = pass1.body().get(pass1.body().size() - 1);
        Map<String, Substitution.AssocSub> copyAssocs = new java.util.LinkedHashMap<>();
        for (var e : parentCopySubNavs.entrySet()) {
            var sn = e.getValue();
            copyAssocs.put(e.getKey(), new Substitution.AssocSub(
                    sn.prefix(), sn.rowVar(), sn.bindings(),
                    parent.classFqn() + "." + e.getKey(),
                    java.util.Set.of(), Map.of(), csv, rowType,
                    Map.of(), sn.children()));
        }
        Set<String> unconvertedPar = new LinkedHashSet<>(
                Pipelines.slotAliases(parent.pipeline()));
        unconvertedPar.removeAll(parentCopySlotPrefixes.keySet());
        for (String outer : free) {
            Substitution srcSub = new Substitution(new Substitution.Target(
                    new Substitution.RowScope(outer, csv,
                            parent.classFqn(), parent.mappingFqn(),
                            parent.rowVar(), parent.bindings(), rowType,
                            unconvertedPar, parentCopySlotPrefixes, Map.of()),
                    new Substitution.Registries(copyAssocs, java.util.Set.of(),
                            Map.of(), Map.of(), null, null),
                    Substitution.TemporalView.NONE, true, true));
            body = srcSub.rewriteLambda(new com.legend.compiler.spec.typed
                    .TypedLambda(List.of(outer), List.of(body), pred.info()))
                    .body().get(0);
        }
        // land both sides on the ONE joined row: target reads prefix,
        // parent-copy reads rename (parent columns ride unprefixed)
        body = Pipelines.prefixColumns(body, ct, landPrefix,
                v -> new com.legend.compiler.spec.typed.TypedVariable(
                        rowVar, rowInfo));
        body = Pipelines.rewriteRowReads(body, csv, Map.of(), java.util.Set.of(),
                v -> new com.legend.compiler.spec.typed.TypedVariable(
                        rowVar, rowInfo));
        // the result kind is the INPUT lambda's (a Boolean pred, or a
        // computed mapper's scalar — this rewriter serves both)
        Type.Param res = pred.functionType().result();
        return new com.legend.compiler.spec.typed.TypedLambda(
                List.of(rowVar), List.of(body),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(rowType,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                                res),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
    }


    /** The target pipe widened with one LEFT join per nested-association
     * read (prefix {@code <prop>_}), the prefixes, and a SubNav per read
     * for the synthetic predicate's substitution. */
    private record NestedWidening(TypedSpec pipe,
            Map<String, String> prefixByProp,
            Map<String, Substitution.SubNav> subNavs) {
    }

    /** NESTED-ASSOCIATION widening (the navigate() rule): a condition or a
     * synthetic predicate reading {@code $tgt.<assocProp>.<col>} joins the
     * nested association's target into the materialized pipe, prefixed —
     * the recursive navigate, exactly the chain-walk precedent (task #78). */
    private NestedWidening widenNestedAssocs(TemporalFrame temporal,
            ClassSource target, StoreResolver.Context context,
            Map<String, Set<String>> nestedAssocReads,
            Map<String, Set<List<String>>> nestedAssocTails, TypedSpec basePipe,
            String chainKey) {
        Map<String, String> prefixByProp = new java.util.LinkedHashMap<>();
        Map<String, Substitution.SubNav> subNavs = new java.util.LinkedHashMap<>();
        for (var ne : nestedAssocReads.entrySet()) {
            AssocJoin aj2 = aggJoinMaterial(temporal, target, ne.getKey(),
                    context, ne.getValue(),
                    nestedAssocTails.getOrDefault(ne.getKey(), java.util.Set.of()),
                    chainKey + "." + ne.getKey());
            String pfx = ne.getKey() + "_";
            // the nested join's own sub-tree (deeper tails) composes under
            // this end's prefix — relative to THIS target's row
            subNavs.put(ne.getKey(), new Substitution.SubNav(pfx,
                    aj2.target().rowVar(), aj2.target().bindings(),
                    NavMaterializer.composeSubNavPrefixes(pfx, aj2.targetSubNavs())));
            Type.RelationType curRow = Type.requireRelationSchema(basePipe.info().type());
            java.util.List<Type.Column> wcols =
                    new java.util.ArrayList<>(curRow.columns());
            for (Type.Column c : aj2.targetRow().columns()) {
                wcols.add(new Type.Column(pfx + c.name(),
                        c.type(), c.multiplicity()));
            }
            basePipe = new com.legend.compiler.spec.typed.TypedJoin(
                    basePipe, aj2.targetPipeline(),
                    leftKind(), java.util.Objects.requireNonNull(aj2.condition()),
                    java.util.Optional.of(pfx), null,
                    new ExprType(Type.relation(new Type.RelationType(wcols)),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE),
                false /* resolver-synth */);
            prefixByProp.put(ne.getKey(), pfx);
        }
        return new NestedWidening(basePipe, prefixByProp, subNavs);
    }

    /** The association CONDITION's target-side reads: slot/navigate
     * demand plus NESTED-association reads (the navigate() rule) —
     * returns the condition's target param name (null when the
     * predicate shape is unrecognized). Extracted seam (guardrail). */
    private @com.legend.base.Nullable String scanCondTargetReads(ClassSource cs,
            com.legend.model.AssociationDefinition assoc, String real,
            String targetClass, ClassSource target, Set<String> targetSlots,
            Set<String> targetDemand, Set<String> navStepKeys,
            Set<String> tNavDemand3,
            Map<String, Set<String>> nestedAssocReads) {
        String result = null;
        var bindE = associationBindingInClosure(cs.mappingFqn(),
                assoc.qualifiedName()).orElse(null);
        if (bindE != null) {
            var fnsE = ctx.findFunction(bindE.predicateFunctionFqn());
            var cfE = fnsE.size() == 1 ? specs.compile(fnsE.get(0)) : null;
            TypedSpec lastE = cfE == null ? null
                    : cfE.body().get(cfE.body().size() - 1);
            if (lastE instanceof TypedNativeCall callE
                    && callE.args().size() == 5
                    && callE.args().get(4) instanceof TypedLambda condE
                    && condE.parameters().size() == 2) {
                String classAFqnE = ((Type.ClassType)
                        fnsE.get(0).parameters().get(0).type()).fqn();
                boolean parentIsAE = ctx.isSubtype(cs.classFqn(), classAFqnE);
                String forwardE = forwardPropertyOf(fnsE.get(0), assoc);
                boolean reverseE = cs.classFqn().equals(targetClass)
                        ? !forwardE.equals(real)
                        : !parentIsAE;
                String tgtVarE = condE.parameters().get(reverseE ? 0 : 1);
                result = tgtVarE;
                boolean propSpaceE = callE.args().get(2)
                        instanceof com.legend.compiler.spec.typed.TypedCString;
                for (TypedSpec b : condE.body()) {
                    // JOINSLOT reads (the ColSpec-join emission) and
                    // navigate-step reads both count
                    CorrelatedSubselects.collectAliasReads(b, tgtVarE,
                            targetSlots, targetDemand);
                    CorrelatedSubselects.collectAliasReads(b, tgtVarE,
                            navStepKeys, tNavDemand3);
                    // PROPERTY-SPACE cond (route A): a `$tgt.prop` read
                    // demands whatever the target's BINDING for prop reads
                    // — a +prop mapped through a join chain reads a slot
                    // (batch 110, testCrossMappingWithRelOpWithJoinKeys:
                    // `+ceoId: @employee_ceo | ceo.identifier`)
                    if (propSpaceE) {
                        for (TypedSpec bnd : propertyBindingsRead(b, tgtVarE,
                                target)) {
                            CorrelatedSubselects.collectAliasReads(bnd,
                                    target.rowVar(), targetSlots, targetDemand);
                            CorrelatedSubselects.collectAliasReads(bnd,
                                    target.rowVar(), navStepKeys, tNavDemand3);
                        }
                    }
                    // NESTED-ASSOCIATION reads ($tgt.address.city where
                    // address is an association of the target class):
                    // the navigate() rule — the nested association joins
                    // into the target after materialization (below)
                    collectNestedAssocReads(b, tgtVarE, targetClass,
                            nestedAssocReads);
                }
            }
        }
        return result;
    }

    /** The target's BINDINGS behind every {@code $var.prop} read in
     * {@code n} (property-space conditions): the demand a property read
     * implies is whatever its mapping reads. */
    private static List<TypedSpec> propertyBindingsRead(TypedSpec n, String var,
            ClassSource target) {
        List<TypedSpec> out = new ArrayList<>();
        collectPropertyBindings(n, var, target, out);
        return out;
    }

    private static void collectPropertyBindings(TypedSpec n, String var,
            ClassSource target, List<TypedSpec> out) {
        String prop = null;
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(var)) {
            prop = pa.property();
        } else if (n instanceof TypedNativeCall lpc
                && lpc.callee().qualifiedName().equals(
                        com.legend.builtin.Pure.LEGACY_LOCAL_PROPERTY_FQN)
                && lpc.args().size() == 2
                && lpc.args().get(0) instanceof com.legend.compiler.spec.typed.TypedVariable lv
                && lv.name().equals(var)
                && lpc.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCString ls) {
            // the route-A spelling of a set-LOCAL (+prop) read
            prop = ls.value();
        }
        if (prop != null) {
            TypedSpec bnd = target.bindings().get(prop);
            if (bnd != null) {
                out.add(bnd);
            }
            return;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(var)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            collectPropertyBindings(c, var, target, out);
        }
    }

    /** SOURCE-SIDE nested reads (the navigate() rule, parent side): the
     * condition reads {@code $s.<assocProp>.<col>} where assocProp is an
     * association of the PARENT class — register the parent's own assoc
     * join FIRST (its prefixed columns land on the accumulated root row,
     * exactly like a query-demanded navigation) and re-point the
     * condition reads at them. Root-level (hop 0) only. */
    AssocJoin withSourceNestedAssocs(TemporalFrame temporal, ClassSource cs,
            AssocJoin aj, StoreResolver.Context context,
            java.util.List<AssocJoin> assocJoins,
            Map<String, AssocJoin> joinsByChain,
            Map<String, Substitution.AssocSub> assocs) {
        TypedLambda cond = aj.condition();
        if (cond == null || cond.parameters().size() != 2) {
            return aj;
        }
        String srcVar = cond.parameters().get(0);
        Map<String, Set<String>> reads = new java.util.LinkedHashMap<>();
        for (TypedSpec b : cond.body()) {
            collectNestedAssocReads(b, srcVar, cs.classFqn(), reads);
        }
        if (reads.isEmpty()) {
            return aj;
        }
        Map<String, String> prefixByProp = new java.util.LinkedHashMap<>();
        java.util.List<Type.Column> lookupCols = new java.util.ArrayList<>();
        for (var re : reads.entrySet()) {
            String prop = re.getKey();
            AssocJoin aj0 = joinsByChain.get(prop);
            if (aj0 == null) {
                aj0 = associationJoin(temporal, cs, prop, context, false,
                        re.getValue(), prop);
                assocJoins.add(aj0);
                joinsByChain.put(prop, aj0);
                assocs.put(prop, new Substitution.AssocSub(aj0.prefix(),
                        aj0.target().rowVar(), aj0.target().bindings(),
                        aj0.target().classFqn(),
                        Pipelines.slotAliases(aj0.target().pipeline()),
                        aj0.targetSlotPrefixes(), null, null,
                        temporal.milestoneColumnsOf(
                                aj0.target().pipeline(),
                                aj0.target().classFqn())));
            }
            prefixByProp.put(prop, aj0.prefix());
            for (Type.Column c : aj0.targetRow().columns()) {
                lookupCols.add(new Type.Column(aj0.prefix() + c.name(),
                        c.type(), c.multiplicity()));
            }
        }
        TypedLambda cond2 = rewriteNestedAssocCondReads(cond, srcVar,
                prefixByProp, new Type.RelationType(lookupCols));
        return new AssocJoin(aj.prefix(), aj.target(), aj.targetPipeline(),
                aj.targetRow(), cond2, aj.targetSlotPrefixes(),
                aj.targetSubNavs(), aj.corrSubPred(), null, aj.rowDropping());
    }

    /** Nested-association reads in an association condition:
     * {@code $tgt.<assocProp>.<col>} where assocProp is an association
     * of the target class (the navigate() rule — task #78). */
    private void collectNestedAssocReads(TypedSpec n, String tgtVar,
            String targetClass, Map<String, Set<String>> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedPropertyAccess mid
                && mid.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(tgtVar)
                && ctx.findAssociationOf(targetClass, mid.property())
                        .isPresent()) {
            out.computeIfAbsent(mid.property(),
                    k -> new LinkedHashSet<>()).add(pa.property());
            return;
        }
        for (TypedSpec c : n.children()) {
            collectNestedAssocReads(c, tgtVar, targetClass, out);
        }
    }

    /** The condition with nested-association reads re-pointed at the
     * WIDENED target row's prefixed columns. */
    static TypedLambda rewriteNestedAssocCondReads(TypedLambda cond,
            @com.legend.base.Nullable String tgtVar, Map<String, String> prefixByProp,
            Type.RelationType row) {
        if (tgtVar == null || prefixByProp.isEmpty()) {
            return cond;
        }
        java.util.List<TypedSpec> nb =
                new java.util.ArrayList<>(cond.body().size());
        for (TypedSpec b : cond.body()) {
            nb.add(nestedCondRead(b, tgtVar, prefixByProp, row));
        }
        return new TypedLambda(cond.parameters(), nb, cond.info());
    }

    private static TypedSpec nestedCondRead(TypedSpec n, String tgtVar,
            Map<String, String> pfx, Type.RelationType row) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedPropertyAccess mid
                && mid.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(tgtVar)
                && pfx.containsKey(mid.property())) {
            String col = pfx.get(mid.property()) + pa.property();
            Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equals(col)).findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "resolver bug: nested-association read column '"
                                    + col + "' missing from the widened"
                                    + " target row"));
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    new com.legend.compiler.spec.typed.TypedVariable(tgtVar,
                            new ExprType(row,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE)),
                    col, new ExprType(c.type(), c.multiplicity()));
        }
        if (n instanceof TypedNativeCall c2) {
            java.util.List<TypedSpec> args =
                    new java.util.ArrayList<>(c2.args().size());
            for (TypedSpec a : c2.args()) {
                args.add(nestedCondRead(a, tgtVar, pfx, row));
            }
            return c2.withChildren(args);
        }
        return n;
    }

    /** {@code λ(s,t). AND_k s.k == t.k} — the parent-key join-back of the
     * correlated aggregated subselect (key names preserved on both sides). */
    TypedLambda pkEqualityCond(List<String> keys,
            Type.RelationType srcRow, Type.RelationType subRow) {
        return pkEqualityCond(keys, keys, srcRow, subRow);
    }

    /** As above with the sub-side key columns RENAMED ({@code subKeys}
     * aligned with {@code keys}) — the exploding subselect projects parent
     * keys under collision-proof aliases. */
    TypedLambda pkEqualityCond(List<String> keys, List<String> subKeys,
            Type.RelationType srcRow, Type.RelationType subRow) {
        return pkEqualityCond(keys, subKeys, srcRow, subRow, false);
    }

    /** {@code orCombine}: UNION-split key pairs (ID_0/ID_1 — each NULL
     * off-member) join back on OR of same-name equalities; NULL=NULL is
     * not true in SQL, so exactly the member's own pair matches (the
     * engine's unionBase join-back — task #27 U4). */
    TypedLambda pkEqualityCond(List<String> keys, List<String> subKeys,
            Type.RelationType srcRow, Type.RelationType subRow,
            boolean orCombine) {
        var eqFns = ctx.findFunction("meta::pure::functions::boolean::equal")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        var andFns = ctx.findFunction("meta::pure::functions::boolean::"
                + (orCombine ? "or" : "and"))
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (eqFns.size() != 1 || andFns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected one 2-arg"
                    + " boolean::equal and boolean::and/or");
        }
        var boolOne = new ExprType(Type.Primitive.BOOLEAN,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec acc = null;
        for (int ki = 0; ki < keys.size(); ki++) {
            String k = keys.get(ki);
            String sk = subKeys.get(ki);
            var sCol = srcRow.columns().stream()
                    .filter(c -> c.name().equals(k)).findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "resolver bug: parent key '" + k
                            + "' missing from the source row"));
            TypedSpec eq = new TypedNativeCall(eqFns.get(0), List.of(
                    new com.legend.compiler.spec.typed.TypedPropertyAccess(
                            new com.legend.compiler.spec.typed.TypedVariable("s",
                                    new ExprType(srcRow,
                                            com.legend.compiler.element.type
                                                    .Multiplicity.Bounded.ONE)),
                            k, new ExprType(sCol.type(), sCol.multiplicity())),
                    new com.legend.compiler.spec.typed.TypedPropertyAccess(
                            new com.legend.compiler.spec.typed.TypedVariable("t",
                                    new ExprType(subRow,
                                            com.legend.compiler.element.type
                                                    .Multiplicity.Bounded.ONE)),
                            sk, new ExprType(sCol.type(), sCol.multiplicity()))),
                    boolOne);
            acc = acc == null ? eq
                    : new TypedNativeCall(andFns.get(0), List.of(acc, eq), boolOne);
        }
        return new com.legend.compiler.spec.typed.TypedLambda(
                List.of("s", "t"), List.of(acc),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(srcRow,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE),
                                        new Type.Param(subRow,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                                new Type.Param(Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ONE));
    }

    private static String freshName(String base, Set<String> taken) {
        String n = base;
        int k = 2;
        while (taken.contains(n)) {
            n = base + k++;
        }
        taken.add(n);
        return n;
    }

    private static void collectVarNames(
            com.legend.compiler.spec.typed.TypedSpec n, Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            out.add(v.name());
        }
        n.children().forEach(c -> collectVarNames(c, out));
    }

    /** Shadow-AWARE free-variable reads: a name is bound only within its
     * binder's subtree — never by global name collision (the F2/F4
     * capture family). Enters lambdas through their bodies with the
     * params added to {@code bound}. */
    private static void collectFreeVars(TypedSpec n, Set<String> bound,
            Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            if (!bound.contains(v.name())) {
                out.add(v.name());
            }
            return;
        }
        if (n instanceof TypedLambda l) {
            Set<String> b2 = new LinkedHashSet<>(bound);
            b2.addAll(l.parameters());
            for (TypedSpec b : l.body()) {
                collectFreeVars(b, b2, out);
            }
            return;
        }
        for (TypedSpec c : n.children()) {
            collectFreeVars(c, bound, out);
        }
    }

    /** Alpha-rename any binder of {@code cond} colliding with a name in
     * {@code taken}, rewriting its reads through the ONE shadow-aware
     * row-read rewriter. The composed condition then cannot capture a
     * user variable by construction. */
    private static TypedLambda freshenBinders(TypedLambda cond,
            Set<String> taken) {
        List<String> params = new ArrayList<>(cond.parameters());
        List<TypedSpec> body = new ArrayList<>(cond.body());
        boolean changed = false;
        for (int i = 0; i < params.size(); i++) {
            String p0 = params.get(i);
            if (!taken.contains(p0)) {
                continue;
            }
            String p1 = p0;
            int k = 2;
            while (taken.contains(p1) || params.contains(p1)) {
                p1 = p0 + "_c" + k++;
            }
            final String to = p1;
            for (int j = 0; j < body.size(); j++) {
                body.set(j, Pipelines.rewriteRowReads(body.get(j), p0,
                        Map.of(), Set.of(), v -> new com.legend.compiler.spec
                                .typed.TypedVariable(to, v.info())));
            }
            params.set(i, p1);
            taken.add(p1);
            changed = true;
        }
        return changed ? new TypedLambda(params, body, cond.info()) : cond;
    }

    private static Type.RelationType rowOr(Type t,
            Type.RelationType fallback) {
        Type.RelationType rt = Type.schemaView(t);
        return rt != null ? rt : fallback;
    }
}
