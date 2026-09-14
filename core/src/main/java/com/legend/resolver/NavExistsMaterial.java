package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The NAVIGATE-SLOT exists arm (class-typed Join PM head): the nav step
 * carries the target extent + oriented (s, t) predicate; the material
 * registers under the head — or, for an EMBEDDED-union head
 * ({@code $f.bridge.employees->exists}), under the DOTTED consumer path
 * with the union-level LIFT as the serving navigate. Extracted from
 * {@link StoreResolver#registerExistsSubs} whole (2026-08-31
 * embedded-union burn); the scope hook keeps the resolver's private
 * nested-scope construction on its own side.
 */
final class NavExistsMaterial {

    private NavExistsMaterial() {
    }

    /** The resolver's nested-scope construction, passed in. */
    interface ScopeFn {
        StoreResolver.NestedScope scope(ClassSource t, String key,
                TypedSpec pipe);
    }

    /** Registers the head's exists material into {@code existsSubs};
     * no-ops (leaving the head loud downstream) when the navigate step
     * or its target is not serveable here. Caller owns the
     * bindings-containsKey dispatch. */
    static void register(Map<String, Substitution.ExistsSub> existsSubs,
            ModelContext ctx, ClassSources sources,
            SyntheticHeads synthetics, TemporalFrame temporal,
            AssociationJoins assocMaterial, CorrelatedSubselects corrSubs,
            ClassSource cs, String head, List<String> path,
            boolean filterTwoHop, List<TypedSpec> ops,
            Map<String, Substitution.AssocSub> parentAssocs,
            ScopeFn scopeFn) {
        String navAlias = SyntheticHeads.realHead(head);
        String registerKey = head;
        TypedSpec hb1 = cs.bindings().get(navAlias);
        if (hb1 instanceof TypedNativeCall toc
                && toc.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(
                        toc.callee().qualifiedName())) {
            hb1 = toc.args().get(0);
        }
        if (filterTwoHop
                // UNION frames only: the leaf's serving navigate is the
                // union-level LIFT. A non-union embedded exists path
                // keeps its per-leaf otherwise/partial route
                // (innerjoin-isolation regression receipt — registering
                // here walled compositeChainTarget)
                && Pipelines.containsConcatenate(cs.pipeline())
                && Pipelines.navSteps(cs.pipeline()).get(navAlias) == null
                && hb1 instanceof TypedNewInstance ctor0
                && ctor0.properties().get(SyntheticHeads
                        .realHead(path.get(1)))
                        instanceof TypedPropertyAccess cf
                && cf.source() instanceof TypedVariable cfv
                && cfv.name().equals(cs.rowVar())
                && Pipelines.outerNavSteps(cs.pipeline())
                        .containsKey(cf.property())) {
            // EMBEDDED head (embedded-union nav burn): the ctor's LEAF
            // field is the nav read — the exists material registers
            // under the DOTTED consumer path, serving
            // $f.bridge.employees->exists(...) like a direct nav.
            navAlias = cf.property();
            registerKey = String.join(".", path);
        }
        // dotted (embedded) keys address the UNION FRAME: only the
        // lifted navigate above the concatenate is meaningful from the
        // union row — the deep last-wins map would hand back a MEMBER
        // THREAD's same-named navigate whose raw member-row reads are
        // unresolvable outside its thread (the §5 '_r0.ID' producer,
        // proven by creation-site tag)
        var nav = (registerKey.equals(head)
                ? Pipelines.navSteps(cs.pipeline())
                : Pipelines.outerNavSteps(cs.pipeline())).get(navAlias);
        if (nav == null || !(nav.target()
                instanceof TypedGetAll tg)
                // eager material only when the target class IS mapped
                // here (M2M nav targets live upstream — must not throw
                // for a rewrite that may never fire)
                || !sources.binds(cs.mappingFqn(), tg.classFqn())) {
            return;
        }
        ClassSource t = sources.navTarget(cs, tg.classFqn(), nav, registerKey);
        Set<String> tSlots0 = Pipelines.slotAliases(t.pipeline());
        Set<String> tDemand0 = new LinkedHashSet<>();
        Set<String> innerLeaves = new LinkedHashSet<>();
        if (registerKey.equals(head)) {
            innerLeaves.addAll(InnerDemand.leaves(ops, head));
        } else {
            // DOTTED (embedded) key: the emptiness lambdas hang off the
            // full consumer path — leaves(ops, head) only speaks
            // single-segment heads
            for (TypedLambda lam : InnerDemand.lambdas(ops, path)) {
                if (!lam.parameters().isEmpty()) {
                    InnerDemand.collectParamPathHeads(lam,
                            lam.parameters().get(0), innerLeaves);
                }
            }
        }
        for (TypedLambda liftedPred0 : synthetics.allPreds(registerKey)) {
            for (TypedSpec b : liftedPred0.body()) {
                InnerDemand.collectParamPathHeads(b,
                        liftedPred0.parameters().get(0), innerLeaves);
            }
        }
        for (String leaf : innerLeaves) {
            TypedSpec lb = t.bindings().get(leaf);
            if (lb != null) {
                CorrelatedSubselects.collectAliasReads(lb, t.rowVar(),
                        tSlots0, tDemand0);
            }
        }
        tDemand0 = Pipelines.closeOverConditions(t.pipeline(), tDemand0);
        // #69: a CORRELATED pred's TARGET-side reads may hop the
        // target's OWN class-typed navigate steps ($e.address.name over
        // the navigated rows) — demand those steps, let the
        // materialization join them, and build depth-1 SubNavs for the
        // composition's pass-1 dispatch. Deeper hops stay loud (empty
        // children).
        TypedLambda corrNav0 = synthetics.correlatedPred(registerKey);
        Map<String, String> predNavAliases = new LinkedHashMap<>();
        Set<String> tNavDemand = InnerDemand.navStepDemand(t,
                Pipelines.navSteps(t.pipeline()).keySet(), corrNav0,
                synthetics.allPreds(registerKey),
                InnerDemand.leafChains(ops, registerKey), predNavAliases);
        Pipelines.Materialized tMat0 = tNavDemand.isEmpty()
                ? Pipelines.materialize(
                        t.pipeline(), tDemand0, t.classFqn())
                : Pipelines.materialize(
                        t.pipeline(), tDemand0, tNavDemand, t.classFqn(),
                        (al2, tc2) -> Pipelines.materialize(
                                sources.get(cs.mappingFqn(), tc2, cs.scope())
                                        .pipeline(),
                                java.util.Set.of(), tc2).pipeline());
        Map<String, Substitution.SubNav> tSubNavs = new LinkedHashMap<>();
        for (var pne : predNavAliases.entrySet()) {
            String pfx = tMat0.slotPrefixes().get(pne.getValue());
            var stepN = java.util.Objects.requireNonNull(
                    Pipelines.navSteps(t.pipeline()).get(pne.getValue()));
            var stepT = stepN.target();
            if (pfx == null || !(stepT instanceof TypedGetAll stg)) {
                continue;
            }
            ClassSource sub = sources.navTarget(t, stg.classFqn(), stepN, pne.getValue());
            tSubNavs.put(pne.getKey(), new Substitution.SubNav(
                    pfx, sub.rowVar(), sub.bindings()));
        }
        // UNION target: member threads carry the key columns the
        // navigate predicate binds on (mirrors the assoc route)
        TypedSpec tPipe0 = tMat0.pipeline();
        if (nav.predicate().parameters().size() == 2) {
            Set<String> tgtReads = new LinkedHashSet<>();
            for (TypedSpec b : nav.predicate().body()) {
                Pipelines.collectVarReads(b,
                        nav.predicate().parameters().get(1), tgtReads);
            }
            tPipe0 = Pipelines.widenConcatenateForKeys(tPipe0, tgtReads);
        }
        TypedSpec tTemporal = temporal.temporalTargetPipe(cs, t,
                registerKey,
                temporal.applyJoinTemporalFilters(tPipe0, t, Map.of()));
        final ClassSource ft = t;
        // a CORRELATED lifted pred composes into the nav step's own
        // (parent, target) condition — both rows in scope, the same
        // composition as the association route
        TypedLambda navCond = nav.predicate();
        TypedLambda corrNav = corrNav0;
        if (corrNav != null) {
            navCond = assocMaterial.andCorrelatedIntoCondition(
                    navCond, corrNav, cs, t, tMat0.slotPrefixes(),
                    parentAssocs, tSubNavs);
        }
        final Map<String, Substitution.SubNav> ftSubNavs = tSubNavs;
        tTemporal = synthetics.applyToPipe(registerKey, tTemporal,
                (p, pred) -> CorrelatedSubselects.predFilteredPipe(p, ft,
                        tMat0.slotPrefixes(), ftSubNavs,
                        pred, cs.mappingFqn()));
        Pipelines.Materialized tMat = new Pipelines.Materialized(
                tTemporal, tMat0.slotPrefixes(), tMat0.stripped());
        // DOTTED (embedded) key: multiplicity comes off the NAV STEP's
        // own stamped info — the class property lookup only speaks
        // single-segment names
        boolean navToMany = !registerKey.equals(head)
                ? !(nav.info().multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded nb
                        && Integer.valueOf(1).equals(nb.upper()))
                : !(ctx.findProperty(cs.classFqn(), SyntheticHeads.realHead(head))
                .map(pr -> pr.multiplicity())
                .filter(mm -> mm instanceof com.legend.compiler.element.type
                        .Multiplicity.Bounded bb
                        && Integer.valueOf(1).equals(bb.upper()))
                .isPresent());
        // #70 COMPOSITE chain-backed target: a navigate condition
        // reading a SIBLING JOINSLOT pulls the slot table INTO the
        // target pipeline, correlated outward by hop-1's condition
        // (see CorrelatedSubselects.compositeChainTarget).
        TypedSpec chainPipe = tMat.pipeline();
        if (corrNav == null && navCond.parameters().size() == 2) {
            CorrelatedSubselects.CompositeChain cc =
                    corrSubs.compositeChainTarget(
                            cs, navCond, chainPipe);
            if (cc != null) {
                chainPipe = cc.pipeline();
                navCond = cc.orientedCond();
            }
        }
        StoreResolver.NestedScope navNs = scopeFn.scope(t, registerKey,
                chainPipe);
        existsSubs.put(registerKey, new Substitution.ExistsSub(navNs.pipeline(),
                navCond, t.rowVar(), t.bindings(),
                navNs.row(),
                t.classFqn(), Pipelines.slotAliases(t.pipeline()),
                tMat0.slotPrefixes(), navToMany)
                .withInnerRegs(navNs.regs())
                .withSubNavs(tSubNavs));
    }
}
