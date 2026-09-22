package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flatten PROVENANCE (the navigation-depth leg, 2026-09-02): the
 * navigate slots a hop materializes INSIDE its target as tails ride the
 * composed pipeline under composed prefixes; the next flatten of such a
 * head re-roots on them, a leaf path reads them through the AssocSub.
 * Also the inverse case — a slot the composition did NOT carry gets the
 * class's own step spliced onto the composed row. Extracted from
 * StoreResolver (file-size guardrail).
 */
final class NavProvenance {

    private final ClassSources sources;
    private final AssociationJoins assocMaterial;
    private final java.util.function.Supplier<com.legend.compiler.element.TypedFunction> coalesce;

    NavProvenance(ClassSources sources, AssociationJoins assocMaterial,
            java.util.function.Supplier<com.legend.compiler.element.TypedFunction> coalesce) {
        this.sources = sources;
        this.assocMaterial = assocMaterial;
        this.coalesce = coalesce;
    }

    /** A navigate-slot hop off a COMPOSED source whose step was stripped
     * inside an earlier hop's target (a to-many slot behind a row-count
     * op is never pre-joined there): the class's OWN step spliced onto
     * the composed row, its left reads re-pointed through the composed
     * prefix — the slot route then serves it as usual. Null when the hop
     * is not one of the class's navigate slots. */
    @com.legend.base.Nullable ClassSource spliceOwnStep(ClassSource src, String hop) {
        if (src.composedPrefix().isEmpty()) {
            return null;
        }
        ClassSource own = src.setId() != null
                ? sources.get(src.mappingFqn(), src.classFqn(), src.setId(), null, "", src.scope())
                : sources.get(src.mappingFqn(), src.classFqn(), src.scope());
        var ownSteps = Pipelines.navSteps(own.pipeline());
        TypedSpec ob = own.bindings().get(hop);
        String oa = ob == null ? null
                : InnerDemand.navSlotAlias(ob, own.rowVar(), ownSteps.keySet());
        if (oa == null || ob == null) {
            return null;
        }
        TypedNavigate st = java.util.Objects.requireNonNull(ownSteps.get(oa));
        TypedLambda pred = st.predicate();
        String lp = pred.parameters().get(0);
        Type.RelationType composedRow = src.rowType();
        TypedSpec body = Pipelines.prefixColumns(
                pred.body().get(pred.body().size() - 1), lp, src.composedPrefix(),
                v -> new TypedVariable(lp, new ExprType(composedRow,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
        // a union-threaded key (a member-union hop composed the row) reads
        // as the coalesce over its member threads
        body = FlattenOps.coalesceThreadedReads(body, lp, composedRow,
                coalesce.get());
        TypedNavigate st2 = st.withSourceAndPredicate(src.pipeline(),
                new TypedLambda(pred.parameters(), List.of(body), pred.info()),
                src.pipeline().info());
        // the hop's binding re-reads the spliced slot off the composed row
        // var (a nested scope's slot route finds the step through it —
        // group F burn 2026-09-02)
        java.util.Map<String, TypedSpec> bindings = new java.util.LinkedHashMap<>(src.bindings());
        bindings.put(hop, new com.legend.compiler.spec.typed.TypedPropertyAccess(
                new TypedVariable(src.rowVar(), new ExprType(composedRow,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                oa, ob.info()));
        return new ClassSource(src.mappingFqn(), src.classFqn(), src.setId(), st2,
                src.rowVar(), bindings, src.rowType(), src.sourceClass(),
                src.deferredWalls(), src.composedPrefix())
                .withScope(src.scope());
    }

    /** A navigate-slot hop's NESTED target (flattenNavSlot's inner
     * materialization): DEPTH through the hop — a downstream path
     * continuing THROUGH this nested head ({@code $t.schema.name} off the
     * hop's re-rooted Table) materializes the nested target WITH its own
     * demanded slots (the association route's depth leg, for slots); its
     * NavMat is recorded so the hop's provenance carries the SubNav tree.
     * No such path: the plain materialization with the union keys. */
    /** {@code owner} is the slot's TARGET whose own step {@code a2} nests
     * here (never the flatten source: a same-named step there is another
     * class's). */
    TypedSpec nestedTarget(NavMaterializer navMaterializer, TemporalFrame temporal,
            ClassSource src, ClassSource owner, String a2, String tc2,
            Map<String, String> headNavAlias,
            java.util.Set<List<String>> downstreamPaths,
            Map<String, NavMaterializer.NavMat> nestedMats) {
        List<List<String>> through = FlattenOps.tailsThrough(a2, headNavAlias, downstreamPaths);
        if (!through.isEmpty()) {
            String head2 = headNavAlias.entrySet().stream()
                    .filter(e -> e.getValue().equals(a2)).map(Map.Entry::getKey)
                    .findFirst().orElse(a2);
            NavMaterializer.NavMat nm = navMaterializer.navTargetMaterialized(
                    temporal, sources.navTarget(owner, tc2, ClassSources.stepOf(owner, a2), head2),
                    src.mappingFqn(), tc2, src.scope(), through,
                    head2, TemporalContext.NONE);
            nestedMats.put(a2, nm);
            return nm.pipeline();
        }
        return Pipelines.materialize(
                NestedUnionKeys.pipeline(sources, owner, tc2, a2, headNavAlias, downstreamPaths),
                java.util.Set.of(), tc2).pipeline();
    }

    /** The hop's demanded NAV HEADS as provenance (flattenNavSlot): each
     * head's target rides the composed pipeline under {@code prefix +
     * its inner prefix}; a head whose nested target materialized WITH
     * its own demanded slots (the depth leg) carries its join-slot
     * prefixes (nav-HEAD bindings stay bare — their SubNav dispatches
     * them) and its SubNav tree, so the next flatten registers each as
     * a head. Returns the hop's SubNav map (hoisted-filter dispatch). */
    Map<String, Substitution.SubNav> registerHopHeads(ClassSource src, String prefix,
            Map<String, String> headNavAlias, Map<String, String> innerPrefixes,
            Map<String, TypedNavigate> tNavSteps,
            Map<String, NavMaterializer.NavMat> nestedMats,
            Map<String, Substitution.AssocSub> provOut) {
        Map<String, Substitution.SubNav> hopSubNavs = new LinkedHashMap<>();
        for (var he : headNavAlias.entrySet()) {
            String ip = innerPrefixes.get(he.getValue());
            if (ip == null) {
                continue;   // step not materialized: the read stays loud
            }
            var navN = java.util.Objects.requireNonNull(tNavSteps.get(he.getValue()));
            var navT = navN.target();
            if (!(navT instanceof com.legend.compiler.spec.typed.TypedGetAll ng)) {
                continue;
            }
            ClassSource sub = sources.navTarget(src, ng.classFqn(), navN, he.getValue());
            NavMaterializer.NavMat nmat = nestedMats.get(he.getValue());
            if (nmat == null) {
                provOut.put(he.getKey(), new Substitution.AssocSub(
                        prefix + ip, sub.rowVar(), sub.bindings(), sub.classFqn(),
                        Pipelines.slotAliases(sub.pipeline())));
                hopSubNavs.put(he.getKey(), new Substitution.SubNav(
                        ip, sub.rowVar(), sub.bindings()));
                continue;
            }
            Map<String, String> subSlotOnly = new LinkedHashMap<>(nmat.slotPrefixes());
            subSlotOnly.keySet().removeAll(Pipelines.navSteps(sub.pipeline()).keySet());
            provOut.put(he.getKey(), new Substitution.AssocSub(
                    prefix + ip, sub.rowVar(), sub.bindings(), sub.classFqn(),
                    Pipelines.slotAliases(sub.pipeline()),
                    subSlotOnly, null, null, Map.of(), nmat.subNavs()));
            hopSubNavs.put(he.getKey(), new Substitution.SubNav(
                    ip, sub.rowVar(), sub.bindings(), nmat.subNavs()));
        }
        return hopSubNavs;
    }

    /** Flatten PROVENANCE for a navigate slot materialized INSIDE a hop
     * (as a tail): the slot's target rows ride the composed pipeline
     * under {@code outerPrefix + sub.prefix()}; the next flatten of that
     * head re-roots on them (flattenMaterializedNav), a leaf path reads
     * them through the AssocSub, and the sub's own materialized slots
     * ride along as SubNav children. The class is the navigate STEP's
     * target (a routed set's class), not the property's declared type. */
    void register(Map<String, Substitution.AssocSub> provOut,
            String head, String outerPrefix, Substitution.SubNav sub,
            ClassSource owner) {
        String navClass = navStepTargetClass(owner, head);
        if (navClass == null) {
            return;
        }
        ClassSource navSrc = sources.navTarget(owner, navClass, navStepOf(owner, head),
                SyntheticHeads.realHead(head));
        provOut.putIfAbsent(head, new Substitution.AssocSub(
                outerPrefix + sub.prefix(), sub.rowVar(), sub.bindings(),
                navClass, Pipelines.slotAliases(navSrc.pipeline()),
                Map.of(), null, null, Map.of(),
                rebaseSubNavs(sub.children(), sub.prefix())));
    }

    /** A SubNav tree's prefixes are composed relative to the ROOT target
     * of its materialization (NavMaterializer); re-rooting the tree under
     * one of its nodes strips that node's prefix from every descendant
     * (the ancestor prefix is a documented invariant of the tree, so
     * this is structural, not string surgery). */
    private static Map<String, Substitution.SubNav> rebaseSubNavs(
            Map<String, Substitution.SubNav> kids, String ancestorPrefix) {
        if (kids.isEmpty()) {
            return kids;
        }
        Map<String, Substitution.SubNav> out = new LinkedHashMap<>();
        for (var e : kids.entrySet()) {
            Substitution.SubNav k = e.getValue();
            if (!k.prefix().startsWith(ancestorPrefix)) {
                throw new IllegalStateException("resolver bug: sub-nav '"
                        + e.getKey() + "' prefix '" + k.prefix()
                        + "' does not extend its ancestor's '" + ancestorPrefix + "'");
            }
            out.put(e.getKey(), new Substitution.SubNav(
                    k.prefix().substring(ancestorPrefix.length()), k.rowVar(),
                    k.bindings(), rebaseSubNavs(k.children(), ancestorPrefix)));
        }
        return out;
    }

    /** The class a navigate-slot head of {@code owner} lands on: the
     * step's own getAll target (a routed set's class), else the declared
     * property / association-end type. */
    @com.legend.base.Nullable String navStepTargetClass(ClassSource owner,
            String head) {
        TypedNavigate step = navStepOf(owner, head);
        if (step != null && step.target() instanceof TypedGetAll tg) {
            return tg.classFqn();
        }
        return assocMaterial.hopTargetClass(owner.classFqn(), head);
    }

    /** The navigate step a head of {@code owner} reads, or null (an
     * association end, an embedded head). */
    @com.legend.base.Nullable TypedNavigate navStepOf(ClassSource owner, String head) {
        TypedSpec b = owner.bindings().get(SyntheticHeads.realHead(head));
        var steps = Pipelines.navSteps(owner.pipeline());
        String alias = b == null ? null
                : InnerDemand.navSlotAlias(b, owner.rowVar(), steps.keySet());
        return alias == null ? null : steps.get(alias);
    }

}
