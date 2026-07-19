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

    AssociationJoins(ModelContext ctx, ClassSources sources,
            SpecCompiler specs, SyntheticHeads synthetics) {
        this.ctx = ctx;
        this.sources = sources;
        this.specs = specs;
        this.synthetics = synthetics;
    }

    /** The join material for an aggregated to-many head: the association
     * route, or the navigate-slot route (class-typed Join PM). */
    AssocJoin aggJoinMaterial(TemporalFrame temporal, ClassSource cs, String head, StoreResolver.Context context,
                                      Set<String> leaves) {
        // synthetic identities (#fN/#cN) bind by their REAL property — the
        // raw lookup missed the navigate-slot route and fell into the
        // association route, which errors when the property is PM-mapped
        TypedSpec binding = cs.bindings().get(SyntheticHeads.realHead(head));
        if (binding == null) {
            return associationJoin(temporal, cs, head, context, false, leaves);
        }
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String alias = StoreResolver.navSlotAlias(binding, cs.rowVar(), navSteps.keySet());
        var nav = navSteps.get(alias);
        String targetClass = ((TypedGetAll)
                nav.target()).classFqn();
        ClassSource t = sources.get(cs.mappingFqn(), targetClass);
        Set<String> targetSlots = Pipelines.slotAliases(t.pipeline());
        Set<String> targetDemand = new LinkedHashSet<>();
        if (!targetSlots.isEmpty()) {
            for (String leaf : leaves) {
                TypedSpec b = t.bindings().get(leaf);
                if (b != null) {
                    StoreResolver.collectAliasReads(b, t.rowVar(), targetSlots, targetDemand);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(t.pipeline(), targetDemand);
        Pipelines.Materialized tMat = Pipelines.materialize(
                t.pipeline(), targetDemand, t.classFqn());
        TypedSpec tPipe0 = temporal.temporalTargetPipe(cs, t, head,
                temporal.applyJoinTemporalFilters(tMat.pipeline(), t, Map.of()));
        // a lifted head's parked material (filter / union branches)
        // applies to the aggregated target exactly like the plain routes;
        // a CORRELATED pred cannot apply here — loud, never dropped
        if (synthetics.correlatedPred(head) != null) {
            throw new NotImplementedException("correlated filtered navigation"
                    + " '" + SyntheticHeads.realHead(head) + "' is not"
                    + " supported on the aggregated route yet");
        }
        tPipe0 = synthetics.applyToPipe(head, tPipe0, (p, pred) ->
                StoreResolver.predFilteredPipe(p, t, tMat.slotPrefixes(),
                        pred, cs.mappingFqn()));
        return new AssocJoin(prefixFor(head, cs), t, tPipe0,
                (Type.RelationType)
                        tPipe0.info().type(),
                nav.predicate(), tMat.slotPrefixes());
    }

    /** A demanded association navigation, ready to emit as a prefixed LEFT join. */
    record AssocJoin(String prefix, ClassSource target,
                             TypedSpec targetPipeline,
                             Type.RelationType targetRow,
                             TypedLambda condition,
                             Map<String, String> targetSlotPrefixes) {}

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

    /** Deterministic prefix with ordinal bump on collision against the parent row (plan §2.3). */
    static String prefixFor(String head, ClassSource cs) {
        Set<String> taken = new LinkedHashSet<>();
        for (Type.Column c : cs.rowType().columns()) {
            taken.add(c.name());
        }
        String prefix = head + "_";
        int ordinal = 2;
        while (hasPrefixCollision(prefix, taken)) {
            prefix = head + "_" + ordinal++ + "_";
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
            var hit = m.get().associationBindings().stream()
                    .filter(ab -> ab.associationFqn().equals(assocFqn))
                    .findFirst();
            if (hit.isPresent()) {
                return hit;
            }
            for (var inc : m.get().includes()) {
                queue.add(inc.mappingPath());
            }
        }
        return java.util.Optional.empty();
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
        // A SYNTHETIC head resolves by its underlying property; its parked
        // predicate joins the leaf demand (the pred's own reads pull the
        // target's slots) and wraps the finished target pipeline below.
        String real = SyntheticHeads.realHead(head);
        List<TypedLambda> synthPreds = synthetics.allPreds(head);
        if (!synthPreds.isEmpty()) {
            Set<String> withPredLeaves = new LinkedHashSet<>(demandedLeaves);
            for (TypedLambda sp : synthPreds) {
                for (TypedSpec b : sp.body()) {
                    StoreResolver.collectParamPathHeads(b, sp.parameters().get(0),
                            withPredLeaves);
                }
            }
            demandedLeaves = withPredLeaves;
        }
        var assoc = ctx.findAssociationOf(cs.classFqn(), real).orElseThrow(() ->
                new MappingResolutionException("property '" + real + "' of class '"
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
        ClassSource target = sources.get(cs.mappingFqn(), targetClass);
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
                    StoreResolver.collectAliasReads(b, target.rowVar(), targetSlots, targetDemand);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(target.pipeline(), targetDemand);
        Pipelines.Materialized tMat0 = Pipelines.materialize(
                target.pipeline(), targetDemand, target.classFqn());
        Pipelines.Materialized tMat = new Pipelines.Materialized(
                temporal.temporalTargetPipe(cs, target, chainKey,
                        temporal.applyJoinTemporalFilters(tMat0.pipeline(), target,
                                Map.of())),
                tMat0.slotPrefixes(), tMat0.stripped());

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
                                .or(() -> ctx.mappingPoison(cs.mappingFqn(),
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
                || !call.callee().qualifiedName().equals("meta::legend::lite::legacyAssocPredicate")
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
        if (!classAFqn.equals(cs.classFqn()) && !classAFqn.equals(targetClass)) {
            throw new IllegalStateException("resolver bug: association predicate '"
                    + binding.predicateFunctionFqn() + "' first param class '"
                    + classAFqn + "' is neither parent '" + cs.classFqn()
                    + "' nor target '" + targetClass + "'");
        }
        boolean reverse = cs.classFqn().equals(targetClass)
                ? !assoc.property1().propertyName().equals(real)
                : !cs.classFqn().equals(classAFqn);
        TypedLambda oriented = cond;
        if (reverse) {
            var ft = (Type.FunctionType)
                    cond.info().type();
            var swapped = new Type.FunctionType(
                    List.of(ft.params().get(1), ft.params().get(0)),
                    ft.result());
            oriented = new TypedLambda(List.of(cond.parameters().get(1),
                    cond.parameters().get(0)), cond.body(),
                    new ExprType(swapped,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        }
        // A CORRELATED lifted predicate ANDs into the CONDITION — the one
        // place both rows are in scope: its own param substitutes against
        // the TARGET bindings over the condition's target row; the residual
        // OUTER-variable reads substitute against the PARENT's bindings
        // over the condition's source row (audit 14 B-F1's correlation
        // pass). Runs BEFORE key collection so the pred's target reads
        // widen distinct/union keys too.
        TypedLambda corr = synthetics.correlatedPred(head);
        if (corr != null) {
            oriented = andCorrelatedIntoCondition(oriented, corr, cs, target,
                    tMat.slotPrefixes());
        }
        // TARGET-SIDE join-key collection: a distinct-narrowed target must
        // expose the key columns the association condition binds on.
        Set<String> tgtReads = new LinkedHashSet<>();
        for (TypedSpec b : oriented.body()) {
            Pipelines.collectVarReads(b, oriented.parameters().get(1), tgtReads);
        }
        TypedSpec tPipe = Pipelines.widenDistinctForKeys(tMat.pipeline(), tgtReads);
        // UNION target: member threads carry the key columns the
        // association condition binds on (engine partial-union goldens)
        tPipe = Pipelines.widenConcatenateForKeys(tPipe, tgtReads);
        // audit 10: the target pipeline's OWN materialized slot joins to
        // milestoned tables filter by the temporal context too (every
        // milestoned table alias filters — the dead wall this replaces)
        tPipe = temporal.applyJoinTemporalFilters(tPipe, target, Map.of());
        tPipe = synthetics.applyToPipe(head, tPipe, (p, pred) ->
                StoreResolver.predFilteredPipe(p, target, tMat.slotPrefixes(),
                        pred, cs.mappingFqn()));
        return new AssocJoin(prefixFor(head, cs), target, tPipe,
                (Type.RelationType)
                        tPipe.info().type(),
                oriented, tMat.slotPrefixes());
    }

    /** The correlation pass: two sequential substitutions over the lifted
     * predicate — own param via TARGET bindings onto the condition's
     * target row; each residual FREE variable via the PARENT's bindings
     * onto the condition's source row — then AND into the condition body. */
    TypedLambda andCorrelatedIntoCondition(TypedLambda cond,
            TypedLambda pred, ClassSource parent, ClassSource target,
            Map<String, String> targetSlotPrefixes) {
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
        var ft = (Type.FunctionType) cond.info().type();
        // the assoc-route cond declares concrete relation params; the
        // navigate-step emission is GENERIC (TypeVars) — the actual
        // pipelines carry the row shapes either way
        Type.RelationType srcRow = rowOr(ft.params().get(0).type(),
                parent.rowType());
        Type.RelationType tgtRow = rowOr(ft.params().get(1).type(),
                target.rowType());
        // pass 1: the pred's own param -> target bindings over tgtParam
        Substitution tgtSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(pred.parameters().get(0), tgtParam,
                        target.classFqn(), target.mappingFqn(),
                        target.rowVar(), target.bindings(), tgtRow,
                        unconvertedTgt, targetSlotPrefixes, Map.of()),
                Substitution.Registries.NONE, Substitution.TemporalView.NONE,
                true, true));
        TypedLambda pass1 = tgtSub.rewriteLambda(pred);
        // pass 2: each residual free variable (collected pre-pass-1,
        // shadow-aware) -> parent bindings over srcParam
        TypedSpec body = pass1.body().get(pass1.body().size() - 1);
        for (String outer : free) {
            Substitution srcSub = new Substitution(new Substitution.Target(
                    new Substitution.RowScope(outer, srcParam,
                            parent.classFqn(), parent.mappingFqn(),
                            parent.rowVar(), parent.bindings(),
                            srcRow,
                            new LinkedHashSet<>(
                                    Pipelines.slotAliases(parent.pipeline())),
                            Map.of(), Map.of()),
                    Substitution.Registries.NONE,
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
        return t instanceof Type.RelationType rt ? rt : fallback;
    }
}
