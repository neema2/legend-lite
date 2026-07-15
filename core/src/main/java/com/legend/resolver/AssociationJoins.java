// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
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
        TypedSpec binding = cs.bindings().get(head);
        if (binding == null) {
            return associationJoin(temporal, cs, head, context, false, leaves);
        }
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String alias = StoreResolver.navSlotAlias(binding, cs.rowVar(), navSteps.keySet());
        var nav = navSteps.get(alias);
        String targetClass = ((com.legend.compiler.spec.typed.TypedGetAll)
                nav.target()).classFqn();
        ClassSource t = sources.get(cs.mappingFqn(), targetClass);
        Set<String> targetSlots = Pipelines.slotAliases(t.pipeline());
        Set<String> targetDemand = new java.util.LinkedHashSet<>();
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
                temporal.applyJoinTemporalFilters(tMat.pipeline(), t, java.util.Map.of()));
        return new AssocJoin(prefixFor(head, cs), t, tPipe0,
                (com.legend.compiler.element.type.Type.RelationType)
                        tPipe0.info().type(),
                nav.predicate(), tMat.slotPrefixes());
    }

    /** A demanded association navigation, ready to emit as a prefixed LEFT join. */
    record AssocJoin(String prefix, ClassSource target,
                             TypedSpec targetPipeline,
                             com.legend.compiler.element.type.Type.RelationType targetRow,
                             TypedLambda condition,
                             Map<String, String> targetSlotPrefixes) {}

    /**
     * A chained hop's prefix, ordinal-bumped against the ACCUMULATED column
     * set: the source row plus every already-registered join's prefixed
     * columns — the same guard {@link #prefixFor} gives hop 0.
     */
    static String chainedPrefix(String base, ClassSource cs,
                                        Map<String, AssocJoin> joinsByChain) {
        Set<String> taken = new java.util.LinkedHashSet<>();
        for (com.legend.compiler.element.type.Type.Column c : cs.rowType().columns()) {
            taken.add(c.name());
        }
        for (AssocJoin aj : joinsByChain.values()) {
            for (com.legend.compiler.element.type.Type.Column c : aj.targetRow().columns()) {
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
        Set<String> taken = new java.util.LinkedHashSet<>();
        for (com.legend.compiler.element.type.Type.Column c : cs.rowType().columns()) {
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
        TypedLambda synthPred = synthetics.pred(head);
        if (synthPred != null) {
            Set<String> withPredLeaves = new java.util.LinkedHashSet<>(demandedLeaves);
            for (TypedSpec b : synthPred.body()) {
                StoreResolver.collectParamPathHeads(b, synthPred.parameters().get(0),
                        withPredLeaves);
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
        if (!forExists
                && !(end.multiplicity() instanceof com.legend.parser.Multiplicity.Concrete)) {
            throw new NotImplementedException("navigation of association end '$"
                    + head + "' with non-concrete multiplicity "
                    + end.multiplicity() + " is not supported");
        }
        String targetClass = ((com.legend.parser.TypeExpression.NameRef)
                end.targetClass()).name();
        ClassSource target = sources.get(cs.mappingFqn(), targetClass);
        // The TARGET's own join slots materialize on demand too: a demanded
        // leaf whose binding reads a slot ($p.firm.country where country is
        // @FirmCountry-mapped) pulls that slot's LEFT join into the target
        // pipeline — nested navigation joins, the W4 slice.
        Set<String> targetSlots = Pipelines.slotAliases(target.pipeline());
        Set<String> targetDemand = new java.util.LinkedHashSet<>();
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
                                java.util.Map.of())),
                tMat0.slotPrefixes(), tMat0.stripped());

        // The predicate function: mapping's AssociationBinding for the assoc.
        var mapping = ctx.findMapping(cs.mappingFqn()).orElseThrow();
        var binding = mapping.associationBindings().stream()
                .filter(ab -> ab.associationFqn().equals(assoc.qualifiedName()))
                .findFirst()
                .orElseThrow(() -> new MappingResolutionException("association '"
                        + assoc.qualifiedName() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'"
                        // a dropped/poisoned property route often lands here
                        // (the assoc fallback) — surface the recorded reason
                        + ctx.mappingPoison(cs.mappingFqn(), cs.classFqn())
                                .map(r -> " (" + r + ")").orElse(""),
                        assoc.qualifiedName()));
        var fns = ctx.findFunction(binding.predicateFunctionFqn());
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: association predicate '"
                    + binding.predicateFunctionFqn() + "' has " + fns.size() + " overloads");
        }
        var cf = specs.compile(fns.get(0));
        TypedSpec last = cf.body().get(cf.body().size() - 1);
        if (!(last instanceof com.legend.compiler.spec.typed.TypedNativeCall call)
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
        String classAFqn = ((com.legend.compiler.element.type.Type.ClassType)
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
            var ft = (com.legend.compiler.element.type.Type.FunctionType)
                    cond.info().type();
            var swapped = new com.legend.compiler.element.type.Type.FunctionType(
                    java.util.List.of(ft.params().get(1), ft.params().get(0)),
                    ft.result());
            oriented = new TypedLambda(java.util.List.of(cond.parameters().get(1),
                    cond.parameters().get(0)), cond.body(),
                    new com.legend.compiler.element.type.ExprType(swapped,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        }
        // TARGET-SIDE join-key collection: a distinct-narrowed target must
        // expose the key columns the association condition binds on.
        Set<String> tgtReads = new java.util.LinkedHashSet<>();
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
        tPipe = temporal.applyJoinTemporalFilters(tPipe, target, java.util.Map.of());
        if (synthPred != null) {
            tPipe = StoreResolver.predFilteredPipe(tPipe, target, tMat.slotPrefixes(),
                    synthPred, cs.mappingFqn());
        }
        return new AssocJoin(prefixFor(head, cs), target, tPipe,
                (com.legend.compiler.element.type.Type.RelationType)
                        tPipe.info().type(),
                oriented, tMat.slotPrefixes());
    }
}
