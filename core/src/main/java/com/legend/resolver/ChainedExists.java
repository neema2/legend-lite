package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * THE NESTED-EXISTS RUNG (dotted emptiness whose MID hop is itself
 * TO-MANY): {@code isNotEmpty(filter($this.employees.addresses, pred))}
 * where {@code employees} is a to-many navigate head. The flat
 * dotted-path registration (registerDottedExistsSubs) rides an
 * ALREADY-JOINED mid hop (an AssocSub on the outer row) — a to-many mid
 * hop never joins flat (it would fan the outer row out), so the EXISTS
 * material must carry the whole chain itself.
 *
 * <p>Engine parity: the inner emptiness processes as ONE correlated
 * EXISTS whose subselect FROM-tree joins the entire navigation chain
 * (mid JOIN leaf), correlated outward by the MID hop's join condition
 * against the parent row (RelationalGraphTree exists-processing: the
 * join-tree nodes of every hop live INSIDE the exists subselect).
 *
 * <p>Construction: the LEAF target's materialized pipeline is the base
 * (its bindings serve the predicate's target-side reads unprefixed);
 * the MID pipeline joins in under a chain-private prefix with the
 * leaf-hop nav condition; the oriented condition re-points the mid-hop
 * condition's target-side reads onto the prefixed columns. Shapes
 * outside the rung (slot-reading nav conditions, milestoned hops,
 * association-mapped hops) return null — the loud wall stays.
 */
final class ChainedExists {

    private ChainedExists() {
    }

    /** The exploded 2-hop EXISTS material for {@code path} (mid.leaf)
     * over {@code cs}, or null when the shape is outside the rung. */
    static Substitution.@com.legend.base.Nullable ExistsSub explodedTwoHop(
            ClassSources sources, TemporalFrame temporal, ClassSource cs,
            List<String> path, List<TypedSpec> ops,
            BiFunction<ClassSource, TypedSpec, StoreResolver.NestedScope>
                    nestedScope) {
        String midHead = SyntheticHeads.realHead(path.get(0));
        String leafHead = SyntheticHeads.realHead(path.get(1));
        // MID hop: a navigate-slot binding on the parent (to-many heads
        // in exists position never register flat assoc material).
        TypedNavigate midNav = navStep(cs, midHead);
        if (midNav == null
                || !(midNav.target() instanceof TypedGetAll midG)
                || !sources.binds(cs.mappingFqn(), midG.classFqn())) {
            return null;
        }
        ClassSource mid = sources.navTarget(cs, midG.classFqn(), midNav, midHead);
        // LEAF hop: a navigate-slot binding on the MID target.
        TypedNavigate leafNav = navStep(mid, leafHead);
        if (leafNav == null
                || !(leafNav.target() instanceof TypedGetAll leafG)
                || !sources.binds(cs.mappingFqn(), leafG.classFqn())) {
            return null;
        }
        ClassSource t = sources.navTarget(mid, leafG.classFqn(), leafNav, leafHead);
        TypedLambda midCond = midNav.predicate();
        TypedLambda leafCond = leafNav.predicate();
        // OUTSIDE the rung: milestoned hops (no temporal stamping built
        // here), multi-statement or slot-reading nav conditions (the
        // composite-chain family), non-2-param conditions.
        if (midCond.parameters().size() != 2 || midCond.body().size() != 1
                || leafCond.parameters().size() != 2
                || leafCond.body().size() != 1
                || !temporal.milestoneColumnsOf(mid.pipeline(),
                        mid.classFqn()).isEmpty()
                || !temporal.milestoneColumnsOf(t.pipeline(),
                        t.classFqn()).isEmpty()
                || condReadsSlots(midCond, cs, mid)
                || condReadsSlots(leafCond, mid, t)) {
            return null;
        }
        // LEAF target materialization — column-slot demand only; the
        // inner predicates' NAV reads ($b.location.street) are the
        // nested scope's job (its assoc materials widen the composite
        // below — materializing them here too would double-join:
        // 'duplicate column location_ID').
        List<TypedLambda> innerPreds = InnerDemand.lambdas(ops, path);
        Set<String> innerLeaves = new LinkedHashSet<>();
        for (TypedLambda lam : innerPreds) {
            if (!lam.parameters().isEmpty()) {
                InnerDemand.collectParamPathHeads(lam,
                        lam.parameters().get(0), innerLeaves);
            }
        }
        Set<String> tSlots = Pipelines.slotAliases(t.pipeline());
        Set<String> tDemand = new LinkedHashSet<>();
        for (String leaf : innerLeaves) {
            TypedSpec lb = t.bindings().get(leaf);
            if (lb != null) {
                CorrelatedSubselects.collectAliasReads(lb, t.rowVar(), tSlots,
                        tDemand);
            }
        }
        tDemand = Pipelines.closeOverConditions(t.pipeline(), tDemand);
        Pipelines.Materialized tMat = Pipelines.materialize(t.pipeline(),
                tDemand, t.classFqn());
        // UNION leaf target: member threads carry the key columns the
        // leaf-hop condition binds on (mirrors the flat routes).
        TypedSpec leafPipe = tMat.pipeline();
        Set<String> tgtReads = new LinkedHashSet<>();
        for (TypedSpec b : leafCond.body()) {
            Pipelines.collectVarReads(b, leafCond.parameters().get(1),
                    tgtReads);
        }
        leafPipe = StackBuilder.demandForKeys(leafPipe, tgtReads);
        Type.RelationType leafRow = Type.relationSchema(leafPipe.info().type());
        if (leafRow == null) {
            return null;
        }
        // MID pipeline (slot-undemanded: its condition reads base
        // columns only — guarded above) joins the leaf base under a
        // chain-private prefix with the LEAF-hop condition.
        TypedSpec midPipe = Pipelines.materialize(mid.pipeline(), Set.of(),
                mid.classFqn()).pipeline();
        Set<String> midKeys = new LinkedHashSet<>();
        for (TypedSpec b : midCond.body()) {
            Pipelines.collectVarReads(b, midCond.parameters().get(1), midKeys);
        }
        for (TypedSpec b : leafCond.body()) {
            Pipelines.collectVarReads(b, leafCond.parameters().get(0), midKeys);
        }
        midPipe = StackBuilder.demandForKeys(midPipe, midKeys);
        Type.RelationType midRow = Type.relationSchema(midPipe.info().type());
        if (midRow == null) {
            return null;
        }
        String pfx = midHead + "_";
        boolean clash = true;
        while (clash) {
            clash = false;
            for (Type.Column c : leafRow.columns()) {
                if (c.name().startsWith(pfx)) {
                    pfx = "_" + pfx;
                    clash = true;
                }
            }
        }
        List<Type.Column> compCols = new ArrayList<>(leafRow.columns());
        for (Type.Column c : midRow.columns()) {
            compCols.add(new Type.Column(pfx + c.name(), c.type(),
                    c.multiplicity()));
        }
        Type.RelationType compRow = new Type.RelationType(compCols);
        var one = Multiplicity.Bounded.ONE;
        // join condition (left = leaf base row, right = mid row): the
        // LEAF-hop nav condition oriented onto the join's two rows.
        Set<String> taken = new LinkedHashSet<>(midCond.parameters());
        taken.addAll(leafCond.parameters());
        String lv = freshName("_x2l", taken);
        String rv = freshName("_x2r", taken);
        final String lvF = lv;
        final String rvF = rv;
        TypedSpec jb = Pipelines.rewriteRowReads(
                leafCond.body().get(0), leafCond.parameters().get(1),
                Map.of(), Set.of(),
                v -> new TypedVariable(lvF, new ExprType(leafRow, one)));
        jb = Pipelines.rewriteRowReads(jb, leafCond.parameters().get(0),
                Map.of(), Set.of(),
                v -> new TypedVariable(rvF, new ExprType(midRow, one)));
        TypedLambda joinCond = new TypedLambda(List.of(lv, rv), List.of(jb),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(leafRow, one),
                                new Type.Param(midRow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        TypedSpec composite = new TypedJoin(leafPipe, midPipe,
                AssociationJoins.leftKind(), joinCond, Optional.of(pfx), null,
                new ExprType(Type.relation(compRow), one),
                false /* resolver-synth */);
        // oriented (parent, chain-row) condition: the MID-hop condition
        // with its target-side reads re-pointed at the prefixed columns.
        String pv = midCond.parameters().get(0);
        String tv = freshName("_x2t", taken);
        final String tvF = tv;
        TypedSpec oc = Pipelines.prefixColumns(midCond.body().get(0),
                midCond.parameters().get(1), pfx,
                v -> new TypedVariable(tvF, new ExprType(compRow, one)));
        TypedLambda oriented = new TypedLambda(List.of(pv, tv), List.of(oc),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(cs.rowType(), one),
                                new Type.Param(compRow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        StoreResolver.NestedScope dns = nestedScope.apply(t, composite);
        return new Substitution.ExistsSub(dns.pipeline(), oriented,
                t.rowVar(), t.bindings(), dns.row(), t.classFqn(),
                Pipelines.slotAliases(t.pipeline()), tMat.slotPrefixes(),
                true)
                .withInnerRegs(dns.regs());
    }

    /** The parent's navigate step behind {@code head}'s binding, or null
     * (unmapped head, non-nav binding, association-mapped head). */
    private static @com.legend.base.Nullable TypedNavigate navStep(
            ClassSource parent, String head) {
        var navSteps = Pipelines.navSteps(parent.pipeline());
        String alias = InnerDemand.navSlotAlias(parent.bindings().get(head),
                parent.rowVar(), navSteps.keySet());
        return alias == null ? null : navSteps.get(alias);
    }

    /** True when the hop condition reads either side's JOIN SLOTS —
     * the composite-chain family, outside this rung. */
    private static boolean condReadsSlots(TypedLambda cond,
            ClassSource sourceSide, ClassSource targetSide) {
        Set<String> sSlots = Pipelines.slotAliases(sourceSide.pipeline());
        Set<String> tSlots = Pipelines.slotAliases(targetSide.pipeline());
        for (TypedSpec b : cond.body()) {
            if (Pipelines.referencesAliasOn(b, cond.parameters().get(0),
                    sSlots)
                    || Pipelines.referencesAliasOn(b,
                            cond.parameters().get(1), tSlots)) {
                return true;
            }
        }
        return false;
    }

    private static String freshName(String base, Set<String> taken) {
        String n = base;
        int i = 2;
        while (taken.contains(n)) {
            n = base + i++;
        }
        taken.add(n);
        return n;
    }
}
