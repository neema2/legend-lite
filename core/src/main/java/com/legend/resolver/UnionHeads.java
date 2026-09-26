package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.model.MappingDefinition;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.element.ModelContext;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * UNION heads ({@code #uN}): the join material of a concatenate of
 * navigation chains through DIFFERENT head properties —
 * {@code $t.subAccount.oe->concatenate($t.otherAccount.oe).name}.
 *
 * <p>Engine semantics (processConcatenate, pureToSQLQuery.pure:2709;
 * buildConcatenateSubSelect :2889): each branch chain becomes one member
 * select, the members are aligned onto one column list (every member's
 * join keys ride the union, NULL in the members that do not own them —
 * alignJoinAndPkColumnsForUnion) and UNION ALL-ed into ONE
 * {@code unionalias_N} derived table, LEFT-joined on the OR of the branch
 * join conditions; the leaf reads the union's shared column. Rows
 * explode when several branches match (a LEFT join, never a coalesce).
 *
 * <p>Ours: member j = branch j's hop-0 target material (the association
 * route or the navigate-slot route, whichever the head is) with a second
 * hop chained INSIDE the member (a target navigate slot rides the SubNav,
 * an embedded ctor drills, an association hop LEFT-joins the member);
 * the union row = one column per demanded leaf property + every member's
 * condition keys aligned BY NAME across the members (the engine's
 * alignJoinAndPkColumnsForUnion: a key column the member lacks projects
 * NULL; two branches keyed on a same-named column SHARE it — the
 * ComplexReturnType golden joins {@code unionalias_0.ID = root.FIRMID or
 * unionalias_0.ID = root.ADDRESSID} over ONE {@code ID}, and its rows
 * include the cross matches); the head's condition = OR over members of
 * the branch condition re-pointed at the union row.
 */
final class UnionHeads {
    private final ModelContext ctx;
    private final ClassSources sources;
    private final SyntheticHeads synthetics;
    private final AssociationJoins joins;
    private final @com.legend.base.Nullable NavMaterializer navMaterializer;

    UnionHeads(ModelContext ctx, ClassSources sources,
            SyntheticHeads synthetics, AssociationJoins joins,
            @com.legend.base.Nullable NavMaterializer navMaterializer) {
        this.ctx = ctx;
        this.sources = sources;
        this.synthetics = synthetics;
        this.joins = joins;
        this.navMaterializer = navMaterializer;
    }

    /** One hop's target material, whichever route served it. */
    private record Hop(ClassSource target, TypedSpec pipeline,
                       Type.RelationType row, TypedLambda cond,
                       Map<String, String> slotPrefixes,
                       Map<String, Substitution.SubNav> subNavs) {}

    /** A leaf expression still spelled over its own row variable. */
    private record Read(TypedSpec expr, String rowVar, String prefix) {}

    /** One branch as a member relation: its pipe and row, the demanded
     * leaves as expressions over {@code m}, its hop-0 condition and the
     * target-side key columns that condition reads. */
    private record Member(TypedSpec pipe, Type.RelationType row,
                          Map<String, TypedSpec> leaves, TypedLambda cond,
                          List<String> keys) {}

    private static final String MEMBER_VAR = "m";

    AssociationJoins.AssocJoin material(TemporalFrame temporal, ClassSource cs,
            String head, StoreResolver.Context context, Set<String> leaves) {
        SyntheticHeads.UnionSpec spec = synthetics.unionSpec(head);
        if (leaves.isEmpty()) {
            throw new NotImplementedException("concatenated navigation of '"
                    + spec.classFqn() + "' read as a whole value is not"
                    + " supported yet");
        }
        List<Member> members = new ArrayList<>(spec.paths().size());
        for (List<String> path : spec.paths()) {
            members.add(member(temporal, cs, path, context, leaves));
        }
        // THE STACK builds the union: each branch is an arm whose "set" is
        // its member relation (the demanded leaves as its bindings), its
        // hop-0 condition's target reads projected as route keys BY NAME —
        // two branches keyed on a same-named column share it (the engine's
        // alignJoinAndPkColumnsForUnion), a key an arm lacks is NULL there
        MappingDefinition mapping = ctx.findMapping(cs.mappingFqn()).orElseThrow(() ->
                new MappingResolutionException("unknown mapping '" + cs.mappingFqn() + "'",
                        cs.mappingFqn()));
        List<StackBuilder.Arm> arms = new ArrayList<>(members.size());
        for (Member m : members) {
            ClassSource branch = new ClassSource(cs.mappingFqn(), spec.classFqn(),
                    ClassSource.UNION_SET_ID, m.pipe(), MEMBER_VAR, m.leaves(), m.row());
            arms.add(new StackBuilder.Arm(branch, m.pipe(), new TypedNavigate.Route(
                    m.pipe(), m.pipe(), m.cond(), m.keys(), m.keys())));
        }
        ClassSource target = sources.stacks().stackOf(cs.mappingFqn(), spec.classFqn(), mapping,
                arms, List.of(), false);
        Type.RelationType urow = target.rowType();
        List<TypedNavigate.Route> routes = new ArrayList<>(arms.size());
        for (StackBuilder.Arm a : arms) {
            routes.add(java.util.Objects.requireNonNull(a.route()));
        }
        TypedLambda cond = sources.stacks().orOverRoutes(routes, cs.rowType(), urow);
        return new AssociationJoins.AssocJoin(
                AssociationJoins.prefixFor(head, cs), target, target.pipeline(), urow,
                cond, Map.of(), Map.of(), null, null, false);
    }

    private Member member(TemporalFrame temporal, ClassSource cs,
            List<String> path, StoreResolver.Context context,
            Set<String> leaves) {
        String h0 = path.get(0);
        List<String> tail = path.subList(1, path.size());
        if (tail.size() > 1) {
            throw new NotImplementedException("concatenated navigation branch '$"
                    + String.join(".", path) + "' deeper than two hops is not"
                    + " supported yet");
        }
        Set<List<String>> navTails = new LinkedHashSet<>();
        if (!tail.isEmpty()) {
            for (String l : leaves) {
                List<String> t = new ArrayList<>(tail);
                t.add(l);
                navTails.add(t);
            }
        }
        Set<String> leaves0 = tail.isEmpty() ? leaves : Set.of(tail.get(0));
        Hop h = hop(temporal, cs, h0, context, leaves0, navTails);
        TypedSpec pipe = h.pipeline();
        Type.RelationType row = h.row();
        Map<String, Read> reads = new LinkedHashMap<>();
        if (tail.isEmpty()) {
            for (String l : leaves) {
                reads.put(l, new Read(leafBinding(h.target(), l, h.slotPrefixes(),
                        h.subNavs()), h.target().rowVar(), ""));
            }
        } else {
            String t0 = tail.get(0);
            Substitution.SubNav sn = h.subNavs().get(t0);
            TypedSpec tb = h.target().bindings().get(t0);
            if (sn != null) {
                // the target's own navigate slot, materialized INTO the
                // hop (its leaves ride the SubNav's prefix)
                for (String l : leaves) {
                    reads.put(l, new Read(requireLeaf(sn.bindings().get(l),
                            h.target().classFqn(), t0, l), sn.rowVar(), sn.prefix()));
                }
            } else if (tb != null
                    && Pipelines.unwrapToOne(tb) instanceof TypedNewInstance ctor) {
                // an EMBEDDED ctor on the target row: drill its fields
                for (String l : leaves) {
                    reads.put(l, new Read(requireLeaf(ctor.properties().get(l),
                            h.target().classFqn(), t0, l), h.target().rowVar(), ""));
                }
            } else {
                // an ASSOCIATION hop of the target: LEFT-joined INSIDE the
                // member (the member is the branch CHAIN, keyed by hop 0)
                AssociationJoins.AssocJoin aj1 = joins.associationJoin(temporal,
                        h.target(), t0, context, false, leaves, h0 + "." + t0,
                        Set.of());
                List<Type.Column> cols = new ArrayList<>(row.columns());
                for (Type.Column c : aj1.targetRow().columns()) {
                    cols.add(new Type.Column(aj1.prefix() + c.name(), c.type(),
                            c.multiplicity()));
                }
                row = new Type.RelationType(cols);
                pipe = new TypedJoin(pipe, aj1.targetPipeline(),
                        AssociationJoins.leftKind(),
                        java.util.Objects.requireNonNull(aj1.condition(),
                                "association hop without a condition"),
                        Optional.of(aj1.prefix()), null,
                        new ExprType(Type.relation(row), Multiplicity.Bounded.ONE),
                        false /* resolver-synth */);
                for (String l : leaves) {
                    reads.put(l, new Read(leafBinding(aj1.target(), l,
                            aj1.targetSlotPrefixes(), aj1.targetSubNavs()),
                            aj1.target().rowVar(), aj1.prefix()));
                }
            }
        }
        var mInfo = new ExprType(row, Multiplicity.Bounded.ONE);
        Map<String, TypedSpec> leafExprs = new LinkedHashMap<>();
        for (var e : reads.entrySet()) {
            Read r = e.getValue();
            leafExprs.put(e.getKey(), Pipelines.prefixColumns(r.expr(), r.rowVar(),
                    r.prefix(), v -> new TypedVariable(MEMBER_VAR, mInfo)));
        }
        Set<String> keys = new LinkedHashSet<>();
        TypedLambda cond = h.cond();
        for (TypedSpec b : cond.body()) {
            Pipelines.collectVarReads(b, cond.parameters().get(1), keys);
        }
        return new Member(pipe, row, leafExprs, cond, new ArrayList<>(keys));
    }

    /** A scalar leaf of {@code target}, its slot-backed reads flattened
     * onto the materialized row; a class-typed leaf is a further hop
     * (loud — the union projects VALUES). */
    private static TypedSpec leafBinding(ClassSource target, String leaf,
            Map<String, String> slotPrefixes,
            Map<String, Substitution.SubNav> subNavs) {
        TypedSpec b = requireLeaf(target.bindings().get(leaf), target.classFqn(),
                null, leaf);
        if (Type.asClassType(Pipelines.unwrapToOne(b).info().type()) instanceof Type.ClassType
                || Pipelines.unwrapToOne(b) instanceof TypedNewInstance) {
            throw new NotImplementedException("concatenated navigation leaf '"
                    + leaf + "' of '" + target.classFqn()
                    + "' is class-typed — a further hop past the union is not"
                    + " supported yet");
        }
        if (!slotPrefixes.isEmpty()) {
            Map<String, String> slotOnly = new LinkedHashMap<>(slotPrefixes);
            slotOnly.keySet().removeAll(subNavs.keySet());
            if (!slotOnly.isEmpty()) {
                b = Pipelines.rewriteRowReads(b, target.rowVar(), slotOnly,
                        Set.of(), java.util.function.UnaryOperator.identity());
            }
        }
        return b;
    }

    private static TypedSpec requireLeaf(@com.legend.base.Nullable TypedSpec b,
            String classFqn, @com.legend.base.Nullable String via, String leaf) {
        if (b == null) {
            throw new MappingResolutionException("property '" + leaf
                    + "' of class '" + classFqn + "'"
                    + (via == null ? "" : " (through '" + via + "')")
                    + " is not mapped", classFqn);
        }
        return b;
    }

    /** Hop 0 of a branch: the association route, or the navigate-slot
     * route for a class-typed Join PM head (the corrNavHeads shape —
     * NavMaterializer target material + the slot's own predicate). */
    private Hop hop(TemporalFrame temporal, ClassSource cs, String h0,
            StoreResolver.Context context, Set<String> leaves0,
            Set<List<String>> navTails) {
        TypedSpec binding = cs.bindings().get(h0);
        if (binding == null) {
            AssociationJoins.AssocJoin aj = joins.associationJoin(temporal, cs,
                    h0, context, false, leaves0, h0, navTails);
            return new Hop(aj.target(), aj.targetPipeline(), aj.targetRow(),
                    java.util.Objects.requireNonNull(aj.condition(),
                            "association join without a condition"),
                    aj.targetSlotPrefixes(), aj.targetSubNavs());
        }
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String alias = InnerDemand.navSlotAlias(binding, cs.rowVar(),
                navSteps.keySet());
        if (alias == null || navMaterializer == null) {
            throw new NotImplementedException("concatenated navigation through"
                    + " '" + h0 + "' of '" + cs.classFqn()
                    + "' (an embedded / inline head) is not supported yet");
        }
        var nav = java.util.Objects.requireNonNull(navSteps.get(alias));
        if (!(nav.target() instanceof TypedGetAll tg)) {
            throw new NotImplementedException("concatenated navigation through"
                    + " '" + h0 + "' of '" + cs.classFqn()
                    + "' (a non-class navigate target) is not supported yet");
        }
        ClassSource target = sources.navTarget(cs, tg.classFqn(), nav, h0);
        NavMaterializer.NavMat mat = navMaterializer.navTargetMaterialized(
                temporal, target, cs.mappingFqn(), tg.classFqn(), cs.scope(),
                new ArrayList<>(navTails), h0, TemporalContext.NONE);
        TypedSpec tPipe = temporal.temporalTargetPipe(cs, target, h0,
                temporal.applyJoinTemporalFilters(mat.pipeline(), target,
                        Map.of()));
        return new Hop(target, tPipe,
                Type.requireRelationSchema(tPipe.info().type()), nav.predicate(),
                mat.slotPrefixes(), mat.subNavs());
    }

}
