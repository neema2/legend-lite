package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DOTTED emptiness ({@code $p.a.b->exists(...)}, {@code isNotEmpty($p.a.b)}):
 * the leaf of a navigation path consumed under an emptiness call
 * registers a correlated EXISTS material under the dotted path, at the
 * ROOT scope and inside every nested scope alike (a scope is a scope —
 * its predicates navigate the same depth; navigation-depth leg,
 * 2026-09-02). Extracted from StoreResolver (file-size guardrail); the
 * nested-scope builder rides in as a function, like ChainedExists.
 */
final class DottedExists {

    /** The resolver's nested-scope builder. */
    interface Scoper {
        StoreResolver.NestedScope scope(ClassSource t, List<TypedSpec> ops,
                List<String> path, StoreResolver.Context context, TypedSpec pipe);
    }

    private final ClassSources sources;
    private final AssociationJoins assocMaterial;
    private final ModelContext ctx;
    private final SyntheticHeads synthetics;

    DottedExists(ClassSources sources, AssociationJoins assocMaterial,
            ModelContext ctx, SyntheticHeads synthetics) {
        this.sources = sources;
        this.assocMaterial = assocMaterial;
        this.ctx = ctx;
        this.synthetics = synthetics;
    }

    /** The emptiness-consumed navigation paths of a scope: its ops'
     * predicates/sort keys, plus the TERMINAL's lambdas at the root scope
     * (a nested scope has no terminal — its paths are its ops' alone). */
    record EmptinessPaths(Set<List<String>> chain,
                                  Set<List<String>> nested) {
        static EmptinessPaths of(List<TypedSpec> ops,
                List<TypedLambda> terminalLambdas) {
            Set<List<String>> chain = new LinkedHashSet<>();
            Set<List<String>> nested = new LinkedHashSet<>();
            for (TypedSpec op : ops) {
                if (op instanceof TypedFilter f) {
                    for (TypedSpec b : f.predicate().body()) {
                        InnerDemand.collectEmptinessChainPaths(b,
                                f.predicate().parameters().get(0), chain, nested);
                    }
                }
                if (op instanceof TypedSortBy sb) {
                    for (TypedSpec b : sb.key().body()) {
                        InnerDemand.collectEmptinessChainPaths(b,
                                sb.key().parameters().get(0), chain, nested);
                    }
                }
            }
            for (TypedLambda fn : terminalLambdas) {
                for (TypedSpec b : fn.body()) {
                    InnerDemand.collectEmptinessChainPaths(b,
                            fn.parameters().get(0), chain, nested);
                }
            }
            return new EmptinessPaths(chain, nested);
        }
    }

    void register(TemporalFrame temporal, ClassSource cs,
            List<TypedSpec> ops, EmptinessPaths paths,
            StoreResolver.Context context,
            Map<String, Substitution.AssocSub> assocs,
            Map<String, Substitution.ExistsSub> existsSubs, Scoper nestedScope) {
        // 2a''. CLASS-TYPED LEAF under an emptiness call — isNotEmpty(
        // $p.a.b) where b is itself a navigation step on the CHAIN TARGET:
        // correlated EXISTS on the exploded chain row (engine: semi-join +
        // key null check — row-per-row identical truth value). The leaf's
        // nav material registers under the DOTTED path; the oriented
        // condition's parent-side reads PRE-PREFIX onto the chain's joined
        // columns so the exists rewrite's plain var swap lands them on the
        // row. Registered after join-key collection: these conditions read
        // CHAIN columns, never root keys. ONLY paths actually under an
        // emptiness call register — eager registration stamped temporal
        // context on chains the ordinary (inheritance-aware) route owns
        // (regressed testBiTemporalToBiTemporalDatePropagation).
        Set<List<String>> emptinessChainPaths = paths.chain();
        Set<List<String>> nestedEmptinessPaths = paths.nested();
        Set<List<String>> allEmptinessPaths =
                new LinkedHashSet<>(emptinessChainPaths);
        allEmptinessPaths.addAll(nestedEmptinessPaths);
        for (List<String> path : allEmptinessPaths) {
            if (path.size() < 2) {
                continue;
            }
            String dotted = String.join(".", path);
            Substitution.AssocSub chain = assocs.get(
                    String.join(".", path.subList(0, path.size() - 1)));
            if (existsSubs.containsKey(dotted)) {
                continue;
            }
            // NESTED-EXISTS rung: an emptiness consumed INSIDE another
            // exists' predicate lives in the enclosing SUBSELECT's scope —
            // a to-many mid hop there must NOT correlate through the flat
            // outer join (that form is per-FANNED-row, the engine's truth
            // for DIRECT filter-position emptiness — testIsEmptyNested's
            // golden — but wrong inside a subselect where the fanned row
            // is not the evaluation row); the EXISTS material carries the
            // whole exploded chain itself (ChainedExists; engine: every
            // hop's join-tree node lives inside the exists subselect).
            boolean midToMany = path.size() == 2
                    && !(ctx.findProperty(cs.classFqn(),
                            SyntheticHeads.realHead(path.get(0)))
                            .map(pr -> pr.multiplicity())
                            .filter(mm -> mm instanceof com.legend.compiler
                                    .element.type.Multiplicity.Bounded bb
                                    && Integer.valueOf(1).equals(bb.upper()))
                            .isPresent());
            if (midToMany && nestedEmptinessPaths.contains(path)
                    && !emptinessChainPaths.contains(path)
                    && !synthetics.hasPred(path.get(0))
                    && !synthetics.hasPred(dotted)) {
                Substitution.ExistsSub two = ChainedExists.explodedTwoHop(
                        sources, temporal, cs, path, ops,
                        (t2, pipe2) -> nestedScope.scope(t2, ops, path,
                                context, pipe2));
                if (two != null) {
                    existsSubs.put(dotted, two);
                    continue;
                }
            }
            if (chain == null) {
                continue;
            }
            String leafName = path.get(path.size() - 1);
            String leaf = SyntheticHeads.realHead(leafName);
            ClassSource parent = sources.get(cs.mappingFqn(), chain.targetClassFqn(), cs.scope());
            TypedLambda cond;
            TypedSpec tPipe;
            ClassSource t;
            Map<String, String> tPrefixes;
            TypedSpec leafBinding = parent.bindings().get(leaf);
            if (leafBinding != null) {
                TypedSpec inner = leafBinding;
                if (inner instanceof TypedNativeCall c1 && c1.args().size() == 1
                        && com.legend.builtin.Pure.isToOneCall(c1.callee().qualifiedName())) {
                    inner = c1.args().get(0);
                }
                var pNavSteps = Pipelines.navSteps(parent.pipeline());
                String alias = InnerDemand.navSlotAlias(inner, parent.rowVar(),
                        pNavSteps.keySet());
                var nav = alias == null ? null : pNavSteps.get(alias);
                if (nav == null || !(nav.target()
                        instanceof TypedGetAll tg)
                        || !sources.binds(cs.mappingFqn(), tg.classFqn())) {
                    continue;
                }
                t = sources.navTarget(cs, tg.classFqn(), nav, java.util.Objects.requireNonNull(alias));
                Pipelines.Materialized tm = Pipelines.materialize(
                        t.pipeline(), Set.of(), t.classFqn());
                TypedSpec p0 = tm.pipeline();
                cond = nav.predicate();
                if (cond.parameters().size() == 2) {
                    Set<String> tgtReads = new LinkedHashSet<>();
                    for (TypedSpec b0 : cond.body()) {
                        Pipelines.collectVarReads(b0,
                                cond.parameters().get(1), tgtReads);
                    }
                    p0 = Pipelines.widenConcatenateForKeys(p0, tgtReads);
                }
                tPipe = temporal.temporalTargetPipe(parent, t, dotted,
                        temporal.applyJoinTemporalFilters(p0, t, Map.of()));
                tPrefixes = tm.slotPrefixes();
                final TypedSpec tp = tPipe;
                final var tpx = tPrefixes;
                final ClassSource tt = t;
                StoreResolver.requireNoCorrelatedPred(synthetics, leafName, "exists-navigation");
                tPipe = synthetics.applyToPipe(leafName, tp, (p, pred) ->
                        CorrelatedSubselects.predFilteredPipe(p, tt, tpx, pred, cs.mappingFqn()));
            } else if (ctx.findAssociationOf(parent.classFqn(), leaf)
                    .isPresent()) {
                // the nested predicate's path HEADS demand target slots —
                // without them, a slot-backed read inside the exists
                // predicate ($e.address.name) finds an unmaterialized slot
                Set<String> innerDemand = new LinkedHashSet<>();
                for (TypedLambda il : InnerDemand.lambdas(ops, path)) {
                    if (!il.parameters().isEmpty()) {
                        InnerDemand.collectParamPathHeads(il, il.parameters().get(0),
                                innerDemand);
                    }
                }
                AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, parent, leafName, context,
                        true, innerDemand, dotted);
                t = aj.target();
                cond = aj.condition();
                tPipe = aj.targetPipeline();
                tPrefixes = aj.targetSlotPrefixes();
            } else {
                continue;
            }
            // audit 23 #75: a MISSING property must not silently default
            // to to-many semantics — the demand scan and G disagree
            if (ctx.findProperty(parent.classFqn(), leaf).isEmpty()) {
                throw new IllegalStateException("resolver bug: exists-leaf"
                        + " property '" + leaf + "' is not declared on '"
                        + parent.classFqn() + "' — G admitted a read the"
                        + " model does not carry");
            }
            boolean leafToMany = !(ctx.findProperty(parent.classFqn(), leaf)
                    .map(pr -> pr.multiplicity())
                    .filter(mm -> mm instanceof com.legend.compiler.element.type
                            .Multiplicity.Bounded bb
                            && Integer.valueOf(1).equals(bb.upper()))
                    .isPresent());
            if (chain.readVar() != null) {
                throw new IllegalStateException("resolver bug: dotted-path"
                        + " EXISTS registration over a read-var AssocSub —"
                        + " the chain prefix pre-prefixing assumes joined-row"
                        + " reads (audit 14 B-F7)");
            }
            String pv = java.util.Objects.requireNonNull(cond, "cond").parameters().get(0);
            TypedSpec cbody = Pipelines.prefixColumns(
                    cond.body().get(cond.body().size() - 1), pv,
                    chain.prefix(), v -> v);
            TypedLambda chainedCond = new TypedLambda(cond.parameters(),
                    List.of(cbody), cond.info());
            StoreResolver.NestedScope dns = nestedScope.scope(t, ops, path, context, tPipe);
            existsSubs.put(dotted, new Substitution.ExistsSub(dns.pipeline(),
                    chainedCond, t.rowVar(), t.bindings(),
                    dns.row(),
                    t.classFqn(), Pipelines.slotAliases(t.pipeline()),
                    tPrefixes, leafToMany)
                    .withInnerRegs(dns.regs()));
        }

    }
}
