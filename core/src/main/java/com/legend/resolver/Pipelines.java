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
final class Pipelines {

    /** One {@code toOne()} look-through — the multiplicity coercion is
     * transparent to structure (audit 23: this idiom had ~12 hand copies;
     * new code calls THIS). Returns the argument when {@code n} is
     * {@code toOne(x)}, else {@code n} unchanged. */
    static TypedSpec unwrapToOne(TypedSpec n) {
        return n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::multiplicity::toOne")
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
    }

    /** All navigate-step aliases in {@code pipeline} (class-typed Join PMs). */
    static Map<String, TypedNavigate> navSteps(
            TypedSpec pipeline) {
        Map<String, TypedNavigate> out = new LinkedHashMap<>();
        collectNavSteps(pipeline, out);
        return out;
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

    static Materialized materialize(TypedSpec pipeline, Set<String> demanded,
                                    String classFqn) {
        return materialize(pipeline, demanded, Set.of(), classFqn, null);
    }

    static Materialized materialize(TypedSpec pipeline, Set<String> demanded,
                                    Set<String> demandedNavs, String classFqn,
                                    TargetResolver targets) {
        Set<String> all = slotAliases(pipeline);
        if (all.isEmpty() && navSteps(pipeline).isEmpty()) {
            return new Materialized(pipeline, Map.of(), Set.of());
        }
        Map<String, String> prefixes = new LinkedHashMap<>();
        Set<String> stripped = new LinkedHashSet<>();
        TypedSpec out = walk(pipeline, demanded, demandedNavs, targets,
                prefixes, stripped, classFqn);
        return new Materialized(out, prefixes, stripped);
    }

    private static TypedSpec walk(TypedSpec n, Set<String> demanded,
                                  Set<String> demandedNavs, TargetResolver targets,
                                  Map<String, String> prefixes, Set<String> stripped,
                                  String classFqn) {
        return switch (n) {
            case TypedJoinSlot js -> {
                if (containsSlot(js.target())) {
                    throw new IllegalStateException("resolver bug: join slot '"
                            + js.alias() + "' carries a nested slot in its target;"
                            + " the normalizer emits linear chains only");
                }
                TypedSpec left = walk(js.source(), demanded, demandedNavs, targets, prefixes, stripped, classFqn);
                if (!demanded.contains(js.alias())) {
                    stripped.add(js.alias());
                    yield left;   // JOIN CANCELLED: nothing reads through it
                }
                String prefix = js.alias() + "_";
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
                Type.RelationType leftRow = (Type.RelationType) left.info().type();
                Type.RelationType rightRow = (Type.RelationType) js.target().info().type();
                List<Type.Column> cols = new ArrayList<>(leftRow.columns());
                for (Type.Column c : rightRow.columns()) {
                    cols.add(new Type.Column(prefix + c.name(), c.type(), c.multiplicity()));
                }
                yield new TypedJoin(left, js.target(),
                        new TypedEnumValue(JOIN_KIND_FQN, "LEFT",
                                new ExprType(new Type.EnumType(JOIN_KIND_FQN),
                                        Multiplicity.Bounded.ONE)),
                        cond, Optional.of(prefix),
                        new ExprType(new Type.RelationType(cols), Multiplicity.Bounded.ONE));
            }
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
                String prefix = alias + "_";
                prefixes.put(alias, prefix);
                TypedSpec targetPipeline = targets.pipelineFor(alias, ga.classFqn());
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
                Type.RelationType leftRow = (Type.RelationType) left.info().type();
                Type.RelationType rightRow =
                        (Type.RelationType) targetPipeline.info().type();
                List<Type.Column> cols = new ArrayList<>(leftRow.columns());
                for (Type.Column c : rightRow.columns()) {
                    cols.add(new Type.Column(prefix + c.name(), c.type(), c.multiplicity()));
                }
                yield new TypedJoin(left, targetPipeline,
                        new TypedEnumValue(JOIN_KIND_FQN, "LEFT",
                                new ExprType(new Type.EnumType(JOIN_KIND_FQN),
                                        Multiplicity.Bounded.ONE)),
                        cond, Optional.of(prefix),
                        new ExprType(new Type.RelationType(cols), Multiplicity.Bounded.ONE));
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
                                    body, col.fn().info())));
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
                    when containsSlot(d.source()) -> {
                Set<String> prefixesBefore = new LinkedHashSet<>(prefixes.values());
                TypedSpec src = walk(d.source(), scalarSlotAliases(d.source()),
                        demandedNavs, targets, prefixes, stripped, classFqn);
                Type.RelationType row = (Type.RelationType) src.info().type();
                if (d.columns() != null && !d.columns().isEmpty()) {
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
                            cols, new ExprType(new Type.RelationType(outCols),
                                    Multiplicity.Bounded.ONE));
                }
                // WHOLE-ROW distinct (empty column list -> DISTINCT *):
                // dedup exactly what the source PROJECTS — naming the row
                // type's columns would reference ones a milestoned scan
                // does not project.
                yield new TypedDistinct(src,
                        List.of(), new ExprType(row, Multiplicity.Bounded.ONE));
            }
            default -> {
                if (containsSlot(n)) {
                    throw new NotImplementedException("mapping pipeline for '"
                            + classFqn + "' has " + n.getClass().getSimpleName()
                            + " above join slot(s); H3-pending");
                }
                yield n;
            }
        };
    }

    /**
     * View ~groupBy above slots (AccountPnl): the groupBy's key/agg
     * lambdas are the only consumers of those slots — demand their own
     * reads, materialize the source in a branch scope, rewrite the reads
     * to the prefixed columns (mirror of the project-over-slots arm).
     */
    private static TypedSpec groupByOverSlots(TypedGroupBy g,
            TargetResolver targets, String classFqn) {
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
                    a.reduce(), a.orderKey(), a.orderAsc()));
        }
        return new TypedGroupBy(gSrc, gKeys, gAggs, g.info());
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
        if (d.columns() != null && !d.columns().isEmpty()
                && !(d.source() instanceof TypedSelect)) {
            Type.RelationType srow = (Type.RelationType) d.source().info().type();
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
                    new ExprType(new Type.RelationType(outCols),
                            Multiplicity.Bounded.ONE)));
        }
        if (!(d.source() instanceof TypedSelect sel)) {
            return pipeline;
        }
        Type.RelationType selRow = (Type.RelationType) sel.info().type();
        Set<String> have = new LinkedHashSet<>();
        for (Type.Column c : selRow.columns()) {
            have.add(c.name());
        }
        Type.RelationType srcRow = (Type.RelationType) sel.source().info().type();
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
        ExprType row = new ExprType(new Type.RelationType(newRowCols),
                Multiplicity.Bounded.ONE);
        TypedSpec ns = new TypedSelect(
                sel.source(), newCols, row);
        return rewrap.apply(new TypedDistinct(
                ns, d.columns() == null || d.columns().isEmpty() ? d.columns()
                        : newCols, row));
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
    static TypedSpec widenConcatenateForKeys(TypedSpec pipeline, Set<String> cols) {
        if (pipeline instanceof TypedFilter f) {
            TypedSpec inner = widenConcatenateForKeys(f.source(), cols);
            if (inner == f.source()) {
                return pipeline;
            }
            return new TypedFilter(inner, f.predicate(),
                    new ExprType(inner.info().type(), Multiplicity.Bounded.ONE));
        }
        if (!(pipeline instanceof TypedConcatenate cat)) {
            return pipeline;
        }
        Type.RelationType row = (Type.RelationType) cat.info().type();
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
        if (!(side instanceof TypedProject p)) {
            throw new NotImplementedException(
                    "a navigation join over this union demands key columns "
                    + missing + ", but a union member is a "
                    + side.getClass().getSimpleName()
                    + " — only projected members widen");
        }
        Type.RelationType srcRow = (Type.RelationType) p.source().info().type();
        List<TypedFuncCol> newCols =
                new ArrayList<>(p.columns());
        List<Type.Column> outCols = new ArrayList<>(
                ((Type.RelationType) p.info().type()).columns());
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
                Type sibling = null;
                for (TypedSpec m : members) {
                    if (m.info().type() instanceof Type.RelationType mr) {
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
                new ExprType(new Type.RelationType(outCols),
                        Multiplicity.Bounded.ONE));
    }

    private static Type.Column columnOf(Type.RelationType row, String name) {
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

    private static Set<String> slotAliasUniverse(Set<String> stripped,
                                                 Map<String, String> prefixes) {
        Set<String> all = new LinkedHashSet<>(stripped);
        all.addAll(prefixes.keySet());
        return all;
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
            case TypedNativeCall c -> new TypedNativeCall(c.callee(),
                    c.args().stream().map(a ->
                            rewriteRowReads(a, rowVar, prefixes, stripped, varRewrite))
                            .toList(), c.info());
            case TypedCollection c ->
                    new TypedCollection(
                            c.elements().stream().map(e ->
                                    rewriteRowReads(e, rowVar, prefixes, stripped, varRewrite))
                                    .toList(), c.info());
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
                            c.target(), c.info());
            case TypedTypeRef ignored -> n;
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
            case TypedNativeCall c -> new TypedNativeCall(c.callee(),
                    c.args().stream().map(a -> prefixColumns(a, rowVar, colPrefix, varRewrite))
                            .toList(), c.info());
            case TypedCollection c ->
                    new TypedCollection(
                            c.elements().stream().map(e ->
                                    prefixColumns(e, rowVar, colPrefix, varRewrite)).toList(),
                            c.info());
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
                            c.target(), c.info());
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

    private static boolean containsSlot(TypedSpec n) {
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
}
