package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        TypedSpec pipelineFor(String targetClassFqn);
    }

    /** All navigate-step aliases in {@code pipeline} (class-typed Join PMs). */
    static Map<String, com.legend.compiler.spec.typed.TypedNavigate> navSteps(
            TypedSpec pipeline) {
        Map<String, com.legend.compiler.spec.typed.TypedNavigate> out = new LinkedHashMap<>();
        collectNavSteps(pipeline, out);
        return out;
    }

    private static void collectNavSteps(TypedSpec n,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNavigate nav
                && nav.alias().isPresent()) {
            out.put(nav.alias().get(), nav);
        }
        for (TypedSpec c : n.children()) {
            if (!(n instanceof com.legend.compiler.spec.typed.TypedNavigate nav)
                    || c == nav.source()) {   // only the chain spine
                collectNavSteps(c, out);
            }
        }
    }

    /** All slot aliases present in {@code pipeline}, in source order. */
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
                                java.util.function.UnaryOperator.identity())).toList(),
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
            case com.legend.compiler.spec.typed.TypedNavigate nav
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
                if (!(nav.target() instanceof com.legend.compiler.spec.typed.TypedGetAll ga)) {
                    throw new IllegalStateException("resolver bug: navigate step '"
                            + alias + "' target is "
                            + nav.target().getClass().getSimpleName()
                            + ", expected the class extent");
                }
                String prefix = alias + "_";
                prefixes.put(alias, prefix);
                TypedSpec targetPipeline = targets.pipelineFor(ga.classFqn());
                // The condition speaks (parent row, target TABLE row) — the
                // 4-arg emission; prior joinslot sub-row reads prefix.
                TypedLambda condLam = nav.predicate();
                String leftParam = condLam.parameters().get(0);
                TypedLambda cond = new TypedLambda(condLam.parameters(),
                        condLam.body().stream().map(b -> rewriteRowReads(
                                b, leftParam, prefixes, stripped,
                                java.util.function.UnaryOperator.identity())).toList(),
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
                boolean readsSlot = false;
                for (TypedSpec b : f.predicate().body()) {
                    if (referencesAliasOn(b, f.predicate().parameters().get(0),
                            slotAliasUniverse(stripped, prefixes))) {
                        readsSlot = true;
                        break;
                    }
                }
                if (readsSlot) {
                    throw new NotImplementedException("mapping ~filter for '"
                            + classFqn + "' reads through a join slot;"
                            + " join-mediated mapping filters are H3-pending");
                }
                yield new TypedFilter(src, f.predicate(), src.info());
            }
            // UNION pipelines: each concatenate branch materializes
            // INDEPENDENTLY (its projection's own slot reads are its demand)
            case com.legend.compiler.spec.typed.TypedConcatenate cc ->
                    new com.legend.compiler.spec.typed.TypedConcatenate(
                            walk(cc.left(), demanded, demandedNavs, targets,
                                    prefixes, stripped, classFqn),
                            walk(cc.right(), demanded, demandedNavs, targets,
                                    prefixes, stripped, classFqn),
                            cc.info());
            // a PROJECT over a slotted member pipeline: the colspec lambdas
            // demand their own slot reads; materialize the source with that
            // demand and rewrite the reads to the prefixed columns
            case com.legend.compiler.spec.typed.TypedProject pr
                    when containsSlot(pr.source()) || !navSteps(pr.source()).isEmpty() -> {
                Set<String> slotAliases = slotAliases(pr.source());
                Set<String> ownDemand = new LinkedHashSet<>();
                for (var col : pr.columns()) {
                    String rv = col.fn().parameters().get(0);
                    for (TypedSpec b : col.fn().body()) {
                        collectSlotReads(b, rv, slotAliases, ownDemand);
                    }
                }
                ownDemand = closeOverConditions(pr.source(), ownDemand);
                Map<String, String> branchPrefixes = new LinkedHashMap<>();
                Set<String> branchStripped = new LinkedHashSet<>();
                TypedSpec src = walk(pr.source(), ownDemand, Set.of(), targets,
                        branchPrefixes, branchStripped, classFqn);
                List<com.legend.compiler.spec.typed.TypedFuncCol> cols =
                        new java.util.ArrayList<>(pr.columns().size());
                for (var col : pr.columns()) {
                    String rv = col.fn().parameters().get(0);
                    List<TypedSpec> body = col.fn().body().stream()
                            .map(b -> rewriteRowReads(b, rv, branchPrefixes,
                                    branchStripped,
                                    java.util.function.UnaryOperator.identity()))
                            .toList();
                    cols.add(new com.legend.compiler.spec.typed.TypedFuncCol(
                            col.name(), new TypedLambda(col.fn().parameters(),
                                    body, col.fn().info())));
                }
                yield new com.legend.compiler.spec.typed.TypedProject(src, cols, pr.info());
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

    /** Slot aliases read through {@code rowVar} in {@code n} ($row.slot...). */
    private static void collectSlotReads(TypedSpec n, String rowVar,
            Set<String> slotAliases, Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(rowVar)
                && slotAliases.contains(pa.property())) {
            out.add(pa.property());
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
                                     java.util.function.UnaryOperator<TypedSpec> varRewrite) {
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
            case com.legend.compiler.spec.typed.TypedCollection c ->
                    new com.legend.compiler.spec.typed.TypedCollection(
                            c.elements().stream().map(e ->
                                    rewriteRowReads(e, rowVar, prefixes, stripped, varRewrite))
                                    .toList(), c.info());
            case com.legend.compiler.spec.typed.TypedIf i ->
                    new com.legend.compiler.spec.typed.TypedIf(
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
            case com.legend.compiler.spec.typed.TypedCString ignored -> n;
            case com.legend.compiler.spec.typed.TypedCInteger ignored -> n;
            case com.legend.compiler.spec.typed.TypedCFloat ignored -> n;
            case com.legend.compiler.spec.typed.TypedCDecimal ignored -> n;
            case com.legend.compiler.spec.typed.TypedCBoolean ignored -> n;
            case com.legend.compiler.spec.typed.TypedCDate ignored -> n;
            case com.legend.compiler.spec.typed.TypedEnumValue ignored -> n;
            // JSON/variant-source bindings are casts over variant reads:
            // to(get($row.data, 'k'), @T) — the cast rides, the reads
            // rewrite (plan §F12: substitution doesn't care).
            case com.legend.compiler.spec.typed.TypedCast c ->
                    new com.legend.compiler.spec.typed.TypedCast(
                            rewriteRowReads(c.source(), rowVar, prefixes, stripped, varRewrite),
                            c.target(), c.info());
            case com.legend.compiler.spec.typed.TypedTypeRef ignored -> n;
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
                                   java.util.function.UnaryOperator<TypedSpec> varRewrite) {
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
            case com.legend.compiler.spec.typed.TypedCollection c ->
                    new com.legend.compiler.spec.typed.TypedCollection(
                            c.elements().stream().map(e ->
                                    prefixColumns(e, rowVar, colPrefix, varRewrite)).toList(),
                            c.info());
            case com.legend.compiler.spec.typed.TypedIf i ->
                    new com.legend.compiler.spec.typed.TypedIf(
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
            case com.legend.compiler.spec.typed.TypedCast c ->
                    new com.legend.compiler.spec.typed.TypedCast(
                            prefixColumns(c.source(), rowVar, colPrefix, varRewrite),
                            c.target(), c.info());
            case com.legend.compiler.spec.typed.TypedCString ignored -> n;
            case com.legend.compiler.spec.typed.TypedCInteger ignored -> n;
            case com.legend.compiler.spec.typed.TypedCFloat ignored -> n;
            case com.legend.compiler.spec.typed.TypedCDecimal ignored -> n;
            case com.legend.compiler.spec.typed.TypedCBoolean ignored -> n;
            case com.legend.compiler.spec.typed.TypedCDate ignored -> n;
            case com.legend.compiler.spec.typed.TypedEnumValue ignored -> n;
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
