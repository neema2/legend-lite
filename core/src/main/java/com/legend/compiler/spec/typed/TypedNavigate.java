package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * The clean-sheet graph-traversal primitive {@code navigate}
 * (MAPPING_CLEAN_SHEET.md §3) &mdash; one node, three positions:
 *
 * <ul>
 *   <li>{@link Form#PRE_MAP} {@code rel->navigate(~firm: Firm.all(), {r,f|…})}
 *       &mdash; widens a Relation with a named class-typed sub-row; row count
 *       multiplies like {@code join}, the sub-row column is {@code [1]} per
 *       output row (§3.4). Result: {@code Relation<S + (alias:Target)>}.</li>
 *   <li>{@link Form#POST_MAP} {@code Class.all()->navigate(~slot: T.all(), {p,t|…})}
 *       &mdash; fills a DECLARED class property via an instance-space predicate;
 *       no instance multiplication. Result: the source {@code C[*]} unchanged.</li>
 *   <li>{@link Form#INLINE} {@code slot = navigate(T.all(), {t|…})} inside
 *       {@code ^Class(…)} &mdash; the constructor-slot sugar; multiplicity is
 *       collected per the slot's declaration. Result: {@code T[*]}.</li>
 * </ul>
 *
 * @param source    the relation / class collection being widened (the target
 *                  extent itself for the inline form)
 * @param alias     the sub-row / property-slot name; empty for the inline form
 * @param target    the navigated class extent (the colspec thunk's body); for
 *                  the inline form this IS {@link #source}
 * @param predicate the checked navigation predicate
 * @param pairedPredicate the STRICT member-paired variant of a MERGED union
 *                  navigate condition. The engine's two subsystems disagree
 *                  on diagonal union routes: its relational path MERGES
 *                  (partiallyMilestoning golden cross-matches by key value)
 *                  while its graph executor pairs strictly per member
 *                  (rootLevel SameStore golden serializes null). The merged
 *                  form is {@link #predicate}; graph children consult this
 *                  variant when present. Traversable via
 *                  {@link #children()} (callee collection and walks see it);
 *                  never lowered — GraphEmission reads it at the same
 *                  raw-pipeline point it reads the predicate, and its
 *                  suffixed reads reference frame columns both unions
 *                  always project.
 * @param form      which of the three positions this navigate occupies
 * @param info      the result per the form's rule above
 */
public record TypedNavigate(TypedSpec source, Optional<String> alias, TypedSpec target,
                            TypedLambda predicate, Optional<TypedLambda> pairedPredicate,
                            @com.legend.base.Nullable String frameName, Form form, ExprType info,
                            List<Route> routes) implements TypedSpec {

    /** ONE ROUTE of a several-route legacy navigate (legacy routes as
     * composition): the target set as its own function ({@code target}: a
     * user call, or a class extent), the rows the author's condition reads,
     * the condition as written over (source row, target row), the target
     * columns that condition reads (in order) and the union-row key names
     * they project under — minted by the checker, shared by routes of one
     * shape. The node's {@link #predicate} is the OR of the routes'
     * conditions re-pointed at the union row; the resolver builds that
     * union from the routes ({@code ClassSources.routedUnionSource}). */
    public record Route(TypedSpec target, TypedSpec rows, TypedLambda cond,
                        List<String> targetReads, List<String> keyNames) {
        public Route {
            targetReads = List.copyOf(targetReads);
            keyNames = List.copyOf(keyNames);
        }
    }

    public TypedNavigate {
        routes = List.copyOf(routes);
    }

    public TypedNavigate(TypedSpec source, Optional<String> alias, TypedSpec target,
                         TypedLambda predicate, Optional<TypedLambda> pairedPredicate,
                         @com.legend.base.Nullable String frameName, Form form, ExprType info) {
        this(source, alias, target, predicate, pairedPredicate, frameName, form, info, List.of());
    }

    /** frameName: the target's derived-table identity (a VIEW-backed
     * navigate's view name; the ColSpec alias-metadata channel, exactly
     * the TypedJoinSlot precedent) — null for physical-table targets.
     * The nav-step materialization threads it onto its TypedJoin. */
    public TypedNavigate(TypedSpec source, Optional<String> alias, TypedSpec target,
                         TypedLambda predicate, Optional<TypedLambda> pairedPredicate,
                         Form form, ExprType info) {
        this(source, alias, target, predicate, pairedPredicate, null, form, info);
    }

    /** The common form: no paired variant. */
    public TypedNavigate(TypedSpec source, Optional<String> alias, TypedSpec target,
                         TypedLambda predicate, Form form, ExprType info) {
        this(source, alias, target, predicate, Optional.empty(), null, form, info);
    }

    /** The three syntactic positions of §3 — one conceptual primitive. */
    public enum Form {
        PRE_MAP, POST_MAP, INLINE
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new java.util.ArrayList<>(form == Form.INLINE
                ? List.of(source, predicate)
                : List.of(source, target, predicate));
        pairedPredicate.ifPresent(out::add);
        // every route's target (a set's function call), rows and condition
        // are expression children: callee collection and walks see them
        for (Route r : routes) {
            out.add(r.target());
            out.add(r.rows());
            out.add(r.cond());
        }
        return out;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        int base = form == Form.INLINE ? 2 : 3;
        int paired = pairedPredicate.isPresent() ? 1 : 0;
        TypedSpec.expectChildren(kids, base + paired + 3 * routes.size(), "TypedNavigate");
        TypedSpec tgt = form == Form.INLINE ? target : kids.get(1);
        TypedLambda pred = (TypedLambda) kids.get(form == Form.INLINE ? 1 : 2);
        java.util.Optional<TypedLambda> pairedPred = pairedPredicate.isPresent()
                ? java.util.Optional.of((TypedLambda) kids.get(base))
                : java.util.Optional.empty();
        List<Route> rs = new java.util.ArrayList<>(routes.size());
        for (int i = 0; i < routes.size(); i++) {
            int at = base + paired + 3 * i;
            Route r = routes.get(i);
            rs.add(new Route(kids.get(at), kids.get(at + 1), (TypedLambda) kids.get(at + 2),
                    r.targetReads(), r.keyNames()));
        }
        return new TypedNavigate(kids.get(0), alias, tgt, pred, pairedPred,
                frameName, form, info, rs);
    }
    /** The same step over another source (its routes, alias, target,
     * predicates and frame kept) — the ONE way a walker rebuilds a step. */
    public TypedNavigate withSource(TypedSpec src, ExprType newInfo) {
        return new TypedNavigate(src, alias, target, predicate, pairedPredicate, frameName, form, newInfo, routes);
    }

    /** The same step with another predicate. */
    public TypedNavigate withPredicate(TypedLambda pred) {
        return new TypedNavigate(source, alias, target, pred, pairedPredicate, frameName, form, info, routes);
    }

    /** The same step over another source and with another predicate. */
    public TypedNavigate withSourceAndPredicate(TypedSpec src, TypedLambda pred, ExprType newInfo) {
        return new TypedNavigate(src, alias, target, pred, pairedPredicate, frameName, form, newInfo, routes);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedNavigate(source, alias, target, predicate, pairedPredicate, frameName, form, info, routes);
    }
}
