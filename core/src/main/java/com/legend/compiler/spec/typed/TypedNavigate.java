package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

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
 * @param form      which of the three positions this navigate occupies
 * @param info      the result per the form's rule above
 */
public record TypedNavigate(TypedSpec source, Optional<String> alias, TypedSpec target,
                            TypedLambda predicate, Form form, ExprType info) implements TypedSpec {

    /** The three syntactic positions of §3 — one conceptual primitive. */
    public enum Form {
        PRE_MAP, POST_MAP, INLINE
    }

    @Override
    public List<TypedSpec> children() {
        return form == Form.INLINE
                ? List.of(source, predicate)
                : List.of(source, target, predicate);
    }
}
