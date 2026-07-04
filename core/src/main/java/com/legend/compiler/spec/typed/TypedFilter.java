package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked {@code filter} (engine {@code TypedFilter}) &mdash; one node for
 * both overloads, {@code filter<T>(Relation<T>[1], {T[1]->Boolean[1]}):Relation<T>[1]}
 * and the collection {@code filter<T>(T[*], {T[1]->Boolean[1]}):T[*]}; lowering
 * disambiguates by the source's type. Filter only removes rows/elements, so
 * {@link #info()} is the source type unchanged (checked generically against the
 * registered signature; this node is <em>emission</em>, not a bespoke rule).
 *
 * @param source    the relation or collection being filtered
 * @param predicate the boolean row/element predicate
 * @param info      the result type &mdash; the source's, from the signature's {@code Relation<T>}/{@code T[*]} return
 */
public record TypedFilter(TypedSpec source, TypedLambda predicate, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source, predicate);
    }
}
