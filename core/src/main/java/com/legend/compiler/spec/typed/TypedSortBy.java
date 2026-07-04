package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A collection {@code sortBy(key)} / {@code sortByReversed(key)} (engine
 * {@code SortChecker.checkCollectionSortBy}) &mdash; a fixed-direction sort by a
 * key-extraction lambda: {@code sortBy<T,U|m>(col:T[m], key:{T[1]->U[1]}[0..1]):T[m]}.
 * Checked generically; the direction is the construct's identity, stamped here
 * for lowering.
 *
 * @param source    the collection being sorted
 * @param key       the key-extraction lambda
 * @param ascending {@code true} for {@code sortBy}, {@code false} for {@code sortByReversed}
 * @param info      the source type unchanged ({@code T[m]})
 */
public record TypedSortBy(TypedSpec source, TypedLambda key, boolean ascending,
                          ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source, key);
    }
}
