package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

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
                          @com.legend.base.Nullable String keyAlias, ExprType info) implements TypedSpec {

    /** The common alias-less sort ({@code keyAlias} is the engine's
     * {@code !alias} path suffix — a sortBy path key materializes as the
     * root form's {@code o_<alias>} column). */
    public TypedSortBy(TypedSpec source, TypedLambda key, boolean ascending,
            ExprType info) {
        this(source, key, ascending, null, info);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source, key);
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 2, "TypedSortBy");
        return new TypedSortBy(kids.get(0), (TypedLambda) kids.get(1), ascending, keyAlias, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSortBy(source, key, ascending, keyAlias, info);
    }
}
