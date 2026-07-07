package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type used as a value &mdash; the checked form of a {@code @Type} annotation
 * ({@code @Integer}, {@code @Relation<(name:String)>}). Typed as a
 * <em>prototype value of the target type itself</em> ({@code ExprType(target, [1])}),
 * exactly real Pure's convention: {@code cast<T|m>(source:Any[m], type:T[1]):T[m]}
 * takes the target as a {@code T[1]} argument, so the generic path binds {@code T}
 * from this value and {@code cast}/{@code to}/{@code toMany} need no bespoke rule.
 *
 * @param target the resolved target type (a relation target is the bare row-struct, G-&alpha;)
 * @param info   {@code target[1]}
 */
public record TypedTypeRef(Type target, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
