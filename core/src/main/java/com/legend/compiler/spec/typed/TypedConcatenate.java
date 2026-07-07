package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked relation {@code concatenate} (SQL {@code UNION ALL}) &mdash;
 * {@code concatenate<T>(rel1, rel2):Relation<T>[1]}: both sides must carry the
 * <em>same</em> schema (the shared {@code T} enforces it in unification).
 *
 * @param left  the first relation
 * @param right the second relation
 * @param info  the shared schema
 */
public record TypedConcatenate(TypedSpec left, TypedSpec right, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(left, right);
    }
}
