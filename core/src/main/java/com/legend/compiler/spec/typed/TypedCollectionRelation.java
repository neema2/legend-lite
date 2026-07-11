package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * {@code relation::variant::flatten(collection, ~col)} &mdash; a scalar or
 * variant COLLECTION materialized as a ONE-COLUMN relation (real
 * flatten.pure). Lowers to {@code SELECT UNNEST(value) AS col}.
 *
 * @param value  the collection expression (scalar position)
 * @param column the single output column's name
 * @param info   the {@code Relation<(col:T)>} schema
 */
public record TypedCollectionRelation(TypedSpec value, String column, ExprType info)
        implements TypedSpec {

    @Override
    public List<TypedSpec> children() {
        return List.of(value);
    }
}
