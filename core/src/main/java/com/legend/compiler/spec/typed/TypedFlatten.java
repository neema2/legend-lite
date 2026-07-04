package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A relation {@code flatten(~col)} (engine {@code TypedFlatten}) &mdash; unnests
 * a semi-structured column: one output row per element, the source schema
 * preserved with {@code column} widened to {@code Variant} (engine-lite's
 * behavior; real Pure's single-column {@code Relation<Z>} signature is a
 * documented divergence the checker carries engine-side).
 *
 * @param source the relation being unnested
 * @param column the flattened column (validated against the source schema)
 * @param info   the source schema with {@code column} &rarr; {@code Variant}, {@code [1]}
 */
public record TypedFlatten(TypedSpec source, String column, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }
}
