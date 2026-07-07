package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A {@code sourceUrl('…')} external-data source (engine {@code TypedSourceUrl})
 * &mdash; a one-column relation of semi-structured rows,
 * {@code (data:Variant)[1]}, never the signature's {@code Relation<Any>}.
 *
 * @param url  the source location literal
 * @param info {@code (data:Variant)[1]}
 */
public record TypedSourceUrl(String url, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
