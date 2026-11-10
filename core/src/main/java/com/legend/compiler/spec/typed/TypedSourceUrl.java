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

    /** The hidden row-ordinal column a JSON source frame's relation carries beside {@code data} (the resolver's
     *  {@code JsonSourceFrame}): each element's place in the payload, the frame's row identity -- two sets composed
     *  over the same frame correlate on it. The payload's session table holds it where the relation reads it
     *  ({@code planner.DataSources}, A5). */
    public static final String ORDINAL = "u_frame_ord__";

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedSourceUrl");
        return this;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedSourceUrl(url, info);
    }
}
