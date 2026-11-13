package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A {@code sourceUrl('…')} external-data source (engine {@code TypedSourceUrl})
 * &mdash; a one-column relation of semi-structured rows,
 * {@code (data:Variant)[1]}, never the signature's {@code Relation<Any>}; or a
 * JSON connection's FRAME (B2a, 2026-11-11: its payload a {@code data:} source
 * the plan loads), which also reads each record's place, {@link #ORDINAL}.
 *
 * @param url  the source location literal
 * @param info {@code (data:Variant)[1]}, a frame's {@code (data:Variant,
 *             u_frame_ord__:Integer)[1]}
 * @param records whether the source is a JSON connection's frame: its rows
 *             the payload's records and their places, not an array's
 *             elements (the MIR's {@code SqlSource.SourceUrl.Form})
 */
public record TypedSourceUrl(String url, ExprType info, boolean records) implements TypedSpec {

    /** A frame's column of each record's place in its payload, from 0: the
     *  row identity two sets composed over one frame correlate on (never a
     *  class property, never a binding). The source's table holds it where
     *  the source reads it ({@code planner.DataSources}); the MIR's spelling
     *  of it is {@code SqlSource.SourceUrl.PLACE} (one name: DataSourcesRecordsTest). */
    public static final String ORDINAL = "u_frame_ord__";

    /** A {@code sourceUrl('…')}: its rows the payload array's elements. */
    public TypedSourceUrl(String url, ExprType info) {
        this(url, info, false);
    }

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
        return new TypedSourceUrl(url, info, records);
    }
}
