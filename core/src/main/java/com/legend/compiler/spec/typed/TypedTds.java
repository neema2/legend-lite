package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * An inline TDS literal {@code #TDS col:Type, … / rows #} (engine
 * {@code TypedTdsLiteral}) &mdash; a relation <em>source</em> whose schema comes
 * from the header (explicit {@code col:Type} annotations, or inference from the
 * first data row), never the signature's {@code Relation<Any>}. The column
 * types live on {@link #info()} (the resolved row-struct); the raw cell grid is
 * carried for lowering (a SQL {@code VALUES} clause).
 *
 * @param rows the data rows as raw cell text, in source order
 * @param info the parsed schema, {@code (col:Type, …)[1]}
 */
public record TypedTds(List<List<String>> rows, ExprType info) implements TypedSpec {
    public TypedTds {
        rows = rows.stream().map(List::copyOf).toList();
    }

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
