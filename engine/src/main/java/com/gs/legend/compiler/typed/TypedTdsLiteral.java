package com.gs.legend.compiler.typed;

import com.gs.legend.ast.TdsLiteral;
import com.gs.legend.compiler.ExpressionType;

/**
 * Inline TDS (tabular data set) literal: Pure's {@code #TDS col1, col2\nv1, v2\n#}
 * syntax. Compiled to a VALUES clause in SQL.
 *
 * <p>Carries the original parsed {@link TdsLiteral} (columns + rows) unchanged;
 * per-cell types are inferred / resolved and the relation's schema is in
 * {@link #info()} via {@code Type.Relation(schema)}.
 */
public record TypedTdsLiteral(
        TdsLiteral data,
        ExpressionType info
) implements TypedSpec {}
