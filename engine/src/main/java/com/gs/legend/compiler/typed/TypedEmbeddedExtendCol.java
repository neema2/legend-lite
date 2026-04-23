package com.gs.legend.compiler.typed;

import java.util.List;

/**
 * Embedded extend: a parent-class property that groups sub-properties whose values
 * live on physical columns of the parent's table (no JOIN). Produced by
 * {@link com.gs.legend.compiler.checkers.ExtendChecker} for synthesized sourceSpec
 * extend clauses of the form
 * {@code ~propName:{-> ~[sub1:r|$r.COL1, sub2:r|$r.COL2, ...]}} where the 0-param
 * outer lambda and the inner ColSpecArray are first-class, not a raw-AST pattern.
 *
 * <p>Unlike {@link TypedScalarExtendCol}, embedded extends do not add a column to
 * the source schema — they declare a logical sub-object that {@code MappingResolver}
 * lowers to a {@link com.gs.legend.compiler.StoreResolution.JoinResolution} with
 * {@code embedded=true}, and {@code PlanGenerator} projects via direct parent-row
 * column references.
 *
 * @param alias          Parent property name that groups the sub-properties.
 * @param subColumns     Ordered sub-property → physical column on the parent table.
 */
public record TypedEmbeddedExtendCol(
        String alias,
        List<Sub> subColumns
) implements TypedExtendCol {
    /** One embedded sub-property → physical column on the parent's table. */
    public record Sub(String propertyName, String columnName) {}
}
