package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedFlatten;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;

/**
 * {@code source->flatten(~col)} — expand a JSON-array-valued column into
 * one row per element.
 *
 * <p>Lowers to {@link SqlRelation.SelectExcept} with the original column
 * dropped and an {@code UNNEST(CAST(col AS JSON[]))} addition aliased to
 * the same name, producing the DuckDB form:
 * <pre>
 *   SELECT * EXCLUDE ("col"), UNNEST(CAST("col" AS JSON[])) AS "col"
 *   FROM (source) AS t
 * </pre>
 *
 * <p>Mirrors the legacy {@code generateFlatten} shape (pre-port reference
 * line 728). The result column is typed as {@code Variant} (per
 * {@link com.gs.legend.compiler.checkers.FlattenChecker}); downstream
 * {@code extend(~x: _ | $_.col->get(...)})} sees an unnested element and
 * applies variant access against it.
 */
public final class FlattenLowering {
    private FlattenLowering() {}

    public static SqlRelation lower(TypedFlatten n, LoweringContext ctx) {
        SqlRelation source = Lowerer.lowerRelation(n.source(), ctx);
        String col = n.column();
        SqlExpr unnest = new SqlExpr.Unnest(
                new SqlExpr.VariantArrayCast(new SqlExpr.ColumnRef(col), "JSON"));
        return new SqlRelation.SelectExcept(
                source,
                List.of(col),
                List.of(new SqlRelation.ExtendCol(col, unnest)));
    }

    /**
     * Lower {@link TypedFlatten} in scalar list position. Legacy
     * {@code generateFlatten} is relational-only; this stub exists so the
     * {@code lowerScalar} switch is exhaustive over the dual-form records.
     * If a real test surfaces this path, port the legacy logic then.
     */
    public static com.gs.legend.sqlgen.SqlExpr lowerAsListExpr(TypedFlatten n, LoweringContext ctx) {
        throw new PlanGenNotPortedException(n, "flatten-scalar-arm",
                "flatten in scalar position not yet ported; legacy doesn't use this path");
    }
}
