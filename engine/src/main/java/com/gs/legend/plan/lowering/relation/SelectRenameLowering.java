package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.ColRename;
import com.gs.legend.compiler.typed.TypedDistinct;
import com.gs.legend.compiler.typed.TypedRename;
import com.gs.legend.compiler.typed.TypedSelect;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Column-level ops: {@link TypedSelect} (subset columns), {@link TypedRename}
 * (alias columns), and {@link TypedDistinct} (SELECT DISTINCT).
 *
 * <p>All three are mechanical wrappers around the lowered source; schema
 * transformations are carried by the corresponding {@link SqlRelation} variant.
 */
public final class SelectRenameLowering {
    private SelectRenameLowering() {}

    public static SqlRelation lower(TypedSelect n, LoweringContext ctx) {
        return new SqlRelation.Select(Lowerer.lowerRelation(n.source(), ctx), n.cols());
    }

    public static SqlRelation lower(TypedRename n, LoweringContext ctx) {
        Map<String, String> renames = new LinkedHashMap<>();
        for (ColRename r : n.renames()) renames.put(r.from(), r.to());
        return new SqlRelation.Rename(Lowerer.lowerRelation(n.source(), ctx), renames);
    }

    /**
     * {@code distinct()} — the {@link TypedDistinct#columns()} field carries an
     * optional explicit column subset. An empty list means "distinct over all
     * output columns" (the common case) so we emit a plain {@link SqlRelation.Distinct}.
     * An explicit column subset folds into a {@code Distinct(Select(...))} stack.
     */
    public static SqlRelation lower(TypedDistinct n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        if (n.columns().isEmpty()) return new SqlRelation.Distinct(src);
        return new SqlRelation.Distinct(new SqlRelation.Select(src, n.columns()));
    }
}
