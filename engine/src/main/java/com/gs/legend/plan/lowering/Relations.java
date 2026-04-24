package com.gs.legend.plan.lowering;

import com.gs.legend.plan.sql.SqlRelation;

/**
 * Small utilities shared across relation-operator lowering rules. Kept out of
 * {@link SqlRelation} itself because they need a {@link LoweringContext}
 * (fresh-alias supply).
 */
public final class Relations {
    private Relations() {}

    /**
     * Ensures the given relation has a referenceable SQL alias. If it does
     * ({@link SqlRelation#alias()} non-null), returns it unchanged. Otherwise
     * wraps it in a fresh {@link SqlRelation.SubqueryRel} so downstream
     * scalar lowering can reference its columns by the wrapper's alias.
     */
    public static SqlRelation ensureAliased(SqlRelation rel, LoweringContext ctx) {
        if (rel.alias() != null) return rel;
        return new SqlRelation.SubqueryRel(rel, ctx.nextAlias());
    }
}
