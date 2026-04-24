package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.Navigation;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;

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

    /**
     * Installs a {@link NavScope}'s accumulated {@link Navigation}s onto
     * {@code source} as LEFT JOINs. Each {@link Navigation.JoinNav}'s
     * {@code sourceParam} binds to its parent prefix's alias (or the root
     * {@code srcAlias} when {@code parentPrefix} is empty); its
     * {@code targetParam} binds to the alias that {@link NavScope} pre-allocated.
     *
     * <p>Because {@link NavScope#navigate} allocates real SQL aliases eagerly
     * via {@code ctx.aliases()}, each {@link Navigation.JoinNav#abstractAlias()}
     * is already the real alias — no post-install substitution needed.
     *
     * @param source   Aliased source relation (guarantee via {@link #ensureAliased}).
     * @param srcAlias Source alias for navs whose {@code parentPrefix} is empty.
     * @param srcStore Store resolution for the root source (null for TDS).
     * @param scope    NavScope with pre-registered navs; empty → returns source unchanged.
     * @param ctx      Lowering context.
     */
    public static SqlRelation install(
            SqlRelation source,
            String srcAlias,
            StoreResolution srcStore,
            NavScope scope,
            LoweringContext ctx) {
        if (scope == null || scope.isEmpty()) return source;
        SqlRelation rel = source;
        for (Navigation nav : scope.toList()) {
            if (!(nav instanceof Navigation.JoinNav j)) {
                throw new IllegalStateException(
                        "Relations.install(scope): only JoinNav supported, got " + nav.getClass());
            }
            String alias = j.abstractAlias();                  // real alias (option B)
            NavScope.Entry entry = scope.lookup(j.prefix());
            String parentAlias = entry.parentPrefix().isEmpty()
                    ? srcAlias
                    : scope.lookup(entry.parentPrefix()).nav().abstractAlias();
            SqlRelation target = new SqlRelation.TableRef(null, j.targetTable(), alias, List.of());
            TypedSpec cond = j.condition();
            if (cond == null) {
                throw new IllegalStateException(
                        "Relations.install: JoinNav.condition is null for prefix=" + j.prefix());
            }
            // Parent's store (source side of this hop's join) is either the
            // caller's source store (parent root) or the target resolution of
            // the parent nav. Pull it off the parent entry; fall back to the
            // nav's own target resolution when parent is root and caller
            // didn't thread a store (TDS case — no store resolution).
            StoreResolution parentStore = entry.parentPrefix().isEmpty()
                    ? srcStore
                    : scope.lookup(entry.parentPrefix()).targetResolution();
            LoweringContext condCtx = ctx
                    .bindVar(j.sourceParam(), new SqlExpr.Identifier(parentAlias), parentStore)
                    .bindVar(j.targetParam(), new SqlExpr.Identifier(alias), j.targetResolution());
            SqlExpr on = Lowerer.lowerScalar(cond, condCtx);
            rel = new SqlRelation.Join(rel, target, SqlRelation.JoinType.LEFT, on);
        }
        return rel;
    }

}
