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
            NavScope.Entry entry = scope.lookup(nav.prefix());
            String parentAlias = entry.parentPrefix().isEmpty()
                    ? srcAlias
                    : scope.lookup(entry.parentPrefix()).nav().abstractAlias();
            rel = switch (nav) {
                case Navigation.JoinNav j     -> installJoin(rel, j, entry, parentAlias, srcStore, scope, ctx);
                case Navigation.UnnestNav u   -> installUnnest(rel, u, parentAlias);
                case Navigation.ExistsNav x   -> throw new IllegalStateException(
                        "Relations.install: ExistsNav not supported (prefix=" + x.prefix() + ")");
                case Navigation.SubqueryNav s -> throw new IllegalStateException(
                        "Relations.install: SubqueryNav not supported (prefix=" + s.prefix() + ")");
                case Navigation.LateralNav l  -> throw new IllegalStateException(
                        "Relations.install: LateralNav not supported (prefix=" + l.prefix() + ")");
            };
        }
        return rel;
    }

    /**
     * Install a {@link Navigation.JoinNav} as {@code LEFT JOIN target ON condition}.
     * Body extracted verbatim from the previous monolithic loop.
     */
    private static SqlRelation installJoin(SqlRelation rel, Navigation.JoinNav j,
                                           NavScope.Entry entry, String parentAlias,
                                           StoreResolution srcStore, NavScope scope,
                                           LoweringContext ctx) {
        String alias = j.abstractAlias();
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
        return new SqlRelation.Join(rel, target, SqlRelation.JoinType.LEFT, on);
    }

    /**
     * Install a {@link Navigation.UnnestNav} as
     * {@code LEFT JOIN <LateralUnnest(parent.<arrayProperty>, alias, fields)> ON TRUE}.
     * The dialect projects each element field up so {@code alias."<field>"}
     * resolves directly — no caller-side struct drilling (architecture: row-shape
     * uniformity). {@code ON TRUE} preserves outer rows whose array is empty;
     * unnested element-field columns are NULL on those rows.
     */
    private static SqlRelation installUnnest(SqlRelation rel, Navigation.UnnestNav u,
                                             String parentAlias) {
        SqlExpr arrayRef = new SqlExpr.Column(parentAlias, u.arrayProperty());
        List<String> fields = List.copyOf(u.targetResolution().propertyToColumn().values());
        SqlRelation right = new SqlRelation.LateralUnnest(
                arrayRef, u.abstractAlias(), fields, List.of());
        return new SqlRelation.Join(rel, right, SqlRelation.JoinType.LEFT,
                new SqlExpr.BoolLiteral(true));
    }
}
