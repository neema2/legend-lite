package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.Navigation;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * Result of {@link #install}: the source relation with all requested
     * navigations installed, plus a map from each {@link Navigation}'s
     * {@code abstractAlias} token to the real SQL alias allocated for it.
     *
     * <p>{@link #aliasTable} iterates in insertion order (LinkedHashMap) so
     * callers needing an ordered alias chain (e.g., extend for positional
     * param binding) can read {@code aliasTable.values()}.
     */
    public record Installed(SqlRelation relation, Map<String, String> aliasTable) {}

    /**
     * Installs a list of {@link Navigation}s onto {@code source} as LEFT JOINs
     * and returns the joined relation plus an {@code abstractAlias → real SQL
     * alias} map.
     *
     * <p>v1 scope: only {@link Navigation.JoinNav} is supported; each JoinNav's
     * {@code sourceParam} binds to {@code srcAlias} (flat — no chained
     * navigation), its {@code targetParam} binds to a freshly allocated alias.
     * {@link Navigation.ExistsNav}, {@link Navigation.SubqueryNav}, and
     * {@link Navigation.LateralNav} are reserved and throw on encounter.
     *
     * @param source   Aliased source relation (guarantee via {@link #ensureAliased}).
     * @param srcAlias Source alias that every JoinNav's {@code sourceParam} binds to.
     * @param navs     Navigations to install, in order. Empty list returns source unchanged.
     * @param store    Active store for scalar lowering of join conditions.
     * @param ctx      Lowering context (fresh-alias supply).
     */
    public static Installed install(
            SqlRelation source,
            String srcAlias,
            List<Navigation> navs,
            StoreResolution store,
            LoweringContext ctx) {
        SqlRelation rel = source;
        Map<String, String> aliasTable = new LinkedHashMap<>();
        for (Navigation nav : navs) {
            switch (nav) {
                case Navigation.JoinNav j -> {
                    String a = ctx.nextAlias();
                    aliasTable.put(j.abstractAlias(), a);
                    SqlRelation target = new SqlRelation.TableRef(null, j.targetTable(), a, List.of());
                    TypedSpec cond = j.condition();
                    if (cond == null) {
                        throw new IllegalStateException(
                                "Relations.install: JoinNav.condition is null for prefix=" + j.prefix());
                    }
                    LoweringContext condCtx = ctx
                            .withVar(j.sourceParam(), new SqlExpr.Identifier(srcAlias))
                            .withVar(j.targetParam(), new SqlExpr.Identifier(a))
                            .withStore(store);
                    SqlExpr on = Lowerer.lowerScalar(cond, condCtx);
                    rel = new SqlRelation.Join(rel, target, SqlRelation.JoinType.LEFT, on);
                }
                case Navigation.ExistsNav ignored -> throw new IllegalStateException(
                        "Relations.install: ExistsNav not emitted in v1");
                case Navigation.SubqueryNav ignored -> throw new IllegalStateException(
                        "Relations.install: SubqueryNav not emitted in v1");
                case Navigation.LateralNav ignored -> throw new IllegalStateException(
                        "Relations.install: LateralNav reserved — not emitted in v1");
            }
        }
        return new Installed(rel, aliasTable);
    }
}
