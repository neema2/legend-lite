package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.Navigation;
import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedFilter;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.NavScope;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers {@link TypedFilter} (Pure {@code src->filter(x | pred)}) into
 * {@link SqlRelation.Filter}, lowering each Boolean leaf as a scalar
 * {@link SqlExpr} and wrapping any leaf that crosses an association in a
 * {@link SqlExpr.Exists} subquery.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Lower the source; bind the lambda parameter to the source's alias.</li>
 *   <li>Walk the predicate's Boolean tree. Connectives ({@code and / or /
 *       not}) lower to {@link SqlExpr.And} / {@link SqlExpr.Or} /
 *       {@link SqlExpr.Not}; everything else is a Boolean leaf.</li>
 *   <li>For each leaf: lower its body with a fresh per-leaf {@link NavScope}.
 *       {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
 *       writes any non-embedded association hop into that scope. If the scope
 *       is empty the lowered expression is the leaf result. If non-empty the
 *       leaf becomes {@link SqlExpr.Exists}({@link SqlRelation.Filter}(<i>chain</i>,
 *       <i>corr ∧ body</i>)) where <i>chain</i> is the per-leaf TableRef +
 *       INNER JOIN sequence built from the scope's hops, <i>corr</i> is the
 *       first-hop's outer-correlated join condition, and <i>body</i> is the
 *       lowered leaf body.</li>
 *   <li>Wrap the resulting predicate in a {@link SqlRelation.Filter} over the
 *       (aliased) source.</li>
 * </ol>
 *
 * <h3>Why scalar EXISTS, not relational SEMI-JOIN</h3>
 * SQL's EXISTS is a Boolean scalar; it composes through the predicate tree
 * (AND/OR/NOT, nested, inside CASE) like any other Boolean. A relational
 * SEMI-JOIN can only be lifted out of a top-level conjunction — under OR or
 * NOT lifting changes semantics. Lowering each leaf to an inline
 * {@link SqlExpr.Exists} sidesteps that constraint and unifies all
 * connective contexts under one mechanism.
 */
public final class FilterLowering {
    private FilterLowering() {}

    public static SqlRelation lower(TypedFilter n, LoweringContext ctx) {
        SqlRelation source = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(source, ctx);

        TypedLambda pred = n.predicate();
        if (pred.parameters().size() != 1) {
            throw new PureCompileException(
                    "[plangen-c0954a] filter predicate must have exactly 1 parameter, got "
                            + pred.parameters().size());
        }
        if (pred.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(n, "filter-empty-body");
        }
        String paramName = pred.parameters().get(0).name();
        TypedSpec terminal = pred.body().get(pred.body().size() - 1);

        StoreResolution outerStore = ctx.storeFor(n.source());
        String outerAlias = aliased.alias();
        LoweringContext baseCtx = ctx.bindVar(
                paramName, new SqlExpr.Identifier(outerAlias), outerStore);

        SqlExpr predicate = processBoolean(terminal, baseCtx, outerAlias, outerStore);
        if (isTrue(predicate)) {
            return aliased;
        }
        return new SqlRelation.Filter(aliased, predicate);
    }

    /**
     * Recursive boolean-tree walker. Connectives ({@code and / or / not})
     * lower to {@link SqlExpr.And} / {@link SqlExpr.Or} / {@link SqlExpr.Not};
     * non-connective nodes are Boolean leaves. Leaves with association
     * crossings become {@link SqlExpr.Exists} subqueries; leaves without
     * crossings stay as plain scalars.
     */
    private static SqlExpr processBoolean(
            TypedSpec node, LoweringContext baseCtx,
            String outerAlias, StoreResolution outerStore) {
        if (node instanceof TypedNativeCall nc) {
            String fn = nc.func().name();
            if ("and".equals(fn) || "or".equals(fn)) {
                List<SqlExpr> parts = new ArrayList<>(nc.args().size());
                for (TypedSpec arg : nc.args()) {
                    parts.add(processBoolean(arg, baseCtx, outerAlias, outerStore));
                }
                return "and".equals(fn) ? andOf(parts) : orOf(parts);
            }
            if ("not".equals(fn)) {
                return new SqlExpr.Not(processBoolean(
                        nc.args().get(0), baseCtx, outerAlias, outerStore));
            }
        }
        // Boolean leaf: lower with a fresh local NavScope to detect any
        // non-embedded association hops the leaf body navigates.
        NavScope local = new NavScope();
        LoweringContext leafCtx = baseCtx.withNavScope(local);
        SqlExpr leafLowered = Lowerer.lowerScalar(node, leafCtx);
        if (local.isEmpty()) {
            return leafLowered;
        }
        return buildExists(local, baseCtx, leafLowered, outerAlias, outerStore);
    }

    /**
     * Builds a {@link SqlExpr.Exists} from a per-leaf NavScope. The first
     * hop's source side is the outer relation (correlation); subsequent
     * hops chain as INNER JOINs inside the EXISTS subquery's source.
     *
     * <p>Resulting EXISTS subquery shape:
     * {@code EXISTS (SELECT * FROM <chain> WHERE <first-corr> AND <leaf body>)}.
     */
    private static SqlExpr buildExists(
            NavScope local, LoweringContext baseCtx, SqlExpr leafLowered,
            String outerAlias, StoreResolution outerStore) {
        List<Navigation> navs = local.toList();
        SqlRelation inner = null;
        List<SqlExpr> innerConditions = new ArrayList<>();

        for (Navigation nav : navs) {
            if (!(nav instanceof Navigation.JoinNav j)) {
                throw new IllegalStateException(
                        "FilterLowering: only JoinNav supported in EXISTS scope, got "
                                + nav.getClass());
            }
            NavScope.Entry entry = local.lookup(j.prefix());
            boolean isFirst = entry.parentPrefix().isEmpty();
            String parentAlias = isFirst
                    ? outerAlias
                    : local.lookup(entry.parentPrefix()).nav().abstractAlias();
            StoreResolution parentStore = isFirst
                    ? outerStore
                    : local.lookup(entry.parentPrefix()).targetResolution();
            SqlRelation target = new SqlRelation.TableRef(
                    null, j.targetTable(), j.abstractAlias(), List.of());

            // Lower the join condition with sourceParam → parentAlias and
            // targetParam → this hop's alias.
            LoweringContext condCtx = baseCtx
                    .bindVar(j.sourceParam(), new SqlExpr.Identifier(parentAlias), parentStore)
                    .bindVar(j.targetParam(), new SqlExpr.Identifier(j.abstractAlias()), j.targetResolution());
            SqlExpr on = Lowerer.lowerScalar(j.condition(), condCtx);

            if (isFirst) {
                inner = target;
                innerConditions.add(on);                 // correlation
            } else {
                inner = new SqlRelation.Join(
                        inner, target, SqlRelation.JoinType.INNER, on);
            }
        }
        innerConditions.add(leafLowered);
        SqlRelation filtered = new SqlRelation.Filter(inner, andOf(innerConditions));
        return new SqlExpr.Exists(filtered);
    }

    /** AND-fold a list, dropping trivially-true entries and unwrapping singletons. */
    private static SqlExpr andOf(List<SqlExpr> parts) {
        List<SqlExpr> kept = new ArrayList<>(parts.size());
        for (SqlExpr p : parts) {
            if (!isTrue(p)) kept.add(p);
        }
        if (kept.isEmpty()) return new SqlExpr.BoolLiteral(true);
        if (kept.size() == 1) return kept.get(0);
        return new SqlExpr.And(kept);
    }

    /** OR-fold a list; unwraps singletons. */
    private static SqlExpr orOf(List<SqlExpr> parts) {
        if (parts.isEmpty()) return new SqlExpr.BoolLiteral(false);
        if (parts.size() == 1) return parts.get(0);
        return new SqlExpr.Or(parts);
    }

    private static boolean isTrue(SqlExpr e) {
        return e instanceof SqlExpr.BoolLiteral bl && bl.value();
    }
}

