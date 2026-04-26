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
 * {@link SqlRelation.Filter}, splitting the predicate into Boolean leaves
 * and lifting any leaf that crosses an association into a
 * {@link SqlRelation.SemiJoin} clause.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Lower the source; bind the lambda parameter to the source's alias.</li>
 *   <li>Walk the predicate's Boolean tree, peeling {@code and / or / not}
 *       connectives. Everything else is a Boolean leaf.</li>
 *   <li>For each leaf: lower its body with a fresh per-leaf {@link NavScope}.
 *       {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
 *       writes any non-embedded association hop into that scope. After
 *       lowering:
 *       <ul>
 *         <li><strong>Empty scope</strong> → leaf has no association
 *             crossings; the lowered expression goes directly into the
 *             outer Filter's predicate.</li>
 *         <li><strong>Non-empty scope</strong> → build a {@link SqlRelation.SemiJoin}
 *             whose right side is the per-leaf chain of {@code TableRef}/
 *             {@code INNER JOIN}s (from the scope's hops) and whose condition
 *             is the AND of (outer-correlated first-hop join condition) and
 *             (lowered leaf body). The dialect renders SemiJoin as a
 *             correlated {@code EXISTS} subquery.</li>
 *       </ul>
 *   </li>
 *   <li>Stitch: chain the per-leaf SemiJoins onto the source; wrap in a
 *       {@link SqlRelation.Filter} carrying the AND of all non-crossing
 *       leaves' direct predicates. If the direct predicate is trivially
 *       {@code true} (every leaf was a SemiJoin) the Filter wrap is elided
 *       and the SemiJoin chain is the final relation.</li>
 * </ol>
 *
 * <h3>Connective handling</h3>
 * Per-leaf SemiJoin emission is correct only when the leaf appears in a
 * <em>pure-AND</em> context — every ancestor in the predicate tree must be
 * {@code and}. Under {@code or} or {@code not}, lifting a leaf to an outer
 * SemiJoin would change semantics (an OR-branch's existence test must NOT
 * filter rows globally). Such cases throw {@link PlanGenNotPortedException}
 * with tag {@code filter:assoc-under-or} / {@code filter:assoc-under-not};
 * inline-EXISTS lowering for those branches is a follow-up.
 */
public final class FilterLowering {
    private FilterLowering() {}

    /** Per-leaf SemiJoin clause emitted during predicate processing. */
    private record SemiJoinClause(SqlRelation inner, SqlExpr condition) {}

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

        // Walk the boolean tree. Each leaf either contributes to the direct
        // predicate (no association crossings) or emits a SemiJoinClause
        // (has crossings; pure-AND context only).
        List<SemiJoinClause> sjs = new ArrayList<>();
        SqlExpr directPred = processBoolean(
                terminal, baseCtx, /*pureAnd=*/true, sjs, outerAlias, outerStore);

        // Stitch: source [⋉ ... ⋉] [WHERE direct].
        SqlRelation current = aliased;
        for (SemiJoinClause sj : sjs) {
            current = new SqlRelation.SemiJoin(current, sj.inner(), sj.condition());
        }
        if (isTrue(directPred) && !sjs.isEmpty()) {
            return current;
        }
        return new SqlRelation.Filter(current, directPred);
    }

    /**
     * Recursive boolean-tree walker. Returns the SqlExpr to AND into the
     * outer Filter's predicate; appends any per-leaf SemiJoin clauses to
     * {@code sjs}. {@code pureAnd} is true while every ancestor is an
     * {@code and} connective.
     */
    private static SqlExpr processBoolean(
            TypedSpec node, LoweringContext baseCtx, boolean pureAnd,
            List<SemiJoinClause> sjs, String outerAlias, StoreResolution outerStore) {
        if (node instanceof TypedNativeCall nc) {
            String fn = nc.func().name();
            if ("and".equals(fn)) {
                List<SqlExpr> parts = new ArrayList<>();
                for (TypedSpec arg : nc.args()) {
                    parts.add(processBoolean(arg, baseCtx, pureAnd, sjs, outerAlias, outerStore));
                }
                return andOf(parts);
            }
            if ("or".equals(fn) || "not".equals(fn)) {
                // Children of OR/NOT are not in pure-AND context — any leaf
                // with crossings under such a node would need inline EXISTS
                // lowering (not yet ported).
                List<SqlExpr> parts = new ArrayList<>();
                List<SemiJoinClause> innerSjs = new ArrayList<>();
                for (TypedSpec arg : nc.args()) {
                    parts.add(processBoolean(arg, baseCtx, /*pureAnd=*/false, innerSjs, outerAlias, outerStore));
                }
                if (!innerSjs.isEmpty()) {
                    throw PlanGenNotPortedException.stage3(
                            node, "filter:assoc-under-" + fn);
                }
                return "not".equals(fn)
                        ? new SqlExpr.Not(parts.get(0))
                        : new SqlExpr.Or(parts);
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
        if (!pureAnd) {
            throw PlanGenNotPortedException.stage3(node, "filter:assoc-not-in-pure-and");
        }
        sjs.add(buildSemiJoin(local, baseCtx, leafLowered, outerAlias, outerStore));
        return new SqlExpr.BoolLiteral(true);
    }

    /**
     * Builds a SemiJoinClause from a per-leaf NavScope. The first hop's
     * source side is the outer relation (correlation); subsequent hops chain
     * as INNER JOINs inside the inner relation. The first hop's join
     * condition becomes part of the SemiJoin's condition (carries the
     * outer correlation); subsequent hops' join conditions go on their
     * INNER JOIN's ON clause.
     *
     * <p>Final SemiJoin condition = (first-hop correlation) AND (lowered leaf body).
     */
    private static SemiJoinClause buildSemiJoin(
            NavScope local, LoweringContext baseCtx, SqlExpr leafLowered,
            String outerAlias, StoreResolution outerStore) {
        List<Navigation> navs = local.toList();
        SqlRelation inner = null;
        List<SqlExpr> outerConditions = new ArrayList<>();

        for (Navigation nav : navs) {
            if (!(nav instanceof Navigation.JoinNav j)) {
                throw new IllegalStateException(
                        "FilterLowering: only JoinNav supported in SemiJoin scope, got "
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
                outerConditions.add(on);                // correlation goes in SemiJoin WHERE
            } else {
                inner = new SqlRelation.Join(
                        inner, target, SqlRelation.JoinType.INNER, on);
            }
        }
        outerConditions.add(leafLowered);
        return new SemiJoinClause(inner, andOf(outerConditions));
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

    private static boolean isTrue(SqlExpr e) {
        return e instanceof SqlExpr.BoolLiteral bl && bl.value();
    }
}

