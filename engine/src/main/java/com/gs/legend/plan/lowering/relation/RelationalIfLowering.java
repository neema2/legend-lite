package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedCBoolean;
import com.gs.legend.compiler.typed.TypedIf;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * Lowers a {@link TypedIf} at relation position into a single
 * {@link SqlRelation}.
 *
 * <h3>Routing</h3>
 * <p>The if-node is "relational" iff {@link MappingResolver} stamped a
 * store on it (which it does when both branches produce class-typed
 * collections). When there's no stamped store, the if is a scalar
 * expression and we delegate to {@link LoweringContext#toRelation},
 * which lowers it as a {@code CASE WHEN} {@link SqlExpr} and wraps in a
 * single-column row. This keeps the dispatch decision out of
 * {@link Lowerer} so the top-level switch stays uniform delegation.
 *
 * <h3>Relational paths</h3>
 * <ol>
 *   <li><strong>Constant folding</strong> — when the condition is a
 *       compile-time {@link TypedCBoolean}, lower only the chosen branch.
 *       Keeps the SQL clean for the common static-flag pattern (config /
 *       feature gates) and avoids the redundant UNION ALL / WHERE FALSE
 *       branch.</li>
 *   <li><strong>Runtime UNION ALL</strong> — wrap each branch in a
 *       {@link SqlRelation.Filter} keyed by the lowered condition (and its
 *       negation), then combine with {@link SqlRelation.Union} in
 *       UNION-ALL mode. Effectively: {@code SELECT * FROM thenBr WHERE c
 *       UNION ALL SELECT * FROM elseBr WHERE NOT c}. Works for arbitrary
 *       scalar conditions and for branches with the same row schema (which
 *       the type checker enforces by requiring matching class types).</li>
 * </ol>
 *
 * <p>Branches authored as {@code |expr} are typed as {@link TypedLambda}
 * by the type checker; we transparently unwrap zero-parameter lambdas to
 * their body's last statement so the relation-producing expression is
 * reached without an extra dispatch hop. {@link Lowerer#lowerRelation}
 * already does the same unwrap on its lambda arm.
 */
public final class RelationalIfLowering {
    private RelationalIfLowering() {}

    public static SqlRelation lower(TypedIf n, LoweringContext ctx) {
        // Scalar if (no store stamped on the if-node): lower the CASE WHEN
        // expression and wrap the scalar in a single-column row.
        if (ctx.storeFor(n) == null) {
            return ctx.toRelation(n);
        }

        TypedSpec thenBr = unwrapLambda(n.thenBranch());
        TypedSpec elseBr = unwrapLambda(n.elseBranch());
        TypedSpec cond = n.condition();

        // Constant-fold {@code if(true|false, ...)}: pick the chosen branch
        // verbatim. The unchosen branch is dead code; lowering it would
        // produce a {@code WHERE FALSE} subquery the optimizer would have
        // to eliminate anyway.
        if (cond instanceof TypedCBoolean cb) {
            return Lowerer.lowerRelation(cb.value() ? thenBr : elseBr, ctx);
        }

        // Runtime conditional: UNION ALL with each branch filtered by the
        // condition's truth value. SqlRelation.Union(left, right, all=true)
        // emits UNION ALL.
        SqlExpr c = Lowerer.lowerScalar(cond, ctx);
        SqlRelation thenRel = new SqlRelation.Filter(
                Lowerer.lowerRelation(thenBr, ctx), c);
        SqlRelation elseRel = new SqlRelation.Filter(
                Lowerer.lowerRelation(elseBr, ctx), new SqlExpr.Not(c));
        return new SqlRelation.Union(thenRel, elseRel, true);
    }

    /**
     * Unwrap a zero-parameter {@link TypedLambda} to its body's last
     * statement. Mirrors the lambda-unwrap done by
     * {@link Lowerer#lowerRelation}'s {@code TypedLambda} arm so callers
     * that hand us {@code |expr}-shaped branches don't need to know about
     * the lambda wrapper.
     */
    private static TypedSpec unwrapLambda(TypedSpec node) {
        return node instanceof TypedLambda lam
                && lam.parameters().isEmpty()
                && !lam.body().isEmpty()
                ? lam.body().get(lam.body().size() - 1)
                : node;
    }
}
