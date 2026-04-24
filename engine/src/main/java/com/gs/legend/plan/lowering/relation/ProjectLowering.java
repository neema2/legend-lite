package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedProject;
import com.gs.legend.compiler.typed.TypedProjectionCol;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers {@link TypedProject} (Pure {@code src->project([^col1, ^col2, ...])}) to
 * {@link SqlRelation.Project}.
 *
 * <p>Each {@link TypedProjectionCol} carries an alias plus a lambda that
 * computes the column value from a row parameter. The lambda is lowered with
 * the parameter bound to the source's alias; the current store resolution is
 * installed so {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
 * can map property names to physical columns.
 *
 * <p><strong>Stage 3 scope</strong>: projections with {@code associationPath}
 * (navigation through associations) are not handled — those require JOIN
 * expansion and are deferred. Such projections surface as
 * {@link PlanGenNotPortedException} tagged {@code projection:assoc-path}.
 */
public final class ProjectLowering {
    private ProjectLowering() {}

    public static SqlRelation lower(TypedProject n, LoweringContext ctx) {
        SqlRelation source = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(source, ctx);
        String alias = aliased.alias();
        var store = ctx.storeFor(n.source());

        // Right-architecture path: one NavScope shared across all projection
        // lambdas so duplicate prefixes dedupe to a single LEFT JOIN.
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();

        List<SqlRelation.Projection> projections = new ArrayList<>(n.projections().size());
        for (TypedProjectionCol p : n.projections()) {
            TypedLambda lam = p.expression();
            if (lam.parameters().size() != 1) {
                throw PlanGenNotPortedException.stage3(n, "projection:multi-param-lambda");
            }
            var body = lam.body();
            if (body.isEmpty()) {
                throw PlanGenNotPortedException.stage3(n, "projection:empty-body");
            }
            String paramName = lam.parameters().get(0).name();
            TypedSpec terminal = body.get(body.size() - 1);

            LoweringContext inner = ctx
                    .bindVar(paramName, new SqlExpr.Identifier(alias), store)
                    .withNavScope(scope);
            SqlExpr expr = Lowerer.lowerScalar(terminal, inner);
            projections.add(new SqlRelation.Projection(p.alias(), expr));
        }
        SqlRelation joined = Relations.install(aliased, alias, store, scope, ctx);
        return new SqlRelation.Project(joined, projections);
    }
}
