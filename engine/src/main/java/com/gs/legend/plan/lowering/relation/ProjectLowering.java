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

        // Collect every association path referenced by any projection body, so
        // all of them share a single JOIN-chain on the aliased source.
        java.util.Set<java.util.List<String>> allPaths = new java.util.LinkedHashSet<>();
        for (TypedProjectionCol p : n.projections()) {
            if (p.expression().parameters().isEmpty() || p.expression().body().isEmpty()) continue;
            String pn = p.expression().parameters().get(0).name();
            var body = p.expression().body().get(p.expression().body().size() - 1);
            allPaths.addAll(AssocJoinLifter.collectPaths(body, pn));
        }
        var lifted = AssocJoinLifter.liftPaths(aliased, alias, store, allPaths, ctx);
        SqlRelation joined = lifted.relation();
        var bindings = lifted.bindings();

        // {@code TypedProjectionCol.associationPath} mirrors the inner body's
        // {@link com.gs.legend.compiler.typed.TypedPropertyAccess#associationPath()}
        // and is purely informational at this layer — the lambda body carries
        // all the structure we need, and the scalar lowering resolves
        // association hops (embedded cases) through
        // {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}.
        // Non-embedded paths surface from that rule with a precise sub-case
        // label, so this pass doesn't need to filter them upfront.
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
                    .withVar(paramName, new SqlExpr.Identifier(alias))
                    .withStore(store)
                    .withAssocBindings(bindings);
            SqlExpr expr = Lowerer.lowerScalar(terminal, inner);
            projections.add(new SqlRelation.Projection(p.alias(), expr));
        }
        return new SqlRelation.Project(joined, projections);
    }
}
