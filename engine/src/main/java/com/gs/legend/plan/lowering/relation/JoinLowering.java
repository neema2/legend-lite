package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedAsOfJoin;
import com.gs.legend.compiler.typed.TypedJoin;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binary relational ops: {@link TypedJoin} (inner / outer / cross joins) and
 * {@link TypedAsOfJoin} (time-window lookup joins).
 *
 * <p>The condition lambda exposes two parameters — one for each side — so
 * property access inside it resolves via the correct alias. Stage 5 binds both
 * to fresh aliases generated when ensuring the sides are {@code FROM}-addressable.
 *
 * <p><strong>Stage 5 scope</strong>:
 * <ul>
 *   <li>{@link TypedJoin} — simple {@code JOIN ... ON ...} with the HIR's
 *       {@link com.gs.legend.compiler.typed.JoinType}. {@code renames} are
 *       threaded into an outer {@link SqlRelation.Rename} so downstream
 *       column references match the Pure semantics.</li>
 *   <li>{@link TypedAsOfJoin} — still deferred; requires
 *       {@link SqlRelation.AsOfJoin} printer support.</li>
 * </ul>
 */
public final class JoinLowering {
    private JoinLowering() {}

    public static SqlRelation lower(TypedJoin n, LoweringContext ctx) {
        SqlRelation left  = Relations.ensureAliased(Lowerer.lowerRelation(n.left(),  ctx), ctx);
        SqlRelation right = Relations.ensureAliased(Lowerer.lowerRelation(n.right(), ctx), ctx);

        TypedLambda cond = n.condition();
        if (cond.parameters().size() != 2) {
            throw PlanGenNotPortedException.stage3(n, "join:non-binary-condition");
        }
        if (cond.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(n, "join:empty-condition-body");
        }
        String leftParam  = cond.parameters().get(0).name();
        String rightParam = cond.parameters().get(1).name();
        TypedSpec terminal = cond.body().get(cond.body().size() - 1);

        LoweringContext inner = ctx
                .withVar(leftParam,  new SqlExpr.Identifier(left.alias()))
                .withVar(rightParam, new SqlExpr.Identifier(right.alias()))
                .withStore(ctx.storeFor(n.left()));

        SqlExpr on = Lowerer.lowerScalar(terminal, inner);
        SqlRelation.JoinType type = switch (n.joinType()) {
            case INNER -> SqlRelation.JoinType.INNER;
            case LEFT  -> SqlRelation.JoinType.LEFT;
            case RIGHT -> SqlRelation.JoinType.RIGHT;
            case FULL  -> SqlRelation.JoinType.FULL;
            case CROSS -> SqlRelation.JoinType.CROSS;
        };

        SqlRelation join = new SqlRelation.Join(left, right, type, on);
        if (n.renames().isEmpty()) return join;
        return new SqlRelation.Rename(join, new LinkedHashMap<>(n.renames()));
    }

    public static SqlRelation lower(TypedAsOfJoin n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage3(n, "asOfJoin");
    }

    // Suppress unused-import lint for the re-exported Map type used by renames() above.
    @SuppressWarnings("unused")
    private static void _touch(Map<String, String> m) { m.size(); }
}
