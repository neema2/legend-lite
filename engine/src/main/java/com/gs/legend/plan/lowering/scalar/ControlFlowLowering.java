package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedBlock;
import com.gs.legend.compiler.typed.TypedCast;
import com.gs.legend.compiler.typed.TypedEval;
import com.gs.legend.compiler.typed.TypedIf;
import com.gs.legend.compiler.typed.TypedLet;
import com.gs.legend.compiler.typed.TypedMap;
import com.gs.legend.compiler.typed.TypedMatch;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedZip;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * Scalar-side control-flow lowering:
 * {@link TypedIf} ({@code if/then/else}),
 * {@link TypedLet} / {@link TypedBlock} (let bindings + sequencing),
 * {@link TypedCast} (pure type cast),
 * and several less-common carriers ({@link TypedMatch}, {@link TypedZip},
 * {@link TypedEval}, {@link TypedMap}).
 *
 * <p><strong>Stage 4 scope</strong>:
 * <ul>
 *   <li>{@link TypedBlock} processes inner {@link TypedLet}s as sequential let
 *       bindings, lowering their value expressions as scalar {@link SqlExpr}s
 *       and binding the name in a per-statement {@link LoweringContext}. The
 *       block's result is the scalar lowering of its final statement.</li>
 *   <li>{@link TypedIf} lowers to {@link SqlExpr.CaseWhen}.</li>
 *   <li>{@link TypedCast} rewrites to {@link SqlExpr.Cast} with the Pure type
 *       name; dialect translation is deferred to print time.</li>
 *   <li>{@link TypedLet} at scalar position is only meaningful inside a block;
 *       a bare let (no consuming expression) throws as a caller bug.</li>
 *   <li>The remaining carriers stay on the stage-4 backlog.</li>
 * </ul>
 */
public final class ControlFlowLowering {
    private ControlFlowLowering() {}

    public static SqlExpr lower(TypedIf n, LoweringContext ctx) {
        return new SqlExpr.CaseWhen(
                Lowerer.lowerScalar(n.condition(),  ctx),
                Lowerer.lowerScalar(n.thenBranch(), ctx),
                Lowerer.lowerScalar(n.elseBranch(), ctx));
    }

    public static SqlExpr lower(TypedLet n, LoweringContext ctx) {
        // A bare TypedLet at scalar position has no successor to consume its
        // binding: its value is the expression's result (the legacy PlanGen
        // also unwrapped `let x = e` to `e` in this shape). TypedBlock handles
        // the multi-statement case with real sequencing and binding.
        return Lowerer.lowerScalar(n.value(), ctx);
    }

    public static SqlExpr lower(TypedBlock n, LoweringContext ctx) {
        LoweringContext cur = ctx;
        int last = n.stmts().size() - 1;
        for (int i = 0; i < last; i++) {
            TypedSpec s = n.stmts().get(i);
            if (s instanceof TypedLet let) {
                SqlExpr value = Lowerer.lowerScalar(let.value(), cur);
                cur = cur.withVar(let.name(), value);
            } else {
                // Side-effecting intermediate statement — not supported in scalar
                // SQL; lower for consistency but discard.
                Lowerer.lowerScalar(s, cur);
            }
        }
        return Lowerer.lowerScalar(n.stmts().get(last), cur);
    }

    public static SqlExpr lower(TypedCast n, LoweringContext ctx) {
        SqlExpr inner = Lowerer.lowerScalar(n.expr(), ctx);
        // Multiplicity-only coercions (toOne / toMany) have no Pure type change —
        // pass the inner expression through unchanged so downstream printers don't
        // emit a spurious CAST.
        if (n.targetType() == null) return inner;
        String pureTypeName = n.targetType().typeName();
        if (pureTypeName == null) {
            throw PlanGenNotPortedException.stage3(n, "cast:unnamed-target-type");
        }
        return new SqlExpr.Cast(inner, pureTypeName);
    }

    public static SqlExpr lower(TypedMatch n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }

    public static SqlExpr lower(TypedZip n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }

    public static SqlExpr lower(TypedEval n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }

    public static SqlExpr lower(TypedMap n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }
}
