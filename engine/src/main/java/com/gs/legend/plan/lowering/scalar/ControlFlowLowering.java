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
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;
import com.gs.legend.model.m3.Type;

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
                cur = cur.bindVar(let.name(), value, null);
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
        // Multiplicity-only coercions (toOne / toMany without a type change) have
        // no Pure type change — pass the inner expression through unchanged so
        // downstream printers don't emit a spurious CAST.
        if (n.targetType() == null) return inner;
        // Cast to {@code Any} (the top type) is a Pure-side identity. Emitting
        // {@code CAST(... AS Any)} would fail at the dialect because there's
        // no SQL type for {@code Any}; passthrough is the right semantic.
        if (n.targetType() == com.gs.legend.model.m3.Primitive.ANY) return inner;
        String pureTypeName = n.targetType().typeName();
        if (pureTypeName == null) {
            throw PlanGenNotPortedException.stage3(n, "cast:unnamed-target-type");
        }
        // Type-changing cast with a multi target: emit a list-element cast
        // ({@code CAST(expr AS T[])}) so the result stays a SQL list.
        // Two cases land here:
        //   • {@code Variant[1] -> T[*]} (toMany from a JSON array).
        //   • {@code S[*] -> T[*]} (cast on a list source, e.g.
        //     {@code []->cast(@Integer)} widens an empty {@code Any[*]} to
        //     {@code Integer[*]}).
        // Without this, generic {@code CAST(list AS T)} collapses the list
        // to a single value and downstream list operations (list_concat,
        // list_reduce) reject the result.
        if (n.info() != null && n.info().isMany()) {
            return new SqlExpr.VariantArrayCast(inner, pureTypeName);
        }
        // Variant-to-primitive cast: rewrite {@code VariantAccess} ({@code ->})
        // to {@code VariantTextAccess} ({@code ->>}) so JSON string quotes are
        // stripped before the CAST (legacy line 4053-4057). Applies to primitive
        // scalar types only — Variant-to-Variant casts are identity.
        if (n.targetType() instanceof com.gs.legend.model.m3.Primitive p
                && p != com.gs.legend.model.m3.Primitive.VARIANT
                && inner instanceof SqlExpr.VariantAccess va) {
            inner = new SqlExpr.VariantTextAccess(va.expr(), va.key());
        }
        return new SqlExpr.Cast(inner, pureTypeName);
    }

    public static SqlExpr lower(TypedMatch n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }

    public static SqlExpr lower(TypedZip n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }

    /**
     * {@code $f->eval(args...)} — where {@code $f} is a function-typed variable.
     *
     * <p>Mirrors legacy pass-through behavior (see legacy line 4063-4075, case
     * {@code "eval"} in scalar-function dispatch): if the applicable is a
     * lambda-parameter binding, the parameter is already bound upstream (via
     * {@code withVars} during user-function inlining) to the lambda's lowered
     * body. Lowering the {@code TypedVariable} resolves to that binding, which
     * is what the call's result should be.
     *
     * <p>Args are evaluated for side-effects (to match legacy's recursive
     * compilation) and then discarded — the function body has already been
     * inlined by {@code TypeChecker}/{@code UserCallLowering}.
     */
    public static SqlExpr lower(TypedEval n, LoweringContext ctx) {
        for (TypedSpec arg : n.args()) {
            Lowerer.lowerScalar(arg, ctx);
        }
        return Lowerer.lowerScalar(n.applicable(), ctx);
    }

    /**
     * {@code source->map(mapper)} → {@code list_transform(source, x -> body)}.
     *
     * <p>Ported from legacy {@code generateScalarFunctionCall} case {@code "map"}
     * (see {@code docs/reference/plangen-legacy-pre-port.java.txt} line 4098):
     * lower the source as a scalar, lower the mapper's body with the lambda
     * parameter bound to a bare column reference named after the param, and
     * wrap in a {@link SqlExpr.LambdaExpr} / {@link SqlExpr.FunctionCall}.
     *
     * <p>Variant sources: if the compiler typed the source as a variant (JSON),
     * the legacy path wrapped it in {@code CAST(... AS JSON[])}. We reproduce
     * that by reading {@link TypedSpec#info()} on the source.
     */
    public static SqlExpr lower(TypedMap n, LoweringContext ctx) {
        SqlExpr source = Lowerer.lowerScalar(n.source(), ctx);

        // Variant sources need a JSON[] cast before iteration (DuckDB can't
        // list_transform over a raw JSON scalar).
        if (isVariantType(n.source())) {
            source = new SqlExpr.VariantArrayCast(source, "JSON");
        }

        var params = n.mapper().parameters();
        String elemParam = params.isEmpty() ? "x" : params.get(0).name();

        // Bind the lambda param to a bare column reference so nested property
        // access inside the body renders as {@code x.prop} / {@code x} itself.
        SqlExpr paramBinding = new SqlExpr.Identifier(elemParam);
        LoweringContext inner = ctx.bindVar(elemParam, paramBinding, null);

        var body = n.mapper().body();
        SqlExpr lambdaBody = body.isEmpty()
                ? new SqlExpr.NullLiteral()
                : Lowerer.lowerScalar(body.get(body.size() - 1), inner);

        SqlExpr lambda = new SqlExpr.LambdaExpr(java.util.List.of(elemParam), lambdaBody);
        return new SqlExpr.FunctionCall("listTransform", java.util.List.of(source, lambda));
    }

    private static boolean isVariantType(TypedSpec n) {
        // Defensive: not every source has Primitive typed info; treat absence as non-variant.
        var info = n.info();
        if (info == null) return false;
        var t = info.type();
        if (t == null) return false;
        String name = t.typeName();
        return "Variant".equalsIgnoreCase(name) || "Json".equalsIgnoreCase(name);
    }

    // ====================================================================
    // Relational entry points — operator-specific routing decisions for
    // TypedBlock and TypedCast at relation root. Lowerer's case statement
    // delegates straight here; no `?:` lives in the dispatch layer.
    // ====================================================================

    /**
     * Lower a {@link TypedBlock} at relational position.
     *
     * <p>Walks intermediate statements: each {@link TypedLet} binds its
     * value (relational values via {@link LoweringContext#bindRel} —
     * the captured HIR is re-lowered at each {@code $r} reference;
     * scalar values via {@link LoweringContext#bindVar}). Non-let
     * intermediates are lowered for side-effect parity and discarded.
     * The block's value is the tail dispatched via
     * {@link LoweringContext#toRelation}.
     */
    public static SqlRelation lowerBlock(TypedBlock n, LoweringContext ctx) {
        LoweringContext cur = ctx;
        int last = n.stmts().size() - 1;
        for (int i = 0; i < last; i++) {
            TypedSpec s = n.stmts().get(i);
            if (s instanceof TypedLet let) {
                if (cur.isRelationalSource(let.value())) {
                    cur = cur.bindRel(let.name(), let.value());
                } else {
                    SqlExpr value = Lowerer.lowerScalar(let.value(), cur);
                    cur = cur.bindVar(let.name(), value, null);
                }
            } else {
                // Side-effecting intermediate — lower for parity, discard.
                Lowerer.lowerScalar(s, cur);
            }
        }
        return cur.toRelation(n.stmts().get(last));
    }

    /**
     * Lower a {@link TypedCast} at relational position.
     *
     * <p>A cast to a {@link Type.Relation} is a structural schema
     * declaration with no value work — the inner relation flows through
     * unchanged. Other cast targets lower as scalar via
     * {@link LoweringContext#toRelation}, which emits the {@code CAST(...)}
     * and wraps as a one-row relation.
     */
    public static SqlRelation lowerCast(TypedCast n, LoweringContext ctx) {
        return n.targetType() instanceof Type.Relation
                ? Lowerer.lowerRelation(n.expr(), ctx)
                : ctx.toRelation(n);
    }
}
