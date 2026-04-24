package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.CollectionBuild;
import com.gs.legend.compiler.typed.Concatenation;
import com.gs.legend.compiler.typed.FoldStrategy;
import com.gs.legend.compiler.typed.MapReduce;
import com.gs.legend.compiler.typed.SameType;
import com.gs.legend.compiler.typed.TypedFold;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;

/**
 * {@link TypedFold} scalar reducer. Ported from legacy
 * {@code generateScalarFunctionCall} case {@code "fold"}
 * (see {@code docs/reference/plangen-legacy-pre-port.java.txt} line 4122).
 *
 * <p>Four-way dispatch on {@link FoldStrategy} (stamped by {@code FoldChecker}):
 * <ul>
 *   <li>{@link Concatenation} — {@code fold+add} over collections &rarr;
 *       {@code listConcat(init, source)}.</li>
 *   <li>{@link SameType} — element type equals accumulator type &rarr;
 *       {@code listReduce(source, (acc, elem) -> body, cast(init))}.</li>
 *   <li>{@link MapReduce} — decomposable via element transform composed with
 *       reducer &rarr;
 *       {@code listReduce(listTransform(source, elem -> xform), (acc, x) -> reducerBody, init)}.</li>
 *   <li>{@link CollectionBuild} — accumulator is a list, non-decomposable &rarr;
 *       wrap each element, {@code listReduce} with unwrap post-process on body.</li>
 * </ul>
 *
 * <p>The {@code firstArgIsList} guard from the legacy (wrap scalar sources
 * in {@code wrapList(source)}) is reproduced here via
 * {@link TypedSpec#info()} {@code .isMany()}.
 */
public final class FoldLowering {
    private FoldLowering() {}

    public static SqlExpr lower(TypedFold n, LoweringContext ctx) {
        TypedLambda reducer = n.reducer();
        if (reducer.parameters().size() < 2) {
            throw PlanGenNotPortedException.stage4(n);
        }
        String elemParam = reducer.parameters().get(0).name();
        String accParam = reducer.parameters().get(1).name();

        SqlExpr source = Lowerer.lowerScalar(n.source(), ctx);
        if (!isMany(n.source())) {
            source = new SqlExpr.FunctionCall("wrapList", List.of(source));
        }
        SqlExpr init = Lowerer.lowerScalar(n.init(), ctx);

        return switch (n.strategy()) {
            // Path 1: fold+add → listConcat(init, source)
            case Concatenation ignored ->
                    new SqlExpr.FunctionCall("listConcat", List.of(init, source));

            // Path 2: T == V → listReduce(source, (acc, elem) -> body, cast(init))
            case SameType ignored -> {
                SqlExpr lambdaBody = lowerLambdaBody(reducer, elemParam, accParam, ctx);
                SqlExpr lambda = new SqlExpr.LambdaExpr(
                        List.of(accParam, elemParam), lambdaBody);
                // Cast init to the accumulator type so list_reduce's seed
                // matches the list element type (DuckDB requires exact match,
                // e.g., INTEGER seed 0 vs BIGINT[] list).
                SqlExpr castInit = init;
                var info = n.info();
                if (info != null && info.type() != null && info.type().typeName() != null) {
                    castInit = new SqlExpr.Cast(init, info.type().typeName());
                }
                yield new SqlExpr.FunctionCall("listReduce",
                        List.of(source, lambda, castInit));
            }

            // Path 3: T ≠ V, decomposable → listTransform + listReduce
            case MapReduce(TypedSpec transform, TypedSpec reducerBody,
                           String mrAccParam, String mrFreshParam) -> {
                SqlExpr transformBody = lowerBindingParam(transform, elemParam, ctx);
                SqlExpr transformLambda = new SqlExpr.LambdaExpr(
                        List.of(elemParam), transformBody);
                SqlExpr mapped = new SqlExpr.FunctionCall("listTransform",
                        List.of(source, transformLambda));

                // listReduce(mapped, (mrAcc, mrFresh) -> reducerBody, init)
                LoweringContext reducerCtx = ctx
                        .bindVar(mrAccParam, new SqlExpr.Identifier(mrAccParam), null)
                        .bindVar(mrFreshParam, new SqlExpr.Identifier(mrFreshParam), null);
                SqlExpr compiledReducer = Lowerer.lowerScalar(reducerBody, reducerCtx);
                SqlExpr reducerLambda = new SqlExpr.LambdaExpr(
                        List.of(mrAccParam, mrFreshParam), compiledReducer);
                yield new SqlExpr.FunctionCall("listReduce",
                        List.of(mapped, reducerLambda, init));
            }

            // Path 4: V = List<T>, non-decomposable → wrap + listReduce + unwrap
            case CollectionBuild ignored -> {
                SqlExpr wrapBody = new SqlExpr.FunctionCall("wrapList",
                        List.of(new SqlExpr.Identifier(elemParam)));
                SqlExpr wrapLambda = new SqlExpr.LambdaExpr(
                        List.of(elemParam), wrapBody);
                SqlExpr wrappedSource = new SqlExpr.FunctionCall("listTransform",
                        List.of(source, wrapLambda));

                SqlExpr lambdaBody = lowerLambdaBody(reducer, elemParam, accParam, ctx);
                SqlExpr unwrapped = unwrapElemRefs(lambdaBody, elemParam);
                SqlExpr lambda = new SqlExpr.LambdaExpr(
                        List.of(accParam, elemParam), unwrapped);
                yield new SqlExpr.FunctionCall("listReduce",
                        List.of(wrappedSource, lambda, init));
            }
        };
    }

    /** Lower the reducer's terminal-body expression with both params bound. */
    private static SqlExpr lowerLambdaBody(TypedLambda reducer, String elemParam,
                                           String accParam, LoweringContext ctx) {
        var body = reducer.body();
        if (body.isEmpty()) return new SqlExpr.NullLiteral();
        LoweringContext inner = ctx
                .bindVar(elemParam, new SqlExpr.Identifier(elemParam), null)
                .bindVar(accParam, new SqlExpr.Identifier(accParam), null);
        return Lowerer.lowerScalar(body.get(body.size() - 1), inner);
    }

    /** Lower a scalar expression with one lambda parameter bound. */
    private static SqlExpr lowerBindingParam(TypedSpec n, String paramName, LoweringContext ctx) {
        LoweringContext inner = ctx.bindVar(paramName,
                new SqlExpr.Identifier(paramName), null);
        return Lowerer.lowerScalar(n, inner);
    }

    /**
     * CollectionBuild post-process: every reference to the (now-wrapped) elem
     * param in the body is rewritten as {@code listExtract(elem, 1)}.
     * Ported from legacy {@code unwrapElemRefs} (line 4381).
     */
    private static SqlExpr unwrapElemRefs(SqlExpr expr, String elemParam) {
        if (expr instanceof SqlExpr.Identifier id && id.name().equals(elemParam)) {
            return new SqlExpr.FunctionCall("listExtract",
                    List.of(expr, new SqlExpr.NumericLiteral(1)));
        }
        if (expr instanceof SqlExpr.FunctionCall fc) {
            List<SqlExpr> newArgs = fc.args().stream()
                    .map(a -> unwrapElemRefs(a, elemParam)).toList();
            return new SqlExpr.FunctionCall(fc.name(), newArgs);
        }
        if (expr instanceof SqlExpr.Binary b) {
            return new SqlExpr.Binary(
                    unwrapElemRefs(b.left(), elemParam), b.op(),
                    unwrapElemRefs(b.right(), elemParam));
        }
        // Literals, CASE, Cast, etc. carry no identifier references we need to rewrite.
        return expr;
    }

    private static boolean isMany(TypedSpec n) {
        var info = n.info();
        return info != null && info.isMany();
    }
}
