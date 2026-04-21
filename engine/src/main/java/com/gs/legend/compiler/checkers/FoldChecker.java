package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.CollectionBuild;
import com.gs.legend.compiler.typed.Concatenation;
import com.gs.legend.compiler.typed.FoldStrategy;
import com.gs.legend.compiler.typed.MapReduce;
import com.gs.legend.compiler.typed.SameType;
import com.gs.legend.compiler.typed.TypedFold;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Signature-driven type checker + strategy classifier for {@code fold()}.
 *
 * <p>
 * Signature:
 * {@code fold<T,V>(source:T[*], lambda:{T[1],V[1]->V[1]}[1], init:V[1]):V[1]}
 *
 * <p>
 * Two responsibilities:
 * <ol>
 * <li>Type-check via resolveOverload/unify/resolve (standard checker flow)</li>
 * <li>Classify fold → stamp {@link TypeInfo.FoldSpec} (PlanGenerator reads
 * it)</li>
 * </ol>
 *
 * <p>
 * Pure type-checking only — no AST rewriting, no dialect awareness.
 * DuckDB-specific lowering is handled downstream in PlanGenerator.
 */
public class FoldChecker extends AbstractChecker {

    public FoldChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedFold check(AppliedFunction af, TypedSpec source,
            TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3) {
            throw new PureCompileException(
                    "fold() requires 3 parameters: source, lambda, init");
        }

        NativeFunctionDef def = resolveOverload("fold", params, source);

        // Compile init first so V binds from the init expression.
        TypedSpec init = env.compileExpr(params.get(2), ctx);

        ExpressionType sourceExprType = source.expressionType();
        Type sourceTypeForUnify = sourceExprType.type();
        var bindings = unify(def, Arrays.asList(
                new ExpressionType(sourceTypeForUnify, sourceExprType.multiplicity()),
                null,                         // param[1]: lambda — skip in unify
                init.expressionType()));

        if (!(params.get(1) instanceof LambdaFunction lambdaAst)) {
            throw new PureCompileException("fold() argument 2 must be a lambda");
        }

        // Early-exit for the Concatenation pattern: body is {@code add(acc, elem)},
        // structurally correct — skip {@link #compileLambdaBody} which would fail
        // on {@code add()} when T=Any (empty sources). We still need a TypedLambda
        // to carry in the HIR, so build one via the shared compile pipeline with a
        // lenient context — but simplest is to reuse {@link #compileLambdaArg} and
        // swallow any body-compile error path via the isFoldAddPattern early-exit
        // below. For now, produce a minimal placeholder TypedLambda by compiling
        // params only (body stays as the AST add() node interpreted by PlanGen).
        if (isFoldAddPattern(lambdaAst)) {
            TypedLambda reducer = compileLambdaArg(
                    lambdaAst, def.params().get(1), bindings, source, ctx, "fold");
            ExpressionType outputType = resolveOutput(def, bindings, "fold()");
            return new TypedFold(source, reducer, init, new Concatenation(), outputType);
        }

        // Signature-driven param binding for the reducer lambda, then classify.
        TypedLambda reducer = compileLambdaArg(
                lambdaAst, def.params().get(1), bindings, source, ctx, "fold");

        Type resolvedT = bindings.getOrDefault("T", Primitive.ANY);
        Type resolvedV = bindings.getOrDefault("V", Primitive.ANY);
        FoldStrategy strategy = classifyFold(lambdaAst, resolvedT, resolvedV,
                init.expressionType().multiplicity(), ctx, source);

        ExpressionType outputType = resolveOutput(def, bindings, "fold()");
        return new TypedFold(source, reducer, init, strategy, outputType);
    }

    // ==================== Strategy Classification ====================

    /**
     * Classifies fold into one of 4 strategies.
     * Order: Concatenation → SameType → MapReduce → CollectionBuild.
     * Uses resolved T, V from unify() and the init's compiled multiplicity.
     */
    private FoldStrategy classifyFold(LambdaFunction lambda,
            Type resolvedT, Type resolvedV,
            Multiplicity initMultiplicity,
            TypeChecker.CompilationContext ctx,
            TypedSpec source) {
        // 1. Concatenation: body is {@code add(acc, elem)}.
        if (isFoldAddPattern(lambda)) return new Concatenation();

        // 2. SameType: T == V, but only when init is scalar. List init implies
        // a collection accumulator → CollectionBuild. Normalize PrecisionDecimal
        // → DECIMAL so accumulators of any precision count as the same kind
        // (matches the convention in {@code AbstractChecker.unifyTypeVar}).
        Type tNorm = resolvedT instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : resolvedT;
        Type vNorm = resolvedV instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : resolvedV;
        if (tNorm.equals(vNorm) && !initMultiplicity.isMany()) return new SameType();

        // 3. T ≠ V: try decomposing body → MapReduce.
        String accParam = lambda.parameters().size() >= 2
                ? lambda.parameters().get(1).name()
                : "y";
        ValueSpecification transform = extractElementTransform(
                lambda.body().get(0), accParam);
        if (transform != null) {
            // Compile the transform with elem param bound to T.
            String elemParam = lambda.parameters().get(0).name();
            var transformCtx = bindLambdaParam(ctx, elemParam, resolvedT, source);
            TypedSpec transformTyped = env.compileExpr(transform, transformCtx);

            // Build and compile the reducer: {@code op(acc, __mr_x)} with both
            // params bound to V.
            String freshParam = "__mr_x";
            String fullFunctionRef = (lambda.body().get(0) instanceof AppliedFunction af)
                    ? af.function() : "plus";
            var accVar = new Variable(accParam);
            var freshVar = new Variable(freshParam);
            var reducerBodyAst = new AppliedFunction(fullFunctionRef,
                    java.util.List.of(accVar, freshVar));

            var reducerCtx = bindLambdaParam(ctx, accParam, resolvedV, source);
            reducerCtx = bindLambdaParam(reducerCtx, freshParam, resolvedV, source);
            TypedSpec reducerBodyTyped = env.compileExpr(reducerBodyAst, reducerCtx);

            return new MapReduce(transformTyped, reducerBodyTyped, accParam, freshParam);
        }

        // 4. T ≠ V, not decomposable → CollectionBuild (marker).
        return new CollectionBuild();
    }

    // ==================== Helpers ====================

    /**
     * True if body is the identity-add pattern: {e, a | $a->add($e)}.
     */
    private static boolean isFoldAddPattern(LambdaFunction lf) {
        if (lf.parameters().size() < 2 || lf.body().isEmpty())
            return false;
        String elemParam = lf.parameters().get(0).name();
        String accParam = lf.parameters().get(1).name();
        if (lf.body().get(0) instanceof AppliedFunction bodyAf
                && SymbolTable.extractSimpleName(bodyAf.function()).equals("add")
                && bodyAf.parameters().size() == 2) {
            var addSource = bodyAf.parameters().get(0);
            var addElem = bodyAf.parameters().get(1);
            return addSource instanceof Variable accVar && accVar.name().equals(accParam)
                    && addElem instanceof Variable elemVar && elemVar.name().equals(elemParam);
        }
        return false;
    }

    /**
     * Extracts element-only transform by stripping the accumulator from
     * the left spine of ANY binary op chain (not just plus).
     *
     * <p>
     * Example: {@code plus(plus(acc, '; '), p.name)} → {@code plus('; ', p.name)}
     * <p>
     * Example: {@code times(acc, length(x))} → {@code length(x)}
     *
     * @return element-only subtree, or null if not decomposable
     */
    private static ValueSpecification extractElementTransform(
            ValueSpecification body, String accParam) {
        if (!(body instanceof AppliedFunction af))
            return null;
        if (af.parameters().size() != 2)
            return null;
        String op = SymbolTable.extractSimpleName(af.function());

        ValueSpecification left = af.parameters().get(0);
        ValueSpecification right = af.parameters().get(1);

        // Base case: left is acc variable → return right
        if (left instanceof Variable v && v.name().equals(accParam))
            return right;

        // Recursive: left is same op chain containing acc
        if (left instanceof AppliedFunction leftAf
                && SymbolTable.extractSimpleName(leftAf.function()).equals(op)) {
            ValueSpecification stripped = extractElementTransform(left, accParam);
            if (stripped != null)
                return new AppliedFunction(af.function(), List.of(stripped, right));
        }

        return null;
    }

}
