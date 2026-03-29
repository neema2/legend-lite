package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker + strategy classifier for {@code fold()}.
 *
 * <p>Signature: {@code fold<T,V>(source:T[*], lambda:{T[1],V[1]->V[1]}[1], init:V[1]):V[1]}
 *
 * <p>Two responsibilities:
 * <ol>
 *   <li>Type-check via resolveOverload/unify/resolve (standard checker flow)</li>
 *   <li>Classify fold → stamp {@link TypeInfo.FoldSpec} (PlanGenerator reads it)</li>
 * </ol>
 *
 * <p>Pure type-checking only — no AST rewriting, no dialect awareness.
 * DuckDB-specific lowering is handled downstream in PlanGenerator.
 */
public class FoldChecker extends AbstractChecker {

    public FoldChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3)
            throw new PureCompileException("fold() requires 3 parameters: source, lambda, init");

        // 1. Resolve overload
        NativeFunctionDef def = resolveOverload("fold", params, source);

        // 2. Compile init to bind V
        TypeInfo initInfo = env.compileExpr(params.get(2), ctx);

        // 3. Unify: T from source, V from init
        ExpressionType sourceExprType = source.expressionType();
        GenericType sourceTypeForUnify = sourceExprType.type();
        var bindings = unify(def, Arrays.asList(
                new ExpressionType(sourceTypeForUnify, sourceExprType.multiplicity()),  // param[0]: T[*]
                null,                                                // param[1]: lambda — skip
                initInfo.expressionType()                           // param[2]: V[1]
        ));

        // 4. Compile lambda via signature-driven param binding
        if (!(params.get(1) instanceof LambdaFunction lambda))
            throw new PureCompileException("fold() argument 2 must be a lambda");

        // 4a. Early-exit for Concatenation: body is add(acc, elem) — structurally correct,
        //     skip compileLambdaBody which would fail on add() when T=Any (empty sources).
        if (isFoldAddPattern(lambda)) {
            ExpressionType outputType = resolveOutput(def, bindings, "fold()");
            return TypeInfo.builder()
                    .expressionType(outputType)
                    .foldSpec(new TypeInfo.FoldSpec.Concatenation())
                    .build();
        }

        PType.FunctionType ft = extractFunctionType(def.params().get(1));

        TypeChecker.CompilationContext lambdaCtx = ctx;
        for (int p = 0; p < lambda.parameters().size() && p < ft.paramTypes().size(); p++) {
            String paramName = lambda.parameters().get(p).name();
            GenericType resolvedParamType = resolve(ft.paramTypes().get(p).type(), bindings,
                    "fold() lambda param '" + paramName + "'");
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedParamType, source);
        }
        compileLambdaBody(lambda, lambdaCtx);

        // 5. Classify strategy from resolved T, V
        GenericType resolvedT = bindings.getOrDefault("T", GenericType.Primitive.ANY);
        GenericType resolvedV = bindings.getOrDefault("V", GenericType.Primitive.ANY);
        TypeInfo.FoldSpec spec = classifyFold(lambda, resolvedT, resolvedV, ctx, source);

        // 6. Output type + FoldSpec
        ExpressionType outputType = resolveOutput(def, bindings, "fold()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .foldSpec(spec)
                .build();
    }

    // ==================== Strategy Classification ====================

    /**
     * Classifies fold into one of 4 strategies.
     * Order: Concatenation → SameType → MapReduce → CollectionBuild.
     * Uses resolved T, V from unify() — no null checks, no fallbacks.
     */
    private TypeInfo.FoldSpec classifyFold(LambdaFunction lambda,
                                           GenericType resolvedT, GenericType resolvedV,
                                           TypeChecker.CompilationContext ctx,
                                           TypeInfo source) {
        // 1. Concatenation: body is add(acc, elem)
        if (isFoldAddPattern(lambda))
            return new TypeInfo.FoldSpec.Concatenation();

        // 2. SameType: T == V
        if (resolvedT.typeName().equals(resolvedV.typeName()))
            return new TypeInfo.FoldSpec.SameType();

        // 3. T ≠ V: try decomposing body → MapReduce
        String accParam = lambda.parameters().size() >= 2
                ? lambda.parameters().get(1).name() : "y";
        ValueSpecification transform = extractElementTransform(
                lambda.body().get(0), accParam);
        if (transform != null) {
            // extractElementTransform creates synthetic wrapper nodes — compile to stamp them
            String elemParam = lambda.parameters().get(0).name();
            var transformCtx = bindLambdaParam(ctx, elemParam, resolvedT, source);
            env.compileExpr(transform, transformCtx);

            // Build and compile the reducer: op(acc, __mr_x) with both params bound to V
            String freshParam = "__mr_x";
            String fullFunctionRef = (lambda.body().get(0) instanceof AppliedFunction af)
                    ? af.function() : "plus";
            var accVar = new Variable(accParam);
            var freshVar = new Variable(freshParam);
            var reducerBody = new AppliedFunction(fullFunctionRef,
                    java.util.List.of(accVar, freshVar));

            // Compile in a context with both params typed as V
            var reducerCtx = bindLambdaParam(ctx, accParam, resolvedV, source);
            reducerCtx = bindLambdaParam(reducerCtx, freshParam, resolvedV, source);
            env.compileExpr(reducerBody, reducerCtx);

            return new TypeInfo.FoldSpec.MapReduce(transform, reducerBody, accParam, freshParam);
        }

        // 4. T ≠ V, not decomposable → CollectionBuild (marker)
        return new TypeInfo.FoldSpec.CollectionBuild();
    }

    // ==================== Helpers ====================

    /**
     * True if body is the identity-add pattern: {e, a | $a->add($e)}.
     */
    private static boolean isFoldAddPattern(LambdaFunction lf) {
        if (lf.parameters().size() < 2 || lf.body().isEmpty()) return false;
        String elemParam = lf.parameters().get(0).name();
        String accParam = lf.parameters().get(1).name();
        if (lf.body().get(0) instanceof AppliedFunction bodyAf
                && TypeInfo.simpleName(bodyAf.function()).equals("add")
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
     * <p>Example: {@code plus(plus(acc, '; '), p.name)} → {@code plus('; ', p.name)}
     * <p>Example: {@code times(acc, length(x))} → {@code length(x)}
     *
     * @return element-only subtree, or null if not decomposable
     */
    private static ValueSpecification extractElementTransform(
            ValueSpecification body, String accParam) {
        if (!(body instanceof AppliedFunction af)) return null;
        if (af.parameters().size() != 2) return null;
        String op = TypeInfo.simpleName(af.function());

        ValueSpecification left = af.parameters().get(0);
        ValueSpecification right = af.parameters().get(1);

        // Base case: left is acc variable → return right
        if (left instanceof Variable v && v.name().equals(accParam))
            return right;

        // Recursive: left is same op chain containing acc
        if (left instanceof AppliedFunction leftAf
                && TypeInfo.simpleName(leftAf.function()).equals(op)) {
            ValueSpecification stripped = extractElementTransform(left, accParam);
            if (stripped != null)
                return new AppliedFunction(af.function(), List.of(stripped, right));
        }

        return null;
    }

}
