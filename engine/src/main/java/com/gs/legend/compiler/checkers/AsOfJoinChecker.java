package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedAsOfJoin;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Signature-driven type checker for {@code asOfJoin()} — Relation API.
 *
 * <p>Registered signatures:
 * <pre>
 * asOfJoin&lt;T,V&gt;(rel1:Relation&lt;T&gt;[1], rel2:Relation&lt;V&gt;[1],
 *               match:Function&lt;{T[1],V[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;T+V&gt;[1]
 * asOfJoin&lt;T,V&gt;(rel1:Relation&lt;T&gt;[1], rel2:Relation&lt;V&gt;[1],
 *               match:Function&lt;{T[1],V[1]-&gt;Boolean[1]}&gt;[1],
 *               join:Function&lt;{T[1],V[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;T+V&gt;[1]
 * asOfJoin&lt;T,V&gt;(..., prefix:String[1]):Relation&lt;T+V&gt;[1]  (4- and 5-param variants)
 * </pre>
 *
 * <p>Same signature-driven pattern as {@link JoinChecker}:
 * resolveOverload → unify → compile lambdas with original schemas →
 * prefix-as-binding-mutation → resolveOutput.
 *
 * @see JoinChecker
 */
public class AsOfJoinChecker extends AbstractChecker {

    public AsOfJoinChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedAsOfJoin check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("asOfJoin", params, source);

        TypedSpec left = source;
        TypedSpec right = env.compileExpr(params.get(1), ctx);

        ExpressionType leftExpr = left.expressionType();
        ExpressionType rightExpr = right.expressionType();
        var bindings = unify(def, Arrays.asList(leftExpr, rightExpr, null, null));

        Type.Schema leftSchema = left.schema();
        Type.Schema rightSchema = right.schema();

        TypedLambda matchCondition = null;
        if (params.get(2) instanceof LambdaFunction matchLambda) {
            matchCondition = compileConditionLambda(
                    matchLambda, def, 2, bindings, left, right, ctx);
        }

        TypedLambda keyCondition = null;
        if (params.size() >= 4
                && params.get(3) instanceof LambdaFunction keyLambda) {
            keyCondition = compileConditionLambda(
                    keyLambda, def, 3, bindings, left, right, ctx);
        }

        String rightPrefix = extractRightPrefix(params);
        Map<String, String> renames = applyPrefixToBindings(
                bindings, leftSchema, rightSchema, rightPrefix);

        ExpressionType outputExpr = resolveOutput(def, bindings, "asOfJoin");
        return new TypedAsOfJoin(left, right, matchCondition,
                Optional.ofNullable(keyCondition), renames, outputExpr);
    }

    // ========== Helpers ==========

    /**
     * Applies right-prefix renaming by mutating the V binding.
     * Same logic as {@link JoinChecker#applyPrefixToBindings}.
     */
    private Map<String, String> applyPrefixToBindings(
            Bindings bindings,
            Type.Schema leftSchema,
            Type.Schema rightSchema,
            String rightPrefix) {


        Set<String> leftColNames = leftSchema.columns().keySet();
        Set<String> duplicates = new LinkedHashSet<>();
        for (String name : rightSchema.columns().keySet()) {
            if (leftColNames.contains(name)) {
                duplicates.add(name);
            }
        }

        if (duplicates.isEmpty()) {
            return Map.of();
        }

        if (rightPrefix == null) {
            throw new PureCompileException(
                    "asOfJoin produces duplicate columns " + duplicates
                            + ". Supply a right-side prefix parameter to disambiguate: "
                            + "->asOfJoin(right, {t, q | ...}, {t, q | ...}, 'prefix')");
        }

        Map<String, Type> prefixedColumns = new LinkedHashMap<>();
        Map<String, String> renames = new LinkedHashMap<>();
        for (var entry : rightSchema.columns().entrySet()) {
            String name = entry.getKey();
            if (duplicates.contains(name)) {
                String prefixed = rightPrefix + "_" + name;
                prefixedColumns.put(prefixed, entry.getValue());
                renames.put(name, prefixed);
            } else {
                prefixedColumns.put(name, entry.getValue());
            }
        }

        var prefixedSchema = Type.Schema.withoutPivot(prefixedColumns);
        bindings.put("V", new Type.Tuple(prefixedSchema));

        return renames;
    }

    /**
     * Compiles a join condition lambda — fully signature-driven.
     *
     * <p>Extracts the FunctionType from the signature param at {@code paramIdx},
     * resolves each lambda param type from bindings, binds them, and compiles the body.
     */
    private TypedLambda compileConditionLambda(LambdaFunction lambda,
                                         NativeFunctionDef def, int paramIdx,
                                         Bindings bindings,
                                         TypedSpec left, TypedSpec right,
                                         TypeChecker.CompilationContext ctx) {
        if (lambda.parameters().isEmpty() || lambda.body().isEmpty()) return null;

        Type.FunctionType ft = extractFunctionType(def.params().get(paramIdx));
        if (lambda.parameters().size() != ft.params().size()) {
            throw new PureCompileException(
                    "asOfJoin() condition lambda has " + lambda.parameters().size()
                            + " params, signature requires " + ft.params().size());
        }

        TypeChecker.CompilationContext lambdaCtx = ctx;
        TypedSpec[] sources = { left, right };
        List<com.gs.legend.compiler.typed.TypedParam> typedParams =
                new ArrayList<>(ft.params().size());
        for (int i = 0; i < ft.params().size(); i++) {
            String paramName = lambda.parameters().get(i).name();
            Type resolvedType = resolve(ft.params().get(i).type(), bindings,
                    "asOfJoin() condition param " + i);
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedType, sources[i]);
            typedParams.add(new com.gs.legend.compiler.typed.TypedParam(
                    paramName, resolvedType,
                    com.gs.legend.model.m3.Multiplicity.ONE));
        }

        TypedSpec body = compileLambdaBody(lambda, lambdaCtx);
        validateLambdaReturn(body, ft, bindings, "asOfJoin");
        return new TypedLambda(typedParams, List.of(body), body.expressionType());
    }

    /**
     * Extracts optional right-side prefix (CString) — scans from index 3 onward.
     */
    private static String extractRightPrefix(List<ValueSpecification> params) {
        for (int i = 3; i < params.size(); i++) {
            if (params.get(i) instanceof CString(String value)) {
                return value;
            }
        }
        return null;
    }
}
