package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

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
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // --- 1. Resolve overload (3, 4, or 5-param) ---
        NativeFunctionDef def = resolveOverload("asOfJoin", params, source);

        TypeInfo left = source;

        // --- 2. Compile right source (param[1]) ---
        TypeInfo right = env.compileExpr(params.get(1), ctx);

        // --- 3. Unify: bind T from left, V from right ---
        ExpressionType leftExpr = left.expressionType();
        ExpressionType rightExpr = right.expressionType();
        var bindings = unify(def, Arrays.asList(leftExpr, rightExpr, null, null));

        // --- 4. Compile condition lambdas — signature-driven ---
        GenericType.Relation.Schema leftSchema = left.schema();
        GenericType.Relation.Schema rightSchema = right.schema();

        // Match condition (param[2])
        if (params.get(2) instanceof LambdaFunction matchLambda) {
            compileConditionLambda(matchLambda, def, 2, bindings, left, right, ctx);
        }

        // Optional key condition (param[3], only if it's a lambda, not a CString prefix)
        if (params.size() >= 4
                && params.get(3) instanceof LambdaFunction keyLambda) {
            compileConditionLambda(keyLambda, def, 3, bindings, left, right, ctx);
        }

        // --- 5. Prefix-as-binding-mutation ---
        String rightPrefix = extractRightPrefix(params);
        Map<String, String> renames = applyPrefixToBindings(
                bindings, leftSchema, rightSchema, rightPrefix);

        // --- 6. resolveOutput computes Relation<T+V'> ---
        ExpressionType outputExpr = resolveOutput(def, bindings, "asOfJoin");

        return TypeInfo.builder()
                .joinColumnRenames(renames)
                .expressionType(outputExpr)
                .build();
    }

    // ========== Helpers ==========

    /**
     * Applies right-prefix renaming by mutating the V binding.
     * Same logic as {@link JoinChecker#applyPrefixToBindings}.
     */
    private Map<String, String> applyPrefixToBindings(
            Bindings bindings,
            GenericType.Relation.Schema leftSchema,
            GenericType.Relation.Schema rightSchema,
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

        Map<String, GenericType> prefixedColumns = new LinkedHashMap<>();
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

        var prefixedSchema = GenericType.Relation.Schema.withoutPivot(prefixedColumns);
        bindings.put("V", new GenericType.Tuple(prefixedSchema));

        return renames;
    }

    /**
     * Compiles a join condition lambda — fully signature-driven.
     *
     * <p>Extracts the FunctionType from the signature param at {@code paramIdx},
     * resolves each lambda param type from bindings, binds them, and compiles the body.
     */
    private void compileConditionLambda(LambdaFunction lambda,
                                         NativeFunctionDef def, int paramIdx,
                                         Bindings bindings,
                                         TypeInfo left, TypeInfo right,
                                         TypeChecker.CompilationContext ctx) {
        if (lambda.parameters().isEmpty() || lambda.body().isEmpty()) return;

        PType.FunctionType ft = extractFunctionType(def.params().get(paramIdx));
        if (lambda.parameters().size() != ft.paramTypes().size()) {
            throw new PureCompileException(
                    "asOfJoin() condition lambda has " + lambda.parameters().size()
                            + " params, signature requires " + ft.paramTypes().size());
        }

        TypeChecker.CompilationContext lambdaCtx = ctx;
        TypeInfo[] sources = { left, right };
        for (int i = 0; i < ft.paramTypes().size(); i++) {
            String paramName = lambda.parameters().get(i).name();
            GenericType resolvedType = resolve(ft.paramTypes().get(i).type(), bindings,
                    "asOfJoin() condition param " + i);
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedType, sources[i]);
        }

        TypeInfo bodyType = compileLambdaBody(lambda, lambdaCtx);
        validateLambdaReturn(bodyType, ft, bindings, "asOfJoin");
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
