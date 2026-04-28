package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.JoinType;
import com.gs.legend.compiler.typed.TypedJoin;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Signature-driven type checker for {@code join()} — Relation API.
 *
 * <p>Registered signatures:
 * <pre>
 * join&lt;T,V&gt;(rel1:Relation&lt;T&gt;[1], rel2:Relation&lt;V&gt;[1],
 *           joinKind:JoinKind[1],
 *           f:Function&lt;{T[1],V[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;T+V&gt;[1]
 *
 * join&lt;T,V&gt;(rel1:Relation&lt;T&gt;[1], rel2:Relation&lt;V&gt;[1],
 *           joinKind:JoinKind[1],
 *           f:Function&lt;{T[1],V[1]-&gt;Boolean[1]}&gt;[1],
 *           prefix:String[1]):Relation&lt;T+V&gt;[1]
 * </pre>
 *
 * <p>Flow:
 * <ol>
 *   <li>{@link #resolveOverload} — picks 4-param or 5-param overload</li>
 *   <li>{@link #unify(NativeFunctionDef, List)} — binds T (left schema) and V (right schema)</li>
 *   <li>Compile condition lambda with <b>original</b> T, V (validates {@code $l.id == $r.id})</li>
 *   <li>If prefix present: apply rename to V binding (prefix duplicate columns)</li>
 *   <li>{@link #resolveOutput} — computes {@code Relation<T+V'>} via SchemaAlgebra Union</li>
 * </ol>
 *
 * <p>The prefix param is syntactic sugar for {@code rename()} on the right source.
 * The compiler desugars it at the type-variable binding level: V is mutated in the
 * bindings map <b>after</b> lambda validation but <b>before</b> output resolution.
 * This keeps join fully signature-driven while preserving the prefix UX.
 *
 * @see AbstractChecker
 */
public class JoinChecker extends AbstractChecker {

    public JoinChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedJoin check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("join", params, source);

        TypedSpec left = source;
        TypedSpec right = env.compileExpr(params.get(1), ctx);

        ExpressionType leftExpr = left.expressionType();
        ExpressionType rightExpr = right.expressionType();
        var bindings = unify(def, Arrays.asList(leftExpr, rightExpr, null, null));

        JoinType joinType = JoinType.valueOf(resolveJoinType(params));

        Type.Schema leftSchema = left.schema();
        Type.Schema rightSchema = right.schema();

        TypedLambda condition = null;
        int conditionIdx = 3;
        if (conditionIdx < params.size()
                && params.get(conditionIdx) instanceof LambdaFunction lambda) {
            condition = compileConditionLambda(
                    lambda, def, conditionIdx, bindings, left, right, ctx);
        }

        String rightPrefix = extractRightPrefix(params);
        Map<String, String> renames = applyPrefixToBindings(
                bindings, leftSchema, rightSchema, rightPrefix);

        ExpressionType outputExpr = resolveOutput(def, bindings, "join");
        return new TypedJoin(left, right, condition, joinType, renames, def, outputExpr);
    }

    // ========== Helpers ==========

    /**
     * Applies right-prefix renaming by mutating the V binding in-place.
     * Returns the rename map (original → prefixed) for PlanGenerator.
     *
     * <p>When a prefix is provided, ALL right-side columns are renamed to
     * {@code prefix_originalName}. This makes output column names fully
     * deterministic — no conflict detection needed.
     *
     * <p>If no prefix is provided and there are duplicate columns, throws.
     */
    private Map<String, String> applyPrefixToBindings(
            Bindings bindings,
            Type.Schema leftSchema,
            Type.Schema rightSchema,
            String rightPrefix) {

        if (rightPrefix == null) {
            // No prefix: check for duplicates
            Set<String> leftColNames = leftSchema.columns().keySet();
            for (String name : rightSchema.columns().keySet()) {
                if (leftColNames.contains(name)) {
                    throw new PureCompileException(
                            "Join produces duplicate column '" + name
                                    + "'. Supply a right-side prefix parameter to disambiguate: "
                                    + "->join(right, JoinType.INNER, {l, r | ...}, 'prefix')");
                }
            }
            return Map.of();
        }

        // Prefix provided: rename ALL right-side columns to prefix_name
        Map<String, Type> prefixedColumns = new LinkedHashMap<>();
        Map<String, String> renames = new LinkedHashMap<>();
        for (var entry : rightSchema.columns().entrySet()) {
            String prefixed = rightPrefix + "_" + entry.getKey();
            prefixedColumns.put(prefixed, entry.getValue());
            renames.put(entry.getKey(), prefixed);
        }

        // Mutate V binding: replace original Tuple with prefixed Tuple
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
                    "join() condition lambda has " + lambda.parameters().size()
                            + " params, signature requires " + ft.params().size());
        }

        TypeChecker.CompilationContext lambdaCtx = ctx;
        TypedSpec[] sources = { left, right };
        List<com.gs.legend.compiler.typed.TypedParam> typedParams =
                new ArrayList<>(ft.params().size());
        for (int i = 0; i < ft.params().size(); i++) {
            String paramName = lambda.parameters().get(i).name();
            Type resolvedType = resolve(ft.params().get(i).type(), bindings,
                    "join() condition param " + i);
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedType, sources[i]);
            typedParams.add(new com.gs.legend.compiler.typed.TypedParam(
                    paramName, resolvedType,
                    com.gs.legend.model.m3.Multiplicity.ONE));
        }

        TypedSpec body = compileLambdaBody(lambda, lambdaCtx);
        validateLambdaReturn(body, ft, bindings, "join");
        return new TypedLambda(typedParams, List.of(body), body.expressionType());
    }

    /**
     * Extracts join type from params[2].
     * The signature declares {@code joinKind:JoinKind[1]} — must be an enum value.
     */
    static String resolveJoinType(List<ValueSpecification> params) {
        if (params.size() < 3) return "INNER";
        ValueSpecification joinTypeParam = params.get(2);

        if (joinTypeParam instanceof EnumValue ev) {
            return ev.value().toUpperCase();
        }

        throw new PureCompileException(
                "join(): joinKind must be a JoinKind enum value (e.g., JoinKind.INNER), got "
                        + joinTypeParam.getClass().getSimpleName());
    }

    /**
     * Extracts optional right-side prefix (CString) from the last param.
     * join(left, right, JoinType, condition, 'prefix') — param[4]
     */
    static String extractRightPrefix(List<ValueSpecification> params) {
        if (params.size() >= 5
                && params.get(4) instanceof CString(String value)) {
            return value;
        }
        return null;
    }
}
