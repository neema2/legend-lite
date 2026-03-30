package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;
import java.util.Map;

/**
 * Signature-driven type checker for {@code map()}.
 *
 * <p>Two overloads:
 * <ul>
 *   <li>Collection: {@code map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>):V[*]}</li>
 *   <li>Optional:   {@code map<T,V>(value:T[0..1], func:Function<{T[1]->V[0..1]}>):V[0..1]}</li>
 * </ul>
 *
 * <p>Fully signature-driven: T is bound from source via {@link #unify}, lambda
 * param type resolved via {@link #resolve}, lambda body compiled to determine V,
 * and output type constructed via {@link #resolveOutput}.
 *
 * <p>Unlike {@code filter()} where the output type equals the source type,
 * {@code map()} transforms element type T to V. The lambda body's return type
 * determines V, which is then used to resolve the output multiplicity from the
 * matched signature overload.
 */
public class MapChecker extends AbstractChecker {

    public MapChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2)
            throw new PureCompileException("map() requires 2 parameters: source, func");

        // 1. Resolve overload (T[*] vs T[0..1]) using compiled source type.
        //    T[*] overload = collection map (iterate elements, returns V[*])
        //    T[0..1] overload = optional map (null-safe transform, returns V[0..1])
        //    Multiplicity scoring disambiguates: [0..1] source prefers T[0..1],
        //    [*] source prefers T[*].
        NativeFunctionDef def = resolveOverload("map", params, source,
                Map.of(0, source.expressionType()));

        // 2. Unify: bind T from source expression type
        var bindings = unify(def, source.expressionType());

        // 3. Extract lambda FunctionType from signature param[1]
        PType.FunctionType ft = extractFunctionType(def.params().get(1));
        if (!(params.get(1) instanceof LambdaFunction lambda)) {
            throw new PureCompileException("map() argument 2 must be a lambda");
        }
        if (lambda.parameters().size() != ft.paramTypes().size()) {
            throw new PureCompileException(
                    "map() lambda has " + lambda.parameters().size()
                            + " params, signature requires " + ft.paramTypes().size());
        }

        // 4. Bind lambda param using resolved T
        String paramName = lambda.parameters().get(0).name();
        GenericType resolvedParamType = resolve(ft.paramTypes().get(0).type(), bindings,
                "map() lambda param");
        TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(ctx, paramName,
                resolvedParamType, source);

        // 5. Compile lambda body to determine V
        TypeInfo bodyType = compileLambdaBody(lambda, lambdaCtx);

        // 6. Bind V from lambda body's return type
        //    The signature has V as a type variable — we bind it from the actual
        //    lambda result so resolveOutput can construct the correct return type.
        GenericType bodyGenericType = bodyType.type();
        if (bodyGenericType != null) {
            bindings.put("V", bodyGenericType);
        }

        // 7. Output type from signature return: V[*] or V[0..1]
        ExpressionType outputType = resolveOutput(def, bindings, "map()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .build();
    }
}
