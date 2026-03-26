package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;
import java.util.Map;

/**
 * Signature-driven type checker for {@code filter()}.
 *
 * <p>Two overloads:
 * <ul>
 *   <li>Relational: {@code filter<T>(rel:Relation<T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1]}</li>
 *   <li>Collection: {@code filter<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):T[*]}</li>
 * </ul>
 *
 * <p>Fully signature-driven: type variable T is bound via {@link #unify}, lambda param
 * type is resolved via {@link #resolve}, output constructed via {@link #resolveOutput}.
 */
public class FilterChecker extends AbstractChecker {

    public FilterChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("filter", params, source);

        // 2. Validate source + bind type variables (one pass)
        var bindings = unify(def, source.expressionType());

        // 3. Lambda structure (from signature param[1] → FunctionType)
        PType.FunctionType ft = extractFunctionType(def.params().get(1));
        if (!(params.get(1) instanceof LambdaFunction lambda)) {
            throw new PureCompileException("filter() argument 2 must be a lambda");
        }
        if (lambda.parameters().size() != ft.paramTypes().size()) {
            throw new PureCompileException(
                    "filter() lambda has " + lambda.parameters().size()
                            + " params, signature requires " + ft.paramTypes().size());
        }

        // 4. Bind lambda param using resolved type variable
        String paramName = lambda.parameters().get(0).name();
        GenericType resolvedParamType = resolve(ft.paramTypes().get(0).type(), bindings,
                "filter() lambda param");
        TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(ctx, paramName,
                resolvedParamType, source);

        // 5. Compile lambda body + validate return (from FunctionType + bindings)
        TypeInfo bodyType = compileLambdaBody(lambda, lambdaCtx);
        validateLambdaReturn(bodyType, ft, bindings, "filter");

        // 6. Resolve associations (class-based or relational with mapping)
        Map<String, TypeInfo.AssociationTarget> associations = Map.of();
        if (source.mapping() != null) {
            associations = env.resolveAssociations(lambda.body(), source.mapping());
        }

        // 7. Output type from signature's return type + bindings
        ExpressionType outputType = resolveOutput(def, bindings, "filter()");
        return TypeInfo.builder()
                .mapping(source.mapping())
                .associations(associations)
                .expressionType(outputType)
                .build();
    }
}
