package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;

import java.util.List;

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

        // 3–5. Compile lambda arg via signature (bind param, compile body, validate return)
        if (params.get(1) instanceof LambdaFunction lambda) {
            compileLambdaArg(lambda, def.params().get(1), bindings, source, ctx, "filter");
        }

        // 6. Output type from signature's return type + bindings
        ExpressionType outputType = resolveOutput(def, bindings, "filter()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .build();
    }
}
