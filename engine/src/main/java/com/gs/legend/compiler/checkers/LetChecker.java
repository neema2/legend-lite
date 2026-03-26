package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;

import java.util.Arrays;
import java.util.List;


/**
 * Signature-driven type checker for {@code letFunction()}.
 *
 * <p>Pure's {@code let} binds a local variable to a value expression:
 * {@code let x = 42; $x + 1}. The parser emits this as
 * {@code letFunction('x', 42)}.
 *
 * <p>Two overloads:
 * <ul>
 *   <li>{@code letFunction<T>(name:String[1], value:T[1]):T[1]} — standard variable binding</li>
 *   <li>{@code letFunction<T>(value:T[*]):T[*]} — expression-only let</li>
 * </ul>
 *
 * <p>Fully signature-driven: type variable T is bound from the value expression
 * via {@link #unify}, output type resolved via {@link #resolveOutput}.
 */
public class LetChecker extends AbstractChecker {

    public LetChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // 1. Resolve overload — disambiguated by arity (2-arg vs 1-arg)
        NativeFunctionDef def = resolveOverload("letFunction", params, source);

        // 2. Compile params and unify T from the value expression
        Bindings bindings;
        if (params.size() >= 2) {
            // Standard form: letFunction('varName', valueExpr)
            TypeInfo nameInfo = env.compileExpr(params.get(0), ctx);
            TypeInfo valueInfo = env.compileExpr(params.get(1), ctx);
            bindings = unify(def, Arrays.asList(
                    nameInfo.expressionType(),
                    valueInfo.expressionType()
            ));
        } else {
            // Single-param form: letFunction(valueExpr)
            TypeInfo valueInfo = env.compileExpr(params.get(0), ctx);
            bindings = unify(def, List.of(valueInfo.expressionType()));
        }

        // 3. Output type from signature return type + bindings
        ExpressionType outputType = resolveOutput(def, bindings, "letFunction()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .build();
    }
}
