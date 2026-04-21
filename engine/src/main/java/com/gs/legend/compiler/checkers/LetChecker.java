package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedLet;
import com.gs.legend.compiler.typed.TypedSpec;

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

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("letFunction", params, source);

        // Compile the value expression + (when present) the name literal. Both
        // contribute to the unification actuals so T binds from the value arg.
        if (params.size() >= 2) {
            TypedSpec nameTyped  = env.compileExpr(params.get(0), ctx);
            TypedSpec valueTyped = env.compileExpr(params.get(1), ctx);
            var bindings = unify(def, Arrays.asList(
                    nameTyped.expressionType(), valueTyped.expressionType()));
            ExpressionType outputType = resolveOutput(def, bindings, "letFunction()");
            String name = ((CString) params.get(0)).value();
            return new TypedLet(name, valueTyped, outputType);
        }

        // Single-param form: {@code letFunction(valueExpr)} is a no-op wrapper —
        // typewise identical to its value. Collapse to the value's typed HIR.
        return env.compileExpr(params.get(0), ctx);
    }
}
