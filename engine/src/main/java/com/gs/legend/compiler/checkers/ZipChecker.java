package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;

import java.util.ArrayList;
import java.util.List;


/**
 * Signature-driven type checker for {@code zip()}.
 *
 * <p>{@code zip(set1, set2)} pairs two collections element-wise.
 * Signature: {@code zip<T,U>(set1:T[*], set2:U[*]):Pair<T,U>[*]}
 *
 * <p>T and U are bound from the element types of both collections via
 * {@link #unify}, and the output {@code Pair<T,U>[*]} is resolved
 * via {@link #resolveOutput}.
 */
public class ZipChecker extends AbstractChecker {

    public ZipChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // 1. Resolve overload — zip always has arity 2
        NativeFunctionDef def = resolveOverload("zip", params, source);

        // 2. Compile remaining params and collect ExpressionTypes
        List<ExpressionType> actuals = new ArrayList<>();
        actuals.add(source.expressionType());  // param[0] already compiled by caller
        for (int i = 1; i < params.size(); i++) {
            TypeInfo argInfo = env.compileExpr(params.get(i), ctx);
            actuals.add(argInfo.expressionType());
        }

        // 3. Unify type variables — bind T from set1, U from set2
        var bindings = unify(def, actuals);

        // 4. Output type from signature — Pair<T,U>[*]
        ExpressionType outputType = resolveOutput(def, bindings, "zip()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .build();
    }
}
