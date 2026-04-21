package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedSpec;

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

    public TypedNativeCall check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("zip", params, source);

        // Compile non-source args; collect typed children + ExpressionTypes for
        // unification (T from set1, U from set2).
        List<ExpressionType> actuals = new ArrayList<>(params.size());
        List<TypedSpec> args = new ArrayList<>(params.size());
        actuals.add(source.expressionType());
        args.add(source);
        for (int i = 1; i < params.size(); i++) {
            TypedSpec arg = env.compileExpr(params.get(i), ctx);
            actuals.add(arg.expressionType());
            args.add(arg);
        }

        var bindings = unify(def, actuals);
        ExpressionType outputType = resolveOutput(def, bindings, "zip()"); // Pair<T,U>[*]
        return new TypedNativeCall(def, args, outputType);
    }
}
