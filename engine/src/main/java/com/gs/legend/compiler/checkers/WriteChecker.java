package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedWrite;

import java.util.ArrayList;
import java.util.List;


/**
 * Signature-driven type checker for {@code write()}.
 *
 * <p>{@code write()} is a mutation operation that writes relation data
 * to a target table. The source must be a relation; the output is
 * always {@code Integer[1]} (count of rows written).
 *
 * <p>Overloads:
 * <ul>
 *   <li>{@code write<T>(source:Relation<T>[1], target:Any[1]):Integer[1]}</li>
 *   <li>{@code write<T>(source:Relation<T>[1]):Integer[1]}</li>
 * </ul>
 *
 * <p>Fully signature-driven: T is bound from source via {@link #unify},
 * output type (Integer[1]) resolved via {@link #resolveOutput}.
 */
public class WriteChecker extends AbstractChecker {

    public WriteChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedWrite check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("write", params, source);

        // Compile non-source args (the optional destination) and collect
        // ExpressionTypes for unification. {@code source} is already compiled.
        List<ExpressionType> actuals = new ArrayList<>(params.size());
        actuals.add(source.expressionType());
        TypedSpec destination = null;
        for (int i = 1; i < params.size(); i++) {
            TypedSpec arg = env.compileExpr(params.get(i), ctx);
            actuals.add(arg.expressionType());
            if (i == 1) destination = arg;
        }

        var bindings = unify(def, actuals);
        ExpressionType outputType = resolveOutput(def, bindings, "write()"); // Integer[1]
        return new TypedWrite(source, destination, outputType);
    }
}
