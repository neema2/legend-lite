package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;

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

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // 1. Resolve overload (1-arg vs 2-arg form)
        NativeFunctionDef def = resolveOverload("write", params, source);

        // 2. Compile all params and collect ExpressionTypes for unification
        List<ExpressionType> actuals = new ArrayList<>();
        actuals.add(source.expressionType());  // param[0] already compiled by caller
        for (int i = 1; i < params.size(); i++) {
            TypeInfo argInfo = env.compileExpr(params.get(i), ctx);
            actuals.add(argInfo.expressionType());
        }

        // 3. Unify type variables — bind T from source relation
        var bindings = unify(def, actuals);

        // 4. Output type from signature return type + bindings → Integer[1]
        ExpressionType outputType = resolveOutput(def, bindings, "write()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .build();
    }
}
