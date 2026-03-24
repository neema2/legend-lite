package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Signature-driven type checker for {@code from()}.
 *
 * <p>{@code from()} is a runtime binding — it tells the engine which
 * Runtime to use for execution. Type-wise it is a passthrough: the
 * output type equals the source type.
 *
 * <p>Overloads:
 * <ul>
 *   <li>{@code from<T>(source:Relation<T>[1], runtime:Any[1]):Relation<T>[1]}</li>
 *   <li>{@code from<T>(source:Relation<T>[1]):Relation<T>[1]}</li>
 *   <li>{@code from<T>(source:T[*], mapping:Any[1], runtime:Any[1]):T[*]} — M2M</li>
 * </ul>
 *
 * <p>Fully signature-driven: T is bound from source via {@link #unify},
 * output type resolved via {@link #resolveOutput}. Mapping is propagated
 * from the source TypeInfo.
 */
public class FromChecker extends AbstractChecker {

    public FromChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // 1. Resolve overload based on arity + source type
        NativeFunctionDef def = resolveOverload("from", params, source);

        // 2. Compile all params and collect ExpressionTypes for unification
        List<ExpressionType> actuals = new ArrayList<>();
        actuals.add(source.expressionType());  // param[0] already compiled by caller
        for (int i = 1; i < params.size(); i++) {
            TypeInfo argInfo = env.compileExpr(params.get(i), ctx);
            actuals.add(argInfo.expressionType());
        }

        // 3. Unify type variables — bind T from source + validate param types
        Map<String, GenericType> bindings = unify(def, actuals);

        // 4. Output type from signature return type + bindings
        ExpressionType outputType = resolveOutput(def, bindings, "from()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .mapping(source.mapping())
                .build();
    }
}
