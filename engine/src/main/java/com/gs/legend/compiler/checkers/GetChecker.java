package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;

import java.util.List;

/**
 * Type checker for {@code get(source, key)}. Variant navigation on a JSON-backed source:
 * {@code $variant->get('fieldName')} for field access, {@code $variant->get(index)} for
 * array-index access. Return type is {@code Variant[0..1]} per the native signature
 * registered in {@link com.gs.legend.compiler.BuiltinRegistry}; chained gets compose,
 * with final typed extraction via {@code ->to(@Type)}.
 */
public class GetChecker extends AbstractChecker {

    public GetChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // Compile args so they have TypeInfo for downstream PlanGenerator. Second arg's
        // compiled type determines String-vs-Integer key dispatch at SQL rendering time.
        env.compileExpr(params.get(0), ctx);
        if (params.size() > 1) {
            env.compileExpr(params.get(1), ctx);
        }

        // Resolve against the registered signature — validates Variant source, returns
        // Variant[0..1] from the signature rather than hardcoding.
        NativeFunctionDef def = resolveOverload("get", params, source);
        var bindings = unify(def, source.expressionType());
        return TypeInfo.builder()
                .expressionType(resolveOutput(def, bindings, "get()"))
                .build();
    }
}
