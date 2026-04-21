package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedSpec;

import java.util.ArrayList;
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

    public TypedNativeCall check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Compile every argument so downstream consumers have typed nodes. The key
        // arg's compiled type (String vs Integer) picks field-vs-index semantics.
        List<TypedSpec> args = new ArrayList<>(params.size());
        for (ValueSpecification v : params) {
            args.add(env.compileExpr(v, ctx));
        }

        // Resolve against the registered signature — validates the Variant source and
        // returns {@code Variant[0..1]} from the signature rather than hard-coding.
        NativeFunctionDef def = resolveOverload("get", params, source);
        var bindings = unify(def, source.expressionType());
        return new TypedNativeCall(def, args,
                resolveOutput(def, bindings, "get()"));
    }
}
