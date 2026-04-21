package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSerialize;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Checker for {@code serialize(graphFetchSource, #{Tree}#)}.
 *
 * <p>Validates source is ClassType (from graphFetch). Return type is
 * Variant[1] (JSON) per the registered signature — serialize is a format
 * conversion from class instances to JSON, matching legend-engine's
 * {@code serialize_T_MANY__RootGraphFetchTree_1__String_1_}.
 */
public class SerializeChecker extends AbstractChecker {

    public SerializeChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        TypedSpec sourceTyped = env.compileExpr(af.parameters().get(0), ctx);

        if (!(sourceTyped.type() instanceof Type.ClassType)) {
            throw new PureCompileException(
                    "serialize() requires a class-based source (from graphFetch) — "
                            + "got " + sourceTyped.type());
        }

        // Resolve overload from registry —
        // serialize<T>(T[*], RootGraphFetchTree<T>[1]):Variant[1].
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("serialize", params, source);

        // Bind T from source ClassType.
        List<ExpressionType> actuals = new ArrayList<>(1);
        actuals.add(sourceTyped.expressionType());
        var bindings = unify(def, actuals);

        // Output is Variant[1] (JSON). The {@code format} field tags the
        // serialization dialect — only JSON is currently supported.
        ExpressionType outputType = resolveOutput(def, bindings, "serialize()");
        return new TypedSerialize(sourceTyped, "json", outputType);
    }
}
