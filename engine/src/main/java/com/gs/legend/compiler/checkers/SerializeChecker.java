package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

/**
 * Checker for {@code serialize(graphFetchSource, #{Tree}#)}.
 *
 * <p>Validates source came from graphFetch (JSON type), compiles serialize
 * spec argument if provided. Return type is JSON.
 */
public class SerializeChecker extends AbstractChecker {

    public SerializeChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        TypeInfo sourceInfo = env.compileExpr(af.parameters().get(0), ctx);

        if (!(sourceInfo.type() instanceof GenericType.Primitive p)
                || p != GenericType.Primitive.JSON) {
            throw new PureCompileException(
                    "serialize() requires a graphFetch source — "
                            + "call ->graphFetch(#{...}#) before ->serialize()");
        }

        // serialize() is a pass-through — graphFetch already compiled everything
        return sourceInfo;
    }
}
