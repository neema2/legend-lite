package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

/**
 * Checker for {@code serialize(graphFetchSource, #{Tree}#)}.
 *
 * <p>Type-checks:
 * <ol>
 *   <li>Source must have a graphFetchSpec (must come from graphFetch())</li>
 *   <li>Stamps returnType = JSON (serialized graph output)</li>
 * </ol>
 */
public class SerializeChecker extends AbstractChecker {

    public SerializeChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        // Compile source (must be a graphFetch result)
        TypeInfo sourceInfo = env.compileExpr(af.parameters().get(0), ctx);

        // (1) Source must have a graphFetchSpec
        com.gs.legend.plan.GraphFetchSpec spec = sourceInfo.graphFetchSpec();
        if (spec == null) {
            throw new PureCompileException(
                    "serialize() requires a graphFetch source — "
                            + "call ->graphFetch(#{...}#) before ->serialize()");
        }

        // Override with serialize tree if provided
        if (af.parameters().size() > 1 && af.parameters().get(1) instanceof ClassInstance ci
                && ci.value() instanceof com.gs.legend.ast.GraphFetchTree gft) {
            var targetClass = sourceInfo.mapping().targetClass();
            spec = GraphFetchChecker.toGraphFetchSpec(gft, targetClass);
        }

        // (2) Stamp expressionType = JSON (serialized graph output)
        return TypeInfo.from(sourceInfo)
                .graphFetchSpec(spec)
                .expressionType(ExpressionType.one(GenericType.Primitive.JSON))
                .build();
    }
}
