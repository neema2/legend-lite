package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.LambdaFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;

import java.util.List;

/**
 * Signature-driven type checker for standalone {@code traverse(source, target, cond)}.
 *
 * <p>Models an association relationship: "source and target are related by this condition."
 * Used by MappingNormalizer to wrap join conditions as compilable Relation API expressions.
 * GetAllChecker compiles these so TypeInfo is stamped on all inner nodes before MappingResolver
 * reads them.
 *
 * <p>Signature:
 * <pre>
 * traverse&lt;T,V&gt;(source:Relation&lt;T&gt;[1], target:Relation&lt;V&gt;[1],
 *               cond:Function&lt;{T[1],V[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;V&gt;[1]
 * </pre>
 *
 * <p>Fully signature-driven: T and V bound via multi-param {@link #unify},
 * lambda compiled via {@link #compileLambdaArg}, output via {@link #resolveOutput}.
 */
public class TraverseChecker extends AbstractChecker {

    public TraverseChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // 1. Compile target (params[1]) FIRST — needed for overload resolution
        //    (structural matching needs compiled type for non-source Relation params)
        TypeInfo target = env.compileExpr(params.get(1), ctx);

        // 2. Resolve overload with compiled types for both Relation params
        var compiledTypes = java.util.Map.of(
                0, source.expressionType(),
                1, target.expressionType());
        NativeFunctionDef def = resolveOverload("traverse", params, source, compiledTypes);

        // 3. Multi-param unify: bind T from source, V from target (null = skip lambda)
        var bindings = unify(def, java.util.Arrays.asList(source.expressionType(), target.expressionType(), null));

        // 4. Compile condition lambda — fully signature-driven
        if (params.get(2) instanceof LambdaFunction lambda) {
            compileLambdaArg(lambda, def.params().get(2), bindings, null, ctx, "traverse");
        }

        // 5. Resolve output from signature: Relation<V>[1]
        ExpressionType output = resolveOutput(def, bindings, "traverse");
        return TypeInfo.builder()
                .expressionType(output)
                .build();
    }
}
