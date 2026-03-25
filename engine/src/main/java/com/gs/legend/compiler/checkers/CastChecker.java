package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.GenericTypeInstance;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Signature-driven type checker for {@code cast()}.
 *
 * <p>Signature: {@code cast<T|m>(source:Any[m], type:T[1]):T[m]}
 *
 * <p>Semantics: changes the type to T (from the @Type arg), preserves the
 * source multiplicity m. That's it — no special cases.
 *
 * <p>T is resolved from the {@code @Type} argument ({@link GenericTypeInstance}).
 * The builder pre-resolves all types at parse time, so {@code resolvedType()}
 * is always available — no string re-parsing needed.
 *
 * <p>Source mapping metadata is propagated through the cast — the type changes
 * but the mapping context (for SQL codegen) survives.
 */
public class CastChecker extends AbstractChecker {

    public CastChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Validate arity against signature (exactly 2 params: source + @Type)
        resolveOverload("cast", params, source);

        // Compile the @Type arg (param[1]) — registers it in the side table
        env.compileExpr(params.get(1), ctx);

        // Resolve T from the @Type argument
        GenericType targetType = resolveTargetType(params.get(1));

        // m = source multiplicity (preserved through cast)
        ExpressionType output = new ExpressionType(targetType, source.expressionType().multiplicity());

        // Propagate source mapping through the cast
        return TypeInfo.from(source)
                .expressionType(output)
                .build();
    }

    /**
     * Extracts the target type T from a {@code @Type} parameter.
     * The param must be a {@link GenericTypeInstance} — cast always has one.
     */
    private GenericType resolveTargetType(ValueSpecification typeParam) {
        if (typeParam instanceof GenericTypeInstance gti) {
            return gti.resolvedType();
        }
        throw new PureCompileException(
                "cast: second argument must be a @Type annotation, got " + typeParam.getClass().getSimpleName());
    }
}
