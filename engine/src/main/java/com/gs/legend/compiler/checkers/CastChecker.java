package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.GenericTypeInstance;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Signature-driven type checker for {@code cast()}.
 *
 * <p>Signature: {@code cast<T|m>(source:Any[m], type:T[1]):T[m]}
 *
 * <p>Fully signature-driven flow:
 * <ol>
 *   <li>{@code resolveOverload} — validates arity against registered signature</li>
 *   <li>{@code unify} — validates source (Any accepts all types, binds nothing)</li>
 *   <li>T bound from {@code @Type} argument ({@link GenericTypeInstance#resolvedType()})</li>
 *   <li>m bound from source's actual multiplicity (mult-var binding)</li>
 *   <li>{@code resolveOutput} — computes T[m] from bindings</li>
 * </ol>
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

        // 1. Validate arity against signature (exactly 2 params: source + @Type)
        NativeFunctionDef def = resolveOverload("cast", params, source);

        // 2. Validate source — unify binds nothing for Any[m] but validates structure
        unify(def, source.expressionType());

        // 3. Compile the @Type arg (param[1]) — registers it in the side table
        env.compileExpr(params.get(1), ctx);

        // 4. Build unified Bindings: T from @Type, m from source multiplicity
        GenericType targetType = resolveTargetType(params.get(1));
        var bindings = new Bindings();
        bindings.put("T", targetType);
        Multiplicity sourceMult = source.expressionType().multiplicity();
        bindings.mults().put("m", sourceMult);

        // 5. Resolve output T[m] from signature + bindings
        ExpressionType output = resolveOutput(def, bindings, "cast()");

        // 7. Propagate source mapping through the cast
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
