package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Type checker for {@code get(source, key)} — variant navigation.
 *
 * <p>{@code get()} always returns {@code Variant[0..1]} (JSON).
 * Type conversion is handled separately by {@code to(@Type)} → {@code T[0..1]}
 * and {@code toMany(@Type)} → {@code T[*]}, aligned with legend-engine's
 * variant API.
 *
 * <p>Access pattern resolution (field vs index) is handled by PlanGenerator,
 * which reads the key literal (param[1]) directly from the AST.
 */
public class GetChecker extends AbstractChecker {

    public GetChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Compile key argument (param[1])
        if (params.size() > 1) {
            env.compileExpr(params.get(1), ctx);
        }

        // get() always returns untyped variant — JSON[0..1]
        return TypeInfo.builder()
                .expressionType(ExpressionType.zeroOrOne(GenericType.Primitive.JSON))
                .build();
    }
}
