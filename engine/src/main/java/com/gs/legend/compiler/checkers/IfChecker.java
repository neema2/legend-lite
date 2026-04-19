package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Type checker for {@code if(condition, thenLambda, elseLambda)}.
 *
 * <p>{@code if()} branches are zero-arg lambdas (thunks). When compiled,
 * each thunk produces a TypeInfo reflecting the body's return type.
 * The output type is the common type of both branches:
 * <ul>
 *   <li>Same type → propagate directly</li>
 *   <li>Both numeric → widen to {@code Number}</li>
 *   <li>Otherwise → widen to {@code Any}</li>
 * </ul>
 *
 * <p>Note: {@code if()} does not use the standard {@code resolveOverload →
 * unify → resolveOutput} pipeline because the signature's thunk parameters
 * ({@code Function<{->T[*]}>}) don't match the ExpressionTypes produced
 * by compiling lambda bodies. The type resolution is structural: compile
 * both branches, extract their return types, compute the common type.
 */
public class IfChecker extends AbstractChecker {

    public IfChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Compile condition (param[0])
        env.compileExpr(params.get(0), ctx);

        // Compile then-branch (param[1]) — thunk body return type
        TypeInfo thenInfo = env.compileExpr(params.get(1), ctx);
        Type resultType = thenInfo.type();

        // Compile else-branch (param[2]) — thunk body return type
        if (params.size() >= 3) {
            TypeInfo elseInfo = env.compileExpr(params.get(2), ctx);
            Type elseType = elseInfo.type();
            if (!resultType.equals(elseType)) {
                resultType = Type.commonSupertype(resultType, elseType);
            }
        }

        return TypeInfo.builder()
                .expressionType(ExpressionType.one(resultType))
                .build();
    }
}
