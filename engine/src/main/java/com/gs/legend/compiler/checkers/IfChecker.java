package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedIf;
import com.gs.legend.compiler.typed.TypedSpec;
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

    public TypedIf check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Compile condition + both branch thunks. Each branch is a zero-arg
        // lambda; compileExpr produces its body's TypedSpec (with the body's
        // return type carried on .info()).
        TypedSpec condition = env.compileExpr(params.get(0), ctx);
        TypedSpec thenBranch = env.compileExpr(params.get(1), ctx);
        TypedSpec elseBranch = params.size() >= 3
                ? env.compileExpr(params.get(2), ctx) : null;

        // Result type = common supertype of branch return types.
        Type resultType = thenBranch.type();
        if (elseBranch != null && !resultType.equals(elseBranch.type())) {
            resultType = Type.commonSupertype(resultType, elseBranch.type());
        }
        return new TypedIf(condition, thenBranch, elseBranch,
                ExpressionType.one(resultType));
    }
}
