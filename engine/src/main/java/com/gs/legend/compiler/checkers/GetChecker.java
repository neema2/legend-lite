package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Type checker for {@code get(source, key)}.
 * Variant navigation: {@code get(variant, stringKey)} → JSON[0..1].
 */
public class GetChecker extends AbstractChecker {

    public GetChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        env.compileExpr(params.get(0), ctx);

        if (params.size() > 1) {
            env.compileExpr(params.get(1), ctx);
        }
        return TypeInfo.builder()
                .expressionType(ExpressionType.zeroOrOne(Type.Primitive.JSON))
                .build();
    }
}
