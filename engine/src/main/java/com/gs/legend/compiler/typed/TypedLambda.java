package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * Lambda expression: {@code {x, y | body}}.
 *
 * <p>Body is a list to accommodate statement sequences; most lambdas have
 * a single-element body holding the result expression.
 */
public record TypedLambda(
        List<TypedParam> parameters,
        List<TypedSpec> body,
        ExpressionType info
) implements TypedSpec {
    public TypedLambda {
        parameters = List.copyOf(parameters);
        body = List.copyOf(body);
    }
}
