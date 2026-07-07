package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked lambda argument (engine {@code TypedLambda}). Its parameter
 * types were solved from the surrounding call (the bidirectional lambda step,
 * §3.4), so by the time the {@code body} was checked every parameter was
 * concrete. {@link #info()} carries the lambda's {@code FunctionType}.
 *
 * @param parameters parameter names, in order
 * @param body       the type-checked body statements
 * @param info       the lambda's function type
 */
public record TypedLambda(List<String> parameters, List<TypedSpec> body, ExprType info) implements TypedSpec {
    public TypedLambda {
        parameters = List.copyOf(parameters);
        body = List.copyOf(body);
    }

    @Override
    public List<TypedSpec> children() {
        return body;
    }
}
