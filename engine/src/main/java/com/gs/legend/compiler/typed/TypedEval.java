package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * Lambda invocation on a function-typed variable: {@code $f->eval(args...)}.
 *
 * <p>Produced exclusively for the case where the receiver is a
 * {@link TypedVariable} whose {@code type()} is a {@code FunctionType} —
 * typically a function-typed parameter of an enclosing user function. All
 * other eval forms (colSpec, function reference, literal lambda) are
 * structurally rewritten by {@code EvalChecker} and do not produce a
 * {@code TypedEval}.
 *
 * @param applicable The function-typed variable being evaluated.
 * @param args       Typed actual arguments, in source order.
 * @param info       Type + multiplicity of the call's return value.
 */
public record TypedEval(
        TypedVariable applicable,
        List<TypedSpec> args,
        ExpressionType info
) implements TypedSpec {
    public TypedEval {
        args = List.copyOf(args);
    }
}
