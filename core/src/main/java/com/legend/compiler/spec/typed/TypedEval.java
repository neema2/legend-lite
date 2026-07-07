package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A checked {@code eval} application (engine {@code EvalChecker}): a literal
 * lambda applied to caller-scope arguments (typed as the &beta;-reduced body), or
 * a function-typed variable applied to arguments (typed from its declared
 * function type; the concrete lambda arrives at each call site). The colspec and
 * function-reference forms of {@code eval} desugar away before checking and never
 * reach this node.
 *
 * @param fn   the function value: a {@link TypedLambda} (body checked with the
 *             arguments bound) or a {@link TypedVariable} of function type
 * @param args the caller-scope arguments, in order
 * @param info the result &mdash; the lambda body's type, or the variable's declared return
 */
public record TypedEval(TypedSpec fn, List<TypedSpec> args, ExprType info) implements TypedSpec {
    public TypedEval {
        args = List.copyOf(args);
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>(args.size() + 1);
        out.add(fn);
        out.addAll(args);
        return out;
    }
}
