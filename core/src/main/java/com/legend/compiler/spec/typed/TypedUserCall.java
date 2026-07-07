package com.legend.compiler.spec.typed;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked call to a user-defined function (engine {@code TypedUserCall}).
 * The resolved {@code callee} (the chosen overload) is carried <strong>on the
 * node</strong>, not in a sidecar (§5), so lowering reads it directly.
 *
 * @param callee the resolved overload this call dispatches to
 * @param args   the type-checked argument expressions, in source order
 * @param info   the call's result type (the callee's declared return, resolved)
 */
public record TypedUserCall(TypedFunction callee, List<TypedSpec> args, ExprType info) implements TypedSpec {
    public TypedUserCall {
        args = List.copyOf(args);
    }

    @Override
    public List<TypedSpec> children() {
        return args;
    }
}
