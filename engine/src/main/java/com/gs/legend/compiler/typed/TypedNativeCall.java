package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/**
 * Catch-all for native scalar functions that don't have a dedicated variant
 * — arithmetic, string ops, date ops, comparisons, {@code if}, {@code cast},
 * etc. The resolved {@link NativeFunctionDef} carries the signature; the
 * type checker picked it at compile time.
 *
 * <p>PlanGenerator dispatches to the dialect's scalar renderer based on
 * {@code func.name()}.
 */
public record TypedNativeCall(
        NativeFunctionDef func,
        List<TypedSpec> args,
        ExpressionType info
) implements TypedNative {
    public TypedNativeCall {
        args = List.copyOf(args);
    }

    /** {@link TypedNative#def()} alias for {@link #func()} (semantic identity). */
    @Override public NativeFunctionDef def() { return func; }
}
