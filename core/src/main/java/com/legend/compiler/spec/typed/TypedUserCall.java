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
 * @param pos    the call-name token's source span when this node came from a
 *               parsed call (the same channel {@link TypedNativeCall#pos} rides;
 *               the reference differential joins calls by position, execution
 *               plan step 1, 2026-09-26), null for a synthesized call
 */
public record TypedUserCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
                            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) implements TypedSpec {
    public TypedUserCall {
        args = List.copyOf(args);
    }

    /** A synthesized call: no source span. */
    public TypedUserCall(TypedFunction callee, List<TypedSpec> args, ExprType info) {
        this(callee, args, info, null);
    }

    @Override
    public List<TypedSpec> children() {
        return args;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        return new TypedUserCall(callee, kids, info, pos);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedUserCall(callee, args, info, pos);
    }
}
