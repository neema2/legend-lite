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
 * @param origin the source node the typer read the call from, compared by
 *               identity: a let in a lambda body is spliced into its uses
 *               before typing, so each use is typed as its own call, every
 *               copy of the one origin -- the copies are one evaluation of the
 *               call (docs/OBJECT_IDENTITY_2026_10_20.md §3.2). Null for a
 *               call the compiler builds
 * @param untracked whether the objects its body builds have no identity the
 *               product tracks (§4: the call is a lambda-body let's value read
 *               inside a nested lambda)
 */
public record TypedUserCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
                            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos,
                            @com.legend.base.Nullable Object origin, boolean untracked) implements TypedSpec {
    public TypedUserCall {
        args = List.copyOf(args);
    }

    /** A synthesized call: no source span. */
    public TypedUserCall(TypedFunction callee, List<TypedSpec> args, ExprType info) {
        this(callee, args, info, null);
    }

    public TypedUserCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) {
        this(callee, args, info, pos, null, false);
    }

    @Override
    public List<TypedSpec> children() {
        return args;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        return new TypedUserCall(callee, kids, info, pos, origin, untracked);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedUserCall(callee, args, info, pos, origin, untracked);
    }

    /** This call, read from {@code source}. */
    public TypedUserCall withOrigin(Object source) {
        return new TypedUserCall(callee, args, info, pos, source, untracked);
    }

    /** This call, the objects its body builds untracked (§4). */
    public TypedUserCall asUntracked() {
        return new TypedUserCall(callee, args, info, pos, origin, true);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TypedUserCall c && callee.equals(c.callee) && args.equals(c.args) && info.equals(c.info)
                && java.util.Objects.equals(pos, c.pos) && origin == c.origin && untracked == c.untracked;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(callee, args, info, pos, System.identityHashCode(origin), untracked);
    }
}
