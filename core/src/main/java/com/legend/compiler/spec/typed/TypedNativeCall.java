package com.legend.compiler.spec.typed;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked call to a native (built-in) function &mdash; the generic node
 * for library natives on the signature-driven path (G-&eta;: core structural
 * constructs get their own distinct nodes instead). The resolved {@code callee}
 * (the chosen overload) rides <strong>on the node</strong>, never a name string
 * (§5/§6) &mdash; lowering dispatches on the callee's identity, symmetric with
 * {@link TypedUserCall}.
 *
 * @param callee the resolved native overload this call dispatches to
 * @param args   the type-checked argument expressions, in source order
 * @param info   the resolved result type
 * @param pos    the call-name token's source span, when this node came from a
 *               parsed {@code AppliedFunction} (null on synthesized calls) —
 *               the raise-emission provenance channel (leg 2: interpreted
 *               {@code AssertError} hands the matcher the RAISING expression's
 *               source info, and our raising expressions are native calls).
 *               Excluded from equality, exactly the {@code AppliedFunction}
 *               idiom — a position is provenance, never call identity.
 */
public record TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements TypedSpec {
    public TypedNativeCall {
        args = List.copyOf(args);
    }

    /** Position-free form — synthesis, rewrites, tests. */
    public TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info) {
        this(callee, args, info, null);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TypedNativeCall other
                && callee.equals(other.callee())
                && args.equals(other.args())
                && info.equals(other.info());
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(callee, args, info);
    }

    /** The native's simple name (e.g. {@code length}) &mdash; display convenience, not a dispatch key. */
    public String function() {
        return callee.qualifiedName();
    }

    @Override
    public List<TypedSpec> children() {
        return args;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        return new TypedNativeCall(callee, kids, info, pos);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedNativeCall(callee, args, info, pos);
    }
}
