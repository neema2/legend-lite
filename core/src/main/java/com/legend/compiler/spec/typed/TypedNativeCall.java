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
 */
public record TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info) implements TypedSpec {
    public TypedNativeCall {
        args = List.copyOf(args);
    }

    /** The native's simple name (e.g. {@code length}) &mdash; display convenience, not a dispatch key. */
    public String function() {
        return callee.qualifiedName();
    }

    @Override
    public List<TypedSpec> children() {
        return args;
    }
}
