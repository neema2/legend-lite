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
 * @param outsideStore THE STAMP (the edition program's step 2, 2026-11-25; the user: "why cant we stamp the substring
 *               if database or not when we know it and then use that at the right time?"): this call is outside any
 *               query sent to a store. A neutral fact about where the program wrote the call, set only where a
 *               compatibility door asks for it, while functions are expanded into the query
 *               ({@code UserCallInliner}: the last moment a {@code let} and an {@code execute()} boundary can still be
 *               told) and read only where the lowering picks a rule ({@code lowering.Scalars.lower}), which hands it
 *               to the door's table. The product never sets it. It is a field and not a wrapper node so that it
 *               rides along when a pass rebuilds the call: {@link #withChildren}, {@link #withInfo} and
 *               {@link #withArgs} keep it, and a pass that rebuilds a call from its callee uses {@link #withArgs}.
 *               Part of equality: two calls that differ in it lower differently.
 */
public record TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos, boolean outsideStore)
        implements TypedSpec {
    public TypedNativeCall {
        args = List.copyOf(args);
    }

    /** A call inside a store query, or one no door asked to stamp: every call the typer and the passes make. */
    public TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(callee, args, info, pos, false);
    }

    /** Position-free form — synthesis, rewrites, tests. */
    public TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info) {
        this(callee, args, info, null, false);
    }

    /** This call with other arguments and type: its callee, its position and its stamp kept. The one way a pass
     *  rebuilds a call it was given. */
    public TypedNativeCall withArgs(List<TypedSpec> newArgs, ExprType newInfo) {
        return new TypedNativeCall(callee, newArgs, newInfo, pos, outsideStore);
    }

    /** This call stamped as outside any store query, or not. */
    public TypedNativeCall stampedOutsideStore(boolean outside) {
        return outside == outsideStore ? this : new TypedNativeCall(callee, args, info, pos, outside);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TypedNativeCall other
                && callee.equals(other.callee())
                && args.equals(other.args())
                && info.equals(other.info())
                && outsideStore == other.outsideStore();
    }

    /** An unstamped call hashes as it did before the stamp existed, so nothing keyed on a call moves for the
     *  product. */
    @Override
    public int hashCode() {
        int h = java.util.Objects.hash(callee, args, info);
        return outsideStore ? 31 * h + 1 : h;
    }

    /** An unstamped call prints as it did before the stamp existed (a record's own form of its first four
     *  components): a plan handle's id is made from a typed tree's text ({@code plan:<hash>:<length>}, pinned in
     *  pure/v1's plan-text golden), and the product never stamps. */
    @Override
    public String toString() {
        return "TypedNativeCall[callee=" + callee + ", args=" + args + ", info=" + info + ", pos=" + pos
                + (outsideStore ? ", outsideStore=true" : "") + "]";
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
        return new TypedNativeCall(callee, kids, info, pos, outsideStore);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedNativeCall(callee, args, info, pos, outsideStore);
    }
}
