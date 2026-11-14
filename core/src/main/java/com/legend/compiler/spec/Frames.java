package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.WindowFrame;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;

import java.util.List;

/**
 * Window-frame classification (remediation T3.1): {@code rows(a,b)} /
 * {@code _range(...)} / {@code _rangeInterval(...)} decided ONCE, where the
 * checker sees the literal bounds (a bound that is no literal yet -- a
 * function's parameter -- is kept as its expression for the inliner to
 * substitute into: {@code WindowFrame.Bound.Offset}; an interval's stays
 * literal-only) — including the boundary validation the
 * engine also performs at check time (its {@code ExtendChecker}). The
 * lowerer maps the resulting {@link WindowFrame} to the SQL IR shape 1:1;
 * it no longer recovers PRECEDING/FOLLOWING from the sign of a literal.
 */
final class Frames {

    private Frames() {
    }

    static WindowFrame classify(TypedSpec spec) {
        if (!(spec instanceof TypedNativeCall call)) {
            throw new IllegalStateException("window frame expects rows()/range(), got "
                    + spec.getClass().getSimpleName());
        }
        // INTERVAL ranges (_range(n, DurationUnit, m, DurationUnit) and the
        // unbounded mixes): each bounded side pairs an Integer with its
        // DurationUnit — RANGE BETWEEN INTERVAL n UNIT PRECEDING/FOLLOWING.
        List<TypedSpec> as = call.args();
        // THE FAMILY IS A CLOSED TYPE (NativeFn.Frame, batch 4b): rows / _range
        // are frames, unbounded is a BOUND — an exhaustive switch, no default
        com.legend.builtin.NativeFn.Frame frame = com.legend.builtin.NativeFn.Frame
                .of(call.callee().id()).orElseThrow(() -> new IllegalStateException(
                        "window frame expects rows()/_range(), got " + call.callee().qualifiedName()));
        boolean interval = as.stream().anyMatch(a ->
                a instanceof TypedEnumValue ev
                        && ev.enumFqn().equals("meta::pure::functions::date::DurationUnit"));
        return switch (frame) {
            case UNBOUNDED -> throw new IllegalStateException(
                    "unbounded() is a frame BOUND, not a frame");
            case ROWS -> new WindowFrame(WindowFrame.Kind.ROWS,
                    bound(call.args().get(0), true), bound(call.args().get(1), false));
            case RANGE -> interval ? intervalFrame(as)
                    : new WindowFrame(WindowFrame.Kind.RANGE,
                            bound(call.args().get(0), true), bound(call.args().get(1), false));
        };
    }

    private static WindowFrame intervalFrame(List<TypedSpec> as) {
        {
            WindowFrame.Bound from;
            WindowFrame.Bound to;
            int i = 0;
            if (isUnboundedCall(as.get(i))) {
                from = new WindowFrame.Bound.UnboundedPreceding();
                i += 1;
            } else {
                from = intervalBound(as.get(i), as.get(i + 1), true);
                i += 2;
            }
            if (i < as.size() && isUnboundedCall(as.get(i))) {
                to = new WindowFrame.Bound.UnboundedFollowing();
            } else {
                to = intervalBound(as.get(i), as.get(i + 1), false);
            }
            return new WindowFrame(WindowFrame.Kind.RANGE, from, to);
        }
        // Bound VALIDATION lives in the LOWERING (Lowerer.sqlFrame):
        // interpreted pure and the engine's relational executor both
        // raise the invalid-boundary error LAZILY at eval (spec witness
        // testRows/Range_InvalidWindowFrameBoundary passes through
        // assertError) — lowering-time keeps "never bad SQL" (a bad
        // frame still never renders) while staying observable to the
        // deferred-body catch (the timeBucket precedent).
    }

    private static WindowFrame.Bound bound(TypedSpec arg, boolean fromSide) {
        // a literal by its sign, unbounded() by its side, anything else a bound the database computes (an Offset the
        // inliner substitutes into: docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, C1) -- the one rule
        return WindowFrame.bound(arg, fromSide);
    }

    private static boolean isUnboundedCall(TypedSpec arg) {
        return arg instanceof TypedNativeCall c
                && com.legend.builtin.NativeFn.Frame.of(c.callee().id()).orElse(null)
                        == com.legend.builtin.NativeFn.Frame.UNBOUNDED;
    }

    /** One INTERVAL frame side: signed Integer + DurationUnit literal. */
    private static WindowFrame.Bound intervalBound(TypedSpec amount, TypedSpec unit,
            boolean fromSide) {
        Number n = numericBound(amount);
        if (n == null || !(unit instanceof TypedEnumValue ev)) {
            throw new IllegalStateException("interval frame bound needs a literal"
                    + " Integer and a DurationUnit literal");
        }
        long v = n.longValue();
        if (v < 0) {
            return new WindowFrame.Bound.IntervalPreceding(-v, ev.value());
        }
        if (v > 0) {
            return new WindowFrame.Bound.IntervalFollowing(v, ev.value());
        }
        return new WindowFrame.Bound.CurrentRow();
    }

    /** The numeric value of a literal frame bound, or null (RANGE takes decimals). */
    private static @com.legend.base.Nullable Number numericBound(TypedSpec arg) {
        return WindowFrame.literal(arg);
    }
}
