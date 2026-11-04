package com.legend.compiler.spec.typed;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;

import java.util.ArrayList;
import java.util.List;

/**
 * A CLASSIFIED window frame (remediation T3.1): {@code rows(a,b)} /
 * {@code _range(...)} decided ONCE, at the checker, where the literal
 * bounds are validated &mdash; not re-derived in the lowerer from the
 * sign of a numeric literal. Pure data; the SQL layer's
 * {@code SqlExpr.WindowCall.Frame} is a 1:1 structural map of this shape
 * (the typed layer must not depend on the SQL IR).
 *
 * <p>A bound that is not a literal when the checker sees it -- a function's parameter, arithmetic over one, a query's
 * parameter -- is an {@link Bound.Offset}: its typed expression, which β-inlining substitutes the call's arguments into
 * ({@link TypedOver#children}) and classifies again ({@link #withOffsets}): an argument that makes it a literal makes it
 * a literal bound, as if written so (upstream evaluates the window during planning and reads the numbers: legend-engine
 * {@code pureToSQLQuery.pure}'s {@code processWindowFrame}). One that stays an expression is the database's to compute
 * (docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, C1; ruling L2, the user, 2026-10-31).
 */
public record WindowFrame(Kind kind, Bound from, Bound to) {

    public enum Kind { ROWS, RANGE }

    public sealed interface Bound {
        record UnboundedPreceding() implements Bound {
        }

        record Preceding(Number n) implements Bound {
        }

        record CurrentRow() implements Bound {
        }

        record Following(Number n) implements Bound {
        }

        record UnboundedFollowing() implements Bound {
        }

        /** {@code INTERVAL n UNIT PRECEDING} — the _RangeInterval frame side. */
        record IntervalPreceding(long n, String unit) implements Bound {
        }

        record IntervalFollowing(long n, String unit) implements Bound {
        }

        /**
         * A bound the database computes: {@code value}, a number that is no literal here. Its direction is read from
         * its shape, since its sign is known only when the database works it out: {@code -(e)} is {@code e} before the
         * current row ({@link #preceding}), anything else {@code value} after it. Upstream reads the direction from the
         * sign of the value it computed; where the two differ the database's check refuses
         * (docs/SEMANTICS_REGISTER.md, S129).
         */
        record Offset(TypedSpec value) implements Bound {

            /** Whether the bound is written {@code -(e)}: {@code e} before the current row. */
            public boolean preceding() {
                return negated(value) != null;
            }

            /** The offset's size as written: {@code e} of {@code -(e)}, else the value itself. */
            public TypedSpec magnitude() {
                TypedSpec inner = negated(value);
                return inner != null ? inner : value;
            }
        }

        /**
         * {@code arg}, one side of {@code rows(a, b)} or of a numeric {@code _range(a, b)}, as a bound: a literal by its
         * sign (a negative literal arrives as a unary minus around the number: {@code -0} is {@code 0 PRECEDING}, as it
         * always read), {@code unbounded()} by its side, anything else an {@link Offset} -- the one rule the checker
         * and the inliner's substitution both classify by.
         */
        static Bound of(TypedSpec arg, boolean fromSide) {
            TypedSpec inner = negated(arg);
            if (inner != null && literal(inner) instanceof Number n) {
                return new Preceding(n);
            }
            if (literal(arg) instanceof Number n) {
                double v = n.doubleValue();
                if (v < 0) {
                    return new Preceding(negate(n));
                }
                if (v > 0) {
                    return new Following(n);
                }
                return new CurrentRow();
            }
            if (arg instanceof TypedNativeCall c
                    && NativeFn.Frame.of(c.callee().id()).orElse(null) == NativeFn.Frame.UNBOUNDED) {
                return fromSide ? new UnboundedPreceding() : new UnboundedFollowing();
            }
            Long folded = spelled(arg);
            if (folded != null && folded != Long.MIN_VALUE) {   // a size of -MIN_VALUE is no long: the database's
                // a bound spelled in integers: the number it spells, read by its sign as a literal is (C6.2b)
                return folded < 0 ? new Preceding(-folded) : folded > 0 ? new Following(folded) : new CurrentRow();
            }
            return new Offset(arg);
        }
    }

    /** {@code arg} as a bound of a side of a frame ({@link Bound#of}). */
    public static Bound bound(TypedSpec arg, boolean fromSide) {
        return Bound.of(arg, fromSide);
    }

    /** The numeric value of a literal frame bound, or null (RANGE takes decimals); a negative literal is a unary minus
     *  around the number. */
    public static @com.legend.base.Nullable Number literal(TypedSpec arg) {
        TypedSpec inner = negated(arg);
        if (inner != null) {
            Number n = literal(inner);
            return n == null ? null : -n.doubleValue();
        }
        return switch (arg) {
            case TypedCInteger c -> c.value().longValue();
            case TypedCFloat c -> c.value();
            case TypedCDecimal c -> c.value();
            default -> null;
        };
    }

    /** The expressions of this frame's {@link Bound.Offset}s, from then to: its typed children. */
    public List<TypedSpec> offsets() {
        List<TypedSpec> out = new ArrayList<>(2);
        if (from instanceof Bound.Offset o) {
            out.add(o.value());
        }
        if (to instanceof Bound.Offset o) {
            out.add(o.value());
        }
        return out;
    }

    /** This frame with its {@link Bound.Offset}s' expressions replaced, in order, by {@code values}, each classified
     *  again ({@link Bound#of}): an argument that made one a literal makes it a literal bound. */
    public WindowFrame withOffsets(List<TypedSpec> values) {
        if (values.size() != offsets().size()) {
            throw new IllegalStateException("a frame of " + offsets().size() + " computed bounds given " + values.size());
        }
        int i = 0;
        Bound f = from;
        if (from instanceof Bound.Offset) {
            f = Bound.of(values.get(i++), true);
        }
        Bound t = to;
        if (to instanceof Bound.Offset) {
            t = Bound.of(values.get(i), false);
        }
        return new WindowFrame(kind, f, t);
    }

    /**
     * A FRAME BOUND IS STRUCTURE, FOLDED WHEN SPELLED (docs/TENET_CHARTER.md C6.2b, the user 2026-11-04): {@code arg},
     * where it is integer arithmetic over spelled integers alone -- plus, minus, times and a unary minus, as a library's
     * window writes its span once its arguments are substituted ({@code -(6 * 31 - 1)}) -- is the number it spells,
     * worked exactly; null where it is anything else (a parameter, a column, a division, a float) or overflows a long.
     * The frame is then the literal one: its direction the number's sign and its order checked when it is lowered, as
     * legend-engine reads a frame from the numbers it computes while it plans (docs/SEMANTICS_REGISTER.md S129). Only a
     * frame's bound is folded here, never a row's value.
     */
    static java.lang.@com.legend.base.Nullable Long spelled(TypedSpec arg) {
        if (arg instanceof TypedCInteger c) {
            Number v = c.value();
            if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                return v.longValue();
            }
            return v instanceof java.math.BigInteger b && b.bitLength() < 64 ? b.longValue() : null;
        }
        if (!(arg instanceof TypedNativeCall nc) || nc.args().size() != 1) {
            return null;
        }
        com.legend.model.FunctionId id = nc.callee().id();
        boolean plus = INTEGER_PLUS.equals(id);
        boolean minus = INTEGER_MINUS.equals(id);
        boolean times = INTEGER_TIMES.equals(id);
        if (!plus && !minus && !times) {
            return null;
        }
        TypedSpec only = nc.args().get(0);
        List<TypedSpec> terms = only instanceof TypedCollection coll ? coll.elements() : List.of(only);
        if (terms.isEmpty()) {
            return null;
        }
        long[] v = new long[terms.size()];
        for (int i = 0; i < v.length; i++) {
            Long t = spelled(terms.get(i));
            if (t == null) {
                return null;
            }
            v[i] = t;
        }
        try {
            if (minus && v.length == 1) {
                return Math.negateExact(v[0]);
            }
            long out = v[0];
            for (int i = 1; i < v.length; i++) {
                out = plus ? Math.addExact(out, v[i]) : minus ? Math.subtractExact(out, v[i]) : Math.multiplyExact(out, v[i]);
            }
            return out;
        } catch (ArithmeticException overflow) {
            return null;
        }
    }

    private static final com.legend.model.FunctionId INTEGER_PLUS = com.legend.model.FunctionId.of(Pure.PLUS__INTEGER_MANY);
    private static final com.legend.model.FunctionId INTEGER_MINUS = com.legend.model.FunctionId.of(Pure.MINUS__INTEGER_MANY);
    private static final com.legend.model.FunctionId INTEGER_TIMES = com.legend.model.FunctionId.of(Pure.TIMES__INTEGER_MANY);

    /** {@code e} of {@code -(e)} (a unary minus), else null. Pure's {@code minus} is variadic: {@code a - b} is
     *  {@code minus([a, b])}, one argument too, so the unary form is the one whose argument is one value (as the
     *  lowering tells them, {@code Scalars}' minus rule). */
    private static @com.legend.base.Nullable TypedSpec negated(TypedSpec arg) {
        return arg instanceof TypedNativeCall neg && Pure.AT_MATH_MINUS.contains(neg.callee().id())
                && neg.args().size() == 1
                && neg.args().get(0).info().multiplicity()
                        instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && Integer.valueOf(1).equals(b.upper()) ? neg.args().get(0) : null;
    }

    private static Number negate(Number n) {
        return switch (n) {
            case Long l -> -l;
            case Double d -> -d;
            case java.math.BigDecimal b -> b.negate();
            default -> -n.doubleValue();
        };
    }
}
