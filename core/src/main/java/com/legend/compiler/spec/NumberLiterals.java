// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * THE LITERAL RULE (the user's ruling of 2026-11-26, "okay for float/decimal literal compare", option (b) of the
 * decision put to them; docs/records/edition-plan-to-finish-stage-b-2026-11-25.md section 7a, ruling 4): a number
 * literal written against a value of another number type reads as that type when it is exact there. {@code $x.qty > 0}
 * over a Float is {@code $x.qty > 0.0}; {@code $x.salary < 50000} over a Decimal is {@code < 50000D};
 * {@code $x.rate >= 2.375} over a Decimal is {@code >= 2.375D}. It is the equality rule's own reading of a literal: a
 * value is what it denotes, not how it was written, and a literal has no type of its own until it meets the value it
 * is compared with (as most typed languages read one).
 *
 * <p>What it does not reach, each still a comparison across types: two values neither of which is a literal (two
 * columns, a column and a computed value, a variable); two literals ({@code 1 == 1.0}: nothing gives one the other's
 * type); a literal that is not exact in the other type ({@code 1.5} against an Integer, an Integer beyond a double's
 * 53 bits against a Float); a Decimal literal ({@code 1.5D}: its suffix states its type). A collection of number
 * literals reads the same way against a value ({@code $x.qty->in([0, 1])}), every element or none.
 *
 * <p>The literal is read by its WRITTEN digits: {@code 0.1} against a Decimal is the Decimal {@code 0.1}, never the
 * double nearest to it. Nothing is computed from a row's value; the compiler rewrites a constant token (the folds'
 * rule: constants and facts fixed by the types).
 */
final class NumberLiterals {

    private NumberLiterals() {
    }

    /** {@code crossing}'s comparison with its literal side read in the other side's type, or null where the rule does
     *  not reach it. */
    static @Nullable TypedSpec read(TypedCrossTypeComparison crossing) {
        if (!(crossing.comparison() instanceof TypedNativeCall call) || call.args().size() != 2
                || !number(crossing.left()) || !number(crossing.right())) {
            return null;
        }
        TypedSpec l = call.args().get(0);
        TypedSpec r = call.args().get(1);
        boolean leftWritten = written(l);
        if (leftWritten == written(r)) {
            // two literals (a Decimal literal is one: 1D == 1), or none: nothing gives one side the other's type
            return null;
        }
        // read where every literal of the side is an Integer or a Float literal, exact in the other type; a Decimal
        // literal is never reread (as() answers null for it)
        TypedSpec read = leftWritten ? as(l, (Type.Primitive) crossing.right())
                : as(r, (Type.Primitive) crossing.left());
        if (read == null) {
            return null;
        }
        return call.withChildren(leftWritten ? List.of(read, r) : List.of(l, read));
    }

    private static boolean number(Type t) {
        return t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT || t == Type.Primitive.DECIMAL;
    }

    /** Whether {@code side} is written as number literals alone, of any number type: one, signed or not, or a
     *  collection of them. */
    private static boolean written(TypedSpec side) {
        if (side instanceof TypedCollection c) {
            return !c.elements().isEmpty() && c.elements().stream().allMatch(NumberLiterals::literal);
        }
        return literal(side);
    }

    private static boolean literal(TypedSpec side) {
        TypedSpec unsigned = side instanceof TypedNativeCall minus && minus.args().size() == 1
                && Pure.AT_MATH_MINUS.contains(minus.callee().id()) ? minus.args().get(0) : side;
        return unsigned instanceof TypedCInteger || unsigned instanceof TypedCFloat || unsigned instanceof TypedCDecimal;
    }

    /** {@code side} read as {@code target}, or null where one of its literals is not exact there or is a Decimal
     *  literal. */
    private static @Nullable TypedSpec as(TypedSpec side, Type.Primitive target) {
        if (!(side instanceof TypedCollection c)) {
            return literal(side, target);
        }
        List<TypedSpec> elements = new ArrayList<>(c.elements().size());
        for (TypedSpec e : c.elements()) {
            TypedSpec read = literal(e, target);
            if (read == null) {
                return null;
            }
            elements.add(read);
        }
        if (elements.isEmpty()) {
            return null;
        }
        // Decimals of different scales are one type to the rule; the collection's is the widest scale's
        Type element = elements.get(0).info().type();
        for (TypedSpec e : elements) {
            element = wider(element, e.info().type());
        }
        return c.withChildren(elements).withInfo(new ExprType(element, c.info().multiplicity()));
    }

    private static Type wider(Type a, Type b) {
        return a instanceof Type.PrecisionDecimal x && b instanceof Type.PrecisionDecimal y && y.scale() > x.scale()
                ? b : a;
    }

    /** One literal read as {@code target}: a new literal of that type holding the same number, or null where the
     *  number is not exact there. */
    private static @Nullable TypedSpec literal(TypedSpec literal, Type.Primitive target) {
        BigDecimal digits = digits(literal);
        if (digits == null) {
            return null;
        }
        return switch (target) {
            case FLOAT -> {
                double d = digits.doubleValue();
                // exact: the double holds the written number (a whole number within 53 bits, a fraction of twos)
                yield Double.isFinite(d) && new BigDecimal(d).compareTo(digits) == 0
                        ? new TypedCFloat(d, null, ExprType.one(Type.Primitive.FLOAT)) : null;
            }
            case DECIMAL -> {
                int scale = Math.max(0, digits.scale());
                yield scale > Type.PrecisionDecimal.MAX_PRECISION ? null
                        : new TypedCDecimal(digits.scale() < 0 ? digits.setScale(0) : digits, ExprType.one(
                                new Type.PrecisionDecimal(Type.PrecisionDecimal.MAX_PRECISION, scale)));
            }
            case INTEGER -> {
                BigDecimal whole = digits.stripTrailingZeros();
                if (whole.scale() > 0) {
                    yield null;
                }
                BigInteger big = whole.toBigIntegerExact();
                yield new TypedCInteger(big.bitLength() < 64 ? (Number) big.longValue() : big,
                        ExprType.one(Type.Primitive.INTEGER));
            }
            case STRING, BOOLEAN, BYTE, STRICT_DATE, DATE_TIME, STRICT_TIME, NUMBER, DATE, LATEST_DATE ->
                    throw new IllegalStateException("a number literal is read as a number type, not " + target);
        };
    }

    /** The number {@code side} writes, when it is an Integer or a Float literal, with its sign: the written digits
     *  (a Float's exact digits where the parser kept them, else the double's shortest text, which is what was
     *  written). Null for anything else, a Decimal literal included: its suffix states its type. */
    private static @Nullable BigDecimal digits(TypedSpec side) {
        if (side instanceof TypedNativeCall minus && minus.args().size() == 1
                && Pure.AT_MATH_MINUS.contains(minus.callee().id())) {
            BigDecimal inner = unsigned(minus.args().get(0));
            return inner == null ? null : inner.negate();
        }
        return unsigned(side);
    }

    private static @Nullable BigDecimal unsigned(TypedSpec side) {
        return switch (side) {
            case TypedCInteger i -> new BigDecimal(i.big());
            case TypedCFloat f -> f.exact() != null ? f.exact()
                    : Double.isFinite(f.value()) ? new BigDecimal(Double.toString(f.value())) : null;
            default -> null;
        };
    }
}
