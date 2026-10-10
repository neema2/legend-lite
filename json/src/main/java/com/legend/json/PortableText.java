// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * A double's text, both ways, as the JDK converts it, written here in plain Java so the JVM and the tab (TeaVM's class
 * library, 0.15: the WebAssembly build) give the same answer. The tab's round trip and the protocol twin test found
 * TeaVM's differ (the protocol program's leg 5, 2026-10-09): its {@code Double.toString} can pick another of two
 * shortest decimals ({@code 602.2708129882813} where the JDK writes {@code 602.2708129882812}), and its
 * {@code Double.parseDouble} (and {@code BigDecimal.doubleValue}) can land one unit in the last place off
 * ({@code 2.323575964036956E17}). Guarded: ArchitectureTest bans the platform's conversions in the parser's and the
 * protocol's.
 */
public final class PortableText {

    private static final BigDecimal TWO = BigDecimal.valueOf(2);
    private static final BigInteger TWO_TO_52 = BigInteger.ONE.shiftLeft(52);

    private PortableText() {
    }

    /**
     * {@code Double.parseDouble(text)} for a decimal ({@link BigDecimal}'s syntax and range: a sign, digits, a point,
     * an exponent) with the float literal's suffix {@code parseDouble} takes ({@code f}, {@code F}, {@code d},
     * {@code D}; the engine's grammar writes {@code f}/{@code F} on a FLOAT): the double nearest its exact value, ties
     * to the even one (IEEE 754's round to nearest, as the JDK rounds). A negative zero, which a {@link BigDecimal}
     * cannot hold, is read from the sign written.
     */
    public static double doubleOf(String text) {
        String decimal = text;
        if (!decimal.isEmpty() && "fFdD".indexOf(decimal.charAt(decimal.length() - 1)) >= 0) {
            decimal = decimal.substring(0, decimal.length() - 1);
        }
        double d = doubleOf(new BigDecimal(decimal));
        return d == 0 && decimal.startsWith("-") ? -0.0 : d;
    }

    /** 10^0 .. 10^22: the powers of ten a double holds exactly. */
    private static final double[] EXACT_POWERS_OF_TEN = {
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
        1e20, 1e21, 1e22,
    };

    /** {@link #doubleOf(String)} for an exact decimal: the double nearest it, ties to the even one. */
    public static double doubleOf(BigDecimal value) {
        int signum = value.signum();
        if (signum == 0) {
            return 0.0;
        }
        BigDecimal abs = value.abs();
        int scale = abs.scale();
        double magnitude;
        if (abs.unscaledValue().bitLength() <= 53 && scale >= -22 && scale <= 22) {
            // the common case, exactly and fast (Clinger's): the digits and the power of ten are each a double exactly,
            // and one IEEE multiplication or division of two exact doubles is the nearest double to the exact result
            double digits = abs.unscaledValue().longValue();
            magnitude = scale >= 0 ? digits / EXACT_POWERS_OF_TEN[scale] : digits * EXACT_POWERS_OF_TEN[-scale];
        } else {
            magnitude = nearestDouble(abs);
        }
        return signum < 0 ? -magnitude : magnitude;
    }

    private static double nearestDouble(BigDecimal value) {
        long decimalExponent = (long) value.precision() - value.scale() - 1;   // value = d.ddd... x 10^decimalExponent
        if (decimalExponent > 309) {
            return Double.POSITIVE_INFINITY;
        }
        if (decimalExponent < -325) {
            return 0.0;   // below half the least double
        }
        int scale = value.scale();
        BigInteger num = scale <= 0 ? value.unscaledValue().multiply(BigInteger.TEN.pow(-scale)) : value.unscaledValue();
        BigInteger den = scale <= 0 ? BigInteger.ONE : BigInteger.TEN.pow(scale);
        // value = q x 2^e + a remainder, q of 53 bits: e from the bit lengths, one step up if the quotient ran over
        int e = num.bitLength() - den.bitLength() - 53;
        BigInteger[] qr = scaledQuotient(num, den, e);
        if (qr[0].bitLength() > 53) {
            e++;
            qr = scaledQuotient(num, den, e);
        }
        if (e < -1074) {
            e = -1074;   // subnormal: the least exponent, fewer bits
            qr = scaledQuotient(num, den, e);
        }
        BigInteger q = qr[0];
        int half = qr[1].shiftLeft(1).compareTo(qr[2]);
        if (half > 0 || half == 0 && q.testBit(0)) {
            q = q.add(BigInteger.ONE);
            if (q.bitLength() > 53) {
                q = q.shiftRight(1);
                e++;
            }
        }
        if (e > 971) {
            return Double.POSITIVE_INFINITY;   // past (2^53 - 1) x 2^971
        }
        long m = q.longValue();
        long bits = q.compareTo(TWO_TO_52) < 0 ? m : ((long) (e + 1075) << 52) | (m - (1L << 52));
        return Double.longBitsToDouble(bits);
    }

    /** {@code num / den} scaled by {@code 2^-e}: the quotient, the remainder and the divisor they are over. */
    private static BigInteger[] scaledQuotient(BigInteger num, BigInteger den, int e) {
        BigInteger n = e <= 0 ? num.shiftLeft(-e) : num;
        BigInteger d = e <= 0 ? den : den.shiftLeft(e);
        BigInteger[] qr = n.divideAndRemainder(d);
        return new BigInteger[] {qr[0], qr[1], d};
    }

    /**
     * {@code Double.toString(v)} as the JDK writes it since 19, by its javadoc's definition: of the decimals that round
     * to {@code v} (round to nearest, ties to even), those of the least length -- of length 1 or 2 when the least is 1
     * -- and of those the one closest to {@code v}, the one with the even significand on a tie; written plain when its
     * exponent is in [-3, 7), else in computerized scientific notation. Computed exactly, with {@link BigDecimal}.
     */
    public static String doubleText(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "Infinity" : "-Infinity";
        }
        long bits = Double.doubleToRawLongBits(v);
        String sign = bits < 0 ? "-" : "";
        long magnitude = bits & Long.MAX_VALUE;
        if (magnitude == 0) {
            return sign + "0.0";
        }
        double a = Double.longBitsToDouble(magnitude);
        BigDecimal exact = new BigDecimal(a);
        // the decimals that round to a: half way to each neighbour, the ends included when a's significand is even
        BigDecimal below = exact.add(new BigDecimal(Double.longBitsToDouble(magnitude - 1))).divide(TWO);
        double next = Double.longBitsToDouble(magnitude + 1);
        BigDecimal above = Double.isInfinite(next) ? exact.add(exact.subtract(below))
                : exact.add(new BigDecimal(next)).divide(TWO);
        boolean ends = (magnitude & 1) == 0;
        int e = exact.precision() - exact.scale() - 1;   // a = d.ddd... × 10^e
        int length = 1;
        while (nearest(exact, e, length, below, above, ends) == null) {
            length++;
        }
        // the grid of a longer length holds every point of a shorter one's, so one rounds to a at this length too
        BigDecimal d = java.util.Objects.requireNonNull(nearest(exact, e, Math.max(length, 2), below, above, ends));
        return sign + format(d.stripTrailingZeros());
    }

    /**
     * Of the two decimals at the grid of {@code length} digits from {@code 10^e} that bracket {@code exact}, the one
     * closer to it that rounds to it (on a tie, the even multiple), or null: none of that length rounds to it. Any
     * decimal of at most that length near {@code exact} sits on that grid, and the interval holds {@code exact}, so a
     * nearer one of the length rounds to it whenever a farther one does.
     */
    private static @com.legend.base.Nullable BigDecimal nearest(BigDecimal exact, int e, int length, BigDecimal below,
            BigDecimal above, boolean ends) {
        int scale = length - 1 - e;
        BigDecimal down = exact.setScale(scale, RoundingMode.FLOOR);
        if (down.compareTo(exact) == 0) {
            return down;
        }
        BigDecimal up = down.add(BigDecimal.ONE.scaleByPowerOfTen(-scale));
        boolean downIn = within(down, below, above, ends);
        boolean upIn = within(up, below, above, ends);
        if (!downIn && !upIn) {
            return null;
        }
        if (downIn != upIn) {
            return downIn ? down : up;
        }
        int closer = exact.subtract(down).compareTo(up.subtract(exact));
        if (closer != 0) {
            return closer < 0 ? down : up;
        }
        return down.unscaledValue().testBit(0) ? up : down;
    }

    private static boolean within(BigDecimal d, BigDecimal below, BigDecimal above, boolean ends) {
        int lo = d.compareTo(below);
        int hi = d.compareTo(above);
        return ends ? lo >= 0 && hi <= 0 : lo > 0 && hi < 0;
    }

    /** A decimal, its trailing zeros stripped, laid out as {@code Double.toString} lays it out. */
    private static String format(BigDecimal d) {
        String digits = d.unscaledValue().toString();
        int n = digits.length();
        int i = -d.scale();                // d = digits × 10^i
        int e = n + i - 1;
        if (e >= -3 && e < 0) {
            return "0." + "0".repeat(-e - 1) + digits;
        }
        if (e >= 0 && e < 7) {
            return i >= 0 ? digits + "0".repeat(i) + ".0" : digits.substring(0, n + i) + "." + digits.substring(n + i);
        }
        return digits.charAt(0) + "." + (n == 1 ? "0" : digits.substring(1)) + "E" + e;
    }
}
