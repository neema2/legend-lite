// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.teavm.classlib.impl.text;

import java.math.BigInteger;

/**
 * Exact conversions between IEEE 754 binary floating point and decimal text, as the Java SE specification defines
 * them: {@code Double.toString}/{@code Float.toString} (which decimal is written) and {@code Double.valueOf}/
 * {@code Float.valueOf} (the text read, rounded to the nearest value, a tie to the even significand).
 *
 * <p>PROVENANCE (the clean-room rule, docs/WEB_IMAGE_SPIKE_2026_10_10.md): written from the Java SE 25 API
 * specification of those four methods, from IEEE 754's round-to-nearest-even, from Giulietti, "The Schubfach way to
 * render doubles" (2020) for the formulation of the interval that rounds to a value (in units of 2^(q-2), 4c-2 to
 * 4c+2, 4c-1 below a power of two), and from Clinger, "How to read floating point numbers accurately" (PLDI 1990) for
 * the one fast case of reading (an integer and a power of ten, each exactly a double, combined by one correctly rounded
 * operation). No implementation's code was read or used. The error messages are the JDK's as observed from outside (its
 * behaviour, not its source).
 *
 * <p>WRITING. The specification's choice: of the decimals that round to v, those of the least length n (n &gt;= 2), or
 * of length 1 or 2 when the least is 1; of these the one closest to v; on a tie, the one whose least significant
 * digit is even. Two routes: a fast one for a normal double whose shortest decimal has at most 15 digits (one
 * correctly rounded operation decides whether a candidate rounds to v), and an exact one over integers for the rest.
 *
 * <p>READING. The text's digits and exponent, then the nearest value by integer arithmetic (a quotient with three
 * guard bits and a sticky remainder), or Clinger's fast case.
 */
public final class ExactDecimal {

    private ExactDecimal() {
    }

    /** A decimal: {@code digits x 10^exponent}, digits without trailing zeros, {@code length} of them. */
    public static final class Decimal {
        public long digits;
        public int exponent;
        public int length;
    }

    private static final double[] POW10 = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12,
        1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};

    private static final float[] POW10F = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};

    private static final double LOG10_2 = 0.30102999566398120;

    /** The most digits the fast route writes (below 10^15 a candidate is the only one at its scale); longer
     *  decimals take the exact route (PARK-23: a faster route for 16 and 17 digits raises this). */
    static final int FAST_DIGITS = 15;

    private static final double FAST_LIMIT = POW10[FAST_DIGITS];

    // ================================================================ writing

    /** The decimal {@code Double.toString} writes for {@code v}, finite and positive. */
    public static void shortest(double v, Decimal out) {
        long bits = Double.doubleToRawLongBits(v);
        int biased = (int) (bits >>> 52) & 0x7FF;
        long fraction = bits & 0xFFFFFFFFFFFFFL;
        if (biased != 0 && fast(v, out)) {
            return;
        }
        long c = biased == 0 ? fraction : fraction | (1L << 52);
        int q = biased == 0 ? -1074 : biased - 1075;
        exact(c, q, biased > 1 && fraction == 0, out);
    }

    /** The decimal {@code Float.toString} writes for {@code v}, finite and positive. */
    public static void shortest(float v, Decimal out) {
        int bits = Float.floatToRawIntBits(v);
        int biased = (bits >>> 23) & 0xFF;
        int fraction = bits & 0x7FFFFF;
        long c = biased == 0 ? fraction : fraction | (1 << 23);
        int q = biased == 0 ? -149 : biased - 150;
        exact(c, q, biased > 1 && fraction == 0, out);
    }

    /**
     * A normal double whose shortest decimal has at most 15 digits. At each scale 10^-j, coarsest first, the integer
     * nearest v x 10^j is the only candidate there (below 10^15, one step of it is wider than the whole interval that
     * rounds to v), and one correctly rounded division or multiplication (both operands exact: the integer below
     * 2^53, the power of ten at most 10^22) says whether it rounds to v. The first scale with a candidate holds the
     * shortest decimal, and it is the only one of its length; a coarser one shows as trailing zeros, stripped.
     */
    private static boolean fast(double v, Decimal out) {
        int e10 = (int) Math.floor(Math.log10(v));
        for (int j = -e10 - 2; j <= 16 - e10; j++) {
            if (j < -22 || j > 22) {
                continue;
            }
            double d = Math.rint(j >= 0 ? v * POW10[j] : v / POW10[-j]);
            if (d == 0) {
                continue;
            }
            if (d >= FAST_LIMIT) {
                return false;
            }
            if ((j >= 0 ? d / POW10[j] : d * POW10[-j]) == v) {
                set(out, (long) d, -j);
                return true;
            }
        }
        return false;
    }

    /**
     * Every finite positive value {@code c x 2^q}. The decimals that round to it lie between the midpoints to its
     * neighbours: in units of 2^(q-2), from 4c-2 (4c-1 when c is a power of two above the least exponent, the
     * neighbour below being half as far) to 4c+2, the ends included when c is even (a midpoint rounds to the even
     * significand). The least length is found as the greatest E with a multiple of 10^E in that interval (if a
     * multiple of 10^(E+1) lies there, so does one of 10^E: searched by halving between a scale that surely has one
     * and one that surely has none).
     */
    private static void exact(long c, int q, boolean irregular, Decimal out) {
        BigInteger lo = BigInteger.valueOf(irregular ? 4 * c - 1 : 4 * c - 2);
        BigInteger mid = BigInteger.valueOf(4 * c);
        BigInteger hi = BigInteger.valueOf(4 * c + 2);
        boolean ends = (c & 1) == 0;
        int a = q - 2;
        int has = (int) Math.floor(Math.log10(irregular ? 3 : 4) + a * LOG10_2) - 2;
        int hasNot = (int) Math.floor(Math.log10(4.0 * c + 2) + a * LOG10_2) + 2;
        while (hasNot - has > 1) {
            int e = Math.floorDiv(has + hasNot, 2);
            if (candidates(lo, hi, ends, a, e) != null) {
                has = e;
            } else {
                hasNot = e;
            }
        }
        BigInteger[] at = candidates(lo, hi, ends, a, has);
        long d = nearest(at[0], at[1], mid.multiply(at[2]), at[3]);
        if (d >= 10) {
            set(out, d, has);
            return;
        }
        // the least length is 1: the specification takes the decimals of length 1 or 2 -- at two scales finer every
        // one of them is an integer of at most two significant digits (the interval spans less than a factor of 3,
        // so none lies a further decade down)
        BigInteger[] fine = candidates(lo, hi, ends, a, has - 2);
        long best = -1;
        BigInteger bestGap = null;
        BigInteger target = mid.multiply(fine[2]);
        for (long k = fine[0].longValue(); k <= fine[1].longValue(); k++) {
            if (significant(k) > 2) {
                continue;
            }
            BigInteger gap = BigInteger.valueOf(k).multiply(fine[3]).subtract(target).abs();
            int cmp = bestGap == null ? -1 : gap.compareTo(bestGap);
            if (cmp < 0 || cmp == 0 && lastDigit(k) % 2 == 0) {
                best = k;
                bestGap = gap;
            }
        }
        set(out, best, has - 2);
    }

    /**
     * The integers d with d x 10^e in the interval [lo, hi] x 2^a (ends as said), with the factor 2^a / 10^e as
     * num/den: {dlo, dhi, num, den}, or null when there is none.
     */
    private static BigInteger[] candidates(BigInteger lo, BigInteger hi, boolean ends, int a, int e) {
        BigInteger num = BigInteger.ONE;
        BigInteger den = BigInteger.ONE;
        if (a >= 0) {
            num = num.shiftLeft(a);
        } else {
            den = den.shiftLeft(-a);
        }
        if (e >= 0) {
            den = den.multiply(BigInteger.TEN.pow(e));
        } else {
            num = num.multiply(BigInteger.TEN.pow(-e));
        }
        BigInteger[] low = lo.multiply(num).divideAndRemainder(den);
        BigInteger dlo = low[1].signum() == 0 && ends ? low[0] : low[0].add(BigInteger.ONE);
        BigInteger[] high = hi.multiply(num).divideAndRemainder(den);
        BigInteger dhi = high[1].signum() == 0 && !ends ? high[0].subtract(BigInteger.ONE) : high[0];
        return dlo.compareTo(dhi) <= 0 ? new BigInteger[] {dlo, dhi, num, den} : null;
    }

    /** Of the integers dlo..dhi, the one nearest scaled/den (a tie to the even one). */
    private static long nearest(BigInteger dlo, BigInteger dhi, BigInteger scaled, BigInteger den) {
        BigInteger[] fr = scaled.divideAndRemainder(den);
        BigInteger below = fr[0];
        BigInteger above = below.add(BigInteger.ONE);
        boolean belowIn = below.compareTo(dlo) >= 0 && below.compareTo(dhi) <= 0;
        boolean aboveIn = above.compareTo(dlo) >= 0 && above.compareTo(dhi) <= 0;
        if (!belowIn) {
            return above.longValue();
        }
        if (!aboveIn) {
            return below.longValue();
        }
        int cmp = fr[1].shiftLeft(1).compareTo(den);
        if (cmp < 0) {
            return below.longValue();
        }
        if (cmp > 0) {
            return above.longValue();
        }
        return below.testBit(0) ? above.longValue() : below.longValue();
    }

    private static int significant(long k) {
        while (k % 10 == 0) {
            k /= 10;
        }
        return digitsOf(k);
    }

    private static long lastDigit(long k) {
        while (k % 10 == 0) {
            k /= 10;
        }
        return k % 10;
    }

    private static void set(Decimal out, long digits, int exponent) {
        while (digits % 10 == 0) {
            digits /= 10;
            exponent++;
        }
        out.digits = digits;
        out.exponent = exponent;
        out.length = digitsOf(digits);
    }

    /** The decimal digits of {@code x}, not negative -- or Long.MIN_VALUE, the one value Math.abs leaves negative, whose
     *  magnitude (2^63) has 19. */
    public static int digitsOf(long x) {
        if (x == Long.MIN_VALUE) {
            return 19;
        }
        int n = 1;
        while (x >= 10) {
            x /= 10;
            n++;
        }
        return n;
    }

    // ================================================================ reading

    /** {@code Double.parseDouble}: the specification's grammar, the nearest double. */
    public static double parseDouble(String text) {
        return Double.longBitsToDouble(parse(text, false));
    }

    /** {@code Float.parseFloat}: the specification's grammar, the nearest float (rounded once, from the text). */
    public static float parseFloat(String text) {
        return Float.intBitsToFloat((int) parse(text, true));
    }

    /** The nearest double to {@code (negative ? -1 : 1) x digits x 10^e10}: {@code BigDecimal.doubleValue}. */
    public static double toDouble(boolean negative, String digits, long e10) {
        long bits = decimal(digits, e10, false);
        return Double.longBitsToDouble(negative ? bits | Long.MIN_VALUE : bits);
    }

    /** The nearest float to {@code (negative ? -1 : 1) x digits x 10^e10}: {@code BigDecimal.floatValue}. */
    public static float toFloat(boolean negative, String digits, long e10) {
        int bits = (int) decimal(digits, e10, true);
        return Float.intBitsToFloat(negative ? bits | Integer.MIN_VALUE : bits);
    }

    private static NumberFormatException invalid(String text) {
        return new NumberFormatException("For input string: \"" + text + "\"");
    }

    /** IEEE bits of the value {@code text} reads as: the Java SE grammar (FloatValue). */
    private static long parse(String text, boolean single) {
        String s = text.trim();
        if (s.isEmpty()) {
            throw new NumberFormatException("empty String");
        }
        int i = 0;
        int n = s.length();
        boolean negative = false;
        if (s.charAt(0) == '+' || s.charAt(0) == '-') {
            negative = s.charAt(0) == '-';
            i = 1;
        }
        long sign = negative ? (single ? 0x80000000L : Long.MIN_VALUE) : 0;
        if (n - i == 3 && s.startsWith("NaN", i)) {
            return single ? 0x7FC00000L : 0x7FF8000000000000L;
        }
        if (n - i == 8 && s.startsWith("Infinity", i)) {
            return sign | (single ? 0x7F800000L : 0x7FF0000000000000L);
        }
        if (n - i > 1 && s.charAt(i) == '0' && (s.charAt(i + 1) == 'x' || s.charAt(i + 1) == 'X')) {
            return sign | hex(text, s, i + 2, single);
        }
        // Digits [. Digits] or . Digits, then [e [+-] Digits], then [fFdD]
        StringBuilder digits = new StringBuilder();
        long e10 = 0;
        boolean point = false;
        boolean any = false;
        while (i < n) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                any = true;
                if (digits.length() > 0 || c != '0') {
                    digits.append(c);
                }
                if (point) {
                    e10--;
                }
            } else if (c == '.') {
                if (point) {
                    throw new NumberFormatException("multiple points");
                }
                point = true;
            } else {
                break;
            }
            i++;
        }
        if (!any) {
            throw invalid(text);
        }
        if (i < n && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
            i++;
            boolean down = false;
            if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) {
                down = s.charAt(i) == '-';
                i++;
            }
            long exponent = 0;
            boolean expDigits = false;
            while (i < n && s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                if (exponent < 1_000_000_000_000L) {   // beyond any finite or nonzero result: saturate
                    exponent = exponent * 10 + (s.charAt(i) - '0');
                }
                expDigits = true;
                i++;
            }
            if (!expDigits) {
                throw invalid(text);
            }
            e10 += down ? -exponent : exponent;
        }
        if (i < n && "fFdD".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
        if (i != n) {
            throw invalid(text);
        }
        if (digits.length() == 0) {
            return sign;   // a zero, of the sign written
        }
        return sign | decimal(digits.toString(), e10, single);
    }

    /** 0x HexDigits [. HexDigits] p [+-] Digits [fFdD], from just after the "0x". */
    private static long hex(String text, String s, int i, boolean single) {
        int n = s.length();
        StringBuilder digits = new StringBuilder();
        long fractionDigits = 0;
        boolean point = false;
        boolean any = false;
        while (i < n) {
            char c = s.charAt(i);
            if (Character.digit(c, 16) >= 0 && c < 128) {
                any = true;
                digits.append(c);
                if (point) {
                    fractionDigits++;
                }
            } else if (c == '.' && !point) {
                point = true;
            } else {
                break;
            }
            i++;
        }
        if (!any || i >= n || (s.charAt(i) != 'p' && s.charAt(i) != 'P')) {
            throw invalid(text);
        }
        i++;
        boolean down = false;
        if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) {
            down = s.charAt(i) == '-';
            i++;
        }
        long exponent = 0;
        boolean expDigits = false;
        while (i < n && s.charAt(i) >= '0' && s.charAt(i) <= '9') {
            if (exponent < 1_000_000_000_000L) {
                exponent = exponent * 10 + (s.charAt(i) - '0');
            }
            expDigits = true;
            i++;
        }
        if (!expDigits) {
            throw invalid(text);
        }
        if (i < n && "fFdD".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
        if (i != n) {
            throw invalid(text);
        }
        BigInteger m = new BigInteger(digits.toString(), 16);
        if (m.signum() == 0) {
            return 0;
        }
        return round(m, (down ? -exponent : exponent) - 4 * fractionDigits, false, single);
    }

    /** IEEE bits (positive) of the value nearest {@code digits x 10^e10}, digits a positive decimal integer's text. */
    private static long decimal(String digits, long e10, boolean single) {
        int start = 0;
        while (start < digits.length() - 1 && digits.charAt(start) == '0') {
            start++;
        }
        int end = digits.length();
        while (end > start + 1 && digits.charAt(end - 1) == '0') {
            end--;
            e10++;
        }
        String d = digits.substring(start, end);
        if (d.equals("0")) {
            return 0;
        }
        long magnitude = d.length() + e10;   // the value lies in [10^(magnitude-1), 10^magnitude)
        if (magnitude - 1 >= (single ? 39 : 309)) {
            return single ? 0x7F800000L : 0x7FF0000000000000L;   // at least 10^39 / 10^309: past the largest
        }
        if (magnitude <= (single ? -46 : -324)) {
            return 0;   // below 10^-46 / 10^-324: under half the least
        }
        if (!single && d.length() <= 15 && Math.abs(e10) <= 22) {
            double v = Long.parseLong(d);
            return Double.doubleToRawLongBits(e10 >= 0 ? v * POW10[(int) e10] : v / POW10[(int) -e10]);
        }
        if (single && d.length() <= 7 && Math.abs(e10) <= 10) {
            float v = Integer.parseInt(d);
            return Float.floatToRawIntBits(e10 >= 0 ? v * POW10F[(int) e10] : v / POW10F[(int) -e10]) & 0xFFFFFFFFL;
        }
        BigInteger m = new BigInteger(d);
        if (e10 >= 0) {
            return round(m.multiply(BigInteger.TEN.pow((int) e10)), 0, false, single);
        }
        BigInteger den = BigInteger.TEN.pow((int) -e10);
        int precision = single ? 24 : 53;
        // a quotient of at least precision + 3 bits: the rounding reads two guard bits and the sticky remainder
        int shift = Math.max(0, precision + 4 - (m.bitLength() - den.bitLength()));
        BigInteger[] qr = m.shiftLeft(shift).divideAndRemainder(den);
        return round(qr[0], -shift, qr[1].signum() != 0, single);
    }

    /**
     * IEEE bits (positive) of the value nearest {@code (m + f) x 2^e2}, m positive and f, when {@code sticky}, a
     * fraction strictly between 0 and 1 (m then carries at least three bits below the result's last).
     */
    private static long round(BigInteger m, long e2, boolean sticky, boolean single) {
        int precision = single ? 24 : 53;
        int leastBit = single ? -149 : -1074;   // the exponent of the least subnormal's bit
        int bias = single ? 127 : 1023;
        int infinity = single ? 255 : 2047;
        long top = m.bitLength() - 1 + e2;   // the value lies in [2^top, 2^(top+1))
        if (top > bias) {
            return (long) infinity << (single ? 23 : 52);
        }
        if (top < leastBit - 1) {
            return 0;   // under 2^(leastBit-1): below half the least subnormal
        }
        long last = Math.max(top - (precision - 1), leastBit);   // the exponent of the result's last bit
        long drop = last - e2;
        BigInteger kept;
        if (drop <= 0) {
            kept = m.shiftLeft((int) -drop);
        } else {
            kept = m.shiftRight((int) drop);
            boolean half = m.testBit((int) drop - 1);
            boolean rest = sticky || m.getLowestSetBit() < drop - 1;
            if (half && (rest || kept.testBit(0))) {
                kept = kept.add(BigInteger.ONE);
            }
        }
        long significand = kept.longValue();
        if (significand >= 1L << precision) {   // rounding carried into a new bit
            significand >>= 1;
            last++;
        }
        if (significand == 0) {
            return 0;
        }
        long biased;
        long fraction;
        if (significand < 1L << (precision - 1)) {
            biased = 0;   // subnormal: last is the least exponent
            fraction = significand;
        } else {
            biased = last + (precision - 1) + bias;
            fraction = significand - (1L << (precision - 1));
        }
        if (biased >= infinity) {
            return (long) infinity << (single ? 23 : 52);
        }
        return biased << (single ? 23 : 52) | fraction;
    }
}
