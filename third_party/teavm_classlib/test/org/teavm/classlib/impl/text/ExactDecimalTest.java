// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.teavm.classlib.impl.text;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * ExactDecimal held to the JDK, the specification's reference behaviour, on the JVM: the decimal each double and float
 * is written as, the value each text is read as, and BigDecimal's doubleValue and floatValue. The inputs are generated
 * (splitmix64, fixed seeds): random bits, every power of two with its neighbours, decimals as people write them, random
 * decimals of up to 25 digits at any exponent, and the decimals exactly halfway between two neighbouring values and a
 * hair either side of them -- the hardest to read. Counts (2026-10-10): a few seconds; the same code ran nine million
 * values per family without a difference before it landed.
 */
class ExactDecimalTest {

    private long state;

    private long next() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private int below(int bound) {
        return (int) ((next() >>> 1) % bound);
    }

    private final List<String> differences = new ArrayList<>();

    private void same(String what, String mine, String jdk) {
        if (!mine.equals(jdk) && differences.size() < 20) {
            differences.add(what + ": " + mine + " where the JDK has " + jdk);
        }
    }

    private static String written(double v) {
        ExactDecimal.Decimal d = new ExactDecimal.Decimal();
        ExactDecimal.shortest(Math.abs(v), d);
        return (v < 0 ? "-" : "") + d.digits + "E" + d.exponent;
    }

    private static String written(float v) {
        ExactDecimal.Decimal d = new ExactDecimal.Decimal();
        ExactDecimal.shortest(Math.abs(v), d);
        return (v < 0 ? "-" : "") + d.digits + "E" + d.exponent;
    }

    /** The JDK's text as digits and exponent, the form {@link #written} gives. */
    private static String jdk(String text) {
        BigDecimal b = new BigDecimal(text).stripTrailingZeros();
        return (b.signum() < 0 ? "-" : "") + b.unscaledValue().abs() + "E" + -b.scale();
    }

    private static boolean finite(double v) {
        return v != 0 && !Double.isNaN(v) && !Double.isInfinite(v);
    }

    @Test
    void writesEachDoubleAsTheJdkDoes() {
        state = 1;
        for (int i = 0; i < 200_000; i++) {
            double v = Double.longBitsToDouble(next());
            if (finite(v)) {
                same(Long.toHexString(Double.doubleToRawLongBits(v)), written(v), jdk(Double.toString(v)));
            }
        }
        for (long e = 0; e < 2047; e++) {
            for (long b : new long[] {(e << 52) - 1, e << 52, (e << 52) + 1}) {
                double v = Double.longBitsToDouble(b);
                if (b > 0 && finite(v)) {
                    same("power of two " + Long.toHexString(b), written(v), jdk(Double.toString(v)));
                }
            }
        }
        for (int i = 0; i < 100_000; i++) {
            double v = Double.parseDouble(below(100_000) + "." + below(1_000_000));
            if (finite(v)) {
                same("as written " + v, written(v), jdk(Double.toString(v)));
            }
        }
        assertEquals(List.of(), differences);
    }

    @Test
    void writesEachFloatAsTheJdkDoes() {
        state = 2;
        for (int i = 0; i < 200_000; i++) {
            float v = Float.intBitsToFloat((int) next());
            if (v != 0 && !Float.isNaN(v) && !Float.isInfinite(v)) {
                same(Integer.toHexString(Float.floatToRawIntBits(v)), written(v), jdk(Float.toString(v)));
            }
        }
        for (int e = 0; e < 255; e++) {
            for (int b : new int[] {(e << 23) - 1, e << 23, (e << 23) + 1}) {
                float v = Float.intBitsToFloat(b);
                if (b > 0 && v != 0 && !Float.isInfinite(v)) {
                    same("power of two " + Integer.toHexString(b), written(v), jdk(Float.toString(v)));
                }
            }
        }
        assertEquals(List.of(), differences);
    }

    @Test
    void readsEachDecimalAsTheJdkDoes() {
        state = 3;
        for (int i = 0; i < 200_000; i++) {
            StringBuilder digits = new StringBuilder();
            for (int n = 1 + below(25); n > 0; n--) {
                digits.append((char) ('0' + below(10)));
            }
            String s = digits + "e" + (below(700) - 360);
            same(s, bits(ExactDecimal.parseDouble(s)), bits(Double.parseDouble(s)));
            same(s + " (float)", bits(ExactDecimal.parseFloat(s)), bits(Float.parseFloat(s)));
        }
        for (int i = 0; i < 20_000; i++) {
            double v = Double.longBitsToDouble(next() & 0x7FEFFFFFFFFFFFFFL);
            for (String s : halfway(new BigDecimal(v), new BigDecimal(Math.nextUp(v)))) {
                same(s, bits(ExactDecimal.parseDouble(s)), bits(Double.parseDouble(s)));
            }
            float f = Float.intBitsToFloat((int) next() & 0x7F7FFFFF);
            for (String s : halfway(new BigDecimal(f), new BigDecimal(Math.nextUp(f)))) {
                same(s + " (float)", bits(ExactDecimal.parseFloat(s)), bits(Float.parseFloat(s)));
            }
        }
        assertEquals(List.of(), differences);
    }

    /** The decimal halfway between two neighbours, and a unit of its last digit either side. */
    private static List<String> halfway(BigDecimal low, BigDecimal high) {
        BigDecimal mid = low.add(high).divide(BigDecimal.valueOf(2));
        return List.of(mid.toString(), mid.add(mid.ulp()).toString(), mid.subtract(mid.ulp()).toString());
    }

    @Test
    void readsTheSpecificationsGrammarAsTheJdkDoes() {
        String[] forms = {"0", "-0", "+0", "1", "-1", "+1.5", ".5", "5.", "-.5", "1e5", "1E5", "1e+5", "1E-5",
            "1.7976931348623157e308", "1.7976931348623158e308", "1.7976931348623159e308", "1e309", "4.9e-324",
            "2.4703282292062327e-324", "2.4703282292062328e-324", "1e-325", "2.2250738585072011e-308",
            "2.2250738585072012e-308", "9007199254740993", "  2.5", "2.5  ", "\t2.5\n", "2.5d", "2.5f", "2.5D", "2.5F",
            "Infinity", "-Infinity", "+Infinity", "NaN", "-NaN", "infinity", "nan", "NaNd", "Infinityf", "0x1.8p1",
            "0X10P0", "0x.8p-1", "0x1p-1074", "0x1p-1075", "0x1.0000000000001p-1075", "0x1.fffffffffffff8p1023",
            "0x1.fffffffffffff7ffp1023", "0x1p1d", "0x1.8", "0x1.8p", "0xp1", "0x.p1", "-0x", "1_000", "", " ", "1e",
            "e5", ".", "-", "+", "--1", "1.2.3", "1..2", ".5.", "1.2.3f", "1e5.5", "1.2e3.4", "1,5", "1.f", ".f", "f",
            "1d2", "1ef", "١٢٣", "５", "1 ", "1e2147483648", "1e-9999999999999",
            "1.5e-1000000000000", "0.000000000000000000000000000000000000000000000001e40", "1.401298464324817e-45",
            "7.006492321624085e-46", "7.006492321624086e-46", "3.4028235e38", "3.4028236e38", "3.40282357e38",
            "123456789012345678901234567890", "0.30000000000000004"};
        for (String s : forms) {
            same("double [" + s + "]", read(s, false), jdkRead(s, false));
            same("float [" + s + "]", read(s, true), jdkRead(s, true));
        }
        assertEquals(List.of(), differences);
    }

    @Test
    void bigDecimalsDoubleAndFloatValueAreTheJdks() {
        state = 4;
        Random random = new Random(5);
        for (int i = 0; i < 30_000; i++) {
            BigInteger unscaled = new BigInteger(1 + below(120), random);
            int scale = below(800) - 400;
            BigDecimal b = new BigDecimal(unscaled, scale);
            same(b + " doubleValue", bits(ExactDecimal.toDouble(false, unscaled.toString(), -scale)),
                    bits(b.doubleValue()));
            same(b + " floatValue", bits(ExactDecimal.toFloat(false, unscaled.toString(), -scale)),
                    bits(b.floatValue()));
            // as TBigDecimal asks it: negative is signum() < 0, so a zero is never a negative zero
            same("-" + b + " doubleValue", bits(ExactDecimal.toDouble(unscaled.signum() != 0, unscaled.toString(),
                    -scale)), bits(b.negate().doubleValue()));
        }
        assertEquals(List.of(), differences);
    }

    /** PARK-23's anchor (docs/PARKED_WORK_LEDGER.md): a double of 16 or 17 digits is written by the exact route. */
    @Test
    void theFastRouteStopsAtFifteenDigits_park23() {
        assertEquals(15, ExactDecimal.FAST_DIGITS,
                "the fast route now writes more than 15 digits: PARK-23 is closed -- delete its row and this test");
    }

    private static String bits(double v) {
        return Long.toHexString(Double.doubleToRawLongBits(v));
    }

    private static String bits(float v) {
        return Integer.toHexString(Float.floatToRawIntBits(v));
    }

    private static String read(String s, boolean single) {
        try {
            return single ? bits(ExactDecimal.parseFloat(s)) : bits(ExactDecimal.parseDouble(s));
        } catch (NumberFormatException e) {
            return "NumberFormatException: " + e.getMessage();
        }
    }

    private static String jdkRead(String s, boolean single) {
        try {
            return single ? bits(Float.parseFloat(s)) : bits(Double.parseDouble(s));
        } catch (NumberFormatException e) {
            return "NumberFormatException: " + e.getMessage();
        }
    }
}
