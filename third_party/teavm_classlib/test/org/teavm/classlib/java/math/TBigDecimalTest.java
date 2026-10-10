// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.teavm.classlib.java.math;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The corrected TBigDecimal, run on the JVM (it is plain Java), held to java.math.BigDecimal where it was changed:
 * precision() and rounding to a precision, the scaled and the MathContext divisions, equals, doubleValue and floatValue.
 * The cases: the values at each digit count's edge, every long extreme (Long.MIN_VALUE, whose Math.abs stays negative,
 * among them), and generated ones.
 */
class TBigDecimalTest {

    private long state = 7;

    private long next() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private final List<String> differences = new ArrayList<>();

    private void same(String what, Object mine, Object jdk) {
        if (!String.valueOf(mine).equals(String.valueOf(jdk)) && differences.size() < 20) {
            differences.add(what + ": " + mine + " where the JDK has " + jdk);
        }
    }

    private List<String> values() {
        List<String> out = new ArrayList<>(List.of("0", "1", "-1", "9", "10", "99", "100",
                String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MIN_VALUE), String.valueOf(Long.MIN_VALUE + 1),
                "9223372036854775808", "-9223372036854775809", "99999999999999999", "999999999999999999",
                "1000000000000000000", "9999999999999999999", "-648529780849123.5", "-.6", "7711387720.144720958",
                "108088296808187250", "10808829680818725", "0.006", "123.456", "-0.000123"));
        for (int digits = 1; digits <= 40; digits++) {
            out.add("1" + "0".repeat(digits - 1));
            out.add("9".repeat(digits));
            out.add("-" + "9".repeat(digits) + "." + "5");
        }
        for (int i = 0; i < 2_000; i++) {
            long l = next() >> (int) ((next() >>> 1) % 64);
            out.add(l + (i % 3 == 0 ? "" : "E" + ((next() >>> 1) % 40 - 20)));
        }
        return out;
    }

    @Test
    void precisionAndRoundingAreTheJdks() {
        for (String s : values()) {
            TBigDecimal t = new TBigDecimal(s);
            BigDecimal j = new BigDecimal(s);
            same(s + " precision", t.precision(), j.precision());
            for (int p : new int[] {1, 3, 16, 18}) {
                same(s + " round " + p, new TBigDecimal(s).round(new TMathContext(p)),
                        j.round(new MathContext(p)));
            }
        }
        assertEquals(List.of(), differences);
    }

    @Test
    void divisionsAreTheJdks() {
        List<String> values = values();
        for (int i = 0; i + 1 < values.size(); i += 2) {
            String a = values.get(i);
            String b = values.get(i + 1);
            if (new BigDecimal(b).signum() == 0) {
                continue;
            }
            same(a + " / " + b + " scale 10", new TBigDecimal(a).divide(new TBigDecimal(b), 10, TRoundingMode.HALF_EVEN),
                    new BigDecimal(a).divide(new BigDecimal(b), 10, RoundingMode.HALF_EVEN));
            same(a + " / " + b + " DECIMAL64", new TBigDecimal(a).divide(new TBigDecimal(b), TMathContext.DECIMAL64),
                    new BigDecimal(a).divide(new BigDecimal(b), MathContext.DECIMAL64));
        }
        // the scaled division's half-way test past 2^62 (it overflowed): -0.6 / 7711387720.144720958 at scale 10
        same("overflowing half-way", new TBigDecimal("-.6").divide(new TBigDecimal("7711387720.144720958"), 10,
                TRoundingMode.HALF_EVEN), new BigDecimal("-.6").divide(new BigDecimal("7711387720.144720958"), 10,
                RoundingMode.HALF_EVEN));
        assertEquals(List.of(), differences);
    }

    @Test
    void equalsAndConversionsAreTheJdks() {
        List<String> values = values();
        for (int i = 0; i < values.size(); i++) {
            String a = values.get(i);
            String b = values.get((i * 7 + 3) % values.size());
            same(a + " equals " + b, new TBigDecimal(a).equals(new TBigDecimal(b)),
                    new BigDecimal(a).equals(new BigDecimal(b)));
            same(a + " doubleValue", new TBigDecimal(a).doubleValue(), new BigDecimal(a).doubleValue());
            same(a + " floatValue", new TBigDecimal(a).floatValue(), new BigDecimal(a).floatValue());
        }
        same("0 equals a 30-digit value", new TBigDecimal("0").equals(new TBigDecimal("183702659299300726980244159279")),
                false);
        assertEquals(List.of(), differences);
    }
}
