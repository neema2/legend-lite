// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.json;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link PortableText} answers as the JDK does: held to the JDK's own methods here, on the JVM; the tab's build is
 *  held to the JVM's answers by //wasm:round_trip_test. */
class PortableTextTest {

    @Test
    void doubleTextIsTheJdks_atTheEdges() {
        // the definition is the JDK's since 19 (shortest decimal, then closest); an older JDK writes otherwise
        assertTrue(Runtime.version().feature() >= 19, "Double.toString's definition is JDK 19's");
        List<Double> edges = new ArrayList<>(List.of(0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_NORMAL,
                Math.nextDown(Double.MIN_NORMAL), Math.nextUp(Double.MIN_NORMAL), Math.nextDown(Double.MAX_VALUE),
                0.1, 0.2, 0.1 + 0.2, 1.0 / 3, 2.0 / 3, 1e23, 8.41e21, 2e-3, 1e-3, 9.999999999999999e-4, 1e7,
                9999999.999999998, 602.2708129882812, 237.00830078125, 1340.9989051643056, 972.9580688476562,
                5e-324, 4.9e-324, 1.0E-5, 123456789.0, 2.5, 3.5, 1.5, 100.0, 1e16, 1e17, 123.0e-300));
        for (int p = -330; p <= 310; p++) {
            double ten = Double.parseDouble("1e" + p);
            edges.add(ten);
            edges.add(Math.nextUp(ten));
            edges.add(Math.nextDown(ten));
        }
        for (int p = -1074; p <= 1023; p++) {
            double two = Math.scalb(1.0, p);
            edges.add(two);
            edges.add(Math.nextUp(two));
            edges.add(Math.nextDown(two));
        }
        for (double v : edges) {
            assertEquals(Double.toString(v), PortableText.doubleText(v), "bits " + Long.toHexString(Double.doubleToRawLongBits(v)));
            assertEquals(Double.toString(-v), PortableText.doubleText(-v));
        }
    }

    @Test
    void doubleOfIsTheJdks_atTheEdges() {
        List<String> texts = new ArrayList<>(List.of("0", "-0", "0.0", "-0.0", "1", "-1", "0.1", "1e23", "8.41e21",
                "2.323575964036956e17", "3.280473756284846e-133", "1.9043925686624381e-264", "5.5512325242606664e+137",
                "4.9e-324", "2.4703282292062327e-324", "2.4703282292062328e-324", "1e-400", "-1e-400",
                "1.7976931348623157e308", "1.7976931348623158e308", "1.7976931348623159e308", "1e309", "-1e309",
                "2.2250738585072011e-308", "2.2250738585072012e-308", "9007199254740993", "123456789012345678901234567890",
                ".5", "5.", "+2.5", "602.2708129882812", "602.2708129882813",
                // the float literal's suffix parseDouble takes (the engine's grammar writes f/F on a FLOAT)
                "1.5f", "-1.5F", "0.9d", "2D", "1e10f", "-0.0f",
                // the common case's own edges: 53-bit digits, 10^22 either way
                "9007199254740991", "9007199254740992", "9007199254740993e-22", "9007199254740991e22", "1e22", "1e-22",
                "123.45", "0.000123", "4503599627370497.5"));
        // every power of two and of ten, its neighbours, and the midpoints between them, as text both ways
        List<Double> powers = new ArrayList<>();
        for (int p = -1074; p <= 1023; p++) {
            powers.add(Math.scalb(1.0, p));
        }
        for (int p = -323; p <= 308; p++) {
            powers.add(Double.parseDouble("1e" + p));
        }
        for (double v : powers) {
            for (double w : new double[] {Math.nextDown(v), v, Math.nextUp(v)}) {
                if (w > 0 && Double.isFinite(w)) {
                    texts.add(Double.toString(w));
                    texts.add(new java.math.BigDecimal(w).toString());
                    if (Double.isFinite(Math.nextUp(w))) {
                        texts.add(new java.math.BigDecimal(w).add(new java.math.BigDecimal(Math.nextUp(w)))
                                .divide(java.math.BigDecimal.valueOf(2)).toString());
                    }
                }
            }
        }
        SplittableRandom random = new SplittableRandom(20261009);
        // the exact midpoint between a double and the next, and a hair either side: ties go to the even one
        for (int i = 0; i < 4_000; i++) {
            double v = Math.abs(Double.longBitsToDouble(random.nextLong()));
            if (!Double.isFinite(v) || !Double.isFinite(Math.nextUp(v))) {
                continue;
            }
            java.math.BigDecimal mid = new java.math.BigDecimal(v).add(new java.math.BigDecimal(Math.nextUp(v)))
                    .divide(java.math.BigDecimal.valueOf(2));
            java.math.BigDecimal hair = mid.ulp().movePointLeft(3);
            texts.add(mid.toString());
            texts.add(mid.add(hair).toString());
            texts.add(mid.subtract(hair).toString());
        }
        for (String text : texts) {
            assertEquals(Double.doubleToRawLongBits(Double.parseDouble(text)),
                    Double.doubleToRawLongBits(PortableText.doubleOf(text)), text);
        }
    }

    @Test
    void doubleOfIsTheJdks_overRandomDecimals() {
        SplittableRandom random = new SplittableRandom(91002620);
        for (int i = 0; i < 30_000; i++) {
            int digits = random.nextInt(1, 26);
            StringBuilder s = new StringBuilder(random.nextBoolean() ? "-" : "");
            for (int k = 0; k < digits; k++) {
                s.append((char) ('0' + random.nextInt(10)));
                if (k == 0 && digits > 1 && random.nextBoolean()) {
                    s.append('.');
                }
            }
            s.append('e').append(random.nextInt(-345, 330));
            String text = s.toString();
            assertEquals(Double.doubleToRawLongBits(Double.parseDouble(text)),
                    Double.doubleToRawLongBits(PortableText.doubleOf(text)), text);
        }
        // and every double's own text reads back as that double
        for (int i = 0; i < 30_000; i++) {
            double v = Double.longBitsToDouble(random.nextLong());
            if (Double.isFinite(v)) {
                assertEquals(Double.doubleToRawLongBits(v), Double.doubleToRawLongBits(PortableText.doubleOf(Double.toString(v))));
            }
        }
    }

    /** 30,000 and 10,000 (2026-10-09, cut from 300,000 and 100,000 for the target's time: the exact spelling of a
     *  random double runs to hundreds of digits): the edge tests above hold every power of two and of ten, both
     *  neighbours of each, and the bounds; these sample between them. */
    @Test
    void doubleTextIsTheJdks_overRandomBitPatterns() {
        SplittableRandom random = new SplittableRandom(20261009);
        for (int i = 0; i < 30_000; i++) {
            double v = Double.longBitsToDouble(random.nextLong());
            assertEquals(Double.toString(v), PortableText.doubleText(v), "bits " + Long.toHexString(Double.doubleToRawLongBits(v)));
        }
        // and decimals as written: up to 17 digits at every exponent
        for (int i = 0; i < 10_000; i++) {
            double v = Double.parseDouble(random.nextLong(1, 100_000_000_000_000_000L) + "e" + random.nextInt(-340, 300));
            assertEquals(Double.toString(v), PortableText.doubleText(v), "bits " + Long.toHexString(Double.doubleToRawLongBits(v)));
        }
    }
}
