// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * ROUND AS LEGEND-PURE ROUNDS (F47, 2026-11-17), its ties, on the lane's database. Read from legend-pure 5.99.0's
 * compiled runtime ({@code CoreHelper.java:464-508}):
 * <ul>
 *   <li>{@code round(Number):Integer} is half-even ({@code round(2.5)} is 2, {@code round(-2.5)} is -2);</li>
 *   <li>{@code round(Float, n)} is the double's SHORTEST decimal, half-up: {@code BigDecimal.valueOf(d).setScale(n,
 *       HALF_UP)} -- {@code round(1.015, 2)} is 1.02 (the double nearest 1.015 is below it, its shortest decimal is
 *       not), {@code round(0.25, 1)} is 0.3, {@code round(-0.125, 2)} is -0.13;</li>
 *   <li>{@code round(Decimal, n)} is half-up, exact.</li>
 * </ul>
 * The expected values are computed here by legend-pure's own rule, in Java.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class FloatRoundTest {

    private static final String[][] TO_SCALE = {{"0.25", "1"}, {"1.015", "2"}, {"0.125", "2"}, {"-0.125", "2"},
            {"-17.36", "1"}, {"2.675", "2"}, {"123456.785", "2"}};

    @Test
    void roundToAScaleIsHalfUpOverTheShortestDecimal_andToAnIntegerHalfEven() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        List<String> wrong = new ArrayList<>();
        try (Connection c = backend.open()) {
            for (String[] t : TO_SCALE) {
                int n = Integer.parseInt(t[1]);
                // legend-pure: BigDecimal.valueOf(d).setScale(n, HALF_UP), the double again
                double floatWant = BigDecimal.valueOf(Double.parseDouble(t[0])).setScale(n, RoundingMode.HALF_UP)
                        .doubleValue();
                BigDecimal decimalWant = new BigDecimal(t[0]).setScale(n, RoundingMode.HALF_UP);
                Object f = run(model, c, "|round(" + t[0] + ", " + n + ")");
                Object d = run(model, c, "|round(" + t[0] + "D, " + n + ")");
                System.out.println("[float round] " + backend.name() + " round(" + t[0] + ", " + n + "): Float " + f
                        + ", Decimal " + d);
                if (!(f instanceof Number x && x.doubleValue() == floatWant)) {
                    wrong.add("round(" + t[0] + ", " + n + "): want " + floatWant + ", got " + f);
                }
                if (!(d instanceof Number x && new BigDecimal(x.toString()).compareTo(decimalWant) == 0)) {
                    wrong.add("round(" + t[0] + "D, " + n + "): want " + decimalWant + ", got " + d);
                }
            }
            for (String[] t : new String[][] {{"2.5", "2"}, {"-2.5", "-2"}, {"3.5", "4"}, {"0.5", "0"}}) {
                Object r = run(model, c, "|round(" + t[0] + ")");
                if (!(r instanceof Number x && x.longValue() == Long.parseLong(t[1]))) {
                    wrong.add("round(" + t[0] + "): want " + t[1] + ", got " + r);
                }
            }
        }
        assertEquals(List.of(), wrong, backend.name());
    }

    private static Object run(String model, Connection c, String query) {
        try {
            return Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0);
        } catch (RuntimeException refused) {
            return refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }
}
