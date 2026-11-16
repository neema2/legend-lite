// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.ExecuteOptions;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import com.legend.testing.Program;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * PURE'S RAISE UNDER THE COMPATIBILITY RULES, THE PRODUCT'S ANSWER UNDER LITE'S (the user's ruling of 2026-11-16,
 * register S-batch-3): {@code round(x, scale)} over a Float NaN or infinity. Legend-pure reads the Float through
 * {@code BigDecimal.valueOf} and raises a NumberFormatException naming the character it cannot read; lite's product
 * passes the value through, as IEEE and the databases do. A body judged by the compatibility rules raises pure's words;
 * judged by lite's own, it answers the value. On each in-process database that holds a NaN (one that holds none reads
 * {@code parseFloat('NaN')} as 0.0, and so has nothing to raise: it is checked to hold none).
 */
class CompatibilityRaisesTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    /** Each Float pure cannot round, and pure's words for it. */
    private static final Map<String, String> VALUES = Map.of(
            "parseFloat('NaN')", CompatibilityRaises.NAN,
            "parseFloat('Infinity')", CompatibilityRaises.INFINITY,
            "parseFloat('-Infinity')", CompatibilityRaises.INFINITY);

    @Test
    void roundOverANonFiniteFloatRaisesPuresWordsUnderTheCompatibilityRulesAlone() throws Exception {
        List<String> checked = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            String model = backend.withStorelessRuntime(MODEL);
            for (Map.Entry<String, String> v : VALUES.entrySet()) {
                // the product's answer: the value itself, where the database holds it
                Object answered = Lean.service(model, "|" + v.getKey() + "->round(2)",
                        com.legend.testbackend.StorelessRuntime.RUNTIME).rows().get(0).get(0);
                double value = ((Number) answered).doubleValue();
                if (Double.isFinite(value)) {
                    // a database that holds no NaN or infinity reads the text as 0.0: nothing for pure's raise to meet
                    assertEquals(0.0, value, backend.name() + ": " + v.getKey());
                    continue;
                }
                String body = "|assertEquals(1.0, " + v.getKey() + "->round(2))";
                try (Connection c = backend.open()) {
                    // lite's own rules: the value, so the assert fails on it (never a raise)
                    com.legend.error.AssertFailed lite = assertThrows(com.legend.error.AssertFailed.class,
                            () -> Program.run(model, body, com.legend.testbackend.StorelessRuntime.RUNTIME, c,
                                    ExecuteOptions.NONE.withJudgeMode(Program.laneJudge()).withEngineCompat(false)));
                    assertTrue(!String.valueOf(lite.getMessage()).contains("is neither a decimal digit"),
                            backend.name() + ": " + lite.getMessage());
                    // the compatibility rules: pure's raise, in pure's words
                    Throwable compat = assertThrows(Throwable.class, () -> Program.run(model, body,
                            com.legend.testbackend.StorelessRuntime.RUNTIME, c,
                            ExecuteOptions.NONE.withJudgeMode(Program.laneJudge()).withEngineCompat(true)));
                    assertTrue(String.valueOf(compat.getMessage()).contains(v.getValue()),
                            backend.name() + ": " + v.getKey() + ": " + compat);
                }
                checked.add(backend.name() + " " + v.getKey());
            }
        }
        assertTrue(checked.size() >= 6, "at least two databases hold a NaN and both infinities: " + checked);
    }
}
