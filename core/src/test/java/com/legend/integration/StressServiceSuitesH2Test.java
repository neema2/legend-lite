package com.legend.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * GATE 10 on H2 (//core:stress_suites_h2): the stress corpus's service suites ({@link StressSuites}) on H2 2.4.240, a
 * fresh, freshly seeded session per test — the engine's own shape — held to its floor
 * ({@link StressServiceSuitesTest#MIN_PASS_H2}). Enforced by a lane since 2026-10-05 (Bazel workplan P3-12).
 */
@org.junit.jupiter.api.Tag("stress")
@DisplayName("Stress corpus: service test suites through legend-lite (H2)")
class StressServiceSuitesH2Test {

    @Test
    void suites() throws Exception {
        StressServiceSuitesTest.assertAtLeast(true, StressServiceSuitesTest.MIN_PASS_H2);
    }
}
