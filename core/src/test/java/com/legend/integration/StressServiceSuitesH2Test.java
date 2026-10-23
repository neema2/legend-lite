package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * GATE 10 on H2 (//core:stress_suites_h2): the stress corpus's service suites ({@link StressSuites}) on H2 2.4.240, a
 * fresh, freshly seeded session per test — the engine's own shape — held to its floor
 * ({@link #MIN_PASS}; StressServiceSuitesTest's until the dialect program's Phase 6, 2026-10-16). Enforced by a lane
 * since 2026-10-05 (Bazel workplan P3-12).
 */
@org.junit.jupiter.api.Tag("stress")
@DisplayName("Stress corpus: service test suites through legend-lite (H2)")
class StressServiceSuitesH2Test {

    /** The H2 lane's own floor (fresh session per test; the engine's shape): the
     *  DuckDB floor minus the H2 walls (EPOCH_MS/REVERSE, last-digit floats,
     *  timestamp text — ledger F-P). */
    static final int MIN_PASS = 4820;   // 4810 -> 4820 (2026-10-23, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 1: ten more enumeration-literal services, each value under `!=` and `||` of two, all passing) 4676 -> 4810 (2026-10-23, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 1: the typed-value services, 133 of them passing, and CB_NotEqualsNull, whose expectation now reads `!=` over an absent value as legend-pure does; F28 withdrawn) 4671 -> 4676 (2026-10-05: the referee's dateDiff follows legend-pure's elapsed time, F58; five services agree) 4622 -> 4671 (2026-10-05, Bazel workplan P3-12: the first run of the lane that enforces it; 4,671 of 4,736 measured) // 4509 -> 4571 -> 4596 -> 4600 -> 4607 -> 4612 (2-ULP judge policy F-AE; 2026-09-16/17: orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; CORRECTION: the 4602 written at a4c4a883d was measured with the graph-envelope +0000 spelling in the tree, reverted before the commit (ledger F-AA) — 4596 is that commit's measured count; +4 = isAlphaNumeric/splitPart on H2)

    @Test
    void suites() throws Exception {
        StressServiceSuitesTest.assertAtLeast(TestBackends.of(DatabaseType.H2), MIN_PASS);
    }
}
