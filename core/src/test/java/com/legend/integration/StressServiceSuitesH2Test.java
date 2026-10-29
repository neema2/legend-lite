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
    static final int MIN_PASS = 5224;   // 5197 -> 5224 (2026-10-29, docs/STRESS_CORPUS_PLAN_2026_10_19.md: the plan's D8 reversed by the user, as on DuckDB: the 24 deep-stack totals and the three VGN_Sum services pass again; no test that passed fails) 5152 -> 5197 (2026-10-29, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 4 stage 5: the remaining scalars and control flow, 45 of 64 passing; the others are DuckDB's buckets and functions H2 lacks (cbrt, jaroWinklerSimilarity, time_bucket, the bit shifts); no test that passed fails) 5117 -> 5152 (2026-10-29, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 4 stage 4: relation and TDS shapes, 35 of 40 passing; the others are DuckDB's five; no test that passed fails) 5079 -> 5117 (2026-10-28, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 4 stage 3: the aggregates in a groupBy, 62 of 77 passing; the others are DuckDB's ten, H2's AVG of integers to eleven digits, `first` taking another row, and ARG_MAX/ARG_MIN, which H2 lacks; 24 deep-stack grouped totals that passed now fail, their expectation moved from null to legend-pure's 0 (F75); no other test that passed fails) 4910 -> 5079 (2026-10-28, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 4 stage 2: the function vocabulary, 169 of 258 passing; the others are functions H2 lacks (bit operations, base64, levenshtein, reverse, regexpCount, regexpIndexOf), a non-ISO weekOfYear and a Sunday-first firstDayOfWeek, TV-04's trimmed trailing space, 32-bit overflow, indexOf as strpos and map over an absent value; no test that passed fails) 4840 -> 4910 (2026-10-28, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 3: the intraday milestoning services, 70 of 90 passing; the 20 others take parameters, Phase 1's bucket; no test that passed fails) 4820 -> 4840 (2026-10-27, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 2: the seed spines and the deep variants re-rooted at classes with rows; 527 passing tests left the corpus, 547 came in and pass, and no test that passed fails) 4810 -> 4820 (2026-10-23, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 1: ten more enumeration-literal services, each value under `!=` and `||` of two, all passing) 4676 -> 4810 (2026-10-23, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 1: the typed-value services, 133 of them passing, and CB_NotEqualsNull, whose expectation now reads `!=` over an absent value as legend-pure does; F28 withdrawn) 4671 -> 4676 (2026-10-05: the referee's dateDiff follows legend-pure's elapsed time, F58; five services agree) 4622 -> 4671 (2026-10-05, Bazel workplan P3-12: the first run of the lane that enforces it; 4,671 of 4,736 measured) // 4509 -> 4571 -> 4596 -> 4600 -> 4607 -> 4612 (2-ULP judge policy F-AE; 2026-09-16/17: orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; CORRECTION: the 4602 written at a4c4a883d was measured with the graph-envelope +0000 spelling in the tree, reverted before the commit (ledger F-AA) — 4596 is that commit's measured count; +4 = isAlphaNumeric/splitPart on H2)

    @Test
    void suites() throws Exception {
        StressServiceSuitesTest.assertAtLeast(TestBackends.of(DatabaseType.H2), MIN_PASS);
    }
}
