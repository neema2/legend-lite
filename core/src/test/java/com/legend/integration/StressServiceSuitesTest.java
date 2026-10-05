package com.legend.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GATE 10 on DuckDB (//core:stress_suites): the stress corpus's service suites ({@link StressSuites}) on one seeded
 * session per distinct provisioning, shared across the tests that declare it (isolation by write detection: a session
 * that executed a statement with an effect is re-seeded before the next test). The count judged EQUAL to the oracle may
 * only grow: raise {@link #MIN_PASS} with every leg that burns a bucket; never lower it. The measuring knobs (data
 * overrides, row dumps, one service, a session policy) are {@code bazel run //core:stress_tool}'s (Bazel workplan
 * P3-12).
 */
@org.junit.jupiter.api.Tag("stress")   // GATE 10: its own gate, excluded from the core suite by tag
@DisplayName("Stress corpus: service test suites through legend-lite (DuckDB)")
class StressServiceSuitesTest {

    /** The H2 lane's own floor (fresh session per test; the engine's shape): the
     *  DuckDB floor minus the H2 walls (EPOCH_MS/REVERSE, last-digit floats,
     *  timestamp text — ledger F-P). {@link StressServiceSuitesH2Test} holds it. */
    static final int MIN_PASS_H2 = 4676;   // 4671 -> 4676 (2026-10-05: the oracle's dateDiff follows legend-pure's elapsed time, F58; five services agree) 4622 -> 4671 (2026-10-05, Bazel workplan P3-12: the first run of the lane that enforces it; 4,671 of 4,736 measured) // 4509 -> 4571 -> 4596 -> 4600 -> 4607 -> 4612 (2-ULP judge policy F-AE; 2026-09-16/17: orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; CORRECTION: the 4602 written at a4c4a883d was measured with the graph-envelope +0000 spelling in the tree, reverted before the commit (ledger F-AA) — 4596 is that commit's measured count; +4 = isAlphaNumeric/splitPart on H2)
    /** Tests judged equal to the oracle (first full run 2026-09-16: 2,702 of 4,736). Shrink-proof. */
    static final int MIN_PASS = 4705;   // 4700 -> 4705 (2026-10-05: the oracle's dateDiff follows legend-pure's elapsed time, F58; five services agree) 2702 -> 2765 -> 4203 -> 4564 -> 4626 -> 4654 -> 4672 -> 4679 -> 4689 (2-ULP judge policy F-AE; 2026-09-16/17: double division; pins; association anchors; F-M; F-O; orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; isAlphaNumeric + firstHourOfDay on DuckDB + splitPart index base; view column kinds + grouped-predicate scoping + routed sub-join key demand)


    @Test
    void suites() throws Exception {
        assertAtLeast(false, MIN_PASS);
    }

    /** One full run on the backend, its pass count at or above {@code floor}. */
    static void assertAtLeast(boolean h2, int floor) throws Exception {
        StressSuites.Result r = StressSuites.run(new StressSuites.Config(h2, null, "", List.of(), null, com.legend.testing.TestOutputs.dir()));
        assertFalse(r.pass().isEmpty(), "nothing passed — the harness itself is broken");
        assertTrue(r.pass().size() >= floor, r.pass().size() + " tests passed, below the ratchet " + floor
                + " — see stress-suites-fail" + (h2 ? "-h2" : "") + ".txt in the test's outputs");
    }
}
