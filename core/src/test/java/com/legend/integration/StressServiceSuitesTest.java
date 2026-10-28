package com.legend.integration;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GATE 10 on DuckDB (//core:stress_suites): the stress corpus's service suites ({@link StressSuites}) on one seeded
 * session per distinct provisioning, shared across the tests that declare it (isolation by write detection: a session
 * that executed a statement with an effect is re-seeded before the next test). The count judged EQUAL to the referee may
 * only grow: raise {@link #MIN_PASS} with every leg that burns a bucket; never lower it. The measuring knobs (data
 * overrides, row dumps, one service, a session policy) are {@code bazel run //core:stress_tool}'s (Bazel workplan
 * P3-12).
 */
@org.junit.jupiter.api.Tag("stress")   // GATE 10: its own gate, excluded from the core suite by tag
@DisplayName("Stress corpus: service test suites through legend-lite (DuckDB)")
class StressServiceSuitesTest {

    /** Tests judged equal to the referee (first full run 2026-09-16: 2,702 of 4,736). Shrink-proof. */
    static final int MIN_PASS = 4939;   // 4869 -> 4939 (2026-10-28, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 3: the intraday milestoning services, 70 of 90 passing; the 20 others take parameters, Phase 1's bucket; no test that passed fails) 4849 -> 4869 (2026-10-27, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 2: the seed spines and the deep variants re-rooted at classes with rows; 527 passing tests left the corpus, 547 came in and pass, and no test that passed fails) 4839 -> 4849 (2026-10-23, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 1: ten more enumeration-literal services, each value under `!=` and `||` of two, all passing) 4705 -> 4839 (2026-10-23, docs/STRESS_CORPUS_PLAN_2026_10_19.md Phase 1: the typed-value services, 133 of them passing, and CB_NotEqualsNull, whose expectation now reads `!=` over an absent value as legend-pure does; F28 withdrawn) 4700 -> 4705 (2026-10-05: the referee's dateDiff follows legend-pure's elapsed time, F58; five services agree) 2702 -> 2765 -> 4203 -> 4564 -> 4626 -> 4654 -> 4672 -> 4679 -> 4689 (2-ULP judge policy F-AE; 2026-09-16/17: double division; pins; association anchors; F-M; F-O; orElse → coalesce; OR/range navigation aggregates; timestamp JSON spelling; isAlphaNumeric + firstHourOfDay on DuckDB + splitPart index base; view column kinds + grouped-predicate scoping + routed sub-join key demand)


    @Test
    void suites() throws Exception {
        assertAtLeast(TestBackends.platform(), MIN_PASS);
    }

    /** One full run on {@code backend}, its pass count at or above {@code floor}. */
    static void assertAtLeast(TestBackend backend, int floor) throws Exception {
        StressSuites.Result r = StressSuites.run(new StressSuites.Config(backend, null, "", List.of(), null, com.legend.testing.TestOutputs.dir()));
        assertFalse(r.pass().isEmpty(), "nothing passed — the harness itself is broken");
        assertTrue(r.pass().size() >= floor, r.pass().size() + " tests passed, below the ratchet " + floor
                + " — see stress-suites-fail*.txt in the test's outputs");
    }
}
