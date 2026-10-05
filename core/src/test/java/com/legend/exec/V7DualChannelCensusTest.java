// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V7 batch-1 witness (docs/V7_ASSERT_VERDICT_CHARTER.md §5-1): the
 * dual-channel census's accounting — agree/disagree per form, named
 * declines, the side-size histogram — before the corpus sweep leans on
 * it. Measurement-instrument pins only; no verdict flows through here.
 */
// reads process-wide counters (CanonicalDivergence, Census) that every query bumps: runs alone if JUnit ever runs
// classes in parallel, so no other test's calls land between its reset and its assert (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.Isolated
class V7DualChannelCensusTest {

    @Test
    void perFormAccountingAndSummary() {
        CanonicalDivergence.reset();
        try {
            CanonicalDivergence.v7Verdict("assertEquals/2", true, true, "");
            CanonicalDivergence.v7Verdict("assertEquals/2", false, false, "");
            CanonicalDivergence.v7Verdict("assertEquals/2", true, false,
                    "canonical renders differ");
            CanonicalDivergence.v7Verdict("assertSameElements/2", false,
                    true, "host multiset failed, prod passed");
            CanonicalDivergence.v7Declined("assertSameSQL/2",
                    "assert-sql-text-with-exec-passing");
            CanonicalDivergence.v7Declined("assertSameSQL/2",
                    "assert-sql-text-unable-to-exec :: diff-noreplay");
            assertEquals(2, CanonicalDivergence.v7DisagreeCount());
            assertEquals(2, CanonicalDivergence.v7DeclinedCount());
            // the user-ratified OUTCOME buckets are headline columns;
            // 'declined' is real backlog ONLY; sub-reasons ride behind
            // " :: " and count under their bucket prefix
            assertTrue(CanonicalDivergence.v7Summary().startsWith(
                    "dual-channel agree=2 disagree=2 | sql-text:"
                            + " exec-passing=1 text-only=0 UNABLE-TO-EXEC=1"
                            + " | test-data-csv=0 | declined=0"
                            + " | metamodel-quarantined=0"),
                    CanonicalDivergence.v7Summary());
            var report = CanonicalDivergence.v7Report();
            assertTrue(report.contains(
                    "form assertEquals/2 agree=2 disagree=1"), report.toString());
            assertTrue(report.contains(
                    "form assertSameElements/2 agree=0 disagree=1"),
                    report.toString());
            assertTrue(report.contains(
                    "declined assertSameSQL/2 ::"
                            + " assert-sql-text-with-exec-passing = 1"),
                    report.toString());
            assertTrue(report.contains(
                    "declined assertSameSQL/2 ::"
                            + " assert-sql-text-unable-to-exec ::"
                            + " diff-noreplay = 1"),
                    report.toString());
            // both disagreements carry a witness row
            assertEquals(2, report.stream()
                    .filter(l -> l.startsWith("disagree-witness ")).count(),
                    report.toString());
        } finally {
            CanonicalDivergence.reset();
        }
    }

    @Test
    void sideRowHistogramBuckets() {
        CanonicalDivergence.reset();
        try {
            CanonicalDivergence.v7SideRows(0);
            CanonicalDivergence.v7SideRows(1);
            CanonicalDivergence.v7SideRows(2);
            CanonicalDivergence.v7SideRows(3);
            CanonicalDivergence.v7SideRows(7);
            String s = CanonicalDivergence.v7Summary();
            assertTrue(s.endsWith("side-rows 0:1 1:1 2-3:2 4-7:1"), s);
        } finally {
            CanonicalDivergence.reset();
        }
    }
}
