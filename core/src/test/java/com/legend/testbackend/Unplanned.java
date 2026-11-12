// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.executionplan.PlannedSql;

/**
 * A TEST'S OWN TEXT for a session's statements (the C checkpoint's MUST-FIX 1, 2026-11-12): a harness's seed, a probe of
 * the seam itself. The product sends only what a plan carries ({@link PlannedSql}, minted where a plan is read:
 * {@code ArchitectureTest.onlyThePlansReadersMintPlannedSql}, which checks the product's classes); a test types its own
 * text here, named for what it is -- the hosts' {@code unplanned} (engine-client/test/unplanned.ts), in Java.
 */
public final class Unplanned {

    private Unplanned() {
    }

    /** {@code text}, a test's own statement, typed as a session's statements take one. */
    public static PlannedSql sql(String text) {
        return PlannedSql.fromPlan(text);
    }
}
