// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * M4 §3.4 — THE PCT CENSUS GATE, each PCT suite's teardown. Its pins RETIRED 2026-11-09: the label-lie census it
 * asserted (SqlTypeCensus: mismatch == 0, untyped and bottom-mult under their ceilings) had no feeder -- its probe lost
 * its last caller -- so every count read zero by construction, a dead gate; the census is deleted with the static state
 * it held (the user's ruling of 2026-11-09, no mutable static state). The wire census's five pins retired the same way
 * on 2026-10-29 (D64). Their history is in git. What it still does: it prints the bodies each suite ran whole on the
 * program road (the lanes' measurement), judged on the suite's own traffic (Bazel workplan P3-09).
 */
public final class PctCensusGate {

    private PctCensusGate() {
    }

    public static Test wrap(String suite, Test t) {
        return new TestSetup(t) {
            /** The bodies the body router ran whole on the program road before this suite (the test-is-the-product
             *  plan's M6, 2026-11-01: PCT judges on the host, so its assert programs are the first PCT bodies to
             *  move; the count is the lanes' measurement). */
            private long movedStart;

            @Override
            protected void setUp() {
                movedStart = com.legend.diagnostics.Census.count(com.legend.diagnostics.Census.Key.BODIES_MOVED);
            }

            @Override
            protected void tearDown() {
                System.out.println("[pct-census] " + suite + " (its own traffic): bodies-moved="
                        + (com.legend.diagnostics.Census.count(com.legend.diagnostics.Census.Key.BODIES_MOVED) - movedStart));
            }
        };
    }
}
