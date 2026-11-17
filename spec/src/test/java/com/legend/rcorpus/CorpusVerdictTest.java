// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A corpus lane's verdict (Bazel workplan P2-15): its pass is a build action (spec/corpus.bzl), the host judge's (the
 * corpus is compatibility, judged by the HOST alone since 2026-11-17), a cached output with its JUnit exit code and
 * log. This test is the lane's red or green: it fails, quoting the pass's log, unless it passed. Their measured
 * rosters are held to the committed copies by the diff tests of //spec:update_rcorpus_<lane>.
 */
@Tag("heavy")
class CorpusVerdictTest {

    @Test
    void thePassPassed() throws Exception {
        JudgeLedger.requireHostPassed();
    }
}
