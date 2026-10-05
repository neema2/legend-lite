// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A corpus lane's verdict (Bazel workplan P2-15): both passes are build actions (spec/corpus.bzl), the host judge's
 * and the database judge's (which joins the two per assert), each a cached output with its JUnit exit code and log.
 * This test is the lane's red or green: it fails, quoting the failing pass's log, unless both passed. Their measured
 * rosters are held to the committed copies by the diff tests of //spec:update_rcorpus_<lane>.
 */
@Tag("heavy")
class CorpusVerdictTest {

    @Test
    void bothPassesPassed() throws Exception {
        JudgeLedger.requireHostPassed();
        JudgeLedger.requirePassed("database-judge", "legend.judge.database.verdict", "legend.judge.database.log");
    }
}
