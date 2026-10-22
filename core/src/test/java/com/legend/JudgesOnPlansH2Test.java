// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link JudgesOnPlansTest}'s bodies on H2 (moved here at main's merge, 2026-10-22, when a test naming a database must
 *  be named for it): every one but those needing a list encoding H2's dialect has not. */
class JudgesOnPlansH2Test {

    @Test
    void everyBodyHoldsUnderBothJudges() throws Exception {
        JudgesOnPlansTest.assertEveryBodyHolds(TestBackends.of(DatabaseType.H2));
    }
}
