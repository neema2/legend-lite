// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link OutputMatrixTest}'s matrix on H2, against H2's own pins (moved here at main's merge, 2026-10-22, when a test
 *  naming a database must be named for it). */
class OutputMatrixH2Test {

    @Test
    void everyKindOnEveryOutputWritesPuresText_orIsPinnedByName() throws Exception {
        OutputMatrixTest.assertEveryKindWritesPuresText(TestBackends.of(DatabaseType.H2));
    }
}
