// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

/** {@link ObjectIdentityTest}'s evaluations on SQLite (moved here at main's merge, 2026-10-22, when a test naming a
 *  database must be named for it). */
class ObjectIdentitySqliteTest {

    @Test
    void everyEvaluationMakesAnObject_aLetNamesOne() throws Exception {
        ObjectIdentityTest.assertEveryEvaluationMakesAnObject(TestBackends.of(DatabaseType.SQLite));
    }
}
