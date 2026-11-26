// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * {@link WindowReadThroughAList}'s shapes on SQLite, whose list operations are correlated subqueries over
 * {@code json_each}: PCT's rows where the window is computed over its partition (it answered each row's own name
 * before 2026-11-19). The chunked form answers too since 2026-11-26: {@code chunk} with a written size writes a
 * literal pattern (the edition program's step 1, the user's ruling of 2026-11-25 on the Pure-mode gaps). Until then it
 * was refused by name before any window was read, its pattern built from its size, which SQLite does not compile; a
 * computed size is refused still.
 */
class WindowReadThroughAListSQLiteTest {

    @Test
    void answered() throws Exception {
        WindowReadThroughAList.answered(TestBackends.of(DatabaseType.SQLite), Map.of());
    }
}
