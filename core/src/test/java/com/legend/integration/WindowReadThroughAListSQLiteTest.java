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
 * before 2026-11-19). The chunked form is refused by name before any window is read: chunk's pattern is built from its
 * size, and SQLite compiles a literal pattern only (a loud refusal, kept: the compatibility program's owner,
 * 2026-11-19).
 */
class WindowReadThroughAListSQLiteTest {

    @Test
    void answered() throws Exception {
        WindowReadThroughAList.answered(TestBackends.of(DatabaseType.SQLite), Map.of(WindowReadThroughAList.CHUNKED,
                "a regexp_extract_all pattern that is not a literal reached SQLite"));
    }
}
