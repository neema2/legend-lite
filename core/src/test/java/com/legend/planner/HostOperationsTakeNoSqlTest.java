// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * A HOST OPERATION TAKES NO SQL (the final audit, 2026-11-12, the user's ruling): a host names an operation and its data
 * through the boundary, as the tab and Python do ({@code Boundary.hostOperations}), and a request that hands one a
 * query -- a count's source, a relation's -- is refused by name, nothing written. A query is planned from its pull: a
 * snapshot's copy is its own plan, a count a Pure query's.
 */
class HostOperationsTakeNoSqlTest {

    private static IllegalArgumentException refused(String operations) {
        return assertThrows(IllegalArgumentException.class, () -> planner.Boundary.hostOperations("DuckDB", operations));
    }

    @Test
    void aQueryAsAnOperationsSourceIsRefusedByName() {
        String why = "a query is planned from its pull (snapCopy, or a Pure query's plan), never handed to a host"
                + " operation";
        assertEquals(why, refused("[{\"op\":\"count\",\"from\":{\"query\":\"SELECT 1\"}}]").getMessage());
        assertEquals(why, refused("[{\"op\":\"relation\",\"kind\":\"table\",\"target\":{\"name\":\"t\"},"
                + "\"from\":{\"query\":\"SELECT 1\"}}]").getMessage());
        // a refusal in the list writes nothing for the operations before it
        assertEquals(why, refused("[{\"op\":\"schema\",\"name\":\"s\"},{\"op\":\"count\",\"from\":{\"query\":"
                + "\"SELECT 1\"}}]").getMessage());
    }

    @Test
    void aSourceIsARelation() {
        assertEquals("a source is a relation", refused("[{\"op\":\"count\",\"from\":{\"table\":\"t\"}}]").getMessage());
        assertEquals("[[\"SELECT count(*) AS n FROM \\\"t\\\"\"]]", planner.Boundary.hostOperations("DuckDB",
                "[{\"op\":\"count\",\"from\":{\"relation\":{\"name\":\"t\"}}}]"));
    }
}
