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

    /** A relation's select list and a column's replacement were SQL a host handed in (the final audit's 1(d),
     *  2026-11-12): each key refused by name, nothing written. */
    @Test
    void aSelectListOrAReplacementIsRefusedByName() {
        for (String key : java.util.List.of("selectList", "replacing")) {
            String value = key.equals("selectList") ? "\"*\"" : "[{\"column\":\"x\",\"sql\":\"CAST(x AS VARCHAR)\"}]";
            assertEquals("a host operation takes no SQL, and " + key + " is SQL: a copy's conversions are written by the"
                    + " platform from its columns' catalog (convert)", refused("[{\"op\":\"relation\",\"kind\":"
                    + "\"table\",\"target\":{\"name\":\"t\"},\"" + key + "\":" + value + ",\"from\":{\"relation\":"
                    + "{\"name\":\"u\"}}}]").getMessage(), key);
        }
    }

    /** A copy names its source's catalog columns as the catalog answers them, and the platform writes their conversions
     *  (the final audit's 1(d), 2026-11-12): a zoned timestamp and an unsigned 64-bit integer converted, a UUID copied
     *  as it is. */
    @Test
    void aCopyNamesItsColumnsCatalog() {
        String columns = "[{\"name\":\"id\",\"dataType\":\"BIGINT\",\"logicalType\":\"BIGINT\"},"
                + "{\"name\":\"at\",\"dataType\":\"TIMESTAMP WITH TIME ZONE\",\"logicalType\":\"TIMESTAMP WITH TIME ZONE\"},"
                + "{\"name\":\"big\",\"dataType\":\"UBIGINT\",\"logicalType\":\"UBIGINT\"},"
                + "{\"name\":\"u\",\"dataType\":\"UUID\",\"logicalType\":\"UUID\"}]";
        assertEquals("[[\"CREATE OR REPLACE TABLE \\\"copy\\\" AS SELECT * REPLACE (CAST(timezone('UTC', \\\"at\\\") AS"
                + " TIMESTAMP) AS \\\"at\\\", CAST(\\\"big\\\" AS DECIMAL(20,0)) AS \\\"big\\\") FROM \\\"src\\\"\"]]",
                planner.Boundary.hostOperations("DuckDB", "[{\"op\":\"relation\",\"kind\":\"table\",\"target\":"
                        + "{\"name\":\"copy\"},\"replace\":true,\"convert\":" + columns + ",\"from\":{\"relation\":"
                        + "{\"name\":\"src\"}}}]"));
    }

    @Test
    void aSourceIsARelation() {
        assertEquals("a source is a relation", refused("[{\"op\":\"count\",\"from\":{\"table\":\"t\"}}]").getMessage());
        assertEquals("[[\"SELECT count(*) AS n FROM \\\"t\\\"\"]]", planner.Boundary.hostOperations("DuckDB",
                "[{\"op\":\"count\",\"from\":{\"relation\":{\"name\":\"t\"}}}]"));
    }
}
