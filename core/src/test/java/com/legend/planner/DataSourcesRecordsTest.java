// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A JSON connection's payload cut into its records (B2a, 2026-11-11: the frame's E3 pins, moved from the resolver with
 * the cut): each record an object's TEXT as written, never a JSON value in Java -- the database parses each one the
 * plan's load writes into the frame's session table. The scan follows strings: braces inside them are data.
 */
@DisplayName("DataSources.records: a JSON connection's payload (B2a)")
class DataSourcesRecordsTest {

    private static final String URL = "data:application/json,...";

    @Test
    @DisplayName("an array yields its objects; an empty payload is no record")
    void arrayAndEmpty() {
        assertEquals(List.of("{\"a\":1}", "{\"a\":2}"), DataSources.records("[{\"a\":1},{\"a\":2}]", URL));
        assertEquals(List.of(), DataSources.records("  \n ", URL));
        assertEquals(List.of(), DataSources.records("[]", URL));
    }

    @Test
    @DisplayName("a single object yields itself; the connection's row stream yields one record per object")
    void objectAndStream() {
        assertEquals(List.of("{\"a\":1}"), DataSources.records("{\"a\":1}", URL));
        assertEquals(List.of("{\"a\":1}", "{\"a\":2}", "{\"a\":3}"),
                DataSources.records("\n{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", URL));
    }

    @Test
    @DisplayName("braces and quotes inside JSON strings are data: the scan never miscounts them")
    void stringAwareScan() {
        assertEquals(List.of("{\"a\":\"}{\"}", "{\"b\":\"\\\"{\"}"),
                DataSources.records("{\"a\":\"}{\"} {\"b\":\"\\\"{\"}", URL));
        // nested objects and arrays ride inside one record
        assertEquals(List.of("{\"emp\":[{\"n\":\"A\"},{\"n\":\"B\"}]}"),
                DataSources.records("{\"emp\":[{\"n\":\"A\"},{\"n\":\"B\"}]}", URL));
    }

    @Test
    @DisplayName("a payload that is no object, an array of no objects, a truncated stream or an open array is refused")
    void refusedByName() {
        for (String payload : List.of("42", "[1,2]", "[{\"a\":1} {\"a\":2}]", "{\"a\":1", "[{\"a\":1}")) {
            var e = assertThrows(NotImplementedException.class, () -> DataSources.records(payload, URL), payload);
            assertTrue(e.getMessage().contains("payload is an object, an array of objects or a stream of objects")
                    || e.getMessage().contains("payload is one JSON array"), e.getMessage());
        }
    }
}
