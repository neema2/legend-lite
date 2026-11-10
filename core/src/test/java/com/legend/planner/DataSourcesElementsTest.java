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
 * A data: payload's split ({@link DataSources#elements}, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §4, A5): the one splitter
 * of every data: source -- a query's {@code sourceUrl}, an M2M source, a JSON source frame's -- its cases carried over
 * from the frame's own splitter ({@code JsonSourceFrame.objectTexts}, deleted with the frame's literal rows). SCISSORS
 * ONLY: each element its text, cut without a JSON value materializing in Java (the database parses the cells); the scan
 * is string-aware, so braces inside JSON strings are data, not structure.
 */
@DisplayName("A data: payload's split (A5)")
class DataSourcesElementsTest {

    private static final String URL = "data:application/json,...";

    @Test
    @DisplayName("an array yields its elements; an empty payload none")
    void arrayAndEmpty() {
        assertEquals(List.of("{\"a\":1}", "{\"a\":2}"), DataSources.elements("[{\"a\":1},{\"a\":2}]", URL));
        assertEquals(List.of(), DataSources.elements("  \n ", URL));
        assertEquals(List.of(), DataSources.elements("[]", URL));
    }

    @Test
    @DisplayName("a single object yields itself; the engine's row-stream yields one element per object")
    void objectAndStream() {
        assertEquals(List.of("{\"a\":1}"), DataSources.elements("{\"a\":1}", URL));
        assertEquals(List.of("{\"a\":1}", "{\"a\":2}", "{\"a\":3}"),
                DataSources.elements("\n{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", URL));
    }

    @Test
    @DisplayName("braces and quotes inside JSON strings are data -- the scan never miscounts them")
    void stringAwareScan() {
        assertEquals(List.of("{\"a\":\"}{\"}", "{\"b\":\"\\\"{\"}"),
                DataSources.elements("{\"a\":\"}{\"} {\"b\":\"\\\"{\"}", URL));
        // nested objects and arrays ride INSIDE one element
        assertEquals(List.of("{\"emp\":[{\"n\":\"A\"},{\"n\":\"B\"}]}"),
                DataSources.elements("{\"emp\":[{\"n\":\"A\"},{\"n\":\"B\"}]}", URL));
        assertEquals(List.of("{\"a\":\"x,]\"}", "[1, 2]"), DataSources.elements("[{\"a\":\"x,]\"}, [1, 2]]", URL));
    }

    @Test
    @DisplayName("a payload that is no array, object or stream of objects, a truncated one and an unterminated array are refused by name")
    void refusedByName() {
        for (String payload : List.of("42", "{\"a\":1", "[{\"a\":1}", "{\"a\":1},{\"a\":2}")) {
            var e = assertThrows(NotImplementedException.class, () -> DataSources.elements(payload, URL), payload);
            assertTrue(e.getMessage().contains("a JSON array, an object or a stream of objects"), e.getMessage());
        }
    }
}
