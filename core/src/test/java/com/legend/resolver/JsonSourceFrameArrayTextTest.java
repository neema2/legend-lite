// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E3 pins: the JSON source frame's INGRESS is SCISSORS ONLY —
 * {@link JsonSourceFrame#objectTexts} cuts the payload into per-object
 * TEXT spans without ever materializing a JSON value in Java (the
 * DATABASE parses the cells). The scan is string-aware: braces inside
 * JSON strings are data, not structure.
 */
@DisplayName("JsonSourceFrame payload split (E3)")
class JsonSourceFrameArrayTextTest {

    @Test
    @DisplayName("an array yields its objects; an empty payload is a zero-row frame")
    void arrayAndEmpty() {
        assertEquals(List.of("{\"a\":1}", "{\"a\":2}"),
                JsonSourceFrame.objectTexts("[{\"a\":1},{\"a\":2}]", "m::C"));
        assertEquals(List.of(),
                JsonSourceFrame.objectTexts("  \n ", "m::C"));
    }

    @Test
    @DisplayName("a single object yields itself; the engine's row-stream yields one span per object")
    void objectAndStream() {
        assertEquals(List.of("{\"a\":1}"),
                JsonSourceFrame.objectTexts("{\"a\":1}", "m::C"));
        assertEquals(List.of("{\"a\":1}", "{\"a\":2}", "{\"a\":3}"),
                JsonSourceFrame.objectTexts(
                        "\n{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", "m::C"));
    }

    @Test
    @DisplayName("braces and quotes inside JSON strings are data — the scan never miscounts them")
    void stringAwareScan() {
        assertEquals(List.of("{\"a\":\"}{\"}", "{\"b\":\"\\\"{\"}"),
                JsonSourceFrame.objectTexts(
                        "{\"a\":\"}{\"} {\"b\":\"\\\"{\"}", "m::C"));
        // nested objects and arrays ride INSIDE one span
        assertEquals(List.of("{\"emp\":[{\"n\":\"A\"},{\"n\":\"B\"}]}"),
                JsonSourceFrame.objectTexts(
                        "{\"emp\":[{\"n\":\"A\"},{\"n\":\"B\"}]}", "m::C"));
    }

    @Test
    @DisplayName("non-object payloads, truncated streams, and unterminated arrays wall loudly")
    void loudWalls() {
        var e = assertThrows(NotImplementedException.class, () ->
                JsonSourceFrame.objectTexts("42", "m::C"));
        assertTrue(e.getMessage().contains(
                "neither an object nor an array"), e.getMessage());
        var t = assertThrows(NotImplementedException.class, () ->
                JsonSourceFrame.objectTexts("{\"a\":1", "m::C"));
        assertTrue(t.getMessage().contains("truncated"), t.getMessage());
        var u = assertThrows(NotImplementedException.class, () ->
                JsonSourceFrame.objectTexts("[{\"a\":1}", "m::C"));
        assertTrue(u.getMessage().contains("unterminated"), u.getMessage());
    }
}
