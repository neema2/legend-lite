// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.guards;

import com.legend.tools.edgenet.EdgeNormalize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * THE EDGE GOLDEN NET's PROOF, its normalizer's half (S0 of the presentation program, 2026-11-18). The review of PR
 * #123 (2026-11-19) moved it out of the PCT module, which may depend on no tool that sorts (PctDisciplineTest). It sits
 * in this package because this package may use JUnit: the jar pools' rule (tools/deps/pools.bzl) names no tools/edgenet
 * user, and none is added. Planted changes over relation lines as the PCT recorder writes them (rows separated by
 * U+001E), put through {@link EdgeNormalize#normalized}: what the net must pass passes (a swap within a sort-key tie,
 * the rows of a line with no order), what it must catch moves the line (a row-order change across distinct keys).
 * Each plant is built so a full sort of the rows would answer otherwise, so it fails if the normalizer sorts what it
 * must keep.
 *
 * <p>The lines are {@code testSortCanonicalNullOrdering}'s answer on DuckDB (sorted by {@code id}, nulls last).
 */
class EdgeNormalizeTest {

    private static final String RS = "\u001e";

    private static final String COLUMNS = "columns=[{\"name\":\"id\",\"type\":\"Integer\",\"relationalType\":\"INTEGER\"},"
            + "{\"name\":\"name\",\"type\":\"String\",\"relationalType\":\"VARCHAR(1024)\"}]";

    private static String relation(String order, String... rows) {
        return "testSortCanonicalNullOrdering\t001\tRELATION\t" + COLUMNS + (order == null ? "" : " order=" + order)
                + " rows=" + String.join(RS, rows);
    }

    /** The line with its rows joined as the normalizer joins them, in the order given. */
    private static String joined(String order, String... rows) {
        return relation(order, rows).replace(RS, ",");
    }

    /** The line with its rows fully sorted: what a normalizer that ignored the order would write. */
    private static String fullySorted(String order, String... rows) {
        List<String> sorted = new ArrayList<>(List.of(rows));
        Collections.sort(sorted);
        return joined(order, sorted.toArray(String[]::new));
    }

    private static final String SACHIN = "{\"values\":[1,\"Sachin\"]}";
    private static final String GEORGE = "{\"values\":[2,\"George\"]}";
    private static final String PIERRE = "{\"values\":[null,\"Pierre\"]}";
    private static final String GEORGE_TIED = "{\"values\":[2,\"Anne\"]}";
    /** A name holding a quote, as the recorder writes it: the JSON's {@code \"} with its backslash doubled. */
    private static final String OBRIEN = "{\"values\":[3,\"O\\\\\"Brien\"]}";

    @Test
    void aRowOrderChangeAcrossDistinctKeysMovesTheLine() {
        String recorded = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE, PIERRE));
        String planted = EdgeNormalize.normalized(relation("[\"id\"]", GEORGE, SACHIN, PIERRE));
        assertNotEquals(recorded, planted, "the net sees a row-order change across distinct sort keys");
        assertEquals(joined("[\"id\"]", SACHIN, GEORGE, PIERRE), recorded,
                "a sorted answer keeps the database's order across distinct keys");
        assertNotEquals(fullySorted("[\"id\"]", GEORGE, SACHIN, PIERRE), planted,
                "the planted order is not the fully sorted one, so the plant could not pass by a sort");
    }

    @Test
    void aSwapWithinATiePasses() {
        String recorded = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE, GEORGE_TIED, PIERRE));
        String planted = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE_TIED, GEORGE, PIERRE));
        assertEquals(recorded, planted, "a tie's order is the database's: the net does not see it");
    }

    @Test
    void aKeyHoldingAnEscapedCharacterIsRead() {
        // sorted by name: O"Brien before Sachin, the reverse of their rows' text order ([3,...] after [1,...])
        assertEquals(joined("[\"name\"]", OBRIEN, SACHIN), EdgeNormalize.normalized(relation("[\"name\"]", OBRIEN, SACHIN)),
                "distinct keys keep their order, a key with an escaped quote read as JSON");
        assertNotEquals(fullySorted("[\"name\"]", OBRIEN, SACHIN), joined("[\"name\"]", OBRIEN, SACHIN),
                "the rows' order is not their sorted order, so the plant could not pass by a sort");
    }

    @Test
    void aLineWithNoOrderHasItsRowsSorted() {
        assertEquals(fullySorted(null, SACHIN, GEORGE, PIERRE),
                EdgeNormalize.normalized(relation(null, PIERRE, GEORGE, SACHIN)),
                "an unsorted answer's order is the database's, as before S0");
    }

    @Test
    void aFirstKeyNamingNoColumnSortsTheRows() {
        assertEquals(fullySorted("[\"?\"]", SACHIN, GEORGE, PIERRE),
                EdgeNormalize.normalized(relation("[\"?\"]", PIERRE, GEORGE, SACHIN)),
                "a sort on an expression no column projects cannot be followed, so its rows are sorted");
    }

    @Test
    void theLongestPrefixNamingColumnsIsFollowed() {
        // sorted by id, then by an expression no column projects: the net follows id and sorts within its ties
        String line = relation("[\"id\",\"?\"]", PIERRE, GEORGE, GEORGE_TIED, SACHIN);
        assertEquals(joined("[\"id\",\"?\"]", PIERRE, GEORGE_TIED, GEORGE, SACHIN), EdgeNormalize.normalized(line),
                "across distinct ids the rows keep their order; within id 2's tie they are sorted");
        assertNotEquals(EdgeNormalize.normalized(line),
                EdgeNormalize.normalized(relation("[\"id\",\"?\"]", SACHIN, GEORGE, GEORGE_TIED, PIERRE)),
                "a row-order change across distinct ids moves the line");
    }

    @Test
    void aNonRelationLineIsUnchanged() {
        String value = "testArcCosine\t002\tVALUE\ttype=FLOAT values=Double:3.141592653589793;";
        assertEquals(value, EdgeNormalize.normalized(value), "the normalizer orders a relation's rows and nothing else");
    }

    @Test
    void theScannerReadsTheRecordersJson() {
        assertEquals(List.of("1", "\"a,b\"", "[2,3]", "{\"k\":\"]\"}"),
                EdgeNormalize.elements("[1,\"a,b\",[2,3],{\"k\":\"]\"}]").orElseThrow(),
                "top-level elements, not split inside a string, an array or an object");
        assertEquals("O\"Brien\\né", EdgeNormalize.text("\"O\\\"Brien\\\\n\\u00e9\""),
                "a string's escapes read");
    }
}
