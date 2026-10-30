// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.judge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The graph verdict on an INCIDENTAL-order chain: the ROOT array is a
 *  multiset, nested arrays stay ordered (the union graph-fetch witness:
 *  three firms in H2's arrival order vs DuckDB's, identical content). */
class EqualityJsonUnorderedRootTest {

    private static final String B = "{\"legalName\":\"Firm B\",\"employees\":[{\"lastName\":\"Bala\"},{\"lastName\":\"Raman\"}]}";
    private static final String X = "{\"legalName\":\"Firm X\",\"employees\":[{\"lastName\":\"Scott\"},{\"lastName\":\"Anand\"}]}";
    private static final String A = "{\"legalName\":\"Firm A\",\"employees\":[{\"lastName\":\"Roberts\"}]}";

    @Test
    @DisplayName("root arrays with the same elements in another order are equal")
    void rootOrderIsNotAContract() {
        Object expected = Json.parse("[" + B + "," + X + "," + A + "]");
        Object actual = Json.parse("[" + X + "," + A + "," + B + "]");
        assertEquals("$[0].legalName expected Firm B, got Firm X",
                Equality.pureJson(expected, actual));
        assertNull(Equality.pureJsonUnorderedRoot(expected, actual));
    }

    @Test
    @DisplayName("a nested array's order still judges")
    void nestedOrderStillJudges() {
        Object expected = Json.parse("[" + B + "," + X + "]");
        String xSwapped = "{\"legalName\":\"Firm X\",\"employees\":[{\"lastName\":\"Anand\"},{\"lastName\":\"Scott\"}]}";
        Object actual = Json.parse("[" + xSwapped + "," + B + "]");
        // the sort key is the element's full content, so the swapped
        // employees land the two X's at different positions: the verdict
        // is a difference (never a silent pass), wherever it is reported
        assertTrue(Equality.pureJsonUnorderedRoot(expected, actual) != null);
    }

    @Test
    @DisplayName("a missing element is a size difference, never a silent pass")
    void missingElementFails() {
        Object expected = Json.parse("[" + B + "," + X + "]");
        Object actual = Json.parse("[" + X + "]");
        String d = Equality.pureJsonUnorderedRoot(expected, actual);
        assertTrue(d != null && d.contains("expected 2 element(s), got 1") && d.contains("Firm B"), d);
    }

    @Test
    @DisplayName("a non-array root compares as a document")
    void objectRootUnchanged() {
        assertNull(Equality.pureJsonUnorderedRoot(Json.parse(B), Json.parse(B)));
        assertEquals("$.legalName expected Firm B, got Firm X",
                Equality.pureJsonUnorderedRoot(Json.parse(B), Json.parse(X)));
    }
}
