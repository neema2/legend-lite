// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.legend.json.Json;
import com.legend.tools.edgenet.EdgeNormalize;

import org.junit.jupiter.api.Test;

/**
 * THE EDGE GOLDEN NET's PROOF (S0 of the presentation program, 2026-11-18,
 * docs/records/presentation-program-design-2026-11-18.md §4 (d)): planted changes over lines the net records, put
 * through its normalizer. What the net must pass passes (a swap within a sort-key tie, the rows of a line with no
 * order); what it must catch moves the normalized line (a row-order change across distinct keys, a Float's 17th
 * digit, a message). The recorder's half: an SQL-only change moves no line (the activities leave it), and its
 * mask hides an id, never a value.
 *
 * <p>The lines are real: {@code testSortCanonicalNullOrdering}'s answer on DuckDB (sorted by {@code id}, nulls last),
 * a value line of {@code testArcCosine} and an error line of {@code testAdjustByHoursBigNumber}, as the recorder writes
 * them (rows separated by U+001E).
 */
class EdgeNetPlantedTest {

    private static final String RS = "\u001e";

    private static final String COLUMNS = "columns=[{\"name\":\"id\",\"type\":\"Integer\",\"relationalType\":\"INTEGER\"},"
            + "{\"name\":\"name\",\"type\":\"String\",\"relationalType\":\"VARCHAR(1024)\"}]";

    private static String relation(String order, String... rows) {
        return "testSortCanonicalNullOrdering\t001\tRELATION\t" + COLUMNS + (order == null ? "" : " order=" + order)
                + " rows=" + String.join(RS, rows);
    }

    private static final String SACHIN = "{\"values\":[1,\"Sachin\"]}";
    private static final String GEORGE = "{\"values\":[2,\"George\"]}";
    private static final String PIERRE = "{\"values\":[null,\"Pierre\"]}";
    private static final String GEORGE_TIED = "{\"values\":[2,\"Anne\"]}";

    @Test
    void aRowOrderChangeAcrossDistinctKeysMovesTheLine() {
        String recorded = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE, PIERRE));
        String planted = EdgeNormalize.normalized(relation("[\"id\"]", GEORGE, SACHIN, PIERRE));
        assertNotEquals(recorded, planted, "the net sees a row-order change across distinct sort keys");
        assertEquals(relation("[\"id\"]", SACHIN, GEORGE, PIERRE).replace(RS, ","), recorded,
                "a sorted answer keeps the database's order across distinct keys");
    }

    @Test
    void aSwapWithinATiePasses() {
        String recorded = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE, GEORGE_TIED, PIERRE));
        String planted = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE_TIED, GEORGE, PIERRE));
        assertEquals(recorded, planted, "a tie's order is the database's: the net does not see it");
    }

    @Test
    void aKeyHoldingAnEscapedCharacterIsRead() {
        // the recorder doubles each backslash: a key cell holding a quote reaches the normalizer as \\"
        String quoted = "{\"values\":[3,\"O\\\\\"Brien\"]}";
        assertEquals(EdgeNormalize.normalized(relation("[\"name\"]", SACHIN, quoted)),
                relation("[\"name\"]", SACHIN, quoted).replace(RS, ","),
                "distinct keys keep their order, a key with an escaped quote read as JSON");
    }

    @Test
    void aLineWithNoOrderHasItsRowsSorted() {
        assertEquals(EdgeNormalize.normalized(relation(null, SACHIN, GEORGE, PIERRE)),
                EdgeNormalize.normalized(relation(null, PIERRE, GEORGE, SACHIN)),
                "an unsorted answer's order is the database's, as before S0");
    }

    @Test
    void aKeyNamingNoColumnSortsTheRows() {
        assertEquals(EdgeNormalize.normalized(relation("[\"?\"]", SACHIN, GEORGE, PIERRE)),
                EdgeNormalize.normalized(relation("[\"?\"]", PIERRE, GEORGE, SACHIN)),
                "a sort on an expression no column projects cannot be followed, so its rows are sorted");
    }

    @Test
    void aFloatsSeventeenthDigitMovesTheLine() {
        String recorded = "testArcCosine\t002\tVALUE\ttype=ExprType[type=FLOAT, multiplicity=Bounded[lower=1, upper=1]]"
                + " values=Double:3.141592653589793;";
        String planted = recorded.replace("3.141592653589793", "3.1415926535897936");
        assertNotEquals(EdgeNormalize.normalized(recorded), EdgeNormalize.normalized(planted),
                "the net sees a value's 17th digit");
    }

    @Test
    void aMessageChangeMovesTheLine() {
        String recorded = "testAdjustByHoursBigNumber\t001\tERROR\tOut of Range Error: Interval value 12345678912 hours"
                + " out of range";
        String planted = recorded.replace("hours out of range", "hours is out of range");
        assertNotEquals(EdgeNormalize.normalized(recorded), EdgeNormalize.normalized(planted),
                "the net sees a message change");
    }

    /** A relation answer as the server writes legend-engine's JSON, with its activities' SQL. */
    private static Json.Obj answer(String sql) {
        return Json.parseObject("{\"builder\":{\"_type\":\"tdsBuilder\","
                + "\"columns\":[{\"name\":\"id\",\"type\":\"Integer\"}]},"
                + "\"activities\":[{\"_type\":\"relational\",\"sql\":" + Json.toCompact(Json.str(sql)) + "}],"
                + "\"result\":{\"columns\":[\"id\"],\"rows\":[{\"values\":[1]},{\"values\":[2]}]}}");
    }

    @Test
    void anSqlOnlyChangeMovesNoLine() {
        Json.Obj recorded = answer("SELECT \"t0\".\"id\" AS \"id\" FROM \"T\" AS \"t0\" ORDER BY 1");
        Json.Obj planted = answer("SELECT \"t1\".\"id\" AS \"id\" FROM \"T\" AS \"t1\" ORDER BY 1");
        assertEquals(PctCaseRecorder.relationText(recorded, java.util.List.of("id")),
                PctCaseRecorder.relationText(planted, java.util.List.of("id")), "an alias rename is lite's SQL alone");
        assertNotEquals(PctCaseRecorder.activities(recorded), PctCaseRecorder.activities(planted),
                "the informational file still shows it");
    }

    @Test
    void aRawNewlineAndAnEscapedOneAreTwoTexts() {
        assertNotEquals(PctCaseRecorder.flat("'a\nb'"), PctCaseRecorder.flat("'a\\nb'"),
                "EDGE-3's raw newline is not legend-pure's escaped one: the net tells them apart");
    }

    @Test
    void theMaskHidesAnIdNeverAValue() {
        assertEquals("Long:1234567891234567891;", PctCaseRecorder.flat("Long:1234567891234567891;"),
                "a value's digits are the net's to see (until S0 any 16 hexadecimal characters were masked)");
        assertEquals("Double:3.1415926535897936;", PctCaseRecorder.flat("Double:3.1415926535897936;"));
        assertEquals("execution <id> failed",
                PctCaseRecorder.flat("execution 3f2504e0-4f89-11d3-9a0c-0305e82c3301 failed"),
                "an execution id is masked");
    }
}
