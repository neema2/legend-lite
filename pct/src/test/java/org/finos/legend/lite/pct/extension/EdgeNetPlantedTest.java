// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.legend.json.Json;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * THE EDGE GOLDEN NET's PROOF, the recorder's half (S0 of the presentation program, 2026-11-18,
 * docs/records/presentation-program-design-2026-11-18.md §4 (d); re-cut by the review of PR #123, 2026-11-19):
 * planted changes put through the recorder's own writers -- {@link PctCaseRecorder#flat}, every line's text, and
 * {@link PctCaseRecorder#relationText}, a relation's -- where S0's bug was (until S0 the mask hid any run of 16
 * hexadecimal characters, a value's digits among them). What the net must catch moves the recorded text (a Float's
 * 17th digit, a long number in a message); what it must pass does not (an SQL-only change); and the root ORDER BY's
 * keys are written as the plan names them ({@link PctPlanRoute#rootOrder}). The normalizer's half, which sorts, is
 * outside this module (tools/edgenet's EdgeNormalizeTest; PctDisciplineTest).
 */
class EdgeNetPlantedTest {

    /** A relation answer as the server writes legend-engine's JSON: one Float column, its rows, its activities' SQL. */
    private static Json.Obj answer(String sql, String... values) {
        StringBuilder rows = new StringBuilder();
        for (String v : values) {
            rows.append(rows.isEmpty() ? "" : ",").append("{\"values\":[").append(v).append("]}");
        }
        return Json.parseObject("{\"builder\":{\"_type\":\"tdsBuilder\","
                + "\"columns\":[{\"name\":\"x\",\"type\":\"Float\"}]},"
                + "\"activities\":[{\"_type\":\"relational\",\"sql\":" + Json.toCompact(Json.str(sql)) + "}],"
                + "\"result\":{\"columns\":[\"x\"],\"rows\":[" + rows + "]}}");
    }

    private static final String SQL = "SELECT \"t0\".\"x\" AS \"x\" FROM \"T\" AS \"t0\"";

    @Test
    void aFloatsSeventeenthDigitMovesTheLine() {
        // 0.30000000000000004's digits after the point are 17 hexadecimal characters: the mask S0 replaced hid them
        String recorded = "type=FLOAT values=Double:0.30000000000000004;";
        String planted = "type=FLOAT values=Double:0.30000000000000006;";
        assertNotEquals(PctCaseRecorder.flat(recorded), PctCaseRecorder.flat(planted),
                "a value line: the net sees a Float's 17th digit");
        assertNotEquals(PctCaseRecorder.flat(PctCaseRecorder.relationText(answer(SQL, "0.30000000000000004"), null)),
                PctCaseRecorder.flat(PctCaseRecorder.relationText(answer(SQL, "0.30000000000000006"), null)),
                "a relation's cell: the net sees a Float's 17th digit");
    }

    @Test
    void aMessageChangeMovesTheLine() {
        // a message holding a long number: its digits are a run of 16 or more hexadecimal characters
        String recorded = "Out of Range Error: Interval value 1234567891234567891 hours out of range";
        String planted = recorded.replace("1234567891234567891", "1234567891234567892");
        assertNotEquals(PctCaseRecorder.flat(recorded), PctCaseRecorder.flat(planted),
                "the net sees a message's number change");
        assertNotEquals(PctCaseRecorder.flat(recorded), PctCaseRecorder.flat(recorded.replace("hours out", "hours is out")),
                "the net sees a message's words change");
    }

    @Test
    void anSqlOnlyChangeMovesNoLine() {
        Json.Obj recorded = answer(SQL, "1.5");
        Json.Obj planted = answer(SQL.replace("t0", "t1"), "1.5");
        assertEquals(PctCaseRecorder.relationText(recorded, List.of("x")),
                PctCaseRecorder.relationText(planted, List.of("x")), "an alias rename is lite's SQL alone");
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

    /** A relation query's plan on the platform's database, in the edge's style, planned only (no database). */
    private static com.legend.executionplan.ExecutionPlan planned(String query) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(PctBackend.withStorelessRuntime("")), query)
                .executionPlan(com.legend.testbackend.StorelessRuntime.RUNTIME, java.util.Set.of(),
                        com.legend.edge.PureText.UPSTREAM);
    }

    private static final String TDS = "|#TDS\n  id, name\n  2, b\n  1, a\n#";

    @Test
    void theRootOrderNamesTheSortedColumn() {
        assertEquals(List.of("id"), PctPlanRoute.rootOrder(planned(TDS + "->sort(~id->descending())")),
                "a sort on a projected column: its name");
    }

    @Test
    void theRootOrderNamesAnAliasedKeyByItsAlias() {
        assertEquals(List.of("ident"),
                PctPlanRoute.rootOrder(planned(TDS + "->rename(~id, ~ident)->sort(~ident->ascending())")),
                "a sort on a renamed column: the name the answer carries");
    }

    @Test
    void theRootOrderMarksAnUnprojectedKey() {
        assertEquals(List.of("?"),
                PctPlanRoute.rootOrder(planned(TDS + "->sort(~id->ascending())->select(~name)")),
                "a sort on a column the answer does not carry: no name to follow");
    }

    @Test
    void aQueryWithNoRootSortHasNoOrder() {
        assertNull(PctPlanRoute.rootOrder(planned(TDS + "->select(~name)")), "no root ORDER BY: no order= at all");
    }
}
