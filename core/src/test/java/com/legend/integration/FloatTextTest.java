// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A FLOAT'S TEXT ON THE PRODUCT ROAD (phase D's D1.2, 2026-11-15; register S311, the user's ruling of 2026-11-10: "let's
 * go back to 6 ... and use decimal where need"): {@code toString} of a Float is six decimals with the trailing zeros and
 * a trailing point trimmed, the same text on each lane's database (DuckDB, H2, SQLite) -- a column's value and a
 * computed one. legend-pure's print form is the compatibility edge's ({@code edge.PurePrintForms}), pinned beside it.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class FloatTextTest {

    private static final String ROWS = "#TDS\nx:Float\n1.5\n2.0\n-1.25\n1234567.89\n100000000000000000000.0\n#";

    @Test
    void aFloatsTextIsSixDecimalsTrimmed() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            assertEquals(List.of("1.5", "2", "-1.25", "1234567.89", "100000000000000000000"),
                    column(Lean.run(model, "|" + ROWS + "->extend(~t: r|$r.x->toOne()->toString())->select(~t)",
                            StorelessRuntime.RUNTIME, c)), backend.name());
            assertEquals("0.333333", scalar(model, c, "|(1.0 / 3.0)->toString()"), backend.name());
            assertEquals("0.3", scalar(model, c, "|(0.1 + 0.2)->toString()"), backend.name());
            assertEquals("0", scalar(model, c, "|0.0000001->toString()"), backend.name());
            // legend-pure's print form, at the compatibility edge
            assertEquals(List.of("1.5", "2.0", "-1.25"), column(Lean.runInStyle(model, "|" + ROWS
                    + "->extend(~t: r|$r.x->toOne()->toString())->select(~t)", StorelessRuntime.RUNTIME, c,
                    com.legend.edge.PureText.UPSTREAM)).subList(0, 3), backend.name());
            assertEquals("2.0", Lean.runInStyle(model, "|2.0->toString()", StorelessRuntime.RUNTIME, c,
                    com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0).toString(), backend.name());
        }
    }

    private static String scalar(String model, Connection c, String query) throws Exception {
        return Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0).toString();
    }

    private static List<String> column(Lean.Result r) {
        List<String> out = new ArrayList<>();
        for (List<Object> row : r.rows()) {
            out.add(String.valueOf(row.get(0)));
        }
        return out;
    }
}
