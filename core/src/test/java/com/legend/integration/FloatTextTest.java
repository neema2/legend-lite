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

    /** A Float literal's {@code toString} at the compatibility edge is legend-engine's SQL (Phase 5, row 8 of the edge
     *  program, 2026-11-16): the literal as its literal processor writes it, then the database's cast. The default
     *  processor writes legend-pure's print bare, an exact numeric the database prints as written (legend-engine passes
     *  {@code testFloatToStringWithNegativeExponent} on DuckDB); H2's writes {@code CAST(%s AS FLOAT)}, a double, which
     *  H2 prints with an exponent ({@code h2Extension2_1_214.pure:152}). The H2 answers are legend-engine's own: its H2
     *  manifest's expected failures (legend-engine 4.145.0,
     *  {@code legend-engine-xt-relationalStore-h2-PCT/.../relational-h2/EssentialFunctions_manifest.json:628-632}:
     *  {@code testFloatToStringWithNegativeExponent} "expected: '0.000000013421' actual: '1.3421E-8'",
     *  {@code testFloatToStringWithPositiveExponent} "expected: '134210000.0' actual: '1.3421E8'"); the edge is
     *  identical to legend-engine on each database (the orchestrator's ruling, 2026-11-16). */
    @Test
    void anEdgeFloatLiteralsTextIsLegendEnginesSql() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        // legend-engine writes a Float literal CAST(%s AS FLOAT) on this lane's database (its H2 processor)
        boolean floatLiteralCast = backend.name().equals("h2");
        try (Connection c = backend.open()) {
            assertEquals(floatLiteralCast ? "1.3421E-8" : "0.000000013421", edge(model, c, "|0.000000013421->toString()"),
                    backend.name());
            assertEquals(floatLiteralCast ? "1.3421E-8" : "0.000000013421", edge(model, c, "|1.3421e-8->toString()"), backend.name());
            assertEquals(floatLiteralCast ? "1.3421E8" : "134210000.0", edge(model, c, "|1.3421e8->toString()"), backend.name());
            assertEquals("17.0", edge(model, c, "|17.000->toString()"), backend.name());
        }
    }

    private static String edge(String model, Connection c, String query) throws Exception {
        return Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, c, com.legend.edge.PureText.UPSTREAM).rows()
                .get(0).get(0).toString();
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
