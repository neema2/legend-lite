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
 * go back to 6 ... and use decimal where need"; S311 amended by the user's ruling of 2026-11-23, "I think we do it as
 * 2.0"): {@code toString} of a Float is six decimals with the trailing zeros trimmed, a whole value keeping one, the
 * same text on each lane's database (DuckDB, H2, SQLite) -- a column's value and a
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
            // a whole value keeps one fraction digit (S311 amended, the user's ruling of 2026-11-23, "I think we do it
            // as 2.0"; B2a, 2026-11-24)
            assertEquals(List.of("1.5", "2.0", "-1.25", "1234567.89", "100000000000000000000.0"),
                    column(Lean.run(model, "|" + ROWS + "->extend(~t: r|$r.x->toOne()->toString())->select(~t)",
                            StorelessRuntime.RUNTIME, c)), backend.name());
            assertEquals("0.333333", scalar(model, c, "|(1.0 / 3.0)->toString()"), backend.name());
            assertEquals("0.3", scalar(model, c, "|(0.1 + 0.2)->toString()"), backend.name());
            assertEquals("0.0", scalar(model, c, "|0.0000001->toString()"), backend.name());
            // a negative whole value, and format's %f of a whole value: the same rule (B2a's end design audit, 2026-11-24)
            assertEquals("-2.0", scalar(model, c, "|(-2.0 * 1.0)->toString()"), backend.name());
            assertEquals("a 2.0 b", scalar(model, c, "|let x = 2.0 * 1.0; 'a %f b'->meta::pure::functions::string"
                    + "::format([$x]);"), backend.name());
            // legend-pure's print form, at the compatibility edge
            assertEquals(List.of("1.5", "2.0", "-1.25"), column(Lean.runInStyle(model, "|" + ROWS
                    + "->extend(~t: r|$r.x->toOne()->toString())->select(~t)", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)).subList(0, 3), backend.name());
            assertEquals("2.0", Lean.runInStyle(model, "|2.0->toString()", StorelessRuntime.RUNTIME, backend,
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

    /** THE WINDOW UNTIL B2b's S6 (the review of S5a, 2026-11-24): a relation's toString is lite's layout computed by the
     *  database on every plan style (lowering.ProductRelationText), but under the edge's style the edge dialect's value
     *  passes (edge.PurePrintForms) re-spell its Float cells as they re-spell any Float's text there: a table text used
     *  further at the edge writes each Float cell as the edge writes that value's toString -- legend-engine's in-query
     *  cast, 1e-05 on DuckDB, 1.0E-5 on H2 -- neither lite's text nor legend-pure's. No PCT or corpus case meets it (a
     *  last-step table is the doors' Java printer's: PureRelationTextTest, PureRelationTextParityTest). Pinned as the
     *  edge's own Float text, so S6's deletion of the passes moves it to lite's by name. (Until S5a, this test pinned
     *  legend-pure's print, which the edge's own table SQL wrote, PR #118's fix round.) */
    @Test
    void anEdgeTableTextUsedFurtherWritesItsFloatCellsAsTheEdgeWritesAFloat() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String rows = "#TDS\ni:Integer, x:Float\n1, 0.00001\n2, 0.0005\n3, 25000000.0\n#->sort(~i->ascending())";
        String table = Lean.runInStyle(model, "|" + rows + "->select(~x)->toString()", StorelessRuntime.RUNTIME, backend,
                com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0).toString();
        List<String> cells = new ArrayList<>();
        for (List<Object> row : Lean.runInStyle(model, "|" + rows + "->extend(~t: r|$r.x->toOne()->toString())"
                + "->select(~t)", StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.UPSTREAM).rows()) {
            cells.add(String.valueOf(row.get(0)));
        }
        assertEquals("#TDS\n   x\n   " + String.join("\n   ", cells) + "\n#", table, backend.name());
    }

    private static String edge(String model, Connection c, String query) throws Exception {
        return Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, LaneFloor.backend(), com.legend.edge.PureText.UPSTREAM).rows()
                .get(0).get(0).toString();
    }

    /** A Float the store holds as an integer, at the compatibility edge: legend-engine's in-query cast of the column,
     *  its digits ({@code Quantity not in range: 1000000}, the corpus's validation showcase), where the product prints
     *  every Float by one rule ({@code RelationalMappingIntegrationTest.testManyColumnTypes}, {@code 42.0}). Pinned
     *  2026-11-24 (B2a's end design audit) at the SQL each edge dialect writes for the product's node, so that no fixture
     *  is written: the edge's pass over the product's Float text keeps it today, and B2b deletes that pass, so its
     *  replacement must write the same. */
    @Test
    void anIntegerHeldFloatsTextAtTheEdgeIsLegendEnginesCast() {
        com.legend.sql.SqlExpr qty = com.legend.sql.SqlExpr.Column.of("t", "QTY", com.legend.sql.SqlType.Scalar.BIGINT,
                true, com.legend.sql.OutputCol.Origin.PHYSICAL);
        // the lane's own database (each lane runs it: DuckDB, H2, SQLite)
        TestBackend backend = LaneFloor.backend();
        com.legend.sql.dialect.SqlDialect edge = com.legend.edge.PureText.dialect(backend.type());
        assertEquals(edge.render(select(new com.legend.sql.SqlExpr.Cast(qty, com.legend.sql.SqlType.Scalar.VARCHAR,
                        com.legend.sql.SqlExpr.Reason.USER))),
                edge.render(select(com.legend.lowering.LiteralSpelling.floatPrint(qty))), backend.name());
    }

    private static com.legend.sql.SqlSelect select(com.legend.sql.SqlExpr e) {
        return new com.legend.sql.SqlSelect(List.of(new com.legend.sql.SqlSelect.Projection(e, "v", null)), false,
                new com.legend.sql.SqlSource.Table("T1", "t", List.of(new com.legend.sql.OutputCol("QTY",
                        com.legend.sql.SqlType.Scalar.BIGINT, true)), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of());
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
