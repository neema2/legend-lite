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
 * computed one. Since the edition program's step 8 (2026-11-26) it is the compatibility edge's text of a Float in a
 * query too (tenet 1 as the user amended it on 2026-11-25): legend-pure's print is Java's at a last step, at the
 * doors ({@code edge.PrintedRelations}), and legend-engine's cast is a row of its table inside a store query
 * ({@code edge.EngineTable}; {@code edge.EngineFloatTextRowTest}).
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
            // the same text in a query planned in the compatibility edge's style: lite's own (step 8, 2026-11-26)
            assertEquals(List.of("1.5", "2.0", "-1.25"), column(Lean.runInStyle(model, "|" + ROWS
                    + "->extend(~t: r|$r.x->toOne()->toString())->select(~t)", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)).subList(0, 3), backend.name());
            assertEquals("2.0", Lean.runInStyle(model, "|2.0->toString()", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0).toString(), backend.name());
        }
    }

    /** A Float's text in a query planned in the compatibility edge's style is lite's own (the edition program's step 8,
     *  2026-11-26): the edge's SQL pass that wrote legend-engine's in-query cast for it, a literal by legend-engine's
     *  literal processor, is deleted. (Until then this test pinned that cast's answers, legend-engine's own:
     *  {@code 0.000000013421} on DuckDB, {@code 1.3421E-8} on H2. They are a row of legend-engine's table now, inside a
     *  store query at its door: {@code edge.EngineFloatTextRowTest} holds the SQL on every database, and PCT's
     *  legend-engine lane the answers.) legend-pure's print of these at a last step is the doors' Java printer's
     *  ({@code PctPlanRoutePrintTest}). */
    @Test
    void aFloatsTextInAnEdgeQueryIsLitesOwn() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            for (String query : List.of("|0.000000013421->toString()", "|1.3421e-8->toString()", "|1.3421e8->toString()",
                    "|17.000->toString()", "|(1.0 / 3.0)->toString()")) {
                assertEquals(scalar(model, c, query), edge(model, c, query), backend.name() + " " + query);
            }
            assertEquals("0.0", edge(model, c, "|1.3421e-8->toString()"), backend.name());
            assertEquals("134210000.0", edge(model, c, "|1.3421e8->toString()"), backend.name());
        }
    }

    /** A TABLE TEXT USED FURTHER AT THE EDGE (the review of S5a, 2026-11-24, and its re-review, 2026-11-25): a
     *  relation's toString is lite's layout computed by the database on every plan style
     *  (lowering.ProductRelationText). Its FLOAT cells are lite's own text at the edge too since the edition program's
     *  step 8 (2026-11-26: the edge's Float pass is deleted, so the window this test pinned is closed for them). Its
     *  DATETIME cells are still re-spelled by the edge dialect's pass (edge.PurePrintForms) as it re-spells any
     *  DateTime's text there, legend-pure's {@code +0000} form: pinned two ways -- each cell is the edge's own text of
     *  that value, and the table is NOT lite's text, so the deletion of that pass turns this test red by name; the
     *  helper's toCSV used further alike (S5b, 2026-11-25). No PCT or corpus case meets either (a last-step table is the
     *  doors' Java printer's: PureRelationTextTest, PureRelationTextParityTest). */
    @Test
    void anEdgeTableTextUsedFurtherWritesItsCellsAsTheEdgeWritesTheirValues() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String rows = "#TDS\ni:Integer, x:Float, t:DateTime\n1, 0.00001, 2014-01-01T10:20:30.123\n2, 0.0005,"
                + " 2020-02-29T00:00:00\n3, 25000000.0, 2014-01-01T10:20:30.5\n#->sort(~i->ascending())";
        java.util.Map<String, String> lites = java.util.Map.of(
                "x", "#TDS\n   x\n   0.00001\n   0.0005\n   25000000.0\n#",
                "t", "#TDS\n   t\n   2014-01-01T10:20:30.123000000Z\n   2020-02-29T00:00:00.000000000Z\n"
                        + "   2014-01-01T10:20:30.500000000Z\n#");
        for (String col : List.of("x", "t")) {
            String table = Lean.runInStyle(model, "|" + rows + "->select(~" + col + ")->toString()",
                    StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0).toString();
            List<String> cells = new ArrayList<>();
            for (List<Object> row : Lean.runInStyle(model, "|" + rows + "->extend(~c: r|$r." + col
                    + "->toOne()->toString())->select(~c)", StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM).rows()) {
                cells.add(String.valueOf(row.get(0)));
            }
            assertEquals("#TDS\n   " + col + "\n   " + String.join("\n   ", cells) + "\n#", table,
                    backend.name() + " " + col);
            if (col.equals("x")) {
                assertEquals(lites.get(col), table, backend.name() + " x: a Float cell is lite's own text at the edge");
            } else {
                org.junit.jupiter.api.Assertions.assertNotEquals(lites.get(col), table, backend.name() + " " + col
                        + ": the edge's table text is lite's -- the window closed: re-point this test");
            }
            // the helper's toCSV used further (the review of S5b, 2026-11-25): its DateTime cell the same window, the edge's
            // text of its value; its Float cell the window CLOSED, the database's own full-precision text since the
            // user's ruling of 2026-11-25 ("okay make exact"; SqlFn.FLOAT_FULL_TEXT, B3's (B)), which no edge pass
            // re-spells -- the same CSV at the edge's door and in the product
            String csv = Lean.runInStyle(model, "|" + rows + "->select(~" + col + ")->meta::relational::tests::csv::toCSV()",
                    StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0).toString();
            if (col.equals("x")) {
                String product = Lean.runInStyle(model, "|" + rows + "->select(~x)->meta::relational::tests::csv::toCSV()",
                        StorelessRuntime.RUNTIME, backend, com.legend.database.PlanStyle.PRODUCT).rows().get(0).get(0)
                        .toString();
                assertEquals(product, csv, backend.name() + " x toCSV: the database's own text at every door");
                continue;
            }
            assertEquals(col + "\n" + String.join("\n", cells) + "\n", csv, backend.name() + " " + col + " toCSV");
            org.junit.jupiter.api.Assertions.assertNotEquals(lites.get(col).replace("#TDS\n   ", "").replace("\n   ", "\n")
                    .replace("\n#", "\n"), csv, backend.name() + " " + col
                    + ": the edge's CSV is lite's -- the window closed (S6): re-point this test");
        }
    }

    private static String edge(String model, Connection c, String query) throws Exception {
        return Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, LaneFloor.backend(), com.legend.edge.PureText.UPSTREAM).rows()
                .get(0).get(0).toString();
    }

    // (anIntegerHeldFloatsTextAtTheEdgeIsLegendEnginesCast: gone 2026-11-26, the edition program's step 8 -- it held
    // the edge dialect's pass to the cast of the column as held, "so that its replacement must write the same". The
    // replacement is a row of legend-engine's table: edge.EngineFloatTextRowTest holds the same SQL, and the corpus's
    // validation showcase the answer, "Quantity not in range: 1000000".)

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
