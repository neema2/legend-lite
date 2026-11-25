// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.edge.PureRelationTextWriter;
import com.legend.edge.PureText;
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
 * THE JAVA PRINTER AGAINST THE EDGE'S SQL TEXT (stage B's B2, B2b S1, 2026-11-24): legend-pure's text of a relation as
 * the edge composed it in SQL ({@code edge.RelationTextLowering}) and as {@link com.legend.edge.PureRelationText}
 * prints it in Java from the same relation's typed rows, run on the same session in the edge's style, on each lane's
 * database (DuckDB, H2, SQLite). The toString half measured equal on every carrier but the corrections pinned below
 * until B2b's S5a deleted the edge's toString SQL (2026-11-24); the toCSV half stands until S5b, gap 4 pinned.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PureRelationTextParityTest {

    /** The relations: each carrier a relation's cell can hold, an empty cell, a name that needs quotes, no rows. */
    private static final List<String> RELATIONS = List.of(
            "#TDS\n  i:Integer, s:String, b:Boolean\n  1, a, true\n  2, b c, false\n  3, , \n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, f:Float\n  1, 2.0\n  2, 0.00001\n  3, 1.5\n  4, 25000000.0\n  5, -0.25\n"
                    + "  6, 0.1\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, d:StrictDate, t:DateTime\n  1, 2014-01-02, 2014-01-01T10:20:30.123\n"
                    + "  2, 2020-02-29, 2020-02-29T00:00:00\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, d:Date\n  1, 2014-01-02T10:20:30\n  2, 2020-02-29T23:59:59.5\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, s:String\n  1, x\n  2, y\n#->sort(~i->ascending())"
                    + "->extend(~[braced: r|'{' + $r.s->toOne() + '}', bracketed: r|'[it\\'s ' + $r.s->toOne() + ']'])",
            "#TDS\n  i:Integer, s:String\n  1, x\n#->extend(~'my col': r|$r.i->toOne() * 2)",
            "#TDS\n  i:Integer, s:String\n  1, x\n#->filter(r|$r.i == 2)",
            "#TDS\n  g:String, f:Float\n  a, 1.5\n  a, 2.25\n  b, 3.0\n#->groupBy(~g, ~total: r|$r.f : s|$s->sum())"
                    + "->sort(~g->ascending())",
            // the review of 2026-11-24's carriers
            "#TDS\n  i:Integer, t:DateTime\n  1, 2014-01-01T10:20:30.123456\n  2, 2014-01-01T10:20:30.5\n"
                    + "  3, 2014-01-01T10:20:30.1234\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, d:Decimal\n  1, 1.50\n  2, 2.0\n  3, -0.125\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, n:Number\n  1, 21\n  2, 2.5\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, f:Float\n  1, 1.5\n  2, 2.5\n#->groupBy(~i, ~a: r|$r.f : s|$s->average())"
                    + "->sort(~i->ascending())",
            // B2b's midpoint audit (2026-11-24): several rows in no order the program asks for -- each road's order
            "#TDS\n  i:Integer, s:String\n  3, c\n  1, a\n  2, b\n#");

    // (theJavaPrinterWritesTheEdgesSqlText: gone 2026-11-24, stage B's B2b S5a -- the edge's SQL toString is deleted: a
    // relation's toString is lite's own text, computed by the database on every plan style (ProductRelationTextTest),
    // and legend-pure's text of a last step is the doors' Java printer's; its measure held until then, every carrier
    // equal on each lane but the corrections below)

    /**
     * WHERE THE EDGE'S SQL TEXT IS WRONG, the Java printer writes legend-pure's (measured 2026-11-24 on each lane; B2b's
     * door and lift correct these as they route a relation's text through the printer):
     * <ul>
     *   <li>a Date column holding days: legend-pure prints each day ({@code s(value)}, a StrictDate's toString); the
     *       SQL composition keys on the slot, which lite types TIMESTAMP for an abstract Date, and wrote
     *       {@code 2014-01-02T00:00:00.000+0000} on DuckDB, H2 and SQLite. The typed row carries each value's own
     *       precision (the database answered a day), so the printer writes the day;</li>
     *   <li>a Float of 1e20 on SQLite: legend-pure prints {@code 100000000000000000000.0}; the SQL composition's
     *       shortest-digits decomposition wrote {@code 9223372036854775807.0} there (an integer cast that saturates), a
     *       wrong value. DuckDB and H2 wrote the right text;</li>
     *   <li>the Floats where the databases' SQL texts disagree with each other and with legend-pure's print
     *       ({@code CompiledSupport}'s {@code DecimalFormat("0.0")}): {@code -0.0} (DuckDB {@code -0.0}, H2 and SQLite
     *       {@code 0.0}; legend-pure {@code 0.0}), {@code 4.030184897929827E17} (H2 {@code 403018489792982700.0}; legend-pure
     *       {@code 403018489792982720.0}), {@code 1e23} (DuckDB {@code 99999999999999991611392.0}, H2
     *       {@code 100000000000000000000000.0}, SQLite {@code 9223372036854775807.0}; legend-pure
     *       {@code 99999999999999990000000.0}). The printer writes legend-pure's one text on every database.</li>
     * </ul>
     */
    @Test
    void whereTheEdgesSqlTextIsWrongTheJavaPrinterWritesLegendPures() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open(PureText.UPSTREAM)) {
            assertEquals("#TDS\n   i,d\n   1,2014-01-02\n   2,2020-02-29\n#", printed(model,
                    "#TDS\n  i:Integer, d:Date\n  1, 2014-01-02\n  2, 2020-02-29\n#->sort(~i->ascending())", c),
                    backend.name());
            assertEquals("#TDS\n   i,f\n   1,100000000000000000000.0\n   2,-100000000000000000000.0\n#", printed(model,
                    "#TDS\n  i:Integer, f:Float\n  1, 100000000000000000000.0\n  2, -100000000000000000000.0\n#"
                            + "->sort(~i->ascending())", c), backend.name());
            assertEquals("#TDS\n   i,f\n   1,0.0\n   2,403018489792982720.0\n   3,99999999999999990000000.0\n"
                    + "   4,0.30000000000000004\n#", printed(model, "#TDS\n  i:Integer, f:Float\n  1, -0.0\n"
                    + "  2, 403018489792982720.0\n  3, 1.0E23\n  4, 0.30000000000000004\n#->sort(~i->ascending())", c),
                    backend.name());
        }
    }

    /**
     * THE CSV PRINTS AGAINST THE EDGE'S SQL TEXT (B2b's midpoint audit, 2026-11-24: S2 lifts the helper's toCSV, which no
     * measure compared): for each relation, the helper's {@code toCSV()} and {@code toCSV(true)}, the edge's SQL text
     * against the Java printer's -- equal on each lane's database, but for a cell with a time of day, which is the
     * window's gap 4 ({@link #aToCsvsDateTimeCellIsTheWindowsGap4}).
     */
    @Test
    void theCsvPrintsAgainstTheEdgesSqlText() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        List<String> differ = new ArrayList<>();
        java.util.Map<PureRelationTextWriter.Form, String> forms = new java.util.LinkedHashMap<>();
        forms.put(PureRelationTextWriter.Form.TO_CSV, "->meta::relational::tests::csv::toCSV()");
        forms.put(PureRelationTextWriter.Form.TO_CSV_TDS_NULL, "->meta::relational::tests::csv::toCSV(true)");
        try (Connection c = backend.open(PureText.UPSTREAM)) {
            for (String rel : RELATIONS) {
                if (rel.contains("DateTime") || rel.contains("d:Date\n")) {
                    continue;   // a cell with a time of day: gap 4, pinned below
                }
                for (var form : forms.entrySet()) {
                    String sql;
                    String java;
                    try {
                        sql = Lean.runInStyle(model, "|" + rel + form.getValue(), StorelessRuntime.RUNTIME, c,
                                PureText.UPSTREAM).rows().get(0).get(0).toString();
                        java = printed(model, rel, c, form.getKey());
                    } catch (RuntimeException e) {
                        differ.add(form.getKey() + " " + rel.replace("\n", "\\n") + "\n  failed: " + e);
                        continue;
                    }
                    if (!sql.equals(java)) {
                        differ.add(form.getKey() + " " + rel.replace("\n", "\\n") + "\n  SQL:  "
                                + sql.replace("\n", "\\n") + "\n  Java: " + java.replace("\n", "\\n"));
                    }
                }
            }
        }
        assertEquals(List.of(), differ, backend.name());
    }

    /**
     * THE WINDOW'S GAP 4 (measured 2026-11-24 on DuckDB, H2 and SQLite): a toCSV's cell with a time of day is
     * helperFunctions' {@code SimpleDateTimeFormat} in the printer ({@code yyyy-MM-dd HH:mm:ss}: the seconds, no
     * fraction, no zone) and Pure's TDS DateTime text in the edge's SQL; every other cell of the measured relations
     * agrees. Held as measured until S4 reads it on the corpus and the user rules it (the progress record's S2 window).
     */
    @Test
    void aToCsvsDateTimeCellIsTheWindowsGap4() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String rel = "#TDS\n  i:Integer, t:DateTime\n  1, 2014-01-01T10:20:30.123456\n  2, 2014-01-01T10:20:30.5\n#"
                + "->sort(~i->ascending())";
        try (Connection c = backend.open(PureText.UPSTREAM)) {
            assertEquals("i,t\n1,2014-01-01T10:20:30.123456+0000\n2,2014-01-01T10:20:30.5+0000\n",
                    Lean.runInStyle(model, "|" + rel + "->meta::relational::tests::csv::toCSV()",
                            StorelessRuntime.RUNTIME, c, PureText.UPSTREAM).rows().get(0).get(0).toString(),
                    backend.name());
            assertEquals("i,t\n1,2014-01-01 10:20:30\n2,2014-01-01 10:20:30\n",
                    printed(model, rel, c, PureRelationTextWriter.Form.TO_CSV), backend.name());
        }
    }

    /**
     * A TDS's {@code csv} property, measured where it applies (B2b's midpoint audit, 2026-11-24): on a TDS value -- after
     * any relation function {@code .csv} reads a column named csv. The edge's SQL refuses a TDS literal's csv by name
     * (its rows reach the renderer as a mixed list); the printer writes legend-engine's text, so the door's lift answers
     * it (the progress record's S2 window).
     */
    @Test
    void aTdsLiteralsCsvIsRefusedByTheSqlAndWrittenByThePrinter() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String rel = "#TDS\n  i:Integer, s:String\n  3, c\n  1, a\n  2, \n#";
        try (Connection c = backend.open(PureText.UPSTREAM)) {
            Exception refused = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> Lean.runInStyle(model,
                    "|" + rel + ".csv", StorelessRuntime.RUNTIME, c, PureText.UPSTREAM));
            String chain = "";
            for (Throwable t = refused; t != null; t = t.getCause()) {
                chain += t.getMessage() + " | ";
            }
            org.junit.jupiter.api.Assertions.assertTrue(chain.contains("a tagged list reached the renderer"), chain);
            assertEquals("i, s\n3,c\n1,a\n2,TDSNull", printed(model, rel, c, PureRelationTextWriter.Form.TDS_CSV),
                    backend.name());
        }
    }

    /** {@code rel}'s rows, run on {@code c} in the edge's style, printed by the Java printer. */
    private static String printed(String model, String rel, Connection c) throws Exception {
        return printed(model, rel, c, PureRelationTextWriter.Form.TO_STRING);
    }

    /** {@code rel}'s rows, run on {@code c} in the edge's style, printed by the Java printer in {@code form}. */
    private static String printed(String model, String rel, Connection c, PureRelationTextWriter.Form form)
            throws Exception {
        List<String> text = new ArrayList<>();
        com.legend.Execution.run(com.legend.Compiler.query(com.legend.Compiler.compileModel(LaneFloor.model(model, c)),
                                "|" + rel).withOptions(Lean.TEST_OPTIONS), StorelessRuntime.RUNTIME,
                com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                com.legend.exec.ProductRun.Establish.DECLARED_SETUP, java.util.Set.of(), PureText.UPSTREAM,
                new com.legend.result.Output.Rows(new PureRelationTextWriter(form, List.of(), text::add)),
                com.legend.result.Batcher.DEFAULT_CAPACITY);
        if (text.size() != 1) {
            throw new IllegalStateException("the printer answered " + text.size() + " texts");
        }
        return text.get(0);
    }
}
