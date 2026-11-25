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
 * database (DuckDB, H2, SQLite). The toString and toCSV halves measured equal on every carrier but the corrections
 * pinned below, until B2b's S5a and S5b deleted the edge's toString and toCSV SQL (2026-11-24 and 2026-11-25); a TDS's
 * csv stands until S5c.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PureRelationTextParityTest {

    // (theJavaPrinterWritesTheEdgesSqlText: gone 2026-11-24, stage B's B2b S5a -- the edge's SQL toString is deleted: a
    // relation's toString is lite's own text, computed by the database on every plan style (ProductRelationTextTest; its
    // Float and DateTime cells the edge's until S6, FloatTextTest), and legend-pure's text of a last step is the doors' Java printer's; its measure held until then, every carrier
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

    // (theCsvPrintsAgainstTheEdgesSqlText: gone 2026-11-25, stage B's B2b S5b -- the edge's toCSV SQL is deleted: the
    // helper's toCSV is lite's own CSV, computed by the database on every plan style (ProductRelationTextTest; the same
    // window), and
    // legend-pure's text of a last step is the doors' Java printer's; its measure held until then, every carrier equal
    // on each lane but a cell with a time of day)

    /**
     * A toCSV's cell with a time of day in the Java printer: helperFunctions' {@code SimpleDateTimeFormat}
     * ({@code yyyy-MM-dd HH:mm:ss}: the seconds, no fraction, no zone), the helper's own text (B2b S4's correction (b),
     * 2026-11-25; until S5b the edge's SQL wrote Pure's TDS DateTime text, {@code 2014-01-01T10:20:30.123456+0000}, the
     * window's gap 4).
     */
    @Test
    void aToCsvsDateTimeCellIsTheHelpersFormatInThePrinter() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String rel = "#TDS\n  i:Integer, t:DateTime\n  1, 2014-01-01T10:20:30.123456\n  2, 2014-01-01T10:20:30.5\n#"
                + "->sort(~i->ascending())";
        try (Connection c = backend.open(PureText.UPSTREAM)) {
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
