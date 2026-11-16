// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * PURE'S EXACT DECIMAL ANSWERS (C5-J's batch, 1(a)-(c), 2026-11-15): wrong with no error until then. Read in legend-pure
 * 5.99.0, each the answer asserted here:
 * <ul>
 *   <li>{@code round(x, scale)} is HALF_UP, a Decimal's and a Float's alike (compiled CoreHelper.round(BigDecimal, long),
 *       {@code setScale(scale, HALF_UP)}; round(Double, long) through {@code BigDecimal.valueOf}, the double's shortest
 *       text; the interpreter's RoundWithScale). {@code round(x)} alone stays half-even (round.pure's own words and its
 *       PCT tests). Lite wrote SQL's ROUND, half-even on every database;</li>
 *   <li>{@code divide(a, b, scale)} is the exact quotient rounded HALF_UP (CompiledSupport.divideDecimal,
 *       {@code left.divide(right, scale, HALF_UP)}). Lite divided in double, then rounded: 2.01 / 2 at two places was
 *       1.00 on DuckDB, and 12345678901234567.89 / 1 lost its cents;</li>
 *   <li>{@code toDecimal(Float)} is {@code new BigDecimal(d.toString())} (CoreHelper.toDecimal), the double's shortest
 *       text, never its binary expansion.</li>
 * </ul>
 * Each probe runs on each in-process database, over literals and over a DOUBLE and a DECIMAL column. A database that
 * cannot hold Pure's answer gives the closest it holds, pinned per database in {@code pure-decimal/closest.tsv} (SQLite:
 * no decimal type, a Decimal is its double), each row measured -- a row whose database starts answering as Pure does
 * fails here as stale. Red on revert: the lowering's SqlFn.ROUND for round with a scale, or DIVIDE for divide.
 */
class PureDecimalArithmeticTest {

    private static final String MODEL = """
            ###Relational
            Database store::DB
            (
                Table T (ID INTEGER PRIMARY KEY, F DOUBLE, D DECIMAL(20, 2))
            )
            ###Connection
            RelationalDatabaseConnection store::Conn { store: store::DB; CONNECTION }
            ###Runtime
            Runtime test::RT { mappings: [ ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            ###Data
            Data test::D
            {
              Relational
              #{
                default.T:
                  'ID,F,D\\n1,1.005,2.01\\n2,0.125,-2.01\\n3,-0.125,12345678901234567.89\\n4,2.675,10\\n5,1.0049,1\\n6,0.30000000000000004,3\\n';
              }#
            }
            """;

    /** Each probe, a query of one value, and legend-pure's answer to it. */
    private static final Map<String, String> SCALARS = new LinkedHashMap<>();

    static {
        SCALARS.put("round(0.125D, 2)", "0.13");
        SCALARS.put("round(1.005D, 2)", "1.01");
        SCALARS.put("round(-0.125D, 2)", "-0.13");
        SCALARS.put("round(2.5, 0)", "3.0");
        SCALARS.put("round(-2.5, 0)", "-3.0");
        // round without a scale stays half-even, as pure's is
        SCALARS.put("round(16.5)", "16");
        SCALARS.put("round(17.5)", "18");
        SCALARS.put("divide(2.01D, 2D, 2)", "1.01");
        SCALARS.put("divide(-2.01D, 2D, 2)", "-1.01");
        SCALARS.put("divide(12345678901234567.89D, 1D, 2)", "12345678901234567.89");
        SCALARS.put("divide(1D, 3D, 6)", "0.333333");
        SCALARS.put("divide(10D, 4D, 0)", "3");
        // a Float NaN or infinity rounded with a scale passes through, as IEEE and the databases do (the user's ruling,
        // 2026-11-16, register S-batch-3; pure raises, and so do the compatibility rules: CompatibilityRaisesTest)
        SCALARS.put("round(parseFloat('NaN'), 2)", "NaN");
        SCALARS.put("round(parseFloat('Infinity'), 2)", "Infinity");
        SCALARS.put("round(parseFloat('-Infinity'), 2)", "-Infinity");
    }

    /** Over the table, one column per probe, the rows in ID order: legend-pure's answer for each row. */
    private static final Map<String, List<String>> COLUMNS = new LinkedHashMap<>();

    static {
        COLUMNS.put("$x.F->toOne()->round(2)", List.of("1.01", "0.13", "-0.13", "2.68", "1.0", "0.3"));
        COLUMNS.put("$x.F->toOne()->toDecimal()",
                List.of("1.005", "0.125", "-0.125", "2.675", "1.0049", "0.30000000000000004"));
        COLUMNS.put("divide($x.D->toOne(), 2D, 2)",
                List.of("1.01", "-1.01", "6172839450617283.95", "5.00", "0.50", "1.50"));
        COLUMNS.put("divide($x.D->toOne(), 3D, 2)",
                List.of("0.67", "-0.67", "4115226300411522.63", "3.33", "0.33", "1.00"));
    }

    @Test
    void eachDatabaseAnswersAsPureDoesOrItsClosestIsPinned() {
        Map<String, String> closest = closest();
        Map<String, String> measuredClosest = new TreeMap<>();
        List<String> wrong = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            String model = MODEL.replace("CONNECTION", backend.connectionBody());
            for (Map.Entry<String, String> probe : SCALARS.entrySet()) {
                check(backend, probe.getKey(), probe.getValue(), only(Lean.service(model, "|" + probe.getKey(),
                        "test::RT").rows()), closest, measuredClosest, wrong);
            }
            for (Map.Entry<String, List<String>> probe : COLUMNS.entrySet()) {
                List<List<Object>> rows = Lean.service(model, "|#>{store::DB.T}#->sort(~ID->ascending())->extend(~v:x|"
                        + probe.getKey() + ")->select(~v)", "test::RT").rows();
                assertEquals(probe.getValue().size(), rows.size(), backend.name() + ": " + probe.getKey());
                for (int i = 0; i < rows.size(); i++) {
                    check(backend, probe.getKey() + " #" + (i + 1), probe.getValue().get(i), rows.get(i).get(0),
                            closest, measuredClosest, wrong);
                }
            }
        }
        assertEquals(List.of(), wrong, "answers neither Pure's nor the pinned closest");
        assertEquals(closest, measuredClosest, "the closest answers pinned per database moved");
    }

    /** The one value a one-value query answers. */
    private static Object only(List<List<Object>> rows) {
        assertEquals(1, rows.size(), String.valueOf(rows));
        return rows.get(0).get(0);
    }

    /** {@code got} is Pure's {@code expected}, numerically, or the closest pinned for this database, which is recorded. */
    private static void check(TestBackend backend, String probe, String expected, Object got,
            Map<String, String> closest, Map<String, String> measuredClosest, List<String> wrong) {
        String key = backend.name() + "\t" + probe;
        if (same(got, expected)) {
            return;
        }
        String pinned = closest.get(key);
        if (pinned != null && same(got, pinned)) {
            measuredClosest.put(key, pinned);
            return;
        }
        wrong.add(key + ": " + got + ", where Pure answers " + expected);
    }

    private static boolean same(Object got, String expected) {
        if (expected.equals("NaN") || expected.endsWith("Infinity")) {
            return got instanceof Number n && Double.valueOf(expected).equals(n.doubleValue());
        }
        return got instanceof Number n && Double.isFinite(n.doubleValue())
                && new BigDecimal(n.toString()).compareTo(new BigDecimal(expected)) == 0;
    }

    /** A Decimal's scale written as a column's value is known only at run time: refused by name while planning (the
     *  user's ruling, 2026-11-16, register S-batch-2), as a format string known only at run time is (S343). */
    @Test
    void aDivideScaleKnownOnlyAtRunTimeIsRefusedByName() {
        String model = MODEL.replace("CONNECTION", TestBackends.inProcess().get(0).connectionBody());
        Exception refused = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> Lean.service(model,
                "|#>{store::DB.T}#->extend(~v:x|divide($x.D->toOne(), 2D, $x.ID->toOne()))->select(~v)", "test::RT"));
        org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(refused.getMessage()).contains(
                "divide(Decimal, Decimal, scale) needs a LITERAL scale"), refused.toString());
    }

    /** The closest answers pinned per database ({@code pure-decimal/closest.tsv}): database, probe, answer. */
    private static Map<String, String> closest() {
        Map<String, String> out = new TreeMap<>();
        try (java.io.InputStream in = PureDecimalArithmeticTest.class.getResourceAsStream("/pure-decimal/closest.tsv")) {
            String text = new String(java.util.Objects.requireNonNull(in, "the ledger is on the class path").readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] cells = line.split("\t");
                out.put(cells[0] + "\t" + cells[1], cells[2]);
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out;
    }
}
