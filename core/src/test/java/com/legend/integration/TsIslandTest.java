// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TIMESERIES LANGUAGE, v0 (docs/TS_LANGUAGE_BUILD_2026_10_30.md P2; the language docs/TIMESERIES_LANGUAGE_2026_10_29.md):
 * a {@code #TS{...}#} query over a class read as objects, a flat source named by WITH, a table, a filtered source and a
 * relation of the enclosing Pure, each answering what the same calls of the timeseries library written in Pure by hand
 * answer (ruling 2: a figure is the library's call) -- and the figures, worked here as plain arithmetic over the same
 * prices, never by the code under test -- on every in-process database; and every refusal v0 makes, each with its fix.
 *
 * <p>Its own target ({@code //core:ts_island_test}): it reads the timeseries project's file, passed as {@code
 * -Dtimeseries.library}.
 */
class TsIslandTest {

    private static final LocalDate FIRST = LocalDate.of(2024, 1, 20);

    /** AAA's closes, a price a day for 12 days; BBB's, another line of the same table. */
    private static final double[] AAA = prices(12, i -> 100 * (1 + 0.03 * Math.sin(i / 2.0) + 0.004 * i));
    private static final double[] BBB = prices(12, i -> 50 * (1 + 0.02 * Math.cos(i / 1.7) + 0.002 * i));

    private static double[] prices(int n, IntFunction<Double> f) {
        double[] out = new double[n];
        for (int i = 0; i < n; i++) {
            out[i] = Math.round(f.apply(i) * 100) / 100.0;
        }
        return out;
    }

    /** The prices, the instruments (AAA a TECH one, BBB an ENERGY one) and a quote's instrument by its symbol. */
    private static final String STORE = """
            ###Relational
            Database ts::DB
            (
              Table PRICES ( SYMBOL VARCHAR(8), DAY DATE, CLOSE DOUBLE )
              Table INSTRUMENTS ( TICKER VARCHAR(8) PRIMARY KEY, SECTOR VARCHAR(8) )
              Table TICKS ( SYMBOL VARCHAR(8), AT TIMESTAMP, PRICE DOUBLE )
              Join QuoteInstrument(PRICES.SYMBOL = INSTRUMENTS.TICKER)
            )
            """;

    /** The class model over them: a quote, its instrument (to one), the instrument's sector (an enumeration). */
    private static final String CLASSES = """
            ###Pure
            Enum ts::Sector { TECH, ENERGY }
            Class ts::Quote { day: StrictDate[1]; close: Float[1]; }
            Class ts::Instrument { ticker: String[1]; sector: ts::Sector[1]; }
            Association ts::QuoteInstrument { instrument: ts::Instrument[1]; quotes: ts::Quote[*]; }
            ###Mapping
            Mapping ts::M
            (
              ts::Quote: Relational
              {
                ~mainTable [ts::DB] PRICES
                day: [ts::DB] PRICES.DAY,
                close: [ts::DB] PRICES.CLOSE
              }
              ts::Instrument: Relational
              {
                ~primaryKey ([ts::DB] INSTRUMENTS.TICKER)
                ~mainTable [ts::DB] INSTRUMENTS
                ticker: [ts::DB] INSTRUMENTS.TICKER,
                sector: EnumerationMapping Sectors: [ts::DB] INSTRUMENTS.SECTOR
              }
              ts::Sector: EnumerationMapping Sectors { TECH: 'T', ENERGY: 'E' }
              ts::QuoteInstrument: Relational { AssociationMapping (
                instrument: [ts::DB]@QuoteInstrument,
                quotes: [ts::DB]@QuoteInstrument
              ) }
            )
            """;

    private static String library() throws java.io.IOException {
        return Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
    }

    /** The store, the classes, a runtime on {@code backend}'s session, and (with {@code library}) the library. */
    private static String model(TestBackend backend, boolean library) throws java.io.IOException {
        return STORE + CLASSES + """
                ###Connection
                RelationalDatabaseConnection ts::Conn
                {
                  store: ts::DB;
                  %s
                }
                ###Runtime
                Runtime ts::RT { mappings: [ ts::M ]; connections: [ ts::DB: [ c: ts::Conn ] ]; }
                """.formatted(backend.connectionBody()) + (library ? library() : "");
    }

    /** {@code c} given the tables, as {@code backend}'s dialect writes them, and their rows. */
    private static void seed(TestBackend backend, Connection c) throws java.sql.SQLException {
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(STORE).elements()
                .stream().filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        try (java.sql.Statement s = c.createStatement()) {
            for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
        }
        String prices = backend.dialect().storedName(tables.get(0).name(), tables.get(0).quoted());
        try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO " + prices + " VALUES (?, ?, ?)")) {
            for (Map.Entry<String, double[]> line : Map.of("AAA", AAA, "BBB", BBB).entrySet()) {
                for (int i = 0; i < line.getValue().length; i++) {
                    ps.setString(1, line.getKey());
                    ps.setObject(2, FIRST.plusDays(i));
                    ps.setDouble(3, line.getValue()[i]);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
        String instruments = backend.dialect().storedName(tables.get(1).name(), tables.get(1).quoted());
        try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO " + instruments + " VALUES (?, ?)")) {
            for (String[] row : new String[][] {{"AAA", "T"}, {"BBB", "E"}}) {
                ps.setString(1, row[0]);
                ps.setString(2, row[1]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        // AAA's ticks: two moments each day, 10:00 and 15:00, at its close less 1 and its close
        String ticks = backend.dialect().storedName(tables.get(2).name(), tables.get(2).quoted());
        try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO " + ticks + " VALUES (?, ?, ?)")) {
            for (int i = 0; i < AAA.length; i++) {
                for (int hour : new int[] {10, 15}) {
                    ps.setString(1, "AAA");
                    ps.setObject(2, FIRST.plusDays(i).atTime(hour, 0));
                    ps.setDouble(3, hour == 10 ? AAA[i] - 1 : AAA[i]);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
    }

    /** {@code query}'s rows on {@code backend}, each by its column's name: a value's text, or null where it has none. */
    private static List<Map<String, String>> rows(TestBackend backend, String query) throws Exception {
        return rows(backend, Compiler.compileModel(model(backend, true)), query);
    }

    /** {@code query}'s rows on {@code backend} over {@code model}, compiled once. */
    private static List<Map<String, String>> rows(TestBackend backend, com.legend.compiler.element.ModelContext model,
            String query) throws Exception {
        ExecutionPlan plan = Compiler.query(model, query).executionPlan("ts::RT");
        String text;
        try (Connection c = backend.open()) {
            seed(backend, c);
            text = PlanCases.write(plan, c, Map.of(), PlanCases.Written.CSV::writer);
        }
        List<String> lines = new ArrayList<>(Arrays.asList(text.split("\r\n")));
        List<String> header = Arrays.asList(lines.remove(0).split(",", -1));
        List<Map<String, String>> out = new ArrayList<>();
        for (String line : lines) {
            String[] cells = line.split(",", -1);
            assertEquals(header.size(), cells.length, backend + ": " + line);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < cells.length; i++) {
                row.put(header.get(i), cells[i].isEmpty() ? null : cells[i]);
            }
            out.add(row);
        }
        return out;
    }

    /** {@code ts}'s rows and {@code pure}'s, the same on every in-process database -- {@code pure}'s where an item
     *  has a value (R6), its first {@code keys} columns the date and the instrument; {@code ts}'s rows. */
    private static Map<TestBackend, List<Map<String, String>>> same(String ts, String pure, int keys)
            throws Exception {
        Map<TestBackend, List<Map<String, String>>> out = new LinkedHashMap<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{" + ts + "}#");
            List<Map<String, String>> want = rows(backend, pure).stream().filter(r -> r.values().stream().skip(keys)
                    .anyMatch(java.util.Objects::nonNull)).toList();
            assertEquals(want, got, backend + ": the #TS query answers as the library's calls do");
            out.put(backend, got);
        }
        assertTrue(out.size() >= 3, "on every in-process database: " + out.keySet());
        return out;
    }

    /** The column {@code name} of the rows of {@code key}'s {@code value} (every row: null), in order. */
    private static Double[] column(List<Map<String, String>> rows, String key, String value, String name) {
        return rows.stream().filter(r -> key == null || value.equals(r.get(key)))
                .map(r -> r.get(name) == null ? null : Double.valueOf(r.get(name))).toArray(Double[]::new);
    }

    private static void assertClose(Double[] want, Double[] got, String what) {
        assertEquals(want.length, got.length, what + ": the rows");
        for (int i = 0; i < want.length; i++) {
            if (want[i] == null) {
                assertNull(got[i], what + " at " + i);
            } else {
                assertNotNull(got[i], what + " at " + i);
                assertEquals(want[i], got[i], 1e-9 * Math.max(1, Math.abs(want[i])), what + " at " + i);
            }
        }
    }

    private static Double[] each(int n, IntFunction<Double> f) {
        Double[] out = new Double[n];
        for (int i = 0; i < n; i++) {
            out[i] = f.apply(i);
        }
        return out;
    }

    /** The database a query of many lines is held to here: the platform's (one in-process planner re-renders every
     *  nested derived table at each level, and runs out of memory on a FILL's or a SAMPLE BY's, 2026-10-30). */
    private static List<TestBackend> deep() {
        return List.of(TestBackends.platform());
    }

    /** The mean of three observations as gs-quant's mean(x, 3): its first three points dropped (helper.apply_ramp,
     *  Window(3, 3)), a value from the fourth. */
    private static Double mean3(double[] x, int i) {
        return i < 3 ? null : (x[i] + x[i - 1] + x[i - 2]) / 3;
    }

    /** gs-quant's ramp in a chain of the library's table functions (helper.apply_ramp: a window of n observations drops
     *  its line's first n points): {@code c} none in each line's first n rows. */
    private static String ramp(String line, String date, String c, int n) {
        return "->extend(over(~[" + line + "], [~" + date + "->ascending()]), ~__rn:{p,f,r|$p->rowNumber($r)})"
                + "->extend(~__g:r|if($r.__rn > " + n + ", |$r." + c + ", |[]))->without(~[" + c + ", __rn])"
                + "->rename(~__g, ~" + c + ")";
    }

    private static String refused(String ts, boolean library) throws Exception {
        String model = model(TestBackends.platform(), library);
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> Compiler.query(Compiler.compileModel(model), "|#TS{" + ts + "}#").executionPlan("ts::RT"),
                "refused: " + ts);
        return e.getMessage();
    }

    private static final String TS_ONE = """
            SELECT close AS px, avg(close, 3) AS ma3, returns(close) AS r, sum(close, all) AS running
            FROM   Quote ON day BY instrument.ticker""";

    private static final String PURE_ONE = """
            |ts::Quote.all()->project(~[day: x|$x.day, ticker: x|$x.instrument.ticker, close: x|$x.close])
              ->extend(~px: r|$r.close)
              ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-2, 0)), ~close, 3, ~ma3)"""
            + ramp("ticker", "day", "ma3", 3) + """
              ->timeseries::returns(over(~[ticker], [~day->ascending()]), ~close, 1, timeseries::Returns.simple, ~r)
              ->timeseries::sum(over(~[ticker], [~day->ascending()], rows(unbounded(), 0)), ~close, 1, ~running)
              ->select(~[day, ticker, px, ma3, r, running])
              ->sort([~day->ascending(), ~ticker->ascending()])""";

    @Test
    void aClassReadAsObjectsIsTheLibrarysCallsOverItsPathsProjected() throws Exception {
        // R1.1: the paths it reads projected, a path named by its last step; each figure the library's call (R4)
        var answers = same(TS_ONE, PURE_ONE, 2);
        for (Map.Entry<TestBackend, List<Map<String, String>>> a : answers.entrySet()) {
            for (Map.Entry<String, double[]> line : Map.of("AAA", AAA, "BBB", BBB).entrySet()) {
                double[] x = line.getValue();
                String of = a.getKey() + " " + line.getKey() + " ";
                List<Map<String, String>> rows = a.getValue();
                assertClose(each(x.length, i -> x[i]), column(rows, "ticker", line.getKey(), "px"), of + "the close");
                assertClose(each(x.length, i -> mean3(x, i)), column(rows, "ticker", line.getKey(), "ma3"),
                        of + "the mean of three, once the window is full");
                assertClose(each(x.length, i -> i < 1 ? null : x[i] / x[i - 1] - 1),
                        column(rows, "ticker", line.getKey(), "r"), of + "simple returns");
                assertClose(each(x.length, i -> {
                    double s = 0;
                    for (int k = 0; k <= i; k++) {
                        s += x[k];
                    }
                    return s;
                }), column(rows, "ticker", line.getKey(), "running"), of + "the sum to date");
            }
        }
    }

    @Test
    void aWithSourceIsAClasssPathsNamedOnce() throws Exception {
        // R1.2: a flat subquery named by WITH, its paths short; a later item reads an earlier one (R7)
        same("""
                WITH q AS (SELECT instrument.ticker AS sym, day, close AS px FROM Quote)
                SELECT avg(px, 3) AS ma3, px / ma3 - 1 AS dev
                FROM   q ON day BY sym""", """
                |ts::Quote.all()->project(~[sym: x|$x.instrument.ticker, day: x|$x.day, px: x|$x.close])
                  ->timeseries::mean(over(~[sym], [~day->ascending()], rows(-2, 0)), ~px, 3, ~ma3)"""
                + ramp("sym", "day", "ma3", 3) + """
                  ->extend(~dev: r|if($r.px->isEmpty() || $r.ma3->isEmpty(), |[], |$r.px->toOne() / $r.ma3->toOne() - 1))
                  ->select(~[day, sym, ma3, dev])
                  ->sort([~day->ascending(), ~sym->ascending()])""", 2);
    }

    @Test
    void aFlatItemMayTakeTheNameOfThePathItReads() throws Exception {
        // as SQL means it: an item's name is its output's, its expression reads the source's path (Show #TS A6 prints
        // an enumeration as CAST(side AS text) AS side, 2026-11-03)
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> same = rows(backend, "|#TS{WITH q AS (SELECT instrument.ticker AS sym, day,"
                    + " close * 2 AS close FROM Quote) SELECT close AS c FROM q ON day BY sym}#");
            List<Map<String, String>> want = rows(backend, "|#TS{WITH q AS (SELECT instrument.ticker AS sym, day,"
                    + " close AS px FROM Quote) SELECT px * 2 AS c FROM q ON day BY sym}#");
            assertFalse(want.isEmpty(), backend + ": the rows to compare");
            assertEquals(want, same, backend + ": close * 2 AS close is the path close doubled");
        }
    }

    @Test
    void aTableAFilteredSourceAndAVariableAreRelations() throws Exception {
        // R1.3 and R1.4: a table by its simple name, a source's WHERE, a relation of the enclosing Pure
        String pure = """
                ->timeseries::mean(over(~[SYMBOL], [~DAY->ascending()], rows(-2, 0)), ~CLOSE, 3, ~ma3)"""
                + ramp("SYMBOL", "DAY", "ma3", 3) + """
                  ->select(~[DAY, SYMBOL, ma3])
                  ->sort([~DAY->ascending(), ~SYMBOL->ascending()])""";
        same("SELECT avg(CLOSE, 3) AS ma3 FROM PRICES ON DAY BY SYMBOL", "|#>{ts::DB.PRICES}#" + pure, 2);
        same("SELECT avg(CLOSE, 3) AS ma3 FROM (PRICES WHERE SYMBOL = 'AAA') ON DAY BY SYMBOL",
                "|#>{ts::DB.PRICES}#->filter(r|$r.SYMBOL == 'AAA')" + pure, 2);
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, """
                    |let p = #>{ts::DB.PRICES}#->filter(r|$r.SYMBOL == 'BBB');
                    #TS{SELECT avg(CLOSE, 3) AS ma3 FROM $p ON DAY BY SYMBOL}#;""");
            assertClose(each(BBB.length - 3, i -> mean3(BBB, i + 3)), column(got, null, null, "ma3"),
                    backend + ": a variable's relation, a row where the mean has a value (R6)");
        }
    }

    @Test
    void oneLineWithoutByAndTheAnswerFiltered() throws Exception {
        // a source of one instrument has no BY: its line is the whole source; FILTER sees the answer's columns (R8)
        var answers = same("""
                SELECT avg(close, 3) AS ma3, diff(close, 2) AS d2
                FROM   (Quote WHERE instrument.ticker = 'AAA') ON day
                FILTER ma3 IS NOT NULL AND day < '2024-01-29'""", """
                |ts::Quote.all()->project(~[day: x|$x.day, close: x|$x.close, ticker: x|$x.instrument.ticker])
                  ->filter(r|$r.ticker == 'AAA')
                  ->extend(~__ts_line: r|'')
                  ->timeseries::mean(over(~[__ts_line], [~day->ascending()], rows(-2, 0)), ~close, 3, ~ma3)"""
                + ramp("__ts_line", "day", "ma3", 3) + """
                  ->timeseries::diff(over(~[__ts_line], [~day->ascending()]), ~close, 2, ~d2)
                  ->select(~[day, ma3, d2])
                  ->filter(r|$r.ma3->isNotEmpty() && ($r.day->toOne() < %2024-01-29))
                  ->sort([~day->ascending()])""", 1);
        for (Map.Entry<TestBackend, List<Map<String, String>>> a : answers.entrySet()) {
            // the 23rd's mean is the first (gs-quant's ramp: the fourth observation); the 28th the last before the 29th
            assertClose(each(6, i -> mean3(AAA, i + 3)), column(a.getValue(), null, null, "ma3"),
                    a.getKey() + ": the mean of three, from the fourth day to the 28th");
            assertClose(each(6, i -> AAA[i + 3] - AAA[i + 1]), column(a.getValue(), null, null, "d2"),
                    a.getKey() + ": the change over two observations");
        }
    }

    @Test
    void aFigureOfAFigureAndAPerPointExpressionOfFigures() throws Exception {
        // functions nest (R3): the inner figure first, its column the outer's line; per-point SQL over their columns
        var answers = same("""
                SELECT avg(returns(close), 2) AS mr, close / avg(close, 3) - 1 AS dev
                FROM   Quote ON day BY instrument.ticker
                FILTER ticker = 'BBB'""", """
                |ts::Quote.all()->project(~[day: x|$x.day, ticker: x|$x.instrument.ticker, close: x|$x.close])
                  ->timeseries::returns(over(~[ticker], [~day->ascending()]), ~close, 1, timeseries::Returns.simple, ~__ts_1)
                  ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-1, 0)), ~__ts_1, 2, ~mr)"""
                + ramp("ticker", "day", "mr", 2) + """
                  ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-2, 0)), ~close, 3, ~__ts_2)"""
                + ramp("ticker", "day", "__ts_2", 3) + """
                  ->extend(~dev: r|if($r.close->isEmpty() || $r.__ts_2->isEmpty(), |[], |$r.close->toOne() / $r.__ts_2->toOne() - 1))
                  ->select(~[day, ticker, mr, dev])
                  ->filter(r|$r.mr->isNotEmpty() || $r.dev->isNotEmpty())
                  ->filter(r|$r.ticker == 'BBB')
                  ->sort([~day->ascending(), ~ticker->ascending()])""", 2);
        for (Map.Entry<TestBackend, List<Map<String, String>>> a : answers.entrySet()) {
            double[] x = BBB;
            // R6: no row where no item has a value (the first two dates)
            assertClose(each(x.length - 2, i -> (x[i + 2] / x[i + 1] - 1 + x[i + 1] / x[i] - 1) / 2),
                    column(a.getValue(), null, null, "mr"), a.getKey() + ": the mean of two returns");
            assertClose(each(x.length - 2, i -> i + 2 < 3 ? null : x[i + 2] / mean3(x, i + 2) - 1),
                    column(a.getValue(), null, null, "dev"), a.getKey() + ": the close against its mean");
        }
    }

    @Test
    void aSubscriptPicksALineAndLinesMeetOnTheirDates() throws Exception {
        // R2: one instrument's line; a single line beside each instrument's (its broadcast); two lines' difference
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> one = rows(backend, "|#TS{SELECT avg(CLOSE['AAA'], 3) AS ma"
                    + " FROM PRICES ON DAY BY SYMBOL}#");
            assertEquals(List.of("DAY", "ma"), List.copyOf(one.get(0).keySet()), backend + ": one line, no instrument");
            assertClose(each(AAA.length - 3, i -> mean3(AAA, i + 3)), column(one, null, null, "ma"),
                    backend + ": AAA's mean, a row where it has a value (R6)");
            List<Map<String, String>> rel = rows(backend, "|#TS{SELECT CLOSE / CLOSE['AAA'] AS rel"
                    + " FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(BBB.length, i -> BBB[i] / AAA[i]), column(rel, "SYMBOL", "BBB", "rel"),
                    backend + ": each instrument against AAA's line");
            assertClose(each(AAA.length, i -> 1.0), column(rel, "SYMBOL", "AAA", "rel"), backend + ": AAA against itself");
            List<Map<String, String>> spread = rows(backend, "|#TS{SELECT CLOSE['AAA'] - CLOSE['BBB'] AS spread,"
                    + " avg(spread, 3) AS ma FROM PRICES ON DAY BY SYMBOL}#");
            double[] d = new double[AAA.length];
            for (int i = 0; i < d.length; i++) {
                d[i] = AAA[i] - BBB[i];
            }
            assertClose(each(d.length, i -> d[i]), column(spread, null, null, "spread"), backend + ": two lines' spread");
            assertClose(each(d.length, i -> mean3(d, i)), column(spread, null, null, "ma"),
                    backend + ": a later item's mean of an earlier one, beside it");
        }
    }

    @Test
    void aFigureRunsOverItsLinesOwnRowsAndLinesMeetWhereBothHaveOne() throws Exception {
        // R4 and R6: BBB has no row on the 25th; its mean of three skips it, and the spread has no row there
        String source = "(PRICES WHERE SYMBOL = 'AAA' OR DAY <> '2024-01-25')";
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT avg(CLOSE['BBB'], 3) AS mb,"
                    + " CLOSE['AAA'] - CLOSE['BBB'] AS spread FROM " + source + " ON DAY BY SYMBOL}#");
            int gap = 5;
            double[] b = new double[BBB.length - 1];
            double[] a = new double[BBB.length - 1];
            for (int i = 0, k = 0; i < BBB.length; i++) {
                if (i != gap) {
                    b[k] = BBB[i];
                    a[k++] = AAA[i];
                }
            }
            assertEquals(b.length, got.size(), backend + ": a row where an item has a value, none on the 25th");
            assertClose(each(b.length, i -> mean3(b, i)), column(got, null, null, "mb"),
                    backend + ": BBB's mean over its own last three rows");
            assertClose(each(b.length, i -> a[i] - b[i]), column(got, null, null, "spread"),
                    backend + ": the spread on the dates both have");
        }
    }

    @Test
    void severalSourcesEachWithItsAlias() throws Exception {
        // R1: a class and a table, each its own source, their lines meeting on their dates
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT q.close['AAA'] AS a, p.CLOSE['BBB'] AS b,"
                    + " a / b AS ratio FROM Quote q ON day BY instrument.ticker, PRICES p ON DAY BY SYMBOL}#");
            assertEquals(List.of("day", "a", "b", "ratio"), List.copyOf(got.get(0).keySet()),
                    backend + ": the first source's date names the answer's");
            assertClose(each(AAA.length, i -> AAA[i]), column(got, null, null, "a"), backend + ": the class's line");
            assertClose(each(BBB.length, i -> BBB[i]), column(got, null, null, "b"), backend + ": the table's line");
            assertClose(each(AAA.length, i -> AAA[i] / BBB[i]), column(got, null, null, "ratio"),
                    backend + ": the two sources' lines met");
        }
    }

    /** The periods of AAA's and BBB's days, Monday's week (R5): each week's days by their index. */
    private static List<List<Integer>> weeks() {
        Map<LocalDate, List<Integer>> out = new java.util.TreeMap<>();
        for (int i = 0; i < AAA.length; i++) {
            LocalDate d = FIRST.plusDays(i);
            out.computeIfAbsent(d.with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY)),
                    k -> new ArrayList<>()).add(i);
        }
        return List.copyOf(out.values());
    }

    @Test
    void aPeriodIsDatedAtItsLastObservationsDayOrItsFirstOrItsLast() throws Exception {
        // R5 (the user, 2026-10-30: "configurable with sensible default"): AT LAST, the default, the day of the period's
        // last observation; AT START its first day; AT END its last day -- 2024-01-20 a Saturday, the 31st a Wednesday
        Map<String, List<String>> dated = Map.of(
                "", List.of("2024-01-21", "2024-01-28", "2024-01-31"),
                " AT LAST", List.of("2024-01-21", "2024-01-28", "2024-01-31"),
                " AT START", List.of("2024-01-15", "2024-01-22", "2024-01-29"),
                " AT END", List.of("2024-01-21", "2024-01-28", "2024-02-04"));
        for (TestBackend backend : deep()) {
            for (Map.Entry<String, List<String>> d : dated.entrySet()) {
                List<Map<String, String>> got = rows(backend, "|#TS{SELECT last(CLOSE['AAA']) AS c FROM PRICES ON DAY BY"
                        + " SYMBOL SAMPLE BY 1w" + d.getKey() + "}#");
                assertEquals(d.getValue(), got.stream().map(r -> r.get("DAY")).toList(), backend + ":" + d.getKey());
                assertClose(weeks().stream().map(w -> AAA[w.get(w.size() - 1)]).toArray(Double[]::new),
                        column(got, null, null, "c"), backend + ": each week's last close" + d.getKey());
            }
            List<Map<String, String>> month = rows(backend, "|#TS{SELECT last(CLOSE['AAA']) AS c FROM PRICES ON DAY BY"
                    + " SYMBOL SAMPLE BY 1mo AT END}#");
            assertEquals(List.of("2024-01-31"), month.stream().map(r -> r.get("DAY")).toList(), backend + ": a month's end");
        }
        assertTrue(refused("SELECT last(CLOSE['AAA']) AS c FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1w AT MIDDLE", true)
                .contains("dated AT LAST (the last observation's day), AT START or AT END"), "a date it has not");
    }

    @Test
    void aQueryOfTheLanguageIsASourceItsAnswerReadOn() throws Exception {
        // a line computed day by day, then each week's candle of it (the tile's candles of a computed series, its N4)
        String ts = "|#TS{WITH d AS TS (SELECT CLOSE['AAA'] / CLOSE['BBB'] AS r FROM PRICES ON DAY BY SYMBOL)"
                + " SELECT ohlc(first(d.r), max(d.r), min(d.r), last(d.r)) AS c FROM d ON DAY SAMPLE BY 1w}#";
        for (TestBackend backend : deep()) {
            List<Map<String, String>> got = rows(backend, ts);
            List<List<Integer>> weeks = weeks();
            java.util.function.Function<List<Integer>, double[]> ratios = w -> w.stream()
                    .mapToDouble(i -> AAA[i] / BBB[i]).toArray();
            assertClose(weeks.stream().map(w -> ratios.apply(w)[0]).toArray(Double[]::new), column(got, null, null,
                    "c_open"), backend + ": each week's first ratio");
            assertClose(weeks.stream().map(w -> java.util.Arrays.stream(ratios.apply(w)).max().orElseThrow())
                    .toArray(Double[]::new), column(got, null, null, "c_high"), backend + ": its highest");
            assertClose(weeks.stream().map(w -> java.util.Arrays.stream(ratios.apply(w)).min().orElseThrow())
                    .toArray(Double[]::new), column(got, null, null, "c_low"), backend + ": its lowest");
            assertClose(weeks.stream().map(w -> ratios.apply(w)[w.size() - 1]).toArray(Double[]::new),
                    column(got, null, null, "c_close"), backend + ": its last");
            // the same query in FROM, marked alike
            assertEquals(got, rows(backend, "|#TS{SELECT ohlc(first(d.r), max(d.r), min(d.r), last(d.r)) AS c FROM TS"
                    + " (SELECT CLOSE['AAA'] / CLOSE['BBB'] AS r FROM PRICES ON DAY BY SYMBOL) d ON DAY SAMPLE BY 1w}#"),
                    backend + ": FROM TS (...)");
        }
        // a query of the language's own is marked AS TS; a body that is not one is SQL's -- each said with its fix
        assertTrue(refused("WITH d AS (SELECT CLOSE['AAA'] AS r FROM PRICES ON DAY BY SYMBOL) SELECT last(d.r) AS c"
                + " FROM d ON DAY SAMPLE BY 1w", true).contains("write WITH daily AS TS (SELECT ...)"), "unmarked");
        assertTrue(refused("WITH d AS TS (PRICES WHERE SYMBOL = 'AAA') SELECT CLOSE AS c FROM d ON DAY", true)
                .contains("a query of the language's own is SELECT ... FROM source ON date"), "TS of no query");
        // a SQL body's join is a join's ON, never the language's
        String join = refused("WITH q AS (SELECT * FROM PRICES JOIN INSTRUMENTS ON SYMBOL = TICKER) SELECT CLOSE AS c"
                + " FROM q ON DAY", true);
        assertTrue(!join.contains("write WITH daily AS TS") && join.contains("JOIN"), join);
    }

    @Test
    void sampleByAWeekTakesEachWeeksRowsByItsAggregate() throws Exception {
        // R5: a row a week, each line taken by its aggregate, a window then counting weeks
        List<List<Integer>> weeks = weeks();
        for (TestBackend backend : deep()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT sum(CLOSE['AAA']) AS wk_sum,"
                    + " max(CLOSE['BBB']) AS wk_max, avg(CLOSE['AAA']) AS wk_avg, sum(wk_sum, 2) AS two"
                    + " FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1w}#");
            assertEquals(weeks.size(), got.size(), backend + ": a row a week");
            Double[] sums = weeks.stream().map(w -> w.stream().mapToDouble(i -> AAA[i]).sum()).toArray(Double[]::new);
            assertClose(sums, column(got, null, null, "wk_sum"), backend + ": each week's total");
            assertClose(weeks.stream().map(w -> w.stream().mapToDouble(i -> BBB[i]).max().orElseThrow())
                    .toArray(Double[]::new), column(got, null, null, "wk_max"), backend + ": each week's highest");
            assertClose(weeks.stream().map(w -> w.stream().mapToDouble(i -> AAA[i]).average().orElseThrow())
                    .toArray(Double[]::new), column(got, null, null, "wk_avg"), backend + ": each week's average");
            assertClose(each(sums.length, i -> i < 2 ? null : sums[i] + sums[i - 1]), column(got, null, null, "two"),
                    backend + ": a window of two weeks, from the third (gs-quant's ramp)");
            List<String> dates = got.stream().map(r -> r.get("DAY")).toList();
            assertEquals(weeks.stream().map(w -> FIRST.plusDays(w.get(w.size() - 1)).toString()).toList(), dates,
                    backend + ": each week dated at its last day with data");
        }
    }

    @Test
    void aPeriodsLastIsItsLastRowsAndItsDateTheLatestOfItsLines() throws Exception {
        // R5: last(x) the period's last row's value; a period one date for every line, its latest; ARG_MAX where the
        // database has it (DuckDB's)
        List<List<Integer>> weeks = weeks();
        String source = "(PRICES WHERE SYMBOL = 'AAA' OR DAY <> '2024-01-28')";
        List<Map<String, String>> got = rows(TestBackends.platform(), "|#TS{SELECT last(CLOSE['AAA']) AS a,"
                + " last(CLOSE['BBB']) AS b, first(CLOSE['AAA']) AS f FROM " + source + " ON DAY BY SYMBOL SAMPLE BY 1w}#");
        assertClose(weeks.stream().map(w -> AAA[w.get(w.size() - 1)]).toArray(Double[]::new),
                column(got, null, null, "a"), "each week's last");
        assertClose(weeks.stream().map(w -> AAA[w.get(0)]).toArray(Double[]::new), column(got, null, null, "f"),
                "each week's first");
        // BBB has no row on Sunday the 28th: its week's last is the 27th's, the week dated the 28th (AAA's)
        int sunday = 8;
        assertClose(weeks.stream().map(w -> BBB[w.get(w.size() - 1) == sunday ? sunday - 1 : w.get(w.size() - 1)])
                .toArray(Double[]::new), column(got, null, null, "b"), "BBB's last in each week");
        assertEquals(weeks.stream().map(w -> FIRST.plusDays(w.get(w.size() - 1)).toString()).toList(),
                got.stream().map(r -> r.get("DAY")).toList(), "one date a week for every line, its latest");
    }

    @Test
    void fillPutsEachLineOntoTheDatesAnyHas() throws Exception {
        // R6: BBB has no row on the 25th; without FILL the spread has none there, with FILL each way gives BBB one
        String source = "(PRICES WHERE SYMBOL = 'AAA' OR DAY <> '2024-01-25')";
        int gap = 5;
        Map<String, java.util.function.DoubleSupplier> b25 = new LinkedHashMap<>();
        b25.put("PREV", () -> BBB[gap - 1]);
        b25.put("LINEAR", () -> (BBB[gap - 1] + BBB[gap + 1]) / 2);
        b25.put("0", () -> 0);
        for (TestBackend backend : deep()) {
            for (Map.Entry<String, java.util.function.DoubleSupplier> fill : b25.entrySet()) {
                List<Map<String, String>> got = rows(backend, "|#TS{SELECT CLOSE['AAA'] - CLOSE['BBB'] AS spread"
                        + " FROM " + source + " ON DAY BY SYMBOL FILL(" + fill.getKey() + ")}#");
                assertClose(each(AAA.length, i -> AAA[i] - (i == gap ? fill.getValue().getAsDouble() : BBB[i])),
                        column(got, null, null, "spread"), backend + " FILL(" + fill.getKey() + ")");
            }
            List<Map<String, String>> none = rows(backend, "|#TS{SELECT CLOSE['AAA'] - CLOSE['BBB'] AS spread"
                    + " FROM " + source + " ON DAY BY SYMBOL FILL(NULL)}#");
            assertEquals(AAA.length - 1, none.size(), backend + " FILL(NULL): the 25th's spread none, so no row (R6)");
        }
    }

    @Test
    void aLineOfNoInstrumentIsEveryInstrumentsWhateverTheKeysType() throws Exception {
        // R2's broadcast over lines keyed by an enumeration (the instrument's sector: AAA TECH, BBB ENERGY), the lines
        // meeting without FILL and spread under it: the key of a line of no instrument is no sector, the line every
        // sector's (2026-11-01, docs/TS_V1_PROGRAM_2026_11_01.md A1: pinned before the meeting's variants become one)
        for (TestBackend backend : deep()) {
            for (String fill : List.of("", " FILL(PREV)")) {
                List<Map<String, String>> got = rows(backend, "|#TS{SELECT close - close['TECH'] AS rel"
                        + " FROM Quote ON day BY instrument.sector" + fill + "}#");
                String of = backend + (fill.isEmpty() ? " met" : fill) + ": ";
                assertClose(each(AAA.length, i -> 0.0), column(got, "sector", "TECH", "rel"),
                        of + "TECH's line less the TECH line");
                assertClose(each(BBB.length, i -> BBB[i] - AAA[i]), column(got, "sector", "ENERGY", "rel"),
                        of + "ENERGY's line less the TECH line, on every date both have");
            }
        }
    }

    @Test
    void aSpansWindowIsTheDatesAfterTheOneThatFarBack() throws Exception {
        // R4: 3d is the dates after the one three days back, from the line's first date three days on; 3 is three
        // observations. AAA has no row on the 25th: the 26th's three days hold two of its values, its three
        // observations the 23rd's, the 24th's and the 26th's
        int gap = 5;
        List<Integer> days = new ArrayList<>();
        for (int i = 0; i < AAA.length; i++) {
            if (i != gap) {
                days.add(i);
            }
        }
        List<Map<String, String>> got = rows(TestBackends.platform(), "|#TS{SELECT avg(CLOSE['AAA'], 3d) AS cal,"
                + " avg(CLOSE['AAA'], 3) AS obs FROM (PRICES WHERE SYMBOL = 'AAA' AND DAY <> '2024-01-25') ON DAY"
                + " BY SYMBOL}#");
        // R6: no row on the 20th, the 21st and the 22nd, where neither has a value (a span's from three days on,
        // three observations' from the fourth, gs-quant's ramp)
        assertClose(days.stream().skip(3).map(i -> {
            if (i < 3) {
                return null;
            }
            double sum = 0;
            int n = 0;
            for (int k : days) {
                if (k > i - 3 && k <= i) {
                    sum += AAA[k];
                    n++;
                }
            }
            return sum / n;
        }).toArray(Double[]::new), column(got, null, null, "cal"), "a span's mean, from three days on");
        assertClose(java.util.stream.IntStream.range(3, days.size()).mapToObj(j ->
                (AAA[days.get(j)] + AAA[days.get(j - 1)] + AAA[days.get(j - 2)]) / 3).toArray(Double[]::new),
                column(got, null, null, "obs"), "three observations' mean");
    }

    @Test
    void aSpanBackIsAsOfThatDateAndADateWindowRunsFromIt() throws Exception {
        // R9: returns(x, 1w) against the value as of a week back; R4: sum(x, '2024-01-25') the total since the 25th
        List<Map<String, String>> got = rows(TestBackends.platform(), "|#TS{SELECT returns(CLOSE['AAA'], 1w) AS wk,"
                + " sum(CLOSE['AAA'], '2024-01-25') AS since, lag(CLOSE['AAA'], 2d) AS two FROM PRICES ON DAY"
                + " BY SYMBOL}#");
        // R6: no row on the 20th and the 21st, where no item has a value
        assertClose(each(AAA.length - 2, i -> i + 2 < 7 ? null : AAA[i + 2] / AAA[i - 5] - 1),
                column(got, null, null, "wk"), "a week's returns");
        assertClose(each(AAA.length - 2, j -> {
            int i = j + 2;
            if (i < 5) {
                return null;
            }
            double sum = 0;
            for (int k = 5; k <= i; k++) {
                sum += AAA[k];
            }
            return sum;
        }), column(got, null, null, "since"), "the total since the 25th");
        assertClose(each(AAA.length - 2, i -> AAA[i]), column(got, null, null, "two"), "the value two days back");
    }

    @Test
    void anOptionByPlaceOrByName() throws Exception {
        // R10: returns' type, by place or by name
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT returns(CLOSE['AAA'], 1, 'log') AS lp,"
                    + " returns(CLOSE['AAA'], type => 'log') AS ln, returns(CLOSE['AAA'], type => 'absolute') AS ab"
                    + " FROM PRICES ON DAY BY SYMBOL}#");
            Double[] log = each(AAA.length - 1, i -> Math.log(AAA[i + 1]) - Math.log(AAA[i]));
            assertClose(log, column(got, null, null, "lp"), backend + ": log returns by place");
            assertClose(log, column(got, null, null, "ln"), backend + ": log returns by name");
            assertClose(each(AAA.length - 1, i -> AAA[i + 1] - AAA[i]), column(got, null, null, "ab"),
                    backend + ": absolute returns");
        }
    }

    @Test
    void latestIsTheDataLastDate() throws Exception {
        // R9: FILTER DAY > latest - 3d keeps the last three days, computed with every day before
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT avg(CLOSE['AAA'], 3) AS ma"
                    + " FROM PRICES ON DAY BY SYMBOL FILTER DAY > latest - 3d}#");
            assertEquals(List.of("2024-01-29", "2024-01-30", "2024-01-31"), got.stream().map(r -> r.get("DAY")).toList(),
                    backend + ": the last three days");
            assertClose(each(3, i -> mean3(AAA, i + 9)), column(got, null, null, "ma"),
                    backend + ": each with its history");
        }
    }

    @Test
    void latestIsTheSourcesLastDateWhicheverItemsAreRead() throws Exception {
        // R9, as the spec says (the user, 2026-11-02): the last date of the source after its WHERE, one date for the
        // query -- here BBB's 31st, though the only item reads AAA, whose last row is the 30th; the answer's own last
        // date (the 30th) had been read before
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT avg(CLOSE['AAA'], 3) AS ma"
                    + " FROM (PRICES WHERE SYMBOL = 'BBB' OR DAY < '2024-01-31') ON DAY BY SYMBOL"
                    + " FILTER DAY > latest - 3d}#");
            assertEquals(List.of("2024-01-29", "2024-01-30"), got.stream().map(r -> r.get("DAY")).toList(),
                    backend + ": the dates after the 28th, the source's last date the 31st");
        }
    }

    @Test
    void theItemsOfOneColumnAreOneTableEachRowOnce() throws Exception {
        // R7 (2026-11-03): PRICES read with no BY has two rows on each of its dates, AAA's and BBB's -- each row a point
        // (the user, 2026-10-30). The items over its column are columns of one table, each row once: never the
        // column's table joined to itself on its dates (four rows a date, before)
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT CLOSE AS c, CLOSE * 2 AS d, c - 1 AS e"
                    + " FROM PRICES ON DAY}#");
            assertEquals(2 * AAA.length, got.size(), backend + ": a row a point");
            for (Map<String, String> r : got) {
                double c = Double.parseDouble(r.get("c"));
                assertEquals(2 * c, Double.parseDouble(r.get("d")), 1e-9, backend + ": d is its own row's c, twice");
                assertEquals(c - 1, Double.parseDouble(r.get("e")), 1e-9, backend + ": e is its own row's c, less 1");
            }
            // a word that keeps its line's rows is a column of the same table (the time model's D5): each row once. Of
            // a line of several rows a date, a word over a calendar span (a window of every row of its dates, in any
            // order: the same on a date's two rows); a word of n observations has no one answer there (D8, the user,
            // 2026-11-06: wordsOfNObservationsOfALineOfSeveralRowsOnADateAreRefusedNamingTheFix)
            List<Map<String, String>> word = rows(backend, "|#TS{SELECT CLOSE AS c, avg(CLOSE, 2d) AS m"
                    + " FROM PRICES ON DAY}#");
            assertEquals(2 * AAA.length, word.size(), backend + ": a row a point, the word's answer a column");
            Map<String, java.util.Set<String>> byDay = new java.util.TreeMap<>();
            word.forEach(r -> byDay.computeIfAbsent(r.get("DAY"), d -> new java.util.TreeSet<>())
                    .add(String.valueOf(r.get("m"))));
            assertTrue(byDay.values().stream().allMatch(s -> s.size() == 1), backend + ": a date's two rows one mean");
            assertTrue(word.stream().anyMatch(r -> r.get("m") != null), backend + ": a mean past the ramp");
            // each simple statistic's rows form likewise (sum, min, max, std, var, range)
            List<Map<String, String>> stats = rows(backend, "|#TS{SELECT CLOSE AS c, sum(CLOSE, 2d) AS s,"
                    + " min(CLOSE, 2d) AS lo, max(CLOSE, 2d) AS hi, std(CLOSE, 3d) AS sd, var(CLOSE, 3d) AS va,"
                    + " range(CLOSE, 2d) AS rg FROM PRICES ON DAY}#");
            assertEquals(2 * AAA.length, stats.size(), backend + ": the statistics columns of the same table");
            for (Map<String, String> r : stats) {
                if (r.get("hi") != null) {
                    assertEquals(Double.parseDouble(r.get("hi")) - Double.parseDouble(r.get("lo")),
                            Double.parseDouble(r.get("rg")), 1e-9, backend + ": a row's range its high less its low");
                }
            }
            // R7 part 3 (2026-11-03): zscores, drawdown, max_drawdown and lag have rows forms too -- items of the same
            // table, each row once, and each the same as the word asked alone (a calendar span's here, a lag as of a
            // date reading one row of several there: by instrument below)
            List<Map<String, String>> more = rows(backend, "|#TS{SELECT CLOSE AS c, zscores(CLOSE, 3d) AS z,"
                    + " drawdown(CLOSE, 3d) AS dd, max_drawdown(CLOSE, 3d) AS md FROM PRICES ON DAY}#");
            assertEquals(2 * AAA.length, more.size(), backend + ": the three words columns of the same table");
            String by = " FROM PRICES ON DAY BY SYMBOL}#";
            List<Map<String, String>> together = rows(backend, "|#TS{SELECT CLOSE AS c, zscores(CLOSE, 3) AS z,"
                    + " drawdown(CLOSE, 3) AS dd, max_drawdown(CLOSE, 3) AS md, lag(CLOSE, 1) AS l" + by);
            for (String[] w : new String[][] {{"z", "zscores(CLOSE, 3)"}, {"dd", "drawdown(CLOSE, 3)"},
                    {"md", "max_drawdown(CLOSE, 3)"}, {"l", "lag(CLOSE, 1)"}}) {
                List<Map<String, String>> alone = rows(backend, "|#TS{SELECT " + w[1] + " AS " + w[0] + by);
                Map<String, String> asked = new java.util.TreeMap<>();
                alone.forEach(r -> asked.put(r.get("SYMBOL") + " " + r.get("DAY"), r.get(w[0])));
                Map<String, String> beside = new java.util.TreeMap<>();
                together.stream().filter(r -> r.get(w[0]) != null)
                        .forEach(r -> beside.put(r.get("SYMBOL") + " " + r.get("DAY"), r.get(w[0])));
                asked.values().removeIf(java.util.Objects::isNull);
                assertEquals(asked.keySet(), beside.keySet(), backend + ": " + w[1] + "'s points beside the column");
                for (String k : asked.keySet()) {
                    assertEquals(Double.parseDouble(asked.get(k)), Double.parseDouble(beside.get(k)), 1e-9,
                            backend + ": " + w[1] + " at " + k + ", beside the column as alone");
                }
            }
        }
    }

    @Test
    void linesOfSeveralRowsOnADateMeetAsALookupAndBothSoAreRefused() throws Exception {
        // the time model's D1 (ruled 2026-11-03): PRICES read with no BY has two rows on each date (AAA's and BBB's).
        // Met with a line of one row a date, each row pairs with that one (a lookup: each trade at its day's rate)
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> lookup = rows(backend, "|#TS{SELECT p.CLOSE - q.CLOSE['AAA'] AS d"
                    + " FROM PRICES p ON DAY, PRICES q ON DAY BY SYMBOL}#");
            assertEquals(2 * AAA.length, lookup.size(), backend + ": each of a date's rows paired once");
            assertEquals(AAA.length, lookup.stream().filter(r -> Math.abs(Double.parseDouble(r.get("d"))) < 1e-9)
                    .count(), backend + ": AAA's own rows less AAA's, nothing");
            // both lines with several rows on a date: no pairing is right, refused while the query runs, naming the
            // fix -- in an expression (lines meeting), and as two items (the answer's rows spread)
            // (a column read twice is one line: p.CLOSE - q.CLOSE of one table is 0 on each row; a subscript's
            // condition makes another line of the same rows)
            for (String q : List.of("SELECT CLOSE - CLOSE[CLOSE > 0] AS d FROM PRICES ON DAY",
                    "SELECT CLOSE AS a, CLOSE[CLOSE > 0] AS b FROM PRICES ON DAY")) {
                Exception e = assertThrows(Exception.class, () -> rows(backend, "|#TS{" + q + "}#"), backend + ": " + q);
                assertTrue(String.valueOf(e.getMessage()).contains("both have several rows on one date"),
                        backend + ": " + q + " refused by name: " + e.getMessage());
            }
        }
    }

    @Test
    void wordsOfNObservationsOfALineOfSeveralRowsOnADateAreRefusedNamingTheFix() throws Exception {
        // the time model's D8 (the user, 2026-11-06: "Refuse, name the fix"): PRICES read with no BY has two rows on
        // each date (AAA's and BBB's) in no order, so a word that counts observations has no one answer there -- the
        // same SQL gave three answers in three runs (docs/ts-sql-homework/data/equivalence_queries.sql, q13). Refused
        // while the query runs, naming the fix: a rows form (a window of n, lag and diff by obs), a line form (returns,
        // volatility), a word of a word, and a word of two lines (PRICES declares no key, so the count is the data's).
        // Extended the same day ("Okay extend the refusal") to the words that read one of a date's rows without
        // counting: a window to date or since a date, a lag as of a date (an as-of join), a value from the line's
        // first (rebase), an exponential average
        for (TestBackend backend : TestBackends.inProcess()) {
            for (String q : List.of("SELECT mean(CLOSE, 3) AS m", "SELECT CLOSE AS c, diff(CLOSE, 1) AS d",
                    "SELECT lag(CLOSE, 1) AS l", "SELECT returns(CLOSE, 1) AS r", "SELECT volatility(CLOSE, 3) AS v",
                    "SELECT max(diff(CLOSE, 1), 2d) AS mx", "SELECT correlation(CLOSE, CLOSE['AAA'], 3) AS k",
                    "SELECT sum(CLOSE, all) AS s", "SELECT sum(CLOSE, '2024-01-25') AS s", "SELECT lag(CLOSE, 2d) AS l",
                    "SELECT rebase(CLOSE) AS rb", "SELECT exponential_moving_average(CLOSE, 0.5) AS e")) {
                String query = "|#TS{" + q + " FROM PRICES ON DAY}#";
                if (q.contains("['AAA']")) {
                    query = "|#TS{" + q.replace("CLOSE, CLOSE", "p.CLOSE, q.CLOSE") + " FROM PRICES p ON DAY,"
                            + " PRICES q ON DAY BY SYMBOL}#";
                }
                if (q.contains("2d) AS l") && !deep().contains(backend)) {
                    continue; // a lag as of a date is an as-of join, which H2's and SQLite's dialects have none of
                }
                String run = query;
                Exception e = assertThrows(Exception.class, () -> rows(backend, run), backend + ": " + q);
                assertTrue(String.valueOf(e.getMessage()).contains("several rows on one date, which have no order: add BY"),
                        backend + ": " + q + " refused by name: " + e.getMessage());
            }
            // a calendar window reads all of a date's rows at once, in any order: it answers
            assertFalse(rows(backend, "|#TS{SELECT mean(CLOSE, 3d) AS m FROM PRICES ON DAY}#").isEmpty(),
                    backend + ": a calendar window of a line of several rows a date");
            // the fix the refusal names: an instrument's line, one row a date
            List<Map<String, String>> by = rows(backend, "|#TS{SELECT avg(CLOSE['AAA'], 3) AS ma"
                    + " FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length - 3, i -> mean3(AAA, i + 3)), column(by, null, null, "ma"),
                    backend + ": AAA's mean of three");
        }
    }

    @Test
    void aLineOfMomentsMeetingALineOfDaysIsRefusedNamingTheFix() throws Exception {
        // the time model's D4 (ruled 2026-11-03): TICKS has AAA's moments (10:00 and 15:00 each day), PRICES its days. A
        // moment is no day, and the two ways to pair them answer different questions, so v1 refuses where they meet --
        // an expression of both, a word of both, the two as items -- naming the fix (before, the first two found no
        // row, silently, and the third was the type checker's "cannot unite")
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals(2 * AAA.length, rows(backend, "|#TS{SELECT PRICE['AAA'] AS t FROM TICKS ON AT BY SYMBOL}#")
                    .size(), backend + ": a line of moments, a row a moment");
            for (String q : List.of("SELECT p.CLOSE['AAA'] - t.PRICE['AAA'] AS d",
                    "SELECT correlation(p.CLOSE['AAA'], t.PRICE['AAA'], 3) AS k",
                    "SELECT p.CLOSE['AAA'] AS c, t.PRICE['AAA'] AS t",
                    // a word's own meeting (helpers::meetOn) and spreading (spreadOn): the A2 review, 2026-11-03
                    "SELECT interpolate(p.CLOSE['AAA'], t.PRICE['AAA'], 'intersect') AS i",
                    "SELECT interpolate(p.CLOSE['AAA'], t.PRICE['AAA'], 'step') AS i",
                    "SELECT ohlc(p.CLOSE['AAA'], t.PRICE['AAA'], p.CLOSE['AAA'], t.PRICE['AAA']) AS o")) {
                Exception e = assertThrows(Exception.class, () -> rows(backend, "|#TS{" + q
                        + " FROM PRICES p ON DAY BY SYMBOL, TICKS t ON AT BY SYMBOL}#"), backend + ": " + q);
                assertTrue(String.valueOf(e.getMessage()).contains("a moment is no day: add SAMPLE BY 1d"),
                        backend + ": " + q + " refused by name: " + e.getMessage());
            }
        }
        // the fix the refusal names: SAMPLE BY 1d, each day's last tick (15:00's, its close) -- on the platform's
        // database (H2 has no ARG_MAX, which a day's last moment reads)
        for (TestBackend backend : deep()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT last(p.CLOSE['AAA']) - last(t.PRICE['AAA'])"
                    + " AS d FROM PRICES p ON DAY BY SYMBOL, TICKS t ON AT BY SYMBOL SAMPLE BY 1d}#");
            assertClose(each(AAA.length, i -> 0.0), column(got, null, null, "d"), backend + ": a day's close less its"
                    + " last tick");
            // ... and a word's: under SAMPLE BY a line's dates are its days, so the word's lines meet
            List<Map<String, String>> met = rows(backend, "|#TS{SELECT interpolate(last(p.CLOSE['AAA']),"
                    + " last(t.PRICE['AAA']), 'intersect') AS i FROM PRICES p ON DAY BY SYMBOL, TICKS t ON AT BY SYMBOL"
                    + " SAMPLE BY 1d}#");
            assertClose(each(AAA.length, i -> AAA[i]), column(met, null, null, "i"), backend + ": a day's close on"
                    + " the days both have");
        }
    }

    @Test
    void aWordOfAWordCountsFromTheFirstPointOfWhatItReads() throws Exception {
        // R7 (2026-11-03): a word's answer is a column of its line's own table, no point of its ramp's rows (gs-quant's
        // apply_ramp drops a word's first w points), and a figure of it no point where it is none -- so a word of
        // either counts from the first point of what it reads, as over that value's own line
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT avg(avg(CLOSE['AAA'], 3), 3) AS mm,"
                    + " CLOSE['AAA'] / avg(CLOSE['AAA'], 3) - 1 AS dev, avg(dev, 2) AS md FROM PRICES ON DAY BY SYMBOL}#");
            double[] x = AAA;
            Double[] inner = each(x.length, i -> mean3(x, i));
            Double[] dev = each(x.length, i -> i < 3 ? null : x[i] / mean3(x, i) - 1);
            // the answer's rows: from dev's first point (the fourth date), the first row an item has a value in (R6)
            assertClose(each(x.length - 3, i -> i + 3 < 6 ? null : (inner[i + 1] + inner[i + 2] + inner[i + 3]) / 3),
                    column(got, null, null, "mm"), backend + ": a mean of means, from the inner's fourth point");
            assertClose(each(x.length - 3, i -> dev[i + 3]), column(got, null, null, "dev"),
                    backend + ": a figure of a word, none where the word has no point");
            assertClose(each(x.length - 3, i -> i + 3 < 5 ? null : (dev[i + 2] + dev[i + 3]) / 2),
                    column(got, null, null, "md"), backend + ": a word of that figure, from its third point");
        }
    }

    @Test
    void aWindowCountsAnyWholeNumberOfObservations() throws Exception {
        // as gs-quant's Window(w) (the user, 2026-11-02): no cap of 100,000; a window longer than the line never fills,
        // so no row has a value
        List<Map<String, String>> got = rows(TestBackends.platform(), "|#TS{SELECT avg(CLOSE['AAA'], 250000) AS ma"
                + " FROM PRICES ON DAY BY SYMBOL}#");
        assertEquals(List.of(), got, "a window of 250,000 observations over 12 never fills");
    }

    @Test
    void aQueryAnswersAnyNumberOfItemsARowKeptWhereAnyHasAValue() throws Exception {
        // the 12-column cap lifted (the user, 2026-11-02: "Yes array read"): someValued reads the items' columns as one
        // list, so thirteen items run, and a row is kept for its thirteenth item's value when the twelve before have none.
        // Thirteen lines meeting is a query of many lines (deep(): H2's parser runs out of memory on it, 2026-11-02;
        // the array read itself runs on every in-process database, TimeseriesLibraryTest)
        StringBuilder items = new StringBuilder();
        for (int i = 1; i <= 12; i++) {
            items.append("avg(CLOSE['AAA'], 250000) AS e").append(i).append(", ");
        }
        for (TestBackend backend : deep()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT " + items + "CLOSE['AAA'] AS v"
                    + " FROM PRICES ON DAY BY SYMBOL}#");
            assertEquals(AAA.length, got.size(), backend + ": every AAA date kept by the thirteenth item");
            assertEquals(14, got.get(0).size(), backend + ": the date and thirteen items");
            assertNull(got.get(0).get("e12"), backend + ": a window that never fills has no value");
            List<Map<String, String>> none = rows(backend, "|#TS{SELECT " + items + "avg(CLOSE['AAA'], 250000) AS e13"
                    + " FROM PRICES ON DAY BY SYMBOL}#");
            assertEquals(List.of(), none, backend + ": no item has a value, no row");
        }
    }

    @Test
    void aLineOfNoInstrumentAccruedBesideLinesPerInstrumentIsRefusedByName() throws Exception {
        // a word that accrues over time (a basket's members, each with its number in time) does not take a line of no
        // instrument as every instrument's (TsWords.timed, R2; docs/TS_ONE_TREE_PLAN_2026_10_31.md §6a, what stays in
        // Java): the refusal names the fix
        String e = refused("SELECT basket(CLOSE['AAA'], 0.5, CLOSE, 0.5) AS b FROM PRICES ON DAY BY SYMBOL", true);
        assertTrue(e.contains("a line of no instrument accrued with lines per instrument: pick one line with a"
                + " subscript (R2)"), e);
    }

    private static double sampleStd(double[] v, int from, int to) {
        double mean = 0;
        for (int i = from; i <= to; i++) {
            mean += v[i];
        }
        mean /= to - from + 1;
        double sum = 0;
        for (int i = from; i <= to; i++) {
            sum += (v[i] - mean) * (v[i] - mean);
        }
        return Math.sqrt(sum / (to - from));
    }

    @Test
    void theCatalogsFunctionsAreTheTilesLibraryCalls() throws Exception {
        // P4: the tile's functions of the same names (timeseries-query.ts call), each the library's call
        double[] x = AAA;
        double[] r = new double[x.length];
        for (int i = 1; i < x.length; i++) {
            r[i] = x[i] / x[i - 1] - 1;
        }
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT rebase(CLOSE['AAA']) AS rb,"
                    + " change(CLOSE['AAA']) AS ch, percent_change(CLOSE['AAA']) AS pc, range(CLOSE['AAA'], 3) AS rg,"
                    + " volatility(CLOSE['AAA'], 3) AS vol FROM PRICES ON DAY BY SYMBOL}#");
            String of = backend + ": ";
            assertClose(each(x.length, i -> x[i] / x[0] * 100), column(got, null, null, "rb"), of + "rebased to 100");
            assertClose(each(x.length, i -> x[i] - x[0]), column(got, null, null, "ch"), of + "the change from the first");
            assertClose(each(x.length, i -> x[i] / x[0] - 1), column(got, null, null, "pc"), of + "the % change");
            assertClose(each(x.length, i -> i < 3 ? null : Math.max(x[i], Math.max(x[i - 1], x[i - 2]))
                    - Math.min(x[i], Math.min(x[i - 1], x[i - 2]))), column(got, null, null, "rg"), of + "the range");
            // three returns' deviation, annualized by the 252 days a year the daily dates infer, in percent
            assertClose(each(x.length, i -> i < 3 ? null : sampleStd(r, i - 2, i) * Math.sqrt(252) * 100),
                    column(got, null, null, "vol"), of + "volatility over three returns");
        }
    }

    @Test
    void pairsConditionsAndBandsAreTheTilesLibraryCalls() throws Exception {
        // P4 part 2: a pair's statistics over the dates both lines have, a condition's branch, a band's fields
        double[] ra = new double[AAA.length];
        double[] rb = new double[BBB.length];
        for (int i = 1; i < AAA.length; i++) {
            ra[i] = AAA[i] / AAA[i - 1] - 1;
            rb[i] = BBB[i] / BBB[i - 1] - 1;
        }
        for (TestBackend backend : deep()) {
            String of = backend + ": ";
            List<Map<String, String>> pairs = rows(backend, "|#TS{SELECT correlation(CLOSE['AAA'], CLOSE['BBB'], 3)"
                    + " AS c, beta(CLOSE['AAA'], CLOSE['BBB'], 3) AS b FROM PRICES ON DAY BY SYMBOL}#");
            // three returns of each, from the fourth day (R6: the days before have no value)
            assertClose(each(AAA.length - 3, i -> pearson(ra, rb, i + 1, i + 3)), column(pairs, null, null, "c"),
                    of + "the correlation of three returns");
            assertClose(each(AAA.length - 3, i -> covariance(ra, rb, i + 1, i + 3) / covariance(rb, rb, i + 1, i + 3)),
                    column(pairs, null, null, "b"), of + "the beta of three returns");
            List<Map<String, String>> chosen = rows(backend, "|#TS{SELECT compare(CLOSE['AAA'], 100) AS cmp,"
                    + " if(CLOSE['AAA'] > 100, CLOSE['AAA'], 0) AS f FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> (double) Double.compare(AAA[i], 100)), column(chosen, null, null, "cmp"),
                    of + "compare");
            assertClose(each(AAA.length, i -> AAA[i] > 100 ? AAA[i] : 0.0), column(chosen, null, null, "f"),
                    of + "a condition's branch");
            List<Map<String, String>> band = rows(backend, "|#TS{SELECT bollinger_bands(CLOSE['AAA'], 3, 2) AS bb,"
                    + " bb.upper - bb.lower AS spread FROM PRICES ON DAY BY SYMBOL FILTER bb.width > 0}#");
            // a band's fields each the answer's column (R10)
            assertEquals(List.of("DAY", "bb_lower", "bb_upper", "bb_width", "spread"), List.copyOf(band.get(0).keySet()),
                    of + "the answer's columns");
            // from the fourth day: gs-quant's ramp of three
            Double[] mean = each(AAA.length - 3, i -> mean3(AAA, i + 3));
            Double[] std = each(AAA.length - 3, i -> sampleStd(AAA, i + 1, i + 3));
            assertClose(each(mean.length, i -> mean[i] - 2 * std[i]), column(band, null, null, "bb_lower"),
                    of + "the band's lower");
            assertClose(each(mean.length, i -> mean[i] + 2 * std[i]), column(band, null, null, "bb_upper"),
                    of + "the band's upper");
            assertClose(each(mean.length, i -> 4 * std[i]), column(band, null, null, "bb_width"), of + "its width");
            assertClose(each(mean.length, i -> 4 * std[i]), column(band, null, null, "spread"),
                    of + "an item of its fields");
            // as of a date (R9): the last value on or before it, its first before it starts; a candle's fields
            List<Map<String, String>> asOf = rows(backend, "|#TS{SELECT value_at(CLOSE['AAA'], '2024-01-25') AS va,"
                    + " value_at(CLOSE['AAA'], DATE '2024-01-01') AS vb, ohlc(CLOSE['AAA'], CLOSE['BBB'], CLOSE['AAA'],"
                    + " CLOSE['BBB']) AS c FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> AAA[5]), column(asOf, null, null, "va"), of + "as of the 25th");
            // a base from a date (R9): the value as of it, the first a chart shows
            List<Map<String, String>> from = rows(backend, "|#TS{SELECT index(CLOSE['AAA'], from => '2024-01-25') AS ix,"
                    + " percent_change(CLOSE['AAA'], from => '2024-01-25') AS pc FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> AAA[i] / AAA[5]), column(from, null, null, "ix"), of + "an index from the 25th");
            assertClose(each(AAA.length, i -> AAA[i] / AAA[5] - 1), column(from, null, null, "pc"),
                    of + "a % change from the 25th");
            assertClose(each(AAA.length, i -> AAA[0]), column(asOf, null, null, "vb"), of + "as of before the first");
            assertEquals(List.of("DAY", "va", "vb", "c_open", "c_high", "c_low", "c_close"),
                    List.copyOf(asOf.get(0).keySet()), of + "a candle's columns");
            assertClose(each(AAA.length, i -> BBB[i]), column(asOf, null, null, "c_high"), of + "the candle's high");
        }
    }

    @Test
    void everyFunctionOfTheCatalogPlansAndRuns() throws Exception {
        // P4: each of §6's functions the builder writes, as the tile writes it, planned and run (its numbers are the
        // library's own tests' and the tile's; here each call's arguments and columns are held to the library's types)
        String x = "CLOSE['AAA']";
        String y = "CLOSE['BBB']";
        List<String> calls = new ArrayList<>();
        for (String f : List.of("prices", "annualize", "index", "rebase", "change", "percent_change", "winsorize",
                "drawdown", "max_drawdown", "exponential_moving_average", "exponential_std", "exponential_volatility",
                "exponential_spread_volatility", "geometrically_aggregate", "smooth_spikes", "consecutive", "day",
                "month", "year", "quarter", "weekday")) {
            calls.add(f + "(" + x + ")");
        }
        calls.addAll(List.of("first(" + x + ", all)", "last(" + x + ", whole)"));
        for (String f : List.of("range", "zscores", "volatility", "sharpe_ratio", "rsi", "smoothed_moving_average",
                "percentiles", "median", "product", "vol_swap_volatility")) {
            calls.add(f + "(" + x + ", 3)");
        }
        for (String f : List.of("range", "zscores", "volatility", "sharpe_ratio", "median", "drawdown")) {
            calls.add(f + "(" + x + ", 1w)");
        }
        for (String method : List.of("intersect", "nan", "zero", "step")) {
            calls.add("interpolate(" + x + ", " + y + ", '" + method + "')");
        }
        // a window of everything by name, a lead, a per-point power of a figure (the gallery's, P5)
        calls.addAll(List.of("drawdown(" + x + ", w => all)", "lead_lag(" + x + ", " + y + ", lag => -1, w => 3)",
                "power(std(" + x + ", 3), 2)"));
        calls.addAll(List.of("value_at(" + x + ", '2024-01-25')", "value_at(" + x + ", DATE '2024-01-01')",
                "ohlc(" + x + ", " + y + ", " + x + ", " + y + ")", "ohlc(" + x + ", " + x + ", " + x + ", " + x + ")"));
        calls.addAll(List.of("macd(" + x + ", 2, 4, 1)", "percentile(" + x + ", 50, 3)", "dickey_fuller(" + x + ", 5)",
                "half_life(" + x + ", 5)", "filter(" + x + ", 'less_than', 100)",
                "volatility(" + x + ", 3, type => 'log')", "smooth_spikes(" + x + ", 0.5, 'absolute')"));
        for (String f : List.of("correlation", "beta", "cov", "alpha", "r_squared", "hedge_ratio", "residual",
                "tracking_error", "information_ratio", "up_capture", "down_capture", "vwap", "donchian_channels")) {
            calls.add(f + "(" + x + ", " + y + ", 3)");
            calls.add(f + "(" + x + ", " + y + ", 1w)");
        }
        for (String f : List.of("obv", "total_return", "excess_returns", "compare", "union", "append", "prepend")) {
            calls.add(f + "(" + x + ", " + y + ")");
        }
        calls.addAll(List.of("lead_lag(" + x + ", " + y + ", 1, 3)", "total_return(" + x + ", 0.01)",
                "excess_returns(" + x + ", 0.01)", "atr(" + x + ", " + y + ", " + x + ", 3)",
                "stochastic(" + x + ", " + y + ", " + x + ", 3)", "keltner_channels(" + x + ", " + y + ", " + x
                        + ", 3, 3, 2)", "if(" + x + " > 100, " + x + ", " + y + ")", "weighted_sum(" + x + ", 0.6, " + y
                        + ", 0.4)", "basket(" + x + ", 0.6, " + y + ", 0.4)", "basket(" + x + ", 0.5, " + y
                        + ", 0.5, rebalance => 'weekly')", "bollinger_bands(" + x + ", 3)", "bollinger_bands(" + x
                        + ", 1w, 1)"));
        calls.replaceAll(c -> c + " AS v FROM PRICES ON DAY BY SYMBOL");
        // a decomposition counts its cycle in the query's periods; a candle under SAMPLE BY takes each period's own
        String last = "last(" + x + ")";
        calls.addAll(List.of("trend(" + last + ", freq => 'week') AS v FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1d",
                "seasonally_adjusted(" + last + ", freq => 'week') AS v FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1d",
                "seasonally_adjusted(" + last + ", 'multiplicative', 'week') AS v FROM PRICES ON DAY BY SYMBOL"
                        + " SAMPLE BY 1d",
                "ohlc(first(" + x + "), max(" + x + "), min(" + x + "), " + last + ") AS v FROM PRICES ON DAY BY SYMBOL"
                        + " SAMPLE BY 1w"));
        TestBackend backend = TestBackends.platform();
        com.legend.compiler.element.ModelContext model = Compiler.compileModel(model(backend, true));
        List<String> failed = new ArrayList<>();
        for (String call : calls) {
            try {
                List<Map<String, String>> got = rows(backend, model, "|#TS{SELECT " + call + "}#");
                if (got.isEmpty()) {
                    failed.add(call + ": no row has a value");
                }
            } catch (RuntimeException e) {
                failed.add(call + ": " + e.getMessage());
            }
        }
        assertEquals(List.of(), failed, "every function plans and runs on " + backend);
    }

    @Test
    void aSampleBysPeriodsAYearAreTheLibrarys() throws Exception {
        // an annualization under SAMPLE BY counts the sample's periods a year (timeseries::lines::perYear: a day's 252,
        // a week's 52), passed to the word as its sample -- the translator binds no parameter by its name (2026-11-01).
        // Every third day: their gaps infer no periods a year (gs-quant's buckets), so a value at all is the sample's.
        String thin = " FROM (PRICES WHERE DAY = DATE '2024-01-20' OR DAY = DATE '2024-01-23' OR DAY = DATE '2024-01-26'"
                + " OR DAY = DATE '2024-01-29') ON DAY BY SYMBOL";
        Double[] days = each(4, i -> AAA[3 * i] * Math.sqrt(252));
        for (TestBackend backend : deep()) {
            assertClose(days, column(rows(backend, "|#TS{SELECT annualize(last(CLOSE['AAA'])) AS a" + thin
                    + " SAMPLE BY 1d}#"), null, null, "a"), backend + ": SAMPLE BY 1d's 252 a year");
            assertEquals(List.of(), rows(backend, "|#TS{SELECT annualize(CLOSE['AAA']) AS a" + thin + "}#"),
                    backend + ": three days apart, the dates infer none");
            assertClose(each(4, i -> AAA[3 * i] * Math.sqrt(12)), column(rows(backend, "|#TS{SELECT annualize("
                    + "last(CLOSE['AAA']), annualization_factor => 12) AS a" + thin + " SAMPLE BY 1d}#"), null, null,
                    "a"), backend + ": a factor given is the factor");
        }
    }

    @Test
    void stepIsPrevAndNeitherFillsALinesStart() throws Exception {
        // FILL(STEP) is FILL(PREV) (R12, ruled 2026-10-31: gs-quant's step also gives a line's first value before it
        // starts, the future read; docs/SEMANTICS_REGISTER.md S130): none before BBB's first value either way
        String from = " FROM PRICES a ON DAY BY SYMBOL, (PRICES WHERE DAY >= DATE '2024-01-24') b ON DAY BY SYMBOL";
        for (TestBackend backend : deep()) {
            List<Map<String, String>> step = rows(backend, "|#TS{SELECT a.CLOSE['AAA'] - b.CLOSE['BBB'] AS d" + from
                    + " FILL(STEP)}#");
            assertClose(each(AAA.length - 4, i -> AAA[i + 4] - BBB[i + 4]), column(step, null, null, "d"),
                    backend + ": none before BBB's first value (R12)");
            List<Map<String, String>> prev = rows(backend, "|#TS{SELECT a.CLOSE['AAA'] - b.CLOSE['BBB'] AS d" + from
                    + " FILL(PREV)}#");
            assertClose(each(AAA.length - 4, i -> AAA[i + 4] - BBB[i + 4]), column(prev, null, null, "d"),
                    backend + ": none before BBB's first value (R6: a row where an item has one)");
            assertEquals(rows(backend, "|#TS{SELECT drawdown(CLOSE['AAA']) AS dd FROM PRICES ON DAY BY SYMBOL}#"),
                    rows(backend, "|#TS{SELECT drawdown(CLOSE['AAA'], w => all) AS dd FROM PRICES ON DAY BY SYMBOL}#"),
                    backend + ": a window of everything by name");
        }
    }

    @Test
    void theWordsOfTwoLinesAreColumnsOfTheirOneMeeting() throws Exception {
        // R7 part 3 (2026-11-04): two lines met once -- hedge_ratio, residual and cov of their levels each a column of
        // that one table (the library's rows forms), each its line form's answer: least squares over the last three
        // pairs, a value from the third pair (the ramp's rows none, the answer's first rows none of any item)
        String q = "|#TS{SELECT hedge_ratio(CLOSE['AAA'], CLOSE['BBB'], 3) AS h, residual(CLOSE['AAA'], CLOSE['BBB'], 3)"
                + " AS r, cov(CLOSE['AAA'], CLOSE['BBB'], 3) AS c FROM PRICES ON DAY BY SYMBOL}#";
        for (TestBackend backend : deep()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, q);
            Double[] slope = each(AAA.length - 2, i -> covariance(AAA, BBB, i, i + 2) / covariance(BBB, BBB, i, i + 2));
            assertClose(slope, column(got, null, null, "h"), of + "the slope of three pairs");
            assertClose(each(AAA.length - 2, i -> AAA[i + 2] - ((AAA[i] + AAA[i + 1] + AAA[i + 2]) / 3
                    - slope[i] * (BBB[i] + BBB[i + 1] + BBB[i + 2]) / 3 + slope[i] * BBB[i + 2])),
                    column(got, null, null, "r"), of + "the first less its fit");
            assertClose(each(AAA.length - 2, i -> covariance(AAA, BBB, i, i + 2)), column(got, null, null, "c"),
                    of + "the covariance of three pairs");
        }
        // the two lines joined once, the answer that one table: one join, where each word's own meeting and its own
        // table spread onto the answer's dates were six
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        assertEquals(1, sql.split(" JOIN ", -1).length - 1, sql);
    }

    @Test
    void theReturnsWordsOfTwoLinesAreColumnsOfTheirReturnsMeeting() throws Exception {
        // W2 (2026-11-06): correlation, beta, r_squared, alpha and tracking_error take each line's returns first (the
        // library's timeseries::Words.pairs): each line's returns a column of its own table, the two met once, every
        // word a column of that one table -- the line forms' answers: three returns of each, from the fourth day
        double[] ra = new double[AAA.length];
        double[] rb = new double[BBB.length];
        for (int i = 1; i < AAA.length; i++) {
            ra[i] = AAA[i] / AAA[i - 1] - 1;
            rb[i] = BBB[i] / BBB[i - 1] - 1;
        }
        String q = "|#TS{SELECT correlation(CLOSE['AAA'], CLOSE['BBB'], 3) AS c, beta(CLOSE['AAA'], CLOSE['BBB'], 3) AS b,"
                + " r_squared(CLOSE['AAA'], CLOSE['BBB'], 3) AS r2, alpha(CLOSE['AAA'], CLOSE['BBB'], 3) AS a,"
                + " tracking_error(CLOSE['AAA'], CLOSE['BBB'], 3) AS te FROM PRICES ON DAY BY SYMBOL}#";
        double[] gap = new double[AAA.length];
        for (int i = 1; i < AAA.length; i++) {
            gap[i] = ra[i] - rb[i];
        }
        for (TestBackend backend : deep()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, q);
            assertClose(each(AAA.length - 3, i -> pearson(ra, rb, i + 1, i + 3)), column(got, null, null, "c"), of + "the correlation of three returns");
            assertClose(each(AAA.length - 3, i -> covariance(ra, rb, i + 1, i + 3) / covariance(rb, rb, i + 1, i + 3)),
                    column(got, null, null, "b"), of + "the beta of three returns");
            assertClose(each(AAA.length - 3, i -> pearson(ra, rb, i + 1, i + 3) * pearson(ra, rb, i + 1, i + 3)),
                    column(got, null, null, "r2"), of + "the R squared of three returns");
            // alpha and tracking error annualized by the daily dates' 252 periods a year (helpers::annualizedPair)
            assertClose(each(AAA.length - 3, i -> (mean(ra, i + 1, i + 3) - covariance(ra, rb, i + 1, i + 3)
                    / covariance(rb, rb, i + 1, i + 3) * mean(rb, i + 1, i + 3)) * 252), column(got, null, null, "a"),
                    of + "alpha over three returns, annualized");
            assertClose(each(AAA.length - 3, i -> sampleStd(gap, i + 1, i + 3) * Math.sqrt(252) * 100),
                    column(got, null, null, "te"), of + "the tracking error of three returns, in percent");
        }
        // the two lines' returns met once, the answer that one table: one join
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        assertEquals(1, sql.split(" JOIN ", -1).length - 1, sql);
    }

    @Test
    void aDefaultIsReadAsTheCallItsBodyIs() throws Exception {
        // W5 (2026-11-06): correlation(x, y) is the library's correlation($x, $y, 63, $sample), lag(x) its
        // $x->lag(1), returns(x) its $x->returns(1, $type): each default read as that call, its rows form the called
        // overload's -- the default's value written once, in its body -- so each is a column of its line's table, and
        // its answer the explicit call's
        String q = "|#TS{SELECT correlation(CLOSE['AAA'], CLOSE['BBB']) AS c, lag(CLOSE['AAA']) AS l,"
                + " returns(CLOSE['AAA']) AS r FROM PRICES ON DAY BY SYMBOL}#";
        String explicit = "|#TS{SELECT correlation(CLOSE['AAA'], CLOSE['BBB'], 63) AS c, lag(CLOSE['AAA'], 1) AS l,"
                + " returns(CLOSE['AAA'], 1) AS r FROM PRICES ON DAY BY SYMBOL}#";
        for (TestBackend backend : TestBackends.inProcess()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, q);
            List<Map<String, String>> want = rows(backend, explicit);
            for (String col : List.of("c", "l", "r")) {
                assertArrayEquals(column(want, null, null, col), column(got, null, null, col), of + col + " as its call");
            }
            // from the second day: the first is no item's point (lag's ramp, a return's first)
            assertClose(each(AAA.length - 1, i -> AAA[i]), column(got, null, null, "l"), of + "the day before's close");
        }
        // read as its call, a default writes the explicit call's SQL
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        assertEquals(Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), explicit)
                .plan("ts::RT").sql(), sql);
    }

    @Test
    void wordsToDateAreColumnsOfTheirLinesTable() throws Exception {
        // W5 (2026-11-06): a word over its line from its first point to each date ("all", timeseries::Scope) is a
        // column of its line's own table -- its line form's table function over the table's rows, the ramp a count of
        // the window's values -- so a word of a word to date reads the inner word's points alone: the line forms'
        // answers, and no join
        String q = "|#TS{SELECT mean(CLOSE['AAA'], all) AS m, std(CLOSE['AAA'], all) AS s,"
                + " max_drawdown(CLOSE['AAA'], all) AS md, max(diff(CLOSE['AAA'], 2), all) AS hi FROM PRICES ON DAY BY SYMBOL}#";
        for (TestBackend backend : TestBackends.inProcess()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, q);
            assertClose(each(AAA.length, i -> mean(AAA, 0, i)), column(got, null, null, "m"), of + "the mean to date");
            assertClose(each(AAA.length, i -> i == 0 ? null : sampleStd(AAA, 0, i)), column(got, null, null, "s"),
                    of + "the deviation to date, from the second day");
            assertClose(each(AAA.length, i -> {
                double peak = AAA[0];
                double deepest = 0;
                for (int k = 0; k <= i; k++) {
                    peak = Math.max(peak, AAA[k]);
                    deepest = Math.min(deepest, AAA[k] / peak - 1);
                }
                return deepest;
            }), column(got, null, null, "md"), of + "the deepest fall to date");
            // diff over two days has points from the third day: the most of them to date, none before
            assertClose(each(AAA.length, i -> {
                if (i < 2) {
                    return null;
                }
                double most = Double.NEGATIVE_INFINITY;
                for (int k = 2; k <= i; k++) {
                    most = Math.max(most, AAA[k] - AAA[k - 2]);
                }
                return most;
            }), column(got, null, null, "hi"), of + "the most two-day change to date, from the third day");
        }
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        assertFalse(sql.contains(" JOIN "), "every word a column of AAA's table: " + sql);
    }

    @Test
    void aPairsHedgeRatioAndResidualComputeTheirRegressionOnce() throws Exception {
        // W4 (2026-11-06, guide-12): residual reads hedge_ratio's regression -- the same five windows over the pair's
        // met table. SelectLayers laid hedge_ratio's out a level below residual's, where the meeting's date is still
        // its expression and residual's read it as a column: one window under two spellings, computed twice (eight
        // windows). Known by its expanded form, each is one value: five (a covariance, a variance, two means, a count)
        String q = "|#TS{SELECT hedge_ratio(CLOSE['AAA'], CLOSE['BBB'], 3) AS h, residual(CLOSE['AAA'], CLOSE['BBB'], 3)"
                + " AS r FROM PRICES ON DAY BY SYMBOL}#";
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        for (String w : List.of("COVAR_SAMP(", "VAR_SAMP(", "AVG(")) {
            // (the model's PRICES declares no key, so each line's per-period count is a window of its own besides)
            assertEquals(w.equals("AVG(") ? 2 : 1, sql.split("\\b" + java.util.regex.Pattern.quote(w), -1).length - 1,
                    w + " the regression's, once: " + sql);
        }
        // and the answers: each word's as it is alone
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, q);
            assertArrayEquals(column(rows(backend, "|#TS{SELECT hedge_ratio(CLOSE['AAA'], CLOSE['BBB'], 3) AS h FROM"
                    + " PRICES ON DAY BY SYMBOL}#"), null, null, "h"), column(got, null, null, "h"), backend + ": h");
            assertArrayEquals(column(rows(backend, "|#TS{SELECT residual(CLOSE['AAA'], CLOSE['BBB'], 3) AS r FROM"
                    + " PRICES ON DAY BY SYMBOL}#"), null, null, "r"), column(got, null, null, "r"), backend + ": r");
        }
    }

    @Test
    void wordsOverACalendarSpanAreColumnsOfTheirLinesTable() throws Exception {
        // W5 (2026-11-06): count, product and vol_swap_volatility over a calendar span, and the capture ratios over one,
        // as columns of their line's table (or of the two lines' returns met): each its line form's table function --
        // the window (d - 3 days, d] of the daily dates, none until the line's first date is a span back (its ramp)
        String q = "|#TS{SELECT count(CLOSE['AAA'], 3d) AS n, product(CLOSE['AAA'], 3d) AS p,"
                + " vol_swap_volatility(CLOSE['AAA'], 3d) AS vs FROM PRICES ON DAY BY SYMBOL}#";
        double[] lr = new double[AAA.length];
        for (int i = 1; i < AAA.length; i++) {
            lr[i] = Math.log(AAA[i] / AAA[i - 1]);
        }
        // from the fourth day: the three before are every word's ramp, no item's point (R6)
        for (TestBackend backend : TestBackends.inProcess()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, q);
            assertClose(each(AAA.length - 3, i -> 3.0), column(got, null, null, "n"), of + "three days' count");
            assertClose(each(AAA.length - 3, i -> AAA[i + 3] * AAA[i + 2] * AAA[i + 1]), column(got, null, null, "p"),
                    of + "three days' product");
            // the root mean square of the three days' log returns, annualized, in percent
            assertClose(each(AAA.length - 3, i -> {
                double sq = 0;
                for (int k = i + 1; k <= i + 3; k++) {
                    sq += lr[k] * lr[k];
                }
                return Math.sqrt(sq / 3) * Math.sqrt(252) * 100;
            }), column(got, null, null, "vs"), of + "three days' vol swap volatility");
        }
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        assertFalse(sql.contains(" JOIN "), "every word a column of AAA's table: " + sql);
    }

    @Test
    void anAnswerOfALineAndALineMetFromItIsThatLinesRows() throws Exception {
        // W3 (2026-11-06): s is AAA and BBB met; t meets s's table with AAA's change, so t's rows are some of s's.
        // The answer's periods, every period any item has, are s's rows: s's table is the answer's spine, t's values
        // joined onto it -- no union of the two tables' periods, no grouping of them (spec-13's shape: its 5 joins 4)
        String q = "|#TS{SELECT CLOSE['AAA'] - CLOSE['BBB'] AS s, lag(s, 1) * diff(CLOSE['AAA'], 1) AS t"
                + " FROM PRICES ON DAY BY SYMBOL}#";
        for (TestBackend backend : TestBackends.inProcess()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, q);
            assertClose(each(AAA.length, i -> AAA[i] - BBB[i]), column(got, null, null, "s"), of + "every day's s");
            assertClose(each(AAA.length, i -> i == 0 ? null : (AAA[i - 1] - BBB[i - 1]) * (AAA[i] - AAA[i - 1])),
                    column(got, null, null, "t"), of + "t from the second day, none on the first");
        }
        String sql = Compiler.query(Compiler.compileModel(model(TestBackends.platform(), true)), q).plan("ts::RT").sql();
        assertFalse(sql.contains("UNION"), "s's table the spine, no union of periods: " + sql);
        assertFalse(sql.contains("GROUP BY"), "no grouping of periods: " + sql);
    }

    @Test
    void aValueATableMetFromItsBaseCarriesIsReadThere() throws Exception {
        // 2026-11-06 (found by W3's test): s's table met AAA and BBB, so AAA's close is a column of it; t reads AAA's close
        // again beside s, and cov pairs s with it. s's table meets AAA's line again, its column of AAA's close not
        // carried twice -- before, the two copies clashed ("the column '__ts_base_0' already exists")
        for (TestBackend backend : TestBackends.inProcess()) {
            String of = backend + ": ";
            List<Map<String, String>> got = rows(backend, "|#TS{SELECT CLOSE['AAA'] - CLOSE['BBB'] AS s,"
                    + " lag(s, 1) * CLOSE['AAA'] AS t FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> i == 0 ? null : (AAA[i - 1] - BBB[i - 1]) * AAA[i]),
                    column(got, null, null, "t"), of + "t, AAA's close by yesterday's s");
        }
        for (TestBackend backend : deep()) {
            List<Map<String, String>> pair = rows(backend, "|#TS{SELECT CLOSE['AAA'] - CLOSE['BBB'] AS s,"
                    + " cov(s, CLOSE['AAA'], 3) AS c FROM PRICES ON DAY BY SYMBOL}#");
            double[] s = new double[AAA.length];
            for (int i = 0; i < AAA.length; i++) {
                s[i] = AAA[i] - BBB[i];
            }
            assertClose(each(AAA.length, i -> i < 2 ? null : covariance(s, AAA, i - 2, i)), column(pair, null, null, "c"),
                    backend + ": the covariance of s and AAA over three days");
        }
    }

    /** The mean of {@code x} from {@code from} to {@code to}, both included. */
    private static double mean(double[] x, int from, int to) {
        double s = 0;
        for (int i = from; i <= to; i++) {
            s += x[i];
        }
        return s / (to - from + 1);
    }

    /** The sample covariance of {@code x} and {@code y} from {@code from} to {@code to}, both included. */
    private static double covariance(double[] x, double[] y, int from, int to) {
        double mx = 0;
        double my = 0;
        for (int i = from; i <= to; i++) {
            mx += x[i];
            my += y[i];
        }
        mx /= to - from + 1;
        my /= to - from + 1;
        double sum = 0;
        for (int i = from; i <= to; i++) {
            sum += (x[i] - mx) * (y[i] - my);
        }
        return sum / (to - from);
    }

    private static double pearson(double[] x, double[] y, int from, int to) {
        return covariance(x, y, from, to) / Math.sqrt(covariance(x, x, from, to) * covariance(y, y, from, to));
    }

    @Test
    void anEnumerationsValueIsWrittenAsAString() throws Exception {
        // R13: 'TECH' is ts::Sector.TECH, which the mapping stores as 'T' -- not the stored code compared with a string
        String pure = """
                  ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-2, 0)), ~close, 3, ~ma3)"""
                + ramp("ticker", "day", "ma3", 3) + """
                  ->select(~[day, ticker, ma3])
                  ->sort([~day->ascending(), ~ticker->ascending()])""";
        String quotes = "|ts::Quote.all()->project(~[day: x|$x.day, ticker: x|$x.instrument.ticker,"
                + " sector: x|$x.instrument.sector, close: x|$x.close])";
        var tech = same("""
                SELECT avg(close, 3) AS ma3
                FROM   (Quote WHERE instrument.sector = 'TECH') ON day BY instrument.ticker""",
                quotes + "->filter(r|$r.sector == ts::Sector.TECH)" + pure, 2);
        var both = same("""
                SELECT avg(close, 3) AS ma3
                FROM   (Quote WHERE instrument.sector IN ('TECH', 'ENERGY')) ON day BY instrument.ticker""",
                quotes + "->filter(r|$r.sector->in([ts::Sector.TECH, ts::Sector.ENERGY]))" + pure, 2);
        for (TestBackend backend : tech.keySet()) {
            assertClose(each(AAA.length - 3, i -> mean3(AAA, i + 3)), column(tech.get(backend), null, null, "ma3"),
                    backend + ": the TECH instrument's line alone, where its mean has a value (R6)");
            assertEquals(AAA.length + BBB.length - 6, both.get(backend).size(), backend + ": both sectors' lines");
        }
        assertTrue(refused("SELECT avg(close, 3) AS ma3 FROM (Quote WHERE instrument.sector = 'TEK') ON day", true)
                .contains("'TEK' is not a value of ts::Sector: TECH, ENERGY (R13)"), "a value the enumeration has not");
    }

    @Test
    void theWordsAreGsQuantsNames() throws Exception {
        // the vocabulary as ruled (docs/TS_ONE_TREE_PLAN_2026_10_31.md: D2 and R10, gs-quant's names; D3, R3, R14, R15)
        for (TestBackend backend : deep()) {
            String of = backend + ": ";
            // m is months, mo its alias (R14); avg is mean's alias (R15)
            assertEquals(rows(backend, "|#TS{SELECT mean(CLOSE['AAA'], 1mo) AS a FROM PRICES ON DAY BY SYMBOL}#"),
                    rows(backend, "|#TS{SELECT mean(CLOSE['AAA'], 1m) AS a FROM PRICES ON DAY BY SYMBOL}#"), of + "1m is 1mo");
            assertEquals(rows(backend, "|#TS{SELECT mean(CLOSE['AAA'], 3) AS a FROM PRICES ON DAY BY SYMBOL}#"),
                    rows(backend, "|#TS{SELECT avg(CLOSE['AAA'], 3) AS a FROM PRICES ON DAY BY SYMBOL}#"), of + "avg is mean");
            // gs-quant's first and last: the line's first value on every date, and (the future read, by name) its last
            List<Map<String, String>> ends = rows(backend, "|#TS{SELECT first(CLOSE['AAA'], all) AS f,"
                    + " last(CLOSE['AAA'], whole) AS l FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> AAA[0]), column(ends, null, null, "f"), of + "first(x, all)");
            assertClose(each(AAA.length, i -> AAA[AAA.length - 1]), column(ends, null, null, "l"), of + "last(x, whole)");
            // union, a word SQL reserves, is gs-quant's call (R3): x where it has a value, else y
            List<Map<String, String>> u = rows(backend, "|#TS{SELECT union(CLOSE['AAA'], CLOSE['BBB']) AS u FROM PRICES"
                    + " ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> AAA[i]), column(u, null, null, "u"), of + "union");
            // range, filter, weekday: gs-quant's names, the _of spellings gone
            List<Map<String, String>> named = rows(backend, "|#TS{SELECT range(CLOSE['AAA'], 3) AS r, weekday(CLOSE['AAA'])"
                    + " AS wd FROM PRICES ON DAY BY SYMBOL}#");
            assertClose(each(AAA.length, i -> i < 3 ? null : Math.max(AAA[i], Math.max(AAA[i - 1], AAA[i - 2]))
                    - Math.min(AAA[i], Math.min(AAA[i - 1], AAA[i - 2]))), column(named, null, null, "r"), of + "range");
            assertClose(each(AAA.length, i -> (double) (FIRST.plusDays(i).getDayOfWeek().getValue() - 1)),
                    column(named, null, null, "wd"), of + "weekday, 0 Monday");
            assertTrue(rows(backend, "|#TS{SELECT filter(CLOSE['AAA'], 'greater_than', 102) AS f FROM PRICES ON DAY"
                    + " BY SYMBOL}#").stream().allMatch(r -> Double.parseDouble(r.get("f")) <= 102), of + "filter");
        }
        for (String old : List.of("range_of(CLOSE['AAA'], 3)", "first_of(CLOSE['AAA'])", "union_of(CLOSE['AAA'], CLOSE['BBB'])",
                "weekday_of(CLOSE['AAA'])", "filter_values(CLOSE['AAA'])")) {
            assertNotNull(refused("SELECT " + old + " AS x FROM PRICES ON DAY BY SYMBOL", true), old + ": gone");
        }
        // whole is the whole line, a word no window to date takes (D3)
        assertNotNull(refused("SELECT mean(CLOSE['AAA'], whole) AS m FROM PRICES ON DAY BY SYMBOL", true), "mean of whole");
    }

    @Test
    void whatV0RefusesSaysTheFix() throws Exception {
        Map<String, String> refusals = new LinkedHashMap<>();
        refusals.put("SELECT avg(CLOSE, 3) AS m FROM PRICES ON DAY BY SYMBOL WHERE SYMBOL = 'AAA'",
                "put the WHERE with its source");
        refusals.put("SELECT avg(CLOSE, 3) AS m FROM PRICES ON DAY BY SYMBOL FILTER CLOSE > 1",
                "CLOSE isn't a column of the answer (DAY, SYMBOL, m)");
        refusals.put("SELECT avg(CLOSE, 3) AS close FROM PRICES ON DAY BY SYMBOL", "close is a column of the source (R7)");
        refusals.put("SELECT avg(CLOSE, 3) FROM PRICES ON DAY BY SYMBOL", "name the item");
        refusals.put("SELECT avg(CLOSE, 3) m FROM PRICES ON DAY BY SYMBOL", "name an item with AS: ... AS m");
        refusals.put("SELECT avg(CLOSE) AS m FROM PRICES ON DAY BY SYMBOL", "a avg over what? add SAMPLE BY 1w");
        refusals.put("SELECT avg(CLOSE, 0) AS m FROM PRICES ON DAY BY SYMBOL", "a window counts observations");
        // no word of the library: Postgres's window function, refused as the per-point SQL refuses it (S1e: no list of
        // words still to come is kept in core)
        refusals.put("SELECT dense_rank(CLOSE) AS v FROM PRICES ON DAY BY SYMBOL",
                "window function dense_rank requires an OVER clause");
        // an order is a call's last argument's, and only a word that declares one takes it (rank's)
        refusals.put("SELECT avg(CLOSE DESC) AS v FROM PRICES ON DAY BY SYMBOL", "avg has no option order");
        refusals.put("SELECT value_at(CLOSE['AAA']) AS v FROM PRICES ON DAY BY SYMBOL",
                "value_at reads a date: value_at(x, '2024-01-01') (R9)");
        refusals.put("SELECT value_at(CLOSE['AAA'], '2024-13-01') AS v FROM PRICES ON DAY BY SYMBOL",
                "value_at's date is a date");
        refusals.put("SELECT trend(CLOSE['AAA']) AS v FROM PRICES ON DAY BY SYMBOL",
                "trend counts in the query's periods: add SAMPLE BY 1mo (R5)");
        refusals.put("SELECT bollinger_bands(CLOSE['AAA'], 3) AS bb, bb * 2 AS d FROM PRICES ON DAY BY SYMBOL",
                "bb has lower, upper, width: read one, bb.lower (R10)");
        refusals.put("SELECT bollinger_bands(CLOSE['AAA'], 3) * 2 AS d FROM PRICES ON DAY BY SYMBOL",
                "bollinger_bands has lower, upper, width: name it");
        refusals.put("SELECT CLOSE['AAA'] AS a, a.lower AS d FROM PRICES ON DAY BY SYMBOL", "a is one value");
        refusals.put("SELECT bollinger_bands(CLOSE['AAA'], 3) AS bb, bb.middle AS d FROM PRICES ON DAY BY SYMBOL",
                "bb has lower, upper, width, not middle");
        refusals.put("SELECT volatility(CLOSE, 3, 'ln') AS v FROM PRICES ON DAY BY SYMBOL",
                "volatility's type is 'simple', 'log', 'absolute'");
        // gs-quant's volatility(x, w, returns_type, annualization_factor): a fifth is one too many (the catalog's
        // order, the library's widest overload, M3)
        refusals.put("SELECT volatility(CLOSE, 3, 'log', 12, 1) AS v FROM PRICES ON DAY BY SYMBOL",
                "volatility takes 1 line and its parameters w, type, annualization_factor");
        refusals.put("SELECT CLOSE['AAA', 'BBB'] AS a FROM PRICES ON DAY BY SYMBOL", "a subscript names one instrument");
        refusals.put("SELECT close['AAA'] AS a FROM (PRICES WHERE SYMBOL = 'AAA') ON DAY",
                "has no BY: it has no instrument to pick");
        refusals.put("SELECT CLOSE AS c, c['AAA'] AS a FROM PRICES ON DAY BY SYMBOL", "not an item's");
        refusals.put("SELECT CLOSE AS c FROM PRICES ON DAY BY SYMBOL FILTER CLOSE['AAA'] > 1",
                "isn't a column of the answer");
        refusals.put("SELECT q.close - r.close AS d FROM Quote q ON day BY instrument.ticker,"
                + " Quote r ON day BY instrument.sector", "lines by ticker and lines by sector meet on no instrument");
        refusals.put("SELECT returns(CLOSE['AAA'], type => 'ln') AS r FROM PRICES ON DAY BY SYMBOL",
                "type is 'simple', 'log', 'absolute'");
        refusals.put("SELECT avg(CLOSE['AAA'], 3, type => 'log') AS r FROM PRICES ON DAY BY SYMBOL", "avg has no option type");
        refusals.put("SELECT returns(CLOSE['AAA'], type => 'log', 1) AS r FROM PRICES ON DAY BY SYMBOL",
                "an option by place comes before those by name");
        refusals.put("SELECT ln(CLOSE['AAA'] => 1) AS r FROM PRICES ON DAY BY SYMBOL", "an option by name is name => value");
        refusals.put("SELECT avg(CLOSE, 3) AS a FROM PRICES ON DAY BY SYMBOL SAMPLE BY week",
                "SAMPLE BY takes a calendar period");
        refusals.put("SELECT last(CLOSE['AAA']) AS a FROM PRICES ON DAY BY SYMBOL SAMPLE BY 2w",
                "SAMPLE BY takes one calendar period");
        refusals.put("SELECT CLOSE['AAA'] AS a FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1w", "how is CLOSE taken each period");
        refusals.put("SELECT last(CLOSE['AAA']) AS a FROM PRICES ON DAY BY SYMBOL", "last(x) is a period's last value");
        // the instrument named by its value, one way (the user, 2026-11-02): a condition on the BY column refused
        refusals.put("SELECT CLOSE[SYMBOL = 'AAA'] AS a FROM PRICES ON DAY BY SYMBOL",
                "a subscript names its instrument by its value, CLOSE['AAA']: SYMBOL is the BY column (R2)");
        refusals.put("SELECT sum(CLOSE['AAA'] * 2) AS a FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1w",
                "a period's sum is of a source's column");
        refusals.put("SELECT CLOSE AS a FROM PRICES ON DAY BY SYMBOL FILL(LAST)", "FILL is FILL(PREV)");
        refusals.put("SELECT CLOSE AS a FROM PRICES ON DAY BY SYMBOL FILTER a > 0 SAMPLE BY 1w",
                "then FILTER, each once");
        refusals.put("SELECT avg(CLOSE, 3) AS a FROM PRICES ON DAY BY SYMBOL ORDER BY DAY", "#TS has no ORDER BY");
        refusals.put("SELECT CLOSE AS a FROM PRICES ON DAY BY SYMBOL, PRICES ON DAY", "two sources are called PRICES");
        refusals.put("SELECT CLOSE AS a FROM PRICES p ON DAY BY SYMBOL, PRICES r ON DAY", "CLOSE is a column of p and r");
        refusals.put("SELECT avg(CLOSE, 3) AS a FROM PRICES", "a source names its date column");
        refusals.put("SELECT avg(close, 3) AS a FROM Nope ON day", "Nope is not a class or a table of the model");
        refusals.put("SELECT avg(close, 3) AS a FROM Quote ON day BY instrument",
                "instrument is a ts::Instrument: name one of its properties");
        refusals.put("SELECT ticker AS t FROM Instrument ON quotes.day",
                "quotes.day goes through quotes, which holds many");
        refusals.put("SELECT avg(close, 3) AS a FROM Quote ON day BY instrument.tick", "Instrument has no property tick");
        refusals.put("SELECT b AS a, avg(close, 3) AS b FROM Quote ON day BY instrument.ticker", "b is a later item");
        refusals.put("SELECT sum(CLOSE) FILTER (WHERE SYMBOL = 'AAA') AS a FROM PRICES ON DAY",
                "#TS picks a line's rows by its subscript");
        for (Map.Entry<String, String> r : refusals.entrySet()) {
            String message = refused(r.getKey(), true);
            assertTrue(message.contains(r.getValue()), r.getKey() + "\n  refused as: " + message);
            assertTrue(message.startsWith("#TS{...}# at line"), "placed in the island: " + message);
        }
        // a decomposition's cycle no longer than the SAMPLE BY's period: the library's table fails it, refused when
        // planned in its words (S1d)
        String cycle = refused("SELECT trend(last(CLOSE['AAA']), freq => 'week') AS v FROM PRICES ON DAY BY SYMBOL"
                + " SAMPLE BY 1mo", true);
        assertTrue(cycle.contains("a week is no cycle of months"), "the cycle's refusal: " + cycle);
        assertTrue(refused("SELECT avg(CLOSE, 3) AS a FROM PRICES ON DAY", false)
                .contains("#TS uses the timeseries library: add the project timeseries"), "the library is a dependency");
    }

    /** M3: the catalog is the library -- every word over one line the timeseries library defines, its parameters by
     *  place with their kinds and forms, an option's values and its documentation, read from the compiled model. */
    @Test
    void theCatalogIsReadFromTheLibrarysWords() throws java.io.IOException {
        List<com.legend.protocol.ts.TsCatalog.Word> words = com.legend.compiler.spec.TsIslands.catalog(
                Compiler.compileModel(library()));
        Map<String, com.legend.protocol.ts.TsCatalog.Word> byName = new LinkedHashMap<>();
        words.forEach(w -> byName.put(w.name(), w));
        // 81 -> 82 (2026-11-03, A9): rank, each date's place among its instruments (the spec's §6)
        assertEquals(82, words.size(), "the words: " + byName.keySet());
        assertTrue(byName.keySet().stream().noneMatch(n -> n.contains("::")), "the helpers are not words");
        // a word's other spellings are the library's (timeseries::Words.alias, R15): mean's avg, Postgres's stddev and
        // variance; no word is named by one
        assertEquals(List.of("avg"), java.util.Objects.requireNonNull(byName.get("mean")).aliases());
        assertEquals(List.of("stddev"), java.util.Objects.requireNonNull(byName.get("std")).aliases());
        assertEquals(List.of("variance"), java.util.Objects.requireNonNull(byName.get("var")).aliases());
        assertTrue(byName.keySet().stream().noneMatch(List.of("avg", "stddev", "variance")::contains), "an alias is no word");
        for (com.legend.protocol.ts.TsCatalog.Word w : words) {
            assertNotNull(w.doc(), w.name() + " is documented");
            assertTrue(!w.forms().isEmpty(), w.name() + " has forms");
        }
        com.legend.protocol.ts.TsCatalog.Word vol = java.util.Objects.requireNonNull(byName.get("volatility"));
        assertEquals("x line [] [] required; w span [observations, period, all] [] optional; type word [] [simple, log,"
                + " absolute] optional; annualization_factor whole [] [] optional", params(vol));
        assertEquals(4, vol.forms().size(), vol.forms().toString());
        assertEquals("x line [] [] required; w span [observations, period, all, date] [] required", params(java.util.Objects.requireNonNull(
                byName.get("mean"))));
        assertEquals("x line [] [] required; date date [] [] required", params(java.util.Objects.requireNonNull(byName.get("value_at"))));
        assertEquals("x line [] [] required; initial number [] [] optional; from since [] [] optional", params(java.util.Objects
                .requireNonNull(byName.get("index"))));
        assertEquals("x line [] [] required; obs span [observations, period] [] optional; type word [] [simple, log,"
                + " absolute] optional",
                params(java.util.Objects.requireNonNull(byName.get("returns"))));
    }

    private static String params(com.legend.protocol.ts.TsCatalog.Word w) {
        return String.join("; ", w.params().stream().map(p -> p.name() + " " + p.kind() + " " + p.takes() + " "
                + p.words() + " " + (p.required() ? "required" : "optional")).toList());
    }

    // ------------------------------------------------------------------ Show #TS (D12)

    /** The statements the printer is held to: every mark the parser makes, every clause, every source. */
    private static final List<String> PRINTED = List.of(
            "SELECT avg(CLOSE, 3) AS ma3 FROM PRICES ON DAY BY SYMBOL",
            "SELECT mean(CLOSE, 1w) AS m, sum(CLOSE, all) AS s, last(CLOSE, whole) AS l FROM PRICES ON DAY BY SYMBOL",
            "SELECT CLOSE['AAA'] / CLOSE['BBB'] - 1 AS r, returns(CLOSE['AAA'], type => 'log') AS g FROM PRICES ON DAY"
                    + " BY SYMBOL",
            "SELECT CASE WHEN CLOSE > 100 THEN 1 WHEN CLOSE IS NULL THEN NULL ELSE -1 END AS c, -(CLOSE - 100) AS d,"
                    + " (CLOSE + 1) * 2 AS e, CLOSE * (2 + 1) AS f FROM (PRICES WHERE DAY >= DATE '2024-01-22' AND"
                    + " (SYMBOL = 'AAA' OR SYMBOL IN ('BBB'))) ON DAY BY SYMBOL FILTER c > 0",
            "SELECT last(CLOSE['AAA']) AS a, max(CLOSE['BBB']) AS b FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1w AT END",
            "SELECT a.CLOSE['AAA'] - b.CLOSE['BBB'] AS d FROM PRICES a ON DAY BY SYMBOL, (PRICES WHERE DAY >= DATE"
                    + " '2024-01-24') b ON DAY BY SYMBOL FILL(PREV)",
            "WITH q AS (SELECT instrument.ticker AS sym, day, close AS px FROM Quote) SELECT avg(px, 3) AS ma3,"
                    + " px / ma3 - 1 AS dev FROM q ON day BY sym",
            "WITH d AS TS (SELECT CLOSE['AAA'] AS a FROM PRICES ON DAY BY SYMBOL) SELECT last(a) AS w FROM d ON DAY"
                    + " SAMPLE BY 1w",
            "SELECT union(CLOSE['AAA'], CLOSE['BBB']) AS u, 3mo AS z FROM PRICES ON DAY BY SYMBOL");

    @Test
    void aRankIsEachDatesPlaceAmongItsInstruments() throws Exception {
        // the spec's §6 rank(x [DESC]) (A9, 2026-11-03): each date's instruments by x, 1 the first, the parser reading
        // x DESC as the option order => 'descending'; AAA's closes (about 100) are above BBB's (about 50) every day
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> down = rows(backend, "|#TS{SELECT rank(CLOSE DESC) AS r FROM PRICES ON DAY BY SYMBOL}#");
            List<Map<String, String>> up = rows(backend, "|#TS{SELECT rank(CLOSE) AS r FROM PRICES ON DAY BY SYMBOL}#");
            assertEquals(2 * AAA.length, down.size(), backend + ": " + down);
            assertEquals(2 * AAA.length, up.size(), backend + ": " + up);
            for (Map<String, String> row : down) {
                assertEquals("AAA".equals(row.get("SYMBOL")) ? 1 : 2, (int) Double.parseDouble(row.get("r")),
                        backend + ": " + row);
            }
            for (Map<String, String> row : up) {
                assertEquals("AAA".equals(row.get("SYMBOL")) ? 2 : 1, (int) Double.parseDouble(row.get("r")),
                        backend + ": " + row);
            }
        }
    }

    @Test
    void aSeriesIsOneItemCoreReads() {
        // A3 step 2: the one reader -- a formula box's series is one #TS item, read by the query's own parser, printed
        // back as itself, its tree as data (TsTree) for a drawing to walk
        for (String text : List.of("avg(close, 20) AS ma20", "CLOSE['AAA'] / CLOSE['BBB'] - 1 AS r",
                "returns(close, type => 'log') AS g", "sum(volume, 3mo) AS v", "first(close, all) AS f",
                "CASE WHEN close > 100 THEN 1 ELSE -1 END AS c", "px.close AS p",
                "union(close['AAA'], close['BBB']) AS u")) {
            com.legend.protocol.ts.TsAst.Line line = com.legend.protocol.ts.TsParser.item(text);
            assertEquals(text, com.legend.protocol.ts.TsPrinter.print(line), "printed as read");
            assertEquals(text, com.legend.protocol.ts.TsPrinter.print(com.legend.protocol.ts.TsParser.item(
                    com.legend.protocol.ts.TsPrinter.print(line))), "and read back the same");
        }
        Map<String, Object> tree = com.legend.server.TsTree.of(com.legend.protocol.ts.TsParser.item(
                "avg(close['AAA'], 3mo) AS m"));
        assertEquals("m", tree.get("alias"));
        assertEquals("{k=call, name=[avg], args=[{k=pick, of={k=column, parts=[close], at=[1, 5]}, keys=[{k=string,"
                + " value=AAA, at=[1, 11]}], at=[1, 5]}, {k=span, n=3, unit=mo, at=[1, 19]}], star=false, at=[1, 1]}",
                String.valueOf(tree.get("expr")), "the call, its subscript and its span, each in place");
        assertEquals("{k=all, at=[1, 14]}", String.valueOf(((List<?>) ((Map<?, ?>) com.legend.server.TsTree.of(
                com.legend.protocol.ts.TsParser.item("first(close, all)")).get("expr")).get("args")).get(1)),
                "a window of everything marked as the parser marks it");
        com.legend.protocol.sql.SqlTranslationException error = assertThrows(
                com.legend.protocol.sql.SqlTranslationException.class,
                () -> com.legend.protocol.ts.TsParser.item("avg(close, 20) AS a, sum(close) AS b"));
        assertTrue(error.getMessage().contains("one item"), error.getMessage());
        assertEquals(1, error.line());
        com.legend.protocol.sql.SqlTranslationException broken = assertThrows(
                com.legend.protocol.sql.SqlTranslationException.class,
                () -> com.legend.protocol.ts.TsParser.item("avg(close, "));
        assertEquals(1, broken.line(), "an error placed in the text: " + broken.getMessage());
    }

    @Test
    void aQueryPrintedReadsBackAsTheSameText() {
        // D12's exactness: the printed statement, parsed and printed again, is the same text (a fixpoint)
        for (String text : PRINTED) {
            String once = com.legend.protocol.ts.TsPrinter.print(com.legend.protocol.ts.TsParser.parse(text));
            assertEquals(once, com.legend.protocol.ts.TsPrinter.print(com.legend.protocol.ts.TsParser.parse(once)),
                    text);
        }
        assertEquals("SELECT avg(CLOSE, 3) AS ma3 FROM PRICES ON DAY BY SYMBOL", com.legend.protocol.ts.TsPrinter
                .print(com.legend.protocol.ts.TsParser.parse("select avg(CLOSE,3)   as ma3\nfrom PRICES on DAY by"
                        + " SYMBOL")).replace(" as ", " AS "), "the clauses in the language's own words");
    }

    @Test
    void aQueryPrintedAnswersTheSameRows() throws Exception {
        TestBackend backend = TestBackends.platform();
        com.legend.compiler.element.ModelContext model = Compiler.compileModel(model(backend, true));
        for (String text : PRINTED) {
            if (text.contains("3mo AS z")) {
                continue;
            }
            String printed = com.legend.protocol.ts.TsPrinter.print(com.legend.protocol.ts.TsParser.parse(text));
            assertEquals(rows(backend, model, "|#TS{" + text + "}#"), rows(backend, model, "|#TS{" + printed + "}#"),
                    printed);
        }
    }

    /** {@code pure}, a query's text, as the lambda the planner is sent. */
    private static com.legend.protocol.spec.LambdaFunction lambdaOf(String pure) {
        // the tile's query read as lite reads it (its #TS island lite's own), then sent as JSON, as the tile sends it
        return com.legend.protocol.ProtocolReader.lambda(com.legend.protocol.ProtocolEmitter.emitLambda(
                (com.legend.protocol.spec.LambdaFunction) Compiler.parseQuery(pure)));
    }

    @Test
    void aQueryOverGridsIsOneStatementThatAnswersTheSameRows() throws Exception {
        // D12: each grid bound by let, the island reading it -- as one statement, each grid a flat source by WITH
        TestBackend backend = TestBackends.platform();
        com.legend.compiler.element.ModelContext model = Compiler.compileModel(model(backend, true));
        Map<String, String> queries = new LinkedHashMap<>();
        queries.put("a table", "|let px = #>{ts::DB.PRICES}#; #TS{SELECT avg(CLOSE, 3) AS ma3 FROM $px ON DAY BY"
                + " SYMBOL}#;");
        queries.put("a table filtered", "|let px = #>{ts::DB.PRICES}#->filter(r|($r.SYMBOL == 'AAA') ||"
                + " ($r.CLOSE > 100.5 && !$r.DAY->isEmpty())); #TS{SELECT avg(CLOSE, 3) AS ma3 FROM $px ON DAY BY"
                + " SYMBOL}#;");
        queries.put("a class's paths, filtered", "|let q = ts::Quote.all()->project(~[sym: x|$x.instrument.ticker,"
                + " day: x|$x.day, px: x|$x.close])->filter(x|$x.sym->in(['AAA', 'BBB']) && ($x.day >= %2024-01-22));"
                + " #TS{SELECT avg(px, 3) AS ma3, px['AAA'] / px['BBB'] AS r FROM $q ON day BY sym}#;");
        // A6 (2026-11-03): a calculated column is its flat source's item, the Postgres the #SQL reader reads back to
        // Pure's meaning -- a + of text as ||, a / a Float's (3 / 2 is 1.5, never Postgres's integer 1), a Float
        // literal a double, an if a CASE, a later column reading an earlier one, a filter reading one
        queries.put("a table's calculated column", "|let px = #>{ts::DB.PRICES}#->extend(~twice: r|$r.CLOSE->toOne()"
                + " * 2); #TS{SELECT avg(twice, 3) AS m FROM $px ON DAY BY SYMBOL}#;");
        queries.put("a class's calculated columns, filtered by one", "|let q = ts::Quote.all()->project(~[sym:"
                + " x|$x.instrument.ticker, day: x|$x.day, px: x|$x.close])->extend(~[line: x|$x.sym->toOne() + '-x',"
                + " scaled: x|$x.px->toOne() * (3 / 2), sign: x|if($x.px > 100.5, |1.0, |-1.0)])->extend(~[twice: x|"
                + "$x.scaled->toOne() * 2, gap: x|if($x.px < 99.0, |[], |$x.px->toOne() - 99.0)])->filter(x|$x.scaled"
                + " > 150); #TS{SELECT avg(twice, 3) AS m, sign AS s, gap AS g FROM $q ON day BY line}#;");
        com.legend.protocol.ts.TsShow.Types types = com.legend.server.PureV1Api.tsTypes(model);
        Map<String, String> statements = new LinkedHashMap<>();
        for (Map.Entry<String, String> q : queries.entrySet()) {
            com.legend.protocol.ts.TsShow.Shown shown = com.legend.protocol.ts.TsShow.of(lambdaOf(q.getValue()), types);
            assertNull(shown.refused(), q.getKey());
            String statement = java.util.Objects.requireNonNull(shown.statement());
            statements.put(q.getKey(), statement);
            assertTrue(statement.startsWith("WITH "), statement);
            assertEquals(statement, com.legend.protocol.ts.TsPrinter.print(com.legend.protocol.ts.TsParser.parse(
                    statement)), "the statement reads back as itself");
            assertEquals(rows(backend, model, q.getValue()), rows(backend, model, "|#TS{" + statement + "}#"),
                    statement);
        }
        assertEquals("WITH q AS (SELECT instrument.ticker AS sym, day, close AS px FROM ts::Quote WHERE"
                + " instrument.ticker IN ('AAA', 'BBB') AND day >= DATE '2024-01-22') SELECT avg(px, 3) AS ma3,"
                + " px['AAA'] / px['BBB'] AS r FROM q ON day BY sym", statements.get("a class's paths, filtered"),
                "as a person writes it: the class's paths, filtered by them");
        assertEquals("WITH px AS (SELECT *, CLOSE * 2 AS twice FROM ts::DB.PRICES) SELECT avg(twice, 3) AS m FROM px"
                + " ON DAY BY SYMBOL", statements.get("a table's calculated column"), "a table's columns, then its own");
        assertEquals("WITH q AS (SELECT instrument.ticker AS sym, day, close AS px, instrument.ticker || '-x' AS line,"
                + " close * (CAST(3 AS DOUBLE PRECISION) / 2) AS scaled, CASE WHEN close > 100.5 THEN CAST(1.0 AS"
                + " DOUBLE PRECISION) ELSE -CAST(1.0 AS DOUBLE PRECISION) END AS sign, close * (CAST(3 AS DOUBLE"
                + " PRECISION) / 2) * 2 AS twice, CASE WHEN close < 99.0 THEN NULL ELSE close - CAST(99.0 AS DOUBLE"
                + " PRECISION) END AS gap FROM ts::Quote WHERE close * (CAST(3 AS DOUBLE PRECISION) / 2) > 150) SELECT"
                + " avg(twice, 3) AS m, sign AS s, gap AS g FROM q ON day BY line",
                statements.get("a class's calculated columns, filtered by one"), "each over the paths it reads");
        // what the flat form does not say yet is refused by name, never approximated: a window's column, a function
        // not written yet
        for (Map.Entry<String, String> later : Map.of(
                "a window's column", "->extend(over(~SYMBOL, ~DAY->ascending()), ~prev: {p, w, r|$p->lag($r).CLOSE})",
                "calculates by abs", "->extend(~a: r|$r.CLOSE->toOne()->abs())").entrySet()) {
            com.legend.protocol.ts.TsShow.Shown refused = com.legend.protocol.ts.TsShow.of(lambdaOf("|let px ="
                    + " #>{ts::DB.PRICES}#" + later.getValue() + "; #TS{SELECT avg(CLOSE, 3) AS m FROM $px ON DAY BY"
                    + " SYMBOL}#;"), types);
            assertNull(refused.statement(), later.getKey());
            assertTrue(java.util.Objects.requireNonNull(refused.refused()).contains(later.getKey()), refused.refused());
        }
    }
}
