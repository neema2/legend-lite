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

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static Double mean3(double[] x, int i) {
        return i < 2 ? null : (x[i] + x[i - 1] + x[i - 2]) / 3;
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
              ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-2, 0)), ~close, 3, ~ma3)
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
                  ->timeseries::mean(over(~[sym], [~day->ascending()], rows(-2, 0)), ~px, 3, ~ma3)
                  ->extend(~dev: r|if($r.px->isEmpty() || $r.ma3->isEmpty(), |[], |$r.px->toOne() / $r.ma3->toOne() - 1))
                  ->select(~[day, sym, ma3, dev])
                  ->sort([~day->ascending(), ~sym->ascending()])""", 2);
    }

    @Test
    void aTableAFilteredSourceAndAVariableAreRelations() throws Exception {
        // R1.3 and R1.4: a table by its simple name, a source's WHERE, a relation of the enclosing Pure
        String pure = """
                ->timeseries::mean(over(~[SYMBOL], [~DAY->ascending()], rows(-2, 0)), ~CLOSE, 3, ~ma3)
                  ->select(~[DAY, SYMBOL, ma3])
                  ->sort([~DAY->ascending(), ~SYMBOL->ascending()])""";
        same("SELECT avg(CLOSE, 3) AS ma3 FROM PRICES ON DAY BY SYMBOL", "|#>{ts::DB.PRICES}#" + pure, 2);
        same("SELECT avg(CLOSE, 3) AS ma3 FROM (PRICES WHERE SYMBOL = 'AAA') ON DAY BY SYMBOL",
                "|#>{ts::DB.PRICES}#->filter(r|$r.SYMBOL == 'AAA')" + pure, 2);
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> got = rows(backend, """
                    |let p = #>{ts::DB.PRICES}#->filter(r|$r.SYMBOL == 'BBB');
                    #TS{SELECT avg(CLOSE, 3) AS ma3 FROM $p ON DAY BY SYMBOL}#;""");
            assertClose(each(BBB.length - 2, i -> mean3(BBB, i + 2)), column(got, null, null, "ma3"),
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
                  ->timeseries::mean(over(~[__ts_line], [~day->ascending()], rows(-2, 0)), ~close, 3, ~ma3)
                  ->timeseries::diff(over(~[__ts_line], [~day->ascending()]), ~close, 2, ~d2)
                  ->select(~[day, ma3, d2])
                  ->filter(r|$r.ma3->isNotEmpty() && ($r.day->toOne() < %2024-01-29))
                  ->sort([~day->ascending()])""", 1);
        for (Map.Entry<TestBackend, List<Map<String, String>>> a : answers.entrySet()) {
            // the 22nd's mean is the first full one; the 28th the last before the 29th
            assertClose(each(7, i -> mean3(AAA, i + 2)), column(a.getValue(), null, null, "ma3"),
                    a.getKey() + ": the mean of three, from the third day to the 28th");
            assertClose(each(7, i -> AAA[i + 2] - AAA[i]), column(a.getValue(), null, null, "d2"),
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
                  ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-1, 0)), ~__ts_1, 2, ~mr)
                  ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-2, 0)), ~close, 3, ~__ts_2)
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
            assertClose(each(x.length - 2, i -> x[i + 2] / mean3(x, i + 2) - 1),
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
            assertClose(each(AAA.length - 2, i -> mean3(AAA, i + 2)), column(one, null, null, "ma"),
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
            assertClose(each(sums.length, i -> i < 1 ? null : sums[i] + sums[i - 1]), column(got, null, null, "two"),
                    backend + ": a window of two weeks");
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
        // R6: no row on the 20th and the 21st, where neither has a value
        assertClose(days.stream().skip(2).map(i -> {
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
        assertClose(java.util.stream.IntStream.range(2, days.size()).mapToObj(j ->
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
                    + " change(CLOSE['AAA']) AS ch, percent_change(CLOSE['AAA']) AS pc, range_of(CLOSE['AAA'], 3) AS rg,"
                    + " volatility(CLOSE['AAA'], 3) AS vol FROM PRICES ON DAY BY SYMBOL}#");
            String of = backend + ": ";
            assertClose(each(x.length, i -> x[i] / x[0] * 100), column(got, null, null, "rb"), of + "rebased to 100");
            assertClose(each(x.length, i -> x[i] - x[0]), column(got, null, null, "ch"), of + "the change from the first");
            assertClose(each(x.length, i -> x[i] / x[0] - 1), column(got, null, null, "pc"), of + "the % change");
            assertClose(each(x.length, i -> i < 2 ? null : Math.max(x[i], Math.max(x[i - 1], x[i - 2]))
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
            Double[] mean = each(AAA.length - 2, i -> mean3(AAA, i + 2));
            Double[] std = each(AAA.length - 2, i -> sampleStd(AAA, i, i + 2));
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
                "exponential_spread_volatility", "geometrically_aggregate", "smooth_spikes", "consecutive", "first_of",
                "last_of", "day_of", "month_of", "year_of", "quarter_of", "weekday_of")) {
            calls.add(f + "(" + x + ")");
        }
        for (String f : List.of("range_of", "zscores", "volatility", "sharpe_ratio", "rsi", "smoothed_moving_average",
                "percentiles", "median", "product", "vol_swap_volatility")) {
            calls.add(f + "(" + x + ", 3)");
        }
        for (String f : List.of("range_of", "zscores", "volatility", "sharpe_ratio", "median", "drawdown")) {
            calls.add(f + "(" + x + ", 1w)");
        }
        for (String method : List.of("intersect", "nan", "zero", "step")) {
            calls.add("interpolate(" + x + ", " + y + ", '" + method + "')");
        }
        calls.addAll(List.of("value_at(" + x + ", '2024-01-25')", "value_at(" + x + ", DATE '2024-01-01')",
                "ohlc(" + x + ", " + y + ", " + x + ", " + y + ")", "ohlc(" + x + ", " + x + ", " + x + ", " + x + ")"));
        calls.addAll(List.of("macd(" + x + ", 2, 4, 1)", "percentile(" + x + ", 50, 3)", "dickey_fuller(" + x + ", 5)",
                "half_life(" + x + ", 5)", "filter_values(" + x + ", 'less_than', 100)",
                "volatility(" + x + ", 3, type => 'log')", "smooth_spikes(" + x + ", 0.5, 'absolute')"));
        for (String f : List.of("correlation", "beta", "cov", "alpha", "r_squared", "hedge_ratio", "residual",
                "tracking_error", "information_ratio", "up_capture", "down_capture", "vwap", "donchian_channels")) {
            calls.add(f + "(" + x + ", " + y + ", 3)");
            calls.add(f + "(" + x + ", " + y + ", 1w)");
        }
        for (String f : List.of("obv", "total_return", "excess_returns", "compare", "union_of", "append", "prepend")) {
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
                  ->timeseries::mean(over(~[ticker], [~day->ascending()], rows(-2, 0)), ~close, 3, ~ma3)
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
            assertClose(each(AAA.length - 2, i -> mean3(AAA, i + 2)), column(tech.get(backend), null, null, "ma3"),
                    backend + ": the TECH instrument's line alone, where its mean has a value (R6)");
            assertEquals(AAA.length + BBB.length - 4, both.get(backend).size(), backend + ": both sectors' lines");
        }
        assertTrue(refused("SELECT avg(close, 3) AS ma3 FROM (Quote WHERE instrument.sector = 'TEK') ON day", true)
                .contains("'TEK' is not a value of ts::Sector: TECH, ENERGY (R13)"), "a value the enumeration has not");
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
        refusals.put("SELECT rank(CLOSE) AS v FROM PRICES ON DAY BY SYMBOL",
                "rank is one of the catalog's (§6) still to come");
        refusals.put("SELECT value_at(CLOSE['AAA']) AS v FROM PRICES ON DAY BY SYMBOL",
                "value_at reads a date: value_at(x, '2024-01-01') (R9)");
        refusals.put("SELECT value_at(CLOSE['AAA'], '2024-13-01') AS v FROM PRICES ON DAY BY SYMBOL",
                "value_at's date is a date");
        refusals.put("SELECT trend(CLOSE['AAA']) AS v FROM PRICES ON DAY BY SYMBOL",
                "trend counts its cycle in the query's periods: add SAMPLE BY 1mo (R5)");
        refusals.put("SELECT trend(last(CLOSE['AAA']), freq => 'week') AS v FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1mo",
                "trend: a week is no cycle of months");
        refusals.put("SELECT bollinger_bands(CLOSE['AAA'], 3) AS bb, bb * 2 AS d FROM PRICES ON DAY BY SYMBOL",
                "bb has lower, upper, width: read one, bb.lower (R10)");
        refusals.put("SELECT bollinger_bands(CLOSE['AAA'], 3) * 2 AS d FROM PRICES ON DAY BY SYMBOL",
                "bollinger_bands has lower, upper, width: name it");
        refusals.put("SELECT CLOSE['AAA'] AS a, a.lower AS d FROM PRICES ON DAY BY SYMBOL", "a is one value");
        refusals.put("SELECT bollinger_bands(CLOSE['AAA'], 3) AS bb, bb.middle AS d FROM PRICES ON DAY BY SYMBOL",
                "bb has lower, upper, width, not middle");
        refusals.put("SELECT volatility(CLOSE, 3, 'ln') AS v FROM PRICES ON DAY BY SYMBOL",
                "volatility's type is 'simple', 'log', 'absolute'");
        refusals.put("SELECT volatility(CLOSE, 3, 'log', 1) AS v FROM PRICES ON DAY BY SYMBOL",
                "volatility takes 1 line and its parameters w, type");
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
        refusals.put("SELECT sum(CLOSE['AAA'] * 2) AS a FROM PRICES ON DAY BY SYMBOL SAMPLE BY 1w",
                "a period's sum is of a source's column");
        refusals.put("SELECT CLOSE AS a FROM PRICES ON DAY BY SYMBOL FILL(LAST)", "FILL is FILL(PREV)");
        refusals.put("SELECT CLOSE AS a FROM PRICES ON DAY BY SYMBOL FILTER a > 0 SAMPLE BY 1w",
                "then FILTER, each once");
        refusals.put("SELECT avg(CLOSE, 3m) AS a FROM PRICES ON DAY BY SYMBOL", "m is minutes or months");
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
        assertTrue(refused("SELECT avg(CLOSE, 3) AS a FROM PRICES ON DAY", false)
                .contains("#TS uses the timeseries library: add the project timeseries"), "the library is a dependency");
    }
}
