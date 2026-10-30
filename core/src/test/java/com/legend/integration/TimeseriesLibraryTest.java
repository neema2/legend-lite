// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.database.Databases;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DATACUBE'S TIMESERIES LIBRARY ON THE PLATFORM (docs/TIMESERIES_ON_THE_PLATFORM_2026_10_28.md, step B): {@code
 * timeseries}, the timeseries project's Pure file as it is ({@code projects/timeseries/model.pure}: a program, input to the
 * compiler, never the platform's -- docs/WORLD_MAP.md), taken after a model's own sections with no TypeScript in the
 * path. It compiles -- its parse, names, signatures and enumerations; a body generic over relations, as every one of
 * its functions is, is typed at each call over that call's tables (register S112), so here where these tests call one
 * -- and a person's Pure calling its functions answers the same figures on every
 * in-process database (a calendar period's as-of join where the dialect writes one), each worked here as plain
 * arithmetic over the same prices, never by the code under test.
 *
 * <p>Its own target ({@code //core:timeseries_library_test}): it reads DataCube's file, passed as {@code
 * -Dtimeseries.library}.
 */
class TimeseriesLibraryTest {

    private static final LocalDate FIRST = LocalDate.of(2024, 1, 20);

    /** AAA's closes: a price every calendar day for 24 days, so a week back is seven rows back, across a month's end. */
    private static final double[] AAA = prices(24, i -> 100 * (1 + 0.03 * Math.sin(i / 2.0) + 0.004 * i));

    /** BBB's: another line in the same table, so every window is its own line's. */
    private static final double[] BBB = prices(24, i -> 50 * (1 + 0.02 * Math.cos(i / 1.7) + 0.002 * i));

    private static double[] prices(int n, IntFunction<Double> f) {
        double[] out = new double[n];
        for (int i = 0; i < n; i++) {
            out[i] = Math.round(f.apply(i) * 100) / 100.0;
        }
        return out;
    }

    private static String library() throws java.io.IOException {
        return Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
    }

    /** The prices' table: a store of the model's own, which each test's session is given by the platform's own DDL. */
    private static final String STORE = """
            ###Relational
            Database ts::DB ( Table PRICES ( SYMBOL VARCHAR(8), DAY DATE, CLOSE DOUBLE ) )
            """;

    private static final String PRICES = "#>{ts::DB.PRICES}#";

    /** The table, the library's own section, and a runtime on {@code backend}'s session, as it declares it. */
    private static String model(TestBackend backend) throws java.io.IOException {
        return STORE + """
                ###Connection
                RelationalDatabaseConnection ts::Conn
                {
                  store: ts::DB;
                  %s
                }
                ###Runtime
                Runtime ts::RT { mappings: []; connections: [ ts::DB: [ c: ts::Conn ] ]; }
                """.formatted(backend.connectionBody()) + library();
    }

    /** {@code c} given the prices: the table as {@code backend}'s dialect writes it, and its rows bound. */
    private static void seed(TestBackend backend, Connection c) throws java.sql.SQLException {
        com.legend.model.DatabaseDefinition.TableDefinition table = Compiler.parseModel(STORE).elements().stream()
                .filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).findFirst().orElseThrow();
        try (java.sql.Statement s = c.createStatement()) {
            s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(table, null)));
        }
        String target = backend.dialect().storedName(table.name(), table.quoted());
        try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO " + target + " VALUES (?, ?, ?)")) {
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
    }

    /** {@code query}'s rows on {@code backend}, each by its column's name: a value's text, or null where it has none. */
    private static List<Map<String, String>> rows(TestBackend backend, String query) throws Exception {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model(backend)), query)
                .executionPlan("ts::RT");
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

    /** The column {@code name} of {@code symbol}'s rows, in date order: a number, or null. */
    private static Double[] column(List<Map<String, String>> rows, String symbol, String name) {
        return rows.stream().filter(r -> symbol.equals(r.get("SYMBOL")))
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
    void theLibraryCompilesOnThePlatform() throws Exception {
        // its elements: a body generic over relations has no check of its own (Compiler.compileAllBodies, S112)
        List<?> errors = Compiler.compileErrors(model(TestBackends.platform()));
        assertEquals(List.of(), errors, "the library compiles");
    }

    @Test
    void aPersonsPureOverTheTableAnswersAsArithmeticOnEveryDatabase() throws Exception {
        String query = "|" + PRICES + """
                  ->timeseries::mean(over(~SYMBOL, [~DAY->ascending()], rows(-2, 0)), ~CLOSE, 3, ~m)
                  ->timeseries::returns(over(~SYMBOL, [~DAY->ascending()]), ~CLOSE, 1, timeseries::Returns.simple, ~r)
                  ->timeseries::returns(over(~SYMBOL, [~DAY->ascending()]), ~CLOSE, 1, timeseries::Returns.log, ~lr)
                  ->extend(~py:x|252)
                  ->timeseries::volatility(over(~SYMBOL, [~DAY->ascending()], rows(-4, 0)), ~r, 5, ~py, ~v)
                  ->timeseries::periodIndex(~DAY, timeseries::Grain.month, ~mi)
                  ->select(~[SYMBOL, DAY, m, r, lr, v, mi])
                  ->sort([~SYMBOL->ascending(), ~DAY->ascending()])""";
        List<TestBackend> ran = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> rows = rows(backend, query);
            for (Map.Entry<String, double[]> line : Map.of("AAA", AAA, "BBB", BBB).entrySet()) {
                double[] x = line.getValue();
                int n = x.length;
                String of = backend + " " + line.getKey() + " ";
                assertClose(each(n, i -> i < 2 ? null : (x[i] + x[i - 1] + x[i - 2]) / 3),
                        column(rows, line.getKey(), "m"), of + "the mean of three");
                double[] r = new double[n];
                for (int i = 1; i < n; i++) {
                    r[i] = x[i] / x[i - 1] - 1;
                }
                assertClose(each(n, i -> i < 1 ? null : r[i]), column(rows, line.getKey(), "r"), of + "simple returns");
                assertClose(each(n, i -> i < 1 ? null : Math.log(x[i]) - Math.log(x[i - 1])),
                        column(rows, line.getKey(), "lr"), of + "log returns");
                // the first return is none, so the window of five holds five from its sixth row
                assertClose(each(n, i -> i < 5 ? null : sampleStd(r, i - 4, i) * Math.sqrt(252) * 100),
                        column(rows, line.getKey(), "v"), of + "volatility");
                assertClose(each(n, i -> {
                    LocalDate d = FIRST.plusDays(i);
                    return (double) (d.getYear() * 12 + d.getMonthValue() - 1);
                }), column(rows, line.getKey(), "mi"), of + "the month's number");
            }
            ran.add(backend);
        }
        assertTrue(ran.size() >= 3, "on every in-process database: " + ran);
    }

    /**
     * A calendar period back (lagPeriod, and every *Period function on it) is an as-of join: it answers where the
     * database's dialect writes one, and is refused by name, never answered wrong, where it writes none. On this test's
     * first run (2026-10-28) the platform's database answered and the other two in-process ones were refused: the
     * portable rewrite (AsOfAsPick) is one dialect's alone -- docs/TIMESERIES_ON_THE_PLATFORM_2026_10_28.md step E.
     */
    @Test
    void aWeeksReturnsAnswerWhereTheDialectWritesAnAsOfJoin() throws Exception {
        String query = "|" + PRICES + """
                  ->timeseries::returnsPeriod(~SYMBOL, ~DAY, ~CLOSE, 1, meta::pure::functions::date::DurationUnit.WEEKS, timeseries::Returns.simple, ~wr)
                  ->select(~[SYMBOL, DAY, wr])
                  ->sort([~SYMBOL->ascending(), ~DAY->ascending()])""";
        List<TestBackend> answered = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> rows;
            try {
                rows = rows(backend, query);
            } catch (com.legend.sql.dialect.DialectCapability e) {
                assertTrue(e.getMessage().contains("asOfJoin") || e.getMessage().contains("as-of join"),
                        backend + ": refused for its as-of join, not otherwise: " + e.getMessage());
                continue;
            }
            for (Map.Entry<String, double[]> line : Map.of("AAA", AAA, "BBB", BBB).entrySet()) {
                double[] x = line.getValue();
                assertClose(each(x.length, i -> i < 7 ? null : x[i] / x[i - 7] - 1), column(rows, line.getKey(), "wr"),
                        backend + " " + line.getKey() + " a week's returns, a date against its value seven days before");
            }
            answered.add(backend);
        }
        assertTrue(answered.stream().anyMatch(b -> b.type() == Databases.PLATFORM),
                "the platform's own database answers: " + answered);
    }

    @Test
    void aColumnWithGapsIsFilledByStepZeroAndTimeOnEveryDatabase() throws Exception {
        double above = 102;
        String query = "|" + PRICES + """
                  ->filter(x|$x.SYMBOL == 'AAA')
                  ->extend(~[t:x|dateDiff(%%1970-01-01, $x.DAY->toOne(), meta::pure::functions::date::DurationUnit.DAYS), g:x|if($x.CLOSE > %s, |$x.CLOSE, |[])])
                  ->timeseries::fillStep(~SYMBOL, ~DAY, ~g, ~g, ~st)
                  ->timeseries::fillZero(~g, ~z)
                  ->timeseries::fillTime(~SYMBOL, ~DAY, ~g, ~t, ~t, ~g, ~tm)
                  ->select(~[SYMBOL, DAY, st, z, tm])
                  ->sort([~DAY->ascending()])""".formatted(above);
        int n = AAA.length;
        Double[] g = each(n, i -> AAA[i] > above ? AAA[i] : null);
        List<Integer> has = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (g[i] != null) {
                has.add(i);
            }
        }
        assertTrue(has.size() >= 3 && has.size() < n && has.get(0) > 0, "gaps to fill, before the first value too: " + has);
        IntFunction<Integer> lastAt = i -> has.stream().filter(j -> j <= i).reduce((a, b) -> b).orElse(null);
        IntFunction<Integer> nextAt = i -> has.stream().filter(j -> j >= i).findFirst().orElse(null);
        Double[] step = each(n, i -> g[lastAt.apply(i) == null ? has.get(0) : lastAt.apply(i)]);
        Double[] zero = each(n, i -> g[i] == null ? 0.0 : g[i]);
        Double[] time = each(n, i -> {
            Integer a = lastAt.apply(i);
            Integer b = nextAt.apply(i);
            if (a == null || b == null) {
                return null;
            }
            return a.equals(b) ? g[a] : g[a] + (g[b] - g[a]) * (i - a) / (double) (b - a);
        });
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> rows = rows(backend, query);
            assertClose(step, column(rows, "AAA", "st"), backend + " step: its last value, before its first its first");
            assertClose(zero, column(rows, "AAA", "z"), backend + " zero");
            assertClose(time, column(rows, "AAA", "tm"), backend + " time: linear in days between its values before and after");
        }
    }
}
