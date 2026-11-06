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
            Database ts::DB
            (
              Table PRICES ( SYMBOL VARCHAR(8), DAY DATE, CLOSE DOUBLE )
              Table CAL ( SYMBOL VARCHAR(8), DAY DATE, DT TIMESTAMP, CLOSE DOUBLE, V2 DOUBLE )
            )
            """;

    /** CAL's CLOSE of {@code symbol} on its {@code i}-th day. */
    private static double calClose(String symbol, int i) {
        double k = symbol.equals("CCC") ? 100 : 250;
        return Math.round(100 * k * (1 + 0.1 * Math.sin(i / 17.0) + 0.0005 * i)) / 100.0;
    }

    /** CAL's V2 of {@code symbol} on its {@code i}-th day. */
    private static double calV2(String symbol, int i) {
        double k = symbol.equals("CCC") ? 1 : 2.5;
        return Math.round(100 * (50 + 7 * Math.cos(i / (9.0 * k)) + 0.01 * i)) / 100.0;
    }

    /** CAL's days: every business day of 2023 and 2024 -- month ends, a leap day, weekends and a year's end -- each
     *  twice, a date ({@code DAY}) and the same day's midnight ({@code DT}). */
    private static final List<LocalDate> CAL_DAYS = java.util.stream.Stream.iterate(LocalDate.of(2023, 1, 2), d -> d.plusDays(1))
            .takeWhile(d -> !d.isAfter(LocalDate.of(2024, 12, 31)))
            .filter(d -> d.getDayOfWeek() != java.time.DayOfWeek.SATURDAY && d.getDayOfWeek() != java.time.DayOfWeek.SUNDAY)
            .toList();

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
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(STORE).elements().stream()
                .filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        try (java.sql.Statement s = c.createStatement()) {
            for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
        }
        com.legend.model.DatabaseDefinition.TableDefinition cal = tables.get(1);
        try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO "
                + backend.dialect().storedName(cal.name(), cal.quoted()) + " VALUES (?, ?, ?, ?, ?)")) {
            for (String symbol : List.of("CCC", "DDD")) {
                for (int i = 0; i < CAL_DAYS.size(); i++) {
                    ps.setString(1, symbol);
                    ps.setObject(2, CAL_DAYS.get(i));
                    // a date-time as text the database casts, as lite's own loader gives one (setup.RowLoad), its ISO
                    // form with a space -- not a driver's LocalDateTime text: SQLite's driver wrote '2023-01-02T00:00',
                    // whose 'T' sorts after a bound's space (the user, 2026-11-06, W10: "Seed canonical text")
                    ps.setString(3, CAL_DAYS.get(i).atStartOfDay().format(
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", java.util.Locale.ROOT)));
                    ps.setDouble(4, calClose(symbol, i));
                    ps.setDouble(5, calV2(symbol, i));
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
        com.legend.model.DatabaseDefinition.TableDefinition table = tables.get(0);
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
        return rows(backend, Compiler.compileModel(model(backend)), query);
    }

    /** {@link #rows(TestBackend, String)} over a model already compiled. */
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

    /** The column {@code name} of {@code symbol}'s rows, in date order: a number, or null. */
    private static Double[] column(List<Map<String, String>> rows, String symbol, String name) {
        return rows.stream().filter(r -> symbol.equals(r.get("SYMBOL")))
                .map(r -> r.get(name) == null ? null : Double.valueOf(r.get(name))).toArray(Double[]::new);
    }

    private static void assertClose(Double[] want, Double[] got, String what) {
        assertClose(want, got, what, 1e-9);
    }

    private static void assertClose(Double[] want, Double[] got, String what, double relative) {
        assertEquals(want.length, got.length, what + ": the rows");
        for (int i = 0; i < want.length; i++) {
            if (want[i] == null) {
                assertNull(got[i], what + " at " + i);
            } else {
                assertNotNull(got[i], what + " at " + i);
                assertEquals(want[i], got[i], relative * Math.max(1, Math.abs(want[i])), what + " at " + i);
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
    void aRowsSeveralColumnsAreOneListThatIsEmptyWhereNoneHasAValue() throws Exception {
        // read(row, ~[a, b]) (2026-11-02): a row's columns as one list, empty where no column has a value -- the
        // library's someValued over any number of columns, on every database ([$x.m, $x.r] is refused upstream: a
        // literal of several elements takes each [1])
        String query = "|" + PRICES + """
                  ->timeseries::mean(over(~SYMBOL, [~DAY->ascending()], rows(-2, 0)), ~CLOSE, 3, ~m)
                  ->timeseries::returns(over(~SYMBOL, [~DAY->ascending()]), ~CLOSE, 1, timeseries::Returns.simple, ~r)
                  ->filter(x|$x->read(~[m, r])->isNotEmpty())
                  ->select(~[SYMBOL, DAY, m, r])
                  ->sort([~SYMBOL->ascending(), ~DAY->ascending()])""";
        for (TestBackend backend : TestBackends.inProcess()) {
            List<Map<String, String>> rows = rows(backend, query);
            assertEquals(2 * (AAA.length - 1), rows.size(), backend + ": each line's first row (no mean, no return)"
                    + " dropped, every other kept");
        }
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
     * A CALENDAR WINDOW OF A LINE OF DATES IS ONE WINDOW FRAME (2026-11-04): windowPeriod and pairPeriod over a date
     * column are a frame over the date's period key (timeseries::periodKey), where over a date-time column they are the
     * line joined to itself on (date - n unit, date]. Every statistic of the dates' frame is the window's own, worked here
     * from (date - n unit, date] as java.time reads it (minusMonths clamps a month's end, as adjust does), on every
     * in-process database, for months across their ends and a leap day, a year, days and weeks; and on the platform's
     * own database the date-times' join answers the same. (SQLite's join of date-times holds a window's first day too,
     * found here: the date-times' defect, not the frame's.)
     */
    @Test
    void aCalendarWindowOfDatesIsTheWindowAsAFrame() throws Exception {
        List<String> stats = List.of("mean", "sum", "min", "max", "range", "std", "var", "count");
        List<String> pairStats = List.of("corr", "cov");
        Object[][] windows = {{3, "MONTHS"}, {6, "MONTHS"}, {1, "YEARS"}, {10, "DAYS"}, {2, "WEEKS"}};
        List<TestBackend> ran = new ArrayList<>();
        for (TestBackend backend : TestBackends.inProcess()) {
            com.legend.compiler.element.ModelContext model = Compiler.compileModel(model(backend));
            for (Object[] w : windows) {
                int n = (Integer) w[0];
                String unit = (String) w[1];
                String window = n + ", meta::pure::functions::date::DurationUnit." + unit;
                // every statistic of the dates in one query: a frame reads its line once
                StringBuilder frames = new StringBuilder("|#>{ts::DB.CAL}#");
                for (String stat : stats) {
                    frames.append("->timeseries::windowPeriod(~SYMBOL, ~DAY, ~CLOSE, ").append(window)
                            .append(", timeseries::Statistic.").append(stat).append(", ~").append(stat).append(')');
                }
                for (String stat : pairStats) {
                    frames.append("->timeseries::pairPeriod(~SYMBOL, ~DAY, ~CLOSE, ~V2, ").append(window)
                            .append(", timeseries::PairStatistic.").append(stat).append(", ~").append(stat).append(')');
                }
                List<Map<String, String>> got = rows(backend, model, frames + "->sort([~SYMBOL->ascending(), ~DAY->ascending()])");
                assertEquals(2 * CAL_DAYS.size(), got.size(), backend + " " + window + ": the rows");
                for (String stat : java.util.stream.Stream.concat(stats.stream(), pairStats.stream()).toList()) {
                    for (String symbol : List.of("CCC", "DDD")) {
                        // a correlation and a covariance to 1e-7: SQLite has neither, and lite writes each there from
                        // running sums (8e-9 apart from the two-pass figure on a window of three, measured here)
                        assertClose(calWindow(symbol, stat, n, unit), column(got, symbol, stat),
                                backend + " " + symbol + " " + stat + " over " + window,
                                pairStats.contains(stat) ? 1e-7 : 1e-9);
                    }
                }
                // the join of date-times, one statistic a query (a join reads its line three times), on every database
                // (SQLite's since its date-times are seeded as lite writes them, W10 2026-11-06)
                {
                    for (String stat : stats) {
                        List<Map<String, String>> joined = rows(backend, model, "|#>{ts::DB.CAL}#->timeseries::windowPeriod(~SYMBOL, ~DT, ~CLOSE, "
                                + window + ", timeseries::Statistic." + stat + ", ~" + stat + ")->sort([~SYMBOL->ascending(), ~DAY->ascending()])");
                        for (String symbol : List.of("CCC", "DDD")) {
                            assertClose(column(joined, symbol, stat), column(got, symbol, stat),
                                    backend + " " + symbol + " " + stat + " over " + window + ": the join's answer");
                        }
                    }
                }
            }
            ran.add(backend);
        }
        assertTrue(ran.size() >= 3, "on every in-process database: " + ran);
    }

    /** {@code stat} of {@code symbol}'s CAL values over (date - n unit, date] at each of its days, as java.time reads the
     *  window: none where the statistic has none (a deviation of one value, a correlation of no variation). */
    private static Double[] calWindow(String symbol, String stat, int n, String unit) {
        Double[] out = new Double[CAL_DAYS.size()];
        for (int i = 0; i < out.length; i++) {
            LocalDate d = CAL_DAYS.get(i);
            LocalDate lo = switch (unit) {
                case "MONTHS" -> d.minusMonths(n);
                case "YEARS" -> d.minusYears(n);
                case "WEEKS" -> d.minusWeeks(n);
                case "DAYS" -> d.minusDays(n);
                default -> throw new IllegalArgumentException(unit);
            };
            List<double[]> w = new ArrayList<>();
            for (int j = 0; j <= i; j++) {
                if (CAL_DAYS.get(j).isAfter(lo)) {
                    w.add(new double[] {calClose(symbol, j), calV2(symbol, j)});
                }
            }
            int m = w.size();
            double sx = 0;
            double sy = 0;
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            for (double[] p : w) {
                sx += p[0];
                sy += p[1];
                min = Math.min(min, p[0]);
                max = Math.max(max, p[0]);
            }
            double mx = sx / m;
            double my = sy / m;
            double vxx = 0;
            double vyy = 0;
            double vxy = 0;
            for (double[] p : w) {
                vxx += (p[0] - mx) * (p[0] - mx);
                vyy += (p[1] - my) * (p[1] - my);
                vxy += (p[0] - mx) * (p[1] - my);
            }
            out[i] = switch (stat) {
                case "mean" -> mx;
                case "sum" -> sx;
                case "min" -> min;
                case "max" -> max;
                case "range" -> max - min;
                case "count" -> (double) m;
                case "var" -> m < 2 ? null : vxx / (m - 1);
                case "std" -> m < 2 ? null : Math.sqrt(vxx / (m - 1));
                case "cov" -> m < 2 ? null : vxy / (m - 1);
                case "corr" -> m < 2 || vxx == 0 || vyy == 0 ? null : vxy / Math.sqrt(vxx * vyy);
                default -> throw new IllegalArgumentException(stat);
            };
        }
        return out;
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
