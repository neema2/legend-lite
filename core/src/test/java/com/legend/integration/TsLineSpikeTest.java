// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SPIKE (2026-10-31, the user: "Do it"; the evidence of docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, a
 * manual target): the language's functions as line-level Pure -- a line is
 * {@code Relation<(date:StrictDate, key:String, value:Number)>}, one value per date per instrument -- called the way one
 * generic rule would call them, against today's #TS (TsFunctions' per-function Java) on the same rows: the answers the
 * same, and the SQL's size, its joins and its run time side by side; then whether a line may be dated by date-times.
 */
class TsLineSpikeTest {

    private static final String LINE = "Relation<(date:StrictDate, key:String, value:Number)>[1]";

    private static final String STORE = """
            ###Relational
            Database ts::DB
            (
              Table PRICES ( SYMBOL VARCHAR(8), DAY DATE, CLOSE DOUBLE, VOLUME DOUBLE )
              Table STAMPED ( SYMBOL VARCHAR(8), AT TIMESTAMP, CLOSE DOUBLE )
            )
            """;

    /** The line-level functions: each a function of lines to a line, its plumbing (the window over the instrument,
     *  ordered by date; its ramp; its periods a year) written once, here, in Pure. Each frame is sized by the function's
     *  parameter, {@code rows(-($w - 1), 0)}: refused before C1 (the note's G1), the database's to compute since. */
    private static final String LINES = """
            ###Pure
            function timeseries::line::volatility(x: %1$s, w: Integer[1]): %1$s
            {
              $x->extend(~ord:r|%%1970-01-01->dateDiff($r.date, meta::pure::functions::date::DurationUnit.DAYS))
                ->timeseries::periodsPerYear(~key, ~ord, meta::pure::functions::date::DurationUnit.DAYS, ~py)
                ->timeseries::returns(over(~[key], [~date->ascending()]), ~value, 1, timeseries::Returns.simple, ~r)
                ->timeseries::volatility(over(~[key], [~date->ascending()], rows(-($w - 1), 0)), ~r, if($w < 2, |2, |$w), ~py, ~v)
                ->select(~[date, key, v])->rename(~v, ~value)
            }

            function timeseries::line::volatility(x: %1$s, n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1]): %1$s
            {
              $x->extend(~ord:r|%%1970-01-01->dateDiff($r.date, meta::pure::functions::date::DurationUnit.DAYS))
                ->timeseries::periodsPerYear(~key, ~ord, meta::pure::functions::date::DurationUnit.DAYS, ~py)
                ->timeseries::returns(over(~[key], [~date->ascending()]), ~value, 1, timeseries::Returns.simple, ~r)
                ->timeseries::volatilityPeriod(~key, ~date, ~value, ~r, $n, $unit, ~py, ~v)
                ->select(~[date, key, v])->rename(~v, ~value)
            }

            function timeseries::line::beta(x: %1$s, y: %1$s, w: Integer[1]): %1$s
            {
              let a = $x->timeseries::returns(over(~[key], [~date->ascending()]), ~value, 1, timeseries::Returns.simple, ~ra)
                ->select(~[date, key, ra]);
              let b = $y->timeseries::returns(over(~[key], [~date->ascending()]), ~value, 1, timeseries::Returns.simple, ~rb)
                ->select(~[date, rb])->rename(~date, ~bdate);
              $a->join($b, meta::pure::functions::relation::JoinKind.INNER, {l, r|$l.date == $r.bdate})
                ->timeseries::beta(over(~[key], [~date->ascending()], rows(-($w - 1), 0)), ~ra, ~rb, if($w < 2, |2, |$w), ~v)
                ->select(~[date, key, v])->rename(~v, ~value);
            }

            function timeseries::line::vwap(price: %1$s, volume: %1$s, w: Integer[1]): %1$s
            {
              $price->join($volume->rename(~date, ~vdate)->rename(~key, ~vkey)->rename(~value, ~vol),
                  meta::pure::functions::relation::JoinKind.INNER, {l, r|($l.date == $r.vdate) && ($l.key == $r.vkey)})
                ->timeseries::tradedValue(~value, ~vol, ~traded)
                ->timeseries::vwap(over(~[key], [~date->ascending()], rows(-($w - 1), 0)), ~value, ~traded, ~vol, $w, ~v)
                ->select(~[date, key, v])->rename(~v, ~value)
            }
            function timeseries::line::mean<D>(x: Relation<(date:D, key:String, value:Number)>[1], w: Integer[1]): Relation<(date:D, key:String, value:Number)>[1]
            {
              $x->timeseries::mean(over(~[key], [~date->ascending()], rows(-2, 0)), ~value, $w, ~v)
                ->select(~[date, key, v])->rename(~v, ~value)
            }
            """.formatted(LINE);

    /** A line of PRICES: `column` of `symbol`'s rows (all of them: null), as the generic rule reads `column[symbol]`. */
    private static String line(String column, String symbol) {
        String rows = symbol == null ? "#>{ts::DB.PRICES}#" : "#>{ts::DB.PRICES}#->filter(r|$r.SYMBOL == '" + symbol + "')";
        return rows + "->select(~[DAY, SYMBOL, " + column + "])->rename(~DAY, ~date)->rename(~SYMBOL, ~key)->rename(~"
                + column + ", ~value)";
    }

    private record Case(String what, String ts, String item, boolean keyed, String pure) {
    }

    private static final List<Case> CASES = List.of(
            new Case("volatility, 63 observations, one line", "SELECT volatility(CLOSE['AAA'], 63) AS v FROM PRICES ON DAY BY SYMBOL",
                    "v", false, "|timeseries::line::volatility(" + line("CLOSE", "AAA") + ", 63)"),
            new Case("volatility, 63 observations, every line", "SELECT volatility(CLOSE, 63) AS v FROM PRICES ON DAY BY SYMBOL",
                    "v", true, "|timeseries::line::volatility(" + line("CLOSE", null) + ", 63)"),
            new Case("volatility, 3 months (calendar)", "SELECT volatility(CLOSE['AAA'], 3mo) AS v FROM PRICES ON DAY BY SYMBOL",
                    "v", false, "|timeseries::line::volatility(" + line("CLOSE", "AAA") + ", 3, meta::pure::functions::date::DurationUnit.MONTHS)"),
            new Case("beta of two instruments, 63", "SELECT beta(CLOSE['AAA'], CLOSE['BBB'], 63) AS b FROM PRICES ON DAY BY SYMBOL",
                    "b", false, "|timeseries::line::beta(" + line("CLOSE", "AAA") + ", " + line("CLOSE", "BBB") + ", 63)"),
            new Case("vwap, one instrument's two columns, 20", "SELECT vwap(CLOSE['AAA'], VOLUME['AAA'], 20) AS vw FROM PRICES ON DAY BY SYMBOL",
                    "vw", false, "|timeseries::line::vwap(" + line("CLOSE", "AAA") + ", " + line("VOLUME", "AAA") + ", 20)"),
            new Case("vwap, every line's two columns, 20", "SELECT vwap(CLOSE, VOLUME, 20) AS vw FROM PRICES ON DAY BY SYMBOL",
                    "vw", true, "|timeseries::line::vwap(" + line("CLOSE", null) + ", " + line("VOLUME", null) + ", 20)"));

    private static String model(TestBackend backend) throws Exception {
        String library = Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
        return STORE + """
                ###Connection
                RelationalDatabaseConnection ts::Conn
                {
                  store: ts::DB;
                  %s
                }
                ###Runtime
                Runtime ts::RT { mappings: [ ]; connections: [ ts::DB: [ c: ts::Conn ] ]; }
                """.formatted(backend.connectionBody()) + library + LINES;
    }

    private static void seed(TestBackend backend, Connection c) throws Exception {
        for (com.legend.model.DatabaseDefinition.TableDefinition t : Compiler.parseModel(STORE).elements().stream()
                .filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList()) {
            try (java.sql.Statement s = c.createStatement()) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
        }
        java.util.Random rnd = new java.util.Random(20261031);
        try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO PRICES VALUES (?, ?, ?, ?)")) {
            for (String sym : List.of("AAA", "BBB", "CCC")) {
                double px = 100;
                LocalDate d = LocalDate.of(2019, 1, 1);
                for (int i = 0; i < 1500; ) {
                    d = d.plusDays(1);
                    if (d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY) {
                        continue;
                    }
                    px *= 1 + rnd.nextGaussian() * 0.012;
                    ps.setString(1, sym);
                    ps.setObject(2, d);
                    ps.setDouble(3, px);
                    ps.setDouble(4, 1_000_000 + rnd.nextInt(500_000));
                    ps.addBatch();
                    i++;
                }
            }
            ps.executeBatch();
        }
    }

    /** {@code text}'s rows, each by its column's name. */
    private static List<Map<String, String>> rows(String text) {
        List<String> lines = new ArrayList<>(Arrays.asList(text.split("\r\n")));
        List<String> header = Arrays.asList(lines.remove(0).split(",", -1));
        List<Map<String, String>> out = new ArrayList<>();
        for (String l : lines) {
            String[] cells = l.split(",", -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < cells.length; i++) {
                row.put(header.get(i), cells[i].isEmpty() ? null : cells[i]);
            }
            out.add(row);
        }
        return out;
    }

    /** Each (date[, line])'s figure, where it has one. */
    private static Map<String, Double> keyed(List<Map<String, String>> rows, String date, String line, String value) {
        Map<String, Double> out = new TreeMap<>();
        for (Map<String, String> r : rows) {
            String v = r.get(value);
            if (v != null) {
                out.put(r.get(date) + (line == null ? "" : "|" + r.get(line)), Double.valueOf(v));
            }
        }
        return out;
    }

    private static int count(String sql, String word) {
        int n = 0;
        String s = sql.toUpperCase(Locale.ROOT);
        for (int i = s.indexOf(word); i >= 0; i = s.indexOf(word, i + 1)) {
            n++;
        }
        return n;
    }

    /** The median of five runs of {@code plan}, in milliseconds, after one run to warm. */
    private static double timed(ExecutionPlan plan, Connection c) throws Exception {
        PlanCases.write(plan, c, Map.of(), PlanCases.Written.CSV::writer);
        double[] ms = new double[5];
        for (int i = 0; i < ms.length; i++) {
            long t0 = System.nanoTime();
            PlanCases.write(plan, c, Map.of(), PlanCases.Written.CSV::writer);
            ms[i] = (System.nanoTime() - t0) / 1e6;
        }
        Arrays.sort(ms);
        return ms[2];
    }

    @Test
    void theLineLevelFunctionsAnswerAsTodaysTsAndHereIsWhatTheyCost() throws Exception {
        TestBackend backend = TestBackends.platform();
        ModelContext ctx = Compiler.compileModel(model(backend));
        StringBuilder report = new StringBuilder("SPIKE-REPORT\n");
        try (Connection c = backend.open()) {
            seed(backend, c);
            for (Case k : CASES) {
                ExecutionPlan ts = Compiler.query(ctx, "|#TS{" + k.ts() + "}#").executionPlan("ts::RT");
                ExecutionPlan pure = Compiler.query(ctx, k.pure()).executionPlan("ts::RT");
                Map<String, Double> want = keyed(rows(PlanCases.write(ts, c, Map.of(), PlanCases.Written.CSV::writer)),
                        "DAY", k.keyed() ? "SYMBOL" : null, k.item());
                Map<String, Double> got = keyed(rows(PlanCases.write(pure, c, Map.of(), PlanCases.Written.CSV::writer)),
                        "date", k.keyed() ? "key" : null, "value");
                assertEquals(want.keySet(), got.keySet(), k.what() + ": the same dates");
                for (Map.Entry<String, Double> e : want.entrySet()) {
                    double g = got.get(e.getKey());
                    assertEquals(e.getValue(), g, 1e-9 * Math.max(1, Math.abs(e.getValue())), k.what() + " at " + e.getKey());
                }
                String tsSql = Compiler.query(ctx, "|#TS{" + k.ts() + "}#").plan("ts::RT").sql();
                String pureSql = Compiler.query(ctx, k.pure()).plan("ts::RT").sql();
                report.append(String.format(Locale.ROOT,
                        "%-42s points %5d | #TS sql %6d chars %2d joins %3d selects %7.1f ms | line sql %6d chars %2d joins %3d selects %7.1f ms%n",
                        k.what(), want.size(), tsSql.length(), count(tsSql, " JOIN "), count(tsSql, "SELECT"), timed(ts, c),
                        pureSql.length(), count(pureSql, " JOIN "), count(pureSql, "SELECT"), timed(pure, c)));
                if (k.what().startsWith("vwap, one")) {
                    report.append("--- #TS SQL (vwap, one):\n").append(tsSql).append("\n--- line SQL (vwap, one):\n")
                            .append(pureSql).append('\n');
                }
            }
        }
        // a line dated by date-times, given to a function of date-dated lines
        String stamped = "|timeseries::line::volatility(#>{ts::DB.STAMPED}#->select(~[AT, SYMBOL, CLOSE])->rename(~AT, ~date)"
                + "->rename(~SYMBOL, ~key)->rename(~CLOSE, ~value), 63)";
        try {
            Compiler.query(ctx, stamped).plan("ts::RT");
            report.append("date-time line: ACCEPTED\n");
        } catch (RuntimeException e) {
            report.append("date-time line: refused -- ").append(e.getMessage().split("\n")[0]).append('\n');
        }
        // a line generic in its date's type: one function for date-dated and date-time-dated lines
        for (String g : List.of("|timeseries::line::mean(" + line("CLOSE", "AAA") + ", 3)",
                "|timeseries::line::mean(#>{ts::DB.STAMPED}#->select(~[AT, SYMBOL, CLOSE])->rename(~AT, ~date)"
                        + "->rename(~SYMBOL, ~key)->rename(~CLOSE, ~value), 3)")) {
            try {
                Compiler.query(ctx, g).plan("ts::RT");
                report.append("generic-date line (").append(g.contains("STAMPED") ? "date-time" : "date").append("): ACCEPTED\n");
            } catch (RuntimeException e) {
                report.append("generic-date line: refused -- ").append(e.getMessage().split("\n")[0]).append('\n');
            }
        }
        System.out.println(report.append("SPIKE-END"));
        assertTrue(true);
    }
}
