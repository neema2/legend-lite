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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCALE, MEASURED (docs/TS_V1_PROGRAM_2026_11_01.md B2; docs/TS_DECISIONS_2026_11_07.md item 7): #TS queries over
 * about ten million rows -- a daily table of 2,000 instruments and 5,040 dates (twenty years of trading days), an
 * intraday table of 100 instruments and 98,280 minute bars each (252 sessions of 390 minutes) -- each query's plan
 * time, its SQL's size and its run time, on the database the lane names ({@code -Dts.backend}: DuckDB unless told;
 * Postgres's lane runs it on the embedded server). The first measurement proposes the budgets and the user rules them
 * (P5); until then it measures and pins nothing.
 *
 * <p>The as-of question (2026-11-07, the user: how do experts run time series on Postgres?): Postgres has no ASOF JOIN,
 * and lite writes one there as a LEFT JOIN whose ON pins the right row to a correlated MAX
 * ({@code CarrierStrategies.asOfEmulation}); the experts' form is {@code LATERAL (... ORDER BY t DESC LIMIT 1)} over an
 * index on {@code (key, t)}. On Postgres both are measured, written by hand over the same tables, and lite's own as-of
 * query explained, first without an index, then with one on {@code (SYMBOL, DAY)}.
 *
 * <p>Manual ({@code //core:ts_scale_measure}, {@code //core:ts_scale_measure_postgres}; {@code --test_output=all}
 * prints the table; its TSV and the plans explained are in the undeclared outputs). The rows are made by the database
 * itself ({@code range}, {@code generate_series}), never sent. On Postgres the tables are UNLOGGED (no WAL: the
 * session's disk), every statement bounded ({@code ts.scale.timeoutSeconds}, 90 unless told): a run past it is
 * recorded as such, not failed.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TsScaleMeasureTest {

    static final int INSTRUMENTS = Integer.getInteger("ts.scale.instruments", 2_000);
    static final int DATES = Integer.getInteger("ts.scale.dates", 5_040);
    static final int BAR_INSTRUMENTS = Integer.getInteger("ts.scale.barInstruments", 100);
    static final int MINUTES = Integer.getInteger("ts.scale.minutes", 98_280);
    static final int TIMEOUT = Integer.getInteger("ts.scale.timeoutSeconds", 90);
    /** The lines of the wide table (0: not run) and a basket's members: the Postgres lane runs fewer (2026-11-07, the
     *  first Postgres run: the 100 lines' query, 428 KB of SQL, grew one backend to 11 GB and the session's memory
     *  limit killed it -- a finding of its own, not repeated on a desk). */
    static final int WIDE_LINES = Integer.getInteger("ts.scale.wideLines", 100);
    static final int BASKET = Integer.getInteger("ts.scale.basket", 500);

    private static final String STORE = """
            ###Relational
            Database ts::DB
            (
              Table PRICES ( SYMBOL VARCHAR(8), DAY DATE, CLOSE DOUBLE )
              Table TICKS ( SYMBOL VARCHAR(8), AT TIMESTAMP, PRICE DOUBLE )
            )
            """;

    /** lite's as-of: a lag by a span reads the line's value as of seven days before each date. */
    private static final String AS_OF_ONE = "SELECT lag(CLOSE['S7'], 7d) AS l FROM PRICES ON DAY BY SYMBOL";
    private static final String AS_OF_EVERY = "WITH s AS TS (SELECT lag(CLOSE, 7d) AS l FROM PRICES ON DAY BY SYMBOL)"
            + " SELECT count(*) AS n FROM s";

    private static TestBackend backend;
    private static boolean postgres;
    private static Connection session;
    private static ModelContext model;
    private static String prices;
    private static String symbol;
    private static String day;
    private static String close;
    private static final List<String> TABLE = new ArrayList<>();
    private static final StringBuilder EXPLAINED = new StringBuilder();

    @BeforeAll
    static void seed() throws Exception {
        String named = System.getProperty("ts.backend");
        backend = TestBackends.named(named == null ? "duckdb" : named);
        postgres = "postgres".equals(backend.name());
        String library = Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
        long t0 = System.nanoTime();
        model = Compiler.compileModel(STORE + """
                ###Connection
                RelationalDatabaseConnection ts::Conn
                {
                  store: ts::DB;
                  %s
                }
                ###Runtime
                Runtime ts::RT { mappings: [ ]; connections: [ ts::DB: [ c: ts::Conn ] ]; }
                """.formatted(backend.connectionBody()), library);
        System.out.printf("the model over the library: %d ms%n", (System.nanoTime() - t0) / 1_000_000);
        session = backend.inProcess() ? backend.open() : backend.openWorkspace();
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(STORE).elements()
                .stream().filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        com.legend.sql.dialect.SqlDialect dialect = backend.dialect();
        prices = dialect.storedName(tables.get(0).name(), tables.get(0).quoted());
        String ticks = dialect.storedName(tables.get(1).name(), tables.get(1).quoted());
        List<com.legend.model.DatabaseDefinition.ColumnDefinition> pc = tables.get(0).columns();
        symbol = dialect.storedName(pc.get(0).name(), pc.get(0).quoted());
        day = dialect.storedName(pc.get(1).name(), pc.get(1).quoted());
        close = dialect.storedName(pc.get(2).name(), pc.get(2).quoted());
        try (Statement s = session.createStatement()) {
            for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
                s.execute(dialect.render(com.legend.setup.Ddl.createTable(t, null)));
            }
            if (postgres) {
                // the session's disk is small: no WAL for the rows (a measurement's tables, never a store's)
                s.execute("ALTER TABLE " + prices + " SET UNLOGGED");
                s.execute("ALTER TABLE " + ticks + " SET UNLOGGED");
            }
            long t1 = System.nanoTime();
            // a price a day: a slow wave per instrument and a drift, so windows have something to measure
            s.execute(postgres
                    ? "INSERT INTO " + prices + " SELECT 'S' || i, DATE '2005-01-03' + d, 100 * (1 + 0.1 * sin(i + d"
                            + " / 50.0)) + d * 0.01 FROM generate_series(0, " + (INSTRUMENTS - 1) + ") i,"
                            + " generate_series(0, " + (DATES - 1) + ") d"
                    : "INSERT INTO " + prices + " SELECT 'S' || i, DATE '2005-01-03' + CAST(d AS INTEGER), 100 * (1 +"
                            + " 0.1 * sin(i + d / 50.0)) + d * 0.01 FROM range(" + INSTRUMENTS + ") r1(i), range("
                            + DATES + ") r2(d)");
            s.execute(postgres
                    ? "INSERT INTO " + ticks + " SELECT 'T' || i, TIMESTAMP '2024-01-02 09:30:00' + m * INTERVAL '1"
                            + " minute', 50 * (1 + 0.05 * sin(i + m / 300.0)) FROM generate_series(0, "
                            + (BAR_INSTRUMENTS - 1) + ") i, generate_series(0, " + (MINUTES - 1) + ") m"
                    : "INSERT INTO " + ticks + " SELECT 'T' || i, TIMESTAMP '2024-01-02 09:30:00' + to_minutes(m),"
                            + " 50 * (1 + 0.05 * sin(i + m / 300.0)) FROM range(" + BAR_INSTRUMENTS + ") r1(i), range("
                            + MINUTES + ") r2(m)");
            if (postgres) {
                s.execute("ANALYZE " + prices);
                s.execute("ANALYZE " + ticks);
                s.execute("SET statement_timeout = '" + TIMEOUT + "s'");
            }
            System.out.printf("%s: %,d daily rows and %,d bars made in %d ms%n", backend.name(),
                    (long) INSTRUMENTS * DATES, (long) BAR_INSTRUMENTS * MINUTES, (System.nanoTime() - t1) / 1_000_000);
        }
        TABLE.add(String.join("\t", "benchmark", "plan ms", "sql bytes", "run ms", "rows"));
    }

    @AfterAll
    static void report() throws Exception {
        if (session != null) {
            session.close();
        }
        String text = String.join("\n", TABLE) + "\n";
        System.out.println("\n" + backend.name() + " (" + (long) INSTRUMENTS * DATES + " daily rows, "
                + (long) BAR_INSTRUMENTS * MINUTES + " bars)\n" + text);
        System.out.println(EXPLAINED);
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "ts-scale-" + backend.name() + ".tsv"), text, StandardCharsets.UTF_8);
            Files.writeString(Path.of(outputs, "ts-scale-" + backend.name() + "-explained.txt"), EXPLAINED.toString(),
                    StandardCharsets.UTF_8);
        }
    }

    /** Whether {@code e} (or a cause) is the database's statement timeout. */
    private static boolean timedOut(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (String.valueOf(t.getMessage()).contains("statement timeout")) {
                return true;
            }
        }
        return false;
    }

    /** {@code ts}'s plan time (the best of three, the model held), its SQL's size and its run (the best of two; one
     *  past the bound is recorded as {@code >Ns}). */
    private static ExecutionPlan measure(String name, String ts) throws Exception {
        String query = "|#TS{" + ts + "}#";
        long plan = Long.MAX_VALUE;
        ExecutionPlan planned = null;
        for (int i = 0; i < 3; i++) {
            long t = System.nanoTime();
            planned = Compiler.query(model, query).executionPlan("ts::RT");
            plan = Math.min(plan, System.nanoTime() - t);
        }
        String sql = java.util.Objects.requireNonNull(planned).answerSql().statement();
        long run = Long.MAX_VALUE;
        String rows = "";
        for (int i = 0; i < 2; i++) {
            long t = System.nanoTime();
            try {
                String text = PlanCases.write(planned, session, Map.of(), PlanCases.Written.CSV::writer);
                run = Math.min(run, System.nanoTime() - t);
                rows = String.valueOf(text.chars().filter(c -> c == '\n').count() - 1);
            } catch (Exception e) {
                if (!timedOut(e)) {
                    throw e;
                }
                rows = "-";
                break;
            }
        }
        TABLE.add(String.join("\t", name, String.valueOf(plan / 1_000_000), String.valueOf(sql.length()),
                run == Long.MAX_VALUE ? ">" + TIMEOUT + "s" : String.valueOf(run / 1_000_000), rows));
        return planned;
    }

    /** A statement written by hand (Postgres's as-of forms), its one count read: its run, or past the bound. */
    private static void handWritten(String name, String sql) throws Exception {
        long t = System.nanoTime();
        String rows;
        String ms;
        try (Statement s = session.createStatement(); ResultSet r = s.executeQuery(sql)) {
            r.next();
            rows = String.valueOf(r.getLong(1));
            ms = String.valueOf((System.nanoTime() - t) / 1_000_000);
        } catch (java.sql.SQLException e) {
            if (!timedOut(e)) {
                throw e;
            }
            rows = "-";
            ms = ">" + TIMEOUT + "s";
        }
        TABLE.add(String.join("\t", name, "", String.valueOf(sql.length()), ms, rows));
    }

    /** Postgres's own account of {@code sql}: run and timed ({@code EXPLAIN ANALYZE}) when it ends within the bound,
     *  else its plan alone. */
    private static void explain(String name, String sql) throws Exception {
        EXPLAINED.append("== ").append(name).append("\n").append(sql).append("\n\n");
        try (Statement s = session.createStatement()) {
            List<String> lines = new ArrayList<>();
            try (ResultSet r = s.executeQuery("EXPLAIN (ANALYZE, BUFFERS) " + sql)) {
                while (r.next()) {
                    lines.add(r.getString(1));
                }
            } catch (java.sql.SQLException e) {
                if (!timedOut(e)) {
                    throw e;
                }
                lines.add("(past " + TIMEOUT + "s: the plan alone)");
                try (ResultSet r = s.executeQuery("EXPLAIN " + sql)) {
                    while (r.next()) {
                        lines.add(r.getString(1));
                    }
                }
            }
            EXPLAINED.append(String.join("\n", lines)).append("\n\n");
        }
    }

    /** The as-of forms on Postgres over the daily table, one line's and every line's: lite's own query explained,
     *  and the two forms by hand -- the correlated MAX lite writes, the LATERAL the experts write. */
    private static void asOfForms(String when) throws Exception {
        if (!postgres) {
            return;
        }
        ExecutionPlan one = Compiler.query(model, "|#TS{" + AS_OF_ONE + "}#").executionPlan("ts::RT");
        if (one.answerSql().slots().isEmpty()) {
            explain("lite's as-of, one line, " + when, one.answerSql().statement());
        }
        String pickMax = "SELECT count(r." + close + ") FROM " + prices + " l LEFT JOIN " + prices + " r ON r." + symbol
                + " = l." + symbol + " AND r." + day + " <= l." + day + " - 7 AND r." + day + " = (SELECT MAX(a." + day
                + ") FROM " + prices + " a WHERE a." + symbol + " = l." + symbol + " AND a." + day + " <= l." + day
                + " - 7)";
        String lateral = "SELECT count(r." + close + ") FROM " + prices + " l LEFT JOIN LATERAL (SELECT p." + close
                + " FROM " + prices + " p WHERE p." + symbol + " = l." + symbol + " AND p." + day + " <= l." + day
                + " - 7 ORDER BY p." + day + " DESC LIMIT 1) r ON true";
        String line = " WHERE l." + symbol + " = 'S7'";
        handWritten("as-of by hand, correlated MAX, one line, " + when, pickMax + line);
        handWritten("as-of by hand, LATERAL, one line, " + when, lateral + line);
        explain("as-of by hand, correlated MAX, one line, " + when, pickMax + line);
        explain("as-of by hand, LATERAL, one line, " + when, lateral + line);
        handWritten("as-of by hand, correlated MAX, every line, " + when, pickMax);
        handWritten("as-of by hand, LATERAL, every line, " + when, lateral);
    }

    @Test
    @Order(1)
    void daily() throws Exception {
        // one instrument's chart: two figures of its line
        measure("daily, one line", "SELECT avg(CLOSE['S7'], 20) AS m, volatility(CLOSE['S7'], 63) AS v"
                + " FROM PRICES ON DAY BY SYMBOL");
        // every instrument's figure, counted by the database (its ten million values computed, one row sent)
        measure("daily, every line", "WITH s AS TS (SELECT avg(CLOSE, 20) AS m FROM PRICES ON DAY BY SYMBOL)"
                + " SELECT count(*) AS n FROM s");
    }

    @Test
    @Order(2)
    void wide() throws Exception {
        List<String> items = new ArrayList<>();
        String[] words = {"avg(%s, 20)", "std(%s, 20)", "returns(%s)", "volatility(%s, 63)", "zscores(%s, 63)",
            "max(%s, 20)", "min(%s, 20)", "diff(%s, 1)", "lag(%s, 5)", "rebase(%s)"};
        for (int i = 0; i < 20; i++) {
            items.add(words[i % words.length].formatted("CLOSE['S" + (i / words.length) + "']") + " AS i" + i);
        }
        measure("wide, 20 items", "SELECT " + String.join(", ", items) + " FROM PRICES ON DAY BY SYMBOL");
        // a table of 100 lines, each its own: the spread puts each line onto the answer's dates in turn
        if (WIDE_LINES == 0) {
            TABLE.add(String.join("\t", "wide, 100 lines", "not run here", "", "", ""));
            return;
        }
        String lines = IntStream.range(0, WIDE_LINES).mapToObj(i -> "CLOSE['S" + i + "'] AS c" + i)
                .collect(Collectors.joining(", "));
        try {
            measure("wide, " + WIDE_LINES + " lines", "SELECT " + lines + " FROM PRICES ON DAY BY SYMBOL");
        } catch (StackOverflowError e) {
            TABLE.add(String.join("\t", "wide, " + WIDE_LINES + " lines", "stack overflow", "", "", ""));
        }
    }

    @Test
    @Order(3)
    void sampled() throws Exception {
        // a period's last and its mean (R5: of a source's column)
        measure("sampled, 1w", "SELECT last(CLOSE['S7']) AS c, avg(CLOSE['S7']) AS a FROM PRICES ON DAY BY SYMBOL"
                + " SAMPLE BY 1w");
        measure("sampled, 1mo", "SELECT last(CLOSE['S7']) AS c, avg(CLOSE['S7']) AS a FROM PRICES ON DAY BY SYMBOL"
                + " SAMPLE BY 1mo");
    }

    @Test
    @Order(4)
    void basket() throws Exception {
        String members = IntStream.range(0, BASKET).mapToObj(i -> "CLOSE['S" + i + "'], " + (1.0 / BASKET))
                .collect(Collectors.joining(", "));
        measure("a basket of " + BASKET, "SELECT basket(" + members + ") AS b FROM PRICES ON DAY BY SYMBOL");
    }

    @Test
    @Order(5)
    void intraday() throws Exception {
        measure("intraday, one line", "SELECT avg(PRICE['T3'], 30) AS m FROM TICKS ON AT BY SYMBOL");
        measure("intraday, one line by day", "SELECT last(PRICE['T3']) AS c FROM TICKS ON AT BY SYMBOL SAMPLE BY 1d");
    }

    @Test
    @Order(6)
    void asOf() throws Exception {
        String sql = measure("as-of lag, one line", AS_OF_ONE).answerSql().statement().toUpperCase(Locale.ROOT);
        measure("as-of lag, every line", AS_OF_EVERY);
        // the form lite writes: a native as-of join where the database has one, the correlated MAX where it has not
        assertTrue(postgres ? sql.contains("MAX(") : sql.contains("ASOF"), sql);
        asOfForms("no index");
    }

    @Test
    @Order(7)
    void asOfIndexed() throws Exception {
        if (!postgres) {
            return;
        }
        // the index the experts' form reads: (key, date)
        long t = System.nanoTime();
        try (Statement s = session.createStatement()) {
            s.execute("SET statement_timeout = 0");
            s.execute("CREATE INDEX prices_symbol_day ON " + prices + " (" + symbol + ", " + day + ")");
            s.execute("ANALYZE " + prices);
            s.execute("SET statement_timeout = '" + TIMEOUT + "s'");
        }
        TABLE.add(String.join("\t", "index on (SYMBOL, DAY) made", "", "", String.valueOf((System.nanoTime() - t)
                / 1_000_000), ""));
        measure("as-of lag, one line, indexed", AS_OF_ONE);
        measure("as-of lag, every line, indexed", AS_OF_EVERY);
        measure("daily, one line, indexed", "SELECT avg(CLOSE['S7'], 20) AS m, volatility(CLOSE['S7'], 63) AS v"
                + " FROM PRICES ON DAY BY SYMBOL");
        asOfForms("indexed");
    }
}
