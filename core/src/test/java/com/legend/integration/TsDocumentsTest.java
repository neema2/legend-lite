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
import java.nio.file.Path;
import java.sql.Connection;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DOCUMENTS' EXAMPLES RUN (docs/TS_V1_PROGRAM_2026_11_01.md A9): every query the {@code #TS} spec
 * (docs/TIMESERIES_LANGUAGE_2026_10_29.md) and the guide for quants (docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md)
 * show is an example of docs/timeseries/examples.sql -- their one source, which writes the documents' copies
 * ({@code bazel run //docs:update_ts_examples}; the documents themselves are never read here: prose is never a test
 * input, G17) -- and this test runs each on the platform's own database (no database is what it is about), over tables shaped as the documents name them -- {@code prices}, {@code rates},
 * {@code macro}, {@code trades}, {@code fx}, {@code yields} and the class {@code MarketQuote} -- filled with five years of
 * business days made up here. Each must answer rows, and each of its columns a value somewhere: an example that reads
 * an instrument the data has not, or is no longer the language, fails here, never in a reader's hands. The numbers are
 * not judged (the frozen answers and the oracle judge those); that the documents' queries are the language is.
 *
 * <p>Its own target ({@code //core:ts_documents_test}): it reads the timeseries project's file and the examples,
 * passed as {@code -Dtimeseries.library} and {@code -Dts.examples}.
 */
class TsDocumentsTest {

    /** The tables the documents read, as they name them; a quote's instrument by its ticker. */
    private static final String STORE = """
            ###Relational
            Database docs::DB
            (
              Table prices ( symbol VARCHAR(16) PRIMARY KEY, asset_class VARCHAR(16), date DATE PRIMARY KEY,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT )
              Table rates ( tenor VARCHAR(8) PRIMARY KEY, years DOUBLE, date DATE PRIMARY KEY, yield DOUBLE )
              Table macro ( series VARCHAR(16) PRIMARY KEY, date DATE PRIMARY KEY, value DOUBLE )
              Table trades ( ticker VARCHAR(16), trade_date DATE, price DOUBLE, qty DOUBLE, side VARCHAR(8),
                venue VARCHAR(8) )
              Table fx ( pair VARCHAR(16) PRIMARY KEY, date DATE PRIMARY KEY, rate DOUBLE )
              Table yields ( tenor VARCHAR(8) PRIMARY KEY, date DATE PRIMARY KEY, yield DOUBLE )
              Table quotes ( ticker VARCHAR(16), quote_time DATE, close_price DOUBLE )
              Table instruments ( ticker VARCHAR(16) PRIMARY KEY )
              Join QuoteInstrument(quotes.ticker = instruments.ticker)
            )
            """;

    /** The class the spec reads as objects: a quote, its instrument (to one). */
    private static final String CLASSES = """
            ###Pure
            Class docs::MarketQuote { quoteTime: StrictDate[1]; closePrice: Float[1]; }
            Class docs::Instrument { ticker: String[1]; }
            Association docs::QuoteInstrument { instrument: docs::Instrument[1]; quotes: docs::MarketQuote[*]; }
            ###Mapping
            Mapping docs::M
            (
              docs::MarketQuote: Relational
              {
                ~mainTable [docs::DB] quotes
                quoteTime: [docs::DB] quotes.quote_time,
                closePrice: [docs::DB] quotes.close_price
              }
              docs::Instrument: Relational
              {
                ~primaryKey ([docs::DB] instruments.ticker)
                ~mainTable [docs::DB] instruments
                ticker: [docs::DB] instruments.ticker
              }
              docs::QuoteInstrument: Relational { AssociationMapping (
                instrument: [docs::DB]@QuoteInstrument,
                quotes: [docs::DB]@QuoteInstrument
              ) }
            )
            """;

    /** The instruments of {@code prices}: each its asset class, its first close, its drift and volatility a year, and
     *  its beta to one market (so they move together as a market does). */
    private record Instrument(String symbol, String asset, double start, double drift, double vol, double beta) {
    }

    private static final List<Instrument> INSTRUMENTS = List.of(
            new Instrument("SPX", "Index", 3250, 0.12, 0.16, 1),
            new Instrument("QQQ", "Index", 210, 0.16, 0.21, 1.15),
            new Instrument("AAPL", "Equity", 75, 0.25, 0.28, 1.15),
            new Instrument("MSFT", "Equity", 160, 0.22, 0.26, 1.05),
            new Instrument("NVDA", "Equity", 6, 0.6, 0.5, 1.6),
            new Instrument("JPM", "Equity", 140, 0.1, 0.25, 1.1),
            new Instrument("XOM", "Equity", 70, 0.08, 0.3, 0.8),
            new Instrument("KO", "Equity", 55, 0.05, 0.16, 0.6),
            new Instrument("PEP", "Equity", 135, 0.05, 0.17, 0.6),
            new Instrument("XLK", "Sector", 92, 0.18, 0.23, 1.15),
            new Instrument("XLE", "Sector", 60, 0.07, 0.31, 0.85),
            new Instrument("XLF", "Sector", 30, 0.08, 0.24, 1.1),
            new Instrument("GLD", "Commodity", 145, 0.08, 0.14, 0.05),
            new Instrument("TLT", "Bond", 136, -0.04, 0.16, -0.25),
            new Instrument("EURUSD", "FX", 1.12, -0.01, 0.07, 0.15));

    private static final LocalDate FIRST = LocalDate.of(2020, 1, 2);
    private static final LocalDate LAST = LocalDate.of(2024, 12, 31);

    /** The business days the tables hold. */
    private static List<LocalDate> days() {
        List<LocalDate> out = new ArrayList<>();
        for (LocalDate d = FIRST; !d.isAfter(LAST); d = d.plusDays(1)) {
            if (d.getDayOfWeek() != DayOfWeek.SATURDAY && d.getDayOfWeek() != DayOfWeek.SUNDAY) {
                out.add(d);
            }
        }
        return out;
    }

    /** Every table's rows, by its name, made up once (the same each run: one seed). */
    private static Map<String, List<Object[]>> rowsOf() {
        SplittableRandom rand = new SplittableRandom(20261103);
        List<LocalDate> days = days();
        Map<String, List<Object[]>> out = new LinkedHashMap<>();
        List<Object[]> prices = new ArrayList<>();
        List<Object[]> quotes = new ArrayList<>();
        List<Object[]> trades = new ArrayList<>();
        List<Object[]> fx = new ArrayList<>();
        double[] close = INSTRUMENTS.stream().mapToDouble(Instrument::start).toArray();
        for (LocalDate day : days) {
            double market = normal(rand);
            for (int i = 0; i < INSTRUMENTS.size(); i++) {
                Instrument x = INSTRUMENTS.get(i);
                double daily = x.vol() / Math.sqrt(252);
                double shock = x.beta() * market * 0.7 + normal(rand) * Math.sqrt(Math.max(0.2, 1 - 0.49 * x.beta()
                        * x.beta()));
                double open = close[i] * Math.exp(normal(rand) * daily * 0.3);
                double next = close[i] * Math.exp(x.drift() / 252 - daily * daily / 2 + daily * shock);
                double high = Math.max(open, next) * (1 + Math.abs(normal(rand)) * daily * 0.5);
                double low = Math.min(open, next) * (1 - Math.abs(normal(rand)) * daily * 0.5);
                close[i] = next;
                long volume = "FX".equals(x.asset()) ? 0 : Math.round(1e7 * (0.7 + 0.3 * Math.abs(shock)));
                prices.add(new Object[] {x.symbol(), x.asset(), day, round(open), round(high), round(low),
                        round(next), volume});
                if ("AAPL".equals(x.symbol()) || "MSFT".equals(x.symbol())) {
                    quotes.add(new Object[] {x.symbol(), day, round(next)});
                }
                if ("AAPL".equals(x.symbol())) {
                    // six trades a day, bought and sold in turn, one in three in a dark pool
                    for (int t = 0; t < 6; t++) {
                        trades.add(new Object[] {"AAPL", day, round(next * (1 + normal(rand) * 0.002)),
                                (double) (100 + rand.nextInt(900)), t % 2 == 0 ? "buy" : "sell",
                                t % 3 == 0 ? "DARK" : "LIT"});
                    }
                }
                if ("EURUSD".equals(x.symbol())) {
                    fx.add(new Object[] {"EURUSD", day, Math.round(next * 10_000) / 10_000.0});
                }
            }
        }
        out.put("prices", prices);
        // the US curve, in percent: near zero, then rising, the front faster than the back (so it inverts)
        List<Object[]> rates = new ArrayList<>();
        List<Object[]> yields = new ArrayList<>();
        String[] tenors = {"3M", "2Y", "5Y", "10Y", "30Y"};
        double[] years = {0.25, 2, 5, 10, 30};
        double[] front = {1, 0.85, 0.65, 0.5, 0.38};
        double drift = 0;
        for (int d = 0; d < days.size(); d++) {
            double level = Math.min(5.3, Math.max(0.1, (d - 500) / 100.0));
            drift += normal(rand) * 0.02;
            for (int t = 0; t < tenors.length; t++) {
                double y = 1.4 + front[t] * (level - 1.4) + years[t] * 0.03 + drift;
                rates.add(new Object[] {tenors[t], years[t], days.get(d), Math.round(y * 1000) / 1000.0});
            }
            yields.add(new Object[] {"DE3M", days.get(d), Math.round((level * 0.7 - 0.5 + drift) * 1000) / 1000.0});
        }
        out.put("rates", rates);
        // CPI at each month's end, about 3% a year
        List<Object[]> macro = new ArrayList<>();
        double cpi = 258;
        for (LocalDate m = FIRST.withDayOfMonth(1); !m.isAfter(LAST); m = m.plusMonths(1)) {
            cpi *= 1 + 0.0025 + normal(rand) * 0.002;
            macro.add(new Object[] {"CPI", m.withDayOfMonth(m.lengthOfMonth()), Math.round(cpi * 100) / 100.0});
        }
        out.put("macro", macro);
        out.put("trades", trades);
        out.put("fx", fx);
        out.put("yields", yields);
        out.put("quotes", quotes);
        out.put("instruments", List.of(new Object[] {"AAPL"}, new Object[] {"MSFT"}));
        return out;
    }

    private static double normal(SplittableRandom rand) {
        return Math.sqrt(-2 * Math.log(1 - rand.nextDouble())) * Math.cos(2 * Math.PI * rand.nextDouble());
    }

    private static double round(double v) {
        return Math.round(v * 100) / 100.0;
    }

    private static String model(TestBackend backend) throws java.io.IOException {
        return STORE + CLASSES + """
                ###Connection
                RelationalDatabaseConnection docs::Conn
                {
                  store: docs::DB;
                  %s
                }
                ###Runtime
                Runtime docs::RT { mappings: [ docs::M ]; connections: [ docs::DB: [ c: docs::Conn ] ]; }
                """.formatted(backend.connectionBody())
                + Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
    }

    /** {@code c} given the tables, as {@code backend}'s dialect writes them, and their rows. */
    private static void seed(TestBackend backend, Connection c) throws java.sql.SQLException {
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(STORE).elements()
                .stream().filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        Map<String, List<Object[]>> rows = rowsOf();
        boolean javaTime = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(backend.type())).decoding().bindsJavaTime();
        try (java.sql.Statement s = c.createStatement()) {
            for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
        }
        for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
            List<Object[]> these = rows.get(t.name());
            String row = "(" + String.join(", ", java.util.Collections.nCopies(these.get(0).length, "?")) + ")";
            // 500 rows a statement: one row a statement takes seconds for the tables' 40,000
            for (int from = 0; from < these.size(); from += 500) {
                List<Object[]> chunk = these.subList(from, Math.min(these.size(), from + 500));
                try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO "
                        + backend.dialect().storedName(t.name(), t.quoted()) + " VALUES "
                        + String.join(", ", java.util.Collections.nCopies(chunk.size(), row)))) {
                    int at = 1;
                    for (Object[] r : chunk) {
                        for (Object v : r) {
                            // a driver that binds no java.time value (its connector's fact) takes a java.sql one
                            ps.setObject(at++, v instanceof java.time.LocalDate d && !javaTime ? java.sql.Date.valueOf(d)
                                    : v);
                        }
                    }
                    ps.execute();
                }
            }
        }
    }

    /** The examples of {@code file}, by id, in order: each the text after its {@code -- @example <id>} line (the
     *  documents' generator, datacube/tools/ts-catalog/TsExamplesDoc.java, reads them the same way). */
    private static Map<String, String> examples(Path file) throws java.io.IOException {
        Map<String, String> out = new LinkedHashMap<>();
        String id = null;
        StringBuilder text = new StringBuilder();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.startsWith("-- @example ")) {
                if (id != null) {
                    out.put(id, text.toString().strip());
                }
                id = line.substring("-- @example ".length()).strip();
                text.setLength(0);
            } else if (id != null) {
                text.append(line).append('\n');
            }
        }
        if (id != null) {
            out.put(id, text.toString().strip());
        }
        return out;
    }

    /** The most SQL an example may be before it is run: a database spends minutes preparing tens of millions of
     *  characters (the spec's pairs trade, 2026-11-03: 23.9 million, the database's prepare not done after ten minutes). */
    private static final int SQL_CAP = 1_000_000;

    /** The examples that do not run yet, by id, each why. Only allowed to shrink: an example listed here that runs
     *  fails the test until its row is deleted. */
    private static final Map<String, String> NOT_YET = Map.of();

    /** The SQL {@code node} sends, its statements' text together: what the database prepares. Read from the execution
     *  plan, which is what runs: a plan that stages a statement's shared relations as tables first (ClickHouse's) is
     *  steps, and the text plan, one answering statement, refuses steps since main's #95 (2026-11-13). */
    private static int sqlSize(ExecutionPlan.Node node) {
        return switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().stream().mapToInt(TsDocumentsTest::sqlSize).sum();
            case ExecutionPlan.TdsResult t -> t.sql().statement().length();
            case ExecutionPlan.Update u -> u.sql().statement().length();
            case ExecutionPlan.Effect e -> e.sql().statement().length();
            case ExecutionPlan.Check c -> c.sql().statement().length();
            case ExecutionPlan.Keyed k -> k.keys().statement().length() + k.body().statement().length();
            case ExecutionPlan.Load l -> 0;
            case ExecutionPlan.Constant c -> 0;
        };
    }

    /** {@code query}'s rows, each by its column's name: a value's text, or null where it has none. */
    private static List<Map<String, String>> rows(ModelContext model, Connection c, String query) throws Exception {
        ExecutionPlan plan = Compiler.query(model, query).executionPlan("docs::RT");
        int size = sqlSize(plan.root());
        if (size > SQL_CAP) {
            throw new IllegalStateException(String.format(java.util.Locale.ROOT, "its SQL is %,d characters, past"
                    + " the %,d a database prepares in reasonable time", size, SQL_CAP));
        }
        String text = PlanCases.write(plan, c, Map.of(), PlanCases.Written.CSV::writer);
        List<String> lines = new ArrayList<>(Arrays.asList(text.split("\r\n")));
        List<String> header = Arrays.asList(lines.remove(0).split(",", -1));
        List<Map<String, String>> out = new ArrayList<>();
        for (String line : lines) {
            String[] cells = line.split(",", -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < header.size(); i++) {
                row.put(header.get(i), i < cells.length && !cells[i].isEmpty() ? cells[i] : null);
            }
            out.add(row);
        }
        return out;
    }

    @Test
    void everyExampleOfTheSpecAndTheGuideRuns() throws Exception {
        TestBackend backend = TestBackends.platform();
        ModelContext model = Compiler.compileModel(model(backend));
        Map<String, String> examples = examples(Runfile.property("ts.examples"));
        List<String> wrong = new ArrayList<>();
        Map<String, String> notYet = new java.util.TreeMap<>();
        int ran = 0;
        try (Connection c = backend.open()) {
            seed(backend, c);
            for (Map.Entry<String, String> e : examples.entrySet()) {
                ran++;
                String why = why(model, c, e.getValue());
                if (why != null) {
                    notYet.put(e.getKey(), why);
                }
            }
        }
        assertTrue(ran >= 29, "the documents' examples read: " + ran);
        for (Map.Entry<String, String> e : notYet.entrySet()) {
            if (!NOT_YET.containsKey(e.getKey())) {
                wrong.add(e.getKey() + "\n    " + e.getValue());
            }
        }
        for (String listed : NOT_YET.keySet()) {
            if (!notYet.containsKey(listed)) {
                wrong.add(listed + "\n    runs now: delete its row of NOT_YET");
            }
        }
        assertEquals(List.of(), wrong, "every example of the documents runs and answers, or is listed in NOT_YET with"
                + " why (only allowed to shrink)");
    }

    /** The examples that answer otherwise on a lane's database, by {@code <database> <id>}, and why: each a register
     *  row or a named open item, listed only while it differs (one that comes to agree fails here). */
    private static final Map<String, String> DIFFERS = Map.of();

    /** The examples not run on a lane's database, by {@code <database> <id>}, each why: an example that never finishes
     *  there, until its named open item lands (none may be a wrong answer: those are DIFFERS rows). */
    private static final Map<String, String> UNRUN = Map.of(
            "postgres spec-16", "past the lane's 120 s on Postgres (DuckDB: under a second): its calendar-period pair"
                    + " words (beta over 1y, r_squared over 3mo, every equity) are self-joins over each line's period,"
                    + " which Postgres runs as a nested comparison per line; docs/TS_DECISIONS_2026_11_07.md item 18"
                    + " (Postgres's performance, with F3 and F4)");

    /**
     * THE DOCUMENTS' EXAMPLES ON A V1 DATABASE (docs/TS_DECISIONS_2026_11_07.md item 9b, 2026-11-07: ClickHouse a v1
     * database, the user's "All four"): every example on DuckDB and on the lane's database ({@code -Dts.backend}),
     * each answer held to DuckDB's ({@link TsAnswers}: the same dates and lines, each figure within 1e-9 of its scale).
     * A statement past the lane's bound ({@code ts.documents.session}, a session statement) is named, not waited on.
     */
    @Test
    void everyExampleAnswersAsOnThePlatform() throws Exception {
        String named = System.getProperty("ts.backend");
        if (named == null) {
            throw new IllegalStateException("the documents' lane names its database (-Dts.backend): its targets select"
                    + " this method alone");
        }
        TestBackend backend = TestBackends.named(named);
        TestBackend platform = TestBackends.platform();
        Map<String, String> examples = examples(Runfile.property("ts.examples"));
        Map<String, Object> want = answers(platform, examples, null);
        List<String> refusedOnThePlatform = want.entrySet().stream().filter(e -> e.getValue() instanceof String)
                .map(e -> e.getKey() + ": " + e.getValue()).toList();
        assertEquals(List.of(), refusedOnThePlatform, "every example answers on DuckDB");
        Map<String, String> run = new LinkedHashMap<>(examples);
        run.keySet().removeIf(id -> UNRUN.containsKey(backend.name() + " " + id));
        Map<String, Object> got = answers(backend, run, System.getProperty("ts.documents.session"));
        List<String> unexpected = new ArrayList<>();
        List<String> agreed = new ArrayList<>();
        StringBuilder report = new StringBuilder();
        int same = 0;
        for (String id : run.keySet()) {
            Object answer = got.get(id);
            String said = answer instanceof String refused ? "refused: " + refused
                    : TsAnswers.difference((TsAnswers.Answer) want.get(id), (TsAnswers.Answer) answer);
            String key = backend.name() + " " + id;
            if (said == null) {
                same++;
                if (DIFFERS.containsKey(key)) {
                    agreed.add(key);
                }
            } else {
                report.append(key).append(": ").append(said).append('\n');
                if (!DIFFERS.containsKey(key)) {
                    unexpected.add(key);
                }
            }
        }
        report.append(backend.name()).append(": ").append(same).append(" of ").append(examples.size())
                .append(" examples as on DuckDB, ").append(examples.size() - run.size()).append(" not run (UNRUN)\n");
        System.out.println(report);
        assertEquals(List.of(), unexpected, "every example answers as on DuckDB, or is listed in DIFFERS with why\n"
                + report);
        assertEquals(List.of(), agreed, "an example listed in DIFFERS that now answers as on DuckDB: delete its row");
    }

    /** Each example's answer on {@code backend}, or the message it was refused with; {@code bound} a session
     *  statement run first where given. */
    private static Map<String, Object> answers(TestBackend backend, Map<String, String> examples,
            @com.legend.base.Nullable String bound) throws Exception {
        ModelContext model = Compiler.compileModel(model(backend));
        Map<String, Object> out = new LinkedHashMap<>();
        // a server's database outlives a connection: the run's own workspace there (dropped as it closes)
        try (Connection c = backend.inProcess() ? backend.open() : backend.openWorkspace()) {
            seed(backend, c);
            if (bound != null) {
                try (java.sql.Statement st = c.createStatement()) {
                    st.execute(bound);
                }
            }
            for (Map.Entry<String, String> e : examples.entrySet()) {
                long started = System.nanoTime();
                String query = "|#TS{" + e.getValue() + "}#";
                try {
                    ExecutionPlan plan = Compiler.query(model, query).executionPlan("docs::RT");
                    int size = sqlSize(plan.root());
                    if (size > SQL_CAP) {
                        throw new IllegalStateException(String.format(java.util.Locale.ROOT, "its SQL is %,d"
                                + " characters, past the %,d a database prepares in reasonable time", size, SQL_CAP));
                    }
                    out.put(e.getKey(), TsAnswers.of(plan, PlanCases.write(plan, c, Map.of(),
                            PlanCases.Written.CSV::writer)));
                } catch (Exception | AssertionError x) {
                    String message = String.valueOf(x.getMessage());
                    String first = message.lines().findFirst().orElse(message);
                    out.put(e.getKey(), first.length() > 240 ? first.substring(0, 240) + "..." : first);
                }
                System.err.println(backend.name() + " " + e.getKey() + ": " + (System.nanoTime() - started) / 1_000_000
                        + " ms");
            }
        }
        return out;
    }

    /** Why {@code example} does not run and answer -- refused, no rows, a column of no value, SQL past the cap -- or
     *  null where it does. */
    private static @com.legend.base.Nullable String why(ModelContext model, Connection c, String example) {
        try {
            List<Map<String, String>> got = rows(model, c, "|#TS{" + example + "}#");
            if (got.isEmpty()) {
                return "no rows";
            }
            for (String column : got.get(0).keySet()) {
                if (got.stream().allMatch(r -> r.get(column) == null)) {
                    return column + " has no value on any of its " + got.size() + " rows";
                }
            }
            return null;
        } catch (Exception | AssertionError e) {
            String message = String.valueOf(e.getMessage());
            String first = message.lines().findFirst().orElse(message);
            return first.length() > 240 ? first.substring(0, 240) + "..." : first;
        }
    }
}
