// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.error.DataError;
import com.legend.error.LegendCompileException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import com.legend.testing.Lean;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ADVERSARIAL MARKETS FIXTURE (projects/timeseries/fixtures/adversarial), on the database the target names
 * ({@code -Dts.backend}, the database the manifest's naive SQL is written for): for each trap its manifest names, a
 * query of the timeseries library ({@code #TS{...}#}, or Pure handing {@code #TS} a relation) judged against the trap's
 * correct answer; and the manifest's naive SQL run as it is, which must give the naive answer the manifest says and
 * not the correct one -- the proof that the trap bites -- while the same SQL on a neighbouring date (a trap's control)
 * gives its control answer, so the trap is the date and not the SQL failing everywhere. Every answer was worked by the
 * fixture's generate.py over prices.csv's own numbers, never by the code under test.
 *
 * <p>Each case's status, value and whether it faces its trap are held by {@code
 * core/src/test/resources/ts-adversarial/ledger.tsv} (the parity lanes' ratchet's problems, NEW, REGRESSION, FIX
 * DETECTED, STATUS MISMATCH, GONE, and VALUE MOVED: a DIFF whose wrong number changes is red too). PASS, lite's answer
 * the correct one; DIFF, another (a finding, ledgered: the words are never edited to make a case pass); REFUSED, lite's
 * refusal. A case that does not face its trap (its source has the trap undone by hand, or the trap is a mistake lite
 * never makes) is evidence of the word, not of the trap, and says so. Rewrite the ledger on purpose with {@code
 * bazel test //core:ts_adversarial_test --strategy=TestRunner=local
 * --jvmopt=-Dts.adversarial.update=$PWD/core/src/test/resources/ts-adversarial/ledger.tsv} (a test argument
 * {@code --jvm_flag} is no JUnitMain argument: measured 2026-11-08). The naive
 * SQL's answers are no ratchet: a naive form that stops giving its answer, or gives the correct one, fails the target,
 * as the fixture is then wrong.
 *
 * <p>Its own target ({@code //core:ts_adversarial_test}): it reads the fixture's files, the timeseries project's file
 * and its ledger, each passed as a {@code -D} property.
 */
class TsAdversarialTest {

    /** The fixture's columns, as prices.csv's header names them (seq, the last, each row's ingest sequence), in three
     *  tables: the rows as they arrived, the same rows with the split applied (prices_adjusted.csv), and the rows in
     *  the reverse of their file order (the tie's second loading); and calendar.csv's. */
    private static final String STORE = """
            ###Relational
            Database adv::DB
            (
              Table PRICES ( SYMBOL VARCHAR(8), DATE DATE, OPEN DOUBLE, HIGH DOUBLE, LOW DOUBLE, CLOSE DOUBLE,
                VOLUME BIGINT, SEQ BIGINT )
              Table PRICES_ADJUSTED ( SYMBOL VARCHAR(8), DATE DATE, OPEN DOUBLE, HIGH DOUBLE, LOW DOUBLE,
                CLOSE DOUBLE, VOLUME BIGINT, SEQ BIGINT )
              Table PRICES_REVERSED ( SYMBOL VARCHAR(8), DATE DATE, OPEN DOUBLE, HIGH DOUBLE, LOW DOUBLE,
                CLOSE DOUBLE, VOLUME BIGINT, SEQ BIGINT )
              Table CALENDAR ( DATE DATE, MARKET VARCHAR(8), IS_BUSINESS_DAY VARCHAR(5) )
            )
            """;

    /** A case: the trap of the manifest it is judged by, the query, the item whose value at the answer's date is
     *  lite's answer, whether the value is a level (1e-9 absolute) or a volatility (1e-8 relative, the contract's
     *  tolerances), and whether the query faces the trap. */
    private record Case(String key, String trap, String query, String item, boolean relative, boolean facesTrap) {
    }

    /** {@code body} as a query: the island alone. */
    private static String ts(String body) {
        return "|#TS{" + body + "}#";
    }

    private static final List<Case> CASES = List.of(
            new Case("holiday_gap", "holiday_gap",
                    ts("SELECT returns(CLOSE['ALPHA'], 1) AS v FROM PRICES ON DATE BY SYMBOL"), "v", false, true),
            new Case("fill_prev_series_start", "fill_prev_series_start",
                    ts("SELECT CLOSE['ALPHA'] - CLOSE['NEWCO'] AS v FROM PRICES ON DATE BY SYMBOL FILL(PREV)"), "v",
                    false, true),
            new Case("weekly_sample_holiday_end", "weekly_sample_holiday_end",
                    ts("SELECT last(CLOSE['ALPHA']) AS v FROM PRICES ON DATE BY SYMBOL SAMPLE BY 1w"), "v", false,
                    true),
            new Case("late_print", "late_print",
                    ts("SELECT returns(CLOSE['BRAVO'], 1) AS v FROM PRICES ON DATE BY SYMBOL"), "v", false, true),
            // the line as it arrived, its 2025-02-05 print and the print's correction: the correct weekly sum takes
            // the correction alone (last print wins, the greatest seq)
            new Case("duplicate_row", "duplicate_row",
                    ts("SELECT sum(VOLUME['BTCUSD']) AS v FROM PRICES ON DATE BY SYMBOL SAMPLE BY 1w"), "v", false,
                    true),
            // ... and over the rows deduplicated by hand in Pure by (symbol, date), the greatest seq kept (a #TS
            // source's subquery takes neither DISTINCT nor GROUP BY), the relation handed to #TS: the fix, not the trap
            new Case("duplicate_row/last_print_source", "duplicate_row",
                    "|let px = #>{adv::DB.PRICES}#->extend(over(~[SYMBOL, DATE], [~SEQ->descending()]),"
                            + " ~rn:{p,f,r|$p->rowNumber($r)})->filter(r|$r.rn == 1);"
                            + " #TS{SELECT sum(VOLUME['BTCUSD']) AS v FROM $px ON DATE BY SYMBOL SAMPLE BY 1w}#;",
                    "v", false, false),
            // ... and in #TS itself, the source naming its sequence: LATEST BY SEQ keeps each date's greatest (the
            // user's ruling of 2026-11-08, "Last print wins", spelled "A clause: LATEST BY seq")
            new Case("duplicate_row/latest_by_seq", "duplicate_row",
                    ts("SELECT sum(VOLUME['BTCUSD']) AS v FROM PRICES ON DATE BY SYMBOL LATEST BY SEQ SAMPLE BY 1w"),
                    "v", false, true),
            // the trap is a change-only filter lite never applies: evidence of the word, not of the trap
            new Case("stale_price_5d", "stale_price_5d",
                    ts("SELECT volatility(CLOSE['BRAVO'], 10, annualization_factor => 252) AS v FROM PRICES ON DATE"
                            + " BY SYMBOL"), "v", true, false),
            // the returns over the rows with the split applied (prices_adjusted.csv): the trap undone by the file
            new Case("split_2_for_1", "split_2_for_1",
                    ts("SELECT returns(CLOSE['BRAVO'], 1) AS v FROM PRICES_ADJUSTED ON DATE BY SYMBOL"), "v", false,
                    false),
            new Case("tenor_3m_month_end", "tenor_3m_month_end",
                    ts("SELECT returns(CLOSE['ALPHA'], 3m) AS v FROM PRICES ON DATE BY SYMBOL"), "v", false, true),
            // R15's wavg(price, volume) is ruled and not yet a word: the week's VWAP as the ratio of its sums, the
            // formula written by hand
            new Case("vwap_zero_volume", "vwap_zero_volume",
                    ts("SELECT sum(t.PV['BRAVO']) / sum(t.VOLUME['BRAVO']) AS v FROM (SELECT *, CLOSE * VOLUME AS PV"
                            + " FROM PRICES) t ON DATE BY SYMBOL SAMPLE BY 1w"), "v", false, false));

    @Test
    void everyTrapBitesTheNaiveSqlAndEachCaseHasItsLedgerStatus() throws Exception {
        Map<String, Trap> traps = manifest(Runfile.property("ts.adversarial.manifest"));
        assertEquals(new java.util.TreeSet<>(traps.keySet()),
                new java.util.TreeSet<>(CASES.stream().map(Case::trap).toList()), "a case for every trap, and no other");
        TestBackend backend = TestBackends.named(backendName());
        com.legend.compiler.element.ModelContext model = Compiler.compileModel(model(backend));
        List<String> problems = new ArrayList<>();
        Map<String, Outcome> outcomes = new LinkedHashMap<>();
        try (Connection c = backend.open()) {
            seed(backend, c);
            for (Trap t : traps.values()) {
                // a holiday is the calendar's: a closed day on its market between two open ones
                if (t.holiday() != null) {
                    problems.addAll(holiday(c, t));
                }
                // the naive SQL, as the manifest writes it: its answer, and never the correct one
                Answer naive = naive(c, t.naiveSql());
                if (!naive.matches(t.naive(), false)) {
                    problems.add("NAIVE: " + t.name() + "'s naive SQL answers " + naive + ", the manifest says "
                            + t.naive());
                }
                if (naive.matches(t.correct(), false)) {
                    problems.add("NAIVE: " + t.name() + "'s naive SQL answers the correct " + t.correct()
                            + ": the trap does not bite");
                }
                // ... and on a neighbouring date its control answer, a value
                if (t.controlSql() != null) {
                    Answer control = naive(c, t.controlSql());
                    if (t.control() == null || t.control().value() == null || !control.matches(t.control(), false)) {
                        problems.add("CONTROL: " + t.name() + "'s naive SQL on its neighbouring date answers "
                                + control + ", the manifest says " + t.control());
                    }
                }
            }
            for (Case k : CASES) {
                Trap t = java.util.Objects.requireNonNull(traps.get(k.trap()));
                Outcome o;
                try {
                    o = judge(k, t, rows(model, c, k.query()));
                } catch (LegendCompileException | DataError e) {
                    o = new Outcome("REFUSED", k.facesTrap(), firstLine(e), "");
                }
                System.out.println("[ts-adversarial] " + k.key() + "\t" + o.status() + "\t"
                        + (o.facesTrap() ? "yes" : "no") + "\t" + o.value() + "\t" + o.detail());
                outcomes.put(k.key(), o);
            }
        }
        problems.addAll(ratchet(outcomes, Runfile.property("ts.adversarial.ledger"),
                System.getProperty("ts.adversarial.update")));
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    @Test
    void sampleByLastOfADatesTiedRowsIsRefusedWhateverTheirLoadOrder() throws Exception {
        // The finding this fixture measured (PR #90, finding 3): SAMPLE BY's last(x) took a period's row by its number
        // in time, its date's seconds, so a date's two rows tied and the database picked -- the same rows loaded in two
        // orders answered two closes. Closed 2026-11-08 (the user's ruling on duplicate prints, "Last print wins, else
        // refuse"): a tie at the moment last(x) or first(x) picks is refused while the query runs, naming the fix
        // (timeseries::lines::onceTaken), in either load order; a source that names its sequence column is the
        // ruling's other half, its spelling to design.
        Map<String, Trap> traps = manifest(Runfile.property("ts.adversarial.manifest"));
        String date = java.util.Objects.requireNonNull(traps.get("duplicate_row").duplicate(), "duplicate_row's date");
        TestBackend backend = TestBackends.named(backendName());
        com.legend.compiler.element.ModelContext model = Compiler.compileModel(model(backend));
        try (Connection c = backend.open()) {
            seed(backend, c);
            List<Double> closes = new ArrayList<>();
            try (java.sql.PreparedStatement ps = c.prepareStatement(
                    "SELECT CLOSE FROM PRICES WHERE SYMBOL = 'BTCUSD' AND DATE = ? ORDER BY SEQ")) {
                ps.setObject(1, java.time.LocalDate.parse(date));
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        closes.add(rs.getDouble(1));
                    }
                }
            }
            assertEquals(2, closes.size(), "the duplicate's date has two rows, the print and its correction");
            for (String table : List.of("PRICES", "PRICES_REVERSED")) {
                for (String taken : List.of("last", "first")) {
                    String q = ts("SELECT " + taken + "(CLOSE['BTCUSD']) AS v FROM " + table + " ON DATE BY SYMBOL"
                            + " SAMPLE BY 1d");
                    Exception e = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> rows(model, c, q),
                            table + ": " + taken + "(x) of " + date + "'s tie");
                    assertTrue(String.valueOf(e.getMessage()).contains("under SAMPLE BY met a line with several rows at"
                            + " one moment"), table + ": " + taken + "(x) refused by name: " + e.getMessage());
                }
            }
            // a line of no tie answers as it did: ALPHA's weeks
            assertTrue(!rows(model, c, ts("SELECT last(CLOSE['ALPHA']) AS v FROM PRICES ON DATE BY SYMBOL"
                    + " SAMPLE BY 1d")).isEmpty(), "a line of one row a date");
            // the source naming its sequence, LATEST BY SEQ: the correction, whatever the load order -- last and first
            // alike, the date one row
            for (String table : List.of("PRICES", "PRICES_REVERSED")) {
                for (String taken : List.of("last", "first")) {
                    List<Map<String, String>> on = on(rows(model, c, ts("SELECT " + taken + "(CLOSE['BTCUSD']) AS v FROM "
                            + table + " ON DATE BY SYMBOL LATEST BY SEQ SAMPLE BY 1d")), date);
                    assertEquals(1, on.size(), table + ": one period on " + date);
                    assertEquals(closes.get(closes.size() - 1), value(on, "v"), table + ": " + taken
                            + "(x) LATEST BY SEQ is the correction");
                }
            }
        }
    }

    // ------------------------------------------------------------------ the database

    /** The database the target names ({@code -Dts.backend}, a test backend's name): no default, the BUILD file says. */
    private static String backendName() {
        String name = System.getProperty("ts.backend");
        if (name == null) {
            throw new IllegalStateException("-Dts.backend is not set: the BUILD file names the database");
        }
        return name;
    }

    private static String model(TestBackend backend) throws IOException {
        return STORE + """
                ###Connection
                RelationalDatabaseConnection adv::Conn
                {
                  store: adv::DB;
                  %s
                }
                ###Runtime
                Runtime adv::RT { mappings: [ ]; connections: [ adv::DB: [ c: adv::Conn ] ]; }
                """.formatted(backend.connectionBody())
                + Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
    }

    /** The tables, as the dialect writes them, and the fixture's rows: prices.csv's and prices_adjusted.csv's in their
     *  files' order (a late print and a corrected print where they arrived), prices.csv's again in the reverse order,
     *  and calendar.csv's. */
    private static void seed(TestBackend backend, Connection c) throws Exception {
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(STORE).elements()
                .stream().filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        try (java.sql.Statement s = c.createStatement()) {
            for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
        }
        String columns = "symbol,date,open,high,low,close,volume,seq";
        List<String> prices = rows(Runfile.property("ts.adversarial.prices"), columns);
        List<String> reversed = new ArrayList<>(prices);
        java.util.Collections.reverse(reversed);
        Map<String, List<String>> data = Map.of("PRICES", prices, "PRICES_ADJUSTED",
                rows(Runfile.property("ts.adversarial.prices_adjusted"), columns),
                "PRICES_REVERSED", reversed,
                "CALENDAR", rows(Runfile.property("ts.adversarial.calendar"), "date,market,is_business_day"));
        for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
            List<String> lines = java.util.Objects.requireNonNull(data.get(t.name()), t.name());
            boolean calendar = t.name().equals("CALENDAR");
            String table = backend.dialect().storedName(t.name(), t.quoted());
            try (java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO " + table
                    + (calendar ? " VALUES (?, ?, ?)" : " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"))) {
                for (String line : lines) {
                    String[] x = line.split(",", -1);
                    if (calendar) {
                        ps.setObject(1, java.time.LocalDate.parse(x[0]));
                        ps.setString(2, x[1]);
                        ps.setString(3, x[2]);
                    } else {
                        ps.setString(1, x[0]);
                        ps.setObject(2, java.time.LocalDate.parse(x[1]));
                        for (int i = 2; i < 6; i++) {
                            ps.setDouble(i + 1, Double.parseDouble(x[i]));
                        }
                        ps.setLong(7, Long.parseLong(x[6]));
                        ps.setLong(8, Long.parseLong(x[7]));
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    /** A fixture file's rows, its header the contract's. */
    private static List<String> rows(Path file, String header) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertEquals(header, lines.get(0), file + ": the contract's columns");
        return lines.subList(1, lines.size());
    }

    /** A holiday trap's calendar: its holiday closed on its market, the trading days either side of it open -- read
     *  by the database, the flags compared. */
    private static List<String> holiday(Connection c, Trap t) throws java.sql.SQLException {
        List<String> problems = new ArrayList<>();
        Map<String, String> want = new LinkedHashMap<>();
        want.put(t.dateRange().get(0), "true");
        want.put(java.util.Objects.requireNonNull(t.holiday()), "false");
        want.put(t.dateRange().get(1), "true");
        try (java.sql.PreparedStatement ps = c.prepareStatement(
                "SELECT IS_BUSINESS_DAY FROM CALENDAR WHERE MARKET = ? AND DATE = ?")) {
            for (Map.Entry<String, String> day : want.entrySet()) {
                ps.setString(1, t.market());
                ps.setObject(2, java.time.LocalDate.parse(day.getKey()));
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    String got = rs.next() ? rs.getString(1) : "no row";
                    if (!day.getValue().equals(got)) {
                        problems.add("CALENDAR: " + t.name() + ": " + t.market() + " on " + day.getKey()
                                + " is_business_day " + got + ", the trap needs " + day.getValue());
                    }
                }
            }
        }
        return problems;
    }

    /** {@code query}'s rows, each by its column's name: a value's text, or null where it has none. */
    private static List<Map<String, String>> rows(com.legend.compiler.element.ModelContext model, Connection c,
            String query) throws Exception {
        ExecutionPlan plan = Compiler.query(model, query).withOptions(Lean.TEST_OPTIONS).executionPlan("adv::RT");
        String text = PlanCases.write(plan, c, Map.of(), PlanCases.Written.CSV::writer);
        List<String> lines = new ArrayList<>(Arrays.asList(text.split("\r\n")));
        List<String> header = Arrays.asList(lines.remove(0).split(",", -1));
        List<Map<String, String>> out = new ArrayList<>();
        for (String line : lines) {
            String[] cells = line.split(",", -1);
            assertEquals(header.size(), cells.length, line);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < cells.length; i++) {
                row.put(header.get(i), cells[i].isEmpty() ? null : cells[i]);
            }
            out.add(row);
        }
        return out;
    }

    /** A case's outcome, of lite's rows: PASS where the item's value on the correct answer's date is the correct one
     *  (and, where the trap names a witness, the value on its date is the witness's), DIFF otherwise -- several rows
     *  on one date among them; its value lite's on the answer's date. */
    private static Outcome judge(Case k, Trap t, List<Map<String, String>> rows) {
        List<Map<String, String>> on = on(rows, t.correct().date());
        List<Map<String, String>> seen = t.witness() == null ? List.of() : on(rows, t.witness().date());
        if (on.size() > 1 || seen.size() > 1) {
            return new Outcome("DIFF", k.facesTrap(), "several rows", "on one date: " + (on.size() > 1 ? on : seen));
        }
        Answer lite = new Answer(t.correct().date(), value(on, k.item()));
        Answer witness = t.witness() == null ? null : new Answer(t.witness().date(), value(seen, k.item()));
        boolean witnessed = t.witness() == null || witness.matches(t.witness(), k.relative());
        String value = lite.value() == null ? "none" : String.valueOf(lite.value());
        return lite.matches(t.correct(), k.relative()) && witnessed ? new Outcome("PASS", k.facesTrap(), value, "")
                : new Outcome("DIFF", k.facesTrap(), value, "lite " + lite + ", correct " + t.correct()
                        + (witnessed ? "" : "; on the witness's date lite " + witness + ", the witness " + t.witness()));
    }

    /** Lite's rows dated {@code date}. */
    private static List<Map<String, String>> on(List<Map<String, String>> rows, String date) {
        return rows.stream().filter(r -> date.equals(r.get("DATE"))).toList();
    }

    /** The item's value on the one row of {@code on}, none where there is no row or no value; a row without the item's
     *  column is the test's own error, never lite's refusal. */
    private static @com.legend.base.Nullable Double value(List<Map<String, String>> on, String item) {
        if (on.isEmpty()) {
            return null;
        }
        if (!on.get(0).containsKey(item)) {
            throw new AssertionError("no column " + item + " in " + on.get(0).keySet() + ": the case names its item");
        }
        String v = on.get(0).get(item);
        return v == null ? null : Double.valueOf(v);
    }

    /** The naive SQL's answer: its one row's {@code date} and {@code value}. A naive SQL answering no row, several,
     *  no date or another column is the fixture's error. */
    private static Answer naive(Connection c, String sql) throws java.sql.SQLException {
        try (java.sql.Statement s = c.createStatement(); java.sql.ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("a naive SQL answers one row, its date and its value; this one none: " + sql);
            }
            java.sql.ResultSetMetaData md = rs.getMetaData();
            String date = null;
            Double value = null;
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (md.getColumnLabel(i).equalsIgnoreCase("date")) {
                    date = rs.getString(i);
                } else if (md.getColumnLabel(i).equalsIgnoreCase("value")) {
                    Object o = rs.getObject(i);
                    value = o == null ? null : ((Number) o).doubleValue();
                } else {
                    throw new AssertionError("a naive SQL answers date and value, not " + md.getColumnLabel(i));
                }
            }
            if (date == null) {
                throw new AssertionError("a naive SQL answers its date: " + sql);
            }
            if (rs.next()) {
                throw new AssertionError("a naive SQL answers one row, not several: " + sql);
            }
            return new Answer(date, value);
        }
    }

    private static String firstLine(RuntimeException e) {
        return String.valueOf(e.getMessage()).lines().findFirst().orElse("");
    }

    // ------------------------------------------------------------------ the answers

    /** A value read at a date (none: no value); both always dated. */
    private record Answer(String date, @com.legend.base.Nullable Double value) {

        /** Whether this answer is {@code want}: the same date, and the same value within the tolerance, or none for
         *  none. */
        boolean matches(Answer want, boolean relative) {
            if (!date.equals(want.date())) {
                return false;
            }
            if (value == null || want.value() == null) {
                return value == null && want.value() == null;
            }
            double tolerance = relative ? 1e-8 * Math.abs(want.value()) : 1e-9;
            return Math.abs(value - want.value()) <= tolerance;
        }

        @Override
        public String toString() {
            return "{date: " + date + ", value: " + value + "}";
        }
    }

    /** A trap of the manifest: its naive SQL and answer, its correct answer, its control (the naive SQL on a
     *  neighbouring date and that answer), a witness, its holiday on its market, the duplicate's date. */
    private record Trap(String name, List<String> dateRange, String naiveSql, Answer naive, Answer correct,
            @com.legend.base.Nullable String controlSql, @com.legend.base.Nullable Answer control,
            @com.legend.base.Nullable Answer witness, @com.legend.base.Nullable String holiday,
            @com.legend.base.Nullable String market, @com.legend.base.Nullable String duplicate) {
    }

    /** The manifest's traps, read from the subset of YAML generate.py writes: a trap a list item ({@code - name:}),
     *  its fields one a line, the SQL literal blocks, the answers flow maps of a date and a value, a date range a list
     *  of two dates. A field these tests read and cannot is refused. */
    static Map<String, Trap> manifest(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        Map<String, Trap> out = new LinkedHashMap<>();
        int i = lines.indexOf("traps:");
        if (i < 0) {
            throw new IllegalStateException(file + " has no traps:");
        }
        i++;
        while (i < lines.size()) {
            String name = after(lines.get(i), "  - name: ");
            Map<String, String> fields = new LinkedHashMap<>();
            i++;
            while (i < lines.size() && lines.get(i).startsWith("    ")) {
                String line = lines.get(i);
                String key = line.substring(4, line.indexOf(':'));
                String value = line.substring(line.indexOf(':') + 1).strip();
                i++;
                if (value.equals("|")) {
                    StringBuilder block = new StringBuilder();
                    while (i < lines.size() && lines.get(i).startsWith("      ")) {
                        block.append(lines.get(i).substring(6)).append('\n');
                        i++;
                    }
                    value = block.toString();
                }
                if (fields.put(key, value) != null) {
                    throw new IllegalStateException("the trap " + name + " has " + key + " twice");
                }
            }
            if (out.put(name, new Trap(name, dates(required(fields, "date_range", name)),
                    required(fields, "naive_sql", name), answer(required(fields, "naive_sql_answer", name)),
                    answer(required(fields, "correct_answer", name)), fields.get("control_sql"),
                    fields.containsKey("control_answer") ? answer(fields.get("control_answer")) : null,
                    fields.containsKey("witness") ? answer(fields.get("witness")) : null, quoted(fields.get("holiday")),
                    quoted(fields.get("market")), quoted(fields.get("duplicate")))) != null) {
                throw new IllegalStateException("the manifest has the trap " + name + " twice");
            }
        }
        return out;
    }

    private static String after(String line, String prefix) {
        if (!line.startsWith(prefix)) {
            throw new IllegalStateException("expected '" + prefix + "...', read '" + line + "'");
        }
        return line.substring(prefix.length());
    }

    private static String required(Map<String, String> fields, String key, String trap) {
        String v = fields.get(key);
        if (v == null) {
            throw new IllegalStateException("the trap " + trap + " has no " + key);
        }
        return v;
    }

    /** A quoted scalar's text ({@code 'NYSE'}), none for none. */
    private static @com.legend.base.Nullable String quoted(@com.legend.base.Nullable String scalar) {
        if (scalar == null) {
            return null;
        }
        if (!scalar.startsWith("'") || !scalar.endsWith("'")) {
            throw new IllegalStateException("a quoted scalar is '...': " + scalar);
        }
        return scalar.substring(1, scalar.length() - 1).replace("''", "'");
    }

    /** {@code ['YYYY-MM-DD', 'YYYY-MM-DD']}. */
    private static List<String> dates(String flow) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\\['(\\d{4}-\\d{2}-\\d{2})', '(\\d{4}-\\d{2}-\\d{2})']").matcher(flow);
        if (!m.matches()) {
            throw new IllegalStateException("a date range is ['YYYY-MM-DD', 'YYYY-MM-DD']: " + flow);
        }
        return List.of(m.group(1), m.group(2));
    }

    /** {@code {date: 'YYYY-MM-DD', value: <number or null>}}. */
    private static Answer answer(String flow) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\\{date: '(\\d{4}-\\d{2}-\\d{2})', value: (null|-?[0-9.eE+-]+)}").matcher(flow);
        if (!m.matches()) {
            throw new IllegalStateException("an answer is {date: 'YYYY-MM-DD', value: number or null}: " + flow);
        }
        return new Answer(m.group(1), m.group(2).equals("null") ? null : Double.valueOf(m.group(2)));
    }

    // ------------------------------------------------------------------ the ledger

    /** A case's outcome: its status, whether it faces its trap, lite's value on the answer's date (or the refusal's
     *  first line), the detail. */
    private record Outcome(String status, boolean facesTrap, String value, String detail) {
    }

    /** The ledger's header: a case, its status on the target's database (the lane's name), whether it faces its trap,
     *  lite's value, the detail. */
    private static String header() {
        return "case\t" + backendName() + "\tfaces_trap\tvalue\tdetail";
    }

    /** The parity lanes' ratchet over this target's cases (SqlParity.ratchet's statuses and problems), the value and
     *  faces_trap compared too: each case against its ledger row; with {@code update} set, the ledger rewritten there
     *  instead. A case twice in the ledger is a problem. */
    private static List<String> ratchet(Map<String, Outcome> outcomes, Path ledger,
            @com.legend.base.Nullable String update) throws IOException {
        if (update != null) {
            StringBuilder b = new StringBuilder("# the adversarial fixture's cases, each one's status on the target's"
                    + " database, whether it faces its trap and lite's value: the ratchet TsAdversarialTest holds\n"
                    + header() + "\n");
            outcomes.forEach((k, o) -> b.append(k).append('\t').append(o.status()).append('\t')
                    .append(o.facesTrap() ? "yes" : "no").append('\t').append(o.value()).append('\t')
                    .append(o.detail()).append('\n'));
            Files.writeString(Path.of(update), b.toString(), StandardCharsets.UTF_8);
            return List.of();
        }
        List<String> problems = new ArrayList<>();
        Map<String, String[]> pinned = new LinkedHashMap<>();
        for (String line : Files.readAllLines(ledger, StandardCharsets.UTF_8)) {
            if (line.startsWith("#") || line.equals(header()) || line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 5) {
                problems.add("LEDGER: a row is case, status, faces_trap, value, detail: " + line);
            } else if (pinned.put(f[0], f) != null) {
                problems.add("LEDGER: " + f[0] + " is in the ledger twice");
            }
        }
        outcomes.forEach((k, o) -> {
            String[] was = pinned.get(k);
            if (was == null) {
                problems.add("NEW: " + k + " is " + o.status() + " and not in the ledger" + detail(o));
            } else if (!was[1].equals(o.status())) {
                problems.add((was[1].equals("PASS") ? "REGRESSION" : o.status().equals("PASS") ? "FIX DETECTED"
                        : "STATUS MISMATCH") + ": " + k + " was " + was[1] + ", now " + o.status() + detail(o));
            } else if (!was[3].equals(o.value())) {
                problems.add("VALUE MOVED: " + k + " was " + was[3] + ", now " + o.value() + detail(o));
            } else if (!was[2].equals(o.facesTrap() ? "yes" : "no")) {
                problems.add("FACES_TRAP MOVED: " + k + " was " + was[2] + " in the ledger");
            }
        });
        pinned.keySet().stream().filter(k -> !outcomes.containsKey(k))
                .forEach(k -> problems.add("GONE: " + k + " is in the ledger and not a case"));
        return problems;
    }

    private static String detail(Outcome o) {
        return o.detail().isEmpty() ? "" : " (" + o.detail() + ")";
    }
}
