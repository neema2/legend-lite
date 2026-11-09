// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.sql.dialect.CatalogModel;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE GS-QUANT PARITY OF #TS (CLAUDE.local.md's contract; tools/gs-quant-oracle/README.md): every case of the oracle's
 * mapping ({@code tools/gs-quant-oracle/mapping.yaml}) -- a word of the timeseries library as a {@code #TS} item over
 * the gallery's samples (//datacube:ts_replay_samples) -- run through the Pure library on DuckDB and judged against
 * gs-quant's own answer, snapped by {@code tools/gs-quant-oracle/snap.py} into {@code answers.jsonl.gz}: the same dates,
 * each value within 1e-9 of its series' scale, never less than 1e-9 (a level: the user's ruling of 2026-11-08, as the
 * replays judge; it was 1e-9 absolute), or 1e-8 relative for a volatility, a correlation or a beta. Each case's
 * verdict -- PASS, DIFF, REFUSED (lite refuses it), GS_ERROR (gs-quant raised), SKIP (no gs-quant twin) -- is pinned in
 * the ledger's {@code duckdb} column ({@code tools/gs-quant-oracle/ledger.tsv}); a verdict that moves fails the test.
 * Run with {@code --jvm_flag=-Dts.gsquant.update=<tools/gs-quant-oracle/ledger.tsv in the workspace>} to rewrite the
 * column. The answers are checked against the mapping first (UNSNAPPED, STALE, GONE, VERSION fail the test).
 *
 * <p>A DIFF is a finding, never a reason to edit a word: each is written, with its first points apart, gs-quant's
 * source line, the {@code #TS} statement and lite's SQL, to the test's undeclared outputs ({@code gsquant-findings.md}).
 * A DIFF a register row rules carries the row in the ledger's {@code register} column.
 *
 * <p>The tolerance is the contract's. The gallery's oracle ({@code tools/ts-oracle}) holds the drawn series to
 * gs-quant at 1e-6 relative; this holds every word to gs-quant's own function, case by case.
 */
class TsGsquantParityTest {

    /** The lines a case's {@code {x}} and {@code {y}} name over the samples (snap.py's X and Y). */
    private static final String X = "NVDA";
    private static final String Y = "SPX";

    /** The words judged at 1e-8 relative: a volatility, a correlation, a beta (the contract's). */
    private static final Set<String> RELATIVE = Set.of("volatility", "exponential_volatility",
            "exponential_spread_volatility", "vol_swap_volatility", "correlation", "beta");

    private static final double LEVEL = 1e-9;
    private static final double RATIO = 1e-8;

    /** The samples' tables (TsReplayTest's columns), named as the cases' statements read them. */
    private static final Map<String, List<CatalogModel.Column>> SAMPLES = new LinkedHashMap<>();

    static {
        SAMPLES.put("prices", List.of(text("symbol"), text("asset_class"), date("date"), number("open"),
                number("high"), number("low"), number("close"), whole("volume")));
        SAMPLES.put("rates", List.of(text("tenor"), number("years"), date("date"), number("yield")));
        SAMPLES.put("macro", List.of(text("series"), date("date"), number("value")));
    }

    /** The samples the gallery's series read (TsReplayTest's): the cases' and the cash (item 13b). */
    private static final Map<String, List<CatalogModel.Column>> GALLERY_SAMPLES = new LinkedHashMap<>(SAMPLES);

    static {
        GALLERY_SAMPLES.put("cash", List.of(text("currency"), date("date"), number("rate"), number("index")));
    }

    private static CatalogModel.Column text(String name) {
        return new CatalogModel.Column(name, "VARCHAR", "VARCHAR", null, null, false);
    }

    private static CatalogModel.Column date(String name) {
        return new CatalogModel.Column(name, "DATE", "DATE", null, null, false);
    }

    private static CatalogModel.Column number(String name) {
        return new CatalogModel.Column(name, "DOUBLE", "DOUBLE", null, null, false);
    }

    private static CatalogModel.Column whole(String name) {
        return new CatalogModel.Column(name, "BIGINT", "BIGINT", null, null, false);
    }

    // ------------------------------------------------------------------ the mapping (snap.py's line_cases mirrors it)

    /** One case: its word, its args, its #TS item, its gs-quant text, the struct field it reads (or null), the
     *  register row that rules a DIFF on it (or "-"), and its word's gs-quant source line. */
    record Case(String word, String args, String ts, String gs, String column, String register, String src) {
        String key() {
            return word + " " + args;
        }
    }

    /** The ledger's spelling of "no register row rules this case" (a case of mapping.yaml with no {@code known:}). */
    static final String NO_REGISTER_ROW = "-";

    /** A word of the mapping: its cases, or none where it is SKIP; ruled, a row that is no word yet (R15's wavg). */
    record Word(String word, boolean skip, List<Case> cases, boolean ruled) {
    }

    private static final Pattern WORD = Pattern.compile("  - word: (\\S+)");
    private static final Pattern SKIP = Pattern.compile("  - \\{word: (\\w+),.*");
    private static final Pattern KNOWN = Pattern.compile("    known: \\{(.*)}");
    private static final Pattern SRC = Pattern.compile("    src: (\\S+)");
    private static final Pattern CASE = Pattern.compile("      - \\[(.*)]");

    /** The mapping read line by line (snap.py's {@code line_cases}, which holds its YAML reader to this). */
    static List<Word> mapping(Path file) throws IOException {
        List<Word> words = new ArrayList<>();
        String word = null;
        String src = "";
        Map<String, String> known = new LinkedHashMap<>();
        List<String[]> rows = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            Matcher m;
            if ((m = WORD.matcher(line)).matches() || (m = SKIP.matcher(line)).matches()) {
                if (word != null) {
                    words.add(word(word, src, known, rows));
                }
                word = m.group(1);
                src = "";
                known = new LinkedHashMap<>();
                rows = new ArrayList<>();
                if (m.pattern() == SKIP) {
                    words.add(new Word(word, true, List.of(), line.contains(" ruled: ")));
                    word = null;
                }
            } else if ((m = KNOWN.matcher(line)).matches()) {
                for (String part : m.group(1).split(", ")) {
                    String[] kv = part.split(": ");
                    // a key with a comma is double-quoted (YAML's flow map splits a plain one there)
                    known.put(kv[0].startsWith("\"") && kv[0].endsWith("\"") ? kv[0].substring(1, kv[0].length() - 1) : kv[0], kv[1]);
                }
            } else if ((m = SRC.matcher(line)).matches()) {
                src = m.group(1);
            } else if ((m = CASE.matcher(line)).matches()) {
                rows.add(flowScalars(m.group(1)).toArray(String[]::new));
            }
        }
        if (word != null) {
            words.add(word(word, src, known, rows));
        }
        return words;
    }

    private static Word word(String word, String src, Map<String, String> known, List<String[]> rows) {
        List<Case> cases = new ArrayList<>();
        for (String[] r : rows) {
            cases.add(new Case(word, r[0], r[1], r[2], r.length > 3 ? r[3] : null,
                    known.containsKey(r[0]) ? known.get(r[0]) : NO_REGISTER_ROW,
                    src));
        }
        return new Word(word, false, cases, false);
    }

    /** A case's gs text that is Show Python's call: the case's #TS item as the printer prints it, read from calls.tsv. */
    static final String PRINTED = "printed";

    /**
     * The mapping with each case's gs-quant call resolved: the mapping's own text, or where it writes {@code printed},
     * Show Python's -- calls.tsv, which //datacube:timeseries_builder_test prints from the case's #TS item (item 14f,
     * step 2: the oracle's gs-quant side the printer's; snap.py's {@code resolved}, the same rule). A {@code printed}
     * case the printer prints no call for is this test's error, never a verdict.
     */
    static List<Word> printed(List<Word> words, Path callsFile) throws IOException {
        Map<String, String> calls = new LinkedHashMap<>();
        for (String line : Files.readAllLines(callsFile, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#") || line.startsWith("case\t")) {
                continue;
            }
            int tab = line.indexOf('\t');
            calls.put(line.substring(0, tab), line.substring(tab + 1));
        }
        List<Word> out = new ArrayList<>();
        for (Word w : words) {
            List<Case> cases = new ArrayList<>();
            for (Case c : w.cases()) {
                if (!PRINTED.equals(c.gs())) {
                    cases.add(c);
                    continue;
                }
                String call = calls.get(c.key());
                if (call == null || call.equals("STATEMENT") || call.startsWith("REFUSED")) {
                    throw new IllegalStateException("PRINTED: " + c.key() + " is `printed`, and calls.tsv holds "
                            + (call == null ? "no row for it" : call) + " (rewrite calls.tsv, or write its call)");
                }
                cases.add(new Case(c.word(), c.args(), c.ts(), call, c.column(), c.register(), c.src()));
            }
            out.add(new Word(w.word(), w.skip(), cases, w.ruled()));
        }
        return out;
    }

    /** A YAML flow sequence's scalars as the mapping writes them: "..." (with \" and \\) or plain, split at ", ". */
    static List<String> flowScalars(String text) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            if (text.charAt(i) == '"') {
                StringBuilder buf = new StringBuilder();
                int j = i + 1;
                while (text.charAt(j) != '"') {
                    if (text.charAt(j) == '\\') {
                        j++;
                    }
                    buf.append(text.charAt(j));
                    j++;
                }
                out.add(buf.toString());
                i = j + 1;
            } else {
                int j = text.indexOf(", ", i);
                j = j < 0 ? text.length() : j;
                out.add(text.substring(i, j));
                i = j;
            }
            if (text.startsWith(", ", i)) {
                i += 2;
            }
        }
        return out;
    }

    /** The pins (tools/gs-quant-oracle/pins.env, KEY=VALUE as tools/oracle-pins.env: gs-quant, pandas, numpy, scipy,
     *  statsmodels and Python's minor) as one stack, snap.py's {@code stack}: KEY=VALUE sorted, {@code ;}-joined -- what
     *  every answer's hash and every answers file's metadata hold. A pin missing is refused, never defaulted. */
    static String pin(Path file) throws IOException {
        Map<String, String> pins = new TreeMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            int eq = line.indexOf('=');
            if (!line.startsWith("#") && eq > 0) {
                pins.put(line.substring(0, eq).strip(), line.substring(eq + 1).strip());
            }
        }
        for (String key : List.of("GS_QUANT_VERSION", "PANDAS_VERSION", "NUMPY_VERSION", "SCIPY_VERSION",
                "STATSMODELS_VERSION", "TA_LIB_VERSION", "PYTHON_VERSION")) {
            if (!pins.containsKey(key)) {
                throw new IllegalStateException(file + " has no " + key);
            }
        }
        return String.join(";", pins.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList());
    }

    /** What a case's answer is for (snap.py's sha): its two texts, its column, the data (over a fixture, with the
     *  symbol it runs as x) and gs-quant's release, the pin -- a gallery series' its key in place of its #TS (snap.py's
     *  snap_gallery: gs-quant's answer is its call's alone, and the series' Show #TS is run as it stands). */
    static String sha(Case c, String dataSha, String pin) throws Exception {
        String ts = GALLERY_WORD.equals(c.word()) ? c.args() : c.ts();
        String joined = String.join("\n\0\n", ts, c.gs(), c.column() == null ? "" : c.column(), dataSha, pin);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(
                joined.getBytes(StandardCharsets.UTF_8)));
    }

    // ------------------------------------------------------------------ the answers

    /** The answers file: its meta (each case's sha and gs-quant's status), each case's points by date, and what its
     *  rows' spines say (SPINE: a row whose [first, last, count] is not the samples' own dates). */
    record Answers(Json.Obj meta, Map<String, Map<String, List<Double>>> points, Map<String, String> symbols,
            List<String> problems) {
    }

    /** Each line's dates in the samples, sorted: a price line by its symbol, a macro series by its name. */
    static Map<String, List<String>> lineDates(Json.Obj samples) {
        Map<String, java.util.TreeSet<String>> out = new LinkedHashMap<>();
        for (String[] sample : new String[][] {{"prices", "symbol"}, {"macro", "series"}}) {
            List<String> lines = Arrays.asList(samples.getString(sample[0]).split("\n"));
            List<String> header = Arrays.asList(lines.get(0).split(","));
            int key = header.indexOf(sample[1]);
            int date = header.indexOf("date");
            for (String line : lines.subList(1, lines.size())) {
                if (!line.isBlank()) {
                    String[] cells = line.split(",", -1);
                    out.computeIfAbsent(cells[key], k -> new java.util.TreeSet<>()).add(cells[date]);
                }
            }
        }
        Map<String, List<String>> sorted = new LinkedHashMap<>();
        out.forEach((k, v) -> sorted.put(k, List.copyOf(v)));
        return sorted;
    }

    /** A row's spine (snap.py's spine): the sorted union of the dates of the lines its symbol names. */
    static List<String> spine(String symbol, Map<String, List<String>> lineDates) {
        java.util.TreeSet<String> out = new java.util.TreeSet<>();
        for (String line : symbol.split(",")) {
            out.addAll(java.util.Objects.requireNonNull(lineDates.get(line), "no line " + line + " in the samples"));
        }
        return List.copyOf(out);
    }

    /** The gzipped answers (README.md, "The files"): one row a series, {word, args, symbol, dates, values} -- values
     *  along the row's spine from dates' first to its last, dates [first, last, count] (count checked against the
     *  spine), or the dates themselves for a series off its spine. */
    static Answers answers(Path file, Map<String, List<String>> lineDates) throws IOException {
        Json.Obj meta = null;
        Map<String, Map<String, List<Double>>> points = new LinkedHashMap<>();
        Map<String, String> symbols = new LinkedHashMap<>();
        List<String> problems = new ArrayList<>();
        try (java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(
                new java.util.zip.GZIPInputStream(Files.newInputStream(file)), StandardCharsets.UTF_8))) {
            for (String line = in.readLine(); line != null; line = in.readLine()) {
                if (line.isBlank()) {
                    continue;
                }
                Json.Obj o = Json.parseObject(line);
                if (o.has("meta")) {
                    meta = ((Json.Obj) o.get("meta"));
                    continue;
                }
                // over a fixture (its meta names one) a case's key carries the symbol it ran as x, snap.py's
                String key = o.getString("word") + " " + o.getString("args")
                        + (meta != null && meta.get("fixture") instanceof Json.Str
                                ? " @" + o.getString("symbol").split(",")[0] : "");
                symbols.put(key, o.getString("symbol"));
                List<Json.Node> dates = ((Json.Arr) o.get("dates")).items();
                List<Json.Node> values = ((Json.Arr) o.get("values")).items();
                List<String> along;
                if (dates.size() == 3 && dates.get(2) instanceof Json.Num count) {
                    if (dates.get(0) instanceof Json.Null) {
                        along = List.of();
                    } else {
                        List<String> spine = spine(o.getString("symbol"), lineDates);
                        int first = spine.indexOf(((Json.Str) dates.get(0)).value());
                        int last = spine.indexOf(((Json.Str) dates.get(1)).value());
                        along = first < 0 || last < first ? List.of() : spine.subList(first, last + 1);
                    }
                    if (along.size() != count.longValue() || along.size() != values.size()) {
                        problems.add("SPINE: " + key + ": dates " + dates + " over " + along.size() + " of the samples'"
                                + " dates, " + values.size() + " values");
                        continue;
                    }
                } else {
                    along = dates.stream().map(d -> ((Json.Str) d).value()).toList();
                    if (along.size() != values.size()) {
                        problems.add("SPINE: " + key + ": " + along.size() + " dates of its own, " + values.size()
                                + " values");
                        continue;
                    }
                }
                // a date's every value, in the row's order (a row of its own dates may name one date twice)
                Map<String, List<Double>> series = new TreeMap<>();
                for (int i = 0; i < along.size(); i++) {
                    Json.Node v = values.get(i);
                    if (v instanceof Json.Null) {
                        continue;
                    }
                    series.computeIfAbsent(along.get(i), k -> new ArrayList<>()).add(v instanceof Json.Str s ? switch (s.value()) {
                        case "Inf" -> Double.POSITIVE_INFINITY;
                        case "-Inf" -> Double.NEGATIVE_INFINITY;
                        default -> throw new IllegalStateException("a value " + s.value());
                    } : ((Json.Num) v).doubleValue());
                }
                points.put(key, series);
            }
        }
        if (meta == null) {
            throw new IllegalStateException(file + " has no meta line");
        }
        return new Answers(meta, points, symbols, problems);
    }

    /** UNSNAPPED, STALE and GONE: the answers against the cases a dataset runs (each key and its sha); an answers file
     *  snapped under another release than the pin is STALE whole. */
    static List<String> guards(Map<String, String> expected, Answers answers, String pin, String name) {
        List<String> out = new ArrayList<>();
        if (!pin.equals(answers.meta().getString("pins"))) {
            out.add("STALE: " + name + "'s answers were snapped under " + answers.meta().getString("pins")
                    + ", the pins are " + pin + " (tools/gs-quant-oracle/snap.sh --write)");
        }
        Json.Obj snapped = (Json.Obj) answers.meta().get("cases");
        expected.forEach((key, sha) -> {
            if (!snapped.has(key)) {
                out.add("UNSNAPPED: " + name + " " + key + " (tools/gs-quant-oracle/snap.sh --write)");
            } else if (!((Json.Obj) snapped.get(key)).getString("sha").equals(sha)) {
                out.add("STALE: " + name + " " + key + " (its texts, its data or the pin moved since the snap)");
            }
        });
        for (String k : snapped.fields().keySet()) {
            if (!expected.containsKey(k)) {
                out.add("GONE: " + name + " " + k);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ the lite side

    /** A price file's columns by NAME, typed as the samples' (a fixture's trailing {@code seq}, its ingest order, a
     *  whole number); a name this test does not know is refused, never guessed. */
    static List<CatalogModel.Column> columnsOf(String header) {
        List<CatalogModel.Column> out = new ArrayList<>();
        for (String name : header.strip().split(",")) {
            out.add(switch (name) {
                case "symbol", "asset_class", "tenor", "series" -> text(name);
                case "date" -> date(name);
                case "open", "high", "low", "close", "years", "yield", "value" -> number(name);
                case "volume", "seq" -> whole(name);
                default -> throw new IllegalStateException("a price file's column " + name + ": no type for it");
            });
        }
        return out;
    }

    /** The tables' DDL, a connection, a runtime and the library. */
    private static String model(TestBackend backend, Map<String, List<CatalogModel.Column>> tableColumns)
            throws IOException {
        List<CatalogModel.Table> tables = new ArrayList<>();
        tableColumns.forEach((name, columns) -> tables.add(new CatalogModel.Table(null, name, columns, true)));
        return CatalogModel.database("local::DB", tables, backend.dialect()).text() + """

                ###Connection
                RelationalDatabaseConnection local::Conn
                {
                  store: local::DB;
                  %s
                }
                ###Runtime
                Runtime local::RT { mappings: [ ]; connections: [ local::DB: [ c: local::Conn ] ]; }
                """.formatted(backend.connectionBody())
                + Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8);
    }

    /** Each table's CSV rows into {@code c} (TsReplayTest's seed), each cell read by its column's name. */
    private static void seed(TestBackend backend, Connection c, Map<String, String> csv,
            Map<String, List<CatalogModel.Column>> tableColumns, String model) throws Exception {
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(model).elements()
                .stream().filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
            try (java.sql.Statement s = c.createStatement()) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
            List<CatalogModel.Column> columns = tableColumns.get(t.name());
            List<String> lines = Arrays.asList(csv.get(t.name()).split("\n"));
            List<String> header = Arrays.asList(lines.get(0).strip().split(","));
            String marks = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + backend.dialect().storedName(t.name(),
                    t.quoted()) + " VALUES (" + marks + ")")) {
                for (String line : lines.subList(1, lines.size())) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] cells = line.strip().split(",", -1);
                    for (int i = 0; i < columns.size(); i++) {
                        String v = cells[header.indexOf(columns.get(i).name())];
                        switch (columns.get(i).dataType()) {
                            case "VARCHAR" -> ps.setString(i + 1, v);
                            case "DOUBLE" -> ps.setDouble(i + 1, Double.parseDouble(v));
                            case "BIGINT" -> ps.setLong(i + 1, Long.parseLong(v));
                            case "DATE" -> ps.setObject(i + 1, LocalDate.parse(v));
                            default -> throw new IllegalStateException("a sample column of type "
                                    + columns.get(i).dataType());
                        }
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    /** A case's #TS statement: its item over the prices, {x} and {y} the two lines' closes, {x.high} (any price column)
     *  x's symbol's column (a reference word's lines: item 14f, step 4); or its own whole statement. */
    static String statement(Case c, String x, String y) {
        String ts = c.ts().replaceAll("\\{x\\.(\\w+)}", "$1[symbol = '" + x + "']").replaceAll("\\{y\\.(\\w+)}", "$1[symbol = '" + y + "']")
                .replace("{x}", "close[symbol = '" + x + "']").replace("{y}", "close[symbol = '" + y + "']");
        return ts.startsWith("WITH ") ? ts
                : "WITH px AS (SELECT * FROM local::DB.prices) SELECT " + ts + " AS ts_value FROM px ON date";
    }

    /** lite's answer: each date's value of the case's column (none not written), and the plan's SQL. */
    record Lite(Map<String, List<Double>> points, String sql) {
    }

    static Lite lite(ModelContext model, Connection c, Case k, String x, String y) throws Exception {
        String query = "|#TS{" + statement(k, x, y) + "}#";
        ExecutionPlan plan = Compiler.query(model, query).executionPlan("local::RT");
        String sql = Compiler.query(model, query).plan("local::RT").sql();
        TsAnswers.Answer answer = TsAnswers.of(plan, PlanCases.write(plan, c, Map.of(),
                PlanCases.Written.CSV::writer));
        List<String> names = answer.columns().stream().map(ExecutionPlan.Column::name).toList();
        String want = k.column() == null ? "ts_value" : "ts_value_" + k.column();
        int date = names.indexOf("date");
        int value = names.indexOf(want);
        if (date < 0 || value < 0) {
            throw new IllegalStateException("no date and " + want + " column among " + names);
        }
        // a date's every value: a line with two rows on one date answers two where lite computes over both
        Map<String, List<Double>> out = new TreeMap<>();
        for (List<String> row : answer.rows()) {
            String v = row.get(value);
            if (v != null) {
                out.computeIfAbsent(row.get(date), d -> new ArrayList<>()).add(Double.parseDouble(v));
            }
        }
        return new Lite(out, sql);
    }

    // ------------------------------------------------------------------ the judge

    /** A verdict and, for a DIFF or a REFUSED, what differs. */
    record Verdict(String status, String detail) {
    }

    /** Within the tolerance: a volatility, a correlation or a beta relative; a level within {@code level}, LEVEL of its
     *  series' scale (judge's); an infinity itself. */
    private static boolean same(boolean relative, double g, double l, double level) {
        if (Double.isInfinite(g)) {
            return l == g;
        }
        return off(relative, g, l) <= (relative ? RATIO : level);
    }

    /** A level's tolerance: LEVEL of its series' own scale, the largest magnitude of gs-quant's figures, never less than
     *  LEVEL -- as //core:ts_replay_* judges (TsAnswers). The user's ruling of 2026-11-08 on PR #91's finding 10: a sum at
     *  BTC's level (4.4e6, a float64 spacing of 9.3e-10) differed by a few units in the last place, pandas and the
     *  database adding in their own orders, and 1e-9 absolute asked them to be bit-exact. */
    static double levelTolerance(Map<String, List<Double>> gs) {
        double scale = gs.values().stream().flatMap(List::stream).filter(v -> !v.isInfinite() && !v.isNaN())
                .mapToDouble(Math::abs).max().orElse(0);
        return LEVEL * Math.max(1, scale);
    }

    private static double off(boolean relative, double g, double l) {
        return relative ? Math.abs(l - g) / Math.max(Math.abs(g), Double.MIN_NORMAL) : Math.abs(l - g);
    }

    /** Whether a case is judged relative: its word's (a gallery series' its outermost call's: Show Python's name of it,
     *  gs-quant's, which the RELATIVE words keep). */
    static boolean relative(Case c) {
        if (!GALLERY_WORD.equals(c.word())) {
            return RELATIVE.contains(c.word());
        }
        Matcher m = Pattern.compile("^(\\w+)\\(").matcher(c.gs());
        return m.find() && RELATIVE.contains(m.group(1));
    }

    /** gs-quant's points against lite's: the same dates, each date the same number of values, and each date's values
     *  alike as a bag (sorted: lite's order among one date's rows is the database's, S149; gs-quant's is the file's). */
    static Verdict judge(Case c, Map<String, List<Double>> gs, Map<String, List<Double>> lite) {
        boolean relative = relative(c);
        double level = levelTolerance(gs);
        List<String> onlyGs = gs.keySet().stream().filter(d -> !lite.containsKey(d)).toList();
        List<String> onlyLite = lite.keySet().stream().filter(d -> !gs.containsKey(d)).toList();
        List<String> apart = new ArrayList<>();
        long count = 0;
        double worst = 0;
        int gsPoints = gs.values().stream().mapToInt(List::size).sum();
        int litePoints = lite.values().stream().mapToInt(List::size).sum();
        for (Map.Entry<String, List<Double>> e : gs.entrySet()) {
            List<Double> l = lite.get(e.getKey());
            if (l == null) {
                continue;
            }
            List<Double> g = e.getValue().stream().sorted().toList();
            List<Double> ls = l.stream().sorted().toList();
            boolean alike = g.size() == ls.size();
            for (int i = 0; alike && i < g.size(); i++) {
                alike = same(relative, g.get(i), ls.get(i), level);
            }
            if (!alike) {
                count++;
                for (int i = 0; i < Math.min(g.size(), ls.size()); i++) {
                    if (!Double.isInfinite(g.get(i))) {
                        worst = Math.max(worst, off(relative, g.get(i), ls.get(i)));
                    }
                }
                if (apart.size() < 3) {
                    apart.add(e.getKey() + ": gs-quant " + (g.size() == 1 ? g.get(0) : g) + ", lite "
                            + (ls.size() == 1 ? ls.get(0) : ls));
                }
            }
        }
        if (onlyGs.isEmpty() && onlyLite.isEmpty() && apart.isEmpty()) {
            return new Verdict("PASS", gsPoints + " points");
        }
        StringBuilder d = new StringBuilder();
        d.append(gsPoints).append(" gs-quant points, ").append(litePoints).append(" lite's; ").append(count)
                .append(" dates apart (worst ").append(relative ? "relative " : "absolute ").append(worst).append("), ")
                .append(onlyGs.size()).append(" gs-quant's alone").append(onlyGs.isEmpty() ? "" : " from "
                        + onlyGs.get(0)).append(", ").append(onlyLite.size()).append(" lite's alone")
                .append(onlyLite.isEmpty() ? "" : " from " + onlyLite.get(0));
        if (!apart.isEmpty()) {
            d.append("; ").append(String.join("; ", apart));
        }
        return new Verdict("DIFF", d.toString());
    }

    // ------------------------------------------------------------------ the ledger

    /** {@code why}'s second half, this test's account of a DIFF or a REFUSED (snap.py's LITE); the first, gs-quant's. */
    static final String LITE = " | lite: ";

    /** The ledger's columns (snap.py's COLUMNS), read and written by name. */
    static final List<String> COLUMNS = List.of("case", "gsquant", "duckdb", "register", "digest", "note", "why");

    private static final String LEDGER_HEADER = """
            # THE GS-QUANT ORACLE'S LEDGER (tools/gs-quant-oracle/README.md): each case of mapping.yaml, `<word> <args>`, its
            # gs-quant side (snap.py: OK, GS_ERROR, SKIP) and its lite side on DuckDB (//core:ts_gsquant_parity_test: PASS,
            # DIFF, REFUSED, GS_ERROR, SKIP), the register row a DIFF is ruled by, gs-quant's series' sha256 (snap.py: --check
            # holds a live run to it), a note (dup-span: the series covers a date with two rows of one line), and why. A
            # verdict moves only with the test's -Dts.gsquant.update, never by hand.
            # snapped under pins.env: %s
            case\tgsquant\tduckdb\tregister\tdigest\tnote\twhy
            """;

    /** The ledger's rows, each its cells in COLUMNS' order (the header must name them so). */
    static Map<String, String[]> ledger(Path file) throws IOException {
        Map<String, String[]> out = new LinkedHashMap<>();
        List<String> names = null;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.startsWith("#") || line.isBlank()) {
                continue;
            }
            List<String> cells = Arrays.asList(line.split("\t", -1));
            if (names == null) {
                names = cells;
                if (!names.equals(COLUMNS)) {
                    throw new IllegalStateException(file + "'s columns are " + names + ", not " + COLUMNS);
                }
                continue;
            }
            out.put(cells.get(0), cells.toArray(String[]::new));
        }
        return out;
    }

    // ------------------------------------------------------------------ the datasets

    /** One snapped dataset: its name, its tables' CSV and columns, its data's sha, its answers and ledger, each line's
     *  dates, and the symbols it runs each case over as x -- the samples' one (null: NVDA, no suffix) or a fixture's
     *  trap symbols (each key ` @<symbol>`, snapped by snap.py --fixture). */
    record Dataset(String name, Map<String, String> csv, Map<String, List<CatalogModel.Column>> columns, String dataSha,
            Path answers, Path ledger, Map<String, List<String>> lineDates, List<String> ats) {
    }

    static String sha256(String text) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(
                text.getBytes(StandardCharsets.UTF_8)));
    }

    static Dataset samples() throws Exception {
        String text = Files.readString(Runfile.property("ts.gsquant.samples"), StandardCharsets.UTF_8);
        Json.Obj samples = Json.parseObject(text);
        Map<String, String> csv = new LinkedHashMap<>();
        SAMPLES.keySet().forEach(t -> csv.put(t, samples.getString(t)));
        List<String> ats = new ArrayList<>();
        ats.add(null);
        return new Dataset("samples", csv, SAMPLES, sha256(text), Runfile.property("ts.gsquant.answers"),
                Runfile.property("ts.gsquant.ledger"), lineDates(samples), ats);
    }

    /** A fixture's price file ({@code -Dts.gsquant.<name>.prices}), its answers and ledger; its symbols are its
     *  ledger's (snap.py runs each trap's symbol, from the fixture's manifest). */
    static Dataset fixture(String name) throws Exception {
        String text = Files.readString(Runfile.property("ts.gsquant." + name + ".prices"), StandardCharsets.UTF_8);
        Map<String, String> csv = Map.of("prices", text);
        Map<String, List<CatalogModel.Column>> columns = Map.of("prices", columnsOf(text.split("\n", 2)[0]));
        Path ledger = Runfile.property("ts.gsquant." + name + ".ledger");
        List<String> ats = new ArrayList<>(new java.util.LinkedHashSet<>(ledger(ledger).keySet().stream()
                .filter(k -> k.contains(" @")).map(k -> k.substring(k.indexOf(" @") + 2)).toList()));
        Map<String, java.util.TreeSet<String>> dates = new LinkedHashMap<>();
        List<String> lines = Arrays.asList(text.split("\n"));
        List<String> header = Arrays.asList(lines.get(0).strip().split(","));
        for (String line : lines.subList(1, lines.size())) {
            if (!line.isBlank()) {
                String[] cells = line.strip().split(",", -1);
                dates.computeIfAbsent(cells[header.indexOf("symbol")], k -> new java.util.TreeSet<>())
                        .add(cells[header.indexOf("date")]);
            }
        }
        Map<String, List<String>> lineDates = new LinkedHashMap<>();
        dates.forEach((k, v) -> lineDates.put(k, List.copyOf(v)));
        return new Dataset(name, csv, columns, sha256(text), Runfile.property("ts.gsquant." + name + ".answers"),
                ledger, lineDates, ats);
    }

    // ------------------------------------------------------------------ the gallery (item 14f, step 4b)

    /** A gallery case's word: its key {@code series <view> <id>}, a struct's field {@code series <view> <id>.<field>}
     *  (snap.py's GALLERY_WORD). */
    static final String GALLERY_WORD = "series";

    /** The pages whose tables a series' Show #TS reads, {@code <page>_<sample>} (the parity test's, TsReplayTest's). */
    private static final List<String> PAGES = List.of("gallery", "markets");

    private static final Pattern GALLERY_KNOWN = Pattern.compile("  known: \\{(.*)}");

    /** The gallery's cases and the series no case runs (by key, with why: a tile setting gs-quant's call has not,
     *  DataCube's own spelling). */
    record Gallery(Word word, Map<String, String> skipped) {
    }

    /**
     * THE GALLERY'S SERIES (gallery.jsonl, which //datacube:timeseries_ts_parity_test writes): every series of the gallery
     * and the markets example, each field of a struct its own case -- its gs-quant call Show Python's (snap.py runs it
     * over the pages' samples), its lite side its Show #TS statement (show-ts.txt, the same series' as the replays run),
     * the register row a DIFF of it is ruled by (mapping.yaml's {@code gallery: known:}).
     */
    static Gallery gallery(Path rows, Path statements, Path mapping) throws IOException {
        Map<String, String> shown = new LinkedHashMap<>();
        for (String line : Files.readAllLines(statements, StandardCharsets.UTF_8)) {
            int colon = line.indexOf(": ");
            if (colon > 0) {
                shown.put(line.substring(0, colon), line.substring(colon + 2));
            }
        }
        Map<String, String> known = new LinkedHashMap<>();
        boolean inGallery = false;
        for (String line : Files.readAllLines(mapping, StandardCharsets.UTF_8)) {
            Matcher m;
            if (line.equals("gallery:")) {
                inGallery = true;
            } else if (inGallery && (m = GALLERY_KNOWN.matcher(line)).matches() && !m.group(1).isBlank()) {
                for (String part : m.group(1).split(", ")) {
                    String[] kv = part.split(": ");
                    known.put(kv[0].startsWith("\"") && kv[0].endsWith("\"") ? kv[0].substring(1, kv[0].length() - 1) : kv[0], kv[1]);
                }
            }
        }
        List<Case> cases = new ArrayList<>();
        Map<String, String> skipped = new LinkedHashMap<>();
        for (String line : Files.readAllLines(rows, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            Json.Obj o = Json.parseObject(line);
            String id = o.getString("series");
            List<String> fields = o.get("fields") instanceof Json.Arr a
                    ? a.items().stream().map(n -> ((Json.Str) n).value()).toList()
                    : java.util.Collections.singletonList(null);
            for (String field : fields) {
                String args = field == null ? id : id + "." + field;
                if (!(o.get("skip") instanceof Json.Null)) {
                    skipped.put(GALLERY_WORD + " " + args, o.getString("skip"));
                    continue;
                }
                String statement = shown.get(id);
                if (statement == null) {
                    throw new IllegalStateException("no Show #TS statement of " + id + " in show-ts.txt");
                }
                cases.add(new Case(GALLERY_WORD, args, statement, o.getString("python"), field,
                        known.getOrDefault(args, NO_REGISTER_ROW), "-"));
            }
        }
        for (String k : known.keySet()) {
            if (cases.stream().noneMatch(c -> c.args().equals(k))) {
                throw new IllegalStateException("mapping.yaml's gallery known " + k + " is no case of the gallery's");
            }
        }
        return new Gallery(new Word(GALLERY_WORD, false, cases, true), skipped);
    }

    /** The gallery's dataset: the pages' samples as each page's tables ({@code gallery_prices}, {@code markets_cash}),
     *  its answers and ledger; every row its own dates. */
    static Dataset galleryDataset() throws Exception {
        String text = Files.readString(Runfile.property("ts.gsquant.samples"), StandardCharsets.UTF_8);
        Json.Obj samples = Json.parseObject(text);
        Map<String, String> csv = new LinkedHashMap<>();
        Map<String, List<CatalogModel.Column>> columns = new LinkedHashMap<>();
        for (String page : PAGES) {
            GALLERY_SAMPLES.forEach((sample, cols) -> {
                csv.put(page + "_" + sample, samples.getString(sample));
                columns.put(page + "_" + sample, cols);
            });
        }
        List<String> ats = new ArrayList<>();
        ats.add(null);
        return new Dataset("gallery", csv, columns, sha256(text), Runfile.property("ts.gsquant.gallery.answers"),
                Runfile.property("ts.gsquant.gallery.ledger"), Map.of(), ats);
    }

    /** One dataset judged: each case's verdict, its findings appended, its problems added; its ledger rewritten into
     *  {@code update} (a directory) when given. {@code skipped}: cases no answer is snapped for, by key, with why. */
    static void judgeDataset(Dataset d, List<Word> words, String pin, Pattern only, StringBuilder findings,
            List<String> problems, String update, Map<String, String> skipped) throws Exception {
        Answers answers = answers(d.answers(), d.lineDates());
        Json.Obj snapped = (Json.Obj) answers.meta().get("cases");
        Map<String, String> expected = new LinkedHashMap<>();
        Map<String, Verdict> verdicts = new LinkedHashMap<>();
        for (String at : d.ats()) {
            for (Word w : words) {
                if (w.skip()) {
                    verdicts.put(at == null ? w.word() : w.word() + " @" + at, new Verdict("SKIP", "no gs-quant twin"));
                    continue;
                }
                for (Case k : w.cases()) {
                    String key = at == null ? k.key() : k.key() + " @" + at;
                    if (at != null && (k.ts().startsWith("WITH") || k.gs().matches("(?s).*\\bcpi\\b.*"))) {
                        verdicts.put(key, new Verdict("SKIP", "a whole statement over the samples' own lines"));
                        continue;
                    }
                    expected.put(key, sha(k, at == null ? d.dataSha() : d.dataSha() + "@" + at, pin));
                    verdicts.put(key, null);
                }
            }
        }
        skipped.forEach((key, why) -> verdicts.put(key, new Verdict("SKIP", why)));
        problems.addAll(guards(expected, answers, pin, d.name()));
        problems.addAll(answers.problems());
        TestBackend platform = TestBackends.platform();
        String model = model(platform, d.columns());
        ModelContext ctx = Compiler.compileModel(model);
        try (Connection c = platform.open()) {
            seed(platform, c, d.csv(), d.columns(), model);
            for (String at : d.ats()) {
                for (Word w : words) {
                    for (Case k : w.cases()) {
                        String key = at == null ? k.key() : k.key() + " @" + at;
                        if (!expected.containsKey(key)) {
                            continue;
                        }
                        if (only != null && !only.matcher(key).find()) {
                            verdicts.remove(key);
                            continue;
                        }
                        if (!snapped.has(key)) {
                            continue;   // UNSNAPPED, reported by the guards
                        }
                        Json.Obj entry = (Json.Obj) snapped.get(key);
                        if ("GS_ERROR".equals(entry.getString("status"))) {
                            verdicts.put(key, new Verdict("GS_ERROR", entry.getString("error")));
                            continue;
                        }
                        // gs-quant answers no such field (a band's width): a gallery case snapped as SKIP, with why
                        if ("SKIP".equals(entry.getString("status"))) {
                            verdicts.put(key, new Verdict("SKIP", entry.getString("error")));
                            continue;
                        }
                        Map<String, List<Double>> gs = answers.points().get(key);
                        if (gs == null) {
                            // an OK case with no answer row: judging it against nothing would pass a lite that
                            // answers nothing
                            problems.add("NO ROW: " + d.name() + " " + key + ": OK in the metadata, no answer row");
                            verdicts.put(key, new Verdict("NO ROW", "OK in the metadata, no answer row"));
                            continue;
                        }
                        int points = gs.values().stream().mapToInt(List::size).sum();
                        if (points != entry.getInt("points")) {
                            problems.add("POINTS: " + d.name() + " " + key + ": its row holds " + points
                                    + " points, the metadata " + entry.getInt("points"));
                        }
                        String x = at == null ? X : at;
                        String y = at == null ? Y : yOf(answers, key, x);
                        Verdict v;
                        String sql = null;
                        try {
                            Lite lite = lite(ctx, c, k, x, y);
                            sql = lite.sql();
                            v = judge(k, gs, lite.points());
                        } catch (com.legend.error.DataError | com.legend.error.LegendCompileException e) {
                            // lite's own refusal alone: a DataError at run time (S149's among them), a
                            // LegendCompileException as the query compiles; any other exception is this test's fault
                            // and fails it
                            v = new Verdict("REFUSED", String.valueOf(e.getMessage()).split("\n")[0]);
                        }
                        verdicts.put(key, v);
                        if (!v.status().equals("PASS")) {
                            findings.append("\n## ").append(d.name()).append(": ").append(key).append(" -- ")
                                    .append(v.status()).append(k.register().equals(NO_REGISTER_ROW) ? "" : " (" + k.register() + ")")
                                    .append("\n\n").append(v.detail()).append("\n\n- gs-quant: `").append(k.gs())
                                    .append("` (gs_quant/timeseries/").append(k.src()).append(")\n- #TS: `")
                                    .append(statement(k, x, y)).append("`\n");
                            if (sql != null) {
                                findings.append("- lite's SQL on DuckDB:\n\n```sql\n").append(sql).append("\n```\n");
                            }
                        }
                    }
                }
            }
        }
        Map<String, String[]> ledger = ledger(d.ledger());
        Map<String, Integer> tally = new TreeMap<>();
        verdicts.values().forEach(v -> tally.merge(v.status(), 1, Integer::sum));
        long pass = verdicts.values().stream().filter(v -> v.status().equals("PASS")).count();
        long dupSpan = verdicts.entrySet().stream().filter(e -> e.getValue().status().equals("PASS"))
                .filter(e -> ledger.containsKey(e.getKey()) && "dup-span".equals(ledger.get(e.getKey())[5])).count();
        System.out.println("gs-quant parity on DuckDB, " + d.name() + ": " + tally + "; " + pass + " PASS, of which "
                + dupSpan + " on a dup-span, where lite and gs-quant agree on the both-rows answer that the fixture"
                + " PR's (#90) last-print-wins proposal would change");
        if (only != null) {
            verdicts.forEach((k, v) -> System.out.println(v.status() + "\t" + k + "\t" + v.detail()));
            return;
        }
        // the ratchet: each case's verdict is the ledger's duckdb column
        for (Map.Entry<String, Verdict> e : verdicts.entrySet()) {
            String[] row = ledger.get(e.getKey());
            String was = row == null ? null : row[2];
            if (!e.getValue().status().equals(was)) {
                problems.add((was == null ? "NOT LEDGERED" : "MOVED " + was + " -> " + e.getValue().status()) + ": "
                        + d.name() + " " + e.getKey() + ": " + e.getValue().detail());
            }
        }
        for (String k : ledger.keySet()) {
            if (!verdicts.containsKey(k)) {
                problems.add("GONE from " + d.name() + "'s cases: " + k);
            }
        }
        if (update != null) {
            StringBuilder out = new StringBuilder(LEDGER_HEADER.formatted(pin));
            for (Map.Entry<String, Verdict> e : verdicts.entrySet()) {
                String[] row = ledger.getOrDefault(e.getKey(), new String[] {e.getKey(), "-", "-", "-", "-", "-", ""});
                String status = e.getValue().status();
                String register = status.equals("PASS") || status.equals("SKIP") ? NO_REGISTER_ROW : registerOf(words, e.getKey().split(" @")[0]);
                // `why` is two halves: gs-quant's (snap.py's, kept) and, for a DIFF or a REFUSED, lite's (this test's)
                String gsHalf = row[6].contains(LITE) ? row[6].substring(0, row[6].indexOf(LITE)) : row[6];
                String why = status.equals("DIFF") || status.equals("REFUSED") ? gsHalf + LITE + e.getValue().detail()
                        : gsHalf;
                out.append(String.join("\t", e.getKey(), row[1], status, register, row[4], row[5], why)).append('\n');
            }
            Path target = Path.of(update).resolve(d.ledger().getFileName().toString());
            Files.writeString(target, out.toString(), StandardCharsets.UTF_8);
            System.out.println("wrote " + target);
        }
    }

    /** A fixture case's y: the second line its answer row names (snap.py's fixture_y), or x where it reads one. */
    private static String yOf(Answers answers, String key, String x) {
        String symbol = answers.symbols().get(key);
        if (symbol == null) {
            throw new IllegalStateException("no answer row for " + key + ": its lines unknown");
        }
        String[] lines = symbol.split(",");
        return lines.length > 1 ? lines[1] : x;
    }

    /** WORDS (snap.py's word_problems): every word of the library (its timeseries::line:: functions, which WORDS.md is
     *  generated from) a row of the mapping, and a row that is no word only where it names its ruling. */
    static List<String> wordProblems(List<Word> words, String library) {
        Set<String> lib = new java.util.TreeSet<>();
        Matcher m = Pattern.compile("timeseries::line::(\\w+)<").matcher(library);
        while (m.find()) {
            lib.add(m.group(1).replaceAll("_+$", ""));
        }
        Map<String, Word> rows = new LinkedHashMap<>();
        words.forEach(w -> rows.put(w.word(), w));
        List<String> out = new ArrayList<>();
        lib.stream().filter(w -> !rows.containsKey(w))
                .forEach(w -> out.add("WORDS: " + w + " is a word of the library with no row in mapping.yaml"));
        rows.values().stream().filter(w -> !lib.contains(w.word()) && !w.ruled())
                .forEach(w -> out.add("WORDS: " + w.word() + " has a row but is no word of the library"));
        return out;
    }

    @Test
    void everyMappedWordAnswersAsGsQuantsOwnFunction() throws Exception {
        List<Word> words = printed(mapping(Runfile.property("ts.gsquant.mapping")), Runfile.property("ts.gsquant.calls"));
        String pin = pin(Runfile.property("ts.gsquant.pins"));
        // a development run over some of the cases (-Dts.gsquant.only=<a regex found in a case's key>): each verdict
        // printed, the ledgers not held
        String only = System.getProperty("ts.gsquant.only");
        Pattern onlyPattern = only == null ? null : Pattern.compile(only);
        // -Dts.gsquant.update=<tools/gs-quant-oracle in the workspace>: each ledger rewritten there
        String update = System.getProperty("ts.gsquant.update");
        List<String> problems = new ArrayList<>(wordProblems(words,
                Files.readString(Runfile.property("timeseries.library"), StandardCharsets.UTF_8)));
        StringBuilder findings = new StringBuilder("# The gs-quant parity's findings (//core:ts_gsquant_parity_test)\n\n"
                + "Each DIFF and REFUSED: the dataset and case, its register row, what differs, gs-quant's function, the"
                + " #TS statement and lite's SQL on DuckDB.\n");
        for (Dataset d : List.of(samples(), fixture("adversarial-raw"), fixture("adversarial-adjusted"))) {
            judgeDataset(d, words, pin, onlyPattern, findings, problems, update, Map.of());
        }
        // the gallery's and the markets example's series, each its Show #TS against gs-quant's call of it (item 14f, 4b)
        Gallery g = gallery(Runfile.property("ts.gsquant.gallery"), Runfile.property("ts.gsquant.gallery.statements"),
                Runfile.property("ts.gsquant.mapping"));
        judgeDataset(galleryDataset(), List.of(g.word()), pin, onlyPattern, findings, problems, update, g.skipped());
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "gsquant-findings.md"), findings.toString(), StandardCharsets.UTF_8);
        }
        if (onlyPattern != null || update != null) {
            return;
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems (a DIFF is a finding: ledger it with"
                + " -Dts.gsquant.update and post it, never edit a word to pass):\n" + String.join("\n",
                problems.subList(0, Math.min(problems.size(), 200))));
    }

    private static String registerOf(List<Word> words, String key) {
        return words.stream().flatMap(w -> w.cases().stream()).filter(c -> c.key().equals(key)).map(Case::register)
                .findFirst().orElseThrow(() -> new IllegalStateException("no case " + key + " in the mapping"));
    }
}
