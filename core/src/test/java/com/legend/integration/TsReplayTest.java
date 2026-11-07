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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE GALLERY ON ANOTHER DATABASE (docs/TS_DECISIONS_2026_11_07.md item 9, decision 12 (a); the tolerance ruled
 * 2026-11-07, §4 there): every series of the timeseries gallery and the markets example as Show #TS prints it -- the
 * parity test's statements (datacube/test/timeseries-ts-parity.test.ts), held equal to the fixture
 * {@code datacube/test/fixtures/timeseries/show-ts.txt} -- over the pages' own samples (samples.ts, emitted by
 * {@code //datacube:ts_replay_samples}) loaded into each database, answered on DuckDB and on the lane's databases, and
 * each answer held to DuckDB's, run beside it: the same dates and lines exactly, each figure within 1e-9 of its
 * series' own scale (the largest magnitude of DuckDB's figures in that column), so a sum taken in another order
 * passes and a wrong figure does not. DuckDB's own answers are held to the frozen digests on DuckDB-WASM by the
 * parity test; here another database is held to DuckDB's.
 *
 * <p>Its own targets ({@code //core:ts_replay_test}: the other in-process databases; {@code
 * //core:ts_replay_postgres_test}: the embedded Postgres, {@code -Dts.backend=postgres}).
 */
class TsReplayTest {

    /** The series that answer otherwise on a database, by {@code <database> <series>}, and why: each a register row
     *  or a named open item, and listed only while it differs (a series that comes to agree fails here). */
    private static final Map<String, String> DIFFERS = Map.of();

    /** The samples' tables as the pages read them into the tab (the parity test's {@code <page>_<sample>}), each
     *  sample's columns typed as the tab's DuckDB reads its CSV. */
    private static final Map<String, List<CatalogModel.Column>> SAMPLES = new LinkedHashMap<>();

    static {
        SAMPLES.put("prices", List.of(text("symbol"), text("asset_class"), date("date"), number("open"),
                number("high"), number("low"), number("close"), whole("volume")));
        SAMPLES.put("rates", List.of(text("tenor"), number("years"), date("date"), number("yield")));
        SAMPLES.put("macro", List.of(text("series"), date("date"), number("value")));
    }

    private static final List<String> PAGES = List.of("gallery", "markets");

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

    /** The databases each series is replayed on beside DuckDB: every other in-process one, or the one the lane names
     *  ({@code -Dts.backend}). */
    private static List<TestBackend> others() {
        String named = System.getProperty("ts.backend", System.getenv("TS_BACKEND"));
        return named == null
                ? TestBackends.inProcess().stream().filter(b -> !b.name().equals(duckDb().name())).toList()
                : List.of(TestBackends.named(named));
    }

    private static TestBackend duckDb() {
        return TestBackends.of(com.legend.model.ConnectionDefinition.DatabaseType.DuckDB);
    }

    /** One Show #TS statement of the fixture: its series' name and its text. */
    private record Statement(String id, String text) {
    }

    private static List<Statement> statements() throws java.io.IOException {
        List<Statement> out = new ArrayList<>();
        for (String line : Files.readAllLines(Runfile.property("ts.replay.statements"), StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            int at = line.indexOf(": ");
            out.add(new Statement(line.substring(0, at), line.substring(at + 2)));
        }
        return out;
    }

    /** The samples' tables, a connection and a runtime on {@code backend}, and the library. */
    private static String model(TestBackend backend) throws java.io.IOException {
        List<CatalogModel.Table> tables = new ArrayList<>();
        for (String page : PAGES) {
            SAMPLES.forEach((sample, columns) -> tables.add(new CatalogModel.Table(null, page + "_" + sample, columns,
                    true)));
        }
        String database = CatalogModel.database("local::DB", tables, duckDb().dialect()).text();
        return database + """

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

    /** {@code c} given the samples' tables, as {@code backend}'s dialect writes them, and their rows. */
    private static void seed(TestBackend backend, Connection c, Json.Obj samples) throws Exception {
        boolean javaTime = com.legend.exec.Connectors.of(backend.type()).decoding().bindsJavaTime();
        List<com.legend.model.DatabaseDefinition.TableDefinition> tables = Compiler.parseModel(model(backend))
                .elements().stream().filter(e -> e instanceof com.legend.model.DatabaseDefinition)
                .flatMap(e -> ((com.legend.model.DatabaseDefinition) e).tables().stream()).toList();
        for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
            try (java.sql.Statement s = c.createStatement()) {
                s.execute(backend.dialect().render(com.legend.setup.Ddl.createTable(t, null)));
            }
            String sample = t.name().substring(t.name().indexOf('_') + 1);
            List<CatalogModel.Column> columns = SAMPLES.get(sample);
            List<String> lines = Arrays.asList(samples.getString(sample).split("\n"));
            assertEquals(columns.stream().map(CatalogModel.Column::name).toList(), Arrays.asList(lines.get(0).split(",")),
                    sample + "'s header");
            String marks = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + backend.dialect().storedName(t.name(),
                    t.quoted()) + " VALUES (" + marks + ")")) {
                for (String line : lines.subList(1, lines.size())) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] cells = line.split(",", -1);
                    for (int i = 0; i < columns.size(); i++) {
                        String v = cells[i];
                        switch (columns.get(i).dataType()) {
                            case "VARCHAR" -> ps.setString(i + 1, v);
                            case "DOUBLE" -> ps.setDouble(i + 1, Double.parseDouble(v));
                            case "BIGINT" -> ps.setLong(i + 1, Long.parseLong(v));
                            case "DATE" -> {
                                if (javaTime) {
                                    ps.setObject(i + 1, LocalDate.parse(v));
                                } else {
                                    ps.setDate(i + 1, java.sql.Date.valueOf(LocalDate.parse(v)));
                                }
                            }
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

    /** An answer: its columns and types, and its rows' cells as text (null where none). */
    private record Answer(List<ExecutionPlan.Column> columns, List<List<String>> rows) {
    }

    /** Each statement's answer on {@code backend}, or the message it was refused with. */
    private static Map<String, Object> replay(TestBackend backend, List<Statement> statements, Json.Obj samples)
            throws Exception {
        ModelContext model = Compiler.compileModel(model(backend));
        Map<String, Object> out = new LinkedHashMap<>();
        // a server's database outlives a connection: the replay's own workspace there (dropped as it closes)
        try (Connection c = backend.inProcess() ? backend.open() : backend.openWorkspace()) {
            seed(backend, c, samples);
            // the lane's bound on a statement's run, where it sets one (a server's: Postgres's statement_timeout)
            String bound = System.getProperty("ts.replay.session", System.getenv("TS_REPLAY_SESSION"));
            if (bound != null && !backend.name().equals(duckDb().name())) {
                try (java.sql.Statement st = c.createStatement()) {
                    st.execute(bound);
                }
            }
            for (Statement s : statements) {
                long started = System.nanoTime();
                try {
                    ExecutionPlan plan = Compiler.query(model, "|#TS{" + s.text() + "}#").executionPlan("local::RT");
                    String text = PlanCases.write(plan, c, Map.of(), PlanCases.Written.CSV::writer);
                    out.put(s.id(), new Answer(java.util.Objects.requireNonNull(plan.answerColumns(),
                            "a #TS query's columns"), rows(text)));
                } catch (Exception | AssertionError e) {
                    out.put(s.id(), String.valueOf(e.getMessage()).split("\n")[0]);
                }
                System.err.println(backend.name() + " " + s.id() + ": " + (System.nanoTime() - started) / 1_000_000
                        + " ms");
            }
        }
        return out;
    }

    private static List<List<String>> rows(String csv) {
        List<String> lines = new ArrayList<>(Arrays.asList(csv.split("\r\n")));
        lines.remove(0);
        List<List<String>> out = new ArrayList<>();
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            out.add(Arrays.stream(line.split(",", -1)).map(x -> x.isEmpty() ? null : x).toList());
        }
        return out;
    }

    private static final Set<String> FIGURES = Set.of("Float", "Number", "Decimal");

    /** Where {@code got} differs from {@code want} (DuckDB's): null where it does not. */
    private static String difference(Answer want, Answer got) {
        List<String> names = want.columns().stream().map(ExecutionPlan.Column::name).toList();
        if (!names.equals(got.columns().stream().map(ExecutionPlan.Column::name).toList())) {
            return "the columns " + got.columns().stream().map(ExecutionPlan.Column::name).toList() + ", DuckDB's "
                    + names;
        }
        List<Integer> keys = new ArrayList<>();
        List<Integer> figures = new ArrayList<>();
        for (int i = 0; i < want.columns().size(); i++) {
            (FIGURES.contains(want.columns().get(i).type()) ? figures : keys).add(i);
        }
        Map<List<String>, List<String>> a = keyed(want, keys, figures);
        Map<List<String>, List<String>> b = keyed(got, keys, figures);
        if (!a.keySet().equals(b.keySet())) {
            List<List<String>> only = a.keySet().stream().filter(k -> !b.containsKey(k)).limit(3).toList();
            List<List<String>> extra = b.keySet().stream().filter(k -> !a.containsKey(k)).limit(3).toList();
            return "the rows' dates and lines: " + a.size() + " on DuckDB, " + b.size() + " here; DuckDB's alone "
                    + only + ", these alone " + extra;
        }
        for (int f = 0; f < figures.size(); f++) {
            double scale = 0;
            for (List<String> v : a.values()) {
                if (v.get(f) != null) {
                    scale = Math.max(scale, Math.abs(Double.parseDouble(v.get(f))));
                }
            }
            for (Map.Entry<List<String>, List<String>> e : a.entrySet()) {
                String x = e.getValue().get(f);
                String y = b.get(e.getKey()).get(f);
                boolean same = x == null ? y == null
                        : y != null && Math.abs(Double.parseDouble(x) - Double.parseDouble(y)) <= 1e-9 * scale;
                if (!same) {
                    return names.get(figures.get(f)) + " at " + e.getKey() + ": " + y + ", DuckDB's " + x
                            + " (scale " + scale + ")";
                }
            }
        }
        return null;
    }

    /** {@code answer}'s figures by its keys' cells (a date and a line), in their order. */
    private static Map<List<String>, List<String>> keyed(Answer answer, List<Integer> keys, List<Integer> figures) {
        Map<List<String>, List<String>> out = new TreeMap<>(java.util.Comparator.comparing(Object::toString));
        for (List<String> row : answer.rows()) {
            List<String> key = keys.stream().map(row::get).toList();
            List<String> previous = out.put(key, figures.stream().map(row::get).toList());
            assertTrue(previous == null, "two rows of one date and line: " + key);
        }
        return out;
    }

    @Test
    void everySeriesAnswersAsOnDuckDb() throws Exception {
        List<Statement> statements = statements();
        assertTrue(statements.size() > 200, "the gallery's statements: " + statements.size());
        Json.Obj samples = Json.parseObject(Files.readString(Runfile.property("ts.replay.samples"),
                StandardCharsets.UTF_8));
        Map<String, Object> reference = replay(duckDb(), statements, samples);
        List<String> refusedOnDuckDb = reference.entrySet().stream().filter(e -> e.getValue() instanceof String)
                .map(e -> e.getKey() + ": " + e.getValue()).toList();
        assertEquals(List.of(), refusedOnDuckDb, "every series answers on DuckDB");
        List<String> unexpected = new ArrayList<>();
        List<String> agreed = new ArrayList<>();
        StringBuilder report = new StringBuilder();
        for (TestBackend backend : others()) {
            long started = System.nanoTime();
            Map<String, Object> got = replay(backend, statements, samples);
            int same = 0;
            for (Statement s : statements) {
                Object answer = got.get(s.id());
                String said = answer instanceof String refused ? "refused: " + refused
                        : difference((Answer) reference.get(s.id()), (Answer) answer);
                String id = backend.name() + " " + s.id();
                if (said == null) {
                    same++;
                    if (DIFFERS.containsKey(id)) {
                        agreed.add(id);
                    }
                } else {
                    report.append(id).append(": ").append(said).append("\n    ").append(s.text()).append('\n');
                    if (!DIFFERS.containsKey(id)) {
                        unexpected.add(id);
                    }
                }
            }
            report.append(backend.name()).append(": ").append(same).append(" of ").append(statements.size())
                    .append(" series as on DuckDB (").append((System.nanoTime() - started) / 1_000_000).append(" ms)\n");
        }
        System.out.println(report);
        assertEquals(List.of(), unexpected, "every series answers as on DuckDB, or is listed in DIFFERS with why\n"
                + report);
        assertEquals(List.of(), agreed, "a series listed in DIFFERS that now answers as on DuckDB: delete its row");
    }
}
