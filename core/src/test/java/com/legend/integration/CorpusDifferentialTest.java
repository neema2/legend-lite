package com.legend.integration;

import com.legend.json.Json;
import com.legend.model.DatabaseDefinition;
import com.legend.model.PackageableElement;
import com.legend.setup.Ddl;
import com.legend.setup.RowLoad;
import com.legend.sql.SqlDdl;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cross-engine differential: legend-lite's SQL, run against the same rows legend-engine
 * runs the .pure testSuites against, compared to the same referee expectations.
 *
 * <p>The third assertion mechanism, wired 2026-10-05 (Bazel workplan P3-18, D3 row 4): until then
 * {@code scripts/corpus/differential.py} ran in no gate and an {@code Assumptions} guard skipped this
 * test in every build. It cannot be wrong about Legend semantics — it only reports whether two independent
 * implementations agree with the reference evaluator — and its blind spot is
 * equally plain: a defect both engines share is invisible to it.
 *
 * <p>Its inputs are a build output: {@code //scripts/corpus:gen_differential} runs
 * {@code scripts/corpus/differential.py} over the committed corpus into one tree ({@code seed.json},
 * {@code expected/}), which {@code //core:corpus_differential_test} names by its seed file,
 * {@code -Dcorpus.differential.seed}.
 *
 * <p>Each service runs on the database its runtime declares ({@link com.legend.Compiler#executesOn}), its SQL that
 * database's dialect's, in a session of that database's test backend, seeded once through the same dialect: the seed
 * is data (each table's columns, with the type the seed gives each, and its rows as text), never one database's SQL.
 * Until 2026-10-17 every service was rendered and run on DuckDB, whatever its runtime declared (defect 4 of
 * docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md, §3).
 *
 * <p>Comparison is on a normalised text form, not JSON, so that neither side's float
 * formatter or key ordering can manufacture a difference. Column kinds travel in the
 * expectation header for exactly that reason.
 */
@DisplayName("Corpus Differential (legend-lite SQL vs oracle)")
@Tag("differential")   // //core:corpus_differential_test alone: its data is that target's (core_tests excludes the tag)
class CorpusDifferentialTest {

    /** The generated seed (resolved by its runfiles path), and expected/ beside it. */
    private static final Path SEED = com.legend.testing.Runfile.property("corpus.differential.seed");
    private static final Path DIFF = SEED.getParent();
    private static final String NULL = "~";

    @Test
    @DisplayName("every stress service agrees with the reference evaluator")
    void differential() throws Exception {

        String model = StressCorpus.model();
        StressCorpus.reportExclusions();
        var ctx = com.legend.Compiler.compileModel(model);
        List<PackageableElement> elements = com.legend.testing.Own.model(model).elements();

        // Services whose disagreement with the reference is already understood and
        // recorded in docs/UPSTREAM_FINDINGS.md. Reported, not tolerated silently — and a
        // quarantined service that starts AGREEING is a failure, so a fix cannot pass
        // unnoticed.
        Map<String, String> known = new LinkedHashMap<>();
        Path qf = DIFF.resolve("expected").resolve("QUARANTINE.txt");
        if (Files.exists(qf)) {
            for (String line : Files.readAllLines(qf)) {
                String[] p = line.split("\t");
                if (p.length >= 3) known.put(p[0], p[1] + " " + p[2]);
            }
        }

        List<String> agree = new ArrayList<>(), disagree = new ArrayList<>(),
                knownFail = new ArrayList<>(), fixed = new ArrayList<>();
        // one seeded session per database the services' runtimes declare, by its backend's name
        Map<String, Connection> sessions = new LinkedHashMap<>();
        try {
            for (var el : elements) {
                if (!(el instanceof com.legend.model.ServiceDefinition svc)) continue;
                if (!svc.qualifiedName().startsWith("stress::")) continue;
                String name = svc.qualifiedName().substring(
                        svc.qualifiedName().lastIndexOf(':') + 1);
                Path exp = DIFF.resolve("expected").resolve(name + ".txt");
                if (!Files.exists(exp)) continue;

                List<String> want = Files.readAllLines(exp);
                String[] header = want.get(0).split("\\|");
                List<String> expectedRows = want.subList(1, want.size());
                List<String> actualRows;
                boolean crashed = false;
                try {
                    var vs = com.legend.compiler.NameResolver.resolveQuery(svc.functionBody());
                    // The service's own runtime, not a fixed one — see StressServiceSuitesTest.
                    String rt = svc.runtimeRef() != null ? svc.runtimeRef() : "stress::RT";
                    // the database the runtime declares: its dialect renders the SQL, its session runs it
                    TestBackend backend = TestBackends.of(com.legend.Compiler.executesOn(ctx, rt).type());
                    Connection conn = sessions.get(backend.name());
                    if (conn == null) {
                        conn = backend.open();
                        try {
                            seed(backend, conn, elements);
                        } catch (Exception e) {
                            conn.close();
                            throw e;
                        }
                        sessions.put(backend.name(), conn);
                    }
                    String sql = backend.dialect().render(
                            com.legend.Compiler.lowerResolved(vs, ctx, rt, false));
                    actualRows = run(conn, sql, header);
                } catch (Exception e) {
                    // a service legend-lite cannot render or run is judged like a wrong answer: named, with its
                    // reason, never a crash that hides every service after it (Bazel workplan P3-18)
                    crashed = true;
                    actualRows = List.of("ERROR " + e.getClass().getSimpleName() + ": "
                            + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                }

                boolean same = expectedRows.equals(actualRows);
                // a quarantined service that CRASHES is not its known divergence: it is a new failure
                if (same && known.containsKey(name)) {
                    fixed.add(name);
                    System.out.println("FIXED " + name + " — remove from quarantine.py");
                } else if (same) {
                    agree.add(name);
                } else if (known.containsKey(name) && !crashed) {
                    knownFail.add(name);
                    System.out.println("KNOWN-DIVERGENCE " + name + " — " + known.get(name));
                } else {
                    disagree.add(name);
                    System.out.println("DISAGREE " + name);
                    diff(expectedRows, actualRows).forEach(l -> System.out.println("    " + l));
                }
            }
        } finally {
            // every session closes; the first failure to close is reported after the rest are closed
            SQLException unclosed = null;
            for (Connection conn : sessions.values()) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    if (unclosed == null) {
                        unclosed = e;
                    }
                }
            }
            if (unclosed != null) {
                throw unclosed;
            }
        }
        System.out.println("databases: " + sessions.keySet());

        System.out.printf("%n%d agree, %d known-divergence, %d unexpected, %d total%n",
                agree.size(), knownFail.size(), disagree.size() + fixed.size(),
                agree.size() + knownFail.size() + disagree.size() + fixed.size());
        assertFalse(agree.isEmpty(), "no services were compared");
        assertTrue(fixed.isEmpty(),
                "these now agree — remove them from quarantine.py: " + fixed);
        assertTrue(disagree.isEmpty(),
                "legend-lite disagrees with the reference evaluator on: " + disagree);
    }

    /**
     * The seed's tables on {@code conn}, every statement rendered by {@code backend}'s dialect: each table made as its
     * store declares it (its names, and which the store declared quoted: the queries spell them so) with the column
     * types the seed gives it, then filled with its rows, each cell text the database casts to its column
     * ({@link RowLoad}). A table no store of the corpus legend-lite loads declares is not made when a file
     * {@link StressExclusions} names declares it (no query of legend-lite's reads it, and one that did would fail as
     * its service's disagreement, named); any other is refused by name.
     */
    private static void seed(TestBackend backend, Connection conn, List<PackageableElement> elements)
            throws Exception {
        var dialect = backend.dialect();
        Map<String, DatabaseDefinition.TableDefinition> declared = declarations(elements);
        Set<String> schemas = new HashSet<>();
        List<String> undeclared = new ArrayList<>();
        try (Statement st = conn.createStatement()) {
            for (Json.Node node : Json.parseObject(Files.readString(SEED)).getArr("tables").items()) {
                Json.Obj table = (Json.Obj) node;
                String schema = table.getStringOr("schema", null);
                String name = table.getString("table");
                DatabaseDefinition.TableDefinition def = declared.get(key(schema, name));
                if (def == null) {
                    undeclared.add(key(schema, name));
                    continue;
                }
                var quoting = Ddl.quoting(def, schema);
                if (schema != null && schemas.add(schema)) {
                    st.execute(dialect.render(new SqlDdl.CreateSchema(schema, quoting.schema())));
                }
                List<SqlDdl.Column> columns = new ArrayList<>();
                for (Json.Node c : table.getArr("columns").items()) {
                    List<Json.Node> column = ((Json.Arr) c).items();
                    String columnName = ((Json.Str) column.get(0)).value();
                    DatabaseDefinition.ColumnDefinition col = def.columns().stream()
                            .filter(d -> d.name().equals(columnName)).findFirst()
                            .orElseThrow(() -> new IllegalStateException("the seed's column " + name + "."
                                    + columnName + " is not declared by its store"));
                    columns.add(new SqlDdl.Column(columnName, col.quoted(), new SqlDdl.ColumnType.Plain(
                            SqlDdl.ColumnType.Kind.valueOf(((Json.Str) column.get(1)).value())), false, false));
                }
                st.execute(dialect.render(new SqlDdl.DropTable(schema, name, quoting)));
                st.execute(dialect.render(new SqlDdl.CreateTable(schema, name, quoting, columns, false)));
                List<List<String>> rows = new ArrayList<>();
                for (Json.Node r : table.getArr("rows").items()) {
                    List<String> cells = new ArrayList<>();
                    for (Json.Node cell : ((Json.Arr) r).items()) {
                        cells.add(switch (cell) {
                            case Json.Str text -> text.value();
                            case Json.Null ignored -> null;
                            default -> throw new IllegalStateException("the seed's cell " + cell + " in "
                                    + key(schema, name) + " is neither text nor null: differential.py writes each"
                                    + " value as the text its column takes");
                        });
                    }
                    rows.add(cells);
                }
                if (!rows.isEmpty()) {
                    st.execute(dialect.render(new RowLoad(schema, name, quoting, List.of(), Set.of(), columns.size(),
                            rows).values()));
                }
            }
        }
        Set<String> excluded = excludedTables();
        List<String> stray = undeclared.stream().filter(t -> !excluded.contains(t)).toList();
        if (!stray.isEmpty()) {
            throw new IllegalStateException("the seed's tables " + stray + " are declared by no store of the corpus"
                    + " legend-lite loads, nor by a file it excludes");
        }
        System.out.println(backend.name() + ": " + undeclared.size()
                + " of the seed's tables are in files legend-lite excludes, not made: " + undeclared);
    }

    /** Every table the files legend-lite excludes declare ({@link StressExclusions}), by {@link #key}: a file it
     *  cannot build a model from still parses. */
    private static Set<String> excludedTables() throws Exception {
        Set<String> out = new HashSet<>();
        for (String file : StressExclusions.EXCLUDED.keySet()) {
            String text;
            try (var in = CorpusDifferentialTest.class.getResourceAsStream("/stress/" + file)) {
                text = new String(Objects.requireNonNull(in, "/stress/" + file + " is not on the classpath")
                        .readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            }
            out.addAll(declarations(com.legend.Compiler.parseModel(text).elements()).keySet());
        }
        return out;
    }

    /** A table's name in {@link #declarations}: {@code schema.table}, or {@code table} in the default schema. */
    private static String key(@com.legend.base.Nullable String schema, String table) {
        return schema == null ? table : schema + "." + table;
    }

    /** Every table the corpus's stores declare, by {@link #key}. Two stores may declare one table (a store includes
     *  another's); they must declare it alike. */
    private static Map<String, DatabaseDefinition.TableDefinition> declarations(List<PackageableElement> elements) {
        Map<String, DatabaseDefinition.TableDefinition> out = new HashMap<>();
        Map<String, DatabaseDefinition> stores = new HashMap<>();
        for (PackageableElement element : elements) {
            if (element instanceof DatabaseDefinition db) {
                stores.put(db.qualifiedName(), db);
            }
        }
        List<DatabaseDefinition> all = new ArrayList<>(stores.values());
        // an ingest's datasets are tables too (docs/DESK_DATA_ROADMAP_2026_10_29.md A2): of the database it makes, a
        // read dataset's columns its source table's
        for (PackageableElement element : elements) {
            if (element instanceof com.legend.model.IngestDefinition ingest) {
                all.add(ingest.database((db, schema, table) -> java.util.Optional.ofNullable(stores.get(db))
                        .flatMap(d -> (schema == null ? d.tables() : d.schemas().stream()
                                .filter(sc -> sc.name().equals(schema)).flatMap(sc -> sc.tables().stream()).toList())
                                .stream().filter(t -> t.name().equals(table)).findFirst())));
            }
        }
        for (DatabaseDefinition db : all) {
            for (var t : db.defaultSchemaTables()) {
                declare(out, key(null, t.name()), t);
            }
            for (var s : db.schemas()) {
                for (var t : s.tables()) {
                    declare(out, key(s.name(), t.name()), t);
                }
            }
        }
        return out;
    }

    private static void declare(Map<String, DatabaseDefinition.TableDefinition> out, String key,
            DatabaseDefinition.TableDefinition table) {
        DatabaseDefinition.TableDefinition before = out.putIfAbsent(key, table);
        if (before != null && !before.equals(table)) {
            throw new IllegalStateException("two stores declare " + key + " differently");
        }
    }

    /** Normalise exactly as scripts/corpus/differential.py does, then sort. */
    private List<String> run(Connection conn, String sql, String[] header) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                StringJoiner j = new StringJoiner("|");
                for (int i = 0; i < header.length; i++) {
                    String kind = header[i].substring(header[i].lastIndexOf(':') + 1);
                    j.add(cell(rs, i + 1, kind));
                }
                rows.add(j.toString());
            }
        }
        return rows.stream().sorted().collect(Collectors.toList());
    }

    private String cell(ResultSet rs, int i, String kind) throws SQLException {
        if ("float".equals(kind)) {
            double d = rs.getDouble(i);
            return rs.wasNull() ? NULL : sixPlaces(d);
        }
        String v = rs.getString(i);
        if (v == null) return NULL;
        // DuckDB renders a TIMESTAMP with fractional seconds; the seed has none.
        if ("timestamp".equals(kind) && v.endsWith(".0")) v = v.substring(0, v.length() - 2);
        return v;
    }

    private List<String> diff(List<String> expected, List<String> actual) {
        List<String> out = new ArrayList<>();
        var e = new LinkedHashSet<>(expected);
        var a = new LinkedHashSet<>(actual);
        e.stream().filter(r -> !a.contains(r)).limit(4)
                .forEach(r -> out.add("expected only: " + r));
        a.stream().filter(r -> !e.contains(r)).limit(4)
                .forEach(r -> out.add("actual   only: " + r));
        if (out.isEmpty()) out.add("same rows, different multiplicity");
        return out;
    }

    /** {@code d} as Python's {@code f"{d:.6f}"} spells it (differential.py's normalise): the EXACT binary value
     *  rounded half-even (%.6f rounds the shortest decimal string half-up: 0.0001625, 0.000162499... exactly, read
     *  0.000163 here and 0.000162 there); a negative that rounds to zero keeps its sign (-0.000000); nan, inf, -inf. */
    static String sixPlaces(double d) {
        if (Double.isNaN(d)) {
            return "nan";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "inf" : "-inf";
        }
        String s = new java.math.BigDecimal(d).setScale(6, java.math.RoundingMode.HALF_EVEN).toPlainString();
        boolean negative = Double.doubleToRawLongBits(d) < 0;
        return negative && !s.startsWith("-") ? "-" + s : s;
    }
}
