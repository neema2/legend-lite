package com.legend.integration;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cross-engine differential: legend-lite's SQL, run against the same rows legend-engine
 * runs the .pure testSuites against, compared to the same oracle expectations.
 *
 * <p>The third assertion mechanism, wired 2026-10-05 (Bazel workplan P3-18, D3 row 4): until then
 * {@code scripts/corpus/differential.py} ran in no gate and an {@code Assumptions} guard skipped this
 * test in every build. It cannot be wrong about Legend semantics — it only reports whether two independent
 * implementations agree with the reference evaluator — and its blind spot is
 * equally plain: a defect both engines share is invisible to it.
 *
 * <p>Its inputs are a build output: {@code //scripts/corpus:gen_differential} runs
 * {@code scripts/corpus/differential.py} over the committed corpus into one tree ({@code seed.sql},
 * {@code expected/}), which {@code //core:corpus_differential_test} names by {@code -Dcorpus.differential}.
 *
 * <p>Comparison is on a normalised text form, not JSON, so that neither side's float
 * formatter or key ordering can manufacture a difference. Column kinds travel in the
 * expectation header for exactly that reason.
 */
@DisplayName("Corpus Differential (legend-lite SQL vs oracle)")
@Tag("differential")   // //core:corpus_differential_test alone: its data is that target's (core_tests excludes the tag)
class CorpusDifferentialTest {

    private static final Path DIFF = com.legend.testing.Runfile.property("corpus.differential");
    private static final String NULL = "~";

    @Test
    @DisplayName("every stress service agrees with the reference evaluator")
    void differential() throws Exception {

        String model = StressCorpus.model();
        StressCorpus.reportExclusions();
        var ctx = com.legend.Compiler.compileModel(model);
        var dialect = new com.legend.sql.dialect.DuckDb();

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
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            seed(conn);
            for (var el : com.legend.testing.Own.model(model).elements()) {
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
                try {
                    var vs = com.legend.compiler.NameResolver.resolveQuery(svc.functionBody());
                    // The service's own runtime, not a fixed one — see StressServiceSuitesTest.
                    String rt = svc.runtimeRef() != null ? svc.runtimeRef() : "stress::RT";
                    String sql = dialect.render(
                            com.legend.Compiler.lowerResolved(vs, ctx, rt, false));
                    actualRows = run(conn, sql, header);
                } catch (Exception e) {
                    // a service legend-lite cannot render or run is judged like a wrong answer: named, with its
                    // reason, never a crash that hides every service after it (Bazel workplan P3-18)
                    actualRows = List.of("ERROR " + e.getClass().getSimpleName() + ": "
                            + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                }

                boolean same = expectedRows.equals(actualRows);
                if (same && known.containsKey(name)) {
                    fixed.add(name);
                    System.out.println("FIXED " + name + " — remove from quarantine.py");
                } else if (same) {
                    agree.add(name);
                } else if (known.containsKey(name)) {
                    knownFail.add(name);
                    System.out.println("KNOWN-DIVERGENCE " + name + " — " + known.get(name));
                } else {
                    disagree.add(name);
                    System.out.println("DISAGREE " + name);
                    diff(expectedRows, actualRows).forEach(l -> System.out.println("    " + l));
                }
            }
        }

        System.out.printf("%n%d agree, %d known-divergence, %d unexpected, %d total%n",
                agree.size(), knownFail.size(), disagree.size() + fixed.size(),
                agree.size() + knownFail.size() + disagree.size() + fixed.size());
        assertFalse(agree.isEmpty(), "no services were compared");
        assertTrue(fixed.isEmpty(),
                "these now agree — remove them from quarantine.py: " + fixed);
        assertTrue(disagree.isEmpty(),
                "legend-lite disagrees with the reference evaluator on: " + disagree);
    }

    private void seed(Connection conn) throws Exception {
        String script = Files.readString(DIFF.resolve("seed.sql"));
        try (Statement st = conn.createStatement()) {
            for (String stmt : script.split(";\\s*\\n")) {
                String s = stmt.lines()
                        .filter(l -> !l.trim().startsWith("--"))
                        .collect(Collectors.joining("\n")).trim();
                if (!s.isEmpty()) st.execute(s);
            }
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
            // the EXACT binary value rounded half-even, as Python's f"{x:.6f}" rounds it: %.6f rounds the shortest
            // decimal string half-up, so 0.0001625 (0.000162499... exactly) read 0.000163 here and 0.000162 there
            return rs.wasNull() ? NULL
                    : new java.math.BigDecimal(d).setScale(6, java.math.RoundingMode.HALF_EVEN).toPlainString();
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
}
