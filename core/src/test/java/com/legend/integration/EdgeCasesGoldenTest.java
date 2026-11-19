// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE EDGE GOLDEN NET's MISSING CASES (S0 of the presentation program, 2026-11-18,
 * docs/records/presentation-program-design-2026-11-18.md §4 (d)): the texts the PR #118 audit found wrong or
 * unwatched (V-EDGE), each answered on DuckDB and H2 under both of the edge's styles and written under {@code
 * -Dlegend.edge.cases=<file>}, so a later slice that moves one shows it as a listed move:
 * <ul>
 *   <li>EDGE-1: a Float cell in a relation's {@code toString} and in a TDS's {@code toCSV}, at the five values the
 *       audit measured (fixed by F7 in the fix round: the net pins the fix);</li>
 *   <li>EDGE-2: a DateTime in a TDS's {@code toCSV} (ISO text today, upstream's {@code yyyy-MM-dd HH:mm:ss}: pinned as
 *       it is, a listed move when its form moves);</li>
 *   <li>EDGE-3: {@code format('%r', ...)} of a string holding a newline (raw today, upstream's {@code \n}: pinned as
 *       it is).</li>
 * </ul>
 * A case that fails is recorded with its message: the net pins what the edge answers, not what it should.
 */
class EdgeCasesGoldenTest {

    private static final String CSV = "->meta::relational::tests::csv::toCSV()";

    /** name -> the query. */
    private static final Map<String, String> CASES = new LinkedHashMap<>();

    static {
        for (String v : List.of("0.00001", "0.0005", "25000000.0", "10000000000000000.0", "100000000000000000000.0")) {
            CASES.put("edge1.toString." + v, "|#TDS\n  x:Float\n  " + v + "\n#->toString()");
            CASES.put("edge1.toCSV." + v, "|#TDS\n  x:Float\n  " + v + "\n#" + CSV);
        }
        CASES.put("edge2.toCSV.dateTime", "|#TDS\n  id:Integer, at:DateTime\n  1, 2024-01-02T10:30:00.123+0000\n#" + CSV);
        CASES.put("edge3.formatRepr.newline", "|format('%r', ['a\\nb'])");
    }

    private static final Map<String, PlanStyle> STYLES = new LinkedHashMap<>();

    static {
        STYLES.put("upstream", com.legend.edge.PureText.UPSTREAM);
        STYLES.put("pure_v1", com.legend.edge.PureText.PURE_V1);
    }

    private static final java.util.regex.Pattern IDS = java.util.regex.Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private static String flat(String s) {
        // a backslash escaped first: a raw newline (EDGE-3) and an escaped one are two texts
        return IDS.matcher(s).replaceAll("<id>").replace("\\", "\\\\").replace('\t', ' ').replace("\r", "")
                .replace("\n", "\\n");
    }

    private static String answer(TestBackend backend, PlanStyle style, String query) {
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        try {
            Lean.Result r = Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, backend, style);
            StringBuilder cells = new StringBuilder(r.shape().name()).append('\t');
            for (List<Object> row : r.rows()) {
                for (Object v : row) {
                    cells.append(v == null ? "null" : v.getClass().getSimpleName() + ":" + v).append(';');
                }
            }
            return cells.toString();
        } catch (RuntimeException e) {
            return "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    @Test
    void everyCasesAnswer() throws IOException {
        StringBuilder out = new StringBuilder();
        // the platform's own engine and legend-engine's database, by role (the dialect-leak ban: a test names no
        // database; the census's DialectLeakRatchetTest caught the names here in the compatibility program's slice 0.2,
        // 2026-11-19): Databases.REPLAY_REFEREE is the type legend-engine's tests run on, lite's own session of it here
        for (DatabaseType type : List.of(com.legend.database.Databases.PLATFORM,
                com.legend.database.Databases.REPLAY_REFEREE)) {
            TestBackend backend = TestBackends.of(type);
            for (Map.Entry<String, PlanStyle> style : STYLES.entrySet()) {
                for (Map.Entry<String, String> c : CASES.entrySet()) {
                    out.append(type.name().toLowerCase(java.util.Locale.ROOT)).append('\t').append(style.getKey())
                            .append('\t').append(c.getKey()).append('\t')
                            .append(flat(answer(backend, style.getValue(), c.getValue()))).append('\n');
                }
            }
        }
        String path = System.getProperty("legend.edge.cases");
        if (path != null) {
            Files.writeString(Path.of(path), out, StandardCharsets.UTF_8);
        }
        assertFalse(out.isEmpty());
    }
}
