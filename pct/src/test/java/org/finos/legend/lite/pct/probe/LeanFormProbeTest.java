// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.probe;

import org.finos.legend.lite.pct.extension.PctBackend;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * THE LEAN PROGRAM'S FORM PROBE (measure only, 2026-11-15; P4's rule: a binding form lands only if measured no slower):
 * each line of the file named by {@code LEAN_PROBE_FILE} is a Pure expression ({@code |...}, planned by lite for the
 * lane's database) or a statement ({@code sql: ...}, a candidate form written by hand), {@code #} a comment. Each is run
 * on the lane's database with the plan's session statements, and printed as {@code [lean-probe]} with its characters,
 * its rows and the best of its timed runs after a warm one. The test always passes: it decides nothing. Run:
 * {@code bazel test //pct:pct_lean_form_probe_<lane> --test_env=LEAN_PROBE_FILE=<file> --test_output=all}.
 */
class LeanFormProbeTest {

    private static final int RUNS = Integer.getInteger("lean.probe.runs", 20);

    @Test
    void probe() throws Exception {
        String file = System.getenv("LEAN_PROBE_FILE");
        if (file == null) {
            System.out.println("[lean-probe] no LEAN_PROBE_FILE");
            return;
        }
        String model = PctBackend.storelessRuntime();
        String runtime = com.legend.testbackend.StorelessRuntime.RUNTIME;
        System.out.println("[lean-probe] lane " + PctBackend.name());
        List<String> session = new ArrayList<>();
        // the session's statements: the plan's target's, read from a trivial plan
        session.addAll(com.legend.Compiler.query(com.legend.Compiler.compileModel(model), "|1")
                .executionPlan(runtime).answerSql().target().session());
        for (String line : Files.readAllLines(Path.of(file), StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String sql;
            if (line.startsWith("sql:")) {
                sql = line.substring(4).trim();
            } else {
                try {
                    sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), line.replace("\\n", "\n"))
                            .executionPlan(runtime).answerSql().statement();
                } catch (RuntimeException e) {
                    System.out.println("[lean-probe] " + line + " | refused at planning: " + e.getMessage());
                    continue;
                }
                System.out.println("[lean-probe] " + line + " | planned: " + sql.replace('\n', ' '));
            }
            try (Connection conn = PctBackend.connect(); Statement st = conn.createStatement()) {
                for (String s : session) {
                    st.execute(s);
                }
                String rows = rows(st, sql);
                long best = Long.MAX_VALUE;
                for (int i = 0; i < RUNS; i++) {
                    long t0 = System.nanoTime();
                    rows(st, sql);
                    best = Math.min(best, System.nanoTime() - t0);
                }
                System.out.printf(java.util.Locale.ROOT, "[lean-probe]   %d chars | %.3f ms | rows %s%n",
                        sql.length(), best / 1e6, rows);
            } catch (java.sql.SQLException e) {
                System.out.println("[lean-probe]   " + sql.length() + " chars | RAISED " + e.getMessage());
            }
        }
    }

    private static String rows(Statement st, String sql) throws java.sql.SQLException {
        StringBuilder b = new StringBuilder();
        try (ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                if (b.length() > 0) {
                    b.append(';');
                }
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append(',');
                    }
                    b.append(rs.getString(i));
                }
            }
        }
        return b.toString();
    }
}
