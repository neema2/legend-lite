// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * PROBE (2026-10-31, docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md, C1): what each database the tests reach does
 * with a window frame whose offset is not a literal -- a constant expression, a bound parameter -- and with the frames
 * an expression can make: a negative offset, and a window whose start comes after its end. Each over the same five
 * rows (x = 0..4), the answer each row's {@code SUM(x)}; a refusal its message's first line. Prints a table between
 * PROBE-REPORT and PROBE-END; asserts nothing (evidence, run by hand: {@code //core:frame_offset_probe_test}).
 */
class FrameOffsetProbeTest {

    private static final String ROWS = "(SELECT 0 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x"
            + " UNION ALL SELECT 3 AS x UNION ALL SELECT 4 AS x)";

    private record Probe(String what, String frame, Integer bound) {
    }

    private static final List<Probe> PROBES = List.of(
            new Probe("literal: 2 PRECEDING", "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", null),
            new Probe("expression: (3 - 1) PRECEDING", "ROWS BETWEEN (3 - 1) PRECEDING AND CURRENT ROW", null),
            new Probe("expression unparenthesized: 3 - 1 PRECEDING", "ROWS BETWEEN 3 - 1 PRECEDING AND CURRENT ROW", null),
            new Probe("parameter: ? PRECEDING (2)", "ROWS BETWEEN ? PRECEDING AND CURRENT ROW", 2),
            new Probe("parameter in expression: (? - 1) PRECEDING (3)", "ROWS BETWEEN (? - 1) PRECEDING AND CURRENT ROW", 3),
            new Probe("expression FOLLOWING: (1 + 1) FOLLOWING", "ROWS BETWEEN CURRENT ROW AND (1 + 1) FOLLOWING", null),
            new Probe("negative expression: (1 - 3) PRECEDING", "ROWS BETWEEN (1 - 3) PRECEDING AND CURRENT ROW", null),
            new Probe("negative parameter: ? PRECEDING (-2)", "ROWS BETWEEN ? PRECEDING AND CURRENT ROW", -2),
            new Probe("reversed: CURRENT ROW AND 1 PRECEDING", "ROWS BETWEEN CURRENT ROW AND 1 PRECEDING", null),
            new Probe("reversed: 1 PRECEDING AND 2 PRECEDING", "ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING", null),
            new Probe("reversed by expression: (2 - 1) PRECEDING AND (1 + 1) PRECEDING",
                    "ROWS BETWEEN (2 - 1) PRECEDING AND (1 + 1) PRECEDING", null),
            new Probe("reversed by parameter: ? PRECEDING AND 2 PRECEDING (1)",
                    "ROWS BETWEEN ? PRECEDING AND 2 PRECEDING", 1),
            new Probe("range expression: RANGE (3 - 1) PRECEDING", "RANGE BETWEEN (3 - 1) PRECEDING AND CURRENT ROW", null),
            new Probe("range parameter: RANGE ? PRECEDING (2)", "RANGE BETWEEN ? PRECEDING AND CURRENT ROW", 2));

    private static String answer(Connection c, Probe p) {
        String sql = "SELECT x, SUM(x) OVER (ORDER BY x " + p.frame() + ") AS s FROM " + ROWS + " AS t ORDER BY x";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            if (p.bound() != null) {
                ps.setInt(1, p.bound());
            }
            List<String> sums = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Object s = rs.getObject(2);
                    sums.add(s == null ? "null" : String.valueOf(((Number) s).longValue()));
                }
            }
            return String.join(",", sums);
        } catch (Exception e) {
            String m = String.valueOf(e.getMessage()).replace('\n', ' ');
            return "REFUSED: " + (m.length() > 110 ? m.substring(0, 110) + "..." : m);
        }
    }

    @Test
    void eachDatabasesWindowFrameOffsets() throws Exception {
        List<DatabaseType> types = new ArrayList<>(List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite));
        for (String name : System.getProperty("probe.servers", "").split(",")) {
            if (!name.isBlank()) {
                types.add(DatabaseType.valueOf(name.trim()));
            }
        }
        StringBuilder report = new StringBuilder("PROBE-REPORT\n");
        for (DatabaseType type : types) {
            TestBackend backend = TestBackends.of(type);
            report.append("== ").append(type).append('\n');
            try (Connection c = backend.open()) {
                for (Probe p : PROBES) {
                    report.append(String.format(Locale.ROOT, "  %-62s %s%n", p.what(), answer(c, p)));
                }
            } catch (Exception e) {
                report.append("  (could not open: ").append(e.getMessage()).append(")\n");
            }
        }
        System.out.println(report.append("PROBE-END"));
    }
}
