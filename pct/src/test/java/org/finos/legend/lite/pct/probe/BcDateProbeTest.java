// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.probe;

import org.finos.legend.lite.pct.extension.PctBackend;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * P3 SITE 4'S MEASUREMENT (measure only; the orchestrator's go of 2026-11-13, docs/LEAN_PRODUCT_P3_2026_11_10.md §6):
 * what the lane's DRIVER answers for a date and a date-time before Christ, and for an AD control, by each getter the
 * decode could use -- the default object, {@code getObject} as {@code LocalDate}, {@code LocalDateTime} and
 * {@code OffsetDateTime}, {@code getString}, and an array's leaves -- so the lane can tell whether a typed getter (or
 * the database's own text) gives the proleptic-Gregorian value with no Java repair. (An array's leaves are read where
 * a cell is an array; a list answer is written per element, so no case here makes one: the array leaf arm's measure
 * needs a column of a list type, the lane's own.) The value is planned by lite for
 * the lane's own database (no SQL spelled here); each line is printed as {@code [bc-probe]}, and the test always
 * passes: it decides nothing. Run per lane: {@code bazel test //pct:pct_bc_date_probe_<lane> --test_output=all}.
 */
class BcDateProbeTest {

    /** The values probed: Pure expressions over the platform's constructors, BC first, then the AD control. */
    private static final List<String> CASES = List.of(
            "|meta::pure::functions::date::date(-43, 3, 15)",
            "|meta::pure::functions::date::date(-43, 3, 15, 10, 30, 0)",
            "|meta::pure::functions::date::date(2024, 1, 2)",
            "|meta::pure::functions::date::date(2024, 1, 2, 10, 30, 0)");

    @Test
    void probe() throws Exception {
        String model = PctBackend.storelessRuntime();
        String runtime = com.legend.testbackend.StorelessRuntime.RUNTIME;
        System.out.println("[bc-probe] lane " + PctBackend.name());
        for (String expression : CASES) {
            String sql;
            try {
                sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), expression)
                        .executionPlan(runtime).answerSql().statement();
            } catch (RuntimeException e) {
                System.out.println("[bc-probe] " + expression + " | refused at planning: " + e.getMessage());
                continue;
            }
            System.out.println("[bc-probe] " + expression + " | sql: " + sql);
            try (Connection c = PctBackend.connect(); Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                if (!rs.next()) {
                    System.out.println("[bc-probe]   no row");
                    continue;
                }
                System.out.println("[bc-probe]   reported type: " + rs.getMetaData().getColumnTypeName(1));
                report("default", () -> rs.getObject(1));
                report("LocalDate", () -> rs.getObject(1, java.time.LocalDate.class));
                report("LocalDateTime", () -> rs.getObject(1, java.time.LocalDateTime.class));
                report("OffsetDateTime", () -> rs.getObject(1, java.time.OffsetDateTime.class));
                report("getString", () -> rs.getString(1));
                report("array leaves", () -> {
                    java.sql.Array a = rs.getArray(1);
                    Object arr = a == null ? null : a.getArray();
                    if (!(arr instanceof Object[] items)) {
                        return arr;
                    }
                    StringBuilder b = new StringBuilder("[");
                    for (Object o : items) {
                        b.append(describe(o)).append("; ");
                    }
                    return b.append("]").toString();
                });
            } catch (java.sql.SQLException e) {
                System.out.println("[bc-probe]   the statement raised: " + e.getMessage());
            }
        }
    }

    @FunctionalInterface
    private interface Read {
        Object read() throws Exception;
    }

    private static void report(String getter, Read read) {
        try {
            System.out.println("[bc-probe]   " + getter + ": " + describe(read.read()));
        } catch (Exception e) {
            System.out.println("[bc-probe]   " + getter + ": raised " + e.getClass().getSimpleName() + ": "
                    + e.getMessage());
        }
    }

    private static String describe(Object o) {
        if (o == null) {
            return "null";
        }
        String era = o instanceof java.util.Date d ? " (era " + eraOf(d) + ")" : "";
        return o.getClass().getName() + " " + o + era;
    }

    private static String eraOf(java.util.Date d) {
        java.util.GregorianCalendar g = new java.util.GregorianCalendar(java.util.TimeZone.getTimeZone("UTC"),
                java.util.Locale.ROOT);
        g.setTime(d);
        return g.get(java.util.Calendar.ERA) == java.util.GregorianCalendar.BC ? "BC" : "AD";
    }
}
