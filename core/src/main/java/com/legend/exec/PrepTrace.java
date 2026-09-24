// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * Diagnostics only (perf homework 2026-09-20, DATABASE_MODE_HOMEWORK §4ac):
 * with {@code LEGEND_LITE_PREP_TRACE=<file>} set, every prepared statement
 * appends one line — {@code chars, prepare-nanos, execute-nanos, kind} —
 * the input to "is prepare time linear in statement text?". Off by
 * default; no verdict reads it.
 */
public final class PrepTrace {

    private PrepTrace() {
    }

    private static final String FILE = System.getenv("LEGEND_LITE_PREP_TRACE");

    /** Prepares {@code stamped} on {@code conn}, recording the prepare time
     * against the bare {@code sql} (a line {@code chars, prepare-nanos, 0, kind}). */
    static java.sql.PreparedStatement prepared(java.sql.Connection conn, String stamped, String sql)
            throws java.sql.SQLException {
        long t0 = System.nanoTime();
        java.sql.PreparedStatement st = conn.prepareStatement(stamped);
        record(sql, System.nanoTime() - t0, 0L);
        return st;
    }

    /** Executes a prepared query, recording the execute time
     * (a line {@code chars, 0, execute-nanos, kind}). */
    static java.sql.ResultSet executed(java.sql.PreparedStatement st, String sql)
            throws java.sql.SQLException {
        long t0 = System.nanoTime();
        java.sql.ResultSet rs = st.executeQuery();
        record(sql, 0L, System.nanoTime() - t0);
        return rs;
    }

    private static void record(String sql, long prepareNanos, long executeNanos) {
        if (FILE == null) {
            return;
        }
        String kind = sql.contains(" AS __ix") || sql.contains(" AS \"__ix\"") ? "fused" : sql.contains(" AS __verdict") ? "verdict"
                : sql.startsWith("WITH frame_") ? "frame" : "other";
        append(FILE, sql.length() + "\t" + prepareNanos + "\t" + executeNanos + "\t" + kind + "\n");
    }

    /** One fused statement's branch sizes: {@code family, chars} per pending verdict. */
    public static void branch(String family, int chars) {
        if (FILE == null) {
            return;
        }
        append(FILE + ".branches", family + "\t" + chars + "\n");
    }

    private static void append(String file, String line) {
        try {
            java.nio.file.Files.writeString(java.nio.file.Path.of(file), line,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("prep trace: " + e.getMessage(), e);
        }
    }
}
