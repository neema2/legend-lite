// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * Diagnostics only (perf homework 2026-09-20, DATABASE_MODE_HOMEWORK §4ac):
 * with {@code LEGEND_LITE_PREP_TRACE=<file>} set, the verdict batch appends
 * each fused statement's branch sizes to {@code <file>.branches}. Off by
 * default; no verdict reads it. The per-statement timing lines (the arms
 * {@code prepared} and {@code executed}: {@code chars, prepare-nanos,
 * execute-nanos, kind}) went 2026-10-29 with the executor that called them
 * (the doors' audit, D64): the plan runner prepares every statement, and no
 * caller remained.
 */
public final class PrepTrace {

    private PrepTrace() {
    }

    private static final String FILE = System.getenv("LEGEND_LITE_PREP_TRACE");

    /** Is the trace on (a file named)? A caller measures a branch only then. */
    public static boolean on() {
        return FILE != null;
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
