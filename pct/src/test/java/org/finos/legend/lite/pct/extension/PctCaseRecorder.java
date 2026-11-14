// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/**
 * DEBUG ONLY: the render census's input (tools/census/README.md). With {@code -Dlegend.diagnostics=pct-cases},
 * every (model, expression) pair the PCT lane executes is appended to {@code pct-cases.tsv} in the
 * test's undeclared outputs -- each field Base64, one case a line -- so the census can lower and
 * render the same cases under every dialect, at any two commits, without the interpreter. Unset
 * (every normal run), nothing is written.
 */
final class PctCaseRecorder {

    private static final String DIR = !com.legend.diagnostics.Diagnostics.on("pct-cases") ? null
            : System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");

    /** The shape census ({@code pct-shapes}, the MongoDB line's homework 1, 2026-10-17): one line per case -- the
     *  plan's answer shape, the query's result type, the Java classes of its typed cells, and the expression -- so a
     *  lane knows which shapes its results carry. */
    private static final String SHAPES_DIR = !com.legend.diagnostics.Diagnostics.on("pct-shapes") ? null
            : System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");

    private PctCaseRecorder() {
    }

    static void recordShape(String expression, PctPlanRoute.Answer answer) {
        if (SHAPES_DIR == null) {
            return;
        }
        java.util.TreeSet<String> classes = new java.util.TreeSet<>();
        for (java.util.List<Object> row : answer.rows().rows()) {
            for (Object v : row) {
                classes.add(v == null ? "null" : v.getClass().getName());
            }
        }
        shapeLine(answer.shape().name(), String.valueOf(answer.resultType()),
                classes.isEmpty() ? "(empty)" : String.join("|", classes), expression);
    }

    /** A case that failed: its error's class and the first line of its message, under the kind {@code ERROR}. */
    static void recordShapeError(String expression, Throwable e) {
        if (SHAPES_DIR == null) {
            return;
        }
        String message = String.valueOf(e.getMessage());
        int nl = message.indexOf('\n');
        shapeLine("ERROR", e.getClass().getSimpleName(), nl < 0 ? message : message.substring(0, nl), expression);
    }

    private static void shapeLine(String kind, String type, String values, String expression) {
        String line = kind + "\t" + type.replace('\t', ' ').replace('\n', ' ') + "\t" + values.replace('\t', ' ')
                + "\t" + Base64.getEncoder().encodeToString(expression.getBytes(StandardCharsets.UTF_8)) + "\n";
        try {
            Files.writeString(Path.of(SHAPES_DIR, "pct-shapes.tsv"), line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void record(String model, String expression) {
        if (DIR == null) {
            return;
        }
        Base64.Encoder b64 = Base64.getEncoder();
        String line = b64.encodeToString(model.getBytes(StandardCharsets.UTF_8)) + "\t"
                + b64.encodeToString(expression.getBytes(StandardCharsets.UTF_8)) + "\n";
        try {
            Files.writeString(Path.of(DIR, "pct-cases.tsv"), line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
