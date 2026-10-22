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
     *  result's kind, its Pure type, the Java classes of its values (a scalar's, a collection's elements'), and the
     *  expression -- so a lane whose results cross the adapter boundary as text knows which shapes it must carry. */
    private static final String SHAPES_DIR = !com.legend.diagnostics.Diagnostics.on("pct-shapes") ? null
            : System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");

    private PctCaseRecorder() {
    }

    static void recordShape(String expression, com.legend.exec.ExecutionResult result) {
        if (SHAPES_DIR == null) {
            return;
        }
        String kind = result.getClass().getSimpleName();
        String type;
        String values;
        switch (result) {
            case com.legend.exec.ExecutionResult.Scalar s -> {
                type = String.valueOf(s.returnType());
                values = s.value() == null ? "null" : s.value().getClass().getName();
            }
            case com.legend.exec.ExecutionResult.Collection c -> {
                type = String.valueOf(c.returnType());
                java.util.TreeSet<String> classes = new java.util.TreeSet<>();
                for (Object v : c.values()) {
                    classes.add(v == null ? "null" : v.getClass().getName());
                }
                values = classes.isEmpty() ? "(empty)" : String.join("|", classes);
            }
            case com.legend.exec.ExecutionResult.TdsText t -> {
                type = String.valueOf(t.returnType());
                values = "text";
            }
            case com.legend.exec.ExecutionResult.Graph g -> {
                type = String.valueOf(g.returnType());
                values = "json";
            }
            case com.legend.exec.ExecutionResult.Tabular t -> {
                type = String.valueOf(t.returnType());
                values = "rows";
            }
        }
        shapeLine(kind, type, values, expression);
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
