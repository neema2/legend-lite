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

    /** THE EDGE GOLDEN NET (2026-11-16, the edge program's Phase 0): with {@code -Dlegend.edge.answers=<file>}, every
     *  case's answer as upstream sees it -- a relation's columns and its rows (sorted: an unordered result's order is
     *  the database's), a value's typed cells, an error's message -- one line per case, keyed by the running test
     *  and its case's place in it, written sorted when the run ends. Trace and execution ids are masked. Unset,
     *  nothing is kept. */
    private static final String EDGE = System.getProperty("legend.edge.answers");

    private static final java.util.Map<String, String> EDGE_LINES = new java.util.TreeMap<>();

    private static final java.util.Map<String, Integer> EDGE_SEQ = new java.util.HashMap<>();

    static {
        if (EDGE != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                StringBuilder out = new StringBuilder();
                synchronized (EDGE_LINES) {
                    EDGE_LINES.forEach((k, v) -> out.append(k).append('\t').append(v).append('\n'));
                }
                try {
                    Files.writeString(Path.of(EDGE), out);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
        }
    }

    private static final java.util.regex.Pattern IDS = java.util.regex.Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|\\b[0-9a-fA-F]{16,}\\b");

    private static String flat(String s) {
        return IDS.matcher(s).replaceAll("<id>").replace('\t', ' ').replace("\r", "").replace("\n", "\\n");
    }

    private static void edge(String expression, String kind, String text) {
        if (EDGE == null) {
            return;
        }
        String test = String.valueOf(System.getProperty("legend.test.current"));
        synchronized (EDGE_LINES) {
            int n = EDGE_SEQ.merge(test, 1, Integer::sum);
            EDGE_LINES.put(test + "\t" + String.format(java.util.Locale.ROOT, "%03d", n), kind + "\t" + flat(text) + "\t" + flat(expression));
        }
    }

    /** A case's answer, for the edge golden net. */
    static void edgeAnswer(String expression, PctPlanRoute.Answer answer) {
        if (EDGE == null) {
            return;
        }
        if (answer.engineJson() != null) {
            com.legend.json.Json.Obj json = com.legend.json.Json.parseObject(answer.engineJson());
            StringBuilder text = new StringBuilder("columns=").append(write(((com.legend.json.Json.Obj) json.get("builder"))
                    .get("columns")));
            java.util.List<String> rows = new java.util.ArrayList<>();
            for (com.legend.json.Json.Node r : ((com.legend.json.Json.Arr) ((com.legend.json.Json.Obj) json.get("result"))
                    .get("rows")).items()) {
                rows.add(write(r));
            }
            java.util.Collections.sort(rows);
            text.append(" rows=").append(String.join(",", rows));
            edge(expression, answer.shape().name(), text.toString());
            return;
        }
        StringBuilder text = new StringBuilder("type=").append(answer.resultType()).append(" values=");
        for (java.util.List<Object> row : answer.rows().rows()) {
            for (Object v : row) {
                text.append(v == null ? "null" : v.getClass().getSimpleName() + ":" + v).append(";");
            }
        }
        edge(expression, answer.shape().name(), text.toString());
    }

    /** A case that raised, for the edge golden net: its message as the test sees it. */
    static void edgeError(String expression, Throwable e) {
        edge(expression, "ERROR", String.valueOf(e.getMessage()));
    }

    private static String write(com.legend.json.Json.Node n) {
        return com.legend.json.Json.toCompact(n);
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
