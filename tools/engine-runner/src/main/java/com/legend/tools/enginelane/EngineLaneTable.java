// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.enginelane;

import com.legend.json.Json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE ENGINE LANE'S TABLE (H7, slice 0.3 of docs/records/compat-program-plan-2026-11-19.md): each PCT test the lane ran
 * as legend-engine's audience ({@code //pct:engine_lane_*}, its outcomes by full name), joined with legend-engine's own
 * manifest for the same database and suite ({@code @legend_engine_src}'s {@code pct-manifests}, spec input only), and
 * classified. The PCT module holds no comparison machinery ({@code PctDisciplineTest}), so the join is here.
 *
 * <ul>
 *   <li><b>legend-engine:</b> {@code PASS} (not in the manifest), {@code REFUSES} (the manifest's error is "No SQL
 *       translation exists for the PURE function '&lt;signature&gt;'": keyed by that signature and the database), or
 *       {@code DEFECT} (any other expected error), its class read from the message ({@link #defect}).</li>
 *   <li><b>lite:</b> {@code PASS} (PCT's assert held: lite answered PCT's expected value) or {@code FAIL}, its class read
 *       from the message ({@link #liteFailure}).</li>
 *   <li><b>verdict:</b> {@code SAME} (both pass), {@code LITE_BEHIND} (legend-engine passes, lite fails),
 *       {@code LITE_ANSWERS} (legend-engine refuses, lite answers PCT's expected value: E-R2's referee),
 *       {@code LITE_FIXES} (legend-engine fails by its defect, lite answers), {@code BOTH_FAIL}, or
 *       {@code MANIFEST_ONLY} (a manifest row for a test the lane did not run).</li>
 * </ul>
 *
 * <p>Statuses and classes are compared, never words: words are the presenter's (B1-R's note, 2026-11-19), and a message
 * holds run-time ids. So the table holds no message text, and moves only when a class moves.
 *
 * <p>Usage: {@code table --db=<db> --suite=<Suite> --manifest=<json> --outcomes=<tsv> --out=<tsv>} for one lane;
 * {@code summary --out=<tsv> --refusals-out=<tsv> <table> ...} for the counts per lane and the refusals by signature.
 */
public final class EngineLaneTable {

    /** legend-engine's refusal: its words for a function its relational lowering has no SQL for. */
    private static final Pattern NO_SQL = Pattern.compile("No SQL translation exists for the PURE function '([^']+)'");

    /** A manifest's test name: the test's full name and its PCT signature. */
    private static final Pattern SIGNED = Pattern.compile("^(.*)_Function_1__Boolean_1_$");

    /** A PCT assert's frame, before the message it carries. */
    private static final Pattern ASSERT_FRAME = Pattern.compile("^Assert failure at \\(resource:[^)]*\\), ");

    static final String HEADER = "test\tengine\tengine_class\tlite\tlite_class\tverdict";

    private EngineLaneTable() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: table --db= --suite= --manifest= --outcomes= --out= | summary"
                    + " --out= --refusals-out= <table> ...");
        }
        Map<String, String> opts = new LinkedHashMap<>();
        List<String> rest = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--") && a.contains("=")) {
                opts.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
            } else {
                rest.add(a);
            }
        }
        switch (args[0]) {
            case "table" -> Files.writeString(Path.of(need(opts, "out")), table(need(opts, "db"), need(opts, "suite"),
                    Files.readString(Path.of(need(opts, "manifest"))),
                    Files.readAllLines(Path.of(need(opts, "outcomes")), StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
            case "summary" -> {
                Map<String, List<String>> tables = new TreeMap<>();
                for (String t : rest) {
                    tables.put(Path.of(t).getFileName().toString().replaceFirst("\\.tsv$", ""),
                            Files.readAllLines(Path.of(t), StandardCharsets.UTF_8));
                }
                Files.writeString(Path.of(need(opts, "out")), summary(tables), StandardCharsets.UTF_8);
                Files.writeString(Path.of(need(opts, "refusals-out")), refusals(tables), StandardCharsets.UTF_8);
            }
            default -> throw new IllegalArgumentException("table or summary, not " + args[0]);
        }
    }

    private static String need(Map<String, String> opts, String key) {
        String v = opts.get(key);
        if (v == null) {
            throw new IllegalArgumentException("--" + key + "= is required");
        }
        return v;
    }

    /** One lane's table: a header naming the lane, then one line per test, sorted by name. */
    static String table(String db, String suite, String manifestJson, List<String> outcomeLines) {
        // the manifest's expected errors by the test's full name (its signature dropped, as the lane names a test)
        Map<String, String> expected = new TreeMap<>();
        for (Json.Node n : Json.parseObject(manifestJson).getArr("exclusions").items()) {
            Json.Obj e = (Json.Obj) n;
            String test = e.getString("test");
            Matcher m = SIGNED.matcher(test);
            if (!m.matches()) {
                throw new IllegalStateException("a manifest test without the PCT signature: " + test);
            }
            expected.putIfAbsent(m.group(1), String.valueOf(e.getStringOr("expectedError", "")));
        }
        Map<String, String[]> rows = new TreeMap<>();
        for (String line : outcomeLines) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", 3);
            String test = f[0];
            boolean litePass = "SUCCESSFUL".equals(f[1]);
            String liteClass = litePass ? "" : liteFailure(f.length > 2 ? f[2] : "");
            // a test the manifest does not list is one legend-engine passes
            String listed = expected.get(test);
            String[] engine = listed == null ? new String[] {"PASS", ""} : engine(listed);
            rows.put(test, new String[] {engine[0], engine[1], litePass ? "PASS" : "FAIL", liteClass,
                    verdict(engine[0], litePass)});
        }
        for (Map.Entry<String, String> e : expected.entrySet()) {
            if (!rows.containsKey(e.getKey())) {
                String[] engine = engine(e.getValue());
                rows.put(e.getKey(), new String[] {engine[0], engine[1], "-", "", "MANIFEST_ONLY"});
            }
        }
        StringBuilder out = new StringBuilder("# the ENGINE lane, " + db + " " + suite + ": " + rows.size()
                + " tests (bazel run //pct:update_engine_lane)\n" + HEADER + "\n");
        rows.forEach((test, r) -> out.append(test).append('\t').append(String.join("\t", r)).append('\n'));
        return out.toString();
    }

    /** legend-engine's outcome for a test, from its manifest's expected error: {class, detail}. */
    static String[] engine(String expectedError) {
        Matcher m = NO_SQL.matcher(expectedError);
        if (m.find()) {
            return new String[] {"REFUSES", m.group(1)};
        }
        return new String[] {"DEFECT", defect(expectedError)};
    }

    private static String verdict(String engine, boolean litePass) {
        return switch (engine) {
            case "PASS" -> litePass ? "SAME" : "LITE_BEHIND";
            case "REFUSES" -> litePass ? "LITE_ANSWERS" : "BOTH_FAIL";
            case "DEFECT" -> litePass ? "LITE_FIXES" : "BOTH_FAIL";
            default -> throw new IllegalStateException(engine);
        };
    }

    /** The class of legend-engine's defect, read from its expected error (measured on DuckDB's and H2's manifests,
     *  2026-11-19: every row of the six falls in one, OTHER the rest). */
    static String defect(String message) {
        String m = message.strip();
        if (answer(m)) {
            return "WRONG_ANSWER";
        }
        if (errorMismatch(m)) {
            return "ERROR_MISMATCH";
        }
        if (any(m, "SQLException", "Binder Error", "Conversion Error", "Catalog Error", "Invalid Input Error",
                "Out of Range Error", "Parser Error", "Data conversion error", "JdbcSQL", "SQL statement")) {
            return "DATABASE_ERROR";
        }
        String lower = m.toLowerCase(Locale.ROOT);
        if (any(lower, "not supported", "does not support", "doesn't support", "unsupported", "not managed yet",
                "couldn't find dynafunction", "is supported only", "are only supported", "only supports",
                "isn't supported", "not implemented", "isn't a literal", "no relational processor matched")) {
            return "UNSUPPORTED";
        }
        if (any(m, "Match failure", "Cast exception", "cannot be cast", "Error dynamically evaluating", "Unexpected error",
                "Function does not exist", "no viable alternative", "Failed to infer", "trying to get an element",
                "initial value type must", "Unused format args", "Expected at most one object",
                "Expected one table alias")) {
            return "PLANNER_FAULT";
        }
        // legend-engine's run of its own plan, or its reading of the answer back into Pure
        if (any(m, "Cannot cast a collection of size", "Date has no day", "NullPointer", "JsonParseException")) {
            return "RUNTIME_FAULT";
        }
        return "OTHER";
    }

    /** The class of lite's failure, read from the run's message (measured on the lane's first run, 2026-11-19). */
    static String liteFailure(String message) {
        String m = ASSERT_FRAME.matcher(message.strip()).replaceFirst("");
        if (answer(m)) {
            return "WRONG_ANSWER";
        }
        if (errorMismatch(m)) {
            return "ERROR_MISMATCH";
        }
        String lower = m.toLowerCase(Locale.ROOT);
        if (any(lower, "reached a dialect without", "not yet implemented", "not implemented", "notimplemented",
                "refuse", "does not plan", "no overload", "unsupportedoperation", "not supported", "dialectcapability")) {
            return "REFUSED";
        }
        if (any(m, "Invalid Input Error", "Out of Range Error", "Binder Error", "Conversion Error", "Catalog Error",
                "Parser Error", "SQL statement", "SQLException")) {
            return "DATABASE_ERROR";
        }
        return "OTHER";
    }

    /** A PCT assert that held a different value: its expected and its actual, or legend-pure's bare "Assert failed". */
    private static boolean answer(String m) {
        return (m.contains("expected:") && m.contains("actual:") && !errorMismatch(m)) || m.contains("Assert failed");
    }

    /** A PCT error test that saw no error, or another error, or the right one at another place. */
    private static boolean errorMismatch(String m) {
        return any(m, "No error was thrown", "The actual message was", "message mismatch", "column mismatch",
                "line mismatch");
    }

    private static boolean any(String m, String... parts) {
        for (String p : parts) {
            if (m.contains(p)) {
                return true;
            }
        }
        return false;
    }

    /** The counts per lane: legend-engine's classes, lite's, and the verdicts. */
    static String summary(Map<String, List<String>> tables) {
        StringBuilder out = new StringBuilder("# the ENGINE lane's counts per lane (bazel run //pct:update_engine_lane)\n"
                + "lane\tkey\tcount\n");
        tables.forEach((lane, lines) -> {
            Map<String, Integer> counts = new TreeMap<>();
            for (String[] r : rows(lines)) {
                counts.merge("tests", 1, Integer::sum);
                counts.merge("engine." + r[1], 1, Integer::sum);
                if (!r[2].isEmpty() && "DEFECT".equals(r[1])) {
                    counts.merge("engine.DEFECT." + r[2], 1, Integer::sum);
                }
                counts.merge("lite." + r[3], 1, Integer::sum);
                if (!r[4].isEmpty()) {
                    counts.merge("lite.FAIL." + r[4], 1, Integer::sum);
                }
                counts.merge("verdict." + r[5], 1, Integer::sum);
            }
            counts.forEach((k, v) -> out.append(lane).append('\t').append(k).append('\t').append(v).append('\n'));
        });
        return out.toString();
    }

    /** legend-engine's refusals by database and signature (the E-R1 table, now a classification under E-R2): how many
     *  tests each refusal fails, and how many of them lite answers with PCT's expected value. */
    static String refusals(Map<String, List<String>> tables) {
        Map<String, int[]> bySignature = new TreeMap<>();
        tables.forEach((lane, lines) -> {
            String db = lane.substring(0, lane.indexOf('_'));
            for (String[] r : rows(lines)) {
                if ("REFUSES".equals(r[1])) {
                    int[] c = bySignature.computeIfAbsent(db + "\t" + r[2], k -> new int[3]);
                    c[0]++;
                    if ("PASS".equals(r[3])) {
                        c[1]++;
                    } else if ("FAIL".equals(r[3])) {
                        c[2]++;
                    }
                }
            }
        });
        StringBuilder out = new StringBuilder("# legend-engine's refusals (\"No SQL translation exists for the PURE"
                + " function\") by database and signature, from its manifests; lite's outcome on those tests as"
                + " legend-engine's audience (bazel run //pct:update_engine_lane)\n"
                + "database\tsignature\ttests\tlite_pass\tlite_fail\n");
        bySignature.forEach((k, c) -> out.append(k).append('\t').append(c[0]).append('\t').append(c[1]).append('\t')
                .append(c[2]).append('\n'));
        return out.toString();
    }

    /** A table's data rows: {test, engine, engine_class, lite, lite_class, verdict}. */
    private static List<String[]> rows(List<String> lines) {
        List<String[]> rows = new ArrayList<>();
        for (String line : lines) {
            if (line.isBlank() || line.startsWith("#") || line.equals(HEADER)) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 6) {
                throw new IllegalStateException("a table line of " + f.length + " fields: " + line);
            }
            rows.add(f);
        }
        return rows;
    }
}
