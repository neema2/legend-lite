// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle.enginelane;

import com.legend.json.Json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE ORACLE'S BATTERIES (H7; CP-9 of docs/records/compat-program-plan-2026-11-19.md, B1-R's design): a battery's
 * requests answered by legend-engine (the heavy half: the oracle re-asked, its answers pinned) and by lite as
 * legend-engine's audience (the per-push half: no legend-engine JVM, compared with the pinned answers).
 *
 * <p>An answer, either side, is one line: {@code <name> TAB <kind> TAB <payload>}:
 * <ul>
 *   <li>{@code ROWS}, the TDS answer's rows as compact JSON (the values of each row, in the answer's order: every
 *       battery request sorts);</li>
 *   <li>{@code REFUSES}, legend-engine's "No SQL translation exists" refusal, its payload the signature;</li>
 *   <li>{@code ERROR}, any other failure, its payload a class ({@link EngineLaneTable#defect}), never words.</li>
 * </ul>
 * The comparison is by kind and rows: {@code AGREE} (the same rows), {@code BOTH_FAIL} (neither answers; the classes
 * are listed, not compared: the words differ by design), or {@code DIFFER}.
 *
 * <p>Usage: {@code engine-answers --in=<GoldenRunMain jsonl> --out=<tsv>}; {@code compare --db= --engine=<tsv>
 * --lite=<tsv> --out=<tsv> [--engine-digest=<txt> --lite-digest=<txt>]}; {@code digest --duckdb=<file> --out=<txt>}
 * (lite's DuckDB driver, the rows the file holds).
 */
public final class Batteries {

    private static final Pattern NO_SQL = Pattern.compile("No SQL translation exists for the PURE function '([^']+)'");

    private Batteries() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--") || !a.contains("=")) {
                throw new IllegalArgumentException("--key=value, not " + a);
            }
            o.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
        }
        switch (args.length == 0 ? "" : args[0]) {
            case "engine-answers" -> write(o.get("out"), engineAnswers(read(o.get("in"))));
            case "compare" -> {
                if (o.containsKey("engine-digest") || o.containsKey("lite-digest")) {
                    String e = Files.readString(Path.of(o.get("engine-digest"))).strip();
                    String l = Files.readString(Path.of(o.get("lite-digest"))).strip();
                    if (!e.equals(l)) {
                        throw new IllegalStateException("the two sides answer from different rows: legend-engine's"
                                + " DuckDB file holds [" + e + "], lite's [" + l + "]");
                    }
                }
                write(o.get("out"), compare(o.get("db"), read(o.get("engine")), read(o.get("lite"))));
            }
            case "digest" -> write(o.get("out"), digestOf(o.get("duckdb")) + "\n");
            default -> throw new IllegalArgumentException("engine-answers, compare or digest");
        }
    }

    private static List<String> read(String path) throws IOException {
        return Files.readAllLines(Path.of(path), StandardCharsets.UTF_8);
    }

    private static void write(String path, String text) throws IOException {
        Files.writeString(Path.of(path), text, StandardCharsets.UTF_8);
    }

    /** A pure/v1 request's {@code model}: the model's text ({@code PureModelContextText}), as compact JSON. */
    public static String textModel(String code) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("_type", "text");
        m.put("code", code);
        return Json.toCompact(m);
    }

    /** One answer line from a pure/v1 answer's status and JSON body. */
    public static String answer(String name, int status, String body) {
        String kind;
        String payload;
        Json.Obj o;
        try {
            o = Json.parseObject(body);
        } catch (RuntimeException notJson) {
            return name + "\tERROR\t" + EngineLaneTable.defect(body);
        }
        if (status == 200 && o.getOr("result", null) instanceof Json.Obj result) {
            StringBuilder rows = new StringBuilder("[");
            for (Json.Node r : result.getArr("rows").items()) {
                if (rows.length() > 1) {
                    rows.append(',');
                }
                rows.append(Json.toCompact(((Json.Obj) r).get("values")));
            }
            kind = "ROWS";
            payload = rows.append(']').toString();
        } else {
            String message = String.valueOf(o.getStringOr("message", body));
            Matcher m = NO_SQL.matcher(message);
            if (m.find()) {
                kind = "REFUSES";
                payload = m.group(1);
            } else {
                kind = "ERROR";
                payload = EngineLaneTable.defect(message);
            }
        }
        return name + "\t" + kind + "\t" + payload;
    }

    /** legend-engine's answers from the oracle's lines ({@code [name, status, body]}; its plans left out). */
    static String engineAnswers(List<String> jsonl) {
        StringBuilder out = new StringBuilder("# legend-engine's answers to the battery (the oracle,"
                + " bazel run //tools/engine-runner:update_batteries)\n");
        for (String line : jsonl) {
            if (line.isBlank() || line.startsWith("#")) {
                if (line.startsWith("#")) {
                    out.append(line).append('\n');
                }
                continue;
            }
            List<Json.Node> f = ((Json.Arr) Json.parse(line)).items();
            String name = ((Json.Str) f.get(0)).value();
            if ("#".equals(name)) {
                // the oracle's header: the versions it ran
                out.append("# ").append(((Json.Str) f.get(2)).value()).append('\n');
                continue;
            }
            if (name.startsWith("plan.")) {
                continue;
            }
            Json.Node s = f.get(1);
            int status = s instanceof Json.Num n ? (int) n.longValue() : Integer.parseInt(((Json.Str) s).value());
            out.append(answer(name, status, ((Json.Str) f.get(2)).value())).append('\n');
        }
        return out.toString();
    }

    /** Each request's agreement: the two sides' kinds, and the verdict. */
    static String compare(String db, List<String> engine, List<String> lite) {
        Map<String, String[]> e = answers(engine);
        Map<String, String[]> l = answers(lite);
        Map<String, Integer> counts = new TreeMap<>();
        StringBuilder rows = new StringBuilder();
        for (Map.Entry<String, String[]> a : e.entrySet()) {
            String[] mine = l.get(a.getKey());
            if (mine == null) {
                throw new IllegalStateException("lite answered no '" + a.getKey() + "'");
            }
            String verdict;
            if ("ROWS".equals(a.getValue()[0]) && "ROWS".equals(mine[0])) {
                verdict = a.getValue()[1].equals(mine[1]) ? "AGREE" : "DIFFER";
            } else if (!"ROWS".equals(a.getValue()[0]) && !"ROWS".equals(mine[0])) {
                verdict = "BOTH_FAIL";
            } else {
                verdict = "DIFFER";
            }
            counts.merge(verdict, 1, Integer::sum);
            // where the two agree, each side's kind; where they differ, each side's answer too (rows or class)
            boolean shown = !"AGREE".equals(verdict);
            rows.append(a.getKey()).append('\t').append(a.getValue()[0]).append(':')
                    .append(shown || !"ROWS".equals(a.getValue()[0]) ? a.getValue()[1] : "").append('\t')
                    .append(mine[0]).append(':').append(shown || !"ROWS".equals(mine[0]) ? mine[1] : "").append('\t')
                    .append(verdict).append('\n');
        }
        if (l.size() != e.size()) {
            throw new IllegalStateException("lite answered " + l.size() + " requests, legend-engine " + e.size());
        }
        StringBuilder out = new StringBuilder("# the battery on " + db + ", lite as legend-engine's audience against"
                + " legend-engine's pinned answers (bazel run //pct:update_batteries): " + counts + "\n"
                + "request\tlegend-engine\tlite\tverdict\n");
        return out.append(rows).toString();
    }

    private static Map<String, String[]> answers(List<String> lines) {
        Map<String, String[]> m = new TreeMap<>();
        for (String line : lines) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\t", 3);
            m.put(f[0], new String[] {f[1], f.length > 2 ? f[2] : ""});
        }
        return m;
    }

    /** The rows a trades DuckDB file holds, as one line: their count, their qty's sum and the md5 of every row's text in
     *  a fixed order (DuckDB computes it). */
    public static String digestOf(String file) throws Exception {
        // read-only: a build input cannot be written, and the digest writes nothing
        java.util.Properties readOnly = new java.util.Properties();
        readOnly.setProperty("duckdb.read_only", "true");
        try (java.sql.Connection c = java.sql.DriverManager.getConnection("jdbc:duckdb:" + file, readOnly);
                java.sql.Statement s = c.createStatement();
                java.sql.ResultSet r = s.executeQuery("SELECT count(*), sum(qty), md5(string_agg(concat_ws('|', region,"
                        + " desk, book, year, qtr, notional, pnl, qty), ';' ORDER BY region, desk, book, year, qtr))"
                        + " FROM TRADES_SCHEMA.TRADES")) {
            r.next();
            return "rows " + r.getLong(1) + ", sum(qty) " + r.getLong(2) + ", md5 " + r.getString(3);
        }
    }
}
