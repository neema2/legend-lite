// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The {@code #IQ{...}#} island over real-world SQL corpora (IQ island, 2026-10-15, the corpus lane's first phase):
 * coverage. Each query of each suite under {@code -Diq.corpus.root} (a directory of suites, each a {@code schema.yaml}
 * of the parity harness's subset and a {@code queries.jsonl} of {@code {"id", "sql", ...}}) is read as an island under
 * {@code -Diq.corpus.profile} (default {@code iq17-default}) and run over the suite's tables, empty, on DuckDB; each is
 * ANSWERED, REFUSED (by name), IQ_ERROR (the island says IQ raises an error: its syntax error, an error code it
 * names), DB_ERROR (the database's own error), CRASH (any other exception) or TIMEOUT. The report -- each status
 * counted, and each status's messages normalized and ranked by how many queries share them -- is written to
 * {@code -Diq.corpus.report} (run with {@code --strategy=TestRunner=local}). The corpora are not this repository's
 * (their licences and size): the lane runs only where the property names them, as the reference checkouts are read.
 *
 * <p>The second phase (IQ island, 2026-10-15): with {@code -Diq.corpus.answers} (the live IQ's answers over the suite's
 * data, loaded into IQ and into each lane's database) each query is judged as a parity run is, into
 * {@code <suite>.judged.tsv}; a FAIL whose rows are IQ's as a multiset is PASS_UNORDERED (a real-world ORDER BY ties).
 * IQ island, 2026-10-16: {@code -Diq.corpus.rows} writes lite's own rows of each FAIL beside it, into
 * {@code <suite>.rows.jsonl}.
 */
class IqCorpusTest {

    /** The platform's own engine: the island's session, its tables and the connection its model declares. */
    private static final TestBackend PLATFORM = TestBackends.platform();

    private enum Status { ANSWERED, REFUSED, IQ_ERROR, DB_ERROR, CRASH, TIMEOUT }

    private record Query(String id, String sql) {
    }

    private record Result(String id, Status status, String message) {
    }

    @Test
    void coverage() throws Exception {
        // its own manual target selects it (core/BUILD.bazel): without its input it fails naming it, never skipping
        // (SkipCensusTest's rule, Bazel workplan P3-14)
        String root = System.getProperty("iq.corpus.root");
        if (root == null) {
            throw new IllegalStateException("no -Diq.corpus.root: the corpus lane runs where it names its corpora"
                    + " (core/BUILD.bazel's iq_corpus_test says how)");
        }
        String report = System.getProperty("iq.corpus.report");
        String profile = System.getProperty("iq.corpus.profile", "iq17-default");
        String only = System.getProperty("iq.corpus.suites");
        long budget = Long.parseLong(System.getProperty("iq.corpus.timeout.ms", "20000"));
        // IQ island, 2026-10-15 (the lane's second phase): the live IQ's answers to the queries over the suite's rows
        // (tools/iq-oracle's answers-file shape), each run judged by the parity harness's own comparison
        String answersFile = System.getProperty("iq.corpus.answers");
        Map<String, com.legend.testcases.IqAnswers.Snap> answers = answersFile == null ? null
                : com.legend.testcases.IqAnswers.read(Path.of(answersFile));
        List<Path> suites;
        try (var dirs = Files.list(Path.of(root))) {
            suites = dirs.filter(d -> Files.exists(d.resolve("queries.jsonl"))).sorted().toList();
        }
        StringBuilder summary = new StringBuilder("# The #IQ{...}# island over real-world SQL: coverage (profile "
                + profile + ")\n\n");
        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "iq-corpus");
            t.setDaemon(true);
            return t;
        });
        for (Path suite : suites) {
            String name = suite.getFileName().toString();
            if (only != null && !Set.of(only.split(",")).contains(name)) {
                continue;
            }
            List<SqlParity.Table> tables = IqParity.schema(suite.resolve("schema.yaml"));
            Set<String> names = IqParity.tableNames(tables);
            String model = IqParity.model(tables, PLATFORM.connectionBody());
            List<Query> queries = read(suite.resolve("queries.jsonl"));
            List<Result> results = new ArrayList<>();
            List<String[]> verdicts = new ArrayList<>();
            StringBuilder rowsOut = System.getProperty("iq.corpus.rows") != null ? new StringBuilder() : null;
            try (Connection session = PLATFORM.open()) {
                IqParity.seed(PLATFORM, session, tables);
                for (Query q : queries) {
                    IqParity.Case c = new IqParity.Case("corpus/" + name, q.id(), q.sql(), List.of(profile), null,
                            null, null, null, null, null, null, null);
                    IqParity.Run parityRun = IqParity.runs(List.of(c)).get(0);
                    String island = IqParity.island(parityRun, names);
                    if (System.getProperty("iq.corpus.debug") != null) {
                        // the query's plan's SQL, for reading a judged run's difference
                        try {
                            System.out.println("DEBUG " + q.id() + "\n" + com.legend.Compiler.query(
                                    com.legend.Compiler.compileModel(model), island).plan(IqParity.RUNTIME).sql());
                        } catch (RuntimeException e) {
                            System.out.println("DEBUG " + q.id() + " (no plan): " + oneLine(e));
                        }
                    }
                    if (answers != null) {
                        com.legend.testcases.IqAnswers.Snap snap = answers.get(parityRun.key());
                        if (snap == null) {
                            continue;
                        }
                        Future<SqlParity.Outcome> judged = pool.submit(() -> IqParity.outcome(parityRun, snap,
                                tables, names, is -> Lean.run(model, is, IqParity.RUNTIME, session)));
                        String verdict;
                        String detail;
                        try {
                            SqlParity.Outcome o = judged.get(budget, TimeUnit.MILLISECONDS);
                            verdict = o.status().name();
                            detail = o.detail().replaceAll("\\s+", " ").strip();
                            if (o.status() == SqlParity.Status.FAIL && !detail.contains("Column")
                                    && sameRowsUnordered(parityRun, snap, model, island, session)) {
                                // a real-world query's ORDER BY may tie: the rows are IQ's, in another order
                                verdict = "PASS_UNORDERED";
                            }
                        } catch (TimeoutException e) {
                            judged.cancel(true);
                            verdict = "TIMEOUT";
                            detail = "over " + budget + " ms";
                        } catch (java.util.concurrent.ExecutionException e) {
                            Throwable t = e.getCause() == null ? e : e.getCause();
                            verdict = "CRASH";
                            detail = oneLine(t);
                        }
                        verdicts.add(new String[] {q.id(), verdict, detail});
                        if (rowsOut != null && verdict.equals("FAIL")) {
                            // IQ island, 2026-10-16: lite's own rows, for reading a FAIL beside the live IQ's
                            try {
                                SqlParity.Grid g = SqlParity.grid(Lean.run(model, island, IqParity.RUNTIME,
                                        session));
                                rowsOut.append("{\"id\":").append(json(q.id())).append(",\"columns\":[");
                                rowsOut.append(String.join(",", g.columns().stream().map(IqCorpusTest::json)
                                        .toList())).append("],\"rows\":[");
                                for (int i = 0; i < g.rows().size() && i < 2000; i++) {
                                    rowsOut.append(i == 0 ? "[" : ",[").append(String.join(",", g.rows().get(i)
                                            .stream().map(cell -> cell == null ? "null" : json(String.valueOf(cell))).toList()))
                                            .append(']');
                                }
                                rowsOut.append("]}\n");
                            } catch (RuntimeException e) {
                                rowsOut.append("{\"id\":").append(json(q.id())).append(",\"error\":")
                                        .append(json(oneLine(e))).append("}\n");
                            }
                        }
                        continue;
                    }
                    Future<Status> run = pool.submit(() -> {
                        Lean.run(model, island, IqParity.RUNTIME, session);
                        return Status.ANSWERED;
                    });
                    Result r;
                    try {
                        run.get(budget, TimeUnit.MILLISECONDS);
                        r = new Result(q.id(), Status.ANSWERED, "");
                    } catch (TimeoutException e) {
                        run.cancel(true);
                        r = new Result(q.id(), Status.TIMEOUT, "over " + budget + " ms");
                    } catch (java.util.concurrent.ExecutionException e) {
                        Throwable t = e.getCause() == null ? e : e.getCause();
                        r = new Result(q.id(), classify(t), oneLine(t));
                    }
                    results.add(r);
                }
            }
            if (answers != null) {
                summary.append(judgedSection(name, verdicts));
                if (rowsOut != null && report != null) {
                    Files.createDirectories(Path.of(report));
                    Files.writeString(Path.of(report).resolve(name + ".rows.jsonl"), rowsOut, StandardCharsets.UTF_8);
                }
                if (report != null) {
                    StringBuilder lines = new StringBuilder();
                    for (String[] v : verdicts) {
                        lines.append(v[0]).append('\t').append(v[1]).append('\t').append(v[2].replace('\t', ' '))
                                .append('\n');
                    }
                    Files.createDirectories(Path.of(report));
                    Files.writeString(Path.of(report).resolve(name + ".judged.tsv"), lines, StandardCharsets.UTF_8);
                }
                continue;
            }
            summary.append(section(name, results));
            if (report != null) {
                Path dir = Path.of(report);
                Files.createDirectories(dir);
                StringBuilder lines = new StringBuilder();
                for (Result r : results) {
                    lines.append(r.id()).append('\t').append(r.status()).append('\t').append(r.message().replace('\t',
                            ' ')).append('\n');
                }
                Files.writeString(dir.resolve(name + ".tsv"), lines, StandardCharsets.UTF_8);
            }
        }
        pool.shutdownNow();
        if (report != null) {
            Files.writeString(Path.of(report).resolve("coverage.md"), summary, StandardCharsets.UTF_8);
        }
        System.out.println(summary);
    }

    /** {@code s} as a JSON string. */
    private static String json(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.append('"').toString();
    }

    /** Whether lite's rows are the live IQ's as a multiset, each cell compared by IQ's type
     *  ({@link IqParity#cellEquals}): a judged FAIL that only orders tied rows otherwise. */
    private static boolean sameRowsUnordered(IqParity.Run run, com.legend.testcases.IqAnswers.Snap snap,
            String model, String island, Connection session) {
        try {
            if (!(com.legend.testcases.IqAnswers.expected(run, snap) instanceof SqlParity.Rows rows)) {
                return false;
            }
            SqlParity.Grid expected = rows.grid();
            SqlParity.Grid actual = SqlParity.grid(Lean.run(model, island, IqParity.RUNTIME, session));
            if (expected.rows().size() != actual.rows().size() || expected.rows().size() > 5000) {
                return false;
            }
            List<List<Object>> left = new ArrayList<>(actual.rows());
            for (List<Object> e : expected.rows()) {
                int found = -1;
                for (int i = 0; i < left.size() && found < 0; i++) {
                    boolean same = true;
                    for (int c = 0; c < e.size() && same; c++) {
                        same = IqParity.cellEquals(e.get(c), left.get(i).get(c), snap.types().get(c));
                    }
                    found = same ? i : -1;
                }
                if (found < 0) {
                    return false;
                }
                left.remove(found);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Each line of a {@code queries.jsonl}: its id and its SQL (the lane's own reading of the two string fields). */
    private static List<Query> read(Path file) throws IOException {
        List<Query> out = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            com.legend.json.Json.Obj o = (com.legend.json.Json.Obj) com.legend.json.Json.parse(line);
            out.add(new Query(((com.legend.json.Json.Str) o.get("id")).value(),
                    ((com.legend.json.Json.Str) o.get("sql")).value()));
        }
        return out;
    }

    /** The parity harness's classes of an island's exception ({@code IqParity.judged}); a refusal that names IQ's own
     *  error ("... is IQ's error -1001030 under its engine ..., refused before translating", or IQ island, 2026-10-16,
     *  one that ends in its code: "... (IQ: SQLCODE -938, ...)") is IQ_ERROR. */
    private static Status classify(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t.getMessage() != null && (t.getMessage().contains("refused before translating")
                    && t.getMessage().contains("is IQ's error -") || t.getMessage().contains("(IQ: SQLCODE -"))) {
                return Status.IQ_ERROR;
            }
            if (t instanceof com.legend.error.NotImplementedException) {
                return Status.REFUSED;
            }
            if (t instanceof com.legend.protocol.sql.SqlTranslationException s) {
                return s.kind() == com.legend.protocol.sql.SqlTranslationException.Kind.UNSUPPORTED ? Status.REFUSED
                        : Status.IQ_ERROR;
            }
            if (t instanceof com.legend.error.IslandSyntaxException) {
                return Status.IQ_ERROR;
            }
            if (t.getClass().getSimpleName().equals("TypeInferenceException") && t.getMessage() != null
                    && t.getMessage().startsWith("#IQ")) {
                return Status.IQ_ERROR;
            }
            if (t instanceof com.legend.error.DataError) {
                return t.getMessage() != null && t.getMessage().contains(
                        com.legend.protocol.sql.iq.IqDialect.REFUSED_AT_RUN_TIME) ? Status.REFUSED : Status.DB_ERROR;
            }
        }
        return Status.CRASH;
    }

    private static String oneLine(Throwable t) {
        String m = t.getClass().getSimpleName() + ": " + t.getMessage();
        return m.replaceAll("\\s+", " ").strip();
    }

    private static final Pattern PLACE = Pattern.compile("#IQ(:[\\w-]+)?\\{\\.\\.\\.\\}# at line \\d+, column \\d+: ");
    private static final Pattern QUOTED = Pattern.compile("'[^']*'|\"[^\"]*\"|`[^`]*`");
    private static final Pattern NUMBER = Pattern.compile("\\b\\d+\\b");

    /** A message as its kind: its place, quoted names and values, and numbers written alike. */
    static String kind(String message) {
        String m = PLACE.matcher(message).replaceAll("");
        m = QUOTED.matcher(m).replaceAll("'…'");
        m = NUMBER.matcher(m).replaceAll("N");
        return m.length() > 220 ? m.substring(0, 220) + "…" : m;
    }

    /** The judged runs of a suite: each verdict counted, and its details' kinds ranked ({@link #kind}). */
    private static String judgedSection(String name, List<String[]> verdicts) {
        Map<String, Integer> counts = new TreeMap<>();
        Map<String, Map<String, List<String>>> kinds = new TreeMap<>();
        for (String[] v : verdicts) {
            counts.merge(v[1], 1, Integer::sum);
            if (!v[1].equals("PASS")) {
                kinds.computeIfAbsent(v[1], k -> new LinkedHashMap<>()).computeIfAbsent(kind(v[2]), k -> new ArrayList<>())
                        .add(v[0]);
            }
        }
        StringBuilder b = new StringBuilder("## " + name + " judged by the live IQ (" + verdicts.size() + " runs)\n\n"
                + "| verdict | runs | share |\n|---|---|---|\n");
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            b.append("| ").append(e.getKey()).append(" | ").append(e.getValue()).append(" | ").append(String.format(
                    Locale.ROOT, "%.1f%%", 100.0 * e.getValue() / Math.max(1, verdicts.size()))).append(" |\n");
        }
        for (Map.Entry<String, Map<String, List<String>>> e : kinds.entrySet()) {
            b.append("\n### ").append(e.getKey()).append(", by detail\n\n| runs | detail | e.g. |\n|---|---|---|\n");
            e.getValue().entrySet().stream().sorted(Comparator.comparing((Map.Entry<String, List<String>> k) -> -k
                    .getValue().size())).limit(80).forEach(k -> b.append("| ").append(k.getValue().size()).append(" | ")
                    .append(k.getKey().replace("|", "\\|")).append(" | ").append(k.getValue().get(0)).append(" |\n"));
        }
        return b.append('\n').toString();
    }

    private static String section(String name, List<Result> results) {
        Map<Status, Integer> counts = new TreeMap<>();
        Map<Status, Map<String, List<String>>> kinds = new TreeMap<>();
        for (Result r : results) {
            counts.merge(r.status(), 1, Integer::sum);
            if (r.status() != Status.ANSWERED) {
                kinds.computeIfAbsent(r.status(), s -> new LinkedHashMap<>()).computeIfAbsent(kind(r.message()),
                        k -> new ArrayList<>()).add(r.id());
            }
        }
        int total = results.size();
        StringBuilder b = new StringBuilder("## " + name + " (" + total + " queries)\n\n| status | queries | share |\n"
                + "|---|---|---|\n");
        for (Map.Entry<Status, Integer> e : counts.entrySet()) {
            b.append("| ").append(e.getKey()).append(" | ").append(e.getValue()).append(" | ").append(String.format(
                    Locale.ROOT, "%.1f%%", 100.0 * e.getValue() / Math.max(1, total))).append(" |\n");
        }
        for (Map.Entry<Status, Map<String, List<String>>> e : kinds.entrySet()) {
            b.append("\n### ").append(e.getKey()).append(", by message\n\n| queries | message | e.g. |\n|---|---|---|\n");
            e.getValue().entrySet().stream().sorted(Comparator.comparing((Map.Entry<String, List<String>> k) -> -k
                    .getValue().size())).limit(60).forEach(k -> b.append("| ").append(k.getValue().size()).append(" | ")
                    .append(k.getKey().replace("|", "\\|")).append(" | ").append(k.getValue().get(0)).append(" |\n"));
        }
        return b.append('\n').toString();
    }
}
