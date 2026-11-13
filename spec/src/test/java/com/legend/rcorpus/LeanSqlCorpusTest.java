// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.leansql.LeanSqlBaseline;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE CORPUS'S PRODUCT-SQL SIZE BASELINE (G3's guard, the corpus set; E3 of the total-wall program, ruled by the
 * orchestrator 2026-11-12, docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §6): every discovered corpus body planned whole
 * ({@code TestExecution.planBody}, planning only, no database) on the lane's backend, and its PRODUCT statements -- the
 * body's own statement nodes: its results, lets' frame runs, writes -- held to {@code rcorpus/lean-sql-baseline.tsv}
 * per database and test package by {@link LeanSqlBaseline}'s rule (growth fails, a shrink past 1% re-pins). Excluded,
 * as ruled: the verdict and canon steps (the judge's: recorded unguarded in {@code judge-sql.tsv} among the test's
 * outputs, for the test road's speed work) and a fixture's setup, seeds and loads (data delivery). Its own targets,
 * {@code //spec:lean_sql_corpus_duckdb} and {@code _h2}; re-pin with {@code bazel run
 * //spec:update_lean_sql_corpus_<lane>}.
 */
@Tag("heavy")
class LeanSqlCorpusTest {

    /** The pins' file in the source tree. */
    static final String PINS = "spec/src/test/resources/rcorpus/lean-sql-baseline.tsv";

    /** The corpus's bodies planned on this JVM's backend: [0] the product's measures, [1] the judge's. */
    @SuppressWarnings("unchecked")
    static Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure>[] measure() throws IOException {
        MinimalCorpus corpus = new MinimalCorpus();
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> product = new java.util.HashMap<>();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> judge = new java.util.HashMap<>();
        com.legend.ExecuteOptions options = com.legend.ExecuteOptions.NONE
                .withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE);
        for (com.legend.test.PureTests.TestCase t : corpus.tests()) {
            String family = t.pkg();
            List<String> productSql = new ArrayList<>();
            List<ExecutionPlan.Node> productNodes = new ArrayList<>();
            List<String> judgeSql = new ArrayList<>();
            try {
                var resolved = Compiler.resolveQuery(List.copyOf(t.fn().body()), t.imports(), corpus.context());
                TestPlan plan = com.legend.TestExecution.planBody(Compiler.queryResolved(corpus.context(), resolved),
                        MinimalCorpus.RUNTIME, options);
                steps(plan.steps(), productSql, productNodes, judgeSql);
            } catch (RuntimeException | StackOverflowError e) {
                family = "refused";
            }
            long chars = productSql.stream().mapToLong(String::length).sum();
            long planChars = 0;
            for (ExecutionPlan.Node n : productNodes) {
                planChars += PlanJson.write(ExecutionPlan.reading(List.of(), n)).length();
            }
            add(product, new LeanSqlBaseline.Key("corpus", database, family), productSql.size(), chars, planChars,
                    t.fqn());
            add(judge, new LeanSqlBaseline.Key("corpus-judge", database, family), judgeSql.size(),
                    judgeSql.stream().mapToLong(String::length).sum(), 0, t.fqn());
        }
        return new Map[] {product, judge};
    }

    private static void add(Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> into, LeanSqlBaseline.Key k, long s,
            long c, long p, String test) {
        LeanSqlBaseline.Measure was = into.get(k);
        into.put(k, was == null ? new LeanSqlBaseline.Measure(s, c, p, test)
                : new LeanSqlBaseline.Measure(was.statements() + s, was.chars() + c, was.plan() + p, was.worst()));
    }

    /** The body's own statement nodes to the product's; the verdict steps' statements to the judge's. */
    private static void steps(List<TestPlan.TestStep> steps, List<String> productSql,
            List<ExecutionPlan.Node> productNodes, List<String> judgeSql) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Run r -> {
                    if (!(r.node() instanceof ExecutionPlan.Load)) {
                        List<String> statements = new ArrayList<>();
                        LeanSqlBaseline.statements(r.node(), statements);
                        productSql.addAll(statements);
                        productNodes.add(r.node());
                    }
                }
                case TestPlan.FrameRun f -> {
                    productSql.add(f.check().sql().statement());
                    productNodes.add(f.check());
                }
                case TestPlan.Verdicts v -> judgeSql.addAll(verdictStatements(v));
                case TestPlan.Branch b -> {
                    steps(b.then(), productSql, productNodes, judgeSql);
                    steps(b.otherwise(), productSql, productNodes, judgeSql);
                }
            }
        }
    }

    /** A verdict group's statements: its fused statement, each verdict's sides (the judge's SQL). */
    private static List<String> verdictStatements(TestPlan.Verdicts v) {
        List<String> out = new ArrayList<>();
        if (v.fused() != null) {
            out.add(v.fused().sql().statement());
        }
        for (TestPlan.Verdict verdict : v.verdicts()) {
            for (TestPlan.Side side : verdict.base()) {
                switch (side) {
                    case TestPlan.Side.Rows rows -> out.add(rows.result().sql().statement());
                    case TestPlan.Side.KeyedRows keyed -> {
                        out.add(keyed.keyed().keys().statement());
                        out.add(keyed.keyed().body().statement());
                    }
                    case TestPlan.Side.Constant c -> {
                    }
                    case TestPlan.Side.FusedCell f -> {
                    }
                }
            }
        }
        return out;
    }

    @Test
    void theCorpusProductSqlIsHeldToItsBaseline() throws Exception {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure>[] measured = measure();
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned = new java.util.HashMap<>();
        try (var in = LeanSqlCorpusTest.class.getResourceAsStream("/rcorpus/lean-sql-baseline.tsv")) {
            if (in != null) {
                LeanSqlBaseline.read(new String(in.readAllBytes(), StandardCharsets.UTF_8)).forEach((k, m) -> {
                    if (k.database().equals(database)) {
                        pinned.put(k, m);
                    }
                });
            }
        }
        // the judge's statements, unguarded: a measurement for the test road's speed work
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "judge-sql.tsv"), LeanSqlBaseline.write(measured[1]),
                    StandardCharsets.UTF_8);
        }
        List<String> drift = LeanSqlBaseline.drift(pinned, measured[0]);
        assertTrue(drift.isEmpty(), "the corpus's product SQL left its baseline (" + PINS + "):\n  "
                + String.join("\n  ", drift));
    }

    /** E-5c's step 1 (O5, ruled 2026-11-13; a measurement, nothing pinned): every corpus body planned under the
     *  DATABASE judge and under BOTH. Under BOTH, the host's base sides by class -- a no-FROM single value (a fused
     *  column's candidate), rows over a FROM, keyed rows, a planning-time constant. Under DATABASE, the verdicts with no
     *  fused row (a keyed member, the split rung, a verdict decided at planning) and with one. And the bodies BOTH
     *  plans that DATABASE refuses (under E-5c each becomes a whole-body refusal, the map's risk 5). */
    static void e5c() throws IOException {
        MinimalCorpus corpus = new MinimalCorpus();
        long[] host = new long[4];
        long[] database = new long[2];
        long bothOnly = 0;
        long bodies = 0;
        List<String> bothOnlyNames = new ArrayList<>();
        // Q-E5c-1's categories (the user's question, 2026-11-13): (A) decided at planning, no row; (B) a keyed member,
        // its own wrapper; (C) the split rung, its own wrapper -- each count with a first example
        long[] categories = new long[4];
        String[] examples = new String[4];
        for (com.legend.test.PureTests.TestCase t : corpus.tests()) {
            bodies++;
            TestPlan both = plan(corpus, t, com.legend.testplan.PlanningOptions.JudgeMode.BOTH);
            TestPlan db = plan(corpus, t, com.legend.testplan.PlanningOptions.JudgeMode.DATABASE);
            if (db != null) {
                categorize(db.steps(), t.fqn(), categories, examples);
            }
            if (both != null) {
                walkHost(both.steps(), host);
            }
            if (db != null) {
                walkDatabase(db.steps(), database);
            }
            if (both != null && db == null) {
                bothOnly++;
                if (bothOnlyNames.size() < 15) {
                    bothOnlyNames.add(t.fqn() + " -- " + refusal(corpus, t));
                }
            }
        }
        System.out.println("e5c " + MinimalCorpus.BACKEND.type().name() + ": bodies " + bodies);
        System.out.println("e5c host sides under BOTH: single value, no FROM " + host[0] + "; rows over a FROM "
                + host[1] + "; keyed rows " + host[2] + "; constants " + host[3]);
        System.out.println("e5c verdicts under DATABASE: in a fused group " + database[0] + "; no fused row "
                + database[1]);
        System.out.println("e5c bodies BOTH plans and DATABASE refuses: " + bothOnly);
        bothOnlyNames.forEach(n -> System.out.println("e5c   " + n));
        String[] what = {"(A) decided at planning, no row", "(B) keyed, its own wrapper", "(C) split rung, its own wrapper",
            "(D) no wrapper (an assertError's or host step)"};
        for (int k = 0; k < 4; k++) {
            System.out.println("e5c " + what[k] + ": " + categories[k] + (examples[k] == null ? "" : "; e.g. " + examples[k]));
        }
    }

    /** The DATABASE judge's refusal of {@code t}'s body, its first line (Q-E5c-2's why). */
    private static String refusal(MinimalCorpus corpus, com.legend.test.PureTests.TestCase t) {
        // the judge-declines diagnostic names the body it declines in ([judge-decline] ... <test> ...)
        System.setProperty("legend.test.current", "REFUSED:" + t.fqn());
        try {
            var resolved = Compiler.resolveQuery(List.copyOf(t.fn().body()), t.imports(), corpus.context());
            com.legend.TestExecution.planBody(Compiler.queryResolved(corpus.context(), resolved), MinimalCorpus.RUNTIME,
                    com.legend.ExecuteOptions.NONE.withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE));
            return "planned";
        } catch (RuntimeException | StackOverflowError e) {
            String m = String.valueOf(e.getMessage());
            return e.getClass().getSimpleName() + ": " + m.lines().findFirst().orElse("").substring(0,
                    Math.min(300, m.lines().findFirst().orElse("").length()));
        }
    }

    /** The DATABASE plan's verdicts with no fused row, by Q-E5c-1's category: (A) a root's step decided at planning or
     *  a row refused at planning (no verdict); (B) a verdict of a group with no fused statement whose wrapper is keyed;
     *  (C) one whose wrapper is a typed result (the split rung). */
    private static void categorize(List<TestPlan.TestStep> steps, String test, long[] c, String[] ex) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Verdicts v -> {
                    if (v.fused() == null) {
                        for (TestPlan.Verdict verdict : v.verdicts()) {
                            int k = verdict.wrapper() instanceof com.legend.executionplan.ExecutionPlan.Keyed ? 1 : 2;
                            if (verdict.wrapper() == null) {
                                k = 3;
                            }
                            c[k]++;
                            if (ex[k] == null) {
                                ex[k] = test + " / " + verdict.name();
                            }
                        }
                    }
                    if (v.roots() != null) {
                        for (com.legend.testexecutionplan.RootLoop.Root r : v.roots().roots()) {
                            for (com.legend.testexecutionplan.RootLoop.Step s : r.steps()) {
                                boolean decided = s instanceof com.legend.testexecutionplan.RootLoop.Decided
                                        || (s instanceof com.legend.testexecutionplan.RootLoop.Row row
                                                && row.verdict() == com.legend.testexecutionplan.RootLoop.NO_VERDICT);
                                if (decided) {
                                    c[0]++;
                                    if (ex[0] == null) {
                                        ex[0] = test + " / " + r.listenerName() + " -- " + describe(s);
                                    }
                                }
                            }
                        }
                    }
                }
                case TestPlan.Branch b -> {
                    categorize(b.then(), test, c, ex);
                    categorize(b.otherwise(), test, c, ex);
                }
                case TestPlan.Run r -> {
                }
                case TestPlan.FrameRun f -> {
                }
            }
        }
    }

    private static String describe(com.legend.testexecutionplan.RootLoop.Step s) {
        com.legend.testexecutionplan.Failure f = switch (s) {
            case com.legend.testexecutionplan.RootLoop.Decided d -> d.failure();
            case com.legend.testexecutionplan.RootLoop.Row row -> row.refusal();
        };
        if (f == null) {
            return "refused at planning";
        }
        String m = String.valueOf(f.message());
        return f.kind() + ": " + m.substring(0, Math.min(160, m.length())).replace('\n', ' ');
    }

    private static @com.legend.base.Nullable TestPlan plan(MinimalCorpus corpus, com.legend.test.PureTests.TestCase t,
            com.legend.testplan.PlanningOptions.JudgeMode mode) {
        try {
            var resolved = Compiler.resolveQuery(List.copyOf(t.fn().body()), t.imports(), corpus.context());
            return com.legend.TestExecution.planBody(Compiler.queryResolved(corpus.context(), resolved),
                    MinimalCorpus.RUNTIME, com.legend.ExecuteOptions.NONE.withJudgeMode(mode));
        } catch (RuntimeException | StackOverflowError e) {
            return null;
        }
    }

    private static void walkHost(List<TestPlan.TestStep> steps, long[] host) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Verdicts v -> {
                    for (TestPlan.Verdict verdict : v.verdicts()) {
                        if (verdict.host() == null) {
                            continue;
                        }
                        for (TestPlan.Side side : verdict.base()) {
                            switch (side) {
                                case TestPlan.Side.Rows r -> host[noFrom(r.result().sql().statement()) ? 0 : 1]++;
                                case TestPlan.Side.KeyedRows k -> host[2]++;
                                case TestPlan.Side.Constant c -> host[3]++;
                                case TestPlan.Side.FusedCell f -> host[0]++;
                            }
                        }
                    }
                }
                case TestPlan.Branch b -> {
                    walkHost(b.then(), host);
                    walkHost(b.otherwise(), host);
                }
                case TestPlan.Run r -> {
                }
                case TestPlan.FrameRun f -> {
                }
            }
        }
    }

    private static void walkDatabase(List<TestPlan.TestStep> steps, long[] database) {
        for (TestPlan.TestStep step : steps) {
            switch (step) {
                case TestPlan.Verdicts v -> database[v.fused() != null ? 0 : 1] += v.verdicts().size();
                case TestPlan.Branch b -> {
                    walkDatabase(b.then(), database);
                    walkDatabase(b.otherwise(), database);
                }
                case TestPlan.Run r -> {
                }
                case TestPlan.FrameRun f -> {
                }
            }
        }
    }

    /** A statement with no FROM at its top level (a measurement's reading of the text: a select of one value). */
    private static boolean noFrom(String sql) {
        String upper = sql.toUpperCase(java.util.Locale.ROOT);
        return !upper.contains(" FROM ") && !upper.contains("\nFROM ") && !upper.startsWith("WITH");
    }

    /** {@code bazel run //spec:update_lean_sql_corpus_<lane>}: this backend's rows measured again and written over
     *  the source tree's pins, the other backends' rows kept ({@code -- e5c}: E-5c's measurement printed instead). */
    public static void main(String[] args) throws IOException {
        if (args.length == 1 && args[0].equals("e5c")) {
            e5c();
            return;
        }
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspace == null) {
            throw new IllegalStateException("run it with bazel run //spec:update_lean_sql_corpus_<lane>");
        }
        Path file = Path.of(workspace, PINS);
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pins = new java.util.HashMap<>();
        if (Files.exists(file)) {
            LeanSqlBaseline.read(Files.readString(file, StandardCharsets.UTF_8)).forEach((k, m) -> {
                if (!k.database().equals(database)) {
                    pins.put(k, m);
                }
            });
        }
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure>[] measured = measure();
        pins.putAll(measured[0]);
        Files.writeString(file, LeanSqlBaseline.write(pins), StandardCharsets.UTF_8);
        long[] p = {0, 0};
        long[] j = {0, 0};
        measured[0].values().forEach(m -> {
            p[0] += m.statements();
            p[1] += m.chars();
        });
        measured[1].values().forEach(m -> {
            j[0] += m.statements();
            j[1] += m.chars();
        });
        System.out.println("corpus " + database + ": product statements " + p[0] + ", chars " + p[1]
                + "; judge statements " + j[0] + ", chars " + j[1] + " (unguarded)");
    }
}
