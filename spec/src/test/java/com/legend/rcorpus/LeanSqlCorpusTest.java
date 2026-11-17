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
 * as ruled: the verdict and canon steps (the judge's: held to their own pins, {@code
 * rcorpus/lean-sql-judge-baseline.tsv}, by the same rule since E-5d) and a fixture's setup, seeds and loads (data delivery). Its own targets,
 * {@code //spec:lean_sql_corpus_duckdb} and {@code _h2}; re-pin with {@code bazel run
 * //spec:update_lean_sql_corpus_<lane>}.
 */
@Tag("heavy")
class LeanSqlCorpusTest {

    /** The pins' file in the source tree. */
    static final String PINS = "spec/src/test/resources/rcorpus/lean-sql-baseline.tsv";

    /** The JUDGE's pins (E-5d, the design doc's §5.9; ruled with E-5, 2026-11-13): the verdict statements of every
     *  corpus body planned under the DATABASE judge -- since E-5c BOTH plans the same program -- held by the same rule
     *  per database and package. Until E-5d they were written unguarded ({@code judge-sql.tsv}). */
    static final String JUDGE_PINS = "spec/src/test/resources/rcorpus/lean-sql-judge-baseline.tsv";

    /** The corpus's bodies planned on this JVM's backend: [0] the product's measures, [1] the judge's. */
    @SuppressWarnings("unchecked")
    static Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure>[] measure() throws IOException {
        MinimalCorpus corpus = new MinimalCorpus();
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> product = new java.util.HashMap<>();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> judge = new java.util.HashMap<>();
        // the corpus's resource store, as its run has (MinimalCorpus's options): a body that loads a CSV plans its
        // load against the file's digest. Only the store: the measure keeps lite's own rules and spelling (round 55f,
        // 2026-11-15: a DataError, "this run was given no store for it", had been counted as a refusal)
        com.legend.ExecuteOptions options = options(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE);
        for (com.legend.test.PureTests.TestCase t : corpus.tests()) {
            String family = t.pkg();
            List<String> productSql = new ArrayList<>();
            List<ExecutionPlan.Node> productNodes = new ArrayList<>();
            List<String> judgeSql = new ArrayList<>();
            long[] judgePlan = {0};
            try {
                var resolved = Compiler.resolveQuery(List.copyOf(t.fn().body()), t.imports(), corpus.context());
                TestPlan plan = com.legend.TestExecution.planBody(Compiler.queryResolved(corpus.context(), resolved),
                        MinimalCorpus.RUNTIME, options);
                steps(plan.steps(), productSql, productNodes, judgeSql, judgePlan);
            } catch (RuntimeException e) {
                if (!com.legend.leansql.LeanSqlBaseline.isRefusal(e)) {
                    throw e;
                }
                // refused at planning: counted as such under its own family, never as a body of no SQL (2026-11-15,
                // PR1's review: a refused body pinned 0 0 0 and could not be told from a smaller one)
                LeanSqlBaseline.Key pk = new LeanSqlBaseline.Key("corpus", database, family);
                LeanSqlBaseline.Key jk = new LeanSqlBaseline.Key("corpus-judge", database, family);
                product.put(pk, product.getOrDefault(pk, new LeanSqlBaseline.Measure(0, 0, 0, "")).refusedOne(t.fqn()));
                judge.put(jk, judge.getOrDefault(jk, new LeanSqlBaseline.Measure(0, 0, 0, "")).refusedOne(t.fqn()));
                continue;
            }
            long planChars = 0;
            for (ExecutionPlan.Node n : productNodes) {
                planChars += PlanJson.write(ExecutionPlan.reading(List.of(), n)).length();
            }
            add(product, new LeanSqlBaseline.Key("corpus", database, family), productSql, planChars, t.fqn());
            add(judge, new LeanSqlBaseline.Key("corpus-judge", database, family), judgeSql, judgePlan[0], t.fqn());
        }
        return new Map[] {product, judge};
    }

    private static void add(Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> into, LeanSqlBaseline.Key k,
            List<String> statements, long p, String test) {
        LeanSqlBaseline.Measure was = into.getOrDefault(k, new LeanSqlBaseline.Measure(0, 0, 0, ""));
        into.put(k, was.plus(statements, p, test));
    }

    /** The body's own statement nodes to the product's; the verdict steps' statements to the judge's. */
    private static void steps(List<TestPlan.TestStep> steps, List<String> productSql,
            List<ExecutionPlan.Node> productNodes, List<String> judgeSql, long[] judgePlan) {
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
                // the judge's statements as the runner sends them (TestPlan.sent: a fused statement or each wrapper, every
                // raising verdict's wrapper, the sides, a branch's condition and both its sides' steps) and each step's
                // plan JSON (2026-11-15, PR1's review: the fused statement and base sides alone, and no plan JSON)
                case TestPlan.Verdicts v -> judged(v, judgeSql, judgePlan);
                case TestPlan.Branch b -> {
                    judged(b, judgeSql, judgePlan);
                    // a side's product steps are the product's (TestPlan.sent leaves them out); its judge steps were
                    // counted above, so they go to a list not kept here (round 55f, 2026-11-15)
                    List<String> counted = new ArrayList<>();
                    long[] countedPlan = {0};
                    steps(b.then(), productSql, productNodes, counted, countedPlan);
                    steps(b.otherwise(), productSql, productNodes, counted, countedPlan);
                }
                // each judge's plan alone, the host's first (the isolated pair, 2026-11-17)
                case TestPlan.Isolated p -> {
                    steps(p.host().steps(), productSql, productNodes, judgeSql, judgePlan);
                    steps(p.database().steps(), productSql, productNodes, judgeSql, judgePlan);
                }
            }
        }
    }

    /** The measure's options under {@code mode}: the corpus's resource store, as its run has (MinimalCorpus's options), so
     *  a body that loads a CSV plans its load against the file's digest; lite's own rules and spelling (round 55f,
     *  2026-11-15: a DataError, "this run was given no store for it", had been counted as a refusal). */
    private static com.legend.ExecuteOptions options(com.legend.testplan.PlanningOptions.JudgeMode mode) {
        // the edge's style, as the corpus lanes plan it (the orchestrator's (A), 2026-11-16): the corpus is upstream's
        // suite and CI runs it through the edge; on the product's ~300 bodies were refused by name and left the metric.
        // The product's own leanness is core's baseline (LeanSqlBaseline, the product's style)
        return new com.legend.ExecuteOptions(null,
                com.legend.executionplan.DataStores.resources(Corpus.RELATIONAL.getParent().getParent()),
                java.util.Set.of()).withJudgeMode(mode).withStyle(com.legend.edge.PureText.UPSTREAM);
    }

    /** {@code step}'s judge statements and plan JSON, added. */
    private static void judged(TestPlan.TestStep step, List<String> judgeSql, long[] judgePlan) {
        for (ExecutionPlan.Node n : TestPlan.sent(step)) {
            LeanSqlBaseline.statements(n, judgeSql);
        }
        judgePlan[0] += com.legend.testexecutionplan.TestPlanJson.writeStep(step).length();
    }

    @Test
    void theCorpusProductSqlIsHeldToItsBaseline() throws Exception {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure>[] measured = measure();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned = pinned("/rcorpus/lean-sql-baseline.tsv");
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> judgePinned = pinned("/rcorpus/lean-sql-judge-baseline.tsv");
        List<String> drift = LeanSqlBaseline.drift(pinned, measured[0]);
        List<String> judgeDrift = LeanSqlBaseline.drift(judgePinned, measured[1]);
        assertTrue(drift.isEmpty() && judgeDrift.isEmpty(), (drift.isEmpty() ? ""
                : "the corpus's product SQL left its baseline (" + PINS + "):\n  " + String.join("\n  ", drift)
                        + "\n") + (judgeDrift.isEmpty() ? ""
                : "the corpus's judge SQL left its baseline (" + JUDGE_PINS + "):\n  "
                        + String.join("\n  ", judgeDrift)));
    }

    /** {@code resource}'s pins for this JVM's database. */
    private static Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned(String resource) throws IOException {
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned = new java.util.HashMap<>();
        try (var in = LeanSqlCorpusTest.class.getResourceAsStream(resource)) {
            if (in != null) {
                LeanSqlBaseline.read(new String(in.readAllBytes(), StandardCharsets.UTF_8)).forEach((k, m) -> {
                    if (k.database().equals(database)) {
                        pinned.put(k, m);
                    }
                });
            }
        }
        return pinned;
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
                    options(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE));
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
                case TestPlan.Isolated p -> {
                    categorize(p.host().steps(), test, c, ex);
                    categorize(p.database().steps(), test, c, ex);
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
                    MinimalCorpus.RUNTIME, options(mode));
        } catch (RuntimeException e) {
            if (!com.legend.leansql.LeanSqlBaseline.isRefusal(e)) {
                throw e;
            }
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
                case TestPlan.Isolated p -> {
                    walkHost(p.host().steps(), host);
                    walkHost(p.database().steps(), host);
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
                case TestPlan.Isolated p -> {
                    walkDatabase(p.host().steps(), database);
                    walkDatabase(p.database().steps(), database);
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
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure>[] measured = measure();
        repin(Path.of(workspace, PINS), database, measured[0]);
        repin(Path.of(workspace, JUDGE_PINS), database, measured[1]);
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
                + "; judge statements " + j[0] + ", chars " + j[1]);
    }

    /** {@code file}'s pins with this database's replaced by {@code measured}, the other databases' kept. */
    private static void repin(Path file, String database, Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> measured)
            throws IOException {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pins = new java.util.HashMap<>();
        if (Files.exists(file)) {
            LeanSqlBaseline.read(Files.readString(file, StandardCharsets.UTF_8)).forEach((k, m) -> {
                if (!k.database().equals(database)) {
                    pins.put(k, m);
                }
            });
        }
        pins.putAll(measured);
        Files.writeString(file, LeanSqlBaseline.write(pins), StandardCharsets.UTF_8);
    }
}
