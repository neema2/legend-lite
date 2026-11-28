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
 * as ruled: the verdict and canon steps (the judge's; its pins went with the corpus's database pass, step 1
 * b523c767d, the orchestrator's ruling of 2026-11-17: no corpus lane sends that SQL, and the judge's SQL is guarded
 * where it runs, core's lean baselines and the ladder) and a fixture's setup, seeds and loads (data delivery). Its own targets,
 * {@code //spec:lean_sql_corpus_duckdb} and {@code _h2}; re-pin with {@code bazel run
 * //spec:update_lean_sql_corpus_<lane>}.
 */
@Tag("heavy")
class LeanSqlCorpusTest {

    /** The pins' file in the source tree. */
    static final String PINS = "spec/src/test/resources/rcorpus/lean-sql-baseline.tsv";

    /** The corpus's bodies planned on this JVM's backend: the product's measures. */
    static Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> measure() throws IOException {
        MinimalCorpus corpus = new MinimalCorpus();
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> product = new java.util.HashMap<>();
        // the corpus's resource store, as its run has (MinimalCorpus's options): a body that loads a CSV plans its
        // load against the file's digest. Only the store: the measure keeps lite's own rules and spelling (round 55f,
        // 2026-11-15: a DataError, "this run was given no store for it", had been counted as a refusal)
        // planned under the host judge (A4 of the compatibility program, stage A ratified by the user 2026-11-19: the
        // corpus is a compatibility audience, and rule 4 of the rules of record judges it by the host alone); the measure
        // counts the body's product statements alone, which neither judge's verdict steps add to
        com.legend.ExecuteOptions options = options(com.legend.testplan.PlanningOptions.JudgeMode.HOST);
        for (com.legend.test.PureTests.TestCase t : corpus.tests()) {
            String family = t.pkg();
            List<String> productSql = new ArrayList<>();
            List<ExecutionPlan.Node> productNodes = new ArrayList<>();
            try {
                var resolved = Compiler.resolveQuery(List.copyOf(t.fn().body()), t.imports(), corpus.context());
                // admitted by the edge's test-body door, as the corpus's runner admits it (B3 step 2, 2026-11-24)
                TestPlan plan = com.legend.TestExecution.planBody(com.legend.edge.UpstreamTestBodies.admitted(
                        Compiler.queryResolved(corpus.context(), resolved)), MinimalCorpus.RUNTIME, options);
                steps(plan.steps(), productSql, productNodes);
            } catch (RuntimeException e) {
                if (!com.legend.leansql.LeanSqlBaseline.isRefusal(e)) {
                    throw e;
                }
                // refused at planning: counted as such under its own family, never as a body of no SQL (2026-11-15,
                // PR1's review: a refused body pinned 0 0 0 and could not be told from a smaller one)
                LeanSqlBaseline.Key pk = new LeanSqlBaseline.Key("corpus", database, family);
                product.put(pk, product.getOrDefault(pk, new LeanSqlBaseline.Measure(0, 0, 0, "")).refusedOne(t.fqn()));
                continue;
            }
            long planChars = 0;
            for (ExecutionPlan.Node n : productNodes) {
                planChars += PlanJson.write(ExecutionPlan.reading(List.of(), n)).length();
            }
            add(product, new LeanSqlBaseline.Key("corpus", database, family), productSql, planChars, t.fqn());
        }
        return product;
    }

    private static void add(Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> into, LeanSqlBaseline.Key k,
            List<String> statements, long p, String test) {
        LeanSqlBaseline.Measure was = into.getOrDefault(k, new LeanSqlBaseline.Measure(0, 0, 0, ""));
        into.put(k, was.plus(statements, p, test));
    }

    /** The body's own statement nodes to the product's; the verdict steps' are the judge's, not measured here. */
    private static void steps(List<TestPlan.TestStep> steps, List<String> productSql,
            List<ExecutionPlan.Node> productNodes) {
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
                case TestPlan.Verdicts v -> {
                    // the judge's
                }
                case TestPlan.Branch b -> {
                    // a side's product steps are the product's; its judge steps are the judge's
                    steps(b.then(), productSql, productNodes);
                    steps(b.otherwise(), productSql, productNodes);
                }
                // each judge's plan alone, the host's first (the isolated pair, 2026-11-17)
                case TestPlan.Isolated p -> {
                    steps(p.host().steps(), productSql, productNodes);
                    steps(p.database().steps(), productSql, productNodes);
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
        return new com.legend.ExecuteOptions(com.legend.testexecutionplan.NoEdge.NONE,
                com.legend.executionplan.DataStores.resources(Corpus.RELATIONAL.getParent().getParent()),
                java.util.Set.of()).withJudgeMode(mode).withStyle(com.legend.edge.PureText.ENGINE_TESTS);
    }

    @Test
    void theCorpusProductSqlIsHeldToItsBaseline() throws Exception {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> measured = measure();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned = pinned("/rcorpus/lean-sql-baseline.tsv");
        List<String> drift = LeanSqlBaseline.drift(pinned, measured);
        assertTrue(drift.isEmpty(), "the corpus's product SQL left its baseline (" + PINS + "):\n  "
                + String.join("\n  ", drift));
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

    /** {@code bazel run //spec:update_lean_sql_corpus_<lane>}: this backend's rows measured again and written over
     *  the source tree's pins, the other backends' rows kept (E-5c's fused measurement went with the fused agreement,
     *  2026-11-17). */
    public static void main(String[] args) throws IOException {
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspace == null) {
            throw new IllegalStateException("run it with bazel run //spec:update_lean_sql_corpus_<lane>");
        }
        String database = MinimalCorpus.BACKEND.type().name();
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> measured = measure();
        repin(Path.of(workspace, PINS), database, measured);
        long[] p = {0, 0};
        measured.values().forEach(m -> {
            p[0] += m.statements();
            p[1] += m.chars();
        });
        System.out.println("corpus " + database + ": product statements " + p[0] + ", chars " + p[1]);
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
