// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.gs.legend.rcorpus;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The relational-corpus scoreboard run (docs/RELATIONAL_CORPUS.md): every
 * {@code <<test.Test>>} function in the covered families executes as data.
 * RECORDS results — regression pinning arrives once the first burn-down
 * stabilizes the counts.
 */
class RelationalCorpusRunner {

    /** First-wave families: what the porting plan calls "families we claim to support". */
    private static final List<String> FAMILIES = List.of(
            "tests/query",
            "tests/mapping/association",
            "tests/mapping/join",
            "tests/mapping/embedded",
            "tests/mapping/enumeration",
            "tests/mapping/distinct",
            "tests/mapping/groupBy",
            "tests/mapping/filter",
            "tests/mapping/inheritance",
            "tests/mapping/innerJoin",
            "tests/mapping/selfJoin",
            "tests/mapping/inClause",
            "tests/mapping/boolean.pure",
            "tests/mapping/dates.pure");

    @Test
    void scoreboard() throws Exception {
        Assumptions.assumeTrue(Corpus.available(), "legend-engine checkout not present");

        List<String> shared = List.of(
                Corpus.read("tests/testModel/simpleTestModel.pure"),
                Corpus.read("tests/testModel/inheritanceTestModel.pure"),
                Corpus.read("tests/relationalSetUp.pure"));
        Runner runner = new Runner(shared, shared);
        // BeforePackage setups live NEXT TO the tests (functions/tests,
        // query, mapping families) — scan every covered file plus the
        // functions/tests dir (meta::relational::tests::query::setUp et al)
        try (Stream<Path> s = Files.walk(Corpus.RELATIONAL.resolve("functions/tests"))) {
            s.filter(f -> f.toString().endsWith(".pure"))
                    .forEach(f -> {
                        try {
                            runner.addBeforePackages(Files.readString(f));
                        } catch (Exception ignore) {
                            // unreadable corpus file: the tests in it bucket anyway
                        }
                    });
        }

        Map<String, List<Runner.Outcome>> byFamily = new LinkedHashMap<>();
        for (String family : FAMILIES) {
            Path p = Corpus.RELATIONAL.resolve(family);
            List<Path> files = new ArrayList<>();
            if (Files.isDirectory(p)) {
                try (Stream<Path> s = Files.walk(p)) {
                    s.filter(f -> f.toString().endsWith(".pure")).sorted().forEach(files::add);
                }
            } else if (Files.exists(p)) {
                files.add(p);
            }
            List<Runner.Outcome> outcomes = new ArrayList<>();
            for (Path f : files) {
                runner.addBeforePackages(Files.readString(f));
            }
            // SETUP files (no test functions) extend the model for every
            // test file of the family. Test files stay per-file: one
            // unparseable sibling must not wall the whole family, and some
            // siblings carry intentionally divergent models.
            List<String> familySources = new ArrayList<>();
            Map<Path, String> testSources = new LinkedHashMap<>();
            for (Path f : files) {
                String src = Files.readString(f);
                if (Corpus.testFunctions(src).isEmpty()) {
                    familySources.add(src);
                } else {
                    testSources.put(f, src);
                }
            }
            runner.useFamily(family, familySources);
            for (Map.Entry<Path, String> e : testSources.entrySet()) {
                runner.useFile(e.getKey().toString(), e.getValue());
                for (Corpus.TestFn fn : Corpus.testFunctions(e.getValue())) {
                    outcomes.add(runner.run(fn));
                }
            }
            if (!outcomes.isEmpty()) {
                byFamily.put(family.replace("tests/", ""), outcomes);
            }
        }

        String header = "# Relational corpus scoreboard (real legend-engine core_relational)\n\n"
                + "RUN-as-data over the local legend-engine checkout; row equality is the\n"
                + "contract, golden SQL is advisory. SHAPE = test body/assert form the\n"
                + "runner does not yet recognize (accounted, not skipped silently).\n";
        List<String> seedFails = runner.seedFailures();
        if (!seedFails.isEmpty()) {
            StringBuilder sf = new StringBuilder("\n## Failed seed statements ("
                    + seedFails.size() + ")\n\n");
            seedFails.forEach(f -> sf.append("- `").append(f).append("`\n"));
            header = header + sf;
        }
        Runner.writeScoreboard(Path.of("../docs/RELATIONAL_CORPUS.md"), byFamily,
                runner.walls(), header);
        System.out.println("[rcorpus] failed seeds: " + seedFails.size());
        byFamily.forEach((f, outs) -> {
            long p = outs.stream().filter(o -> o.status() == Runner.Status.PASS).count();
            System.out.println("[rcorpus] " + f + ": " + p + "/" + outs.size() + " pass");
        });
        System.out.println("[rcorpus] mapping walls: " + runner.walls().size());
        System.out.println("[rcorpus] scoreboard written to docs/RELATIONAL_CORPUS.md");
    }
}
