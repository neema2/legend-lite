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
public class RelationalCorpusRunner {

    /**
     * THE WHOLE core_relational estate: every directory (recursively) under
     * the corpus root that directly contains .pure files is a family. No
     * hand-picked first wave — the denominator is reality; unsupported
     * territories (milestoning, union, ...) show up as walls/errors, never
     * silently out of scope.
     */
    private static List<String> allFamilies() throws Exception {
        List<String> out = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(Corpus.RELATIONAL)) {
            walk.filter(Files::isDirectory)
                    .filter(d -> {
                        try (Stream<Path> files = Files.list(d)) {
                            return files.anyMatch(f -> f.toString().endsWith(".pure"));
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .sorted()
                    .forEach(d -> out.add(Corpus.RELATIONAL.relativize(d).toString()));
        }
        return out;
    }

    @Test
    void scoreboard() throws Exception {
        Assumptions.assumeTrue(Corpus.available(), "legend-engine checkout not present");

        List<String> shared = List.of(
                Corpus.read("tests/testModel/simpleTestModel.pure"),
                Corpus.read("tests/testModel/inheritanceTestModel.pure"),
                Corpus.read("tests/relationalSetUp.pure"),
                // the corpus's OWN executeInDb wrapper surface — its 2-arg
                // wrapper inlines to the 4-arg K-native leaf (S4)
                Corpus.read("relationalExtension.pure"),
                // engine-core collection helpers the corpus consumes
                // (VERBATIM from legend-engine core/pure/corefunctions/
                // collectionExtension.pure:155-166 — only the pair the
                // tests name; the whole file would double-register
                // natives we already carry)
                """
                function meta::pure::tds::extensions::firstNotNull<T>(set:T[*]):T[0..1]
                {
                  $set->filter(v | $v != TDSNull)->first();
                }
                """);
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

        // PRE-SCAN every family file: the setup registry and the setup
        // UNIVERSE must be complete before the FIRST family runs —
        // cross-family setup calls (projection::setUp reaches join's
        // createTablesAndFillDb) resolve regardless of family order
        for (String family : allFamilies()) {
            Path p = Corpus.RELATIONAL.resolve(family);
            try (Stream<Path> s = Files.list(p)) {
                for (Path f : s.filter(x -> x.toString().endsWith(".pure")).toList()) {
                    runner.addBeforePackages(Files.readString(f), family);
                }
            }
        }

        Map<String, List<Runner.Outcome>> byFamily = new LinkedHashMap<>();
        for (String family : allFamilies()) {
            List<Runner.Outcome> outcomes = runFamily(runner, family);
            if (!outcomes.isEmpty()) {
                byFamily.put(family, outcomes);
            }
        }

        String header = "# Relational corpus scoreboard (real legend-engine core_relational)\n\n"
                + "RUN-as-data over the local legend-engine checkout; row equality is the\n"
                + "contract, golden SQL is advisory. SHAPE = test body/assert form the\n"
                + "runner does not yet recognize (accounted, not skipped silently).\n"
                + "Scope: <<test.ToFix>>/<<test.Ignore>> are excluded (engine harness\n"
                + "parity) and so is <<test.ExcludeAlloy>> (legend-lite executes the\n"
                + "in-process Alloy-shaped path).\n";
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
        System.out.println("[rcorpus] walls (mappings + dropped base elements): "
                + runner.walls().size());
        System.out.println("[rcorpus] scoreboard written to docs/RELATIONAL_CORPUS.md");
    }

    /** ONE family through the pipeline — shared by the scoreboard and the
     * family-scoped fast sweep (FamilySweep probe): the family/test-file
     * split, parent setUp/store-only inheritance, module assembly, and the
     * per-test run. */
    public static List<Runner.Outcome> runFamily(Runner runner, String family)
            throws Exception {
        Path p = Corpus.RELATIONAL.resolve(family);
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(p)) {
            s.filter(f -> f.toString().endsWith(".pure")).sorted().forEach(files::add);
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
            if (!Runner.hasTestFunctions(src)) {
                familySources.add(src);
            } else {
                testSources.put(f, src);
            }
        }
        // ANCESTOR setup inheritance was tried and REVERTED: sibling-dir
        // models conflict (tests/ direct files carry alternative Person
        // models) — net 48 vs 64 passes. Families see only their own
        // directory's files — EXCEPT a parent-directory setUp.pure
        // (dedicated setup, no tests): extends/union references the
        // extends family's model/store, the one such file in the corpus.
        Path parentSetup = p.getParent().resolve("setUp.pure");
        if (!p.getParent().equals(Corpus.RELATIONAL) && Files.exists(parentSetup)) {
            String src = Files.readString(parentSetup);
            if (!Runner.hasTestFunctions(src)) {
                familySources.add(0, src);
            }
        }
        // STORE-ONLY parent files (calendarAggregation/calendarStore
        // .pure): a parent-directory source defining ONLY Database
        // elements is the family's store — inheriting it cannot
        // conflict (the reverted ancestor experiment tripped on
        // parent CLASS models, never stores)
        if (!p.getParent().equals(Corpus.RELATIONAL)) {
            try (var sib = Files.list(p.getParent())) {
                for (Path f2 : sib.filter(x ->
                    x.toString().endsWith(".pure")
                    && Files.isRegularFile(x)).sorted().toList()) {
                if (f2.equals(parentSetup)) {
                    continue;
                }
                String src2 = Files.readString(f2);
                boolean storeOnly = !Runner.hasTestFunctions(src2)
                        && src2.lines().anyMatch(l ->
                            l.startsWith("Database "))
                        && src2.lines().noneMatch(l ->
                            l.startsWith("Class ")
                            || l.startsWith("function ")
                            || l.startsWith("Mapping "));
                // FUNCTION-ONLY parent files (tds/tdsExtension.pure,
                // tds/tds.pure): a parent source defining only pure
                // FUNCTIONS is as conflict-free as a store — no model
                // elements to collide (the reverted ancestor experiment
                // tripped on parent CLASS models, never function libs)
                boolean funcOnly = !Runner.hasTestFunctions(src2)
                        && src2.lines().anyMatch(l ->
                            l.startsWith("function "))
                        && src2.lines().noneMatch(l ->
                            l.startsWith("Class ")
                            || l.startsWith("Database ")
                            || l.startsWith("Enum ")
                            || l.startsWith("Association ")
                            || l.startsWith("Mapping "));
                if (storeOnly || funcOnly) {
                    familySources.add(0, src2);
                }
                }
            }
        }
        List<String> modelOnly = new ArrayList<>(testSources.values());
        // DEEP subfamilies reference their parent family's elements
        // (union/relation ~func bodies read union's myDB) — the engine
        // compiles the module together. Depth-guarded: parents at the
        // tests/ root carry alternative models (the reverted ancestor
        // experiment), so only parents >= 3 segments deep inherit.
        String parentKey = null;
        Path parentDir = p.getParent();
        if (parentDir != null && !parentDir.equals(Corpus.RELATIONAL)) {
            String cand = Corpus.RELATIONAL.relativize(parentDir).toString();
            if (cand.split("/").length >= 3) {
                parentKey = cand;
            }
        }
        runner.useFamily(family, familySources, modelOnly, parentKey);
        for (Map.Entry<Path, String> e : testSources.entrySet()) {
            runner.useFile(e.getKey().toString(), e.getValue());
            // Phase C: discovery through the REAL parser — stereotyped
            // functions off the parsed unit, body as AST
            for (Runner.ParsedTest t : Runner.discoverTests(e.getValue())) {
                outcomes.add(runner.run(t));
            }
        }
        return outcomes;
    }
}
