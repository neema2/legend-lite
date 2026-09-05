// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.rcorpus;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The relational-corpus scoreboard run (docs/RELATIONAL_CORPUS.md): every
 * {@code <<test.Test>>} function in the covered families executes as data.
 * RECORDS results — regression pinning arrives once the first burn-down
 * stabilizes the counts.
 */
public class RelationalCorpusRunner {

    /** Engine IMPLEMENTATION files shipped beside their tests: the
     * reference checkout is the SPEC, never runtime (memory rule) — the
     * platform owns these functions (natives + Pure bodies over the system
     * store's rows), so their engine bodies never join a family model.
     * scanRelations.pure: the lineage program (harness burn-down group E —
     * the tree is LineageRows, printed by the database). */
    static final java.util.Set<String> ENGINE_IMPLEMENTATION_FILES = java.util.Set.of(
            "lineage/scanRelations/scanRelations.pure");

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
        // ENGINE-CORPUS-COMPAT (user ruling 2026-08-29, the ENGINE_CASED
        // precedent): the engine's tests assert positionally while
        // relying on H2's implicit scan order — replaying them on DuckDB
        // opts into the explicit scan-order pass. The PLATFORM default
        // stays order-honest (no sort demanded = no order guaranteed).
        System.setProperty("legend.exec.engineScanOrder", "true");

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
                """,
                // engine-core date-format constants (VERBATIM from
                // core/pure/corefunctions/dateExtension.pure:384-392 —
                // the corpus's toCSV date rendering)
                """
                function meta::pure::functions::date::SimpleDateTimeFormat():String[1]
                {
                   '%t{yyyy-MM-dd HH:mm:ss}';
                }

                function meta::pure::functions::date::ISO8601DateFormat():String[1]
                {
                   '%t{yyyy-MM-dd}';
                }
                """,
                // engine-core geo distances (VERBATIM from
                // core/pure/corefunctions/mathExtension.pure:15-48 —
                // the olap rank fail-stubs are not carried)
                """
                function meta::pure::functions::math::earthRadius():Float[1]
                {
                   6371.0;
                }

                function meta::pure::functions::math::distanceHaversineDegrees(lat1Degrees:Number[1],lon1Degrees:Number[1],lat2Degrees:Number[1],lon2Degrees:Number[1]):Number[1]
                {
                   distanceHaversineRadians(toRadians($lat1Degrees),toRadians($lon1Degrees),toRadians($lat2Degrees),toRadians($lon2Degrees));
                }

                function meta::pure::functions::math::distanceHaversineRadians(lat1Radians:Number[1],lon1Radians:Number[1],lat2Radians:Number[1],lon2Radians:Number[1]):Number[1]
                {
                   earthRadius() * angularDistanceInRadians(squareOfHalfTheChord($lat1Radians, $lon1Radians, $lat2Radians, $lon2Radians));
                }

                function <<access.private>> meta::pure::functions::math::squareOfHalfTheChord(lat1Radians:Number[1],lon1Radians:Number[1],lat2Radians:Number[1],lon2Radians:Number[1]):Number[1]
                {
                   pow((sin(($lat2Radians - $lat1Radians) / 2)), 2) + (cos($lat1Radians) * cos($lat2Radians) * pow(sin(($lon2Radians - $lon1Radians) / 2), 2));
                }

                function <<access.private>> meta::pure::functions::math::angularDistanceInRadians(a:Number[1]):Float[1]
                {
                   2.0 * atan2(sqrt($a), sqrt(1 - $a));
                }

                function meta::pure::functions::math::distanceSphericalLawOfCosinesDegrees(lat1Degrees:Number[1],lon1Degrees:Number[1],lat2Degrees:Number[1],lon2Degrees:Number[1]):Number[1]
                {
                   distanceSphericalLawOfCosinesRadians(toRadians($lat1Degrees), toRadians($lon1Degrees), toRadians($lat2Degrees), toRadians($lon2Degrees));
                }

                function meta::pure::functions::math::distanceSphericalLawOfCosinesRadians(lat1Radians:Number[1],lon1Radians:Number[1],lat2Radians:Number[1],lon2Radians:Number[1]):Number[1]
                {
                   earthRadius() * acos((sin($lat1Radians) * sin($lat2Radians)) + (cos($lat1Radians) * cos($lat2Radians) * cos($lon2Radians - $lon1Radians)));
                }
                """);
        Runner runner = new Runner(shared, shared);
        // the platform m2m TEST LIBRARY (Corpus.M2M_TESTS): elements only
        // — qualified refs (testModelConnection*'s M2M mappings) pull the
        // defining files into modules; never setups/expansion
        if (Files.isDirectory(Corpus.M2M_TESTS)) {
            try (Stream<Path> m2m = Files.walk(Corpus.M2M_TESTS)) {
                for (Path f : m2m.filter(x -> x.toString().endsWith(".pure"))
                        .sorted().toList()) {
                    try {
                        runner.registerLibrarySource(Files.readString(f));
                    } catch (Exception ignore) {
                        // unreadable library file: its elements stay dark
                    }
                }
            }
        }
        // the graphFetch DOMAIN-MANAGEMENT library (engine-core
        // core/pure/graphFetch/domain — the Domain/DataSpace test model
        // the relational graphFetch/domain family maps onto); library
        // elements only, pulled by reference like the m2m test library
        Path gfDomain = Corpus.ENGINE_ROOT.resolve(
                "legend-engine-core/legend-engine-core-pure/"
                + "legend-engine-pure-code-compiled-core/"
                + "src/main/resources/core/pure/graphFetch/domain");
        if (Files.isDirectory(gfDomain)) {
            try (Stream<Path> gf = Files.walk(gfDomain)) {
                for (Path f : gf.filter(x -> x.toString().endsWith(".pure"))
                        .sorted().toList()) {
                    try {
                        runner.registerLibrarySource(Files.readString(f));
                    } catch (Exception ignore) {
                        // unreadable library file: its elements stay dark
                    }
                }
            }
        }
        // the named PROGRAM libraries (Corpus.LIBRARY_FILES): library
        // elements only, pulled by reference exactly like the m2m test
        // library; the prelude generator reads the same list
        for (Path lib : Corpus.LIBRARY_FILES) {
            if (Files.isRegularFile(lib)) {
                try {
                    runner.registerLibrarySource(Files.readString(lib));
                } catch (Exception ignore) {
                    // unreadable: the family stays walled as before
                }
            }
        }
        // V7 TENET CORRECTION (2026-08-28, user catch): the assert
        // family is PLATFORM-OWNED registry natives (Pure.java, real
        // signatures verified verbatim; AssertVerdicts is the
        // implementation) — an earlier slice loaded the real
        // legend-pure assert SOURCES here as library files, which made
        // the reference implementation a runtime component of our
        // model. Reference checkouts are SPEC and TEST INPUT, never
        // platform machinery; registerLibrarySource now REFUSES
        // platform-namespace elements outright.
        runner.classLookup = fqn -> {
            try {
                return classIndex().get(fqn);
            } catch (Exception e) {
                return null;
            }
        };
        // BeforePackage setups live NEXT TO the tests (functions/tests,
        // query, mapping families) — scan every covered file plus the
        // functions/tests dir (meta::relational::tests::query::setUp et al)
        // .sorted(): filesystem order is not a contract (STATE_AUDIT S4.4).
        // addBeforePackages feeds putIfAbsent chains that decide WHICH body a
        // helper call expands to, so an unsorted walk makes the scoreboard's
        // wall TEXT flap between runs — demonstrated on testViewToTDS and
        // testResultToJsonStream, which reported different first-failures on
        // consecutive sweeps at identical HEAD and corpus root.
        try (Stream<Path> s = Files.walk(Corpus.RELATIONAL.resolve("functions/tests"))) {
            s.filter(f -> f.toString().endsWith(".pure"))
                    .sorted()
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
                // .sorted(): see the note on the functions/tests walk above
                for (Path f : s.filter(x -> x.toString().endsWith(".pure"))
                        .sorted().toList()) {
                    runner.addBeforePackages(Files.readString(f), family);
                }
            }
        }

        // PHASE 1 — REGISTER EVERY family (unscoped: the ONE global model
        // always compiles the whole corpus, so scoped probes see exactly
        // the model a full sweep sees). PHASE 2 runs tests. Interleaving
        // these froze the global compile at the first family's sources.
        Map<String, Map<Path, String>> familyTests = new LinkedHashMap<>();
        for (String family : allFamilies()) {
            familyTests.put(family, registerFamily(runner, family));
        }
        Map<String, List<Runner.Outcome>> byFamily = new LinkedHashMap<>();
        // -Drcorpus.only=<family-substring>[,<substring>...] scopes the run
        // for fast leg iteration; a scoped run NEVER writes the scoreboard
        // (a partial ledger must not clobber the full one).
        String only = System.getProperty("rcorpus.only", "").trim();
        List<String> onlyFilters = only.isEmpty() ? List.of()
                : List.of(only.split(","));
        for (Map.Entry<String, Map<Path, String>> fam : familyTests.entrySet()) {
            String family = fam.getKey();
            if (!onlyFilters.isEmpty()
                    && onlyFilters.stream().noneMatch(family::contains)) {
                continue;
            }
            List<Runner.Outcome> outcomes = runFamily(runner, family,
                    fam.getValue());
            if (!outcomes.isEmpty()) {
                byFamily.put(family, outcomes);
            }
        }

        // THE DENOMINATOR, stated by the discovery path itself — no external
        // grep, no arithmetic, nothing to argue with. Every core_relational
        // test is either runnable or excluded-with-a-named-reason.
        Map<String, Long> byReason = Runner.CENSUS_EXCLUDED.values().stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        r -> r == null ? "unknown" : r,
                        java.util.TreeMap::new,
                        java.util.stream.Collectors.counting()));
        int runnable = Runner.CENSUS_RUNNABLE.size();
        int excluded = Runner.CENSUS_EXCLUDED.size();
        String census = "\n## Census (core_relational)\n\n"
                + "| | count |\n|---|---:|\n"
                + "| **total `<<test.Test>>` functions** | **" + (runnable + excluded) + "** |\n"
                + "| runnable (this scoreboard) | " + runnable + " |\n"
                + "| excluded by stereotype | " + excluded + " |\n"
                + byReason.entrySet().stream()
                        .map(e -> "| …`<<test." + e.getKey() + ">>` | " + e.getValue() + " |\n")
                        .collect(java.util.stream.Collectors.joining())
                + "\nCounted by the discovery path (`Runner.discoverTests`), keyed by test FQN so a\n"
                + "shared source registered by several families cannot double-count. Run with\n"
                + "`-Drcorpus.includeExcluded` to run the excluded ones too.\n";
        System.out.println("[rcorpus] census: " + (runnable + excluded)
                + " total, " + runnable + " runnable, " + excluded
                + " excluded " + byReason
                + (Runner.INCLUDE_EXCLUDED ? "  (INCLUDED THIS RUN)" : ""));

        String header = "# Relational corpus scoreboard (real legend-engine core_relational)\n\n"
                + "RUN-as-data over the local legend-engine checkout; row equality is the\n"
                + "contract, golden SQL is advisory. SHAPE = test body/assert form the\n"
                + "runner does not yet recognize (accounted, not skipped silently).\n"
                + "Scope: <<test.ToFix>>/<<test.Ignore>> are excluded (engine harness\n"
                + "parity) and so is <<test.ExcludeAlloy>> (legend-lite executes the\n"
                + "in-process Alloy-shaped path)"
                + (Runner.INCLUDE_EXCLUDED
                        ? " — BUT THIS RUN INCLUDED THEM\n(-Drcorpus.includeExcluded).\n"
                        : ".\n")
                + "\nADJUDICATION LEDGER: every non-passing row carries a per-test\n"
                + "evidence-backed verdict (REAL_DEFECT / MISSING_FEATURE /\n"
                + "TESTS_ENGINE_INTERNALS / GOLDEN_TEXT_ONLY /\n"
                + "EXECUTION_TARGET_ARTIFACT / HARNESS_GAP / NEEDS_PROBE), effort,\n"
                + "confidence and falsifier in\n"
                + "docs/e2e-diagnosis-2026-08-15/diagnoses.csv (keyed by test name;\n"
                + "reconciliation log in docs/E2E_DEEP_DIAGNOSIS_2026_08_15.md —\n"
                + "retirements are shrink-only, verdict changes need the row's own\n"
                + "falsifier to fire).\n"
                + census;
        List<String> seedFails = runner.seedFailures();
        if (!seedFails.isEmpty()) {
            StringBuilder sf = new StringBuilder("\n## Failed seed statements ("
                    + seedFails.size() + ")\n\n");
            seedFails.forEach(f -> sf.append("- `").append(f).append("`\n"));
            header = header + sf;
        }
        // the COMMITTED baseline reads BEFORE the sweep rewrites it
        Map<String, Integer> baseline =
                readBaseline(Path.of("../docs/RELATIONAL_CORPUS.md"));
        // GATE BEFORE WRITE. The scoreboard is a COMMITTED artifact, and this
        // sweep used to rewrite it in place and only then assert — so a
        // regression (or, worse, a run against the wrong corpus root) left a
        // corrupted file in the working tree and relied on the operator
        // reading the failure text before committing. The gate's own message
        // said "do not commit the rewritten scoreboard", which is advice, not
        // a mechanism. Compute the verdict here; write only when clean.
        List<String> regressions = new ArrayList<>();
        if (System.getProperty("rcorpus.test", "").trim().isEmpty()
                && !Runner.INCLUDE_EXCLUDED) {
            byFamily.forEach((f, outs) -> {
                long p = outs.stream()
                        .filter(o -> o.status() == Runner.Status.PASS).count();
                Integer b = baseline.get(f);
                if (b != null && p < b) {
                    regressions.add(f + " " + p + " < baseline " + b);
                }
            });
        }
        if (Runner.H2_BACKEND) {
            // the PORTABILITY SWEEP is a different execution target: its
            // ledger never clobbers the DuckDB scoreboard and the DuckDB
            // baseline gate does not apply (H2_BACKEND.md §10 — an H2
            // FAIL must not touch the DuckDB row)
            long p = byFamily.values().stream().flatMap(List::stream)
                    .filter(o -> o.status() == Runner.Status.PASS).count();
            long u = byFamily.values().stream().flatMap(List::stream)
                    .filter(o -> o.status() == Runner.Status.UNSUPPORTED)
                    .count();
            long n = byFamily.values().stream().mapToLong(List::size).sum();
            System.out.println("[rcorpus] h2-backend sweep: " + p + "/" + n
                    + " pass, " + u + " unsupported (typed capability"
                    + " walls) — scoreboard NOT written (DuckDB baseline"
                    + " untouched)");
            // the CAPABILITY BUDGET (§9/§10): every declared renderer gap,
            // counted — growth in a bucket is a visible decision, never
            // silent scope creep
            Map<String, Long> budget = byFamily.values().stream()
                    .flatMap(List::stream)
                    .filter(o -> o.status() == Runner.Status.UNSUPPORTED)
                    .collect(java.util.stream.Collectors.groupingBy(
                            o -> o.detail(),
                            java.util.TreeMap::new,
                            java.util.stream.Collectors.counting()));
            budget.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue()
                            .reversed())
                    .forEach(e -> System.out.println(
                            "[rcorpus] h2-capability " + e.getValue() + "x "
                            + e.getKey()));
            byFamily.forEach((f, outs) -> {
                long fp = outs.stream()
                        .filter(o -> o.status() == Runner.Status.PASS).count();
                System.out.println("[rcorpus] h2-backend " + f + ": " + fp
                        + "/" + outs.size() + " pass");
            });
            if (!onlyFilters.isEmpty()) {
                // scoped h2 probe: per-test detail, exactly like the
                // scoped DuckDB run
                byFamily.forEach((f, outs) -> outs.stream()
                        .filter(o -> o.status() != Runner.Status.PASS)
                        .forEach(o -> System.out.println("[rcorpus]   "
                                + o.status() + " " + o.test() + ": "
                                + o.detail())));
            }
            System.out.println("[rcorpus] failed seeds: " + seedFails.size());
            seedFails.forEach(f -> System.out.println("[rcorpus]   seed-fail: " + f));
            System.out.println("[rcorpus] seed replay: "
                    + Runner.SEED_CALLS.get() + " calls, "
                    + (Runner.SEED_NANOS.get() / 1_000_000) + " ms; raw jdbc "
                    + com.legend.exec.Executor.RAW_CALLS.get() + " stmts, "
                    + (com.legend.exec.Executor.RAW_NANOS.get() / 1_000_000)
                    + " ms");
            // THE H2 LANE ASSERTS (DEEP_AUDIT §11c: gate 5 swept 2,575
            // tests and asserted NOTHING — "1362 could become 1 without
            // moving the verdict"). Floors measured 2026-08-21; pass
            // RATCHETS UP (raise the floor when earned), seeds and the
            // capability budget only shrink.
            if (onlyFilters.isEmpty()) {
                org.junit.jupiter.api.Assertions.assertAll(
                        // 1372 -> 1375 (§4AD batch 5, THE ROUTER FLIP):
                        // +3 h2-lane passes from the lifted fan-out shapes.
                        // 1375 -> 1347 (TDG lane S2, JUSTIFIED): the ~28
                        // converted tests' asserts now REACH H2's renderer
                        // (list vocabulary absent there) — their old H2
                        // "passes" were HARNESS-adjudicated compensation,
                        // not platform verification; the walls below name
                        // the real gap and the h2-lane leg owns burning it
                        // 1347 -> 1329 (slice-1 §10h, JUSTIFIED — the
                        // same pattern as TDG lane S2): the try-run
                        // host-evaluation lane is DELETED, so the 20
                        // quarantined toPostgresModel tests lose their
                        // h2-lane HARNESS-adjudicated passes (host
                        // channel, never platform verification) — 10
                        // wall on the renderer, 10 error at lowering;
                        // worktree receipt: HEAD full h2 sweep = 1349,
                        // this tree = 1329, delta EXACTLY the family
                        () -> org.junit.jupiter.api.Assertions.assertTrue(
                                p >= 1329, "h2 sweep pass fell: " + p
                                        + " < floor 1329 (slice-1 §10h;"
                                        + " prior floor note: SQL-IR slice 2"
                                        + " outputs-from-projections:"
                                        + " 1367 -> 1372 — the milestoning"
                                        + " union-wrap residue healed +"
                                        + " 3 more label-consistency rows."
                                        + " Known residue testChained"
                                        + "JoinsWithUnionsAndIsolation"
                                        + "WithProjectionQueryTableFilter"
                                        + " is a DEMAND divergence, not"
                                        + " origin: the union extent"
                                        + " projects an undemanded"
                                        + " legalName the engine prunes;"
                                        + " our prune is blocked by the"
                                        + " star over the union frame —"
                                        + " charter ORIGIN_ARCHITECTURE"
                                        + "_AUDIT landing record)"),
                        () -> org.junit.jupiter.api.Assertions.assertTrue(
                                seedFails.size() <= 6,
                                "h2 failed seeds grew: " + seedFails.size()
                                        + " > 6"),
                        // 945 -> 946 (slice-3 equality half, 2026-08-28,
                        // JUSTIFIED): one of the 94 newly-compiling
                        // sqltext asserts reaches a REGISTERED h2
                        // renderer capability wall under the h2 backend
                        // — one more plan reading a known gap, not a
                        // widened gap (DuckDB sweep pass counts and all
                        // EQUALITY-0 gates unchanged)
                        // 946 -> 947 (§4AD batch 5, JUSTIFIED — the
                        // design doc's predicted pattern: UNNEST 903 ->
                        // 904; a lifted fan-out shape reads the
                        // REGISTERED h2 UNNEST-placement gap, not a
                        // widened gap; h2 floor +3 in the same commit)
                        () -> org.junit.jupiter.api.Assertions.assertTrue(
                                // 947 -> 949 (TDG lane S1, JUSTIFIED —
                                // the 2 sortBy-over-census-literal rows
                                // route through the platform now and H2
                                // has no list-lambda vocabulary: the
                                // wall IS the honest H2 answer; DuckDB
                                // primary passes them row-verified)
                                // 949 -> 983 (TDG lane S2): the same ~28
                                // tests' asserts wall by NAME on H2 — the
                                // advisory target's honest answer
                                // 983 -> 993 (slice-1 §10h, JUSTIFIED):
                                // 10 quarantined toPostgresModel tests
                                // reach the REGISTERED renderer gaps now
                                // that the host-eval lane is deleted —
                                // known gaps read, not widened
                                u <= 993, "h2 capability walls grew: " + u
                                        + " > 993 — a renderer gap widened"
                                        + " silently"));
            }
            return;
        }
        if (onlyFilters.isEmpty() && Runner.INCLUDE_EXCLUDED) {
            // the 100% ledger is a DIFFERENT denominator (it runs the
            // upstream-skipped tests), so it gets its own file and never
            // touches the DuckDB baseline — same rule as the H2 sweep.
            // Promoting it would make every later normal run look like a
            // mass regression.
            Runner.writeScoreboard(Path.of("../docs/RELATIONAL_CORPUS_ALL.md"), byFamily,
                    runner.walls(), header);
            System.out.println("[rcorpus] 100% ledger written to"
                    + " docs/RELATIONAL_CORPUS_ALL.md (baseline untouched)");
        } else if (!System.getProperty("rcorpus.test", "").trim().isEmpty()) {
            // F4.3 hole-plug: a -Drcorpus.test scoped run bypassed BOTH the
            // only-filter check and the regression gate (which skips when
            // test-scoped), so it wrote a TRUNCATED scoreboard — caught when
            // a stash carried one. Test-scoped runs NEVER write.
            System.out.println("[rcorpus] TEST-SCOPED run (rcorpus.test) —"
                    + " scoreboard NOT written");
            // scoped iteration needs the verdict detail on stdout (the
            // full-run path prints it via the regression gate)
            byFamily.forEach((f, outs) -> outs.stream()
                    .filter(o -> o.status() != Runner.Status.PASS)
                    .forEach(o -> System.out.println("[rcorpus]   " + o.status()
                            + " " + o.test() + ": " + o.detail())));
        } else if (onlyFilters.isEmpty() && regressions.isEmpty()) {
            Runner.writeScoreboard(Path.of("../docs/RELATIONAL_CORPUS.md"), byFamily,
                    runner.walls(), header);
        } else if (onlyFilters.isEmpty()) {
            System.out.println("[rcorpus] REGRESSION — scoreboard NOT written;"
                    + " the committed docs/RELATIONAL_CORPUS.md is intact");
            byFamily.forEach((f, outs) -> outs.stream()
                    .filter(o -> o.status() != Runner.Status.PASS)
                    .forEach(o -> System.out.println("[rcorpus]   " + o.status()
                            + " " + o.test() + ": " + o.detail())));
        } else {
            System.out.println("[rcorpus] SCOPED run (" + only
                    + ") — scoreboard NOT written");
            byFamily.forEach((f, outs) -> outs.stream()
                    .filter(o -> o.status() != Runner.Status.PASS)
                    .forEach(o -> System.out.println("[rcorpus]   " + o.status()
                            + " " + o.test() + ": " + o.detail())));
        }
        System.out.println("[rcorpus] failed seeds: " + seedFails.size());
        seedFails.forEach(f -> System.out.println("[rcorpus]   seed-fail: " + f));
        byFamily.forEach((f, outs) -> {
            long p = outs.stream().filter(o -> o.status() == Runner.Status.PASS).count();
            System.out.println("[rcorpus] " + f + ": " + p + "/" + outs.size() + " pass");
        });
        // MILESTONE 1 (H2_BACKEND.md §12 step 5): real H2 execution of
        // OUR byte-matched SQL, held to our DuckDB rows — additive
        // instrumentation; a diverged count > 0 surfaces as test FAILs.
        System.out.println("[rcorpus] h2-exec (our SQL on H2): "
                + com.legend.harness.H2Verify.M1_VERIFIED.sum()
                + " text-matched + "
                + com.legend.harness.H2Verify.M1_RESCUED.sum()
                + " text-divergent-rescued row-verified, "
                + com.legend.harness.H2Verify.M1_DIVERGED.sum() + " diverged, "
                + com.legend.harness.H2Verify.M1_UNVERIFIABLE.sum()
                + " unverifiable");
        // Per-test M1 verdict roster — UNCONDITIONAL dump (the
        // query-histogram idiom): target/h2-verdicts.txt, one sorted
        // "kind test xN" line each, so a floor move is attributable by
        // diffing two sweeps' files.
        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("target", "h2-verdicts.txt"),
                    com.legend.harness.H2Verify.VERDICT_ROSTER.entrySet()
                            .stream().sorted(java.util.Map.Entry.comparingByKey())
                            .map(e -> e.getKey() + " x" + e.getValue().sum())
                            .collect(java.util.stream.Collectors.joining("\n"))
                    + "\n");
        } catch (java.io.IOException ignore) {
            // best-effort diagnostic (histogram precedent)
        }
        // sqltext homework (2026-09-03): the per-test TEXT-VERDICT roster
        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("target",
                            "sqltext-text-verdict-roster.txt"),
                    com.legend.harness.SqlTextShapes.TEXT_VERDICT_ROSTER
                            .stream().sorted()
                            .collect(java.util.stream.Collectors
                                    .joining("\n"))
                    + "\n");
        } catch (java.io.IOException ignore) {
            // best-effort diagnostic
        }
        // SQLTEXT slice 3a: the emission census (charter §0 — text is
        // a census number, never a verdict; §8.6 turns the diff count
        // into the shrink-only emission ratchet)
        System.out.println("[rcorpus] sqltext-emission: text-matched "
                + com.legend.exec.SqlTextEmission.TEXT_MATCHED.sum()
                + ", text-diverged "
                + com.legend.exec.SqlTextEmission.TEXT_DIVERGED.sum()
                + ", text-verdict "
                + com.legend.exec.SqlTextEmission.TEXT_VERDICT.values()
                        .stream().mapToLong(
                                java.util.concurrent.atomic.LongAdder::sum)
                        .sum());
        com.legend.exec.SqlTextEmission.TEXT_VERDICT.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(),
                        a.getValue().sum()))
                .limit(20)
                .forEach(e -> System.out.println(
                        "[rcorpus] sqltext-text-verdict " + e.getValue().sum()
                                + "x " + e.getKey()));
        // step 13 registry feed: the per-reason unverifiable census
        com.legend.harness.H2Verify.UNVERIFIABLE_CENSUS.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(),
                        a.getValue().sum()))
                .forEach(e -> System.out.println(
                        "[rcorpus] h2-unverifiable-census " + e.getValue().sum()
                        + "x " + e.getKey()));
        // DECLARED-GAP REGISTRY (step 13, §9 semantics): each registered
        // H2-oracle gap has an EXPECTED count measured at registration
        // (c47 census) — GROWTH is a FAIL (silent scope creep), SHRINK
        // prints a retire hint (the row is stale, tighten it).
        if (onlyFilters.isEmpty()) {
            java.util.Map<String, Integer> registry = java.util.Map.of(
                    // forked-H2 leniency: the engine's own 2.1.214 fork
                    // relaxes duplicate result columns; stock H2 rejects.
                    // 10 -> 11 (2026-08-08, ###Mapping protocol switch): the
                    // Relation class-mapping arm of the protocol parser now
                    // reads `~func <fqn>` with no signature spelling, so one
                    // more tests/mapping/modelJoin test compiles and reaches
                    // H2 replay (that family went 42 -> 43 against the REAL
                    // checkout; re-verified by reverting to 10 and watching
                    // it fail, so this is not a stale-corpus artifact).
                    // Every census row
                    // here is a `golden execution` failure — it is ENGINE's
                    // own golden that selects a name twice, not our SQL — so
                    // this is one more instance of the registered gap, not a
                    // new one.
                    // STORY CORRECTED 2026-08-28 (probe on stock
                    // h2-2.1.214): these are NOT engine-golden defects
                    // and NO fork leniency exists — the engine's patched
                    // jar replaces only Mode/TypeInfo. The goldens alias
                    // e.g. "city" AND CITY in one subselect, legal on
                    // the engine's case-SENSITIVE session (H2Defaults);
                    // only OUR CASE_INSENSITIVE_IDENTIFIERS session
                    // collides them. The oracle now retries on
                    // H2Settings.ENGINE_CASED — rows landing here are
                    // seeds that cannot replay case-sensitively.
                    "Duplicate column name", 0,
                    // engine plan-level temp-table for IN lists — a
                    // machinery gap, not a rendering one
                    "tempTableForIn", 6,
                    // F2.3 seed (2026-08-16): the golden-SQL side
                    // channel's catch-and-null, now counted — 56 declines
                    // (the printed census truncates its bucket list; this
                    // ceiling is the assert's own full sum). Dominant
                    // buckets: array/list encodings in the golden-text
                    // dialect, toSQLString shapes, banker's ROUND,
                    // object-space TypedFilter. Shrink-only; each bucket
                    // is a REAL renderer/recognizer gap — adjudicate
                    // before raising.
                    // 56→57 (E2, 2026-08-17): testConcatenateWithFilter's
                    // new PASS rides the LEFT-LATERAL row explosion,
                    // which H2 cannot replay (no LATERAL) — one more
                    // sql-text-side decline, tied to a GAINED test
                    // 57→66 (slice 3 equality half, 2026-08-28,
                    // JUSTIFIED): sides acquire text by REAL EVALUATION;
                    // the old findTerminal returned null SILENTLY for
                    // unmatchable shapes (their failures scattered into
                    // other buckets) — every acquisition failure is now
                    // COUNTED HERE with its cause (EngineStyleH2 dialect
                    // walls: array/LIST/UNNEST/ROUND; splice shapes:
                    // sql() on non-frame receivers). RECATEGORIZATION,
                    // not lost verification: verified 320→321, rescued
                    // 632, diverged 0, unverifiable 145 — all equal or
                    // better in the same sweep, zero test regressions.
                    // 66 -> 67 (tempTable chained-replay burn
                    // 2026-08-30): a converted test's next-in-line
                    // decline surfaced (its Table-not-found row
                    // previously pre-empted the H2-vocabulary side
                    // wall) — recategorization, not lost verification.
                    // 67 -> 69 (withMapping fix 2026-08-30): the two
                    // testFromWithMapping* tests now EXECUTE (SHAPE ->
                    // PASS, +2 corpus, exec-passing +2) and surface
                    // their sql() advisory-side rows in the SAME
                    // TypedUserCall[mapping::sql] H2-vocabulary class
                    // their passing siblings already occupy —
                    // verification GAINED on the primary lane, the
                    // advisory oracle gap merely gained its two
                    // members. JUSTIFIED by exec-passing +2 in the
                    // same commit.
                    "sql-text side", 69);
            registry.forEach((needle, expected) -> {
                long got = com.legend.harness.H2Verify.UNVERIFIABLE_CENSUS
                        .entrySet().stream()
                        .filter(e -> e.getKey().contains(needle))
                        .mapToLong(e -> e.getValue().sum()).sum();
                org.junit.jupiter.api.Assertions.assertTrue(got <= expected,
                        "registered H2-oracle gap '" + needle + "' grew: "
                        + got + " > " + expected);
                if (got < expected) {
                    System.out.println("[rcorpus] registered gap '" + needle
                            + "' shrank (" + got + " < " + expected
                            + ") — retire/tighten the registry row");
                }
            });
        }
        // declaration-vs-fixture skew census (charter §4bZ): every
        // column the setup stream created with a kind contradicting the
        // ###Relational declaration — engine test-data debt, the named
        // explanation for the wire-diverge rows the deleted coercion
        // arms used to hide. Printed on every run BEFORE the gate
        // asserts (scoped iteration needs it); the count pins in the
        // full-run assert block below.
        long skewCols = Runner.FIXTURE_SKEW.values().stream()
                .mapToLong(java.util.Set::size).sum();
        System.out.println("[rcorpus] fixture-skew columns: " + skewCols);
        Runner.FIXTURE_SKEW.forEach((cls, ws) -> {
            System.out.println("[rcorpus] fixture-skew-class: "
                    + ws.size() + "x " + cls);
            ws.forEach(w -> System.out.println(
                    "[rcorpus] fixture-skew-witness: " + cls + " :: " + w));
        });
        // [1]-over-nullable-column census (typed-IR queue item 2):
        // computed per compile at the platform's DeclaredCoercions
        // pairing seam, aggregated by the harness — the unchecked
        // "[1]-property => NOT NULL column" implication, per bucket
        // with honesty buckets for unadjudicated arms. Census NOT
        // warning (engine fixtures fire it wholesale; the diagnostic
        // waits for the dialect split); quantifies model debt inside
        // the 925 wire-breach census. Count pins in the full-run
        // assert block below.
        System.out.println("[rcorpus] required-over-nullable pairings: "
                + reqNullAdjudicated());
        Runner.REQUIRED_OVER_NULLABLE.forEach((bucket, ws) -> {
            System.out.println("[rcorpus] required-over-nullable "
                    + bucket + ": " + ws.size());
            ws.forEach(w -> System.out.println(
                    "[rcorpus] required-over-nullable-witness: "
                            + bucket + " :: " + w));
        });
        // M1 GATE PINNING (H2_BACKEND.md §12 step 13): on a FULL sweep,
        // any divergence fails the build (they already FAIL per-test —
        // this pins the aggregate against silent scoring drift), and the
        // verified count must hold its floor (289 at c43, 296 after the
        // c46 enum-decode rung, 309 after slice 10's engine-text NULLS
        // suppression — 13 rows had diverged from golden text only by a
        // nulls clause; ratchet on deliberate gains). 309→320
        // (2026-08-20 stamp C2-i): provably-single cell reads lower as
        // PLAIN scalar subqueries — 11 more texts byte-match.
        // DIAGNOSTICS BEFORE VERDICTS (the program's first-failure rule):
        // the canon/v7 census prints precede the lane-guard asserts — a
        // tripped guard must never hide the very numbers that diagnose it.
        System.out.println("[canon] " + com.legend.exec.CanonicalDivergence.summary());
        // the ALARM witnesses print first from their reserved buffer —
        // never lost to shared-sample crowding
        com.legend.exec.CanonicalDivergence.sqlDisagreeSamples().forEach(r ->
                System.out.println("[canon] ALARM " + r.family() + " "
                        + r.detail()));
        com.legend.exec.CanonicalDivergence.disagreeSamples().forEach(r ->
                System.out.println("[canon-disagree] " + r.family() + " "
                        // canon keys embed kind text — the NUL
                        // poisons grep/console (this hid the payload
                        // twice); every row prints as ONE clean line
                        + r.detail().replace("\n", "\\n")
                                .replace('\u0000', '#')));
        com.legend.exec.CanonicalDivergence.samples().forEach(r ->
                System.out.println("[canon] " + r.family() + " " + r.detail()));
        System.out.println("[v7] "
                + com.legend.exec.CanonicalDivergence.v7Summary());
        com.legend.exec.CanonicalDivergence.v7Report().forEach(l ->
                System.out.println("[v7] " + l));
        if (onlyFilters.isEmpty()) {
            org.junit.jupiter.api.Assertions.assertEquals(0,
                    com.legend.harness.H2Verify.M1_DIVERGED.sum(),
                    "M1 h2-exec divergences on a full sweep");
            // 320 -> 436 (diff-noreplay burndown 2026-08-28): Graph
            // frames replay — byte-matched goldens of class-mapped
            // queries now row-verify instead of declining non-tabular
            org.junit.jupiter.api.Assertions.assertTrue(
                    // 455 -> 373 floor (charter §8.3b, the 308-test
                    // assertSameSQL migration): the flipped tests'
                    // text-matched verifies LEFT the walk's M1 lane for
                    // the platform arm's row verdicts (emission census
                    // 111 matched + 237 diverged) — walk-lane shrink BY
                    // MIGRATION, never lost verification.
                    // 373 -> 134 (charter §8.3c, the 541-test
                    // exec-sql-read migration): same lane move at 1.75x
                    // the scale — flipped tests verify via the oracle
                    // SPI (sql-verdict agree=1712, disagree=0) instead
                    // of the walk's M1 channel.
                    // 134 -> 85 (charter §8.3d, +199 dual-golden
                    // flips): same oracle-SPI lane move.
                    // 85 -> 83 (effectful cutover, +50 flips): the
                    // transactional flip attempt replaced the static
                    // verb gate — the 44 flipped sql-asserts row-verify
                    // via the arm rows leg, which calls the SAME
                    // machinery (ReplayOracle.verify ->
                    // H2Verify.compareFrame) through the SPI. RECEIPT
                    // (audited): the text-verdict decline census is
                    // byte-identical across the flip (102 = 102) — none
                    // of the 44 became a text verdict or a decline, and
                    // since 3e a declined rows leg cannot flip, so
                    // rows=pass is the only path these took. Their data
                    // is ALSO platform-judged by each test's own value
                    // asserts (expected-TDS vs actual rows).
                    // 83 -> 82 (metamodel-as-relations group F burn,
                    // 2026-09-02): testSubTypeMappingValidWhenMappedExplicitly
                    // flipped to the platform (its _classMappingByClass is a
                    // Pure body over the store) — its assertSameSQL now
                    // row-verifies through the oracle SPI like every other
                    // flipped sql-assert (lane move, not lost verification).
                    // 82 -> 54 (activities AS ROWS, 2026-09-03): the
                    // aggregationAware / routing / union / tds families'
                    // sql($result) reads are navigation over activity rows
                    // now, so those tests flipped whole — their sql-asserts
                    // row-verify through the oracle SPI like every other
                    // flipped sql-assert (lane move, not lost verification;
                    // +65 flips, 0 lost).
                    // 54 -> 22 (batch 26 — the referee's SQL render is the
                    // FRAME's own assembled chain, 2026-09-03): the caller's
                    // lets (a let-bound milestoning date) fold into the render
                    // as they do into the run, so the milestoning family's
                    // sql-asserts row-verify as platform-arm verdicts (lane
                    // move; +76 flips, 0 lost)
                    // 22 -> 20 (batch 29, post-processors): the replaceTables
                    // tests' sql-asserts left the walk's lane (lane move)
                    // 20 -> 12 (batch 34, the assertSameSQL String overload
                    // takes the exec-read arm): 15 flipped tests' sql-asserts
                    // left the walk's lane (lane move, 0 lost, disagree 0)
                    // 12 -> 9 (batch 37, the text-policy gate deleted): the
                    // 36 flipped tests' sql-asserts left the walk's lane
                    // (lane move, 0 lost, disagree 0, passes 2374 -> 2375)
                    // 9 -> 4 (batch 38): the 16 flipped tests' sql-asserts
                    // left the walk's lane (lane move, 0 lost, disagree 0,
                    // passes 2377 stable)
                    // 4 -> 1 (batch 41): the flipped projection tests' text-
                    // matched sql-asserts left the walk's lane (lane move)
                    // 1 -> 0 (batch 56, 2026-09-04): the LAST walk-lane
                    // test (testLessThanFilterAsVariable — a let-bound
                    // lambda in filter position) flipped to the platform
                    // arm; its sql-assert row-verifies through the oracle
                    // SPI (sql-verdict agree +1, disagree 0). The walk's
                    // M1 lane is RETIRED: pinned EXACTLY EMPTY — a test
                    // re-entering it is a regression to the walk.
                    com.legend.harness.H2Verify.M1_VERIFIED.sum() == 0,
                    "M1 h2-exec walk lane is retired (pinned empty), got "
                    + com.legend.harness.H2Verify.M1_VERIFIED.sum());
            // V7 §8.0 leg 0 — the LANE-CLASSIFICATION GUARD (charter
            // scope table, user-ratified 2026-08-28): the sql-text/TDG
            // partition counts pin EXACTLY — an assert can never change
            // lanes silently; a corpus change that moves these updates
            // the pin AND the charter table in the same commit.
            // 961 -> 1529 (§8 leg 4 census split): content-based
            // classification — an assert whose args pull a sql-producer
            // call is a plan-text compare whatever its form name.
            // CONFIRMED at 1529 by the task-#13 slice-2 rewire: the
            // RESOLUTION-backed classifier (exact FQNs, resolvesTo)
            // reproduces this count exactly — the name-sniffing
            // deletion moved the mechanism, not one row.
            // The user-ratified OUTCOME buckets (2026-08-28), measured
            // then pinned EXACTLY (sum = the ratified 1529+123; csv lost
            // its 6 plan-let rows to text-only — the old tdg reason
            // conflated them). exec-passing may only GROW by burndown;
            // UNABLE-TO-EXEC (esp. diff-noreplay 321, the weakest class:
            // text DIFFERS and no replay ran) may only SHRINK.
            // 989 -> 990 (equality half: +1 text-match row-verified)
            // 990 -> 1276 (diff-noreplay burndown 2026-08-28, charter
            // §4AB): GRAPH frames row-verify (goldenGraphCompare — the
            // instance array the database built compares by LABEL
            // against the golden's data aliases; pk_$i/k_* bookkeeping
            // excluded by the engine's own spelling) + the microsecond
            // temporal floor (DuckDB storage precision). 286 rows
            // converted, every one a REAL golden-vs-ours row compare.
            // 1276 -> 1385 (slices 2-4 same day): union/milestoning
            // bookkeeping aliases + frame-side context echo + empty
            // frame row-count verdict (+84); per-key enum decode for
            // class frames (+14); case-collision goldens retried on
            // the engine's own casing, H2Settings.ENGINE_CASED (+11).
            // 1385 -> 1387 (§4AD slice 1 batch 1, value-position
            // fan-out): testQualifierWithIsolationXX +
            // testChainedInnerJoinsWithQualifierInGroupBy row-verify
            // against the engine goldens; 2 more UPGRADED
            // rescued -> byte-matched (testQualifierWithVariableArg ×2).
            // 1387 -> 1396 (§4AD batch 5, THE ROUTER FLIP): +9
            // row-verified — testQualifierWithIsolation (a baseline
            // ERROR, the topology round's predicted win), the
            // filter-mapping overlap pair, and six wrapper/hop-rich
            // projection qualifiers (first()/head() unwrap +
            // per-occurrence bundling), all via the one-owner router.
            // 1396 -> 1448 (§4AD P0.5, THE CORRECTED ORACLE UNPARK —
            // NAV_ROUTING_PLACEMENT_ADDENDUM_4AD): Collection/Scalar
            // verify lane ON, golden rows flattened at the VALUE
            // observable (H2Verify.goldenRowsCompare receipt). The lane
            // is NOT clean: testQualifierWithOperation +
            // testTwoQualifiersWithOperation FAIL as NAMED DEFECTS
            // (batch-5 placement defect, tests/advanced baseline 66 ->
            // 64) — burned by P1, which restores 66. Never re-parked,
            // never re-adjudicated.
            // 1448 -> 1449 (§4AD task #72, strict-read hoist): the
            // formerly-WALLED testInputNotIsolatedWhenPropertyPathIsToOne
            // now executes — its sql-text assert row-verifies (our
            // presence spelling vs the engine's hoisted-pred text).
            // THE DUAL-CHANNEL DISAGREEMENTS — 9 -> 1 (user-ordered
            // adjudication 2026-08-31, each row to a verdict; full
            // record in VERDICT_DISAGREEMENT_BURN_2026_08_30.md §FINAL).
            // 8 were PLATFORM FIXES, not designed splits:
            // - Decimal x4 + temporal x2 (dataType tests): the engine
            //   DECODES wire cells before its strict equality ever runs
            //   (scale erased, subseconds stamped to NINE digits —
            //   receipts R3/R5/R8); we had ported the strict equality
            //   without the decode. Fixed by VALUE-LANE wire-cell
            //   egress conformance (LiteralSpelling.wireValueEgress +
            //   the AssertVerdicts.valueRead host twin — both verdict
            //   channels see the SAME decoded value; TDS raw renders
            //   keep driver spellings, R6).
            // - testDayOfMonth: the same decode at the TDSRow.values
            //   read, PLUS a literal-fidelity bug (nine-digit-written
            //   DateTime literals truncated to six by the %f strftime
            //   round-trip; engine sources spell NINE — several tests
            //   had passed by both sides truncating identically).
            // - testDeepUnionOperation...: the deterministic scan-order
            //   key (ScanOrder, user design 2026-08-29) applied only at
            //   statement ROOTS; production compiles the asserted
            //   relation as a SUBQUERY and missed it. StableScanOrder
            //   now stabilizes FROM-subselect inners too.
            // ALL NINE BURNED — DISAGREE = 0 EXACT (leg 3, same day; 3
            // consecutive byte-identical sweeps). The final three:
            // - populated milestoning dates spell their WRITTEN text on
            //   the map-channel value lanes (literalTextOk — the
            //   engine's population constant is a string; the scalar
            //   Any-pair root keeps its TIMESTAMP carrier, the earlier
            //   blanket form severed its literal channel);
            // - order-sensitive aggregation over a sorted/scanned input
            //   aggregates in H2's own order under ENGINE-COMPAT
            //   (StableScanOrder: user sort keys first, then base-scan
            //   rowids probe-major, ordinals threaded through plain
            //   frames and union legs; the platform itself stays
            //   order-honest — user ruling: replay determinism is
            //   flag-gated, never language semantics);
            // - two renders of UNSORTED queries compare as line
            //   MULTISETS (renderedArm's both-rendered arm — pure
            //   guarantees the row multiset; the toCSV-vs-toCSV pair
            //   flickered run-to-run under the byte compare).
            // EXACT ZERO: any disagreement is a platform bug or a new
            // divergence class — adjudicate, never re-pin upward.
            org.junit.jupiter.api.Assertions.assertEquals(0,
                    com.legend.exec.CanonicalDivergence.v7DisagreeCount(),
                    "dual-channel disagreement appeared (pinned ZERO) —"
                            + " see VERDICT_DISAGREEMENT_BURN_2026_08_30");
            // 1449 -> 1459 (sql-exec burn 2026-08-30, the STITCH-KEY
            // rule): 10 graph-keys declines now EXECUTE and row-verify
            // (golden-only assembly aliases drop, counted on the
            // verdict roster; frame-side strictness unchanged).
            // 1459 -> 1467 (enum include-traversal burn 2026-08-30):
            // the 7-8 enum-decode declines now decode via the INCLUDED
            // mapping's EnumerationMapping and row-verify.
            // 1467 -> 1475 (tempTable chained replay): 8 more asserts
            // execute and row-verify (the synthesized
            // tempTableForIn_<var>/<n> tables replay the engine's
            // runtime artifacts from derivable values).
            // 1475 -> 1495 (TDG 49er replay): 20 generateTestData sqls
            // rows verify by execution-equivalence (golden fetch on the
            // H2 mirror vs ours on DuckDB, multiset row compare under
            // the shared canon) — same rows that left the unable-to-exec
            // lane (70 -> 50 above).
            // 1495 -> 1497 (withMapping fix 2026-08-30, §4AF): the two
            // testFromWithMapping* tests flip SHAPE -> PASS (+2 corpus,
            // agree +2) and their golden-SQL asserts EXECUTE and
            // row-verify — verification gained, first real burn row of
            // the §4AF program.
            // 1497 -> 1526 (TDG chained-fetch live-session refereeing,
            // census §10o leg 1 / TDG charter §S5 landing): the whole TDG
            // unable residue (29) executes and row-verifies — 26 chained
            // hops (OUR side = the generator's live-session per-fetch
            // transcript, engine-parity Result.fetches; GOLDEN side = the
            // mirror with ancestor testDataGen_Temp_* tables synthesized
            // from the test's own earlier goldens, root-first), 2
            // concatenate rows (REAL platform bug fixed: the no-join
            // tableToTDS concatenate arm missed the engine's
            // reOrderAndMergeRelationTree root sort), and the
            // testQualifier H2Compatible hop-0 (golden extraction sees
            // through sqlRemoveFormatting('literal'), evaluated AS
            // WRITTEN through the platform).
            // 1526 -> 1527 (2026-08-31 resolver-bug-4 witness #4,
            // testUnionToUnionJoinSequenceWithMultipleChildrenInUnion-
            // SourceTree): the hoisted-filter source-slot demand fix
            // makes the test execute; its assertSameSQL golden
            // exec-passes.
            // 1527 -> 1528 (row-13 adjudication burn, SQLTEXT charter
            // §6.1 2026-09-01): the skew decline was masking NO bug —
            // testQualifierQueryWithOr's golden fans out 7x the same
            // pk-stamped instance (engine one-object-per-row algebra,
            // its asserts pin nothing); the graph compare's
            // EXTENT_SUBSET pk-collapse renders the real row verdict
            // and the test exec-passes.
            // 1528 -> 1209 (charter §8.3b): 308 assertSameSQL tests
            // flipped — their exec-passing asserts left the walk's
            // classification for the platform arm (ratchet 1688/885,
            // corpus byte-stable 2348, clean passes 1401 -> 1627).
            // EXACT again (§8.3b wobble burn): the 3a-era ±1 was
            // adjudicated — the only REPRODUCIBLE run variance was
            // canon row-order drift (fixed at probeGridText); the flip
            // roster (target/wholetest-flipped.txt) was byte-identical
            // across 6 same-tree sweeps, so the ±1 here was tree drift
            // between mid-cascade measurement runs. Any future move
            // fails loudly and names itself via roster diff + the
            // [ratchet] print.
            long execPassing = com.legend.exec.CanonicalDivergence
                    .v7DeclinedByReasonPrefix(
                            "assert-sql-text-with-exec-passing");
            System.out.println("[ratchet] flipped="
                    + com.legend.harness.WholeTestFlip.flippedCount()
                    + " fallbacks="
                    + com.legend.harness.WholeTestFlip.fallbackCount()
                    + " exec-passing=" + execPassing);
            // 345 -> 344 (metamodel-as-relations group F burn, 2026-09-02):
            // testSubTypeMappingValidWhenMappedExplicitly flipped — its
            // assertSameSQL row-verifies via the platform arm (charter
            // §8.3 lane move; the M1 floor moved 83 -> 82 with it).
            // 344 -> 275 (activities AS ROWS, 2026-09-03): 67 tests flipped
            // whole (aggregationAware, routing, union, tds groupBy ...) —
            // their sql()/sqlRemoveFormatting asserts left the walk's
            // lane for the platform arm (the same lane move as §8.3b-d;
            // the M1 floor moved 82 -> 54 with it).
            // 275 -> 198 (batch 26 — the referee's render is the frame's
            // chain): the 76 flipped tests' sql-asserts left the walk's lane
            // for the platform arm (the same lane move as M1 54 -> 22).
            // 198 -> 180 (batch 27 — render coverage: the chain's own
            // mapping, literal in-lists in the H2 spelling): the 18 flipped
            // tests' sql-asserts left the walk's lane (lane move, 0 lost).
            // 180 -> 171 (batch 30 — effectful helper values): the forced-
            // milestoning tests' sql-asserts left the walk's lane (lane move).
            // 171 -> 170 (batch 31 — the validate desugar in the platform
            // path): one businessdate sql-assert left the walk's lane.
            // 170 -> 167 (batch 32 — plan-execute frames): three TDG
            // sql-asserts left the walk's lane (lane move).
            // 167 -> 149 (batch 34 — assertSameSQL(String, String) takes the
            // exec-read arm): the 15 flipped tests' assertSameSQL asserts
            // left the walk's lane (lane move, passes 2374 stable).
            // 149 -> 140 (batch 35 — engine-style H2 literal-reduction /
            // round spellings): the flipped tests' sql-asserts left the
            // walk's lane (lane move, passes 2374 stable, disagree 0).
            // 140 -> 135 (batch 36 — percentile reducer): the three
            // percentile tests' sql-asserts left the walk's lane (lane
            // move, passes 2374 stable, disagree 0).
            // 135 -> 99 (batch 37 — the text-policy gate deleted): the 36
            // flipped tests' sql-asserts left the walk's lane (lane move,
            // passes 2374 -> 2375, disagree 0).
            // 99 -> 82 (batch 38 — enum decode / join lambda / getters):
            // the flipped tests' sql-asserts left the walk's lane (lane
            // move, passes 2377 stable, disagree 0).
            // 82 -> 79 (batch 39 — lateral explode / plan replay): the
            // flipped tests' sql-asserts left the walk's lane (lane move,
            // passes 2377 -> 2378, disagree 0).
            // 79 -> 76 (batch 41): the flipped projection tests' sql-asserts
            // left the walk's lane (lane move, passes 2378 -> 2379).
            // 76 -> 75 (batch 42): the flipped class-query sql-assert left
            // the walk's lane (lane move).
            // 75 -> 68 (batch 43 — the referee render runs the H2 carrier
            // strategies): the flipped tests' sql-asserts left the walk's
            // lane (lane move, passes 2379 stable, disagree 0).
            // 68 -> 63 (batch 44 — no-decision singles): the flipped TDG
            // tableToTds tests' sql-asserts left the walk's lane (lane
            // move, disagree 0).
            // 63 -> 61 (batch 45): the flipped null-cell tests' sql-asserts
            // left the walk's lane (lane move, disagree 0).
            // 61 -> 60 (batch 46): the flipped exists test's sql-assert left
            // the walk's lane (lane move, disagree 0).
            // 60 -> 59 (batch 50): the flipped user-defined-date-format
            // test's sql-assert left the walk's lane (lane move, disagree 0).
            // 59 -> 58 (batch 53): the flipped routing composition test's
            // sql-assert left the walk's lane (its let-bound
            // getNames()->at(0) is a literal-collection fold; lane move,
            // disagree 0).
            // 58 -> 57 (batch 56, 2026-09-04): the flipped let-bound-lambda
            // filter test (testLessThanFilterAsVariable) left the walk's
            // lane — the walk's M1 text-match lane is now EMPTY (pinned
            // retired above); its sql-assert row-verifies through the
            // oracle SPI (sql-verdict agree +1, disagree 0).
            // 57 -> 55 (batch 57, 2026-09-04): the two flipped hybrid-
            // milestoning union tests (repeat native) left the walk's lane —
            // their sql-asserts row-verify through the oracle SPI
            // (sql-verdict agree +4, disagree 0; M1 rescued 54 -> 52).
            // 55 -> 21 (batch 64, 2026-09-04): the ten chained
            // testDataGeneration tests left the walk's lane — the platform
            // arm's chained-fetch verdict (SqlReplayOracle.verifyFetchChain:
            // ancestor temps materialized from the earlier hops' goldens,
            // the hop's transcript rows multiset-compared) replaced the
            // walk's tdgChainedVerify for them; their 34 fetch-text asserts
            // are ROW verdicts now (sql-verdict disagree 0; dual-channel
            // disagree 0). The walk's exec-passing lane keeps 21.
            // 21 -> 17 (batch 65, 2026-09-04): the four inline in-list
            // temp-table tests left the walk's lane — the platform arm
            // hands the oracle the query's in([...]) literal as a
            // TempTable spec (SqlReplayOracle.verify overload) and the
            // oracle materializes tempTableForIn_N before the replay; their
            // sql-asserts are ROW verdicts (disagree 0).
            // 17 -> 14 (batch 66, 2026-09-05): the eleventh chained TDG test
            // (testQualifier — its hop-0 golden spelled through the String
            // sqlRemoveFormatting) and two multi-node PLAN tests
            // (testMapWithOpenVariable, testExecutionPlanForQueryWithVariable-
            // RundateWithinLambda — Allocation nodes replayed on the oracle,
            // holes filled, the final SQL's rows compared) left the walk's
            // lane for platform-arm ROW verdicts (disagree 0).
            // 14 -> 12 (batch 67, 2026-09-05): the two in-list query-chaining
            // tests left the walk's lane — golden(0) verifies as the rows of
            // the let it populates, golden(1) through tempTableForIn_<let>
            // filled from golden(0)'s rows at the oracle (disagree 0).
            // 12 -> 10 (batch 69a, 2026-09-05): the forced-isolation pair
            // (testQualifierWithOperation, testTwoQualifiersWithOperation)
            // left the walk's exec-passing lane — the referee's value-frame
            // guard is gone, the forced golden's 4 rows ('PeterTest' + three
            // 'Test', pure's plus over an empty operand) row-diverge from
            // our 1-row INNER-joined frame: an honest divergence of OURS.
            // 10 -> 9 (batch 69c): the datePeriods group-by's exec-passing
            // assert (its CSV value assert ran beside the text declines)
            // left the walk's lane — the whole test flipped
            // 9 -> 7 (batch 72a, 2026-09-05): testBusinessDateInjection-
            // FromVarReference's two assertSameSQL asserts left the walk's
            // lane — its statement-root map over the two execute bindings
            // unrolls (LiteralMapUnroll) and the whole test flipped
            org.junit.jupiter.api.Assertions.assertEquals(7, execPassing,
                    // 1208 -> 597 (charter §8.3c): the 541 flipped
                    // exec-sql-read tests' asserts left this lane for
                    // the platform arm (SqlTextVerdicts.tryArmExecRead)
                    // 597 -> 389 (§8.3d): the dual-golden cohort's
                    // asserts likewise
                    // 389 -> 345 (effectful cutover): the transactional
                    // flip attempt let the old "effectful" bucket's
                    // bodies flip — 44 exec-passing asserts left the
                    // walk lane for the arm channel (emission +44).
                    "lane guard: assert-sql-text-with-exec-passing moved —"
                            + " update the charter §8.0 scope table");
            // THE WHOLE-TEST MIGRATION RATCHET (harness-deletion item 1,
            // default-on 2026-08-31): flipped tests score from the
            // platform's assert verdicts; every fallback carries a
            // counted reason (target/wholetest-flip-fallbacks.txt).
            // Fallbacks only SHRINK (each burn moves tests to the
            // platform); flipped only GROWS. The walk deletes at
            // fallbacks=0 (charter: WHOLETEST_COMPILATION_CHARTER.md).
            // 2156/417 -> 2155/418 (TDSNull membership burn: a
            // ^TDSNull()-typed contains needle is an IS NULL scan).
            // 2155/418 -> 2151/422 (bind-once slice 1, families D+E:
            // let-bound mapping/runtime/class refs resolve through the
            // Env alias channel at from()/getAll(); TDG/CSV-census args
            // resolve via SourceSubst.resolveStructuralArgs — refs and
            // lambdas only, computed refs keep their variable).
            // 2151/422 -> 2054/519 (bind-once slice 2, family A: a let
            // whose rhs is a DEFERRED kind — graph-fetch tree literal,
            // mapped/agg colspec — PARKS the raw syntax in the Env alias
            // channel instead of dying; graphFetch/serialize resolve
            // through the alias at their call sites, each use its own
            // independent resolution).
            // 2054/519 -> 2052/521 (bind-once family B: the
            // mayExecuteAlloyTest/mayExecuteLegendTest BRANCH natives
            // registered with their engine signatures and folded to the
            // no-server fallback thunk at the checker (MayExecuteChecker
            // — walk parity: alloyFallback); bare-lambda buckets 27+4
            // burn to 0: 2 vacuous flips (engine serverless-CI parity),
            // 25 correctly reclassified into the parked effectful lane,
            // 4 non-let thunk intermediates now type as expression
            // statements).
            // 2052/521 -> 2027/546 (bind-once slice 4a, lineage lane:
            // scanRelations/relationTreeAsString platform-owned — the
            // native com.legend.lineage.ScanRelations is the
            // implementation (walk parity: LineageRelationsForm verifies
            // the same native); the scan call captures a carrier, the
            // tree-string consumer FOLDS it at check time. 46 of the 97
            // FunctionDefinition-metaprogram rows burn; the remaining 51
            // (pkOfFunc reflection chains 43, scanColumns reflection
            // chains 6, residue 2) are the RATIFIED metamodel-as-data
            // quarantine — LineageForm's own B3-route doc records why a
            // form bridge beats hollow platform vocabulary there).
            // 2027/546 -> 2040/533 NET (2026-09-01, USER RULING — the
            // metamodel revert): THREE mechanisms were reverted after
            // shipping. (1) plan-chain staging (+53: planWalk results
            // consumed as staged literals) — consuming the Java
            // metamodel walk in the verdict path INSTITUTIONALIZES an
            // eviction-listed evaluator; the quarantine covers
            // CONSUMPTION, not just vocabulary growth. (2+3) the
            // scanRelations platform-owning + scan-tree referee (+46:
            // bind-once slice 4a) — same judgment one level up: the
            // lineage Java analyzer is ITSELF a parallel implementation
            // (of the engine's scanRelations analysis AND of the
            // resolver's own join-demand), hardcoded to one call shape;
            // promoting it to platform blessed a duplicate. RATIFIED
            // END-STATE for the whole family: metamodel/lineage/plan
            // facts belong IN THE DATABASE as relations (the
            // Class.all() route — adjacency-list trees, recursive CTEs,
            // metamodel classes mapped onto metamodel tables), computed
            // as a resolver side-output; pure-over-metamodel then
            // lowers to SQL like everything else. Until that program
            // lands, these tests stay walk-scored fallbacks. What
            // STANDS from the batch: the effectful cutover (+12,
            // re-run-safety routing — no evaluation anywhere). The
            // one-time flipped-count decrease is this recorded ruling,
            // never a silent regression.
            // 2040/533 -> 1997/576 (SQLTEXT charter slice 3a,
            // 2026-09-01): the tosqlstring-simple cohort flips — 43-44
            // tests whose every sql assert is the
            // assertEquals(golden, toSQLString({|q}, mapping, dialect))
            // shape now score from the platform's SqlTextVerdicts arm
            // (rows are the verdict; text is the emission census:
            // 18 matched + 7 diverged + 19 text-verdict residue).
            // Corpus +5 (2343 -> 2348: transform/fromPure +4,
            // tds/tests +1 — old text-strict failures whose ROWS agree).
            // 1997/576 -> 1689/884 (charter §8.3b): the
            // assertsamesql-simple cohort — 308 tests in one sweep.
            // EXACT 1688/885 (§8.3b wobble burn): the "admission
            // wobble" never reproduced on a frozen tree — the flip
            // roster was byte-identical across 6 consecutive sweeps;
            // the earlier 1996/577-vs-1997/576 pair straddled harness
            // edits. Exact pins + the [ratchet] print + roster diffs
            // make any real move loud and self-attributing; the
            // migration direction is enforced by attribution (every
            // move lands with its burn in the same commit).
            // 1688/885 -> 1147/1426 (charter §8.3c): the
            // execsqlread-simple cohort — 541 tests in one flip, the
            // single biggest migration of the program.
            // 1147/1426 -> 948/1625 (charter §8.3d): +199
            // dual-golden flips (both actual spellings owned).
            // 948/1625 -> 945/1628 (charter §5 first cut): the
            // plan-text arm — 3 .sqlQuery-navigation tests flip
            // (referee-bound holes, filled-golden replay); whole-plan
            // planToString compares with freemarker-operation holes
            // demote COUNTED (the full-program replayer's future
            // work), walk keeps scoring them.
            // 928/1645 -> 878/1695 (EFFECTFUL CUTOVER — the burn-map
            // item, gate deleted): every effect-bearing body now
            // executes inside a TRANSACTION on the session connection
            // (commit only after the verdict stream passes; rollback +
            // ledger truncate + mirror-detach-if-ahead on any failure
            // exit, so the walk's fallback re-run starts pristine).
            // The static verb classification (effectKind verbs +
            // collectExecInDb, ~90 harness lines) DELETED — re-run
            // safety is a mechanism property now, not a SQL-text scan,
            // so computed-SQL bodies flip: +50 (42 modelJoin whose
            // text-rescue softness CLEARED + 8 TDG/misc). The 82
            // "effectful" rows re-bucketed to TRUE walls: 28
            // generateTestData unclassified-Variable + named
            // singletons; 4 wall-type rows re-spelled wall-exec (the
            // gate's eager typeQueryBody is gone — same failures,
            // surfaced at execution). mirror-detaches=0,
            // rollback-failures=0 on the measuring sweep.
            // 945/1628 -> 943/1630 (TDG scoring flip, first slice):
            // the fetch-text arm (SPI verifyFetchTexts) + the
            // classifier's actual-side fix (assertSize's literal arg
            // had been mispicked); the TDG cohort's remaining tests
            // re-bucketed to their TRUE walls (execute/3 spelling,
            // Any-property reads) — named compiler gaps now, not
            // text policy (text-policy 118 -> 65).
            // 943/1630 -> 928/1645 (the "Any-property" wall was OUR
            // OWN catalog: three widened native spellings corrected to
            // the REAL pure declarations — RowIdentifier
            // .columnValuePairs Pair<String,Any>[*], Table.schema
            // Schema[1], Schema.name String[1]; classDef is
            // native-first, so the widenings shadowed the corpus's own
            // classes and walled every $cv.first / $t.schema.name read
            // (46 tests, one census shape). Movement: 15 alloy-TDG
            // flips (corpus +1), remainder re-bucketed to TRUE walls —
            // 25 effectful, 3 generateTestData unclassified-Variable,
            // 3 planTestDataGeneration Pair-arg typing. No dynamic
            // Any-property checker exists or is wanted.
            // 878/1695 -> 876/1697 (TDG let-adoption): the 31
            // "unclassified argument Variable" walls burned to ZERO —
            // resolveStructuralArgs adopts the TDG data-constructor
            // shapes DEEP (tdgCtorShape; inner args may be let-bound
            // too). +2 flips; the rest re-bucketed TRUE: 15 scalar-
            // lowering (the known 51-row lane), 10 plan-execute
            // values-binding (the chartered referee-binding cut), 3
            // REAL platform divergences (testConstant cohort — the
            // burn list), 1 store-resolution.
            // 876/1697 -> 871/1702 (the foundation probe, docs/
            // PLATFORM_FAIL_ADJUDICATION_2026_09_01.md). TWO burns, both
            // roster-attributed: (1) the MAPPING-SEAM WINDOW RULE —
            // a window inside a Relation ~func class extent is an
            // evaluation boundary (ClassSources.sealExtentWindows +
            // Lowerer.extentBoundary); the class filter had folded INTO
            // the ~func's window select (testMappingWithWindowColumn:
            // John ranked 1st, golden 2nd) — +1 flip, corpus 2349 ->
            // 2350. (2) ONE TEST CLOCK — the engine runs its test JVM
            // under -Duser.timezone=GMT; ours ran the H2 replay oracle
            // on the machine's LOCAL zone against DuckDB's UTC, so the
            // five dateDiff(..., now()) goldens (sqlstring
            // testGenerateDateDiffExpressionForH2ForDifferenceIn*)
            // row-diverged by the zone offset — and the DAYS one only
            // AFTER 21:00 local, which is the ±1 wobble that made
            // HEAD read 877/1696 against this pin in two same-tree
            // evening sweeps. Root pom surefire argLine pins GMT — and
            // because the oracle replays a golden LATER than we executed
            // (two instants), a projected datediff-to-now still raced at
            // unit boundaries under GMT (HOURS/MINUTES each dropped once
            // in five sweeps), so H2Verify.compareFrame declines it BY
            // NAME and §3.7's text contract flips it deterministically
            // (byte-identical spelling). +5 flips, the ROW-verdict
            // diverged bucket 10 -> 5, oracle verdict roster
            // byte-identical, exec-passing unchanged, paired sweeps
            // byte-identical on all three rosters.
            // 871/1702 -> 848/1725 (legacy TDS join, let-bound JoinType):
            // JoinChecker.resolveLetBoundArgs chases `let type =
            // JoinType.X; ->join(tds, $type, {a,b|get*})` through the ONE
            // alias channel so the legacy desugar fires (the 23-test
            // tdsJoin cohort walled "unknown function 'getInteger'" —
            // the census had misfiled it as an unported native; the
            // getters existed all along). +23 flips, oracle roster
            // byte-identical. Residue 2, named: a let-bound
            // {a:TDSRow[1],b:TDSRow[1]|...} condition walls at its own
            // let (deferred-kind candidate, bind-once charter).
            // 848/1725 EXACT again (TDG arm reach, zero net flips —
            // honest movement): the generator carrier is FOLDED to a
            // TestDataGenResult literal per statement by the time the
            // verdict layer looks, and the corpus's assertSqlEquals
            // inlines to assertEquals over sqlRemoveFormatting on BOTH
            // sides, so hasTdgProducer never saw a producer and the
            // exec-read arm claimed the string read as a Result frame —
            // the 29-test TDG cohort fell through to the scalar lowerer.
            // SqlTextVerdicts recognizes the folded literal and routes
            // TDG first: "no scalar lowering" 66 -> 51, the 3 dialect
            // TOP/LIMIT platform-fails dissolve, 12 rows now decline BY
            // NAME (chained fetch — generator temp tables not
            // replayable; the walk's own compile-time decline, text is
            // the contract) and 7 join plan-execute values-binding.
            // mirror-detaches 0 -> 1, deterministic across three sweeps:
            // the arm replays on the mirror, the attempt then fails on
            // the text contract, rollback detaches the ahead-running
            // mirror — failure-path hygiene by design.
            // 848/1725 -> 847/1726 (navigation-depth leg, 2026-09-02):
            // DOTTED emptiness ($x.a.b->exists(...)) now registers inside
            // NESTED scopes exactly as at the root (DottedExists — a
            // scope is a scope), the materializer walks limit/sort/join
            // wrappers above navigate slots, and nav tails ride through
            // the association branch of flattenSource. +1 flip:
            // testNestedExistsWithExistsInAbstractProperty (was wall-exec
            // "exists/forAll predicate references column '_'"). H2
            // verdict roster byte-identical, exec-passing unchanged,
            // paired sweeps byte-identical on all four rosters;
            // quarantine 172/20 and extends 23/23 unchanged. DuckDB
            // driver re-pinned 1.5.0.0 -> 1.4.4.0 in the same batch
            // (upstream LIMIT-in-derived-table defect, root pom): the
            // corpus is INDIFFERENT to it (same rosters).
            // 847/1726 -> 841/1732 (metamodel-as-relations batch 5,
            // 2026-09-02, the REAL-NAME switch): classMappingById /
            // mainTable / superMapping / allSuperSetImplementations /
            // resolvePrimaryKey are Pure bodies over the metamodel store
            // (extends chain + compiled primary keys + columns seeded as
            // rows; declared per-set key facts stamped at Phase E); the
            // natives and the MetamodelWalk/MetamodelSteps arms are gone.
            // +6 flips = the six extends tests the walk used to score
            // (testMainTableForB1/B2/C1/C2, testSuperSetIdsAreCollected,
            // testPrimaryKeyForB), all "no scalar lowering for resolved
            // overload" before. H2 verdict roster byte-identical, exec-
            // passing unchanged, paired sweeps byte-identical on all four
            // rosters; quarantine witness rows 172 -> 151 (the
            // classMappingById refusal spelling is DEAD), walls 20.
            // 841/1732 -> 820/1753 (harness burn-down batch 7 — GROUP F,
            // 2026-09-02): rootClassMappingByClass / _classMappingByClass /
            // view / propertyMappingsByPropertyName / allPropertyMappings /
            // inferRelationalType / dataTypeToSqlText are Pure bodies over
            // the metamodel store (schemas, properties, data types, the
            // relational-operation node trees with the compiler's inferred
            // type stamped, property mappings across the extends chain,
            // set root / class, include visit rank); a query's constructed
            // ^DynaFunction(...) trees are rows too (the resolver's
            // side-output seeds). +21 flips = the 20 typeInference tests
            // of testRelationalExtension.pure (16 row-navigated + 4
            // constructed) and testSubTypeMappingValidWhenMappedExplicitly;
            // the six natives and the walk's mapping / set / property-
            // mapping / view / type arms are gone. Lane moves in-pin:
            // exec-passing 345 -> 344, M1 verified 83 -> 82; quarantine
            // 151/20 -> 125/9 (four refusal spellings retired).
            // 820/1753 -> 791/1782 (harness burn-down batch 14 — GROUP D
            // leg 1, 2026-09-03): the ROUTER'S STRING ENTRY. +29 flips =
            // compileLegendValueSpecification over let-bound constant
            // strings (13 testSubTypeGraphFetch; the parser's quote/eval
            // fold reads the body's let constants, the deferred-let park
            // covers the ->cast(@RootGraphFetchTree) binding, the chain
            // assembly peels if(<literal>)-selected queries),
            // executeLegendQuery as a native RESULT FRAME beside
            // router::execute (XStore milestoning 4, m2m2r milestoned 5,
            // platformOperations 4; vars bind as leading lets coerced by
            // declared type, the json-builder / bare-scalar envelopes are
            // emitted over the chain; a let-bound runtime's setup SQL is
            // collected through the alias channel; Nil conforms to class
            // formals), compileLegendGrammar over a functions-only payload
            // as the QuotedGrammarCall carrier (testGraphFetchMilestoning 3;
            // ->at(i)->cast(@FunctionDefinition) selects structurally).
            // 791/1782 -> 782/1791 (batch 15 — GROUP D leg 2, 2026-09-03):
            // the meta::json tree classes (json.pure:32-70) on the VARIANT
            // lane — parseJSON = the JSON cast, keyValuePairs->filter(key)
            // .value / getValue = the member access, .values = the element
            // list, .value = the text/number/boolean, casts within the
            // family identity, toCompactJSONString/toPrettyJSONString the
            // JSON text (TypedJsonAccess, JsonChecker); the string entry's
            // tdsBuilder/classBuilder RESULT envelopes (TypedJsonResult) with
            // the activity SQL rendered engine-style; a helper whose body is
            // a statement sequence runs as one (executeCallStatement); the
            // inlined string-entry call re-offers to the frame splice after
            // argument substitution; α-renamed query parameters bind by
            // position. +9 = runLegendTest 4 (slice/take/limit/drop
            // WithVariables), paginate 2, enumPushDown 1, testSubTypeGraphFetch
            // 2 (JSONArray sort — list sortBy now zips indices instead of
            // indexing the source inside lambdas).
            // 782/1791 -> 780/1793 (batch 16 — GROUP D remainder, 2026-09-03):
            // a LET-BOUND runtime's value types through the alias channel at
            // from() and the same collectors read it (chain mappings, JSON
            // sources, setup SQL) — with the CSV half: testDataSetupCsv on a
            // LocalH2 specification / a TestDatabaseConnection seeds through
            // CsvSeed against the enclosing connection store's database, a
            // COPIED connection's store found by structural navigation of
            // the copy's source. +2 = testSpecialUnion_m2m2r (the M2M union
            // root resolved through the runtime's ModelChainConnection),
            // testParametrizedEnumFilter (the inline CSV over a copied
            // testRuntime()).
            // 780/1793 -> 778/1795 (batch 17 — group Q opener, 2026-09-03):
            // executionPlan registered VERBATIM (f:FunctionDefinition<Any>[1],
            // executionPlan_generation.pure:25-50) — the per-arity
            // Function<{Any[1]->Any[*]}> overloads rejected Date /
            // Integer[0..1] query parameters by contravariance. +2 =
            // testDefaultOptionalParamIsNullSafe (planToString + contains),
            // testFilterInWithResultSorcedFromAnExpression. The other ten
            // group Q tests now reach the plan-node navigation walls (the
            // TypedMap over rootExecutionNode.executionNodes) — plan nodes
            // as rows is the leg.
            // 778/1795 -> 729/1844 (batch 18 — plan nodes AS ROWS, 2026-09-03):
            // the executor's plan model rides the query as inline rows of
            // plans / plan_nodes / plan_function_parameters /
            // plan_node_closure (PlanRows) under the handle's scope; the
            // plan reads are navigation over them — member-union hops
            // composed (per-hop subtype witnesses, coalesced key threads,
            // scope carried), a chain cast BELOW a flatten hop re-roots at
            // the subtype's extent (CastReRoot), allNodes as a Pure body
            // over the closure rows (the Java arm deleted), the inliner
            // keeps binder names (plan text prints them), pair().first/
            // second folds, upgraded-H2 plan spellings (TIMESTAMP holders,
            // null-safe optional equality, lowercase dateadd, block/temp-
            // table spacing, dotted Integer placeholders). +49, 0 lost.
            // 729/1844 -> 686/1887 (batch 19 — GROUP A, function bodies AS
            // ROWS, 2026-09-03): $f.expressionSequence over a function value
            // (a reference eta-expands to a lambda) is its statements as
            // ValueSpecification rows under the lambda's scope
            // (FunctionBodyRows), each stamped with the compiler's inferred
            // primary key (PkInference — the engine's rules over the typed
            // tree); inferPrimaryKeyColumnNames is a Pure body over those
            // rows; evaluateAndDeactivate is the identity over rows. +43
            // (pkInferenceTests, all), 0 lost.
            // 686/1887 -> 661/1912 (batch 20 — GROUP E, lineage trees AS
            // ROWS, 2026-09-03): a scanRelations handle's relation tree is
            // node rows (LineageRows — the lineage scan's printed lines as
            // data: preorder, indent, kind, name, join label, columns);
            // relationTreeAsString is a Pure body over them (the database
            // prints the tree); the engine's scanRelations.pure is SPEC,
            // never loaded. +25, 0 lost. Named residue: 19 runtime-variant
            // trees whose join labels carry the engine's internal alias
            // breadcrumbs (_d#5_d#2_m1, _dy1, _f_d_r — the Java arm used to
            // strip them from the golden), 3 typer/lowering walls.
            // 661/1912 -> 656/1917 (batch 21 — GROUP I, column lineage AS
            // ROWS, 2026-09-03): a scanColumns handle's column set is
            // column_contexts rows (ColumnLineageRows — the scan's
            // (table, column, context) entries with the owning database
            // and schema); ColumnWithContext.column / Column.owner are
            // joins onto relational_elements, so the test's
            // $t.column.owner->cast(@Table).name is navigation. Two
            // resolver legs: cast(@T) over a value already of class T is
            // the identity (CastChecker), and instance removeDuplicates
            // keeps the materialized TO-ONE slot columns in its DISTINCT
            // tuple (to-many exists materials still stay out). +5, 0 lost.
            // Named residue: testNonDataTypeProperty (class-valued project
            // column — the 'class query under TypedMap' bucket).
            // 656/1917 -> 653/1920 (batch 22 — GROUP H, the expression TREE
            // as rows, 2026-09-03): every node of a function body is a
            // value_specifications row discriminated by its m3 kind
            // (FunctionExpression / InstanceValue / VariableExpression —
            // an Operation set over the one table), parametersValues are
            // the children rows, and the node's Multiplicity is the real
            // m3 object shape (Multiplicity.lowerBound.value) over the
            // same row; getLowerBound is the real body; the engine's
            // expressionSequenceReturnsAtLeastToOneDataType is a Pure
            // body over it. Two reflection folds at typing:
            // evaluateAndDeactivate over a lambda literal is the literal,
            // and deactivate()->cast(@InstanceValue).values->at(0)
            // ->cast(@LambdaFunction<..>) is the lambda. +3
            // (tesIsToOneDataTypeFunctionExpressionSequence ×3), 0 lost.
            // Named residue (engine-generator INTERNAL API, no platform
            // counterpart): testFindFunctionSequenceMultiplicity,
            // testMergeOldAliasToNewAlias, testReAliasMergedJoinOperations,
            // testFindAliasMappingBySchemaName, addDriverTablePkForProject,
            // testImportDataFlow; simpleFunctionExpressionTranslationNow/
            // Adjust read toSQLQuery()->sqlQueryToString(H2) — a plan-text
            // handle leg, not built yet.
            // 653/1920 -> 581/1992 (batch 24 — EXECUTION ACTIVITIES AS ROWS,
            // 2026-09-03): an execute()'s Result is a row under the call's
            // scope and its activities are kind rows (RelationalActivity
            // carrying the SQL the platform ran — its own render, the
            // toSQLString pipeline; no trace comment invented, no rewritten
            // query printed from Java); $r.activities re-roots at the call
            // (ResultEnvelopeSplice.activitiesRowsRead), execute is a HANDLE
            // whose declared Result<T|m> names the row class. The corpus
            // sql()/sqlRemoveFormatting bodies inline over the rows; the
            // referee's classification arms stay (a text-divergent render
            // still row-verifies through the oracle), and so does the
            // rewrittenQuery fold over the Java printer AggAwareActivities
            // (deleting it regressed the NOP family 15 -> 10 passes; it
            // stands until the router records the aggregation-aware
            // rewrite as routed-tree ROWS — batch 25). +72 (aggregationAware
            // 28, routing 13, union 16, tds/groupBy 7, ...), 0 lost.
            // Honest fallback: testSQLComments (the engine's trace-id
            // comment).
            // 581/1992 -> 505/2068 (batch 26 — the referee's render IS the
            // frame's chain, 2026-09-03): the activity SQL renders from the
            // frame's own assembled chain (the caller's lets folded, the
            // mapping attached — one pipeline, not a second re-inlining of
            // the raw lambda), so every sql()/sqlRemoveFormatting read over a
            // let-bound-date milestoning query folds in the referee's lane.
            // +76 (milestoning businessdate 32, contextpropagation 18,
            // processingDate 3; in-list filters 6; TDS concatenation 4;
            // routing/tds 4; ...), 0 lost.
            // 505/2068 -> 487/2086 (batch 27 — referee render COVERAGE,
            // 2026-09-03): the render takes the mapping the chain carries
            // (an in-query withMapping/from with a placeholder ^Mapping()
            // argument — fromMapping 5) and the H2 engine spelling of a
            // LITERAL collection membership is the in-list
            // (x in ('a', 'b') — let-bound lists and literal ->contains;
            // filter::in 11, exists 2). +18, 0 lost.
            // 487/2086 -> 463/2110 (batch 28 — INLINE handles on demand +
            // the unrolled quantified verdict, 2026-09-03): (1) the resolver
            // registers an INLINE handle's rows on first meeting through the
            // executor's registrar (an executionPlan(...) inside an inlined
            // helper has no let to register at — ConstructedInstances.
            // handleRows; relationalMapper 8, executionPlan datetime 3);
            // (2) `[pairs]->map(p| lets; assertEquals(e, a, fmt, args))
            // ->distinct() == [true]` unrolls per literal element and each
            // element's assert adjudicates through the existing arms
            // (sqlstring 13). +24, 0 lost.
            // 463/2110 -> 451/2122 (batch 29 — SQL POST-PROCESSORS,
            // 2026-09-03): the engine's CTE-extraction processor is an
            // SQL-IR pass (SqlPostProcessors.extractSubqueriesAsCtes — every
            // FROM-tree subselect becomes subquery_cte_<level>_<index>, the
            // engine's numbering; a new WITH query variant SqlWith renders
            // in both styles), recognized from the runtime's
            // sqlQueryPostProcessors hook; replaceTables pairs bound through
            // the caller's lets resolve (the recognizer chases lets); a
            // verdict over a frame runs under THAT frame's post-processing
            // env (the rows leg re-executed the frame WITHOUT its renames).
            // +12 (cteExtraction 7, replaceTables 5), 0 lost.
            // 451/2122 -> 446/2127 (batch 30 — effectful helper VALUES +
            // generic multiplicity arguments through name resolution,
            // 2026-09-03): `let runtime = initDatabase()` (DDL effects, then
            // ^Runtime(...)) binds the helper's effect-free value as the let
            // would have (UserCallInliner.helperValueLet; an execute() value
            // becomes a frame) — forced milestoning 4, businessdate 1; and the
            // name resolver's generic rebuild dropped Result<TabularDataSet|1>'s
            // |1, typing every user-function Result.values read [*] (the
            // validation family's wall — now it reaches its real one: the
            // engine's generateValidationQuery library). +5, 0 lost.
            // 446/2127 -> 430/2143 (batch 31 — THE QUERY FRONT DOOR,
            // 2026-09-03): the relational validate(...) raw-space desugar
            // (ValidateDesugar — the engine's generateValidationQuery
            // synthesis over the parsed AST, in main since #45) was wired
            // ONLY from the harness preamble, so the flip path inlined the
            // corpus's Pure validate and walled on its library. Compiler.
            // resolveQuery is now the one entry (desugar, driver-pk option,
            // name resolution) and the flip resolves through it. +16
            // (validation complex 10, showcase 5, businessdate 1), 0 lost.
            // 430/2143 -> 416/2157 (batch 32 — PLAN-EXECUTE FRAMES,
            // 2026-09-03): `$plan->execute($parametersValues, ext)` inside
            // the TDG helper walled on "parametersValues binding pending"
            // because the values argument was the helper's PARAMETER (bound
            // to [] at the call) — chased through the lets now; then the
            // helper's `$result.values->at(0)->cast(@TabularDataSet).rows
            // ->isNotEmpty()` read: a relation's .rows ARE the relation and
            // cast(@TabularDataSet) over a relation is the identity
            // (Anchors.tdsErase — CastChecker's rule the typer could not
            // apply to an envelope read); a TDS-typed root (tableToTDS) is a
            // relation-rooted frame. +14 (testDataGeneration), 0 lost.
            // 416 -> 394 (batch 33, 2026-09-03): the execute() runtime
            // argument's connection content is read THROUGH lets — a
            // JsonModelConnection / ModelChainConnection bound by a let and
            // spliced into a copied runtime (`^$rt(connectionStores =
            // ...->concatenate(^ConnectionStore(connection = $json, ...)))`)
            // now feeds the JSON source frame / chain mappings
            // (TypedFrom.jsonSourcesIn/chainMappingsIn take the executor's
            // let-chase). +22 (XStore ordered 8, XStoreUnion 4, relational
            // chain 4, resultSourcing 4, XStore JsonToDB 2), 0 lost.
            // 394 -> 379 (batch 34, 2026-09-03): the String overload of
            // assertSameSQL (`assertSameSQL($golden, $result->
            // sqlRemoveFormatting())`, engine testAssert.pure) takes the
            // SAME exec-read rows verdict as assertEquals — its arm only
            // knew the Result overload, so the statement fell through to
            // the lowerer ("no scalar lowering for assertEquals" was this
            // fall-through, two mechanisms away). +15 (in-clause joins 3,
            // forced filter 2, concatenate 3, query::function 3, distinct,
            // embedded exists, association mixed, group open variable),
            // 0 lost.
            // 379 -> 369 (batch 35, 2026-09-03): the referee's engine-style
            // H2 render spells reductions over a LITERAL collection as the
            // engine's infix chain (and([a,b,c]), [x,y]->times()),
            // firstNotNull as coalesce, round as round(x[, n]) — the
            // "class query under TypedMap" wall was the sql() read reaching
            // the resolver because the activity render had failed. +10
            // (round 4, tdsFilter and/or 2, firstNotNull 2, divide 1,
            // columnValueDifference 1), 0 lost.
            // 369 -> 366 (batch 36, 2026-09-03): percentile is ONE semantic
            // reducer whose DESCENDING form is the value's within-group
            // order (PERCENTILE_x(p) WITHIN GROUP (ORDER BY v DESC)); the
            // DuckDB encodings (negation, sorted-list pick, the QDISC_DESC
            // pseudo-reducer) left the lowerer for a DuckDB MIR pass
            // (QuantileOrder); H2 / engine-style H2 spell the standard form
            // with the direction. +3 (groupBy percentile, TDS groupBy
            // percentile, percentile window), 0 lost.
            // 366 -> 330 (batch 37, 2026-09-03): the "text-policy" pre-decline
            // gate DELETED — every sql-assert shape is attempted; the
            // sqltext homework (docs/SQLTEXT_HOMEWORK_2026_09_03.md) attempted
            // all 65 gated bodies: 36 flip on rows verdicts (validation
            // milestoning 13, businessdate 8, tdsRestrict 5, showcase 3,
            // qualifier 2, contextpropagation 2, union/slice/dayOfWeek/
            // planSql), 29 wall by their own reason, 0 lost.
            // 330 -> 314 (batch 38, 2026-09-03 — the no-decision burn from
            // the sqltext homework): the exec-read rows leg hands the frame's
            // MAPPING (through the splice hook) to the oracle, which decodes
            // enum columns from the enumeration mapping (includes chased;
            // identity when the enum has no enumeration mapping — real pure
            // decodes by name); a let-bound join condition lambda binds
            // through the alias chase and the declared TDSRow class is the
            // nominal row supertype (InferenceKernel); the TDSRow getters
            // getInteger/getFloat/getDecimal/getDate/getDateTime/
            // getStrictDate/getBoolean are declared (tds.pure:84-114); the
            // assertSameSQL String overload takes the general arm with a
            // let-chased toSQLString lambda. +16 (groupBy agg-to-many 5,
            // enum-mapped projections 6, tdsJoin 2, dayOfWeek, joinStrings,
            // consistency-with-nulls), 0 lost.
            // 314 -> 310 (batch 39, 2026-09-03): the H2-family referee
            // spells a per-row LITERAL collection's lateral explode as the
            // engine's decorrelated UNION ALL keyed by the base row
            // identity (LateralExplodeToUnion; H2 2.1 has no LATERAL) and
            // the engine-style render runs its dialect passes; a plan-text
            // golden replays its ONE sql node instead of the plan text.
            // +4 (concatenate flat 3, testMapWithOpenVariableOutsideBlock),
            // 0 lost.
            // 310 -> 308 (batch 40, 2026-09-03): planTestDataGeneration is a
            // PLAN-flavored TDG carrier (CoreFn PLAN_TEST_DATA_GENERATION;
            // testDataGeneration.pure:818/823) whose planToString prints the
            // platform's own MultiResultSequence text (TestDataGenerator.
            // planText — the harness arm TestDataGenForm.planText stays until
            // the alloy family flips whole). +2 (testConstant_Alloy,
            // testViewChild_Alloy), 0 lost.
            // 308 -> 304 (batch 41, 2026-09-03): LET-BOUND column arguments
            // bind at the consuming project (`let p = [#/Person/firstName#];
            // let n = ['First Name']; ->project($p, $n)`; a col-spec
            // collection under ->cast(@BasicColumnSpecification) parks as a
            // deferred let and binds through the alias chase); the TDG plan
            // for a root without row identifiers is the engine's Error node
            // over the top-5 primary-key sample. +4, 0 lost.
            // 304 -> 303 (batch 42, 2026-09-03): the verdict-arm rows leg
            // hands the oracle the STATIC extent-subset fact of the typed
            // chain (a class extent through filter/sort/limit/...), which
            // arms the graph compare's pk-collapse exactly as the walk lane
            // does — the engine's join fan-out re-manufacturing one object
            // is not a divergence. +1 (testQualifierQueryWithOr), 0 lost.
            // 303 -> 297 (batch 43, 2026-09-03): the engine-style referee
            // render now RUNS the H2 carrier strategies (it had no passes
            // at all) — with literal reductions kept semantic (the engine
            // text's own flat spellings) — plus two explode rungs: a
            // null-dropping list_filter over an exploded concat becomes each
            // branch's WHERE, and the ordered-dedup idiom over rows is
            // DISTINCT. +6 (concatenate 4, distinct, filter-with-in), 0 lost.
            // 297 -> 291 (batch 44, 2026-09-03): no-decision singles —
            // zip is the POSITIONAL pairing of two ordered collections
            // (list_zip into the Pair struct; the per-row project shape
            // stays for two [1] reads of one class chain, a nested zip
            // is a ^Pair column), the Result-envelope splice erases
            // cast(@TabularDataSet)/.rows AFTER splicing their source
            // (->at(0) over a Result<Any> helper parameter), and
            // meta::pure::tds::extend dispatches to the ExtendChecker
            // (the legacy col() normalization). +6 (sort
            // testSortByLambdaDeepOptional; TDG testTableToTDSSimple,
            // WithAppliedFunctions x2, WithConcatenate, WithGroupBy), 0 lost.
            // 291 -> 287 (batch 45, 2026-09-03): if() over a class query
            // decides statically when its condition is the emptiness of a
            // LITERAL collection (the M3 elementOverride read types to the
            // empty literal — LiteralFolds.staticBool isEmpty/isNotEmpty/
            // not); a TDSNull-TYPED collection root ([^TDSNull(),
            // ^TDSNull()]) egresses each cell as the TDSNull value (the
            // wire's one spelling, the referee's sentinel) instead of
            // walling as a lowering defect. +4 (inheritance testGetAll x2,
            // tree testProjectMerge, milestoning column projection), 0 lost.
            // 287 -> 285 (batch 46, 2026-09-03): planToString over a
            // RELATION-rooted body (table accessor / tableToTDS: one node
            // whose TDS tuples resolve through the root table's database;
            // an accessor's columns spell the engine's precisePrimitives
            // with their default relational types — PreciseTypes) and a
            // map over a SCALAR read of an object chain composes the mapper
            // over the read (Pipelines.composeScalarReadMap). +2
            // (relationalTDSTypeForColumnsAndQuoting,
            // testComplexOrExistsToManyProperty), 0 lost.
            // 285 -> 284 (batch 47, 2026-09-03): parseDate is a SEMANTIC
            // node (SqlFn.PARSE_DATE) the dialects spell — the engine-style
            // H2 text is the engine's toTimestamp idiom
            // cast(parsedatetime(x, '<pattern>') as timestamp); the
            // execution dialects cast. +1 (tdsExtend testParseDate), 0 lost.
            // 284 -> 282 (batch 48, 2026-09-03): ENUMERATION MAPPINGS AS ROWS
            // (system store enumeration_mappings / enum_value_mappings /
            // enum_value_sources; m3 EnumerationMapping.enumValueMappings,
            // EnumValueMapping.enum + a lite EnumSourceValue association);
            // enumerationMappingByName and toDomainValue are Pure bodies
            // over the rows (the K-side native deleted). +2 (enumeration
            // testEnumMappings, testEnumMappingsWithInclude), 0 lost.
            // 282 -> 281 (batch 49, 2026-09-03): a LET-BOUND legacy aggregate
            // (`let g = agg(x|…, y|…)`, engine AggregateValue) parks at its
            // binding (Typer.deferredLetRhs) and types against the groupBy
            // that consumes it (GroupByChecker.legacyToModern chases the
            // alias, per element). +1 (testModelConnectionAgg), 0 lost.
            // 281 -> 280 (batch 50, 2026-09-03): the engine-style H2 referee
            // spells the MMMyyyy month-abbreviation parse (the engine's
            // convertToDateH2 rule: concat('01', x) + 'ddMMMyyyy') — the
            // referee render had thrown, walling the frame's sql() read as
            // a class query. +1 (stringToDate H2 user-defined format), 0 lost.
            // 280 -> 279 (batch 51, 2026-09-03): an Any-typed STRUCT FIELD
            // (a Pair<String, Any> second slot on the variant lane) decodes
            // as its value at the wire (Executor.unwrap → decodeAny) —
            // "Firm X" was compared as JSON text with its quotes. +1
            // (selfJoin testSelfJoinPropertyMapping), 0 lost.
            // 279 -> 277 (batch 52, 2026-09-03): POST-PROCESSORS ARE COMPILER
            // PASSES (user catch): the engine's nonExecutable processor is
            // the IR pass SqlPostProcessors.nonExecutable (every select's
            // filter AND 1 = 2), recognised from the connection like
            // replaceTables/CTE extraction; the golden-vs-render verdict
            // arm takes the toSQLString RUNTIME overload (dialect from the
            // connection, the runtime's replaceTables applied to the rows
            // leg). +2 (nonExecutable subqueries, toSQLString replaceTables),
            // 0 lost.
            // 277 -> 267 (batch 53, 2026-09-03): TIER 1 RECURSION (user green
            // light) — a recursive Pure function over a LITERAL instance
            // tree unrolls at compile time: the inliner re-enters a
            // recursive call while its literal argument strictly descends
            // (well-founded, no depth constant), dispatches match arms /
            // map / filter / if lazily on the literal BEFORE their bodies
            // are rewritten, and LiteralUnroll folds property reads, casts,
            // copies and list shape over literals (never inside quoted
            // code — TypedLambda.quoted). THE COMPILER COMPARES, THE
            // DATABASE COMPUTES (docs/WORLD_MAP.md §4): no fold produces a
            // value — toLower stays a residual, a filter over a spelled
            // list with an undecided predicate keeps each element under
            // its own condition, a shape-CASE over two DynaFunctions is
            // judged by the key tree (equality keys in the prelude, __type
            // beside __id, a bare constructed instance is a struct VALUE —
            // the rows lane compared instance trees by root name only).
            // +10 (debugPrint wrapH2Boolean 9, routing composition 1),
            // 0 lost.
            // 267 -> 255 (batch 54, 2026-09-04): OPTION S — the prelude's
            // library SHAPES are GENERATED from the spec (Prelude.java, 230
            // classes / 10 enums; 217 hand copies deleted from Pure.java,
            // three hidden gaps in them found: ElementOverride's package,
            // two guessed sql literals, TabularFunction.schema typed Any);
            // the resolver learned real pure's implicit core imports; the
            // m3 bootstrap shapes come from m3.pure via tools/m3shape.py;
            // the program toPostgresModel compiles through utils.pure
            // (Corpus.LIBRARY_FILES) with the structural folds of
            // WORLD_MAP §4 (size/contains/spelled maps/groupBy/keyValues/
            // get/defaultIfEmpty/isTrue/assert(true)/enumValues/dynamicNew/
            // spelled-integer compares/enum .name, unspelled defaults,
            // declared defaults at ^new, static re-dispatch on the input's
            // declared type, one body rule for let/fail/assert prefixes),
            // and a helper-wrapped assert over CLASS values is judged by
            // the key tree (a wider-declared side by its wire __type).
            // +12 (toPostgresModel literal-only slice A), 0 lost.
            // 255/2318 -> 252/2321 (batch 55a, 2026-09-04): the Java port
            // of toPostgresModel and the host metamodel walk are DELETED
            // (MetamodelWalk 905 + MetamodelSteps 156 + the executor's
            // planWalk/constructNode/constructOp/nodeValue/walkProp/
            // walkFilter arms, 583 lines); the three tests the walk still
            // scored ride the platform: SQLExecutionNode.connection and
            // its datasource specification are plan rows
            // (plan_connections / plan_connection_sqls, PlanRows.
            // connectionRows), a property-less class constructor is the
            // identity struct (__type alone), assertInstanceOf over a class
            // value is the wire's __type up the model's subtype relation
            // (the harness's NodeH string-match arm is deleted). +3, 0 lost
            // (per-family counts and decline rosters identical).
            // 252/2321 -> 251/2322 (batch 55b, 2026-09-04): toPostgresModel
            // slice B, the compiler side — the relational metamodel's
            // children()/childByJoinName() are SystemMetamodel views
            // (functions.pure:288-296), a runtime match over a SYSTEM-STORE
            // row dispatches over the relation's bound kinds (the system
            // mapping's class bindings beneath the declared class), over a
            // primitive over its lattice, the arm scan reads declarations
            // only, the unroll's descent measure is lexicographic (literal
            // size, then a store argument of a class no enclosing activation
            // holds), and four structural folds land: a spelled scalar cast
            // to its primitive, an empty spelled collection cast, and the
            // native concatenate/zip over spelled lists. +1
            // (testConvertJoinStrings), 0 lost.
            // 251/2322 -> 246/2327 (batch 55c, 2026-09-04): the STORE-ROW leg
            // — a constructed instance over one toOne-wrapped element chain
            // (by STRUCTURE: pure is referentially transparent, equal reads
            // of a store chain are one row) is the map of that chain's row
            // (StoreResolver.constructedRowForm; the row's navigations are
            // its join steps, never a subquery per read); the metamodel
            // store maps Schema.tables / Table.schema; a navigate-slot hop
            // threads downstream depth into its nested target
            // (NavMaterializer tails + SubNav provenance — the association
            // route's depth leg, for slots); slot prefixes mint clear of the
            // left row's composed names (Pipelines.slotPrefix, one rule at
            // the three sites); a many-valued list VALUE's map stays a list
            // map in the substitution; the map-binder channel's VALUE is its
            // cell for the canon and for a struct slot (ResultShape.valueInfo);
            // F10 proper: a constructed instance's canonical key text is
            // computed at its CONSTRUCTION site from its own fields and
            // rides the wire as the synthetic __canon field (the bound
            // struct form — one mention per child, linear text), so a
            // JSON-carried nested instance is judged by the classifier that
            // built it (17 instance-key-shape declines in the family -> 0,
            // sql-verdict agree=32 disagree=0). +5 flips (Alias, Table,
            // TabularFunction, SelectSQLQueryWithCTE, Union), 0 lost;
            // dual-channel disagree 0; 0-assert passes 29.
            // Batch 55d (2026-09-04): a POSITIONAL pick over a to-many
            // navigation ($t.columns->at(k)[->cast(@C)].name) lifts into a
            // synthetic to-one head (columns#pN, SyntheticHeads.POSITIONAL)
            // whose join target is the physical row with ordinal == k — the
            // store keeps a column's declaration ordinal
            // (relational_elements.ordinal); the metamodel store maps
            // Table.columns (@TableToColumns); the lift walk reaches into
            // a constructed instance's fields (the map-over-row body); a
            // navigate slot named after a relation accessor (columns/rows)
            // mints clear of it. +1 flip (TableAliasColumn), 0 lost;
            // sql-verdict disagree 0; dual-channel disagree 0.
            // Batch 56 (2026-09-04, no-decision singles): a LET-BOUND
            // lambda literal in a CORE construct's argument position is
            // its literal (Typer.expandLetBoundLambdaArgs — let is
            // immutable and referentially transparent; generic/user calls
            // keep the function VALUE); a MAPPING element read as a
            // metamodel value (mapping.enumerationMappings) is a property
            // access over its system-store row exactly like a database's
            // (Typer.metamodelElementClass). +2 flips
            // (testLessThanFilterAsVariable, testEnumTheSame), 0 lost;
            // sql-verdict disagree 0; dual-channel disagree 0; lane move
            // exec-passing 58 -> 57 (the walk's M1 lane retired, 1 -> 0).
            // Batch 57 (2026-09-04, the mechanical type walls): registry
            // truths from the spec — eval arities 4-6, repeat (n copies over
            // range), Package/Testable m3 shapes with PackageableElement.
            // package, Mapping.includes (direct include rows), the Service
            // metamodel generated; typer rules — a lambda IS an Any
            // (cast(lambda, @FunctionDefinition)), the DOT auto-map over a
            // many receiver ($exts.routerExtensions()), a mapping element
            // read as its system-store row; the static fold's map unroll
            // expands a function-valued helper over the element and folds
            // inside reified accessor lambdas (the digest inliner escape);
            // a TDSRow getter over the column lambda's row lowers as the
            // column read. +2 flips (the hybrid milestoning union pair via
            // repeat), 0 lost; every other probed wall moved to its next
            // honest wall (docs/GATES.md batch 57). sql-verdict disagree 0;
            // dual-channel disagree 0; lane move exec-passing 57 -> 55.
            // Batch 58 (2026-09-04, the H2VERSION decision): SELECT
            // H2VERSION() on an H2-typed connection answers the H2 dialect
            // level the raw-SQL boundary translates from — 2.1.214, the
            // referee's own jar (H2VersionPinTest ties the literal to
            // org.h2.engine.Constants.VERSION). Behind it: toOne over a
            // list-producing call is its checked element; an if whose
            // branches are asserts adjudicates its condition as a value
            // query and the taken branch as the verdict; a primitive
            // literal compared against an Any/JSON cell enters the channel;
            // a lambda cast to a function carrier is the lambda. +7 flips
            // (3 TDG alloy milestoning, 2 sqlstring adjust-date, 2
            // businessdate), 0 lost; sql-verdict disagree 0; dual-channel 0.
            // Batch 59 (2026-09-04, the lineage-tree ROW verdict): a
            // scanRelations tree print's join labels spell the engine's
            // decorated SQL ALIASES (buildUniqueName alias=true:
            // _d#N/_dy/_m/_l/_r/_md/_N — pureToSQLQuery.pure buildNodeId),
            // an artifact of its SQL generation the row charter retired.
            // USER RULING: never normalize the golden text and byte-compare;
            // LineageTreeVerdicts brings BOTH prints to rows by one referee
            // query in the database (TREE_ROWS) and the row lists compare
            // (counted: lineage-rows agree=66). +21 flips (the whole
            // lineage/scanRelations tree family), 0 lost; sql-verdict
            // disagree 0; dual-channel disagree 0.
            // Batch 61 (2026-09-04, acos/asin domain): the engine's H2 SQL
            // for acos/asin out of [-1, 1] yields NaN and the filter drops
            // the row; ours raised "Unable to compute acos of 1.1". The
            // Scalars rule now yields NULL out of domain (CASE WHEN abs(x)
            // > 1 THEN NULL ELSE acos(x)), the same row outcome. +2 flips
            // (testFilterUsingArcCosFunction, testFilterUsingArcSinFunction),
            // 0 lost; sql-verdict disagree 0; dual-channel disagree 0.
            // Batch 62 (2026-09-04, the join chain's terminal column): the
            // engine re-resolves a `@J > @J | table.COL` terminal in the
            // JOINED cursor (pureToSQLQuery resolveJoinElement:
            // reprocessAliases old alias -> op.alias); the spelled table is
            // grammar. RelOpTranslator rebases terminal column refs the
            // chain end DECLARES to the chain end (Pipeline records each
            // slot's target columns); a column it does not declare stays
            // where spelled (TestMappingWithViewJoins' `| firmTable
            // .LEGALNAME` after a hop onto a view). +1 flip
            // (testIsolatioWhereNoConstaintsAndInnerJoin), 0 lost;
            // sql-verdict disagree 0; dual-channel disagree 0.
            // Batch 63 (2026-09-04, the joined table's scan order): the
            // engine-corpus-compat scan-order key (ScanOrder, StableScanOrder
            // — flag-gated, host channel only) is now LEXICOGRAPHIC over the
            // join tree's base-table scans in join order and covers
            // plain-table joins: H2's nested loop emits the driving scan's
            // order and, within one driving row, the joined table's scan
            // order; DuckDB's hash join does not (Product ⋈ Product_Synonym
            // with synonyms 11→P1, 12→P2, 13→P1: (P1,11),(P1,13),(P2,12)).
            // +3 flips (the enum projection rows->at(i) tests:
            // testProjectWithIfWhereBothSidesUseTheSameEnumMapping,
            // testProjectWithIfWhereOneSideIsEnumLiteral,
            // testProjectionWithEnumThroughAssociation), 0 lost;
            // sql-verdict disagree 0; dual-channel disagree 0.
            // Batch 64 (2026-09-04, the chained generator fetch as a ROW
            // verdict): the walk's tdgChainedVerify mechanism moved behind
            // the oracle SPI (verifyFetchChain) — the platform arm
            // addresses a hop by its $testData.sqls->at(i) index and the
            // let-bound generator node, the oracle remembers each hop's
            // golden for the attempt, materializes the ancestor temps
            // (testDataGen_Temp_<T>) from those goldens root-first, runs
            // the hop's golden and multiset-compares the hop's transcript
            // rows (the generator re-run: deterministic reads, text
            // receipt). +10 flips (testDataGeneration: testSimpleTwoTable,
            // ...MultipleStartRows, testSelfJoin, testUnion, testUnionToUnion,
            // testInheritanceMultipleTableJoin, testTableToTDSMultipleJoins,
            // testTableToTdsWithJoinAndOLAPGroupBy, ...WithJoinAndUnion,
            // ...WithJoinToSameTable), 0 lost; sql-verdict disagree 0;
            // dual-channel disagree 0.
            // Batch 65 (2026-09-04, the inline in-list temp table): the
            // engine's tempTableForIn_N holds the query's in([...]) literal
            // (numbered by plan node); the platform arm reads the literal
            // off the frame's typed query (exactly one inline in-collection,
            // one temp name in the golden) and hands the oracle a TempTable
            // spec in Pure terms (kind + values); the oracle spells the H2
            // temp (the walk's literalTempSeeds) as per-verify statements.
            // +4 flips (testInExecutionWithTempTableFor{DateTimes,Dates,
            // Numbers,Strings}), 0 lost; sql-verdict disagree 0;
            // dual-channel disagree 0.
            // Batch 66 (2026-09-05, the golden PLAN replayed node by node):
            // the oracle's verifyPlan runs a plan text's nodes in order —
            // an Allocation's Constant literal or Relational rows bind the
            // later holes, the engine's template helpers (collectionSize,
            // renderCollection, varPlaceHolderToString,
            // optionalVarPlaceHolderOperationSelector, GMTtoTZ, ?replace)
            // evaluate by their published bodies, the final node's filled
            // SQL replays for rows (harness PlanReplay behind the SPI);
            // collection parameters bind two referee elements; the plan
            // lambda's leading lets scope our rows leg; testQualifier's
            // hop-0 spelling reaches the chained TDG arm. +3 flips
            // (testMapWithOpenVariable, testExecutionPlanForQueryWith-
            // VariableRundateWithinLambda, testQualifier), 0 lost;
            // sql-verdict disagree 0; dual-channel disagree 0.
            // Batch 67 (2026-09-05, one by one through the remaining rows):
            // the engine's two-statement in-list plan — golden(0) is the
            // population statement of `let v = <to-many expr>` in the query
            // lambda, so its rows ARE that let's value (the rows leg
            // evaluates the let's expression, wrapped in the frame's
            // mapping); golden(1) reads tempTableForIn_<v>, which the
            // oracle fills from the attempt's remembered population golden
            // (SqlReplayOracle.TempTable "population"); the exec-read arm
            // owns sqlRemoveFormatting($res, n>0) for that shape only. An
            // assert-free body WITH statements (prints) runs through the
            // platform — a clean run is a zero-assert pass (the engine's
            // own contract); only a body with nothing to execute stays a
            // named zero-assert row. +3 flips
            // (testInExecutionWithTempTableAndQueryChaining,
            // ...OnIntegerColumn, twoDBRenameColumns), 0 lost; sql-verdict
            // disagree 0; dual-channel disagree 0. NOT burned, measured:
            // forced-isolation value frames (H2 concat(NULL,'Test') = 'Test'
            // — the forced golden's rows are not droppable NULLs; guard
            // restored), firstDayOfWeek (H2 weeks start Sunday, DuckDB and
            // Pure's own tests Monday — a named divergence).
            // batch 68 (2026-09-05): 187 -> 185 — the instance envelope
            // projects the set's OWN property mappings; the implicitly
            // inherited ones are served on access (StockProduct over
            // milestoningmap: the two milestoned filter-in-mapping tests'
            // graph keys now equal the golden's [id, name, type])
            // batch 69a (2026-09-05): 185 -> 181 — the union sqlQueryMerging
            // pair (^TDSNull() on the variant carrier), the zoned plan's
            // template functions (the helper's parameter let), the
            // assert-free let-execute body
            // batch 69c (2026-09-05): 181 -> 179 — fetchDb primary keys (a
            // function-typed property is not a layout slot; the PK grid
            // finds its database through the typer's let channel) and the
            // datePeriods group-by (a let-bound instance read lifts as a
            // scalar subquery on the execute route too; the chained-plan
            // warning is stripped; the toSQLString arm takes multi-statement
            // lambdas)
            // batch 72a (2026-09-05): 179 -> 176 — a statement-level
            // self-alias let (`let query = $query`, an inlined helper's
            // parameter under the caller's let name) re-binds the outer
            // alias (the two-round test-data generation); a statement-root
            // map over spelled execute bindings unrolls to its element
            // statements (the businessDate var-reference asserts); relation
            // concatenate types POSITIONALLY like the engine's relational
            // lowering (the lineage name-mismatched concatenate). The two
            // malformed `]"` graphFetch goldens stay fallbacks under
            // engine-golden-defect:malformed-json-golden.
            // batch 72b (2026-09-05): 176 -> 168 — objectReferenceIn as a
            // platform program: the generators' spelled pk maps feed the
            // pk-membership rewrite; a let-bound graph tree closes over its
            // lets (the bi-temporal key spelling); the engine's decode is a
            // database expression over the frame's mapping facts; runtime
            // references decode in SQL and a closed from() inside a
            // predicate resolves first. The walk's ObjectRefs.java is gone.
            org.junit.jupiter.api.Assertions.assertEquals(168L,
                    com.legend.harness.WholeTestFlip.fallbackCount(),
                    "whole-test migration ratchet moved: fallbacks");
            org.junit.jupiter.api.Assertions.assertEquals(2405L,
                    com.legend.harness.WholeTestFlip.flippedCount(),
                    "whole-test migration ratchet moved: flipped"
                            + " (diff target/wholetest-flipped.txt)");
            // (TDG S3 rows are unable-to-exec, NOT text-only — user
            // catch: our fetch SQL EXECUTED and its data row-verified)
            // (B1 plan-producer classification was BUILT, MEASURED and
            // REVERTED same day — user catch: the flat sub-reason
            // flattened 141 reason-diverse rows into one coarse label.
            // The MEASUREMENT stands in FULL_RESIDUE_CENSUS §8: every
            // getAll decline is a plan-bearing assert, the 44 stays.)
            // 44 -> 43 (SQLTEXT slice 3a): one text-only assert's test
            // joined the tosqlstring-simple flip cohort — its verdict
            // now comes from the platform arm (rows), so the walk's
            // text-only classification loses the row.
            // 43 -> 40 (§5 first cut): 3 plan-literal text-only
            // asserts joined the plan-text flip cohort — rows verdicts
            // via referee-bound filled-golden replay.
            // 40 -> 35 (batch 15 — GROUP D leg 2, 2026-09-03): the paginate
            // helpers' SQL-text asserts (assertEquals($expectedSql,
            // resultSQL($result)) — the activity SQL read out of the string
            // entry's result JSON) joined the flip cohort: the platform
            // renders the activity text engine-style and the assert judges
            // it in the database; the walk's text-only classification
            // loses those rows.
            // 35 -> 27 (batch 18 — plan nodes AS ROWS, 2026-09-03): eight
            // plan-text asserts (executionPlanTest's testFilterEquals*
            // optional-parameter plans, the datetime pair helpers'
            // planToString reads, testMultiExpressionWithPlatformAndFrom
            // Function) joined the flip cohort — the plan handle's rows
            // ride the query, the text is judged by the plan-text referee
            // arm (upgraded-H2 golden, rows via replay where derivable);
            // the walk's text-only classification loses those rows.
            // 27 -> 26 (batch 40): one TDG plan-text assert left the walk's
            // text-only lane for the platform's planToString (lane move).
            // 26 -> 25 (batch 41): one more TDG plan-text assert left the
            // walk's text-only lane (lane move).
            // 25 -> 24 (batch 52): the flipped toSQLString replaceTables test's
            // text assert left the walk's text-only lane (lane move).
            // 24 -> 17 (batch 58, the H2VERSION decision): the seven flipped
            // H2-compatible tests' text asserts left the walk's text-only
            // lane for the platform's row verdicts (lane move, disagree 0).
            // 17 -> 16 (batch 66, 2026-09-05): testMapWithOpenVariable's
            // plan-text assert left the walk's text-only lane — the oracle's
            // plan replay (Allocation nodes run, holes filled, the final
            // SQL's rows compared) judges it (lane move, disagree 0).
            // 16 -> 15 (batch 69c): the datePeriods toSQLString assert over
            // the chained plan (statement 0 + the engine's warning line) is
            // a row verdict on the calendar let's rows
            org.junit.jupiter.api.Assertions.assertEquals(15,
                    com.legend.exec.CanonicalDivergence
                            .v7DeclinedByReasonPrefix("assert-sql-text-only"),
                    "lane guard: assert-sql-text-only moved — update the"
                            + " charter §8.0 scope table");
            // 502 -> 492 (slice 3 real evaluation): the predicate 16
            // became 10 REAL verified passes (dual-channel agree) + 6
            // recorded divergences (predicate-diverged — dialect-owned
            // text, same policy as assertSameSQL mismatch)
            // 492 -> 206 -> 97 (diff-noreplay burndown slices 1-4):
            // diff-noreplay 321 -> 71, match-noreplay 142 -> 8; then
            // 97 -> 45 (§4AD P0.5, the corrected unpark): the 45
            // collection/scalar PARKED rows all verify now (the park's
            // "set-vs-row adjudication" was the batch-5 placement
            // defect wearing a policy name — addendum §7 item 4), plus
            // the 2 value-observable flatten conversions and 5 more
            // reclassified by execution. Residue = enum underivable,
            // case-sensitive seed replay, graph-keys tail,
            // tempTableForIn, arity, skew, no-gen, predicate-diverged,
            // both-ours (per-cause census in the sweep log).
            // 45 -> 35 (sql-exec burn 2026-08-30): the graph-keys
            // bucket's 10 golden-extra rows converted by the stitch-key
            // rule; the 2 frame-extra rows (multi-statement stitch
            // shape) remain declined by design. Register:
            // docs/VERDICT_DISAGREEMENT_BURN_2026_08_30.md.
            // 35 -> 27 (enum include-traversal): all 8 enum-decode
            // rows burned (the register counted 7; the 8th shared a
            // reason line).
            // 27 -> 21 (tempTable chained replay): the 4 missing-table
            // rows + 2 reclassified; ONLY the 2 statement-pairing arity
            // rows remain of the tempTable family.
            // 21 -> 70 (TDG lane S3, user-corrected classification): the
            // 49 sqls-TEXT rows are UNABLE-TO-EXEC, not text-only — our
            // fetch SQL EXECUTED (the generator ran it; its data is
            // row-verified by the same tests' agreeing assertTestData)
            // and only the GOLDEN replay declined (48 diff-noreplay ::
            // no-root-exec-variable + 1 no-generator). Burnable by the
            // same replay machinery as the rest of this bucket.
            // 70 -> 50 (the 49er REPLAY landed): 20 TDG sqls rows are
            // now FULLY VERIFIED by execution-equivalence (golden on the
            // H2 mirror, ours on DuckDB, rows equal — exec-passing);
            // the 29 remaining ride NAMED causes (26 chained-fetch temp
            // tables, 2 projection-demand divergences, 1 no-generator)
            // 50 -> 21 (chained-fetch live-session refereeing, §10o leg
            // 1): the TDG 29 all execute and row-verify (see the
            // exec-passing 1526 note); the remainder is the relational
            // register's own named residue (the closing-arc roster in
            // VERDICT_DISAGREEMENT_BURN_2026_08_30.md; per-reason census
            // in the sweep log).
            // 21 -> 20 (row-13 adjudication burn 2026-09-01): the one
            // row-cardinality-skew decline row-verifies via the
            // EXTENT_SUBSET pk-collapse (see the exec-passing 1528
            // note); the skew decline reason is DELETED — value/tabular
            // duplication differences now diverge loudly (pure
            // preserves duplicates there).
            // 20 -> 14 (batch 37 — the text-policy gate deleted): six
            // gated tests' unable-to-exec sql-asserts now take platform-arm
            // verdicts (lane move).
            // 14 -> 13 (batch 38): one more gated test's unable-to-exec
            // sql-assert now takes a platform-arm verdict (lane move).
            // 13 -> 11 (batch 67, 2026-09-05): the 2 statement-pairing arity
            // rows of the tempTable family are BURNED — golden(0) verifies
            // as the rows of the let it populates, golden(1) replays with
            // tempTableForIn_<let> filled from golden(0)'s rows.
            // batch 68: 11 -> 9 — the two "graph keys mismatch golden
            // aliases" declines (StockProduct's envelope carried the
            // Product set's implicitly inherited stockProductName /
            // classificationType) are ROW VERDICTS now: the envelope
            // projects the set's own property mappings only
            // 9 -> 8 (batch 69c): the datePeriods "column arity differs"
            // decline is a row verdict (golden(0) = the calendar let's rows)
            // 8 -> 9 (batch 69, the deletion): a byte-equal golden the
            // referee could not replay is ADVISORY now, never verified —
            // one such assert moves from the walk's "verified" count into
            // this lane's decline census (the truth: text agreement only)
            org.junit.jupiter.api.Assertions.assertEquals(9,
                    com.legend.exec.CanonicalDivergence
                            .v7DeclinedByReasonPrefix(
                                    "assert-sql-text-unable-to-exec"),
                    "lane guard: assert-sql-text-unable-to-exec moved —"
                            + " update the charter §8.0 scope table");
            // SLICE Q (charter §4AF, user quarantine ruling 2026-08-30):
            // the METAMODEL QUARANTINE partition — reflection/conversion/
            // function-body-as-data declines, deferred to the
            // metamodel-as-data program (PROGRAM_MAP). 142 = the census
            // §4a's 144 minus the 2 expressionSequence rows that decline
            // under the bare host-unsupported marker (their quarantine
            // ownership is documentary; the census names them). EXACT:
            // growth = a new metamodel decline (adjudicate before
            // accepting); shrink = the deferred program landed something
            // (move the pin with its receipts).
            // SLICE-1 CHANNEL MOVE (charter §4AF, census §10h): with the
            // try-run lane deleted, the toPostgresModel conversion family
            // (20 tests) fails at the TEST level — same failure texts,
            // thrown before per-assert adjudication — so its 35 witness
            // rows left the decline channel (142 -> 107) and the 20
            // tests are counted through the SAME vocabulary on the wall
            // channel. The partition's TEST SET is unchanged.
            // 107 -> 172 (2026-08-31, §4AE growth rule, ADJUDICATED):
            // the TypedMap-65 family is PLAN-NODE MODEL WALKS — the
            // tests' filter lambdas evaluated over plan-node objects,
            // pure code with no store demand (pkOfFunc's class). A burn
            // attempt via the planWalk side door was REVERTED (it grew
            // the parallel evaluator the one-router ruling forbids).
            // A SECOND burn attempt (2026-09-01, plan-chain staging:
            // planWalk results consumed as staged literals, 172->89) was
            // ALSO REVERTED by user ruling: the quarantine covers
            // CONSUMPTION of the Java metamodel walk, not just growing
            // its arms — "no new vocabulary" through a chartered seam is
            // still institutionalizing an eviction-listed evaluator in
            // the verdict path. The deferred program keeps this list;
            // any future burn must come from the program's own design,
            // not a routing of walk output.
            // 172 -> 151 (metamodel-as-relations batch 5, 2026-09-02):
            // the program's OWN design burned the first family — the
            // mapping-metamodel navigation functions are Pure bodies over
            // seeded rows under their real names, so the "resolved
            // overload 'meta::pure::mapping::classMappingById'" refusal no
            // longer exists (its vocabulary entry is retired); the 21 rows
            // it owned resolve through the database. The other spellings
            // are untouched (151 rows).
            // 151/20 -> 125/9 (harness burn-down batch 7 — GROUP F,
            // 2026-09-02): the rootClassMappingByClass / _classMappingByClass
            // / view / inferRelationalType refusal spellings are DEAD (Pure
            // bodies over the metamodel store; the natives are deleted) —
            // their rows and wall tests resolve through the database.
            // 125 -> 77 (harness burn-down batch 18 — GROUP Q, plan nodes
            // AS ROWS, 2026-09-03): the execution-plan read vocabulary's
            // refusals (rootExecutionNode / executionNodes / allNodes /
            // cast(@SQLExecutionNode).sqlQuery / functionParameters over a
            // plan handle) are DEAD — the plan model rides the query as
            // inline rows (PlanRows) and the reads are navigation the
            // database answers; the allNodes native and its Java arm are
            // deleted. Walls unchanged at 9.
            // 77 -> 34 (harness burn-down batch 19 — GROUP A, function bodies
            // AS ROWS, 2026-09-03): the expressionSequence /
            // inferPrimaryKeyColumnNames refusal spellings are DEAD — a
            // function value's statements are rows the database navigates
            // (FunctionBodyRows), the inference a stamped fact (PkInference).
            org.junit.jupiter.api.Assertions.// 34 -> 22 (batch 22 — group H, 2026-09-03): the real m3
            // InstanceValue / FunctionExpression / VariableExpression /
            // Multiplicity classes type reflection chains that used to
            // wall as unknown types; the rows shrink, nothing scored moved
            // 22 -> 5 (batch 30, 2026-09-03): generic MULTIPLICITY arguments
            // survive name resolution, so the reflection chains over
            // Result<T|m> / FunctionDefinition<{->T[*]}> values type — the
            // walls those rows counted are gone; nothing scored moved
            // 5 -> 0 (batch 57, 2026-09-04): the routerExtensions
            // multiplicity refusal is DEAD — pure's DOT auto-map over a
            // many-valued receiver types $exts.routerExtensions(); the five
            // connection-equality tests now wall honestly at the lowering's
            // match over extension-contributed arms (the extension VALUE
            // leg), no longer quarantined; the spelling left the vocabulary
            assertEquals(0,
                    com.legend.exec.CanonicalDivergence.v7QuarantinedCount(),
                    "metamodel quarantine (witness rows) moved off 125 —"
                            + " see FULL_RESIDUE_CENSUS_2026_08_30.md §10j");
            // 9 -> 0 (batch 54, 2026-09-04): the nine quarantined WALL tests
            // walled on library shapes the hand prelude never declared; the
            // generated prelude (option S) declares them, so they compile
            // and are scored like every other test (the witness-row
            // quarantine of 5 is untouched).
            org.junit.jupiter.api.Assertions.assertEquals(0,
                    com.legend.exec.CanonicalDivergence
                            .v7QuarantinedWallCount(),
                    "metamodel quarantine (wall tests) moved off 0 —"
                            + " see FULL_RESIDUE_CENSUS_2026_08_30.md §10h");
            // 117 -> 111 (TDG lane S1): the census folds in the CHECKER
            // — the 6 necessaryColumns asserts route and AGREE.
            // 111 -> 50 (TDG lane S2): the ROW CONTRACT routes (size 26 +
            // testData 35 agree, verdicts in the DB).
            // 50 -> 0 (TDG lane S3): the sqls-TEXT rows ride the
            // golden-SQL referee (sqlTextVerify — outcome-bucketed
            // diff-noreplay in the sql-text lane) and seedDataString
            // routes and AGREES. ZERO-FROZEN: a new row here is a NEW
            // harness compensation — the lane is CLOSED.
            org.junit.jupiter.api.Assertions.assertEquals(0,
                    com.legend.exec.CanonicalDivergence
                            .v7DeclinedByReasonPrefix("assert-test-data-csv"),
                    "lane guard: assert-test-data-csv moved — update the"
                            + " charter §8.0 scope table");
            // leg 7 ratchets: row-verification coverage holds its
            // floor; the unverifiable residue only SHRINKS (the 145
            // burndown — each fix converts an advisory pass into a
            // row-verified pass and moves these two in lockstep).
            // 632/145 -> 791/30 (diff-noreplay burndown 2026-08-28):
            // Graph-frame replay converts divergent-text rows to
            // row-verified rescues and byte-matched rows out of the
            // unverifiable residue — ratchet to measured
            org.junit.jupiter.api.Assertions.assertTrue(
                    // 880 -> 778 floor (charter §8.3b, the same 308-test
                    // migration as the M1_VERIFIED move): rescued
                    // verifies left the walk lane for the platform
                    // arm's row verdicts — migration, not loss.
                    // (777: the one-test admission wobble moves a
                    // rescued verify with it — the same envelope class
                    // as the exec-passing lane pin)
                    // 777 -> 405 (charter §8.3c, the 541-test
                    // exec-sql-read migration): the same lane move as
                    // M1_VERIFIED 373 -> 134 — rescued verifies now
                    // ride the oracle SPI's row verdicts.
                    // 405 -> 246 (§8.3d): the dual-golden lane move.
                    // 246 -> 204 (effectful cutover): the same lane
                    // move as M1_VERIFIED 85 -> 83 — the 42 modelJoin
                    // text-rescued passes now verify via the arm
                    // channel and their rescue flags CLEARED (corpus
                    // clean passes 2101 -> 2143, total 2349 stable).
                    // 204 -> 164 (activities AS ROWS, 2026-09-03): the same
                    // lane move as M1_VERIFIED 82 -> 54 — the 67 flipped
                    // tests' text-rescued sql-asserts now row-verify through
                    // the oracle SPI as platform-arm verdicts.
                    // 164 -> 128 (batch 26): the same lane move as M1_VERIFIED
                    // 54 -> 22.
                    // 128 -> 127 (batch 29): the same lane move
                    // 127 -> 119 (batch 33): the same lane move — the
                    // flipped resultSourcing/chain/XStore tests' rescued
                    // sql-asserts now row-verify as platform-arm verdicts
                    // (passes 2367 -> 2374, 0 flips lost, disagree 0).
                    // 119 -> 109 (batch 34): the same lane move as
                    // M1_VERIFIED 20 -> 12.
                    // 109 -> 108 (batch 36): the same lane move — one
                    // percentile sql-assert now row-verifies as a
                    // platform-arm verdict.
                    // 108 -> 75 (batch 37): the same lane move as
                    // M1_VERIFIED 12 -> 9 (the 36 gated tests' rescued
                    // sql-asserts now row-verify as platform-arm verdicts).
                    // 75 -> 63 (batch 38): the same lane move as
                    // M1_VERIFIED 9 -> 4.
                    // 63 -> 62 (batch 42): the same lane move.
                    // 62 -> 57 (batch 44): the five flipped TDG tableToTds
                    // tests' text-rescued sql-asserts now row-verify through
                    // the arm channel (the same lane move as exec-passing
                    // 68 -> 63; disagree 0).
                    // 57 -> 55 (batch 45): the same lane move as
                    // exec-passing 63 -> 61.
                    // 55 -> 54 (batch 46): the same lane move as
                    // exec-passing 61 -> 60.
                    // 54 -> 52 (batch 57, 2026-09-04): the two flipped hybrid-
                    // milestoning union tests (repeat) row-verify as platform-arm
                    // verdicts (lane move, disagree 0).
                    // 52 -> 18 (batch 64, 2026-09-04): the same lane move as
                    // exec-passing 55 -> 21 — the ten chained
                    // testDataGeneration tests' 34 fetch-text asserts now
                    // row-verify as platform-arm verdicts (the oracle SPI's
                    // verifyFetchChain); their walk-lane rescues CLEARED.
                    // 18 -> 14 (batch 65, 2026-09-04): the same lane move as
                    // exec-passing 21 -> 17 — the four in-list temp-table
                    // tests' sql-asserts now row-verify as platform-arm
                    // verdicts (the oracle materializes tempTableForIn_N).
                    // 14 -> 11 (batch 66, 2026-09-05): the same lane move as
                    // exec-passing 17 -> 14 (testQualifier + two plan tests).
                    // 11 -> 9 (batch 69a, 2026-09-05): the union
                    // sqlQueryMerging pair's text-divergent RESCUES cleared —
                    // both tests flipped (the ^TDSNull() instance on the
                    // variant carrier); their asserts are platform-arm row
                    // verdicts now (disagree 0).
                    // 9 -> 7 (batch 72a, 2026-09-05): the same lane move as
                    // exec-passing 9 -> 7 — testBusinessDateInjectionFrom-
                    // VarReference's two assertSameSQL rescues cleared when
                    // its statement-root map unrolled and the test flipped.
                    com.legend.harness.H2Verify.M1_RESCUED.sum() >= 7,
                    "M1 h2-exec rescued fell below the 7 floor: "
                    + com.legend.harness.H2Verify.M1_RESCUED.sum());
            org.junit.jupiter.api.Assertions.assertTrue(
                    com.legend.harness.H2Verify.M1_UNVERIFIABLE.sum() <= 11,
                    "M1 h2-exec unverifiable grew past the 11 ceiling"
                    + " (leg-7 burndown is shrink-only): "
                    + com.legend.harness.H2Verify.M1_UNVERIFIABLE.sum());
        }
        System.out.println("[rcorpus] seed replay: "
                + Runner.SEED_CALLS.get() + " calls, "
                + (Runner.SEED_NANOS.get() / 1_000_000) + " ms");
        System.out.println("[rcorpus] seed split: ddl "
                + (Runner.DDL_NANOS.get() / 1_000_000) + " ms; raw jdbc "
                + com.legend.exec.Executor.RAW_CALLS.get() + " stmts, "
                + (com.legend.exec.Executor.RAW_NANOS.get() / 1_000_000)
                + " ms");
        System.out.println("[rcorpus] golden channel: "
                + (com.legend.harness.H2Verify.GOLDEN_NANOS.get() / 1_000_000)
                + " ms; xlate: "
                + (com.legend.sql.dialect.RawSqlBoundary.XLATE_NANOS.get() / 1_000_000)
                + " ms");
        System.out.println("[rcorpus] h2-mirror verify: "
                + (com.legend.harness.H2Verify.MIRROR_NANOS.get() / 1_000_000)
                + " ms");
        // TEMPORARY (2026-08-15): full wall reconciliation ledger
        com.legend.exec.TimingLedger.dump();
        // R1 canonical-byte-channel divergence table (CANONICAL_FORM_SPEC
        // §0) and the V7 dual-channel census (V7_ASSERT_VERDICT_CHARTER
        // §4.1) print ABOVE, before the lane guards.
        System.out.println("[rcorpus] walls (mappings + dropped base elements): "
                + runner.walls().size());
        if (System.getProperty("rcorpus.walls") != null) {
            runner.walls().forEach(w ->
                    System.out.println("[rcorpus] WALL " + w));
        }
        if (onlyFilters.isEmpty() && regressions.isEmpty()) {
            System.out.println("[rcorpus] scoreboard written to docs/RELATIONAL_CORPUS.md");
            // TYPED-IR Slice 1: the label-lie census over the whole
            // corpus sweep (instrument -> census -> flip)
            System.out.println("[rcorpus] sqltypes: "
                    + com.legend.exec.SqlTypeCensus.summary());
            // 20 -> 60 (TYPED-IR M1): the top-20 cut hid the mismatch
            // TAIL exactly when the flip needs every class adjudicable
            // (doctrine addendum: an instrument without a consumer is a
            // receipt without an audit — no silent caps on the review
            // surface). 60 -> 120 (N0, §4bZ-V E): the bottom-mult key
            // split by SHAPE — the smallest classes (6x DATE pads) must
            // stay visible for the machine count.
            com.legend.exec.SqlTypeCensus.classes(120).forEach(c ->
                    System.out.println("[rcorpus] sqltypes-class: " + c));
            com.legend.exec.SqlTypeCensus.allSamples().forEach((cls, ws) ->
                    ws.forEach(w -> System.out.println(
                            "[rcorpus] sqltypes-witness: " + cls + " :: "
                                    + w)));
            // §E3 M-N1 — the nullability differential (fact vs label),
            // census-first: the M-N3 flip's payload. Summary on the
            // console, full class/witness decomposition to target/
            // (the h2-verdicts dump idiom — attributable by diffing
            // two sweeps' files). No pin this slice: measured, then
            // adjudicated at M-N2/M-N3 (the converse-tripwire
            // precedent).
            System.out.println("[rcorpus] nullable-diff: "
                    + com.legend.exec.SqlTypeCensus
                            .nullableDifferentialSummary());
            // top classes ALSO on the console (the sqltypes-class
            // idiom): the target/ dump dies at gate 8's `-am clean`
            // (the TimingLedger lesson — a chain-run G4's file is
            // gone by chain end; the console line survives in g4.out)
            com.legend.exec.SqlTypeCensus.nullableDifferentialReport()
                    .stream().skip(1).limit(160).forEach(c ->
                            System.out.println(
                                    "[rcorpus] nullable-diff-class: "
                                            + c));
            try {
                java.nio.file.Files.writeString(
                        java.nio.file.Path.of("target",
                                "nullable-differential.txt"),
                        String.join("\n", com.legend.exec.SqlTypeCensus
                                .nullableDifferentialReport()) + "\n");
            } catch (java.io.IOException ignore) {
                // best-effort diagnostic (histogram precedent)
            }
            // §E3 SLACK CENSUS (the breach tripwire's converse,
            // post-flip precision instrument): nullable-labeled
            // columns that delivered values and never a NULL —
            // evidence, not proof (test-data dependent, deliberately
            // unpinned); ranks the precision refinements. Console
            // classes survive the chain (dump dies at gate 8's clean).
            System.out.println("[rcorpus] nullable-slack: "
                    + com.legend.exec.SqlTypeCensus.slackSummary());
            // §E3-S pad price tag: construction-event upper bound for
            // the WHERE≡INNER refinement (read flips; frame weakening
            // is now a derived fact of Join.outputs() — uncounted)
            System.out.println("[rcorpus] pad-weaken: reads="
                    + com.legend.sql.SqlTyping.PAD_READ_FLIPPED.sum());
            // §4AD navigation-arm census: blast radius of the
            // relational-conformance redesign as NAMED witness lists
            // (charter execution step 1) — console counts here, full
            // per-test lists in target/nav-arm-census.txt (the
            // h2-verdicts dump idiom)
            var navArms = com.legend.lowering.NavArmCensus.snapshot();
            StringBuilder navDump = new StringBuilder();
            navArms.forEach((arm, tests) -> {
                System.out.println("[rcorpus] nav-arm " + arm + ": "
                        + tests.size() + " tests");
                tests.forEach(t2 -> navDump.append(arm).append(' ')
                        .append(t2).append('\n'));
            });
            try {
                java.nio.file.Files.writeString(
                        java.nio.file.Path.of("target",
                                "nav-arm-census.txt"),
                        navDump.toString());
            } catch (java.io.IOException e) {
                System.out.println("[rcorpus] nav-arm dump failed: " + e);
            }
            com.legend.exec.SqlTypeCensus.slackReport().stream().skip(1)
                    .limit(160).forEach(c -> System.out.println(
                            "[rcorpus] nullable-slack-class: " + c));
            try {
                java.nio.file.Files.writeString(
                        java.nio.file.Path.of("target",
                                "nullable-slack.txt"),
                        String.join("\n", com.legend.exec.SqlTypeCensus
                                .slackReport()) + "\n");
            } catch (java.io.IOException ignore) {
                // best-effort diagnostic (histogram precedent)
            }
        }
        // MECHANICAL REGRESSION GATE (audit: this runner carried NO
        // asserts — BUILD SUCCESS regardless of outcome). Every family
        // run IN FULL must meet the committed per-family pass baseline;
        // improvements advance the baseline through the rewritten
        // scoreboard, regressions FAIL the build. Viable only since the
        // flapper elimination (deterministic runner — consecutive sweeps
        // identical). -Drcorpus.test runs skip: partial family counts.
        // computed above, BEFORE the write — see the gate-before-write note
        // ADVISORY-SQL CEILING (deep-audit H5: golden-SQL divergence could
        // not fail the build — structurally wrong SQL passed if one row
        // assert also passed). Down-only: improvements lower it here.
        if (onlyFilters.isEmpty()) {
            int advisorySqlDiffs = byFamily.values().stream()
                    .flatMap(List::stream)
                    .mapToInt(Runner.Outcome::sqlDiffs).sum();
            // measured 2026-08-12 (the deep-audit's 246 counted TESTS,
            // not diffs); +1 2026-08-16: ledger clusters 35/40 changed
            // advisory SQL shape on row-verified tests (expression
            // membership 'in (<expr>)', value-polymorphic Date literals)
            // — rows are the contract, both changes make rows RIGHT.
            // +1 2026-08-16 (batch c45/c51/c52/c53): a newly-flipped
            // row-verified pass carries divergent advisory SQL text
            // (net: pass 2336->2341, sqldiff-pass 246->247, zero
            // pass-count regressions).
            // +10 2026-08-21 (shortcut-audit Blocker 1, ADJUDICATED):
            // the null-drop moved into the compiler — value-collection
            // egress now emits WHERE <cell> IS NOT NULL. The engine
            // performs the SAME drop CLIENT-side (SQLNull -> [] in
            // relationalMappingExecution.pure:480), so its golden text
            // structurally cannot carry the filter; the 10 diffs are
            // that one clause on row-verified tests (functions/tests 8,
            // mapping/join 1, aggregationAware/NOP 1 — witness:
            // testAssociationToManyAutoMap). Rows identical everywhere;
            // pass baseline unchanged at 2332.
            // +3 2026-08-25 (§4bZ-U leg 2, JUSTIFIED): scalar-typed
            // collection egress boxes as [e] before the compact+UNNEST
            // (the bare-scalar list_filter cannot BIND — DuckDB binder
            // receipt on testSubAggregationMultiLevel's lateral) — the
            // boxed spelling diverges from engine golden text on 3
            // row-verified tests; rows are the contract, all pass
            // counts unchanged, corpus untyped hit 0 with this slice.
            // +6 2026-08-28 (slice 3 predicate real-evaluation,
            // JUSTIFIED): six fragment-check predicates (contains
            // 'union_gen_source_pk_0' etc.) now EVALUATE and record
            // their dialect divergence here instead of being invisible
            // advisory skips — strictly more information, rows verified
            // by the same tests' row asserts (charter §4Z addendum)
            // 318 -> 157 -> 76 (diff-noreplay burndown 2026-08-28, down-only
            // ratchet): 161 divergent-text sql asserts converted to
            // row-verified rescues — their diffs now ride the rescue
            // channel (counted, visible), not the advisory-diff channel
            int maxAdvisorySqlDiffs = 76;
            org.junit.jupiter.api.Assertions.assertTrue(
                    advisorySqlDiffs <= maxAdvisorySqlDiffs,
                    "advisory golden-SQL diffs grew: " + advisorySqlDiffs
                            + " > ceiling " + maxAdvisorySqlDiffs);
            System.out.println("[rcorpus] advisory sql diffs: "
                    + advisorySqlDiffs + " (ceiling " + maxAdvisorySqlDiffs + ")");
            // LIVE SOFT-PASS CEILINGS (audit-of-audits #13):
            // CorpusSoftCeilingTest read the COMMITTED markdown while
            // the corpus never runs in CI — it could not go red on a
            // live regression, binding only through the human commit
            // loop. The ceilings now bind HERE, against THIS sweep's
            // own outcomes, and that test is DELETED. Down-only; bump
            // only with a written justification in the same commit
            // (2026-08-21 adjudication set sqldiff 257 / adv 303).
            java.util.List<Runner.Outcome> passes = byFamily.values().stream()
                    .flatMap(java.util.List::stream)
                    .filter(o -> o.status() == Runner.Status.PASS)
                    .toList();
            final long softDiff = passes.stream()
                    .filter(o -> o.sqlDiffs() > 0).count();
            final long softAdv = passes.stream()
                    .filter(o -> o.advisory() > 0).count();
            final long softZero = passes.stream()
                    .filter(o -> o.detail().startsWith("0 asserts")).count();
            final long softRescued = passes.stream()
                    .filter(o -> o.rescued() > 0).count();
            // 257/303 -> 258/304 (§4bZ-U leg 2, 2026-08-25, JUSTIFIED
            // with the advisory-ceiling move in the same commit): the
            // scalar-typed collection egress boxes as [e] (the bare
            // scalar could not BIND under list_filter), so previously
            // byte-exact/advisory-clean passes now differ from engine
            // golden text by exactly that wrap; rows verified, corpus
            // untyped 0.
            // 258 -> 264 (slice 3 predicate real-evaluation 2026-08-28,
            // JUSTIFIED with the advisory-ceiling move in the same
            // commit): six fragment-check predicates now EVALUATE and
            // their tests pass CARRYING a recorded divergence instead
            // of an invisible advisory skip — no exact pass demoted
            // (exec-passing 989 and the pass total unchanged).
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            softDiff <= 264, "sqldiff-pass grew: " + softDiff
                                    + " > 264 — exact passes may have been"
                                    + " demoted; bump only with written"
                                    + " justification"),
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            softAdv <= 304, "adv-pass grew: " + softAdv
                                    + " > 304"),
                    // 27 -> 29 (bind-once family B, 2026-09-01, JUSTIFIED):
                    // mayExecuteAlloyTest folds to its |true no-server
                    // fallback at the CHECKER (MayExecuteChecker, walk
                    // parity alloyFallback) — the two flipped tdsJoin
                    // alloy tests pass VACUOUSLY exactly as the engine's
                    // serverless CI passes them; no assert was demoted.
                    // 29 -> 30 (TDG catalog-spelling burn, JUSTIFIED):
                    // testAlloyTestDatGenForNestedViews is an ASSERT-FREE
                    // alloy body; the walk SHAPE-failed it ("no verifying
                    // assertions" — harness policy), the flip now runs
                    // the whole body on the platform and it completes —
                    // the engine's own semantics for assert-free tests.
                    // Counted here, never a silent promotion.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            softZero <= 30, "0-assert passes grew: "
                                    + softZero + " > 30"),
                    // 613 -> 614 (2026-08-23, relation wall burn): a
                    // PREVIOUSLY-FAILING test (modelJoin testChainedTwoHops)
                    // now PASSES — the aggregate-ORDER-BY hoist kept its
                    // declared null placement (pure DESC null-largest:
                    // 'Apple,null' leads), and its exec text differs from
                    // the golden by exactly that semantic clause, so the
                    // pass carries the rescue flag. Corpus 2332 -> 2333;
                    // a gained pass, not text decay.
                    // 614 -> 751 -> 816 (diff-noreplay slices 1-4,
                    // JUSTIFIED with the advisory-ceiling drop 318->157
                    // in the same commit): Graph-frame replay upgrades
                    // divergent-text advisory skips on PASSING tests to
                    // counted row-verified rescues — the flag moved from
                    // the advisory channel to the rescue channel on the
                    // same tests; no exact pass was demoted (exec-passing
                    // 990 -> 1276, pass baselines unchanged).
                    // 816 -> 823 (§4AD batch 5, THE ROUTER FLIP —
                    // JUSTIFIED with exec-passing 1,387 -> 1,396 in the
                    // same commit): the lift's per-occurrence bundled
                    // frames are row-equal to the engine's flat form by
                    // LEFT-join associativity but text-divergent (nested
                    // vs flat bundling) — 7 passes moved byte-match ->
                    // row-verified rescue; zero passes demoted. Text
                    // re-convergence = the emission-anatomy leg (mirror
                    // the engine's frame shape), not a routing concern.
                    // 823 -> 861 (§4AD P0.5, the corrected unpark —
                    // JUSTIFIED by exec-passing 1,396 -> 1,448 and
                    // unable-to-exec 97 -> 45 in the SAME commit): 38
                    // formerly-unverifiable value-frame passes now
                    // carry the ROW-VERIFIED rescue flag —
                    // verification gained, not text decayed.
                    // 861 -> 863 (§4AD P2): the filter-position PAD
                    // GUARD (the conjoined qualifier pred) is SEMANTIC
                    // text — 2 byte-matched asserts became
                    // row-verified rescues (0 diverged; measured
                    // matched 466->464, rescued 929->931; ATTRIBUTED:
                    // filterFunctionExpressionWithConditionOnLeftAnd-
                    // RightTable + ...WithAndConditionOnRootAndRight-
                    // Table — engine spells the pred ONCE in the ON,
                    // ours ALSO guards the WHERE).
                    // 863 -> 864 (§4AD task #72): the un-walled
                    // testInputNotIsolatedWhenPropertyPathIsToOne passes
                    // CARRYING the row-verified rescue (ERROR -> PASS,
                    // functions/tests 241 -> 242, corpus 2,353 -> 2,354).
                    // 864 -> 871 (sql-exec burn 2026-08-30, JUSTIFIED
                    // by exec-passing 1,449 -> 1,459 + unable-to-exec
                    // 45 -> 35 in the same commit): 7 stitch-key tests'
                    // passes now carry the ROW-VERIFIED rescue —
                    // verification gained, not text decayed.
                    // 871 -> 877 (enum include-traversal, JUSTIFIED by
                    // exec-passing 1,459 -> 1,467 + unable-to-exec
                    // 35 -> 27 in the same commit): 6 enum tests'
                    // passes now carry the ROW-VERIFIED rescue.
                    // 877 -> 881 (tempTable chained replay, JUSTIFIED
                    // by exec-passing 1,467 -> 1,475 + unable-to-exec
                    // 27 -> 21 in the same commit): 4 tempTable tests'
                    // passes now carry the ROW-VERIFIED rescue.
                    // 881 -> 896 (TDG 49er replay, JUSTIFIED by
                    // exec-passing 1,475 -> 1,495 + unable-to-exec
                    // 70 -> 50 in the same commit): 15 generateTestData
                    // TESTS' passes now carry the ROW-VERIFIED rescue.
                    // UNITS CAUTION (audit-corrected): this counter is
                    // per-TEST (rescued() > 0); exec-passing is
                    // per-ASSERT — they do not subtract, and how many
                    // of the 20 asserts byte-matched is unmeasured.
                    // 896 -> 898 (chained-fetch live-session refereeing,
                    // JUSTIFIED by exec-passing 1,497 -> 1,526 +
                    // unable-to-exec 50 -> 21 in the same commit): 2
                    // more TESTS' passes carry the ROW-VERIFIED rescue
                    // (testTableToTdsWithConcatenate — root-sort
                    // platform fix — and testQualifier's H2Compatible
                    // hop 0; the 24 chained asserts sit in tests already
                    // counted rescued).
                    // 898 -> 899 (2026-08-31 embedded-union nav lift):
                    // testDataGeneration testUnionToUnion ERROR -> PASS;
                    // its assertSqlEquals rows-verify against our
                    // differently-spelled union SQL (the standard rescue
                    // lane for engine-SQL-text asserts).
                    // 899 -> 900 (same burn, witness #4 slot-chain):
                    // testUnionToUnionJoinSequenceWithMultipleChildrenIn-
                    // UnionSourceTree's assertSameSQL likewise
                    // row-verifies.
                    // 900 -> 901 (row-13 adjudication burn 2026-09-01,
                    // JUSTIFIED by exec-passing 1527 -> 1528 +
                    // unable-to-exec 21 -> 20 in the same commit):
                    // testQualifierQueryWithOr's text-divergent assert
                    // now ROW-VERIFIES via the EXTENT_SUBSET pk-collapse
                    // — its pass trades the sqldiff (13 -> 12) and
                    // advisory (15 -> 14) softness for the rescue flag.
                    // 901 -> 165 EARNED SHRINK (effectful cutover +
                    // the 3b-3d flip slices behind it): flipped tests'
                    // sql asserts judge on ROWS at the arm — text
                    // divergence is emission census there, never a
                    // softness flag; the rescue flag now marks only
                    // walk-scored tests. Ceiling ratcheted to measured.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            softRescued <= 165, "text-rescued passes grew: "
                                    + softRescued + " > 165"),
                    // contract-program wire ratchets (RE-PINNED at the
                    // 2026-08-24 label flip: 181->114->56 and 130->13 —
                    // adopted HUGEINT labels, registered carriages, then
                    // the pure-Decimal erasure ADOPTION (labels take the
                    // wire's own precision: 58 more exact wire matches);
                    // deterministic counts, ratcheted to measured).
                    // 56 -> 7 (T4 attempt 2, charter §4bR Slice A): the
                    // concrete-Float-over-DECIMAL conform cast at the
                    // MappingNormalizer pairing seam — the 48 DOUBLE<>
                    // DECIMAL(18,6) rows (mapping::dataType family) now
                    // speak DOUBLE on the wire.
                    // 7 -> 4 (the wire-7 review, 2026-08-25): the 3
                    // HUGEINT<>DOUBLE rows healed (SUM tolerance
                    // transport). 4 -> 2 (§4bZ-U leg 4, 2026-08-25):
                    // the fetchDb catalog grids got their DECLARED
                    // JDBC-spec schemas (CatalogGrids.gridSchema) — the
                    // 2x JSON<>VARCHAR SQL_TYPE_NAME rows healed to
                    // typed VARCHAR labels. 2 -> 0 (§4bZ-U ruling,
                    // 2026-08-25 — "burn 2 and 3 to zero"): late-bound
                    // frames DID learn runtime schemas — a by-name
                    // FIELD read now DEMANDS the LIMIT-0 probe
                    // (RawGridSchema's widened gate) and the probe
                    // carries the database's own column types
                    // (GridProbe.probeTypedColumns), so the
                    // dropAndCreateTable cells label BIGINT and the
                    // wire agrees. The bare .rows egress stays
                    // single-query (ExecuteInDbProbeCountTest pins
                    // both sides). HARDENED TO EQUALITY at zero.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .wireDivergeCount(),
                            "corpus wire divergence reappeared: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // adopt-pending burned 130 -> 13 -> 0 (the label
                    // flip + the arithmetic promotion rules): every
                    // integer-aggregate/arith label now speaks its
                    // wire. Hardened to EQUALITY at zero.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .wireAdoptPendingCount(),
                            "wire adopt-pending reappeared: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // THE GUEST LIST (charter §4bZ, 2026-08-25): the
                    // two blanket coercion arms are DELETED; a label/
                    // wire mismatch is tolerated ONLY for reads tagged
                    // at the mapping seam (a declared property/column
                    // kind mismatch — engine carry-through compat).
                    // First audit sweep: ALL 111 arrived tagged (97
                    // VARCHAR + 14 DOUBLE — the sampled attribution is
                    // now machine-proven row-by-row), and the pardon's
                    // deletion EXPOSED 20 hidden rows (the CEILING
                    // rule-vs-emission lie, fixed same sweep). Ceiling:
                    // growth = a new mismatched mapping or an accident
                    // — justify in the commit either way.
                    // 111 -> 153 (wire-7 review, JUSTIFIED move): SUM
                    // transports the tolerance, so sum-over-a-tagged-
                    // read keeps the PURE contract label (Float ->
                    // DOUBLE) instead of adopting the stamp promotion
                    // (HUGEINT) — the 3 testReprocessGroupByAlias wire
                    // rows healed (fixture-skew FLOAT wires genuinely
                    // sum to DOUBLE), +33 DOUBLE<-HUGEINT + 9 equal-
                    // pair propagation slots joined the registered
                    // guest list; row verdicts and scoreboard
                    // byte-stable.
                    // 153 SPLIT BY PROVENANCE (§4Z ledger #1 repin,
                    // 2026-08-26; refined same day to a SHAPE split —
                    // the pair alone cannot tell a seam read from an
                    // aggregate over one, and the machine count showed
                    // even the audited "111" hid 3 aggregate rows):
                    // ORIGIN 108 = bare COLUMN READS with a differing
                    // pair — one row per real mapping-seam kind
                    // mismatch (97 VARCHAR<-BIGINT + 11
                    // DOUBLE<-BIGINT); growth here is a NEW mismatched
                    // mapping, a model fact that must be justified in
                    // the commit. DERIVED 36 = operations over tagged
                    // reads keeping the pure contract label (33 SUM
                    // DOUBLE<-HUGEINT — the wire-7 transport family —
                    // + 3 MAX-style DOUBLE<-BIGINT); moves with
                    // aggregate shapes. TRANSPORTED 9 = equal-pair
                    // propagation slots (DOUBLE<-DOUBLE) — plumbing
                    // that grows with query shape only.
                    // 108/36/15/56 -> 122/68/27/63 (slice-3 equality
                    // half, 2026-08-28, JUSTIFIED as ONE move): the
                    // position-independent toSQLString fold compiles 94
                    // previously-walled sqltext asserts (+~1000 plans);
                    // the tolerance slots are PER-PLAN counts over the
                    // SAME registered seam kinds on the SAME mappings —
                    // more plans reading a known seam, not new model
                    // facts. The EQUALITY-0 quality gates (mismatch,
                    // wire diverge, null-breach, unknown) all HELD in
                    // the same sweep.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            com.legend.exec.SqlTypeCensus
                                    .toleratedOriginCount() <= 122,
                            "mapping-seam ORIGIN tolerated slots grew"
                                    + " (a new mismatched mapping?): "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            com.legend.exec.SqlTypeCensus
                            // §8.3b: 308 migrated tests' asserts are
                            // PRIMARY platform executions now — their
                            // plans joined this census with the flip
                                    .toleratedDerivedCount() <= 78,
                            "tolerance-derived slots grew (an op over"
                                    + " a tagged read): "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // 9 -> 15 (§4bZ-V D1, 2026-08-26, JUSTIFIED): the
                    // D1 value-evidence tripwire exposed 6 valued
                    // INTEGER wires under VARCHAR labels
                    // (testSQLQueryMergingForInnerJoins2's
                    // String-declared p3 over dTable.pk) — the member
                    // frames carried the mapping-seam tag all along,
                    // but SqlUnion's ctor rebuilt outputs from the
                    // pure contract and DROPPED it; union-label
                    // reconciliation now transports tag/type/
                    // nullability across the union node, so the six
                    // slots land here (equal-pair plumbing) and their
                    // wire rows move diverge -> tolerated.
                    // 27 -> 36 (whole-test flip default-on): the +9 are
                    // VARCHAR<-VARCHAR equal-pair slots (witness
                    // testSimpleDistinct id := id) — this guard's own
                    // "grows with query shape only" class; whole-body
                    // plans carry extra pass-through projections.
                    // 40 -> 46 (charter §8.3c): the 541-flip's
                    // whole-body plans carry more pass-through
                    // projections — the same equal-pair plumbing class
                    // as the flip-default-on move; quality gates
                    // (mismatch, diverge, null-breach, unknown) all 0
                    // in the same sweep.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            com.legend.exec.SqlTypeCensus
                                    .toleratedTransportedCount() <= 46,
                            "tolerance-transport slots grew: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // D2 (§4bZ-V, 2026-08-26): every wire probe must
                    // adjudicate — the two old unknowns were
                    // zero-output no-claim frames (now skipped by
                    // doctrine); a NEW unknown is an unreadable or
                    // shape-broken probe, classed + witnessed in the
                    // failure. EQUALITY at zero.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .wireUnknownCount(),
                            "unadjudicated wire probes appeared: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // D1 (§4bZ-V, 2026-08-26): int-or-null settled by
                    // VALUE evidence — every row here is a PROVEN
                    // all-NULL column (no factual wire type exists;
                    // DuckDB spells it INTEGER); a valued column lands
                    // in diverge (EQUALITY-0 above) instead. Ceiling —
                    // grows only with all-NULL result columns (query
                    // shape), ratchet down as shapes burn.
                    // 63 -> 64 (whole-test flip default-on): one more
                    // proven-all-NULL result column — this ceiling's own
                    // query-shape class. 64 -> 67 (§8.3b, the 308-test
                    // migration): three more all-NULL result columns
                    // from migrated tests' now-primary assert-side
                    // executions — same query-shape class. 67 -> 87
                    // (batch 19 — function bodies as rows, 2026-09-03):
                    // FunctionDefinition.expressionSequence registered
                    // (real m3) types testConcatenationOfTemporalTdsQueries
                    // (+WithGroupBy)'s expressionSequence reads, so their
                    // platform attempts now reach execution — the same three witnesses
                    // (testMilestoningColumnProjectionWithNonMilestonedTable's
                    // from/thru NullLit projections, testPywaDateRange,
                    // testFetchDbPrimaryKeysMetaData), more probes of the
                    // same all-NULL shapes; no verdict moved.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            com.legend.exec.SqlTypeCensus
                                    .wireIntOrNullEmptyCount() <= 87,
                            "proven-empty int-or-null columns grew: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // THE LABEL FLIP (TYPED_SQL_IR.md, 2026-08-24):
                    // reconciliation makes a label lie structurally
                    // impossible — the census's mismatch bucket is
                    // EMPTY by construction, pinned as the completed
                    // label-lie program (instrument -> census -> flip).
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .mismatchCount(),
                            "a label lie escaped reconciliation: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // POST-JUDGE TRIPWIRE (TYPED_SQL_IR.md, judge
                    // deleted 2026-08-24): untyped projection roots =
                    // rule-coverage debt AND the leaf-regression
                    // signal (a new unstamped construction site GROWS
                    // this). 1,116 -> 737 -> 717 -> 424 (M4) -> 24
                    // (rules burn) -> 4 (2026-08-25 FULL burn: the
                    // SPLIT->VARCHAR[] rule closed the 20-row XStore
                    // family) -> 0 (§4bZ-U legs, 2026-08-25: fetchDb
                    // grids got DECLARED JDBC-spec schemas; the
                    // scalar-typed collection egress boxes as [e] —
                    // the bare-scalar list_filter could not even BIND,
                    // so the subagg-lateral and concatenate roots were
                    // unbindable emissions the census had been
                    // flagging as type debt). Hardened to EQUALITY at
                    // zero — a new untyped root is a regression, with
                    // its witness in the failure message.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .untypedCount(),
                            "untyped projection roots reappeared — a"
                                    + " missing rule or an unstamped leaf: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // THE NULLABILITY LEDGER (§4bZ-V E, 2026-08-26 —
                    // §4Z ledger #4, the last open burn-down ledger):
                    // N0 machine-counted the 6,472-row backlog as 100%
                    // literal NullLit union-member pads (5707 BIGINT +
                    // 598 VARCHAR + 161 BOOLEAN + 6 DATE — member key
                    // pads, stc_* subtype columns, bitemporal member
                    // columns); N1 made a projected literal NULL
                    // declare its slot nullable at construction
                    // (reconcileLabels), burning 6,472 -> 0 with every
                    // other census bucket byte-identical. Residue
                    // adjudicated EMPTY — EQUALITY at zero on both
                    // corpus lanes: a row here is a COMPUTED bottom (a
                    // NULL-propagating expression) under a required
                    // label, witness in the failure message.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .bottomMultCount(),
                            "computed NULL under a required-multiplicity"
                                    + " label: "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // G3 (§4bZ-V G3, 2026-08-26): the fixture-skew
                    // census PROMOTED to a pinned ceiling — 469
                    // measured, +4 from the two recorded undercount
                    // fixes (schema-qualified creates, constraint-word
                    // columns) = 473. RE-BASED 473 -> 782 with the
                    // slice-1 census re-scope (§10h): the declared side
                    // now walks EVERY database in the global model
                    // (module-DDL scope deleted) — same instrument,
                    // wider honest denominator. Engine test-data debt
                    // (docs/UPSTREAM_DEFECTS.md U19); growth = a new
                    // contradicting fixture, shrink = upstream fixes —
                    // ratchet down.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            Runner.FIXTURE_SKEW.values().stream()
                                    .mapToLong(java.util.Set::size)
                                    .sum() <= 782,
                            "fixture-skew columns grew past the pinned"
                                    + " ceiling 782 — a new declaration-"
                                    + "contradicting CREATE in the setup"
                                    + " streams"),
                    // [1]-OVER-NULLABLE census (typed-IR queue item 2,
                    // 2026-08-26): 520 measured (487 direct + 33
                    // join-terminal) class-mapped required properties
                    // over columns the store leaves nullable — the
                    // engine-fixture model debt the future dialect-split
                    // warning will name, and the static slice of the
                    // 925 wire-breach census. Ceiling, down-only.
                    // 520 -> 529 (metamodel-as-relations group F burn,
                    // 2026-09-02): the metamodel store's SINGLE-TABLE
                    // hierarchies — datatype kinds over data_types
                    // (Varchar/Char/Binary/Varbinary.size, Decimal/
                    // Numeric.precision+scale) and DynaFunction.name over
                    // relational_ops — one ~filter per subclass set (the
                    // engine idiom): each column is non-null on every row of
                    // its own set and NULL on the other kinds' rows by
                    // construction, so the [1] declaration is exact and the
                    // column is nullable at the table level. 9 witnesses,
                    // all "direct", all metamodel-store.
                    // 529 -> 533 (single-table RelationalOperationElement
                    // hierarchy, 2026-09-02, user-ratified): Table, View,
                    // Column and TableAlias `name` [1] now read the shared
                    // relational_elements.name, NULL on the expression-node
                    // kinds' rows — the same idiom, four more witnesses.
                    // 533 -> 534 (plan nodes AS ROWS, batch 18, 2026-09-03):
                    // SQLExecutionNode.sqlQuery [1] over the single-table
                    // plan_nodes.sql_query, NULL on the other node kinds'
                    // rows — the same idiom, one more witness.
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            reqNullAdjudicated() <= 534,
                            "required-over-nullable pairings grew past"
                                    + " the pinned ceiling 534 — a new"
                                    + " [1]-property over a nullable"
                                    + " column entered the corpus"
                                    + " models"),
                    // the census's own blindness must not grow: honesty
                    // buckets (unresolved property/column lookups) pin
                    // at 97 (55 column + 42 property, association-end
                    // injections and scope-block reads) — growth means
                    // the instrument stopped seeing pairings it used to
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            Runner.REQUIRED_OVER_NULLABLE.entrySet()
                                    .stream()
                                    .filter(e -> e.getKey()
                                            .startsWith("unresolved-"))
                                    .mapToLong(e -> e.getValue().size())
                                    .sum() <= 97,
                            "required-over-nullable HONESTY buckets grew"
                                    + " past 97 — the census is going"
                                    + " blind on pairings it cannot"
                                    + " adjudicate"),
                    // E2E-AUDIT CONVERSE CENSUS → §E3 M-N3 TRIPWIRE
                    // (2026-08-27): labels adopt slot-truth nullability
                    // at construction (TypeFact.nullable through the
                    // fact funnel: DDL base frames + join-pad
                    // provenance + probed composition rules + GROUP-BY
                    // refinement), so a wire NULL under a
                    // nullable=false label is a COMPILER BUG — the
                    // fact's never-null proof was false. Burn-down:
                    // 925 measured (E2E audit) -> 841 (M-N2 pad
                    // weakening) -> 0 (the flip; the 605 tightened
                    // over-declared labels produced ZERO breaches —
                    // the DDL proofs held). EQUALITY at zero, always
                    // loud, witnesses in the failure.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0, com.legend.exec.SqlTypeCensus
                                    .nullBreachCount(),
                            "wire NULL under a never-null label (a fact"
                                    + " proof was false): "
                                    + com.legend.exec.SqlTypeCensus
                                            .summary()),
                    // §E3 M-N3: the fact-vs-label differential is a
                    // CONSTRUCTION INVARIANT post-flip (reconciled
                    // labels ARE slotNullable) — a nonzero row means a
                    // frame door bypassed reconciliation or a rebuild
                    // dropped adopted labels. EQUALITY at zero.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            0L, com.legend.exec.SqlTypeCensus
                                    .nullableUnderDeclaredCount()
                                    + com.legend.exec.SqlTypeCensus
                                            .nullableOverDeclaredCount(),
                            "label nullability diverged from slot truth: "
                                    + com.legend.exec.SqlTypeCensus
                                            .nullableDifferentialSummary()),
                    // R1b census pin (CANONICAL_FORM_SPEC §0, measured
                    // 2026-08-22): 27 grid-text verdicts pass only via
                    // the kept leniencies — 6 row-order-only (R2's
                    // canonical ORDER BY burns them) + 21 cross-engine
                    // float arithmetic (H2 decimal vs DuckDB binary —
                    // VALUE differences, the declared numeric policy).
                    // Shrink-only; a bump means a byte-exact verdict
                    // regressed to leniency.
                    () -> org.junit.jupiter.api.Assertions.assertEquals(
                            21, com.legend.exec.CanonicalDivergence
                            // 27 -> 29 (§8.3b): +2 members of the
                            // SAME cross-engine float-print class
                            // (0.5131...013 vs ...014 grid cells) from
                            // migrated tests' newly-platform-judged
                            // asserts — the ULP/canon lane's known
                            // population, host verdicts hold.
                            // 29 -> EXACT 26 (§8.3b wobble burn): the
                            // run-to-run ±1 was row-ORDER drift on
                            // UNORDERED chains (testProjectThroughAsso
                            // — arrival order of a no-ORDER-BY union
                            // is undefined; SQL byte-identical across
                            // JVMs, receipts in the landing record).
                            // The byte channel now judges unordered
                            // grids under the verdict's own declared
                            // row-multiset policy (probeGridText,
                            // sorted-gated, CSVJOIN one-line grids
                            // normalized to rows first — that spelling
                            // had been hiding row-order drift as
                            // cell-diff@line0), which moved every
                            // order-only row to content-agree and made
                            // this census DETERMINISTIC: paired sweeps
                            // byte-identical at 23 = the 21-row
                            // cross-engine float class + 2
                            // assertEquals. Pinned EXACT: any move,
                            // either direction, demands attribution.
                            // 23 -> 21 (§8.3c): probeEqual gained the
                            // SAME compile-time gate (OrderView
                            // .INCIDENTAL -> two-sided sorted compare)
                            // after 3c's flips surfaced 9 value-list
                            // rows whose payloads (e<11.0> a<25.0>)
                            // were positional drift the host had
                            // lawfully disregarded — and the payloads
                            // exonerated the 2 OLD assertEquals rows
                            // as the same class. The roster is now
                            // PURE calendarAggregations float-print
                            // (all 21 rows named, sub-ULP arithmetic).
                                    .disagreeCount(),
                            "canonical-byte divergence moved: "
                                    + com.legend.exec.CanonicalDivergence
                                            .summary() + " (exact pin 21)"),
                    // V1 (OPEN_REGISTER): the DUAL-VERDICT alarm — the
                    // DB byte verdict and the host referee may NEVER
                    // disagree silently; any disagreement fails the
                    // sweep with the census line
                    () -> org.junit.jupiter.api.Assertions.assertTrue(
                            com.legend.exec.CanonicalDivergence
                                    .sqlDisagreeCount() == 0,
                            "DUAL-VERDICT DISAGREEMENT: "
                                    + com.legend.exec.CanonicalDivergence
                                            .summary()
                                    + " witnesses="
                                    + com.legend.exec.CanonicalDivergence
                                            .sqlDisagreeSamples()));
            System.out.println("[rcorpus] soft ceilings: sqldiff " + softDiff
                    + "/258, adv " + softAdv + "/304, 0-asserts " + softZero
                    + "/30, rescued " + softRescued + "/165");
        }
        org.junit.jupiter.api.Assertions.assertTrue(regressions.isEmpty(),
                "CORPUS REGRESSION vs committed docs/RELATIONAL_CORPUS.md: "
                + regressions
                + " — the scoreboard was NOT rewritten and the committed file is"
                + " intact, so there is nothing to revert. Fix the regression, or"
                + " check that -Dlegend.engine.root points at the checkout the"
                + " baseline was generated against.");
    }

    /** The committed scoreboard's per-family PASS counts, parsed BY
     * HEADER NAME — a positional cells[3] read meant any column inserted
     * before it silently degraded the regression gate to green
     * (H2_BACKEND.md §10). Empty (gate skipped, loud) when the file is
     * absent, unreadable, or carries no recognizable 'pass' column. */
    private static Map<String, Integer> readBaseline(Path p) {
        Map<String, Integer> m = new LinkedHashMap<>();
        int passCol = -1;
        try {
            for (String line : java.nio.file.Files.readAllLines(p)) {
                if (!line.startsWith("| ")) {
                    continue;
                }
                String[] cells = line.split("\\|");
                if (line.startsWith("| family")) {
                    for (int i = 0; i < cells.length; i++) {
                        if (cells[i].trim().equals("pass")) {
                            passCol = i;
                        }
                    }
                    continue;
                }
                if (passCol < 0 || cells.length <= passCol
                        || line.contains("**total**")) {
                    continue;
                }
                try {
                    m.put(cells[1].trim(),
                            Integer.parseInt(cells[passCol].trim()));
                } catch (NumberFormatException ignore) {
                    // separator / non-table rows
                }
            }
            if (passCol < 0) {
                throw new IllegalStateException("baseline has no 'pass'"
                        + " header — the regression gate would fail OPEN"
                        + " and the sweep would still WRITE (PX.1; audit"
                        + " §5.1). Fix docs/RELATIONAL_CORPUS.md.");
            }
        } catch (java.io.IOException e) {
            throw new IllegalStateException("baseline unreadable — the"
                    + " regression gate would fail OPEN and the sweep"
                    + " would still WRITE (PX.1; audit §5.1): " + e, e);
        }
        return m;
    }

    /** ONE family through the pipeline — shared by the scoreboard and the
     * family-scoped fast sweep (FamilySweep probe): the family/test-file
     * split, parent setUp/store-only inheritance, module assembly, and the
     * per-test run. */
    public static List<Runner.Outcome> runFamily(Runner runner, String family)
            throws Exception {
        return runFamily(runner, family, registerFamily(runner, family));
    }

    /** RUN phase (two-phase compile-once protocol): every family is
     * already registered; re-point the runner and execute. */
    public static List<Runner.Outcome> runFamily(Runner runner, String family,
            Map<Path, String> testSources) throws Exception {
        runner.selectFamily(family);
        List<Runner.Outcome> outcomes = new ArrayList<>();
        String onlyTest = System.getProperty("rcorpus.test", "").trim();
        // ONE session per family (task #112): seeds replay incrementally
        // inside it — the engine's per-package shared-server semantics
        runner.beginFamilySession();
        try {
            // Phase C: discovery through the REAL parser — stereotyped
            // functions off the parsed unit, body as AST.
            // ENGINE EXECUTION ORDER (PureTestBuilder.buildSuite):
            // sibling package suites sort by name and run BEFORE a
            // package's own tests, which sort by name — NOT source
            // declaration order (a declaration-first polluting INSERT
            // test poisoned 12 downstream tests; study §5.1, proven by
            // exact arithmetic, per-test sessions, and a name filter).
            List<Map.Entry<Path, Runner.ParsedTest>> ordered = new ArrayList<>();
            for (Map.Entry<Path, String> e : testSources.entrySet()) {
                for (Runner.ParsedTest t : Runner.discoverTests(e.getValue())) {
                    if (!onlyTest.isEmpty() && !t.fqn().contains(onlyTest)) {
                        continue;
                    }
                    ordered.add(Map.entry(e.getKey(), t));
                }
            }
            ordered.sort((a, b) -> engineSuiteOrder(a.getValue().fqn(),
                    b.getValue().fqn()));
            for (Map.Entry<Path, Runner.ParsedTest> e : ordered) {
                runner.selectFile(e.getKey().toString());
                outcomes.add(runner.run(e.getValue()));
            }
        } finally {
            runner.endFamilySession();
        }
        return outcomes;
    }

    /** PureTestBuilder.buildSuite's traversal as a comparator: compare
     *  package segments; at the first divergence sort alphabetically;
     *  an ANCESTOR package's own tests run AFTER its sub-suites (deeper
     *  fqn first); same package sorts by test name. */
    static int engineSuiteOrder(String fqnA, String fqnB) {
        String[] a = fqnA.split("::");
        String[] b = fqnB.split("::");
        int i = 0;
        while (i < a.length - 1 && i < b.length - 1 && a[i].equals(b[i])) {
            i++;
        }
        if (i < a.length - 1 && i < b.length - 1) {
            return a[i].compareTo(b[i]);
        }
        if (a.length == b.length) {
            return a[a.length - 1].compareTo(b[b.length - 1]);
        }
        return a.length < b.length ? 1 : -1;
    }

    /** REGISTRATION phase: assemble the family's source set (setups,
     * parent inheritance, cross-family closure) and register it with the
     * runner. Must run for EVERY family before the first test executes —
     * the global model compiles ONCE over the completed registry. */
    public static Map<Path, String> registerFamily(Runner runner,
            String family) throws Exception {
        Path p = Corpus.RELATIONAL.resolve(family);
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.list(p)) {
            s.filter(f -> f.toString().endsWith(".pure")
                    // the engine's own implementation files are SPEC
                    && !ENGINE_IMPLEMENTATION_FILES.contains(
                            Corpus.RELATIONAL.relativize(f).toString()))
                    .sorted().forEach(files::add);
        }
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
                if (ENGINE_IMPLEMENTATION_FILES.contains(
                        Corpus.RELATIONAL.relativize(f2).toString())) {
                    continue;   // the engine's own implementation is SPEC
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
        // CROSS-FAMILY DEPENDENCY CLOSURE: a Database include naming a db
        // DEFINED IN ANOTHER FAMILY's file pulls that file in MODEL-ONLY
        // (the engine compiles the whole PURE graph together; the pulled
        // elements compile, its tests do NOT run here). First-wins module
        // semantics keep this family's own elements on duplicate FQNs.
        {
            Set<String> defined = new HashSet<>();
            List<String> all = new ArrayList<>(familySources);
            all.addAll(testSources.values());
            for (String s2 : all) {
                collectDbNames(s2, defined);
                collectClassNames(s2, defined);
            }
            Deque<String> pending = new ArrayDeque<>(all);
            Set<Path> pulledFiles = new HashSet<>(files);
            while (!pending.isEmpty()) {
                String s2 = pending.poll();
                // the source's import packages — mapping files declare
                // class-mapping heads UNQUALIFIED (shared.pure: `_Person :
                // Relational` under import ...shared::dest::*)
                List<String> imps = s2.lines().map(String::strip)
                        .filter(l -> l.startsWith("import ")
                                && l.endsWith("::*;"))
                        .map(l -> l.substring(7, l.length() - 4))
                        .toList();
                for (String line : s2.lines().map(String::strip).toList()) {
                    List<String> wanted = new ArrayList<>();
                    java.util.regex.Matcher cmHead = java.util.regex.Pattern
                            .compile("^\\*?([\\w:]+)(\\[[\\w,]+\\])? *: *(Relational|Pure)\\b")
                            .matcher(line);
                    if (cmHead.find()) {
                        String cn = cmHead.group(1);
                        if (cn.contains("::")) {
                            wanted.add(cn);
                        } else {
                            for (String imp : imps) {
                                wanted.add(imp + "::" + cn);
                            }
                        }
                    }
                    if (line.startsWith("include ")) {
                        wanted.add(line.substring("include ".length())
                                .strip());
                    } else if (line.startsWith("Class ")
                            && line.contains(" extends ")) {
                        // cross-family EXTENDS closure — the validation
                        // corpus subclasses tests/milestoning classes;
                        // the superclass's file must compile alongside
                        for (String tok : line.substring(
                                line.indexOf(" extends ") + 9)
                                .split("[,\\[{]")) {
                            String t = tok.strip();
                            if (t.contains("::")) {
                                wanted.add(t);
                            } else if (!t.isEmpty()) {
                                // a supertype resolved via import
                                // wildcard (study §5.4b): try each
                                // import prefix — unknown candidates
                                // skip at the index lookup below
                                for (String imp : imps) {
                                    wanted.add(imp + "::" + t);
                                }
                            }
                        }
                    }
                    for (String fqn : wanted) {
                        if (defined.contains(fqn)) {
                            continue;
                        }
                        Path dep = dbIndex().get(fqn);
                        if (dep == null) {
                            dep = classIndex().get(fqn);
                        }
                        if (dep == null || !pulledFiles.add(dep)) {
                            continue;   // unknown stays a loud wall
                        }
                        if (System.getenv("LL_TMP_DEBUG") != null) {
                            System.err.println("[pull] " + fqn + " <- " + dep);
                        }
                        String depSrc = Files.readString(dep);
                        modelOnly.add(depSrc);
                        collectDbNames(depSrc, defined);
                        collectClassNames(depSrc, defined);
                        pending.add(depSrc);
                    }
                }
            }
        }
        runner.useFamily(family, familySources, modelOnly, parentKey);
        for (Map.Entry<Path, String> e : testSources.entrySet()) {
            runner.useFile(e.getKey().toString(), e.getValue());
        }
        return testSources;
    }

    /** Database FQNs defined in {@code src} (line-level indexing only —
     * the model itself still compiles through the platform). */
    private static void collectDbNames(String src, Set<String> out) {
        src.lines().map(String::strip)
                .filter(l -> l.startsWith("Database "))
                .forEach(l -> out.add(dbNameOf(l)));
    }

    private static String dbNameOf(String databaseLine) {
        return databaseLine.substring("Database ".length())
                .replace("(", " ").strip().split("\\s+")[0];
    }

    /** Class FQNs defined in {@code src} (line-level; stereotype block
     * tolerated between the keyword and the FQN). */
    private static void collectClassNames(String src, Set<String> out) {
        src.lines().map(String::strip)
                .filter(l -> l.startsWith("Class "))
                .forEach(l -> {
                    String n = classNameOf(l);
                    if (n != null) {
                        out.add(n);
                    }
                });
    }

    /** ADJUDICATED [1]-over-nullable pairings (the two real buckets;
     * honesty buckets excluded). */
    private static long reqNullAdjudicated() {
        return Runner.REQUIRED_OVER_NULLABLE.entrySet().stream()
                .filter(e -> e.getKey().equals("direct")
                        || e.getKey().equals("join-terminal"))
                .mapToLong(e -> e.getValue().size()).sum();
    }

    private static String classNameOf(String classLine) {
        String t = classLine.substring("Class ".length()).strip();
        if (t.startsWith("<<")) {
            int e = t.indexOf(">>");
            if (e < 0) {
                return null;
            }
            t = t.substring(e + 2).strip();
        }
        if (t.startsWith("{")) {
            // tagged-value block {doc.doc='...'}
            int e = t.indexOf('}');
            if (e < 0) {
                return null;
            }
            t = t.substring(e + 1).strip();
        }
        String n = t.split("[\\s\\[{(]")[0].strip();
        return n.contains("::") ? n : null;
    }

    /** Corpus-wide CLASS index: FQN -> defining file. */
    private static Map<String, Path> classIndexCache;

    private static Map<String, Path> classIndex() throws Exception {
        if (classIndexCache == null) {
            Map<String, Path> ix = new LinkedHashMap<>();
            for (Path root : java.util.List.of(Corpus.RELATIONAL,
                    Corpus.M2M_TESTS)) {
                if (!Files.isDirectory(root)) {
                    continue;
                }
                try (Stream<Path> s = Files.walk(root)) {
                    for (Path f : s.filter(x -> x.toString().endsWith(".pure"))
                            .sorted().toList()) {
                        for (String l : Files.readAllLines(f)) {
                            String t = l.strip();
                            if (t.startsWith("Class ")) {
                                String n = classNameOf(t);
                                if (n != null) {
                                    ix.putIfAbsent(n, f);
                                }
                            }
                        }
                    }
                }
            }
            classIndexCache = ix;
        }
        return classIndexCache;
    }

    /** Corpus-wide Database index: FQN -> defining file (first in sorted
     * walk order — deterministic across sibling duplicates). */
    private static Map<String, Path> dbIndexCache;

    private static Map<String, Path> dbIndex() throws Exception {
        if (dbIndexCache == null) {
            Map<String, Path> ix = new LinkedHashMap<>();
            try (Stream<Path> s = Files.walk(Corpus.RELATIONAL)) {
                for (Path f : s.filter(x -> x.toString().endsWith(".pure"))
                        .sorted().toList()) {
                    for (String l : Files.readAllLines(f)) {
                        String t = l.strip();
                        if (t.startsWith("Database ")) {
                            ix.putIfAbsent(dbNameOf(t), f);
                        }
                    }
                }
            }
            dbIndexCache = ix;
        }
        return dbIndexCache;
    }
}
