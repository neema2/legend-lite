// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SKIP CENSUS (P3-5, phases-3 audit): a skipped test is a claim the
 * suite quietly stops making — so every skip is REGISTERED, named, and
 * shrink-only, the same discipline as every other ledger. Two channels:
 *
 * <ol>
 * <li>{@code @Disabled} rows — each must carry a {@code GAP: ...} reason
 * (the named feature gap it waits on); the per-file count is pinned MAX
 * (burn a gap, tighten the pin — never add a skip without a pin bump
 * and a written justification here).</li>
 * <li>{@code Assumptions.assume*} sites — environment-conditional
 * skips; the FILE SET is pinned exactly (a new assumption-skipping file
 * is a new way for the suite to go quiet).</li>
 * </ol>
 */
class SkipCensusTest {

    /** file basename -> pinned MAX {@code @Disabled(} count. */
    private static final Map<String, Integer> DISABLED_PINS = Map.of(
            // the 15 named grammar/builder GAP rows (2026-08-19 census):
            // extends clause, store substitution x2, scope keyword,
            // Database filters, local property prefix, Relation class
    // mapping, set IDs, extends+filter, include+join, view+join+filter,
            // filter stacking, local property+join+filter, scope+embedded,
            // AggregationAware+join
            "RelationalMappingIntegrationTest.java", 15);

    /** Files permitted to carry {@code Assumptions.assume*} sites. */
    private static final List<String> ASSUMPTION_FILES = List.of(
            // the minimal harness (2026-09-06): skips only when the engine
            // checkout is absent — gate 4 runs it explicitly
            "MinimalCorpusTest.java",
            // skips when the generated expected/ dir is absent — the
            // differential needs its oracle materialized first
            "CorpusDifferentialTest.java",
            // the typing census (SYSTEM_PRELUDE_DESIGN §6, 2026-09-08):
            // skips only when the legend-pure checkout is absent — it is a
            // REPORT (target/spec-body-census.txt), not yet a pin; its
            // numbers are recorded in docs/SPEC_BODY_CENSUS_2026_09_08.md
            "SpecBodyCensusTest.java",
            // the dynafunction registry (upstream boundary batch 5 audit,
            // 2026-09-11): skips only when the legend-engine checkout is
            // absent — the enum is compared EXHAUSTIVELY with the engine's
            // dynaFnToSql registries, which live in that checkout
            "DynaFnRegistryTest.java",
            // batch 5 audit remainder (2026-09-11): the implicit-import sequence
            // and the PlatformTypes spelling parity — both read the checkouts,
            // both skip only when a checkout is absent
            "CoreImportsParityTest.java",
            "PlatformNamesSpellingTest.java",
            // the upstream path manifest (upstream boundary batch 2,
            // 2026-09-10): skips ONLY when a checkout root itself is absent
            // (tools/oracle-roots.sh fails the gates upstream of that); a
            // PRESENT checkout is checked in full — 90 paths, every miss
            // named, the count pinned
            "UpstreamPathManifestTest.java",
            // ---- parser-equivalence (audit-of-audits #11: the walk
            // now covers sibling modules — these 8 carried the exact
            // vacuous-green pattern c4386547 was written to kill,
            // uncensused). All skip on the ORACLE CHECKOUT being
            // absent (legend.engine/pure roots); gate 8's
            // roots_present + skipped() detection is the loud
            // back-stop that keeps the skip from reading as a pass.
            "CorpusCensusTest.java",
            "CorpusManifestTest.java",
            "CorpusSweepTest.java",
            "MigrationSizingTest.java",
            "OwnDialectCensusTest.java",
            "ParseSpeedBenchmarkTest.java",
            "SectionParseSentinelTest.java",
            "SurfaceCensusTest.java");

    private static final Pattern DISABLED =
            Pattern.compile("@Disabled\\(\"([^\"]*)\"\\)");

    @Test
    void disabledRowsAreNamedGapsAndShrinkOnly() throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        List<String> badReasons = new ArrayList<>();
        for (Path f : testSources()) {
            String src = Files.readString(f);
            Matcher m = DISABLED.matcher(src);
            int c = 0;
            while (m.find()) {
                c++;
                if (!m.group(1).startsWith("GAP: ")) {
                    badReasons.add(f.getFileName() + ": @Disabled(\""
                            + m.group(1) + "\")");
                }
            }
            if (c > 0) {
                found.merge(f.getFileName().toString(), c, Integer::sum);
            }
        }
        assertTrue(badReasons.isEmpty(),
                "every @Disabled must name its gap (reason starts 'GAP: ' —"
                + " a skip is a registered claim, not a shrug):" + badReasons);
        for (var e : found.entrySet()) {
            Integer pin = DISABLED_PINS.get(e.getKey());
            assertTrue(pin != null && e.getValue() <= pin,
                    e.getKey() + " has " + e.getValue() + " @Disabled rows"
                    + " (pin " + pin + ") — a NEW skip needs a pin bump"
                    + " with a written justification in this register");
            if (e.getValue() < pin) {
                System.out.println("[skip-census] " + e.getKey()
                        + " shrank to " + e.getValue() + " (pin " + pin
                        + ") — tighten the pin");
            }
        }
        // pins for files with no skips left must be deleted (stale-row rule)
        for (String pinned : DISABLED_PINS.keySet()) {
            assertTrue(found.containsKey(pinned),
                    pinned + " no longer has @Disabled rows — delete its"
                    + " pin (a stale row is a register lying)");
        }
    }

    @Test
    void assumptionSkipFilesArePinnedExactly() throws IOException {
        List<String> found = new ArrayList<>();
        for (Path f : testSources()) {
            String src = Files.readString(f);
            if (src.contains("Assumptions.assume")
                    && !f.getFileName().toString().equals("SkipCensusTest.java")) {
                found.add(f.getFileName().toString());
            }
        }
        found.sort(String::compareTo);
        List<String> pinned = new ArrayList<>(ASSUMPTION_FILES);
        pinned.sort(String::compareTo);
        assertEquals(pinned, found,
                "the assumption-skip FILE SET is pinned exactly — a new"
                + " assumption-skipping file is a new way for the suite"
                + " to go quiet; register it here with its justification");
    }

    private static List<Path> testSources() throws IOException {
        List<Path> out = new ArrayList<>();
        // audit-of-audits #11: the census walks SIBLING MODULES too —
        // core-only scope let 8 assumption-skipping files sit invisible
        // in parser-equivalence (the exact scope-rot this file's own
        // header warns about). pct is included for the same reason.
        for (Path root : List.of(Path.of("src/test/java"),
                Path.of("../parser-equivalence/src/test/java"),
                Path.of("../pct/src/test/java"))) {
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> s = Files.walk(root)) {
                out.addAll(s.filter(p -> p.toString().endsWith(".java"))
                        .toList());
            }
        }
        // floor 270: core 223 + siblings 60 measured 2026-08-21 — a
        // drop below means a MODULE fell out of the walk
        GuardCoverage.assertFloor("SkipCensusTest", out.size(), 270);
        return out;
    }
}
