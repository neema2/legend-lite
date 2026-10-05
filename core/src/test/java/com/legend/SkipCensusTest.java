// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

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
 * <li>environment-conditional skips — any {@code Assumptions} call,
 * {@code assumeTrue/False/That}, {@code assumingThat}, and the
 * {@code @EnabledIf/On/For/In*} and {@code @DisabledIf/On/For/In*}
 * conditions; the
 * FILE SET is pinned exactly (a new conditionally skipping file is a new
 * way for the suite to go quiet).</li>
 * </ol>
 */
@Tag("census")
class SkipCensusTest {

    /** file basename -> pinned MAX {@code @Disabled(} count. */
    private static final Map<String, Integer> DISABLED_PINS = Map.of();
    // EMPTIED 2026-10-05 (Bazel workplan P3-17): RelationalMappingIntegrationTest's 15 GAP rows were empty bodies, so
    // asserted nothing; they are deleted, and the gaps stay listed in docs/OUTSTANDING.md ("Declared platform gaps").

    /** Files permitted to carry a conditional skip ({@link #CONDITIONAL}). */
    private static final List<String> ASSUMPTION_FILES = List.of(
            // skips when the generated expected/ dir is absent -- the
            // differential needs its oracle materialized first (it reads Maven's
            // core/target/diff, so under Bazel it always skips: P3-17 wires or retires it)
            "CorpusDifferentialTest.java",
            // the manifest-world census (charter step 7 / D7, 2026-09-25): a
            // MEASUREMENT program, opt-in by -Dmanifest.census=<module>; skips
            // in the chain by design (P3-17 makes such programs actions or binaries)
            "ManifestWorldCensusTest.java",
            // our side of the reference differential (2026-09-25): a MEASUREMENT
            // program, opt-in by -Dour.resolutions=<module>; skips in the chain
            "OurResolutionsTest.java",
            // the warehouse against a live Postgres: runs only when LEGENDLITE_PG_DSN names one, by its own
            // manual target (//warehouse:postgres_live); an embedded Postgres in the chain is leg P2 of
            // docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md
            "WarehousePostgresLiveTest.java");
    // LEFT 2026-10-05 (Bazel workplan P3-14): MinimalCorpusTest, SpecBodyCensusTest, CoreImportsParityTest,
    // PlatformNamesSpellingTest, UpstreamPathManifestTest and parser-equivalence's CorpusCensusTest,
    // CorpusSweepTest, MigrationSizingTest, OwnDialectCensusTest, ParseSpeedBenchmarkTest and
    // SectionParseSentinelTest. Each skipped when a required input (an upstream tree, the corpus) was absent;
    // under Bazel those are declared inputs, so each now FAILS naming the input instead of going quiet.

    /** A conditional skip: an assumption, or a JUnit condition annotation. */
    private static final Pattern CONDITIONAL =
            Pattern.compile("Assumptions\\.|\\bassume(True|False|That)\\(|\\bassumingThat\\(|@(Enabled|Disabled)(If|On|For|In)");

    private static final Pattern DISABLED =
            Pattern.compile("@Disabled\\(\"([^\"]*)\"\\)");

    /** An {@code @Disabled} annotation without a quoted reason, which {@link #DISABLED} cannot read. */
    private static final Pattern UNREASONED_DISABLED =
            Pattern.compile("(?m)^\\s*@(org\\.junit\\.jupiter\\.api\\.)?Disabled\\b(?!\\(\")");

    @Test
    void disabledRowsAreNamedGapsAndShrinkOnly() throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        List<String> badReasons = new ArrayList<>();
        for (Path f : testSources()) {
            String src = Files.readString(f);
            if (UNREASONED_DISABLED.matcher(src).find()) {
                badReasons.add(f.getFileName() + ": an @Disabled with no quoted reason");
            }
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
            if (CONDITIONAL.matcher(src).find()
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
        for (Path root : List.of(Repo.module("src/test/java"),
                Repo.path("spec/src/test/java"),
                Repo.path("parser-equivalence/src/test/java"),
                Repo.path("pct/src/test/java"),
                Repo.path("warehouse/src/test/java"))) {
            if (!Files.isDirectory(root)) {
                throw new IllegalStateException("SkipCensusTest root " + root + " is not among its inputs: declare it (Bazel workplan P3-14: a missing root failed silently)");
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
