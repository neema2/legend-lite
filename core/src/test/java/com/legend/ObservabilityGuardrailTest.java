// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OBSERVABILITY + DISPATCH FREEZES (Phase 2d) — shrink-only ratchets on
 * three accretion patterns. Each pin is the measured value at freeze
 * time (2026-07-30); the counts may only go DOWN. The planned end
 * states: one Trace switchboard replacing the ad-hoc env flags and
 * stderr prints, and the sealed-CoreFn dispatch (phase-G design leg)
 * replacing spelled-out function-name comparisons.
 */
class ObservabilityGuardrailTest {

    /** The debug env-flag vocabulary at freeze time — TWO naming schemes,
     * which is itself the evidence this grows unmanaged. A new flag means
     * extending the future Trace switchboard, not this list. */
    private static final Set<String> ENV_FLAGS = Set.of(
            "LEGEND_LITE_CARRY_TRACE", "LEGEND_LITE_CMP_DEBUG",
            "LEGEND_LITE_DUMP_SQL", "LEGEND_LITE_NAVDATE_TRACE",
            // the lean ladder's prepare/execute timing trace (2026-09-20): one
            // line per prepared statement to the named file — the input to
            // "is prepare time linear in text / operators"; off by default
            "LEGEND_LITE_PREP_TRACE",
            "LEGEND_LITE_RAW_EXPAND_TRACE", "LEGEND_LITE_SPLIT_TRACE",
            "LEGEND_LITE_STACKS", "LEGEND_LITE_STAMP_TRACE",
            "LL_DUMP_RESOLVED", "LL_FNLR_DEBUG", "LL_LINEAGE_DEBUG",
            "LL_ORD_COUNT", "LL_SQLTEXT_DEBUG",
            // the multiplicity-stamp census instrument (STAMP_DISCIPLINE
            // _PROGRAM.md, 2026-08-20) — the LL_TOL/ORD_COUNT family:
            // a counting instrument destined to FLIP into the failing
            // post-lowering invariant at census zero, then this flag
            // retires. (LEGEND_LITE_STAMP_TRACE is a DIFFERENT stamp —
            // TemporalFrame's milestoning-date trace.)
            "LL_STAMP_COUNT", "LL_TDG_DEBUG",
            "LL_TMP_DEBUG", "LL_TMP_SQL", "LL_TOL_COUNT",
            // deployment config for the HTTP server entrypoint (moved in
            // with the engine-module deletion) — not a debug flag
            "PORT");

    // 40 compiler sites + 3 server-shell sites that moved in with the
    // engine-module deletion (DiagramHandler crash report, the two
    // invalid-PORT warnings in main) — process-boundary operational
    // logging, not debug traces. Shrink-only from here.
    // 32→34 (2026-08-20, measured): StampCensus's [stamp] report lines — the
    // stamp-discipline census instrument (counting family, same as the
    // [tol]/[ord] prints); both retire when the census flips to the
    // failing invariant at zero.
    private static final int STDERR_PRINTS = 34;   // E4 census instrument removed at program close (was 33); re-pinned 2026-08-16 F1.2: harness left src/main (was 43)
    // 111: ObjectRefs' extraction-plumbing recognizers (raw-AST harness
    // vocabulary — generateObjectReferences/decodePkMaps idioms match
    // engine test spellings structurally; reviewed)
    private static final int STRING_DISPATCH_SITES = 87;   // re-pinned 2026-08-16 F1.2: harness left src/main (was 111)

    @Test
    void noNewDebugEnvFlags() throws IOException {
        Set<String> found = new TreeSet<>();
        Pattern p = Pattern.compile("System\\.getenv\\(\"([A-Z_]+)\"\\)");
        for (Path f : mainSources()) {
            Matcher m = p.matcher(Files.readString(f));
            while (m.find()) {
                found.add(m.group(1));
            }
        }
        Set<String> extra = new TreeSet<>(found);
        extra.removeAll(ENV_FLAGS);
        assertTrue(extra.isEmpty(),
                "new debug env flag(s) " + extra + " — extend the Trace"
                + " switchboard instead of adding ad-hoc getenv reads");
    }

    @Test
    void stderrPrintsOnlyShrink() throws IOException {
        int n = countAcrossSources(Pattern.compile("System\\.err\\.println"));
        assertTrue(n <= STDERR_PRINTS,
                "System.err.println sites grew to " + n + " (frozen at "
                + STDERR_PRINTS + ") — route new output through the trace"
                + " facility");
    }

    /** Function-name string dispatch silently ignores unknown spellings —
     * the sealed-CoreFn conversion retires this pattern; until then it
     * may only shrink. */
    @Test
    void stringDispatchOnlyShrinks() throws IOException {
        int n = countAcrossSources(Pattern.compile(
                "(function\\(\\)|simpleName\\(|\\.name\\(\\))\\.equals\\(\""));
        assertTrue(n <= STRING_DISPATCH_SITES,
                "string-compared function-name dispatch grew to " + n
                + " (frozen at " + STRING_DISPATCH_SITES + ") — new"
                + " vocabulary belongs in typed dispatch");
    }

    private static int countAcrossSources(Pattern p) throws IOException {
        int n = 0;
        for (Path f : mainSources()) {
            Matcher m = p.matcher(Files.readString(f));
            while (m.find()) {
                n++;
            }
        }
        return n;
    }

    private static java.util.List<Path> mainSources() throws IOException {
        Path root = Repo.module("src/main/java");
        try (Stream<Path> s = Files.walk(root)) {
            java.util.List<Path> out = s
                    .filter(f -> f.toString().endsWith(".java")).toList();
            GuardCoverage.assertFloor(/* 499->498: HostEval DELETED, Phase 1 batch 2 */ "ObservabilityGuardrailTest",
                    out.size(), 498);
            return out;
        }
    }
}
