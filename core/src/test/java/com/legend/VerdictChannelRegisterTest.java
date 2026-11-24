// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * JUDGING_TWO_MODES §5a — ONE JUDGE. (a) Exactly one class decides host
 * equality: the leniency ({@code Math.ulp}) lives in one class and nowhere
 * else in the product -- compiled legend-pure's 2 ULP, the edge's
 * {@code CompiledPureHostForms} since 2026-11-18 (train 17 item 4: lite
 * compares a Float exactly; {@code Equality.java} until then); (b) the judge's callers are a closed
 * register — the verdict seam and the delegating message facades; a new
 * caller registers consciously with its tenet argument (the Charter's
 * Clause 2c: product equality is World 2, it lowers to SQL).
 */
@Tag("census")
class VerdictChannelRegisterTest {

    private static final List<String> JUDGE_CALLERS = List.of(
            // (AssertVerdicts left 2026-11-07, P1: its one call, the grid byte channel's declared-ULP vector, went with
            // the host judge's dead byte channel)
            // task #27 (2026-09-21): the HOST ARM of the verdict seam — the Java compare
            // over database rows moved out of the router verbatim; the router only
            // classifies and dispatches
            // the test's run since slice 5 (2026-11-09: the host judge's run half, out of the driver)
            "core/src/main/java/com/legend/testexec/HostJudgments.java",
            // message facades: the assert family's spellings, decided by the judge
            "core/src/main/java/com/legend/testexec/PureAsserts.java",
            // the grid compare policy (row order / multiset), cells by the judge —
            // and since 2026-09-20 its float cell rule is the judge's own
            // (Equality.withinTwoUlp; the printed-precision tolerance deleted)
            "core/src/main/java/com/legend/testexec/TdsCompare.java",
            // the judges and their tests under com.legend.judge since 2026-10-30 (D71), the test's run's since slice 5
            "core/src/test/java/com/legend/testexec/EqualityJsonStrictTest.java",
            // 2026-11-03, the user's ruling "no sort can be multiset compare" restored: an unsorted chain's root
            // array a multiset again, its test back beside the strict object-vs-array one
            "core/src/test/java/com/legend/testexec/EqualityJsonUnorderedRootTest.java",
            "core/src/test/java/com/legend/testexec/EqualityWorldsConformanceTest.java",
            "core/src/test/java/com/legend/testexec/PureAssertsTest.java",
            // (EqualityCompatEdgeTest, 2026-11-17 K14's temporary entry: gone with step 2c -- it judges through the edge's
            // door now, the run hook's HostForms, and calls no equality of the judge's)
            // (EqualToJson, RefereeVerify and UpstreamRenderedTexts: gone 2026-11-18, train 17 item 4 -- the 2-ULP bound
            // they read is the edge's CompiledPureHostForms.withinUlps, no longer Equality's)
            // 2026-11-17 (census C1's step 2c): compiled legend-pure's cross-kind rule moved from Equality to the
            // edge's HostForms -- it names the judge's kinds (Equality.Kind), never a second equality
            "core/src/main/java/com/legend/edge/CompiledPureHostForms.java",
            // the hook's signature and lite's form name the judge's kinds (Equality.Kind) -- the same 2026-11-17 move
            "core/src/main/java/com/legend/testexec/HostForms.java",
            // its product form's own class (GUARD's C.9, 2026-11-17)
            "core/src/main/java/com/legend/testexec/LiteHostForms.java",
            // 2026-11-18 (census J29): the host judge's words over the verdict's own facts, deciding nothing
            // (HostWords and its two forms until 2026-11-21, item 3's S2: the host judge's failed asserts as records)
            "core/src/main/java/com/legend/testexec/AssertRecords.java");

    private static final java.util.regex.Pattern JUDGE_REF =
            java.util.regex.Pattern.compile("(?<![A-Za-z0-9_])Equality(::|\\.(?!java\\b))");

    @Test
    void theJudgeHasOneHomeAndAClosedSetOfCallers() throws IOException {
        List<String> callers = new ArrayList<>();
        List<String> ulpSites = new ArrayList<>();
        // Roots are REPOSITORY paths, and a missing one is a failure, not a
        // skip: this walk used to `continue` past an absent root, so from any
        // working directory but core/ it scanned nothing — it went red only
        // because it compares an exact set (Bazel, 2026-09-22).
        for (String root : List.of("core/src/main/java", "core/src/main/duckdb", "core/src/test/java",
                "pct/src/test/java", "spec/src/test/java")) {
            String dir = root;
            if (SourceFiles.under(dir).isEmpty()) {
                throw new IllegalStateException("verdict-channel scan root missing: " + dir
                        + " (Bazel: declare it as data of the test target)");
            }
            try (Stream<Path> s = SourceFiles.under(dir).stream()) {
                for (Path f : s.filter(p -> p.toString().endsWith(".java")).toList()) {
                    String name = f.getFileName().toString();
                    if (name.equals("VerdictChannelRegisterTest.java")
                            || name.equals("Equality.java")) {
                        continue;
                    }
                    String src = Files.readString(f)
                            .replaceAll("(?s)/\\*.*?\\*/", "")
                            .replaceAll("//.*", "");
                    String rel = SourceFiles.path(f);
                    // the JUDGE's own name as a type reference (never a
                    // suffix such as EqualityKeys, never the file name)
                    if (JUDGE_REF.matcher(src).find()) {
                        callers.add(rel);
                    }
                    if (root.startsWith("core/src/main/") && src.contains("Math.ulp(")
                            && !name.equals("CompiledPureHostForms.java")) {
                        ulpSites.add(rel);
                    }
                }
            }
        }
        callers.sort(String::compareTo);
        List<String> pinned = new ArrayList<>(JUDGE_CALLERS);
        pinned.sort(String::compareTo);
        assertEquals(pinned, callers,
                "the host judge (Equality) has a caller outside the closed"
                + " register (JUDGING_TWO_MODES §5a): register it consciously"
                + " with its tenet argument, or route through the verdict seam");
        assertEquals(List.of(), ulpSites,
                "the Float leniency has ONE home, the edge's CompiledPureHostForms.java (§5a): a second"
                + " Math.ulp site is a second judge");
    }
}
