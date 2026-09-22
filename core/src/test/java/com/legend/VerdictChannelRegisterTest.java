// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * JUDGING_TWO_MODES §5a — ONE JUDGE. (a) Exactly one class decides host
 * equality: the leniency ({@code Math.ulp}) lives in {@code Equality.java}
 * and nowhere else in the product; (b) the judge's callers are a closed
 * register — the verdict seam and the delegating message facades; a new
 * caller registers consciously with its tenet argument (the Charter's
 * Clause 2c: product equality is World 2, it lowers to SQL).
 */
class VerdictChannelRegisterTest {

    private static final List<String> JUDGE_CALLERS = List.of(
            "core/src/main/java/com/legend/AssertVerdicts.java",
            // task #27 (2026-09-21): the HOST ARM of the verdict seam — the Java compare
            // over database rows moved out of the router verbatim; the router only
            // classifies and dispatches
            "core/src/main/java/com/legend/HostJudge.java",
            // message facades: the assert family's spellings, decided by the judge
            "core/src/main/java/com/legend/exec/PureAsserts.java",
            // the grid compare policy (row order / multiset), cells by the judge —
            // and since 2026-09-20 its float cell rule is the judge's own
            // (Equality.withinTwoUlp; the printed-precision tolerance deleted)
            "core/src/main/java/com/legend/exec/TdsCompare.java",
            // the service-test runner's EqualToJson routes to serviceJson
            "core/src/main/java/com/legend/test/TestAssertions.java",
            // 2026-09-20 (one float rule): the corpus REFEREE's float cell
            // rule is the judge's own — its 2-ULP pairing of leftover rows
            // calls the one home (the ten-digit normalization it replaced
            // was a second definition of "equal float"; deleted)
            "spec/src/test/java/com/legend/harness/H2Verify.java",
            "core/src/test/java/com/legend/exec/EqualityJsonUnorderedRootTest.java",
            "core/src/test/java/com/legend/exec/EqualityWorldsConformanceTest.java",
            "core/src/test/java/com/legend/exec/PureAssertsTest.java");

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
        for (String root : List.of("core/src/main/java", "core/src/test/java",
                "pct/src/test/java", "spec/src/test/java")) {
            Path dir = Repo.path(root);
            if (!Files.isDirectory(dir)) {
                throw new IllegalStateException("verdict-channel scan root missing: " + dir
                        + " (Bazel: declare it as data of the test target)");
            }
            try (Stream<Path> s = Files.walk(dir)) {
                for (Path f : s.filter(p -> p.toString().endsWith(".java")).toList()) {
                    String name = f.getFileName().toString();
                    if (name.equals("VerdictChannelRegisterTest.java")
                            || name.equals("Equality.java")) {
                        continue;
                    }
                    String src = Files.readString(f)
                            .replaceAll("(?s)/\\*.*?\\*/", "")
                            .replaceAll("//.*", "");
                    String rel = Repo.root().relativize(f.toAbsolutePath().normalize())
                            .toString().replace(java.io.File.separatorChar, '/');
                    // the JUDGE's own name as a type reference (never a
                    // suffix such as EqualityKeys, never the file name)
                    if (JUDGE_REF.matcher(src).find()) {
                        callers.add(rel);
                    }
                    if (root.equals("core/src/main/java") && src.contains("Math.ulp(")) {
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
                "the Float leniency has ONE home, Equality.java (§5a): a second"
                + " Math.ulp site is a second judge");
    }
}
