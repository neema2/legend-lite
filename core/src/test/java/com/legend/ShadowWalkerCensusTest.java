// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;


import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * T4.1 invariant 3 — the normalizer's SHADOW TYPE SYSTEM is a shrink-only
 * pin. Fourteen walkers re-derived the class hierarchy, property lookup,
 * stereotypes and store facts over the parse records
 * (docs/T4_1_KNOWLEDGE_BEFORE_NORMALIZATION_2026_09_13.md &sect;4); step 3
 * retires them family by family onto the one knowledge kernel
 * ({@code ModelBuilder.knowledge()}). Each row is that walker's CALL-SITE
 * count under {@code normalizer/} (definitions excluded). GROWTH is a new
 * shadow; SHRINKAGE means a family moved — ratchet the row down in the
 * same commit with the batch's name.
 */
@Tag("guardrail")
class ShadowWalkerCensusTest {

    private static final String NORMALIZER = "core/src/main/java/com/legend/normalizer";

    private static final Map<String, Integer> REGISTER = new TreeMap<>(Map.ofEntries(
            // The live shadows, at their call-site counts. The retired families (subtype,
            // property, stereotype, store; T4.1 step 3, 2026-09-13; views stage 3,
            // 2026-09-22) were 15 rows pinned at 0 for methods that no longer exist;
            // they were deleted on 2026-09-29 (execution plan W0.5). A new shadow walker
            // is kept out by the rebuild's typed mapping elaboration (W4.1), not by a
            // list of dead names.
            // SUBTYPE: the three mapping-aware walkers keep their E logic and delegate
            // their walks to the kernel
            Map.entry("collectInheritanceMembers", 2),
            Map.entry("nearestMappedAncestor", 1),
            Map.entry("hasMappedSubclass", 1),
            // KIND: the coercion seam still asks the declared platform kind and the
            // physical kind of a column through these two walkers (audit 2026-09-15 P5-1)
            Map.entry("pureKindOf", 1),
            Map.entry("declaredPlatformKind", 3)));

    @Test
    void shadowWalkerCallSitesArePinned() throws IOException {
        Map<String, Integer> actual = new TreeMap<>();
        REGISTER.keySet().forEach(k -> actual.put(k, 0));
        List<Path> files;
        try (Stream<Path> s = SourceFiles.under(NORMALIZER).stream()) {
            files = s.filter(p -> p.toString().endsWith(".java")).toList();
        }
        // the scope must not rot (audit 2026-09-15 P5-7): the walk found
        // the normalizer's sources, not an empty or moved directory
        GuardCoverage.assertFloor("ShadowWalkerCensusTest", files.size(), 20);
        for (Path f : files) {
            for (String line : Files.readAllLines(f)) {
                for (String walker : REGISTER.keySet()) {
                    Pattern call = Pattern.compile("\\b" + walker + "\\(");
                    Pattern def = Pattern.compile("static\\b.*\\b" + walker + "\\(");
                    if (def.matcher(line).find()) {
                        continue;
                    }
                    Matcher m = call.matcher(line);
                    while (m.find()) {
                        actual.merge(walker, 1, Integer::sum);
                    }
                }
            }
        }
        assertEquals(REGISTER, actual, "shadow-walker census drifted"
                + " (docs/T4_1_KNOWLEDGE_BEFORE_NORMALIZATION_2026_09_13.md §4):"
                + " GROWTH is a new shadow of the knowledge kernel — ask"
                + " ModelBuilder.knowledge() instead; SHRINKAGE means a family"
                + " moved — ratchet the row down in the same commit");
    }
}
