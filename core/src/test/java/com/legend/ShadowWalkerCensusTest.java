// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;


import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

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
class ShadowWalkerCensusTest {

    private static final Path NORMALIZER = Repo.module("src/main/java/com/legend/normalizer");

    private static final Map<String, Integer> REGISTER = new TreeMap<>(Map.ofEntries(
            // SUBTYPE FAMILY — RETIRED (T4.1 step 3a, 2026-09-13): the kernel's
            // isSubtype / ancestorsBelow / subtree / directSubtypes answer;
            // the three mapping-aware walkers below keep their E logic and
            // delegate their walks
            Map.entry("isSubclassOf", 0),
            Map.entry("selfAndAncestorsBelow", 0),
            Map.entry("collectInheritanceMembers", 2),
            Map.entry("nearestMappedAncestor", 1),
            Map.entry("hasMappedSubclass", 1),
            // PROPERTY FAMILY — RETIRED (T4.1 step 3b, 2026-09-13): the kernel's
            // hierarchyClass / propertyType / propertyDef / derivedInline /
            // propertyMultiplicity answer (45 + 3 + 1 + 53 + 3 + 2 sites)
            Map.entry("findPropertyTypeDeep", 0),
            Map.entry("findPropertyDefDeep", 0),
            Map.entry("findPropertyType", 0),
            Map.entry("classDef", 0),
            Map.entry("findDerivedInline", 0),
            Map.entry("findPropertyDeclared", 0),
            // STEREOTYPE FAMILY — RETIRED (T4.1 step 3c, 2026-09-13): a fold over
            // the kernel's lineage (MilestoningFacts)
            Map.entry("isBitemporalClass", 0),
            Map.entry("isTemporalClass", 0),
            // STORE FAMILY — RETIRED (T4.1 step 3d, 2026-09-13): the kernel's
            // table / column / columnKind (include-closure aware, schema-aware,
            // view-following) answer; PhysicalTables is deleted; RelationalKinds
            // moved to compiler as the one kind reader (its calls are not
            // shadows and are no longer counted)
            Map.entry("columnPureKind", 0),
            Map.entry("findPhysicalColumn", 0),
            Map.entry("findPhysicalTable", 0),
            Map.entry("tableHasColumn", 0),
            // KIND FAMILY — the two rows 6048acec2 (T4.1 step 3d) DELETED
            // instead of ratcheting (audit 2026-09-15 P5-1): the coercion
            // seam still asks the declared platform kind and the physical
            // kind of a column through these two walkers; pinned at their
            // live call-site counts, shrink-only from here
            Map.entry("pureKindOf", 1),
            Map.entry("declaredPlatformKind", 3),
            // OWED: a view's root table is a STORE fact (T4.1 §8 step 3: "the
            // view root and column kind stamped on compiled stores") — it
            // still walks RelationalOperation records with the normalizer's
            // own collectors and names the mapping in its errors; retire
            // with step 4/6's compiled-store facts
            // 5 -> 0 (views stage 3, 2026-09-22): the view main-table rule
            // moved INTO the kernel — ModelBuilder.viewMainTable, the one
            // owner (the normalizer, the lineage and the test-data
            // generator read it there); no walker of it remains outside
            Map.entry("inferViewMainTable", 0)));

    @Test
    void shadowWalkerCallSitesArePinned() throws IOException {
        Map<String, Integer> actual = new TreeMap<>();
        REGISTER.keySet().forEach(k -> actual.put(k, 0));
        List<Path> files;
        try (Stream<Path> s = Files.walk(NORMALIZER)) {
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
