// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE LEGACY REACH-BACK CENSUS GUARD (docs/LEGACY_MAPPING_REACHBACK_CENSUS.md,
 * 2026-08-30): {@code findLegacyMapping} is the choke point through which
 * post-compile code reaches back into the phase-B parse artifact. The
 * 2026-08 program killed every query-time re-derivation reach-back
 * (PHYS-1/AGG-1/VF-1 read stamps; enum tables ride the compiled artifact)
 * and classified every survivor. This census pins the survivors EXACTLY,
 * per file, occurrence-counted — a NEW reach-back (new file, or growth
 * inside a registered file) fails loudly and routes the author to the
 * census doc's razor before the artifact quietly forks again.
 *
 * <p>The register's categories (each row cites one):
 * <ul>
 *   <li><b>CONSTRUCTION</b> — Phase B&rarr;E code consumes the parse
 *   artifact BY DESIGN (the normalizer package is its sole
 *   post-resolution consumer).</li>
 *   <li><b>SURFACE CONTRACT</b> — engine APIs SPEC'D on the authored
 *   mapping: the Pure metamodel walk, and {@code scanRelations}
 *   ("static form off the mapping" — the tree is arranged by the
 *   MAPPING's join names, a vocabulary the lifted functions do not
 *   carry; SCAN-1 reclassified with this receipt).</li>
 *   <li><b>REGISTRY PLUMBING</b> — the interface declaration, its
 *   implementations, and name-existence checks across both
 *   registries.</li>
 * </ul>
 *
 * <p>Occurrence counts (not just file names) so growth INSIDE a
 * registered file is also a conscious registration — the JDBC census's
 * recorded file-grain limit, closed here where it is cheap.
 */
class LegacyReachbackCensusTest {

    private static final List<String> ROOTS = List.of(
            "core/src/main", "pct/src/main",
            "parser-equivalence/src/main");

    /** Coverage floor: production files scanned on 2026-08-30. Shrink
     * needs a written justification (files deleted); growth is free. */
    private static final int FILE_FLOOR = 250;

    private static final Map<String, Integer> REGISTER = new TreeMap<>(Map.of(
            // CONSTRUCTION (Phase E): the normalizer IS the legacy
            // artifact's consumer — translation, union/association
            // synthesis, include walks over the raw surface
            // MappingNormalizer 4 + UnionSynthesis 3 + AssociationSynthesis 1
            // -> MappingClosures 2 (T4.1 step 4a, 2026-09-13): the nine
            // include walkers are readers of ONE include-order fact computed
            // per mapping over the mappings' surfaces and memoized on the
            // knowledge kernel; its two reaches (the surface lookup, the
            // package-local include probe) are the whole of Phase E's
            // consumption of the authored include graph
            "core/src/main/java/com/legend/normalizer/MappingClosures.java", 2,
            // SURFACE CONTRACT: the Pure metamodel API presents the
            // AUTHORED mapping (includes + own lists; .pure navigation
            // does its own traversal)
            // 3 -> 2 (metamodel-as-relations batch 5, 2026-09-02): the
            // classMappingById/superMapping/allSuperSetImplementations/
            // resolvePrimaryKey arms are Pure bodies over the metamodel
            // store — classMappingByIdIn (one reach) is gone with them
            // MetamodelWalk 2 -> GONE (group F burn, 2026-09-02): the
            // mapping/set/property-mapping handles are Pure bodies over the
            // metamodel store
            // SEED DERIVATION (group F burn): a set's property mappings live
            // only on the parse artifact (the compiled binding is a lifted
            // function) — the store's property_mappings rows read them
            // there, the ONE reach the walk's arms used to make
            "core/src/main/java/com/legend/MetamodelSeeds.java", 1,
            // SURFACE CONTRACT: engine scanRelations is "static form off
            // the mapping" (#44, feature map §14.1) — join-NAME vocabulary
            // exists only on the authored surface (SCAN-1 reclassified)
            "core/src/main/java/com/legend/lineage/ScanRelations.java", 2,
            // REGISTRY PLUMBING: declaration, O(1) impl, overlay
            // existence checks across both registries
            "core/src/main/java/com/legend/compiler/element/ModelContext.java", 1,
            // 4 -> 6 (metamodel-as-relations step 3): unionMemberClasses +
            // routedTargetClass — the union op's DECLARED members and a
            // property mapping's ROUTED target set, read ON the context
            // (the chain-position cast rule's facts; the resolver never
            // touches the parse artifact)
            // 6 -> 4 (T4.1 step 4b, 2026-09-13): unionMemberClasses and
            // routedTargetClass read the facts STAMPED on the compiled
            // mapping (MappingDefinition.NormalizationFacts, computed at
            // Phase E from the mapping's surface); the routedTargetSetOf
            // fallback into the index's legacy walk is gone the same way
            "core/src/main/java/com/legend/compiler/element/PureModelContext.java", 4,
            // the accessor's own declaration (registry plumbing); the walk
            // it used to serve (routedTargetSetOf) died in step 4b
            "core/src/main/java/com/legend/compiler/ModelBuilder.java", 1));

    @Test
    void findLegacyMappingCallersArePinned() throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        int scanned = 0;
        for (String root : ROOTS) {
            Path p = Repo.path(root);
            if (!Files.isDirectory(p)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(p)) {
                for (Path f : files.filter(x -> x.toString().endsWith(".java"))
                        .toList()) {
                    scanned++;
                    String src = stripComments(Files.readString(f));
                    int n = count(src);
                    if (n > 0) {
                        // '/' ALWAYS: the census keys are compared against a
                        // committed doc written with forward slashes, so on
                        // Windows every key differs and the census reads as
                        // pure GROWTH (Windows CI, 2026-09-09). Keys are
                        // REPOSITORY-relative: relativized against the root
                        // rather than by stripping a "../" that only existed
                        // while the working directory was the module.
                        found.put(Repo.root().relativize(f.toAbsolutePath().normalize())
                                .toString().replace(java.io.File.separatorChar, '/'), n);
                    }
                }
            }
        }
        assertTrue(scanned >= FILE_FLOOR,
                "census coverage SHRANK: scanned " + scanned + " < floor "
                        + FILE_FLOOR + " — a source root moved or the walk"
                        + " lost a module");
        assertEquals(REGISTER, found,
                "findLegacyMapping reach-back census drifted"
                        + " (docs/LEGACY_MAPPING_REACHBACK_CENSUS.md): GROWTH"
                        + " means a NEW reach into the phase-B parse artifact"
                        + " — the fact you need belongs ON the compiled"
                        + " artifact (stamped at Phase E) or your consumer is"
                        + " an analysis that walks the lifted functions; read"
                        + " the census razor before registering. SHRINKAGE"
                        + " means a reach-back died — ratchet the row down in"
                        + " the same commit.");
    }

    private static int count(String src) {
        int n = 0;
        int i = 0;
        while ((i = src.indexOf("findLegacyMapping", i)) >= 0) {
            n++;
            i += "findLegacyMapping".length();
        }
        return n;
    }

    /** Line + block comments removed (string-literal contents kept — the
     * token cannot legitimately hide in one). */
    private static String stripComments(String s) {
        return s.replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("//[^\n]*", "");
    }
}
