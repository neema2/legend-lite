package com.legend.integration;

import com.legend.testing.Repo;
import java.nio.file.*;
import java.util.*;

/**
 * Loads the stress corpus for the legend-lite side: the LINKED PROJECTS first (the
 * corpus's mappings include theirs), then every stress file, minus what legend-lite
 * cannot yet build a model from.
 *
 * <p>Both legend-lite tests need the same model, and both need the same exclusions — an
 * unbuildable element does not fail one service, it fails the whole model load, so a
 * single unsupported construct would take the entire legend-lite side of the corpus down
 * with it.
 *
 * <p>Every exclusion is a legend-lite GAP, not a corpus error: legend-engine runs these
 * files. Each carries its reason, {@link LegendLiteGapTest} asserts the gap is still
 * open, and removing one must be a deliberate act.
 */
final class StressCorpus {

    /** The projects the corpus depends on, DEPENDENCIES BEFORE DEPENDENTS — the same
     *  list and order as {@code scripts/corpus/model.py LINKED_PROJECTS}. */
    static final List<String> LINKED_PROJECTS = List.of(
            "core-types", "core-tenor", "core-fx", "core-ratings", "core-instrument",
            "core-calendar", "core-units", "core-account", "core-geo", "fee-core",
            "index-core");

    /** A project's files in SECTION order (model, store, mapping): a file with no
     *  {@code ###} header inherits the section the previous file left open. */
    private static final List<String> SECTION_ORDER = List.of("model.pure", "store.pure",
            "mapping.pure");

    static final Path STRESS = Repo.module("src/test/resources/stress");
    static final Path PROJECTS = Repo.path("projects");

    /** file name -> why legend-lite cannot build a model from it (census 2026-09-16). */
    static final Map<String, String> EXCLUDED = Map.of(
            "29-money.pure",
            "Measure/Unit: 'Unknown type: stress::Money~USD is not a known primitive, "
                    + "class, or enum'. A Measure parses, but its unit types never "
                    + "register as resolvable types.",
            "55-canonical-store.pure",
            "declares canonical::MonetaryTrade over stress::Money~USD, so it falls "
                    + "with 29-money.pure. It also holds the M2M mapping and the "
                    + "ModelChainConnection runtimes.",
            "70-surface-store2.pure",
            "precise primitives: 'Unknown type: meta::pure::precisePrimitives::Varchar'.",
            "71-mapping-surface2.pure",
            "M2M explosion 'part*' (one target instance per source collection element) "
                    + "is refused by the mapping normalizer.",
            "75-surface-gaps.pure",
            "M2M local mapping property '+localTag' colliding with a declared property "
                    + "of the target class is refused by the mapping normalizer.");

    private StressCorpus() {
    }

    /** Every source file the lite side loads, in load order. */
    static List<Path> files() throws Exception {
        List<Path> out = new ArrayList<>();
        for (String project : LINKED_PROJECTS) {
            for (String f : SECTION_ORDER) {
                Path p = PROJECTS.resolve(project).resolve(f);
                if (Files.exists(p)) {
                    out.add(p);
                }
            }
        }
        try (var s = Files.list(STRESS)) {
            for (Path p : s.sorted().toList()) {
                if (!p.toString().endsWith(".pure")
                        || EXCLUDED.containsKey(p.getFileName().toString())) {
                    continue;
                }
                out.add(p);
            }
        }
        return out;
    }

    static String model() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (Path p : files()) {
            sb.append(Files.readString(p)).append("\n");
        }
        return sb.toString();
    }

    static void reportExclusions() {
        EXCLUDED.forEach((f, why) ->
                System.out.println("  EXCLUDED " + f + ": " + why));
    }
}
