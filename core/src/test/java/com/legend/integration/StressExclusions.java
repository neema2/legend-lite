// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import java.util.Map;

/**
 * The stress files legend-lite cannot build a model from, each with why. A class of its own, with no static that
 * resolves a path, so reading it (LegendLiteGapTest) runs nothing else (Bazel workplan P3-02: it lived in
 * StressCorpus, whose static initialiser resolved the corpus and projects roots through Repo).
 */
final class StressExclusions {

    private StressExclusions() {
    }

    /** file name -> why legend-lite cannot build a model from it (census 2026-09-16). */
    static final Map<String, String> EXCLUDED = Map.of(
            "29-money.pure",
            "Measure/Unit: 'Unknown type: stress::Money~USD is not a known primitive, "
                    + "class, or enum'. A Measure parses, but its unit types never "
                    + "register as resolvable types.",
            // 2026-10-30: its M2M mapping without a measure, the ModelChainConnection and stress::CanonicalRT moved to
            // 55-canonical-chain.pure, which lite loads (the improvement plan's Phase 5)
            "55-canonical-store.pure",
            "maps canonical::MonetaryTrade's amount to stress::Money~USD, so it falls "
                    + "with 29-money.pure: the Money measure's mapping and runtime alone.",
            "71-mapping-surface2.pure",
            "M2M explosion 'part*' (one target instance per source collection element) "
                    + "is refused by the mapping normalizer.",
            "75-surface-gaps.pure",
            "M2M local mapping property '+localTag' colliding with a declared property "
                    + "of the target class is refused by the mapping normalizer.",
            // 2026-10-21: the battery's services over 55's mappings and runtimes, generated apart so they leave with
            // it -- the model would otherwise name a mapping and a runtime it has not, which the compiler now refuses
            "94-canonical-services.pure",
            "the services over 55-canonical-store.pure's M2M mappings and runtimes, so they fall with it.",
            // 2026-10-30, the improvement plan's Phase 5: the M2M pack's enumeration transforms and passthrough, apart
            // from 8114-m2m.pure, which lite loads (the user's choice: exclude them rather than defer them in lite)
            "8115-m2m-gaps.pure",
            "M2M enum transformer 'EnumerationMapping M2MSide' is a roadmap feature, and a class-typed property "
                    + "mapped with no set (a passthrough) 'targets unmapped class': both refused by the mapping "
                    + "normalizer.",
            // 2026-10-30: the services over 8115's and 75's M2M mappings, generated apart so they leave with them (the
            // improvement plan's Phase 5), as 94-canonical-services.pure leaves with 55
            "94-gap-services.pure",
            "the services over 8115-m2m-gaps.pure's and 75-surface-gaps.pure's M2M mappings, so they fall with "
                    + "them.",
            // 2026-10-21: legend-pure's Path type, which lite does not declare (SEMANTICS_REGISTER S92)
            "74-path-surface.pure",
            "a function returning a navigation path's type, meta::pure::metamodel::path::Path<U,V|m>: "
                    + "'Unknown type' -- the prelude does not generate the path classes (a kernel gap).");
}
