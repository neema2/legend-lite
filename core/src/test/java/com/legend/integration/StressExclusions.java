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
            "55-canonical-store.pure",
            "declares canonical::MonetaryTrade over stress::Money~USD, so it falls "
                    + "with 29-money.pure. It also holds the M2M mapping and the "
                    + "ModelChainConnection runtimes.",
            "71-mapping-surface2.pure",
            "M2M explosion 'part*' (one target instance per source collection element) "
                    + "is refused by the mapping normalizer.",
            "75-surface-gaps.pure",
            "M2M local mapping property '+localTag' colliding with a declared property "
                    + "of the target class is refused by the mapping normalizer.");
}
