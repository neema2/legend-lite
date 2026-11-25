// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.builtin.Pure;
import com.legend.lowering.EditionTable;
import com.legend.model.FunctionId;
import com.legend.platform.Feature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LEGEND-ENGINE'S TABLE (the edition program's step 2, 2026-11-25; docs/records/edition-plan-to-finish-stage-b-2026-11-25.md):
 * the rows legend-engine's door hands the lowering ({@link EditionTable}). Each row names a product native by what it
 * is; none of legend-engine's SQL is here.
 *
 * <p>Inside a store query legend-engine sends these to the database as it has them (register S130, S134, S383):
 * <ul>
 *   <li>{@code substring}: the database's own SUBSTRING over the arguments as given ({@code substringAsGiven}), or,
 *       under {@code CORRECT_SQL_SUBSTRING_INDEXING}, over {@code start + 1} and {@code end - start}
 *       ({@code substrAsGiven}: legend-engine's processSubstr);</li>
 *   <li>{@code substr}: always corrected ({@code substrAsGiven});</li>
 *   <li>the two-argument String {@code indexOf}: the database's position ({@code sqlPosition}).</li>
 * </ul>
 * Outside any store query (the stamp) legend-engine's own Java computes them, with Pure's meaning: no row.
 */
final class EngineTable implements EditionTable {

    static final EngineTable INSTANCE = new EngineTable();

    private final Map<FunctionId, FunctionId> asGiven;
    private final Map<FunctionId, FunctionId> corrected;

    private EngineTable() {
        Map<FunctionId, FunctionId> given = new HashMap<>();
        byPosition(given, Pure.AT_STRING_SUBSTRING, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN);
        byPosition(given, Pure.AT_STRING_SUBSTR, Pure.AT_LEGEND_LITE_SUBSTR_AS_GIVEN);
        given.put(FunctionId.of(Pure.INDEX_OF__STRING_1__STRING_1), Pure.AT_LEGEND_LITE_SQL_POSITION.get(0));
        Map<FunctionId, FunctionId> flagged = new HashMap<>();
        byPosition(flagged, Pure.AT_STRING_SUBSTRING, Pure.AT_LEGEND_LITE_SUBSTR_AS_GIVEN);
        this.asGiven = Map.copyOf(given);
        this.corrected = Map.copyOf(flagged);
    }

    /** Each overload of {@code from} to the overload of {@code to} at the same place: the two lists are declared in
     *  the same order, the two-argument overload then the three-argument one (builtin.Pure). */
    private static void byPosition(Map<FunctionId, FunctionId> rows, List<FunctionId> from, List<FunctionId> to) {
        if (from.size() != to.size()) {
            throw new IllegalStateException("the engine table pairs overloads by position: " + from + " and " + to);
        }
        for (int i = 0; i < from.size(); i++) {
            rows.put(from.get(i), to.get(i));
        }
    }

    @Override
    public @com.legend.base.Nullable FunctionId lowersAs(FunctionId key, boolean outsideStore, Set<Feature> features) {
        if (outsideStore) {
            return null;
        }
        if (features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING)) {
            FunctionId flagged = corrected.get(key);
            if (flagged != null) {
                return flagged;
            }
        }
        return asGiven.get(key);
    }

    @Override
    public boolean stampsOutsideStore() {
        return true;
    }
}
