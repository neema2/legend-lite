// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * L-B's planted rewriter (EdgeOnlyDataTest.theGuardBites, 2026-11-18, train 18): a {@link SqlRewriter} reading one
 * component to decide -- a product use, as EnumValueTables reads a plan parameter's kind -- and another only to rebuild
 * the record -- a copy. Read as bytecode alone; nothing runs it.
 */
public final class PlantedDecidingRewriter extends SqlRewriter {

    /** A decision: the flag read, no record made. */
    public static boolean decides(PlantedEdgeOnlyMir.Ruled r) {
        return r.flag().isEmpty();
    }

    /** A rebuild: both read to make the record again. */
    public static PlantedEdgeOnlyMir.Ruled rebuild(PlantedEdgeOnlyMir.Ruled r) {
        return new PlantedEdgeOnlyMir.Ruled(r.flag(), r.copied());
    }
}
