// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.lowering.EditionTable;
import com.legend.lowering.LiteEditionTable;
import com.legend.model.ConnectionDefinition.DatabaseType;

/**
 * THE PRODUCT'S EDITION ({@link Edition}; the edition program's step 2, 2026-11-26): no row, no stamp, no flag read.
 * Lite means what its own rules say on every database, so its table is the one with no row
 * ({@link LiteEditionTable}). Its own class, as each two-form kind's product form is (a kind holds signatures only:
 * {@code PlanStyleSurfaceTest}).
 */
public final class LiteEdition implements Edition {

    public static final LiteEdition INSTANCE = new LiteEdition();

    private LiteEdition() {
    }

    @Override
    public boolean stampsOutsideStore() {
        return false;
    }

    @Override
    public boolean transformsOutsideStore(boolean fetchesAGraph, boolean chained) {
        return false;
    }

    @Override
    public boolean readsWrittenFlags() {
        return false;
    }

    @Override
    public boolean rowsByDatabase() {
        return false;
    }

    @Override
    public EditionTable table(@Nullable DatabaseType database) {
        return LiteEditionTable.INSTANCE;
    }
}
