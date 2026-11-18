// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted: a dialect's root for a test's planted renderers outside this package (SqlFnSortedTest's), which name no
 *  dialect themselves (DatabaseOwners). Imported, never built. */
public abstract class PlantedDialectBase extends AnsiSqlRenderer {
    protected PlantedDialectBase() {
        super(null);
    }
}
