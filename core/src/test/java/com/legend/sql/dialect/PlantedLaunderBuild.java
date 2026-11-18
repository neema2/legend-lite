// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of 091b9bde36, 2026-11-18, MUST-FIX 3): a dialect building a node of the function (its own
 *  decomposition), returning text. */
public abstract class PlantedLaunderBuild extends AnsiSqlRenderer {
    protected PlantedLaunderBuild() {
        super(null);
    }

    static String build() {
        return String.valueOf(com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.ABS,
                new com.legend.sql.SqlExpr.IntLit(1)));
    }
}
