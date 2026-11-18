// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (review F of the guard line, 2026-11-18, SHOULD 1; one shape a class, the review of the guard line, G,
 *  SHOULD 1): a dialect handing the function to a keeper of an array of it -- an array-typed parameter is a hand-off. */
public abstract class PlantedLaunderArrayKeeper extends AnsiSqlRenderer {
    protected PlantedLaunderArrayKeeper() {
        super(null);
    }

    static void keepAll() {
        Keeper.keepAll(new com.legend.sql.SqlFn[] {com.legend.sql.SqlFn.ABS});
    }

    /** The keeper, outside the dialect's own code. */
    static final class Keeper {
        static final java.util.List<Object> KEPT = new java.util.ArrayList<>();

        private Keeper() {
        }

        static void keepAll(com.legend.sql.SqlFn[] values) {
            KEPT.addAll(java.util.List.of(values));
        }
    }
}
