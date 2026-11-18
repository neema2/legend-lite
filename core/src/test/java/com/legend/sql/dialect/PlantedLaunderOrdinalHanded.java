// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of the guard line, G, SHOULD 1, 2026-11-18): a dialect handing the function's ordinal to a
 *  keeper whose parameter is an int, setting no field of its own -- the hand-off is the call. */
public abstract class PlantedLaunderOrdinalHanded extends AnsiSqlRenderer {
    protected PlantedLaunderOrdinalHanded() {
        super(null);
    }

    static void keep() {
        Keeper.keep(com.legend.sql.SqlFn.ABS.ordinal());
    }

    /** The keeper, outside the dialect's own code. */
    static final class Keeper {
        static final java.util.List<Object> KEPT = new java.util.ArrayList<>();

        private Keeper() {
        }

        static void keep(int value) {
            KEPT.add(value);
        }
    }
}
