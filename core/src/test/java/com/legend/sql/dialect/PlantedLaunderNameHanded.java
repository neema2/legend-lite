// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of the guard line, G, SHOULD 1, 2026-11-18): a dialect handing the function's name to a
 *  keeper whose parameter is a String, setting no field of its own -- the hand-off is the call. */
public abstract class PlantedLaunderNameHanded extends AnsiSqlRenderer {
    protected PlantedLaunderNameHanded() {
        super(null);
    }

    static void keep() {
        Keeper.keep(com.legend.sql.SqlFn.ABS.name());
    }

    /** The keeper, outside the dialect's own code. */
    static final class Keeper {
        static final java.util.List<Object> KEPT = new java.util.ArrayList<>();

        private Keeper() {
        }

        static void keep(String value) {
            KEPT.add(value);
        }
    }
}
