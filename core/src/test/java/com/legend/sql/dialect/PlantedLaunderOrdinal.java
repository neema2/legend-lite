// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (review F of the guard line, 2026-11-18, SHOULD 1; one shape a class, the review of the guard line, G,
 *  SHOULD 1): a dialect storing the function's ordinal in a field -- the constant carried out as its number. */
public abstract class PlantedLaunderOrdinal extends AnsiSqlRenderer {
    static int ord;

    protected PlantedLaunderOrdinal() {
        super(null);
    }

    static void keepOrdinal() {
        ord = com.legend.sql.SqlFn.ABS.ordinal();
    }
}
