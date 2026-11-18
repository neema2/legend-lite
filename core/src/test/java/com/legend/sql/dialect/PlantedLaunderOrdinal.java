// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (review F of the guard line, 2026-11-18, SHOULD 1): a dialect storing the function's ordinal and its name in
 *  fields -- the constant carried out as the number or the text that names it. */
public abstract class PlantedLaunderOrdinal extends AnsiSqlRenderer {
    static int ord;
    static String name;

    protected PlantedLaunderOrdinal() {
        super(null);
    }

    static void keepOrdinal() {
        ord = com.legend.sql.SqlFn.ABS.ordinal();
    }

    static void keepName() {
        name = com.legend.sql.SqlFn.ABS.name();
    }
}
