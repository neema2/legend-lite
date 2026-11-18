// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (one shape a class, the review of the guard line, G, SHOULD 1, 2026-11-18): a dialect storing the
 *  function's name in a field -- the constant carried out as the text that names it. */
public abstract class PlantedLaunderName extends AnsiSqlRenderer {
    static String name;

    protected PlantedLaunderName() {
        super(null);
    }

    static void keepName() {
        name = com.legend.sql.SqlFn.ABS.name();
    }
}
