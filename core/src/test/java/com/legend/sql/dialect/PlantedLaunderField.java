// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of 091b9bde36, 2026-11-18, MUST-FIX 3): a dialect keeping the function in a field another
 *  class can read. */
public abstract class PlantedLaunderField extends AnsiSqlRenderer {
    static Object kept;

    protected PlantedLaunderField() {
        super(null);
    }

    static void keep() {
        kept = com.legend.sql.SqlFn.ABS;
    }
}
