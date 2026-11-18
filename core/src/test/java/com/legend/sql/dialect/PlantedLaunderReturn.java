// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of 091b9bde36, 2026-11-18, MUST-FIX 3): a dialect handing the function back to its caller. */
public abstract class PlantedLaunderReturn extends AnsiSqlRenderer {
    protected PlantedLaunderReturn() {
        super(null);
    }

    static com.legend.sql.SqlFn fn() {
        return com.legend.sql.SqlFn.ABS;
    }
}
