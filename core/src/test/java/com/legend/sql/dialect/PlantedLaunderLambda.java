// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of 091b9bde36, 2026-11-18, MUST-FIX 3): a dialect's lambda, held in a field, handing the
 *  function back. */
public abstract class PlantedLaunderLambda extends AnsiSqlRenderer {
    static final java.util.function.Supplier<com.legend.sql.SqlFn> SUP = () -> com.legend.sql.SqlFn.ABS;

    protected PlantedLaunderLambda() {
        super(null);
    }
}
