// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (the review of 091b9bde36, 2026-11-18, MUST-FIX 3): a class in the dialects' package implementing
 *  SqlDialect that is no dialect (no AnsiSqlRenderer); its read, though it carries nothing out, is named. */
public abstract class PlantedRendererByInterface implements SqlDialect {
    static String arm(com.legend.sql.SqlFn fn) {
        return fn == com.legend.sql.SqlFn.ABS ? "ABS" : "?";
    }
}
