// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (EdgeSeparationTest.eachGuardBites): a dialect -- a subclass of AnsiSqlRenderer, at the dialects' home --
 *  whose arm compares a function and writes text, carrying nothing out: a renderer's read, not named. */
public abstract class PlantedSqlRendererArm extends AnsiSqlRenderer {
    protected PlantedSqlRendererArm() {
        super(null);
    }

    static String arm(com.legend.sql.SqlFn fn) {
        return fn == com.legend.sql.SqlFn.ABS ? "ABS" : "?";
    }
}
