// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/**
 * A PLANTED RENDERER for {@code EdgeSeparationTest.eachGuardBites} (the edge audit's K16, 2026-11-17): a dialect's arm
 * reads a function's constant to render it, which writes nothing, so the CODE guard does not name it. Test sources only.
 */
public abstract class PlantedSqlRendererArm implements SqlDialect {

    static Object arm() {
        return com.legend.sql.SqlFn.ABS;
    }
}
