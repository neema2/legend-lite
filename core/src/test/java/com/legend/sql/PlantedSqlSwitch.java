// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * A PLANTED DISPATCH for {@code EdgeSeparationTest.eachGuardBites} (the edge audit's K16, 2026-11-17): a switch on a
 * function in the sql package, whose constant javac's switch map reads -- a dispatch, which writes nothing, so the CODE
 * guard does not name it. Test sources only.
 */
final class PlantedSqlSwitch {

    private PlantedSqlSwitch() {
    }

    static int arm(SqlFn f) {
        switch (f) {
            case ABS:
                return 1;
            default:
                return 0;
        }
    }
}
