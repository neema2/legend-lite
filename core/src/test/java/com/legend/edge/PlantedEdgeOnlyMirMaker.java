// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.PlantedEdgeOnlyMir;

/** L-B's planted edge (EdgeOnlyDataTest.theGuardBites, 2026-11-18): it makes the variant only it makes, and reads the
 *  field and names the constant only it reads and names. Read as bytecode alone; nothing runs it. */
public final class PlantedEdgeOnlyMirMaker {

    private PlantedEdgeOnlyMirMaker() {
    }

    /** The edge's use. */
    public static String edgeUse() {
        PlantedEdgeOnlyMir.Made m = new PlantedEdgeOnlyMir.Made("x");
        PlantedEdgeOnlyMir.Shared s = new PlantedEdgeOnlyMir.Shared("n", "t", PlantedEdgeOnlyMir.Kind.RAW);
        return m.text() + s.template();
    }
}
