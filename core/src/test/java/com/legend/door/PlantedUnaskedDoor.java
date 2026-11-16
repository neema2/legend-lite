// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol;

/** A door PLANTED to parse without asking the run's wall, for one test alone (C5-J item 1, 2026-11-15:
 *  {@code ArchitectureTest.parseDoorViolations} names it). A test class, never a product door. */
public final class PlantedUnaskedDoor {

    private PlantedUnaskedDoor() {
    }

    public static Protocol.PureModelContextData records(String source) {
        return PmcdParser.parseModel(source);
    }
}
