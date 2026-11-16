// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol;

/** A door PLANTED to ask the run's wall only after it parses, for one test alone (C5-J item 1, 2026-11-15:
 *  {@code ArchitectureTest.parseDoorViolations} names it). A test class, never a product door. */
public final class PlantedLateDoor {

    private PlantedLateDoor() {
    }

    public static Protocol.PureModelContextData records(String source) {
        Protocol.PureModelContextData records = PmcdParser.parseModel(source);
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        return records;
    }
}
