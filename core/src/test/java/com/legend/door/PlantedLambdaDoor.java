// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol;

/** A door PLANTED to parse in a lambda, for one test alone (C5-J item 1, 2026-11-15: ArchUnit reads the lambda's calls
 *  as the method's, so its question would be asked when the callback is made, not when it parses;
 *  {@code ArchitectureTest.parseDoorViolations} names it). A test class, never a product door. */
public final class PlantedLambdaDoor {

    private PlantedLambdaDoor() {
    }

    public static java.util.function.Function<String, Protocol.PureModelContextData> records() {
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        return source -> PmcdParser.parseModel(source);
    }
}
