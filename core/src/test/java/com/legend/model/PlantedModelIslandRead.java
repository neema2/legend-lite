// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.cypher.CypherAst;
import com.legend.protocol.cypher.CypherParser;

/**
 * A class PLANTED in the model package to read an island's text past the parse doors, for one test alone (C5-J's last
 * round's re-check, 2026-11-15): the model package was exempt from {@code ArchitectureTest.PARSE_BY_THE_DOORS} whole,
 * and may reach the protocol layer the island readers live in, so a model class could be a door that asks nothing. The
 * exemption is the records door's alone now, and the rule fails on this. A test class: the rule's real check reads the
 * product's classes, never this one.
 */
public final class PlantedModelIslandRead {

    private PlantedModelIslandRead() {
    }

    /** A {@code #Cypher} statement read in the model package, past the door. */
    public static CypherAst.Statement read(String text) {
        return CypherParser.parse(text);
    }
}
