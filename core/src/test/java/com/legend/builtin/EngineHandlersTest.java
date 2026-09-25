// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The engine surface by bare name, read from the generated registry (untangle 4b.0). */
class EngineHandlersTest {

    /** {@code from} is an engine handler: a query names it bare, with no import. */
    @Test
    void fromIsAnEngineHandlerQualifyingToTheMappingPackage() {
        assertTrue(EngineHandlers.fqnsOf("from").contains("meta::pure::mapping::from"),
                String.valueOf(EngineHandlers.fqnsOf("from")));
        assertTrue(EngineHandlers.idsOf("from").contains(
                "meta::pure::mapping::from_FunctionDefinition_1__Runtime_1__T_m_"));
    }

    /** {@code map} denotes the collection map among the packages the engine registers it for. */
    @Test
    void mapDenotesTheCollectionMap() {
        assertTrue(EngineHandlers.fqnsOf("map").contains("meta::pure::functions::collection::map"));
    }

    /** A direct {@code register("id", "name", …)} entry (not an {@code h(…)} group) is read too. */
    @Test
    void theDirectRegistrationFormIsRead() {
        assertTrue(EngineHandlers.fqnsOf("not").contains("meta::pure::functions::boolean::not"));
        assertTrue(EngineHandlers.fqnsOf("equal").contains("meta::pure::functions::boolean::equal"));
    }

    /** A name the engine does not register is nobody's on this tier. */
    @Test
    void anUnregisteredNameIsEmpty() {
        assertEquals(List.of(), EngineHandlers.fqnsOf("noSuchEngineFunction"));
    }

    /** The platform's own surface rides as the declared extension. */
    @Test
    void theLiteSurfaceIsRegistered() {
        assertTrue(EngineHandlers.fqnsOf("joinWithPrefix").stream().allMatch(f -> f.startsWith(Pure.Lite.PKG)));
        assertTrue(!EngineHandlers.fqnsOf("joinWithPrefix").isEmpty());
    }

    /** Pinned EXACTLY (audit 2026-09-25: a floor and a ceiling let a stale or
     *  half-read registry pass), engine 4.145.0: 404 names over 836 ids, 169
     *  engine ids the platform declares nowhere. A bump moves the numbers with
     *  its dated reason; a declaration landing lowers the undeclared count. */
    @Test
    void theSurfaceAndItsGapArePinned() {
        int ids = EngineHandlers.names().stream().mapToInt(n -> EngineHandlers.idsOf(n).size()).sum();
        assertEquals(404, EngineHandlers.names().size(), "names");
        assertEquals(836, ids, "engine ids");
        assertEquals(169, EngineHandlers.undeclaredIds().size(), "undeclared engine ids");
    }
}
