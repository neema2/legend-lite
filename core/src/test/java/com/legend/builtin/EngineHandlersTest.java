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

    /** Pinned 2026-09-25, engine 4.145.0: 404 names; 169 engine ids the platform
     *  declares nowhere — the census of what it does not carry, SHRINK-ONLY
     *  (a declaration lands, the number falls, the pin follows). */
    @Test
    void theSurfaceAndItsGapArePinned() {
        assertTrue(EngineHandlers.names().size() >= 404, "names: " + EngineHandlers.names().size());
        assertTrue(EngineHandlers.undeclaredIds().size() <= 169,
                "undeclared engine ids grew: " + EngineHandlers.undeclaredIds().size());
    }
}
