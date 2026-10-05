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

    /** The registry is read WHOLE (audit 2026-09-25: a stale or half-read registry must not pass): every row of the
     *  generated engine-handlers.tsv -- parsed here on its own -- is a name, an id and, where the platform declares it,
     *  an FQN the API reports. Computed from the file, not pinned by hand (Bazel workplan P2-16): the file is
     *  //core:update_generated's, so a bump or a declaration moves it, as a reviewed diff, and nothing here. */
    @Test
    void theApiReportsEveryRowOfTheRegistry() throws java.io.IOException {
        java.util.Map<String, java.util.Set<String>> ids = new java.util.TreeMap<>();
        java.util.Set<String> undeclared = new java.util.TreeSet<>();
        try (java.io.InputStream in = EngineHandlers.class.getResourceAsStream("/com/legend/builtin/engine-handlers.tsv")) {
            for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                if (line.isBlank() || line.startsWith("#") || line.startsWith("name\t")) {   // comments; the column header
                    continue;
                }
                String[] c = line.split("\t", -1);
                ids.computeIfAbsent(c[0], k -> new java.util.TreeSet<>()).add(c[1]);
                if (c[2].isEmpty()) {
                    undeclared.add(c[1]);
                }
            }
        }
        assertTrue(ids.size() > 300, "engine-handlers.tsv lists " + ids.size() + " names: the registry is not being read");
        java.util.Set<String> onlyFile = new java.util.TreeSet<>(ids.keySet());
        onlyFile.removeAll(EngineHandlers.names());
        java.util.Set<String> onlyApi = new java.util.TreeSet<>(EngineHandlers.names());
        onlyApi.removeAll(ids.keySet());
        assertEquals("", (onlyFile.isEmpty() ? "" : "in the file, not the API: " + onlyFile)
                + (onlyApi.isEmpty() ? "" : " in the API, not the file: " + onlyApi), "names");
        for (String name : ids.keySet()) {
            assertEquals(ids.get(name), new java.util.TreeSet<>(EngineHandlers.idsOf(name)), "ids of " + name);
        }
        assertEquals(undeclared, new java.util.TreeSet<>(EngineHandlers.undeclaredIds()), "undeclared engine ids");
    }
}
