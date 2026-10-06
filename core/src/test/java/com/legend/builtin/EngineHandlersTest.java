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
     *  generated engine-handlers.tsv -- upstream's (name, id) pairs, parsed here on its own -- is a name and an id the
     *  API reports, and each id is either one the platform declares (its name then has an FQN) or one it does not
     *  (in undeclaredIds). The names the file does not hold are the platform's own surface. Computed from the file,
     *  not pinned by hand (Bazel workplan P2-16): the file is //core:update_generated's, so only a bump moves it. */
    @Test
    void theApiReportsEveryRowOfTheRegistry() throws java.io.IOException {
        java.util.Map<String, java.util.Set<String>> ids = new java.util.TreeMap<>();
        try (java.io.InputStream in = EngineHandlers.class.getResourceAsStream("/com/legend/builtin/engine-handlers.tsv")) {
            for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                if (line.isBlank() || line.startsWith("#") || line.startsWith("name\t")) {   // comments; the column header
                    continue;
                }
                String[] c = line.split("\t", -1);
                assertEquals(2, c.length, "a row is a name and an id: " + line);
                ids.computeIfAbsent(c[0], k -> new java.util.TreeSet<>()).add(c[1]);
            }
        }
        assertTrue(ids.size() > 300, "engine-handlers.tsv lists " + ids.size() + " names: the registry is not being read");
        java.util.Set<String> onlyFile = new java.util.TreeSet<>(ids.keySet());
        onlyFile.removeAll(EngineHandlers.names());
        assertEquals(java.util.Set.of(), onlyFile, "names in the file, not the API");
        java.util.Set<String> onlyApi = new java.util.TreeSet<>(EngineHandlers.names());
        onlyApi.removeAll(ids.keySet());
        assertTrue(Pure.LITE_SURFACE.containsAll(onlyApi), "names in the API beyond the file are the platform's own"
                + " surface: " + onlyApi);
        java.util.Set<String> undeclared = new java.util.HashSet<>(EngineHandlers.undeclaredIds());
        for (String name : ids.keySet()) {
            assertTrue(EngineHandlers.idsOf(name).containsAll(ids.get(name)), "ids of " + name);
            if (!Pure.LITE_SURFACE.contains(name)) {
                assertEquals(ids.get(name), new java.util.TreeSet<>(EngineHandlers.idsOf(name)), "ids of " + name);
            }
            for (String id : ids.get(name)) {
                assertTrue(undeclared.contains(id) || !EngineHandlers.fqnsOf(name).isEmpty(),
                        id + " is neither declared (an FQN under " + name + ") nor in undeclaredIds");
            }
        }
    }

    /** The surface by function id names exactly the surface's bare names, and every id has a declaration: a broken
     *  entry would vanish from the surface. */
    @Test
    void theSurfaceIsDeclaredByFunctionId() {
        assertEquals(Pure.LITE_SURFACE, Pure.liteSurfaceFunctions().keySet(),
                "Pure.liteSurfaceFunctions names exactly Pure.LITE_SURFACE");
        assertEquals(List.of(), EngineHandlers.unmatchedSurface(), "surface function ids no declaration carries");
        // each name's group is that name's lite overloads: a name paired with the wrong AT_ group fails here
        Pure.liteSurfaceFunctions().forEach((name, group) -> group.forEach(id -> assertTrue(
                id.qualified().startsWith(Pure.Lite.PKG + name + "_"), name + " is paired with " + id)));
    }
}
