// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.finos.legend.engine.protocol.pure.v1.extension.PureProtocolExtension;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ELEMENT DENOMINATOR: every {@code PackageableElement} legend-engine can
 * produce, asked of its own protocol registry.
 *
 * <p>The section roster ({@link EngineSectionRosterTest}) answers "which
 * `###X` can engine read". This answers the finer question the burn-down
 * actually needs: WITHIN those sections, which ELEMENT TYPES exist. A section
 * is not done because its header parses; it is done when every element it can
 * contain round-trips.
 *
 * <p>Source is {@code getExtraProtocolToClassifierPathMap()}, which every
 * extension implements to map its element classes to Pure classifier paths —
 * the same map the JSON layer uses, so it cannot drift from what engine can
 * actually serialise.
 *
 * <p>Prints the roster for {@code docs/SECTION_PROGRAM_HANDOFF.md} and fails
 * if it SHRINKS.
 */
class EngineElementRosterTest {

    /** Element types at the pinned oracle release (release.MODULE.bazel's pins). The burn-down
     *  denominator: 25 sections x these 41 element types is the whole job. */
    private static final int MIN_ELEMENTS = 41;

    @Test
    void listEveryElementTypeEngineCanProduce() {
        Map<String, String> byClass = new TreeMap<>();
        java.util.List<String> unread = new java.util.ArrayList<>();
        for (PureProtocolExtension ext
                : java.util.ServiceLoader.load(PureProtocolExtension.class)) {
            Map<Class<? extends PackageableElement>, String> m;
            try {
                m = ext.getExtraProtocolToClassifierPathMap();
            } catch (RuntimeException e) {
                // an extension that needs a fuller classpath: named, never silent (Bazel workplan P3-14)
                unread.add(ext.getClass().getName() + " (" + e.getClass().getSimpleName() + ")");
                continue;
            }
            m.forEach((k, v) -> byClass.put(k.getSimpleName(), v));
        }

        System.out.println("[engine-elements] " + byClass.size()
                + " packageable element types:");
        byClass.forEach((k, v) ->
                System.out.println("[engine-element] " + k + "  ::  " + v));

        System.out.println("[engine-elements] extensions not read: " + unread);
        assertTrue(byClass.size() >= MIN_ELEMENTS,
                "engine element roster shrank to " + byClass.size() + " (extensions not read: " + unread + ")");
    }
}
