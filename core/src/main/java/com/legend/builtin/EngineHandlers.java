// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE ENGINE SURFACE BY BARE NAME (untangle step 4b): legend-engine resolves a
 * query's function by its bare name against its handler registry first, then
 * through the import group. This is that registry — {@code engine-handlers.tsv},
 * generated from the pinned engine's {@code Handlers.java} and drift-tested,
 * plus the platform's own surface as its declared extension. A bare name the
 * registry holds qualifies to the FQNs the platform declares for the engine's
 * signature ids; a bare name it does not hold is the import group's business,
 * or nobody's.
 */
public final class EngineHandlers {

    private EngineHandlers() {
    }

    /** name → the FQNs the platform declares for the engine's ids under that name. */
    private static final Map<String, List<String>> FQNS;
    /** name → every engine id under that name, declared here or not. */
    private static final Map<String, List<String>> IDS;
    /** The engine's ids the platform declares nowhere — the census of what it does not carry. */
    private static final List<String> UNDECLARED;

    static {
        Map<String, List<String>> fqns = new LinkedHashMap<>();
        Map<String, List<String>> ids = new LinkedHashMap<>();
        List<String> undeclared = new ArrayList<>();
        for (String line : read().split("\n")) {
            if (line.isBlank() || line.startsWith("#") || line.startsWith("name\t")) {
                continue;
            }
            String[] c = line.split("\t", -1);
            if (c.length != 4) {
                throw new IllegalStateException("engine-handlers.tsv: expected 4 columns: " + line);
            }
            ids.computeIfAbsent(c[0], k -> new ArrayList<>()).add(c[1]);
            if (c[2].isEmpty()) {
                undeclared.add(c[1]);
            }
            if (!c[2].isEmpty()) {
                List<String> at = fqns.computeIfAbsent(c[0], k -> new ArrayList<>());
                if (!at.contains(c[2])) {
                    at.add(c[2]);
                }
            }
        }
        fqns.replaceAll((k, v) -> List.copyOf(v));
        ids.replaceAll((k, v) -> List.copyOf(v));
        FQNS = Map.copyOf(fqns);
        IDS = Map.copyOf(ids);
        UNDECLARED = List.copyOf(undeclared);
    }

    /** The engine's signature ids the platform declares nowhere (shrink-only, pinned). */
    public static List<String> undeclaredIds() {
        return UNDECLARED;
    }

    private static String read() {
        try (InputStream in = EngineHandlers.class.getResourceAsStream("/com/legend/builtin/engine-handlers.tsv")) {
            if (in == null) {
                throw new IllegalStateException("engine-handlers.tsv missing from the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("engine-handlers.tsv unreadable", e);
        }
    }

    /** The FQNs a bare {@code name} denotes on the engine surface (empty: not a handler name). */
    public static List<String> fqnsOf(String name) {
        return FQNS.getOrDefault(name, List.of());
    }

    /** The engine's signature ids under a bare {@code name}, declared here or not. */
    public static List<String> idsOf(String name) {
        return IDS.getOrDefault(name, List.of());
    }

    /** Every bare name the engine registers a handler for. */
    public static Set<String> names() {
        return IDS.keySet();
    }
}
