// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import com.legend.model.Function;
import com.legend.model.FunctionId;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.SignatureMangle;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * THE ENGINE SURFACE BY BARE NAME (untangle step 4b): legend-engine resolves a
 * query's function by its bare name against its handler registry first, then
 * through the import group. This is that registry: {@code engine-handlers.tsv},
 * generated from the pinned engine's {@code Handlers.java} alone (each bare name
 * and its signature ids), joined here, when the class loads, with the platform's
 * declarations (the catalog's native or the prelude's function of each id), plus
 * the platform's own surface as its declared extension. A bare name the registry
 * holds qualifies to the FQNs the platform declares for the engine's signature
 * ids; a bare name it does not hold is the import group's business, or nobody's.
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
    /** The platform's surface function ids no declaration carries (EngineHandlersTest holds this empty). */
    private static final List<String> UNMATCHED_SURFACE;

    static {
        // the platform's declaration of each signature id: the catalog's natives, then the prelude's functions
        Map<String, Function> declared = new LinkedHashMap<>();
        for (NativeFunctionDefinition n : Pure.all()) {
            declared.put(SignatureMangle.mangle(n), n);
        }
        for (PackageableElement el : Prelude.elements()) {
            if (el instanceof Function f) {
                declared.putIfAbsent(SignatureMangle.mangle(f), f);
            }
        }
        Map<String, List<String>> fqns = new LinkedHashMap<>();
        Map<String, List<String>> ids = new LinkedHashMap<>();
        List<String> undeclared = new ArrayList<>();
        for (String line : read().split("\n")) {
            if (line.strip().isEmpty() || line.startsWith("#") || line.equals("name\tid")) {
                continue;
            }
            String[] c = line.split("\t", -1);
            if (c.length != 2) {
                throw new IllegalStateException("engine-handlers.tsv: expected 2 columns (name, id): " + line);
            }
            Function f = declared.get(c[1]);
            add(ids, fqns, c[0], c[1], f == null ? null : f.qualifiedName());
            if (f == null) {
                undeclared.add(c[1]);
            }
        }
        // the platform's own surface, its declared extension (the engine's
        // getExtraFunctionHandlerDispatchBuilderInfoCollectors hook, mirrored): each surface name and the lite
        // overloads it stands for, by function id (Pure.liteSurfaceFunctions), joined like the engine's rows
        List<String> unmatched = new ArrayList<>();
        for (Map.Entry<String, List<FunctionId>> e : new TreeMap<>(Pure.liteSurfaceFunctions()).entrySet()) {
            for (FunctionId id : e.getValue()) {
                Function f = declared.get(id.qualified());
                if (f == null) {
                    unmatched.add(id.qualified());
                } else {
                    add(ids, fqns, e.getKey(), id.qualified(), f.qualifiedName());
                }
            }
        }
        fqns.replaceAll((k, v) -> List.copyOf(v));
        ids.replaceAll((k, v) -> List.copyOf(v));
        FQNS = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(fqns));
        IDS = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(ids));
        UNDECLARED = List.copyOf(undeclared);
        UNMATCHED_SURFACE = List.copyOf(unmatched);
    }

    private static void add(Map<String, List<String>> ids, Map<String, List<String>> fqns, String name, String id,
            @com.legend.base.Nullable String fqn) {
        ids.computeIfAbsent(name, k -> new ArrayList<>()).add(id);
        if (fqn != null) {
            List<String> at = fqns.computeIfAbsent(name, k -> new ArrayList<>());
            if (!at.contains(fqn)) {
                at.add(fqn);
            }
        }
    }

    /** The platform's surface function ids ({@link Pure#liteSurfaceFunctions}) no declaration carries: a broken
     *  surface entry. */
    public static List<String> unmatchedSurface() {
        return UNMATCHED_SURFACE;
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
