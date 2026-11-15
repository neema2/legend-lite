// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.deps;

import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE LAYERS OF THE TYPESCRIPT HOSTS (C5-T, 2026-11-15: the TypeScript split of plan and run, as {@link
 * CoreLayeringTest} holds Java's). Every TypeScript layer target (a genquery {@code layer_<name>} of its direct
 * {@code deps}, made beside it) depends on exactly the targets {@code ts-layers.txt} lists for it, both ways: a layer
 * with no line fails, and so does a line with no layer. On top of the exact match, two rules the file cannot loosen:
 *
 * <ul>
 *   <li>A RUN layer reaches records and npm packages only (a run layer of another package too, which is itself held
 *   so): no planner, no server, no door, no app (the orchestrator's note 4, 2026-11-15).</li>
 *   <li>A PLANNER layer reaches nothing that can execute: records only (and another package's planner layer) -- never a
 *   run, a server or a door (the orchestrator's note 1, 2026-11-15).</li>
 * </ul>
 *
 * The protocol library ({@code //pure-protocol:pure_protocol}) is the Pure protocol's records: both may reach it.
 */
class TsLayeringTest {

    /** The protocol library: the Pure protocol's shapes and their builders, a record library to both sides. */
    private static final String PROTOCOL = "//pure-protocol:pure_protocol";

    @Test
    void everyTypeScriptLayerDependsOnExactlyItsListedLayers() throws IOException {
        Map<String, String> expected = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Runfile.property("ts.layers"))) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            expected.put(parts[0], parts.length > 1 ? normalise(List.of(parts[1].split(" "))) : "");
        }
        assertTrue(expected.size() >= 5, "ts-layers.txt lists " + expected.size() + " layers: the guard is not looking");
        // each layer_<name> genquery output, by the layer's label: `<repo>/<package>/layer_<name>`
        Map<String, Path> layerFiles = new LinkedHashMap<>();
        for (String rlocationpath : System.getenv("TS_LAYER_FILES").split(" ")) {
            if (rlocationpath.isBlank()) continue;
            int at = rlocationpath.lastIndexOf("/layer_");
            if (at < 0) throw new IllegalStateException("TS_LAYER_FILES holds " + rlocationpath + ", no layer query");
            String pkg = rlocationpath.substring(rlocationpath.indexOf('/') + 1, at);
            layerFiles.put("//" + pkg + ":" + rlocationpath.substring(at + "/layer_".length()), Runfile.of(rlocationpath));
        }
        List<String> drift = new ArrayList<>();
        for (String layer : layerFiles.keySet()) {
            if (!expected.containsKey(layer)) {
                drift.add(layer + " is a TypeScript layer with no line in tools/deps/ts-layers.txt (add its allowed deps)");
            }
        }
        for (Map.Entry<String, String> e : expected.entrySet()) {
            String layer = e.getKey();
            Path query = layerFiles.get(layer);
            if (query == null) {
                drift.add("ts-layers.txt lists " + layer + ", which has no layer query (layer_" + name(layer)
                        + " beside it, in TS_LAYER_FILES): delete its line, or add the query");
                continue;
            }
            String actual = normalise(Files.readAllLines(query));
            if (!actual.equals(e.getValue())) {
                TreeSet<String> added = new TreeSet<>(List.of(actual.split(" ")));
                added.removeAll(List.of(e.getValue().split(" ")));
                TreeSet<String> removed = new TreeSet<>(List.of(e.getValue().split(" ")));
                removed.removeAll(List.of(actual.split(" ")));
                added.remove("");
                removed.remove("");
                drift.add(layer
                        + (added.isEmpty() ? "" : " NOW DEPENDS ON " + added
                                + " (add it to ts-layers.txt with the task that allows it)")
                        + (removed.isEmpty() ? "" : " NO LONGER DEPENDS ON " + removed
                                + " (delete it from ts-layers.txt in the same push)"));
            }
        }
        assertEquals(List.of(), drift, "the layers of the TypeScript hosts changed");
    }

    @Test
    void aRunLayerReachesRecordsAndNpmAloneAndAPlannerNothingThatExecutes() throws IOException {
        List<String> breaches = new ArrayList<>();
        int runs = 0;
        int planners = 0;
        for (String line : Files.readAllLines(Runfile.property("ts.layers"))) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            String layer = parts[0];
            List<String> deps = parts.length > 1 ? List.of(parts[1].split(" ")) : List.of();
            if (name(layer).equals("run")) {
                runs++;
                for (String dep : deps) {
                    if (!(isRecords(dep) || isNpm(dep) || name(dep).equals("run"))) {
                        breaches.add(layer + " reaches " + dep + ": a run layer reaches records and npm packages alone"
                                + " (no planner, server, door or app)");
                    }
                }
            } else if (name(layer).equals("planner")) {
                planners++;
                for (String dep : deps) {
                    if (!(isRecords(dep) || isNpm(dep) || name(dep).equals("planner"))) {
                        breaches.add(layer + " reaches " + dep + ": a planner reaches nothing that can execute"
                                + " (records and the protocol alone)");
                    }
                }
            }
        }
        assertTrue(runs >= 1 && planners >= 1, "ts-layers.txt names " + runs + " run and " + planners
                + " planner layers: the guard is not looking");
        assertEquals(List.of(), breaches, "a TypeScript layer reaches past its rule");
    }

    /** A label's target name: `//engine-client:run` is `run`. */
    private static String name(String label) {
        return label.substring(label.indexOf(':') + 1);
    }

    private static boolean isRecords(String label) {
        return label.equals(PROTOCOL) || name(label).equals("records");
    }

    private static boolean isNpm(String label) {
        return name(label).startsWith("node_modules/");
    }

    /** The labels, sorted, one space apart. */
    private static String normalise(List<String> labels) {
        TreeSet<String> out = new TreeSet<>();
        for (String l : labels) {
            String s = l.strip();
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return String.join(" ", out);
    }
}
