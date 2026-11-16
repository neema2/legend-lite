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
import java.util.Set;
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
 *
 * <p>2026-11-15 (the C5-T audit's nit): the rule trusts no target by its name. The run and planner layers, the records
 * and the npm packages a run layer may reach are each pinned by label (RUN_LAYERS, PLANNER_LAYERS, RECORDS, RUN_NPM): a
 * new target named {@code :records} or {@code :run}, or a new {@code node_modules/...}, is no record, no run layer and no
 * package a run may reach until it is pinned here, with its dated note.
 *
 * <p>2026-11-15 (the C5-T last round's T1, Java's {@code onlyTheDoorsCompose}): an APP -- DataCube's, Query's and
 * Studio's own code, and what DataCube's builds on -- reaches no run layer, nor the union that holds them, nor Node's
 * host: the run side through the doors alone. Each library an app takes beside the layers reaches none either
 * ({@code app_libraries_run}, a genquery: empty). That an app MODULE imports no run module through what it may reach is
 * the apps' census's ({@code //tools/deps:app_runs_test}).
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

    // 2026-11-15 (the C5-T audit's nit, measured): the layers and the labels the rule reads, each by its label
    /** The run layers: what sends a plan's statements and reads their rows. */
    static final Set<String> RUN_LAYERS = Set.of("//engine-client:run", "//datacube:run");
    /** The planner layers: what plans, and executes nothing. */
    static final Set<String> PLANNER_LAYERS = Set.of("//engine-client:planner", "//datacube:planner");
    /** The server layers (2026-11-15, the C5-T final audit's MUST 1): legend-engine's clients, which reach no engine. */
    static final Set<String> SERVER_LAYERS = Set.of("//engine-client:server");
    /** The engine's API (the same day): the engines' interfaces and the run frame -- what can run. The run layers, the
     *  doors and the apps reach it; no planner and no server does. */
    static final String ENGINE_API = "//engine-client:engine_api";
    /** The npm packages a server layer reaches: the zstd decoder its Arrow answers are read with. */
    static final Set<String> SERVER_NPM = Set.of("//engine-client:node_modules/fzstd");
    /** The records: shapes both sides read, no send, no plan -- and the protocol library, the Pure protocol's. */
    static final Set<String> RECORDS = Set.of("//engine-client:records", "//datacube:records", "//sdlc-client:records",
            "//model-changes:records", PROTOCOL);
    /** The apps (2026-11-15, the C5-T last round's T1): DataCube's, what it builds on, Query's and Studio's. */
    static final Set<String> APP_LAYERS = Set.of("//datacube:src", "//datacube:src_base", "//query:src", "//studio:src");
    /** What no app reaches: the run layers, the union that holds every layer, and Node's host. */
    static final Set<String> NOT_FOR_APPS = Set.of("//engine-client:run", "//datacube:run", "//engine-client:engine_client",
            "//engine-client:node_host");
    /** The npm packages a run layer reaches: DuckDB in the tab, and the Arrow its answers and the warehouse's are read with. */
    static final Set<String> RUN_NPM = Set.of("//engine-client:node_modules/@duckdb/duckdb-wasm",
            "//engine-client:node_modules/apache-arrow");

    @Test
    void aRunLayerReachesRecordsAndNpmAloneAndAPlannerNothingThatExecutes() throws IOException {
        List<String> lines = Files.readAllLines(Runfile.property("ts.layers"));
        long runs = lines.stream().filter((l) -> RUN_LAYERS.stream().anyMatch((r) -> l.startsWith(r + "\t"))).count();
        long planners = lines.stream().filter((l) -> PLANNER_LAYERS.stream().anyMatch((p) -> l.startsWith(p + "\t"))).count();
        assertEquals(RUN_LAYERS.size() + PLANNER_LAYERS.size(), runs + planners, "ts-layers.txt names " + runs + " run and "
                + planners + " planner layers of the " + (RUN_LAYERS.size() + PLANNER_LAYERS.size()) + " pinned: the guard is not looking");
        long servers = lines.stream().filter((l) -> SERVER_LAYERS.stream().anyMatch((v) -> l.startsWith(v + "\t"))).count();
        long api = lines.stream().filter((l) -> l.startsWith(ENGINE_API + "\t")).count();
        assertEquals(SERVER_LAYERS.size() + 1, servers + api, "ts-layers.txt names " + servers + " server layers and " + api
                + " engine's API of the " + (SERVER_LAYERS.size() + 1) + " pinned: the guard is not looking");
        assertEquals(List.of(), ruleBreaches(lines), "a TypeScript layer reaches past its rule");
        long apps = lines.stream().filter((l) -> APP_LAYERS.stream().anyMatch((a) -> l.startsWith(a + "\t"))).count();
        assertEquals(APP_LAYERS.size(), apps, "ts-layers.txt names " + apps + " of the " + APP_LAYERS.size()
                + " apps pinned: the guard is not looking");
    }

    @Test
    void noLibraryOfAnAppReachesTheRunSide() throws IOException {
        List<String> reached = Files.readAllLines(Runfile.property("app.libraries.run")).stream()
                .filter((l) -> !l.isBlank()).toList();
        assertEquals(List.of(), reached, "a library an app takes beside the layers reaches the run side: the app reaches it"
                + " past its doors (//tools/deps:app_libraries_run)");
    }

    @Test
    void theRuleTrustsNoTargetByItsName() {
        List<String> planted = List.of(
                "//engine-client:run\t//engine-client:records //planted:records //engine-client:node_modules/left-pad",
                "//engine-client:planner\t//engine-client:records //datacube:node_modules/fflate //engine-client:run",
                "//planted:run\t//engine-client:records",
                "//planted:planner\t//pure-protocol:pure_protocol",
                "//datacube:src\t//datacube:doors //datacube:run",
                "//studio:src\t//engine-client:engine_client //engine-client:node_host",
                "//query:src\t//engine-client:doors //engine-client:run",
                // 2026-11-15 (the C5-T final audit's MUST 1): the engine's API, reached by a run layer, never by a planner
                // or a server, and itself reaching records alone
                "//datacube:run\t//engine-client:records //engine-client:engine_api",
                "//datacube:planner\t//engine-client:records //engine-client:engine_api",
                "//engine-client:server\t//engine-client:planner //engine-client:engine_api //engine-client:run",
                "//engine-client:engine_api\t//engine-client:records //engine-client:run",
                "//planted:server\t//engine-client:records",
                "//planted:engine_api\t//engine-client:records");
        assertEquals(List.of(
                "//datacube:planner reaches //engine-client:engine_api: a planner reaches nothing that can execute"
                        + " (records and the protocol alone)",
                "//datacube:src reaches //datacube:run: an app reaches the run side through the doors alone",
                "//engine-client:engine_api reaches //engine-client:run: the engine's API reaches records alone",
                "//engine-client:planner reaches //datacube:node_modules/fflate: a planner reaches nothing that can execute"
                        + " (records and the protocol alone)",
                "//engine-client:planner reaches //engine-client:run: a planner reaches nothing that can execute"
                        + " (records and the protocol alone)",
                "//engine-client:run reaches //engine-client:node_modules/left-pad: a run layer reaches records, the"
                        + " engine's API and the npm packages pinned alone (no planner, server, door or app)",
                "//engine-client:run reaches //planted:records: a run layer reaches records, the engine's API and the npm"
                        + " packages pinned alone (no planner, server, door or app)",
                "//engine-client:server reaches //engine-client:engine_api: a server layer reaches no engine's API, run"
                        + " layer or door (records, the planner and its npm package alone)",
                "//engine-client:server reaches //engine-client:run: a server layer reaches no engine's API, run layer or"
                        + " door (records, the planner and its npm package alone)",
                "//planted:engine_api is named the engine's API, and is not the one pinned (TsLayeringTest.ENGINE_API)",
                "//planted:planner is named a planner layer, and no planner layer pinned (TsLayeringTest.PLANNER_LAYERS)",
                "//planted:run is named a run layer, and no run layer pinned (TsLayeringTest.RUN_LAYERS)",
                "//planted:server is named a server layer, and no server layer pinned (TsLayeringTest.SERVER_LAYERS)",
                "//query:src reaches //engine-client:run: an app reaches the run side through the doors alone",
                "//studio:src reaches //engine-client:engine_client: an app reaches the run side through the doors alone",
                "//studio:src reaches //engine-client:node_host: an app reaches the run side through the doors alone"),
                ruleBreaches(planted));
    }

    /** Each line of {@code lines} (ts-layers.txt's) whose layer reaches past its rule, or is named a run or planner
     *  layer and not pinned as one. */
    static List<String> ruleBreaches(List<String> lines) {
        List<String> breaches = new ArrayList<>();
        for (String line : lines) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            String layer = parts[0];
            List<String> deps = parts.length > 1 ? List.of(parts[1].split(" ")) : List.of();
            if (name(layer).equals("run") && !RUN_LAYERS.contains(layer)) {
                breaches.add(layer + " is named a run layer, and no run layer pinned (TsLayeringTest.RUN_LAYERS)");
            } else if (name(layer).equals("planner") && !PLANNER_LAYERS.contains(layer)) {
                breaches.add(layer + " is named a planner layer, and no planner layer pinned (TsLayeringTest.PLANNER_LAYERS)");
            } else if (name(layer).equals("server") && !SERVER_LAYERS.contains(layer)) {
                breaches.add(layer + " is named a server layer, and no server layer pinned (TsLayeringTest.SERVER_LAYERS)");
            } else if (name(layer).equals("engine_api") && !ENGINE_API.equals(layer)) {
                breaches.add(layer + " is named the engine's API, and is not the one pinned (TsLayeringTest.ENGINE_API)");
            } else if (ENGINE_API.equals(layer)) {
                for (String dep : deps) {
                    if (!RECORDS.contains(dep)) {
                        breaches.add(layer + " reaches " + dep + ": the engine's API reaches records alone");
                    }
                }
            } else if (RUN_LAYERS.contains(layer)) {
                for (String dep : deps) {
                    if (!(RECORDS.contains(dep) || ENGINE_API.equals(dep) || RUN_NPM.contains(dep) || RUN_LAYERS.contains(dep))) {
                        breaches.add(layer + " reaches " + dep + ": a run layer reaches records, the engine's API and the npm"
                                + " packages pinned alone (no planner, server, door or app)");
                    }
                }
            } else if (SERVER_LAYERS.contains(layer)) {
                for (String dep : deps) {
                    if (!(RECORDS.contains(dep) || PLANNER_LAYERS.contains(dep) || SERVER_NPM.contains(dep))) {
                        breaches.add(layer + " reaches " + dep + ": a server layer reaches no engine's API, run layer or door"
                                + " (records, the planner and its npm package alone)");
                    }
                }
            } else if (APP_LAYERS.contains(layer)) {
                for (String dep : deps) {
                    if (NOT_FOR_APPS.contains(dep)) {
                        breaches.add(layer + " reaches " + dep + ": an app reaches the run side through the doors alone");
                    }
                }
            } else if (PLANNER_LAYERS.contains(layer)) {
                for (String dep : deps) {
                    if (!(RECORDS.contains(dep) || PLANNER_LAYERS.contains(dep))) {
                        breaches.add(layer + " reaches " + dep + ": a planner reaches nothing that can execute"
                                + " (records and the protocol alone)");
                    }
                }
            }
        }
        return breaches.stream().sorted().toList();
    }

    /** A label's target name: `//engine-client:run` is `run`. */
    private static String name(String label) {
        return label.substring(label.indexOf(':') + 1);
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
