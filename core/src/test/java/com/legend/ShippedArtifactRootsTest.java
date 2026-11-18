// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * K2, CLOSED FOR GOOD BY A MECHANICAL CHECK (the user's ruling of 2026-11-18, ~06:00, cited by the orchestrator:
 * "Product root, edge opt-in" -- each shipped artifact starts at lite's own entry, legend-engine's module loads only when
 * configured, one artifact per surface; the orchestrator's order for the check, 2026-11-18). Two answers of the build
 * graph (core/BUILD.bazel's {@code shipped_edge_reach} and {@code shipped_edge_roots} genqueries), over what ships --
 * the product build CI runs ({@code //:java //:web //:wasm //:native //:sites}, {@code //native:compiler}), the apps
 * ({@code //datacube:app}) and the Python package ({@code //python:wheel}):
 * <ul>
 *   <li>REACH: a target in a shipped artifact's closure that depends directly on an edge target which is no declared
 *   opt-in module ({@code _EDGE_OPT_IN}: the server's {@code //core:edge_server}, and the tab's engine module's entry
 *   {@code //wasm:edge_tab} (2026-11-18, K2's wasm half); the Python host's {@code //native:engine} joins it);</li>
 *   <li>ROOTS: a target in a shipped artifact's closure whose main class is the edge's -- but a declared opt-in module,
 *   one whose direct dependency is a declared opt-in library, as {@code //wasm:engine_planner} is (2026-11-18, the
 *   orchestrator's ruling: "ROOTS exempts declared opt-in modules").</li>
 * </ul>
 * Each artifact still the edge's is pinned with its owner ({@link #REACH_OPEN}, {@link #ROOTS_OPEN}), SHRINK-ONLY: one
 * re-rooted is deleted here, a new one fails. K2 stays an open audit item exactly while one is pinned. A planted
 * edge-rooted artifact ({@code //core:planted_edge_rooted_artifact}) proves both answers bite.
 */
@Tag("guardrail")
class ShippedArtifactRootsTest {

    /** The shipped targets reaching the edge outside an opt-in module, each with its K2 owner. Measured 2026-11-18. */
    static final Map<String, String> REACH_OPEN = Map.of(
            "//native:compiler", "K2 HOST (Python, via DataCube) (2026-11-18): the native library rooted at"
                    + " :edge_entry; the product library at :entry alone, //native:engine the opt-in (the next host train)"
            // (//query-store:page closed 2026-11-18, the host train's merge of train 18: product-rooted at :page_entry
            // by H6, 2026-11-17, its closure edge-free -- //tools/deps:query_store_page_closure_test)
            // (//wasm:planner closed 2026-11-18, K2's wasm half for train 20: rooted at :tab, lite's planner.TabExports,
            // its closure edge-free -- //tools/deps:wasm_planner_closure_test; legend-engine's routes the engine module's,
            // //wasm:engine_planner, shipped beside it as the declared opt-in, :edge_tab in _EDGE_OPT_IN)
            );

    /** The shipped targets whose main class is the edge's, each with its K2 owner. Measured 2026-11-18. */
    static final Map<String, String> ROOTS_OPEN = Map.of(
            // (//core:server re-rooted 2026-11-18 by E-P5's K2 server half, 0c78ae0e00 and a0a14bd7c1: main_class
            // com.legend.server.LegendHttpServer, :edge_server the declared opt-in through MountProvider -- train 17)
            // (//query-store:page closed 2026-11-18, the host train's merge of train 18: main_class the product's
            // com.legend.queries.page.QueriesPage since H6, 2026-11-17)
            // (//wasm:planner closed 2026-11-18, K2's wasm half for train 20: main_class lite's planner.TabExports; the
            // engine module //wasm:engine_planner, main_class com.legend.edge.wasm.EngineExports, is a declared opt-in
            // module -- its direct dependency :edge_tab in _EDGE_OPT_IN -- which this answer exempts)
            );

    @Test
    void eachShippedArtifactStartsAtTheProduct() {
        assertEquals(new TreeSet<>(REACH_OPEN.keySet()), EdgeSeparationTest.buildFound("legend.shipped_edge_reach"),
                "a shipped artifact depends on an edge target outside a declared opt-in module (a new one is a"
                        + " regression; one re-rooted closes: delete its entry)");
        assertEquals(new TreeSet<>(ROOTS_OPEN.keySet()), EdgeSeparationTest.buildFound("legend.shipped_edge_roots"),
                "a shipped artifact's main class is the edge's (a new one is a regression; one re-rooted closes:"
                        + " delete its entry)");
    }

    @Test
    void k2IsOpenExactlyWhileARootIsTheEdges() {
        boolean open = EdgeAuditOpenTest.AUDIT_OPEN.stream().anyMatch(i -> i.id().equals("K2"));
        assertEquals(!REACH_OPEN.isEmpty() || !ROOTS_OPEN.isEmpty(), open, "K2 is an open audit item exactly while a"
                + " shipped artifact is the edge's");
    }

    @Test
    void theRootCheckBites() {
        Set<String> reach = EdgeSeparationTest.buildFound("legend.shipped_edge_reach_planted");
        Set<String> roots = EdgeSeparationTest.buildFound("legend.shipped_edge_roots_planted");
        assertTrue(reach.contains("//core:planted_edge_rooted_artifact"), "the planted artifact's edge reach: " + reach);
        assertTrue(roots.contains("//core:planted_edge_rooted_artifact"), "the planted artifact's edge root: " + roots);
        // the opt-in exemption is for declared modules alone (2026-11-18, the GUARD stream's narrowing): a product
        // artifact carrying a declared opt-in library, as //core:server carries :edge_server, is still checked
        assertTrue(roots.contains("//core:planted_opt_in_carrier"), "an opt-in carrier's edge root: " + roots);
        assertFalse(reach.contains("//core:planted_opt_in_carrier"), "an opt-in library is no edge reach: " + reach);
    }
}
