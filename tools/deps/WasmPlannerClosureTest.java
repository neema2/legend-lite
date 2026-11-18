package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * THE TAB'S PLANNER MODULE IS A PRODUCT ROOT (the user's K2 ruling, ~06:00 2026-11-18: "Product root, edge opt-in"; the
 * orchestrator's option A, the same day). The WebAssembly planner ({@code //wasm:planner}) starts at lite's own entry,
 * {@code planner.TabExports}; legend-engine's routes in the tab are the engine module's ({@code //wasm:engine_planner},
 * its entry {@code com.legend.edge.wasm.EngineExports}), loaded beside it only where an app is configured
 * {@code engine: 'legend-engine'}. Bazel's own answer (the genquery {@code :wasm_planner_closure}) holds the module's
 * entry and its boundary (the guard looking), none of the edge's libraries and no class of {@code com.legend.edge}:
 * the module's class closure holds no edge. Its run-time twin is //wasm:product_module_test (the module runs, and
 * exports no legend-engine route).
 */
class WasmPlannerClosureTest {

    /** The targets that ARE the edge, or answer legend-engine's routes through it. */
    private static final Set<String> EDGE = Set.of("//core:edge", "//core:edge_server", "//core:edge_store_words",
            "//core:edge_service_tests", "//native:edge_entry", "//engine-client:edge", "//datacube:edge_entry",
            "//query-store:edge_page_entry", "//wasm:edge_boundary", "//wasm:edge_tab", "//wasm:engine_planner");

    @Test
    void thePlannerModuleHoldsNoEdge() throws IOException {
        List<String> labels = Files.readAllLines(Runfile.property("closure.wasm_planner")).stream()
                .map(String::strip).filter(l -> !l.isEmpty()).toList();
        assertTrue(labels.contains("//wasm:tab") && labels.contains("//wasm:boundary")
                        && labels.contains("//wasm:src/main/java/planner/TabExports.java"),
                () -> "the module's closure does not hold its entry and its boundary -- the guard is not looking: "
                        + labels.size() + " labels");
        assertEquals(List.of(), labels.stream().filter(EDGE::contains).toList(),
                "the tab's planner module reaches the edge: the module is a product root (K2), legend-engine's routes"
                        + " the engine module's");
        assertEquals(List.of(), labels.stream().filter(l -> l.contains("com/legend/edge/")).toList(),
                "the tab's planner module's build compiles a class of com.legend.edge (K2: product root, edge opt-in)");
    }
}
