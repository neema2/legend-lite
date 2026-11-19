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
 * THE QUERY-STORE PAGE IS A PRODUCT ROOT (the user's K2 ruling, ~06:00 2026-11-18: "Product root, edge opt-in"; the
 * edge audit's H1, its page part closed by H6 at size 0). The page's WebAssembly module ({@code //query-store:page})
 * answers lite's own {@code /api/lite/v1/queries} in lite's words; legend-engine's {@code /api/pure/v1/query} is the
 * server edge's alone, and an app configured for legend-engine talks to a real legend-engine server, so there is no
 * in-page legend-engine store to opt into. Bazel's own answer (the genquery {@code :query_store_page_closure}) holds
 * none of the edge's libraries and no class of {@code com.legend.edge}: the page module's class closure holds no edge.
 */
class QueryStorePageClosureTest {

    /** The targets that ARE the edge, or answer legend-engine's routes through it. */
    private static final Set<String> EDGE = Set.of("//core:edge", "//core:edge_server", "//core:edge_store_words",
            "//core:edge_service_tests", "//native:edge_entry", "//engine-client:edge", "//datacube:edge_entry",
            "//query-store:edge_page_entry", "//wasm:boundary");

    @Test
    void thePageModuleHoldsNoEdge() throws IOException {
        List<String> labels = Files.readAllLines(Runfile.property("closure.query_store_page")).stream()
                .map(String::strip).filter(l -> !l.isEmpty()).toList();
        assertTrue(labels.contains("//query-store:page_entry") && labels.contains("//core:saved_queries"),
                () -> "the page's closure does not hold its entry and the saved-query rules -- the guard is not looking: "
                        + labels.size() + " labels");
        assertEquals(List.of(), labels.stream().filter(EDGE::contains).toList(),
                "the query-store page reaches the edge: the page is a product root (K2), legend-engine's store the server"
                        + " edge's");
        assertEquals(List.of(), labels.stream().filter(l -> l.contains("com/legend/edge/")).toList(),
                "the query-store page's build compiles a class of com.legend.edge (K2: product root, edge opt-in)");
        // the page carries core's saved-query rules and lite's own wire, and no other core library -- no planner, no
        // compiler (2026-11-19, H4's P4 and the orchestrator's option (a), its approval of ~13:05 UTC: the page admits
        // //core:lite_wire and nothing else)
        assertEquals(List.of(), labels.stream().filter(l -> l.matches("//core:[a-z_]+"))
                        .filter(l -> !PAGE_CORE.contains(l)).toList(),
                "the query-store page reaches a core library beyond the saved queries and lite's wire");
    }

    /** The core libraries the page carries: the saved-query rules, and lite's own wire (its user and routes). */
    private static final Set<String> PAGE_CORE = Set.of("//core:saved_queries", "//core:lite_wire");
}
