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
 * THE PRODUCT'S TESTS REACH NO EDGE (Phase 7's build half, F's separation census lines A5 and A6, 2026-11-17). The
 * product test library ({@code //core:core_tests_lib}) and its test cases ({@code //core:test_cases}) compile and run
 * without the compatibility edge: Bazel's own answer (the genquery {@code product_tests_closure}) holds none of the
 * edge's targets, nor the hosts' boundary, which answers legend-engine's routes through the edge. A test that reaches
 * the edge is the edge's test: it is listed in {@code core/edge_tests.bzl} and compiled in
 * {@code //core:core_edge_tests_lib}.
 */
class EdgeSeparationClosureTest {

    /** The targets that ARE the edge, or reach it by design (the boundary answers the edge's routes). */
    private static final Set<String> EDGE = Set.of("//core:edge", "//core:edge_server", "//core:edge_service_tests",
            "//core:core_edge_tests_lib", "//wasm:boundary");

    @Test
    void productTestsReachNoEdge() throws IOException {
        List<String> labels = Files.readAllLines(Runfile.property("closure.product_tests_closure")).stream()
                .map(String::strip).filter(l -> !l.isEmpty()).toList();
        assertTrue(labels.contains("//core:core_tests_lib") && labels.contains("//core:core"),
                () -> "the product tests' closure does not hold the product test library and core -- the guard is not looking: "
                        + labels.size() + " labels");
        assertEquals(List.of(), labels.stream().filter(EDGE::contains).toList(),
                "the product's tests reach the edge (A5): a test that needs an edge class is the edge's test -- list it in"
                        + " core/edge_tests.bzl so it compiles in //core:core_edge_tests_lib");
    }
}
