package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * NOTHING THAT SHIPS REACHES A TEST-ONLY JAR (Bazel workplan P1-25, layer 3 of tools/deps/pools.bzl): every jar the
 * shipped roots reach — through every explicit dependency, of any rule kind — comes from none of the forbidden pools (legend-engine
 * and legend-pure, the engine runner's stack, the test tooling). The roots are product_closure's scope.
 */
class ProductClosureTest {

    @Test
    void noShippedRootReachesATestOnlyPool() throws IOException {
        List<String> jars = Files.readAllLines(Runfile.property("closure.product_closure")).stream()
                .filter(l -> !l.isBlank()).toList();
        assertTrue(jars.stream().anyMatch(j -> j.contains("maven_core//:")),
                "the product closure holds no @maven_core driver — the query is not looking");
        List<String> forbidden = Arrays.asList(System.getProperty("forbidden.pools").split(","));
        List<String> leaks = jars.stream()
                .filter(j -> forbidden.stream().anyMatch(p -> j.contains("++maven+" + p + "//:") || j.contains("@" + p + "//:")))
                .toList();
        assertEquals(List.of(), leaks, "a shipped root reaches a test-only jar (tools/deps/pools.bzl)");
    }
}
