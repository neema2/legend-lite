package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Repo;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * THE WAREHOUSE NEEDS NOTHING OF CORE. Its API, JDBC client and server use the null annotations
 * ({@code //base}) and the JSON codec ({@code //json}), both outside core since 2026-09-26. Before
 * that the warehouse reached all 29 core targets for one class, so every compiler change rebuilt
 * and re-tested it (native image included). The closure is Bazel's own answer.
 */
class WarehouseClosureTest {

    @Test
    void theWarehouseReachesNoCoreTarget() throws IOException {
        List<String> reached = Files.readAllLines(Repo.module("warehouse_closure")).stream()
                .filter(l -> !l.isBlank())
                .sorted()
                .toList();
        assertTrue(reached.contains("//base:base") && reached.contains("//json:json"),
                () -> "the warehouse closure query did not see //base and //json — the guard is not looking: " + reached);
        assertEquals(List.of(), reached.stream().filter(t -> t.startsWith("//core")).toList(),
                "the warehouse reaches core — it needs //base and //json only; a core class it wants belongs in a"
                        + " module of its own, below core");
    }
}
