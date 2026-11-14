package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * NO ARROW LIBRARY IN THE PRODUCT (the result boundary program's R9, the user, 2026-10-26: "I don't want to compromise
 * on that"; built with track A, 2026-10-29). The Arrow format is written by the repository's own framer
 * ({@code //arrow_ipc}, on java.base) and read by test-only readers (pyarrow, the browser's apache-arrow). The
 * closures are Bazel's own answer (genqueries in this package): the planner's plan side, the WebAssembly planner's
 * Java, the native library's and the warehouse server's -- no external repository whose name carries {@code arrow}
 * in any of them, and the framer reaching nothing outside java.base but {@code //base}.
 */
class ArrowClosureTest {

    private static final List<String> CLOSURES = List.of("plan_side_closure", "wasm_closure", "native_closure",
            "native_image_closure");

    @Test
    void noArrowRepositoryInAnyProductClosure() throws IOException {
        for (String c : CLOSURES) {
            List<String> labels = Files.readAllLines(Runfile.property("closure." + c)).stream()
                    .filter(l -> !l.isBlank()).toList();
            assertTrue(labels.stream().anyMatch(l -> l.startsWith("//")),
                    () -> "the " + c + " query saw no first-party target -- the guard is not looking: " + labels);
            assertEquals(List.of(), labels.stream()
                    .filter(l -> l.startsWith("@") && l.toLowerCase(java.util.Locale.ROOT).contains("arrow")).toList(),
                    "an Arrow repository reached " + c + " (R9): the format is our own framer's, a library is a reader in tests");
        }
    }

    @Test
    void theFramerStandsOnJavaBase() throws IOException {
        List<String> deps = Files.readAllLines(Runfile.property("closure.arrow_ipc_closure")).stream()
                .filter(l -> !l.isBlank()).sorted().toList();
        assertEquals(List.of("//arrow_ipc:arrow_ipc", "//base:base"), deps,
                "the Arrow framer reaches a library beyond //base: it compiles into the WebAssembly planner and the native images");
    }
}
