package com.legend.tools.junit.fixtures;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** RunnerTest's Jupiter fixture: run only through JUnitMain.run, never selected on its own. */
public class ProbeTest {
    @Test void alpha() { }
    @Test void beta() { }
    @Test void gamma() { }
    @Test void delta() { }
    @Test void epsilon() { }

    /** One unit to the filter and the shard split, three invocations in test.xml. */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void invocations(int i) { }

    /** Bazel's premature-exit file exists while the tests run (RunnerTest names it in a property). */
    @Test void exitFileExistsWhileTestsRun() {
        String file = System.getProperty("runner_test.exit_file");
        if (file != null) {
            assertTrue(Files.exists(Path.of(file)), "TEST_PREMATURE_EXIT_FILE is absent during the run");
        }
    }
}
