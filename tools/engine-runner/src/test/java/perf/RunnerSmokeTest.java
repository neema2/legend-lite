// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package perf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The engine runner's binaries, each run as a person runs it (its launcher, from this test's runfiles) on a one-class
 * fixture (Bazel workplan P3-26): until now no test exercised them. token_dump is exercised by //tools/engine-runner
 * :vocab, whose output a diff test holds.
 */
class RunnerSmokeTest {

    private static final Path FIXTURE = Runfile.property("runner.smoke.fixture");

    /** The binary's exit code and everything it printed. */
    private record Run(int exit, String out) {}

    private static Run run(String binaryProperty, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(Runfile.property(binaryProperty).toString()));
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().putAll(Runfile.env());
        Process p = builder.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new Run(p.waitFor(), out);
    }

    @Test
    void parseReadsTheFixtureLikeTheReferenceParser() throws Exception {
        Run r = run("runner.parse", FIXTURE.toString());
        assertEquals(0, r.exit(), r.out());
        assertTrue(r.out().contains("1 files, 0 wrong"), r.out());
    }

    @Test
    void liteParseReadsTheFixture() throws Exception {
        Run r = run("runner.lite_parse", FIXTURE.toString());
        assertEquals(0, r.exit(), r.out());
        assertTrue(r.out().contains("1 files, 0 wrong"), r.out());
    }

    @Test
    void testableParsesAndCompilesTheFixture() throws Exception {
        Run r = run("runner.testable", FIXTURE.toString());
        assertEquals(0, r.exit(), r.out());
        assertTrue(r.out().contains("parse ") && r.out().contains("compile "), r.out());
    }

    @Test
    void anEmptyRunIsRefusedNotGreen() throws Exception {
        // a directory holding no .pure file: zero fixtures is not a green run
        Path empty = java.nio.file.Files.createTempDirectory("no-fixtures");
        Run r = run("runner.parse", empty.toString());
        assertEquals(1, r.exit(), r.out());
        assertTrue(r.out().contains("NO FIXTURES FOUND"), r.out());
    }
}
