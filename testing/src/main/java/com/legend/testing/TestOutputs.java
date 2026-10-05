package com.legend.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Where a test writes what it reports beside its verdict (Bazel workplan P3-33): Bazel's undeclared-outputs directory
 * ({@code TEST_UNDECLARED_OUTPUTS_DIR}, collected into {@code bazel-testlogs/.../test.outputs}). A JUnit run as a
 * build action (JUnitAction) has none, and names a scratch directory in {@code -Dlegend.outputs.dir} instead: its
 * side reports are not its outputs. Anything else is an error, never a guess. Outputs are not runfiles, so this is not
 * {@link Runfile}'s.
 */
public final class TestOutputs {

    /** The system property a program that runs tests outside {@code bazel test} sets. */
    public static final String PROPERTY = "legend.outputs.dir";

    private TestOutputs() {}

    /** The directory itself. */
    public static Path dir() {
        String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (dir == null || dir.isEmpty()) {
            dir = System.getProperty(PROPERTY);
        }
        if (dir == null || dir.isEmpty()) {
            throw new IllegalStateException("no test outputs directory: TEST_UNDECLARED_OUTPUTS_DIR is not set (run it"
                    + " with `bazel test`) and neither is -D" + PROPERTY);
        }
        return Path.of(dir);
    }

    /** A file under {@link #dir()}, its parent directories created. */
    public static Path file(String first, String... more) {
        Path p = dir().resolve(Path.of(first, more));
        try {
            Files.createDirectories(p.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return p;
    }
}
