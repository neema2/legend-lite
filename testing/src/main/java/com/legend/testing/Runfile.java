package com.legend.testing;

import com.google.devtools.build.runfiles.Runfiles;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * A test's declared inputs, found through Bazel's own runfiles library (Bazel workplan P1-03). The
 * BUILD file passes each input by its runfiles path, {@code -Dname=$(rlocationpath <label>)}, and the
 * test resolves it here: the same code under a runfiles tree (macOS, Linux) and under a runfiles
 * manifest (Windows), with no path arithmetic of the test's own.
 */
public final class Runfile {

    private static final Runfiles.Preloaded RUNFILES = preload();

    private Runfile() {}

    /** The file or directory at {@code rlocationpath}; fails when it is not in this test's runfiles. */
    public static Path of(String rlocationpath) {
        if (rlocationpath == null || rlocationpath.isEmpty()) {
            throw new IllegalArgumentException("no runfiles path given: set it from the BUILD file with "
                    + "-D<name>=$(rlocationpath <label>)");
        }
        String found = RUNFILES.unmapped().rlocation(rlocationpath);
        if (found == null || !Files.exists(Path.of(found))) {
            throw new IllegalStateException("not in this test's runfiles: " + rlocationpath
                    + " (declare it in the target's data)");
        }
        return Path.of(found);
    }

    /** The file whose runfiles path the system property {@code name} holds. */
    public static Path property(String name) {
        String rlocationpath = System.getProperty(name);
        if (rlocationpath == null) {
            throw new IllegalStateException("-D" + name + " is not set: the BUILD file passes it as "
                    + "$(rlocationpath <label>)");
        }
        return of(rlocationpath);
    }

    /** What a child process needs to find the same runfiles. */
    public static Map<String, String> env() {
        return RUNFILES.unmapped().getEnvVars();
    }

    private static Runfiles.Preloaded preload() {
        try {
            return Runfiles.preload();
        } catch (IOException e) {
            throw new UncheckedIOException("this test's runfiles cannot be read", e);
        }
    }
}
