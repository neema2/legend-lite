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

    private Runfile() {}

    /** The FILE at {@code rlocationpath} (a directory resolves only under a runfiles tree, never by the Windows
     *  manifest: pass a file in it); fails when it is not in this test's runfiles. */
    public static Path of(String rlocationpath) {
        if (rlocationpath == null || rlocationpath.isEmpty()) {
            throw new IllegalArgumentException("no runfiles path given: set it from the BUILD file with "
                    + "-D<name>=$(rlocationpath <label>)");
        }
        String found = Holder.RUNFILES.unmapped().rlocation(rlocationpath);
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

    /** The files whose runfiles paths the environment variable {@code name} holds, space-separated: the BUILD file
     *  passes {@code env = {name: "$(rlocationpaths <label>)"}} (an environment variable stays one value; a JVM flag
     *  would be split). */
    public static java.util.List<Path> envList(String name) {
        String paths = System.getenv(name);
        if (paths == null || paths.isBlank()) {
            throw new IllegalStateException("$" + name + " is not set: the BUILD file passes it as "
                    + "env = {\"" + name + "\": \"$(rlocationpaths <label>)\"}");
        }
        return java.util.Arrays.stream(paths.split(" ")).filter(p -> !p.isEmpty()).map(Runfile::of).toList();
    }

    /** What a child process needs to find the same runfiles. */
    public static Map<String, String> env() {
        return Holder.RUNFILES.unmapped().getEnvVars();
    }

    /** Loaded on first use; a JVM without runfiles fails that use with the reason (and every later one
     *  with NoClassDefFoundError naming this holder). */
    private static final class Holder {
        static final Runfiles.Preloaded RUNFILES = preload();

        private static Runfiles.Preloaded preload() {
            try {
                return Runfiles.preload();
            } catch (IOException e) {
                throw new UncheckedIOException("this test's runfiles cannot be read", e);
            }
        }
    }
}
