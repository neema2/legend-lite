package com.legend.warehouse.server;

import com.google.devtools.build.runfiles.AutoBazelRepository;
import com.google.devtools.build.runfiles.Runfiles;
import com.legend.base.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The server's own runfiles, through rules_java's runfiles library (Bazel workplan P1-16): where it finds DuckDB's
 * library and the postgres extension when started by Bazel ({@code bazel run}, a test's data, {@code bazel-bin}),
 * the native server and the JVM one alike.
 *
 * <p>Started with RUNFILES_MANIFEST_FILE or RUNFILES_DIR (a test passes its own; rules_java's stub sets the
 * manifest for a JVM under {@code bazel run}), those are used as they are. Otherwise the runfiles are found as every
 * runfiles library finds them: JAVA_RUNFILES, then beside the executable ({@code <exe>.runfiles_manifest},
 * {@code <exe>.runfiles/MANIFEST}, {@code <exe>.runfiles/}), then from the working directory {@code bazel run}
 * starts in. A manifest is preferred over a tree: it is there in both runfiles modes, the tree only with runfiles
 * on (not on Windows). None found (a plain install): the server's own defaults do not apply.
 */
@AutoBazelRepository
final class ServerRunfiles {

    private ServerRunfiles() {
    }

    private static boolean searched;
    private static @Nullable Runfiles found;

    /** This repository's canonical name in runfiles paths: {@code _main} when it is the main repository. */
    static String repository() {
        String name = AutoBazelRepository_ServerRunfiles.NAME;
        return name.isEmpty() ? "_main" : name;
    }

    /** The runfiles this process was started with, or null when it has none (a plain install). */
    static synchronized @Nullable Runfiles find() {
        if (!searched) {
            searched = true;
            Map<String, String> env = locate();
            try {
                found = env == null ? null : Runfiles.preload(env).unmapped();
            } catch (IOException e) {
                found = null;
            }
        }
        return found;
    }

    /** A runfile by its runfiles path ({@code $(rlocationpath)}), when it is there. */
    static @Nullable Path rlocation(String path) {
        Runfiles r = find();
        if (r == null || path.isEmpty() || Path.of(path).isAbsolute()) return null;
        String p;
        try {
            p = r.rlocation(path);
        } catch (IllegalArgumentException notARunfilesPath) {
            return null;
        }
        return p != null && Files.exists(Path.of(p)) ? Path.of(p).toAbsolutePath() : null;
    }

    private static @Nullable Map<String, String> locate() {
        Map<String, String> env = new HashMap<>(System.getenv());
        if (!env.getOrDefault("RUNFILES_MANIFEST_FILE", "").isEmpty() || !env.getOrDefault("RUNFILES_DIR", "").isEmpty()) {
            return env;
        }
        // the JVM launcher's own variable: its manifest first, then its tree, as for any runfiles directory
        String javaRunfiles = env.getOrDefault("JAVA_RUNFILES", "");
        if (!javaRunfiles.isEmpty()) {
            Map<String, String> e = from(env, Path.of(javaRunfiles));
            if (e != null) return e;
        }
        List<Path> candidates = new ArrayList<>();
        // the executable as it was started: ProcessHandle's command (on Linux /proc/self/exe, symlinks resolved;
        // argv[0] from /proc/self/cmdline is not)
        ProcessHandle.current().info().command().ifPresent(c -> candidates.add(Path.of(c).toAbsolutePath()));
        try {
            byte[] cmdline = Files.readAllBytes(Path.of("/proc/self/cmdline"));
            int end = 0;
            while (end < cmdline.length && cmdline[end] != 0) end++;
            if (end > 0) candidates.add(Path.of(new String(cmdline, 0, end)).toAbsolutePath());
        } catch (IOException | RuntimeException notLinux) {
            // not Linux
        }
        for (Path exe : candidates) {
            Map<String, String> e = from(env, exe.resolveSibling(exe.getFileName() + ".runfiles"));
            if (e != null) return e;
        }
        // where `bazel run` starts it
        Path cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        for (Path p = cwd; p != null && p.getNameCount() >= cwd.getNameCount() - 1; p = p.getParent()) {
            if (p.getFileName() != null && p.getFileName().toString().endsWith(".runfiles")) return from(env, p);
        }
        return null;
    }

    /** RUNFILES_* for the runfiles at {@code dir} ({@code <exe>.runfiles}): its manifest first, then the tree. */
    private static @Nullable Map<String, String> from(Map<String, String> env, Path dir) {
        Path name = dir.getFileName();
        if (name == null) return null;
        for (Path manifest : new Path[] {dir.resolveSibling(name + "_manifest"), dir.resolve("MANIFEST")}) {
            if (Files.isRegularFile(manifest)) {
                env.put("RUNFILES_MANIFEST_FILE", manifest.toString());
                env.put("RUNFILES_MANIFEST_ONLY", "1");
                return env;
            }
        }
        if (Files.isDirectory(dir)) {
            env.put("RUNFILES_DIR", dir.toString());
            return env;
        }
        return null;
    }
}
