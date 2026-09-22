package com.legend.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Where a test finds the repository's files — one answer under Maven and Bazel.
 *
 * <p>Maven runs a module's tests with the MODULE directory as the working
 * directory, so tests grew three spellings that only work there:
 * {@code Path.of("src/main/java")} for this module, {@code Path.of("..", "spec/…")}
 * for anything else in the repository, and {@code Path.of("target/…")} for output.
 * Bazel runs tests from the runfiles tree, where none of the three resolve. Every
 * such path goes through here instead:
 *
 * <pre>
 *   module("src/main/java")      this module's files        (was Path.of("src/…"))
 *   path("spec/src/test/java")   anything by repository path (was Path.of("..", …))
 *   out("diff")                  somewhere a test may write  (was Path.of("target/…"))
 * </pre>
 *
 * <p>Under Bazel a file must also be declared as {@code data} of the test target,
 * or it is not in the runfiles at all. That is the point rather than a cost: an
 * undeclared read fails instead of working by accident, and the cross-module reads
 * this class serves become edges a reviewer can see in {@code core/BUILD.bazel}.
 *
 * <p>Every path returned is absolute. A guard that filters walked paths by string
 * must therefore compare the path RELATIVE to its walk root ({@link #rel}), or the
 * machine's own directory names leak into the check — a checkout under a directory
 * called {@code plan} would otherwise satisfy {@code contains("/plan/")} for every
 * file.
 */
public final class Repo {

    /** The repository root, and the running test's module under it
     *  ({@code core}, {@code spec}, …) — both derived, never assumed. */
    private static final Path ROOT;
    private static final String MODULE;

    static {
        String srcdir = System.getenv("TEST_SRCDIR");
        if (srcdir != null) {
            // Bazel: the runfiles tree of the main repository, and the module is
            // the package of the running test target (//spec:corpus_duckdb ->
            // spec). Bazel sets all three variables for every test; a missing one
            // means this is not a Bazel test, and guessing would be a fallback
            // (AGENTS.md invariant 4).
            String workspace = required("TEST_WORKSPACE");
            String target = required("TEST_TARGET");
            String label = target.replaceFirst("^@@?", "");
            if (!label.startsWith("//") || label.indexOf(':') < 0) {
                throw new IllegalStateException("unrecognised TEST_TARGET label: " + target);
            }
            ROOT = Path.of(srcdir, workspace).toAbsolutePath().normalize();
            MODULE = label.substring(2, label.indexOf(':'));
        } else {
            // Maven: surefire forks the test JVM in the module directory. Check it
            // rather than assume it — a test run from anywhere else would resolve
            // every path against the wrong tree and fail far from the cause.
            Path cwd = Path.of("").toAbsolutePath().normalize();
            if (cwd.getFileName() == null || !Files.isRegularFile(cwd.resolve("pom.xml"))) {
                throw new IllegalStateException("expected to run in a Maven module directory"
                        + " (one holding a pom.xml) or under Bazel; working directory is " + cwd);
            }
            ROOT = cwd.getParent();
            MODULE = cwd.getFileName().toString();
        }
    }

    private Repo() {}

    private static String required(String variable) {
        String value = System.getenv(variable);
        if (value == null) {
            throw new IllegalStateException(
                    "TEST_SRCDIR is set but " + variable + " is not — not a Bazel test environment");
        }
        return value;
    }

    /** The repository root. */
    public static Path root() {
        return ROOT;
    }

    /** A path inside this module — what tests wrote as {@code Path.of("src/…")}. */
    public static Path module(String first, String... more) {
        return ROOT.resolve(MODULE).resolve(Path.of(first, more));
    }

    /** A path from the repository root — what tests wrote as {@code Path.of("..", …)}. */
    public static Path path(String first, String... more) {
        return ROOT.resolve(Path.of(first, more));
    }

    /**
     * A writable location — what tests wrote as {@code Path.of("target/…")}. Under
     * Bazel, the test's undeclared-outputs directory (collected into
     * {@code bazel-testlogs/…/test.outputs}); under Maven, the module's
     * {@code target}, exactly as before. Parent directories are created.
     */
    public static Path out(String first, String... more) {
        String undeclared = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        Path base = undeclared != null ? Path.of(undeclared) : module("target");
        Path p = base.resolve(Path.of(first, more));
        try {
            Files.createDirectories(p.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return p;
    }

    /** The writable base itself — what tests wrote as {@code Path.of("target")}. */
    public static Path outDir() {
        return out("x").getParent();
    }

    /**
     * {@code p} relative to {@code root}, '/'-separated, with a leading '/' — the
     * form path-string guards compare, independent of where the checkout lives and
     * of the platform separator.
     */
    public static String rel(Path root, Path p) {
        return "/" + root.relativize(p).toString().replace(java.io.File.separatorChar, '/');
    }
}
