package com.legend.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Where a test finds the repository's files. Bazel runs a test in its runfiles
 * tree, not in a module directory, so no test spells a path relative to its
 * working directory; every such path goes through here. Two callers exist: a
 * Bazel TEST (the runfiles of the main repository; the module is the test
 * target's package), and a BUILD ACTION — a generator program run over its
 * declared inputs — which names the tree and module it reads with
 * {@code -Dlegend.repo.root} / {@code -Dlegend.repo.module}. (Under the old Maven
 * build a test ran in its module directory; the three spellings that relied on
 * that are the "was" column below.)
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
        if (System.getProperty("legend.repo.root") != null) {
            // a BUILD ACTION (a generator program), FIRST: an explicit instruction
            // beats the environment — Bazel's java launcher exports TEST_SRCDIR
            // even outside a test (measured 2026-09-22). The action names the tree it
            // reads — its declared inputs, laid out at their repository paths —
            // and the module it runs as. Both are required; neither is guessed.
            ROOT = Path.of(System.getProperty("legend.repo.root")).toAbsolutePath().normalize();
            String module = System.getProperty("legend.repo.module");
            if (module == null || module.isEmpty()) {
                throw new IllegalStateException("-Dlegend.repo.root is set but -Dlegend.repo.module is not");
            }
            MODULE = module;
        } else if (srcdir != null) {
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
            throw new IllegalStateException("Repo needs a Bazel test (TEST_SRCDIR) or a build action"
                    + " naming its tree (-Dlegend.repo.root / -Dlegend.repo.module) — run it with"
                    + " `bazel test` or `bazel run`");
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

    /** The files a declared list names ({@code //tools/jars:defs.bzl} java_jars):
     *  the list's path is {@code -D<property>} (the BUILD file's $(rootpath) for a
     *  test, $(execpath) for a build action), and every line is a path resolved the
     *  same way. The build says which files; nothing is discovered. */
    public static java.util.List<Path> listed(String property) {
        String list = System.getProperty(property);
        if (list == null || list.isEmpty()) {
            throw new IllegalStateException("-D" + property + " is not set — the BUILD file"
                    + " passes the declared list (java_jars) to this program");
        }
        try {
            return Files.readAllLines(path(list)).stream()
                    .filter(line -> !line.isBlank()).map(Repo::path).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the declared list " + list, e);
        }
    }

    /**
     * A writable location — what tests wrote as {@code Path.of("target/…")}. Under
     * Bazel, the test's undeclared-outputs directory (collected into
     * {@code bazel-testlogs/…/test.outputs}); in a build action, a temporary
     * directory. Parent directories are created.
     */
    public static Path out(String first, String... more) {
        String undeclared = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        Path base = System.getProperty("legend.repo.root") != null ? actionScratch()
                : Path.of(Objects.requireNonNull(undeclared,
                        "a Bazel test always has TEST_UNDECLARED_OUTPUTS_DIR"));
        Path p = base.resolve(Path.of(first, more));
        try {
            Files.createDirectories(p.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return p;
    }

    private static Path scratch;

    /** A build action's side reports (a generator's diagnostics) go to a
     *  temporary directory: the action's one declared output is the file it
     *  generates, and its inputs are read-only. */
    private static synchronized Path actionScratch() {
        if (scratch == null) {
            try {
                scratch = Files.createTempDirectory("legend-action-");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return scratch;
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
