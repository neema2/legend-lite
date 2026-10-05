package com.legend.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The files a program is GIVEN, each by a system property that says its form (Bazel workplan P3-33): a build action's
 * {@code -D<name>=$(execpath ...)}, a path from the action's working directory, or a test's
 * {@code -D<name>.rlocation=$(rlocationpath ...)}, resolved through Bazel's runfiles library ({@link Runfile}). The
 * BUILD file names every file; nothing is found from a repository root, and a missing property is an error naming it.
 * The same code reads both, so a class that runs as a test and in an action needs no mode of its own.
 */
public final class ProgramPaths {

    private ProgramPaths() {}

    /** The file {@code name} names. */
    public static Path file(String name) {
        String path = System.getProperty(name);
        if (path != null && !path.isEmpty()) {
            return Path.of(path);
        }
        String rlocation = System.getProperty(name + ".rlocation");
        if (rlocation != null && !rlocation.isEmpty()) {
            return Runfile.of(rlocation);
        }
        throw new IllegalStateException("neither -D" + name + " (a build action's $(execpath)) nor -D" + name
                + ".rlocation (a test's $(rlocationpath)) is set: the BUILD file names this file");
    }

    /** The root of a tree, named by a file that sits at it ({@code @legend_engine_src//:pom.xml}). */
    public static Path rootOf(String name) {
        return file(name).toAbsolutePath().normalize().getParent();
    }

    /** The files a declared list names (java_jars): each line in the list's own form, an action's exec path or a
     *  test's runfiles path ({@code rlocation_paths = True}). */
    public static List<Path> listed(String name) {
        boolean rlocation = System.getProperty(name) == null;
        Path list = file(name);
        try {
            return Files.readAllLines(list, StandardCharsets.UTF_8).stream()
                    .filter(line -> !line.isBlank())
                    .map(line -> rlocation ? Runfile.of(line) : Path.of(line))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the declared list " + list, e);
        }
    }
}
