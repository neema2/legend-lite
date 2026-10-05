// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.testing.Repo;
import com.legend.testing.SourceFiles;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * This repository's files under a directory, the way the running program may read them (Bazel workplan P3-27b). A
 * TEST reads its target's declared list (SourceFiles: -Dlegend.sources), never a walk of a directory, so it runs the
 * same under a runfiles tree and a manifest alone. A generator ACTION (no list) walks its own declared inputs, laid out
 * at their repository paths (Repo), until Bazel workplan P3-33 gives the generators explicit arguments.
 */
final class ModuleFiles {

    private ModuleFiles() {}

    private static boolean declared() {
        return System.getProperty(SourceFiles.PROPERTY) != null;
    }

    /** Every file under the repository directory {@code dir}, ordered by its path ('/'-separated). */
    static List<Path> under(String dir) {
        if (declared()) {
            return SourceFiles.under(dir);
        }
        Path root = Repo.path(dir);
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException(dir + " is not among this action's inputs: declare it");
        }
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(p -> Corpus.within(root, p)))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot walk " + root, e);
        }
    }

    /** The files directly in the repository directory {@code dir}. */
    static List<Path> in(String dir) {
        List<Path> out = new ArrayList<>();
        for (Path p : under(dir)) {
            if (rel(dir, p).indexOf('/', 1) < 0) {
                out.add(p);
            }
        }
        return out;
    }

    /** {@code file}'s path from {@code dir}, '/'-separated, with a leading '/'. */
    static String rel(String dir, Path file) {
        return declared() ? SourceFiles.rel(dir, file) : Corpus.within(Repo.path(dir), file);
    }
}
