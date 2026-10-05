// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * A source tree's files in a FIXED order (Bazel workplan P3-08): by the '/'-separated path relative to the root, the
 * same on every filesystem and platform. {@code Files.walk} returns the filesystem's order, so a loader that reacts
 * to the files it meets (dropping a file that fails, keeping a first occurrence) gave a result that depended on it.
 */
public final class SourceWalk {

    private SourceWalk() {}

    /** Every regular file under {@code root} whose name ends with {@code suffix}, in relative-path order. */
    public static List<Path> inOrder(Path root, String suffix) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(p -> p.toString().endsWith(suffix) && Files.isRegularFile(p))
                    .sorted(Comparator.comparing(p -> relative(root, p)))
                    .toList();
        }
    }

    /** {@code p} relative to {@code root}, '/'-separated. */
    public static String relative(Path root, Path p) {
        return root.relativize(p).toString().replace(java.io.File.separatorChar, '/');
    }
}
