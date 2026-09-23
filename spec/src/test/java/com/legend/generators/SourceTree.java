// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A source tree a generator reads as TEXT: a root directory, with some files
 * replaced by other generators' outputs. When generators chain (natives rewrites
 * Pure.java, and the prelude generator then reads core's sources), each one is
 * handed exactly the tree the chain says it sees — the replacement is declared,
 * never a file some earlier step happened to leave behind.
 *
 * @param root      the directory, e.g. core/src/main/java
 * @param overrides root-relative path ('/'-separated) → the file to read instead
 */
public record SourceTree(Path root, Map<String, Path> overrides) {

    public static SourceTree of(Path root) {
        return new SourceTree(root, Map.of());
    }

    /** The text of {@code relative} — its override if it has one. */
    public String read(String relative) throws IOException {
        Path p = overrides.getOrDefault(relative, root.resolve(relative));
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    /** Every file under the root, root-relative and '/'-separated, sorted. */
    public List<String> files() throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(Files::isRegularFile)
                    .map(p -> root.relativize(p).toString().replace(java.io.File.separatorChar, '/'))
                    .sorted()
                    .toList();
        }
    }
}
