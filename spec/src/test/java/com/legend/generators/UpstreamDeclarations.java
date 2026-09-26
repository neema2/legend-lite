// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.model.Function;
import com.legend.model.PackageableElement;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.model.FunctionId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * EVERY FUNCTION DECLARATION in the pinned upstream trees, read with our own
 * parser: each with the file that declares it, and whether that file belongs to
 * the standard library (the nine platform roots and the five core_functions_*
 * repositories — upstream's own "core", by exact file membership). A file the
 * parser cannot read is named, never skipped silently.
 */
final class UpstreamDeclarations {

    /** One upstream declaration and where it is. */
    record Declared(Function function, FunctionId id, String file, boolean stdlib) {
    }

    final List<Declared> all = new ArrayList<>();
    final List<String> unreadable = new ArrayList<>();
    int files;

    private UpstreamDeclarations() {
    }

    static UpstreamDeclarations load() throws IOException {
        Path pure = com.legend.testing.Upstream.pure();
        Path engine = com.legend.testing.Upstream.engine();
        // the pinned trees are declared inputs: absence is an error, never a skip
        org.junit.jupiter.api.Assertions.assertTrue(Files.isDirectory(pure) && Files.isDirectory(engine),
                "pinned upstream trees not present: " + pure + ", " + engine);
        Set<Path> stdlibFiles = new LinkedHashSet<>();
        for (String r : UpstreamFiles.PLATFORM_ROOTS) {
            stdlibFiles.addAll(pureFiles(pure.resolve(r)));
        }
        for (String r : UpstreamFiles.STDLIB_ENGINE_ROOTS) {
            stdlibFiles.addAll(pureFiles(engine.resolve(r)));
        }
        UpstreamDeclarations out = new UpstreamDeclarations();
        for (Path root : List.of(pure, engine)) {
            for (Path f : pureFiles(root)) {
                out.files++;
                List<PackageableElement> elements;
                try {
                    elements = ElementParser.parse(Files.readString(f, StandardCharsets.UTF_8),
                            Dialect.LEGEND_PLATFORM).elements();
                } catch (RuntimeException e) {
                    out.unreadable.add(root.relativize(f) + "\t" + SpecBodyCensusTest.first(e.getMessage()));
                    continue;
                }
                for (PackageableElement el : elements) {
                    if (el instanceof Function fn) {
                        out.all.add(new Declared(fn, FunctionId.of(fn),
                                root.getFileName() + "/" + root.relativize(f), stdlibFiles.contains(f)));
                    }
                }
            }
        }
        return out;
    }

    /** The declarations by FQN, in reading order. */
    Map<String, List<Declared>> byFqn() {
        Map<String, List<Declared>> m = new LinkedHashMap<>();
        for (Declared d : all) {
            m.computeIfAbsent(d.function().qualifiedName(), k -> new ArrayList<>()).add(d);
        }
        return m;
    }

    private static List<Path> pureFiles(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList();
        }
    }
}
