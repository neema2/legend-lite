// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.deps;

import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE LAYERS OF CORE (execution plan rule 0.12). Every product library in {@code //core} depends on exactly the
 * targets {@code core-layers.txt} lists for it: Bazel's own answer (a genquery of each target's direct {@code deps},
 * in this package) is compared with the committed file, line by line. Java's strict dependencies already stop code
 * from importing a target it does not list; this test stops the LIST from growing unnoticed, so a stage cannot quietly
 * start depending on a later stage (the store resolver on lowering, say) or on the whole compiler.
 *
 * <p>Each rewrite wave ends with its stage carved out as its own target and its line moved toward the plan's target
 * map; a removed edge is recorded by deleting it here, a new one only with the plan item that allows it.
 */
class CoreLayeringTest {

    @Test
    void everyCoreLibraryDependsOnExactlyItsListedLayers() throws IOException {
        Map<String, String> expected = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Runfile.property("core.layers"))) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            expected.put(parts[0], parts.length > 1 ? normalise(List.of(parts[1].split(" "))) : "");
        }
        assertTrue(expected.size() >= 20, "core-layers.txt lists " + expected.size() + " targets — the guard is not looking");
        // each layer_<target> genquery output, by its runfiles path
        Map<String, Path> layerFiles = new LinkedHashMap<>();
        for (String rlocationpath : System.getProperty("core.layer.files").split(",")) {
            layerFiles.put(rlocationpath.substring(rlocationpath.lastIndexOf("/layer_") + "/layer_".length()),
                    Runfile.of(rlocationpath));
        }
        List<String> drift = new ArrayList<>();
        for (Map.Entry<String, String> e : expected.entrySet()) {
            String target = e.getKey();
            Path layer = layerFiles.get(target);
            if (layer == null) {
                throw new IllegalStateException("tools/deps/BUILD.bazel passes no layer_" + target);
            }
            String actual = normalise(Files.readAllLines(layer));
            if (!actual.equals(e.getValue())) {
                TreeSet<String> added = new TreeSet<>(List.of(actual.split(" ")));
                added.removeAll(List.of(e.getValue().split(" ")));
                TreeSet<String> removed = new TreeSet<>(List.of(e.getValue().split(" ")));
                removed.removeAll(List.of(actual.split(" ")));
                added.remove("");
                removed.remove("");
                drift.add("//core:" + target
                        + (added.isEmpty() ? "" : " NOW DEPENDS ON " + added
                                + " (allowed only if the plan's target map allows it; add it to core-layers.txt with the plan item)")
                        + (removed.isEmpty() ? "" : " NO LONGER DEPENDS ON " + removed
                                + " (delete it from core-layers.txt in the same push)"));
            }
        }
        assertEquals(List.of(), drift, "the layers of //core changed");
    }

    /** Bazel labels to the file's short names, sorted, one space apart. */
    private static String normalise(List<String> labels) {
        TreeSet<String> out = new TreeSet<>();
        for (String l : labels) {
            String s = l.strip();
            if (s.isEmpty()) {
                continue;
            }
            s = s.replaceFirst("^//core:", "")
                    .replaceFirst("^//base(:base)?$", "base")
                    .replaceFirst("^//json(:json)?$", "json");
            out.add(s);
        }
        return String.join(" ", out);
    }
}
