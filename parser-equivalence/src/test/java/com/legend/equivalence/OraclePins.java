// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** The oracle pins ({@code //tools:oracle-pins.env}, generated from release.MODULE.bazel), read once: THE release
 *  every upstream identity in this repository must agree with (docs/UPSTREAM_BOUNDARY_PROGRAM.md §3 A). The BUILD
 *  file passes it as {@code -Doracle.pins}: its $(rootpath) to a test, its $(execpath) to a build action, each
 *  resolved by {@link Repo#path}. */
public final class OraclePins {

    private OraclePins() {
    }

    private static final Map<String, String> PINS = load();

    private static Map<String, String> load() {
        // //tools:oracle-pins.env, as the BUILD file names it (a test's runfiles path or an action's exec path)
        Path f = com.legend.testing.ProgramPaths.file("oracle.pins");
        Map<String, String> out = new LinkedHashMap<>();
        try {
            for (String line : Files.readAllLines(f)) {
                String s = line.strip();
                int eq = s.indexOf('=');
                if (s.isEmpty() || s.startsWith("#") || eq < 0) {
                    continue;
                }
                out.put(s.substring(0, eq), s.substring(eq + 1));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!out.containsKey("LEGEND_ENGINE_RELEASE")) {
            throw new IllegalStateException(f + " has no LEGEND_ENGINE_RELEASE");
        }
        return out;
    }

    /** The pinned legend-engine release, e.g. {@code 4.138.2}. */
    public static String engineRelease() {
        return PINS.get("LEGEND_ENGINE_RELEASE");
    }
}
