// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.testing.Repo;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** {@code tools/oracle-pins.env}, read once: THE release every upstream identity in
 *  this repository must agree with (docs/UPSTREAM_BOUNDARY_PROGRAM.md §3 A). The
 *  test JVM runs in the module directory, so the file is one level up; the
 *  repository-root fallback covers an IDE launched from the root. */
public final class OraclePins {

    private OraclePins() {
    }

    private static final Map<String, String> PINS = load();

    private static Map<String, String> load() {
        Path f = Repo.path("tools", "oracle-pins.env");
        // one answer: the repository path (the cwd-relative second guess this had
        // only existed while tests ran from the module directory)
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
            throw new IllegalStateException("tools/oracle-pins.env has no LEGEND_ENGINE_RELEASE: " + f);
        }
        return out;
    }

    /** The pinned legend-engine release, e.g. {@code 4.138.2}. */
    public static String engineRelease() {
        return PINS.get("LEGEND_ENGINE_RELEASE");
    }
}
