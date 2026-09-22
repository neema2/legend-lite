// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PLATFORM-SURFACE QUARANTINE (Dialect levels, user directive
 * 2026-08-12): {@code ElementParser.parsePlatform} — legend-lite's own
 * legend-pure dialect superset — may be referenced ONLY by the
 * whitelisted platform/bootstrap call sites below. User-facing code
 * routes through {@code parseLegendLite} (product surface) or
 * {@code parseStrict}/{@code PmcdParser} (exact-engine surface).
 *
 * <p>The whitelist FREEZES the callers as found at the quarantine's
 * introduction; it may only SHRINK as entries migrate to the product
 * surface. Adding a file here is a reviewed decision that a new
 * component is platform machinery.
 */
class PlatformSurfaceGuardrailTest {

    private static final Set<String> WHITELIST = Set.of(
            // platform/bootstrap loading and internal pipeline
            // (ElementParser left the whitelist 2026-08-12: the named
            // parseLegendPlatform entries are GONE — ONE parse(src, Dialect)
            // entry remains and it names nothing)
            "com/legend/parser/Dialect.java",
            // (the parser-internal DEFAULTS left the whitelist 2026-08-12
            // — HONEST_DEBT #9 executed: every cursor and sub-parser now
            // takes its dialect explicitly, TokenStreamCursor.dialect()
            // is ABSTRACT, and a silent platform-level parse is
            // unrepresentable)
            // (EngineTestExecutor left the whitelist 2026-08-12: its ONLY platform
            // call was the compileLegendGrammar payload seam, now
            // LEGEND_ENGINE — matching the engine's own architecture)
            // (servers AND the Compiler migrated to parseLegendLite
            // 2026-08-12 — the platform surface is bootstrap-only now)
            "com/legend/builtin/Pure.java",
            // the prelude MODULE (SYSTEM_PRELUDE_DESIGN §10, 2026-09-08):
            // generated spec declarations parsed once at class load in the
            // platform dialect — the same bootstrap-loader regime as Pure
            "com/legend/builtin/Prelude.java");

    @Test
    void platformSurfaceCallersAreWhitelisted() throws IOException {
        Path root = Repo.module("src/main/java");
        List<String> offenders;
        try (Stream<Path> files = Files.walk(root)) {
            offenders = files
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> {
                        try {
                            String t = Files.readString(p);
                            return t.contains("parseLegendPlatform(")
                                    || t.contains("Dialect.LEGEND" + "_PLATFORM");
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    })
                    // '/' ALWAYS: the whitelist holds forward-slash paths, so
                    // on Windows (com\legend\...) NOTHING matches and every
                    // legitimate caller reads as a new offender (Windows CI,
                    // 2026-09-09).
                    .map(p -> root.relativize(p).toString().replace(java.io.File.separatorChar, '/'))
                    .filter(p -> !WHITELIST.contains(p))
                    .toList();
        }
        assertTrue(offenders.isEmpty(),
                () -> "NEW callers of the PLATFORM dialect surface — user"
                        + " code must route through parseLegendLite or"
                        + " parseStrict (Dialect levels):\n  "
                        + String.join("\n  ", offenders));
    }
}
