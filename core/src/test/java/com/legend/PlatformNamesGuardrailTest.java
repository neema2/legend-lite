// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two ratchets of docs/EXECUTION_CONTEXT_DESIGN_2026_09_06.md (user
 * ruling 2026-09-06: "are we building a general purpose pure runner or a
 * super hard-coded test runner?").
 *
 * <ol>
 *   <li>A Pure function or class NAME is spelled in ONE place — the catalog
 *       ({@link com.legend.compiler.element.type.PlatformTypes}). Every
 *       other file asks the catalog. The literal {@code equals("meta::…")}
 *       checks outside it are counted and may only SHRINK.</li>
 *   <li>The execution context is a VALUE read once: no file outside
 *       {@link com.legend.compiler.spec.typed.ExecutionContext} walks a
 *       runtime expression for its shape. The retired walkers are named
 *       so they cannot return under their old spelling.</li>
 * </ol>
 */
class PlatformNamesGuardrailTest {

    private static final Path MAIN = Repo.module("src/main/java/com/legend");
    private static final Pattern LITERAL_NAME_CHECK =
            Pattern.compile("equals\\(\"meta::");
    /** The retired runtime-shape walkers: their names may not reappear as
     * methods anywhere outside the one reader. */
    private static final List<String> RETIRED_WALKERS = List.of(
            "chainMappingsIn(", "jsonSourcesIn(", "sqlSetupsIn(", "setupsIn(",
            "connectionNameIn(", "quoteIdentifiersOf(", "timeZoneOf(",
            "connectionInstanceOf(", "databaseTypeOf(", "connectionStoreElementOf(",
            "runRuntimeSetups(");

    @Test
    void pureNamesAreSpelledInTheCatalogOnly() throws IOException {
        int count = 0;
        StringBuilder where = new StringBuilder();
        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (f.getFileName().toString().equals("PlatformTypes.java")) {
                    continue;
                }
                Matcher m = LITERAL_NAME_CHECK.matcher(Files.readString(f));
                int n = 0;
                while (m.find()) {
                    n++;
                }
                if (n > 0) {
                    count += n;
                    where.append(MAIN.relativize(f)).append('=').append(n).append(' ');
                }
            }
        }
        // 73 at batch 114 (2026-09-06) — SHRINK-ONLY: every burn moves a
        // spelling into PlatformTypes; a new literal check anywhere else
        // fails here
        // 60 measured 2026-09-11 (was 73 at batch 114): re-pinned to the
        // measurement — headroom is not a pin
        assertTrue(count <= 60, "literal Pure-name checks outside PlatformTypes grew: "
                + count + " > 60 — " + where);
    }

    /** The catalogs — the files where a Pure FUNCTION name may be spelled
     *  as a literal: the signatures, the registered families, the subsumed
     *  and walled registries, the system metamodel, the type spellings. */
    private static final java.util.Set<String> CATALOG_FILES = java.util.Set.of(
            "Pure.java", "NativeFn.java", "Subsumed.java", "SystemMetamodel.java",
            "PlatformTypes.java", "WalledBodies.java",
            // the legacy TDS vocabulary as a closed enum (batch 5 audit leg C,
            // 2026-09-11): upstream tds.pure / math::olap identities the Typer
            // desugars by exact spelling — a catalog of names, like NativeFn
            "TdsLegacy.java");

    /** A function FQN literal: {@code "meta::…::lowerCamel"} (a class or enum
     *  FQN ends in an upper-case segment and is a TYPE spelling, not dispatch). */
    private static final Pattern FUNCTION_FQN_LITERAL =
            Pattern.compile("\"meta::[A-Za-z_:]*::[a-z][A-Za-z0-9_]*\"");

    /** A bare-name switch arm: {@code case "lowerCamel"} — dispatch on a
     *  simple function name, the string-dispatch shape batch 4b retired from
     *  the executor. The parser's section grammars switch on KEYWORDS and
     *  are not counted. */
    private static final Pattern BARE_NAME_ARM =
            Pattern.compile("case \"[a-z][A-Za-z0-9]*\"");

    @Test
    void functionFqnLiteralsOutsideTheCatalogsOnlyShrink() throws IOException {
        int count = 0;
        StringBuilder where = new StringBuilder();
        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                if (CATALOG_FILES.contains(f.getFileName().toString())) {
                    continue;
                }
                int n = 0;
                for (String line : Files.readAllLines(f)) {
                    String code = line.stripLeading();
                    if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                        continue;
                    }
                    Matcher m = FUNCTION_FQN_LITERAL.matcher(line);
                    while (m.find()) {
                        n++;
                    }
                }
                if (n > 0) {
                    count += n;
                    where.append(MAIN.relativize(f)).append('=').append(n).append(' ');
                }
            }
        }
        // MEASURED 2026-09-11 (upstream boundary, after batch 4b): the
        // compiler's own string dispatch — SHRINK-ONLY; a new function-name
        // literal anywhere but a catalog fails here (USER: "only on typed
        // things that are registered")
        assertTrue(count <= FUNCTION_FQN_LITERALS_MAX, "function-FQN literals outside the catalogs grew: "
                + count + " > " + FUNCTION_FQN_LITERALS_MAX + " — " + where);
    }

    @Test
    void bareNameSwitchArmsOutsideTheParserOnlyShrink() throws IOException {
        int count = 0;
        StringBuilder where = new StringBuilder();
        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                // path ELEMENTS, never a slash-spelled substring: Windows paths
                // are backslash-separated (CI 2026-09-11: the parser package was
                // not excluded there and the count read 417)
                boolean underParser = false;
                for (Path part : MAIN.relativize(f)) {
                    if (part.toString().equals("parser")) {
                        underParser = true;
                    }
                }
                if (underParser || CATALOG_FILES.contains(f.getFileName().toString())) {
                    continue;
                }
                int n = 0;
                for (String line : Files.readAllLines(f)) {
                    String code = line.stripLeading();
                    if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                        continue;
                    }
                    Matcher m = BARE_NAME_ARM.matcher(line);
                    while (m.find()) {
                        n++;
                    }
                }
                if (n > 0) {
                    count += n;
                    where.append(MAIN.relativize(f)).append('=').append(n).append(' ');
                }
            }
        }
        assertTrue(count <= BARE_NAME_ARMS_MAX, "bare-name switch arms outside the parser grew: "
                + count + " > " + BARE_NAME_ARMS_MAX + " — " + where);
    }

    /** Pins, MEASURED at their introduction (2026-09-11) — set from the
     *  first run's count, never from a guess. */
    static final int FUNCTION_FQN_LITERALS_MAX = 324;
    static final int BARE_NAME_ARMS_MAX = 116;

    @Test
    void runtimeShapesAreReadByTheOneReaderOnly() throws IOException {
        StringBuilder found = new StringBuilder();
        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (f.endsWith("ExecutionContext.java")) {
                    continue;
                }
                String text = Files.readString(f);
                for (String w : RETIRED_WALKERS) {
                    if (text.contains(" " + w) || text.contains("." + w)) {
                        found.append(MAIN.relativize(f)).append(':').append(w).append(' ');
                    }
                }
            }
        }
        assertEquals("", found.toString(),
                "a runtime-shape walker reappeared outside ExecutionContext.Reader: ");
        assertTrue(!Files.exists(MAIN.resolve("ConnectionFlags.java")),
                "ConnectionFlags is retired: its readers live in ExecutionContext.Reader");
    }
}
