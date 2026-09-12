// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TWO rules guard the parser boundary (user contract 2026-08-12):
 *
 * <ol>
 *   <li><b>The compiler layer never touches the parser.</b>
 *       {@code com.legend.compiler.**} (checkers, typer, resolver,
 *       inliners) may not reference {@code com.legend.parser} AT ALL —
 *       which subsumes {@code Dialect}. NO allowlist, NO exceptions,
 *       ever: the one violation that existed (a speculative quoted-code
 *       fold in SourceSubst, with zero callers) was deleted rather than
 *       sanctioned.</li>
 *   <li><b>{@code Dialect} is confined by an explicit CLASS list.</b>
 *       Outside {@code com.legend.parser.**}, only the classes below may
 *       name a dialect — and the list STRUCTURALLY refuses entries under
 *       the compiler layer (adding one fails this test before it fails
 *       review). Shrink-only.</li>
 * </ol>
 */
class ParserBoundaryArchTest {

    /** Classes (path suffixes) allowed to reference {@code Dialect}
     *  outside the parser package — each a door or regime definition:
     *  the Compiler DRIVER (product policy + facades), the bootstrap
     *  loader, the two corpus quote/eval implementations, the ide
     *  product surface, the two test-regime fixtures, the corpus
     *  runner's provenance, and the parity harness's surface fixture. */
    private static final Set<String> DIALECT_CLASSES = Set.of(
            "com/legend/Compiler.java",
            "com/legend/builtin/Pure.java",
            // metamodel-store leg (2026-08-28): the system store/mapping
            // are fixed Pure SOURCE parsed once at class load — the same
            // bootstrap-loader regime as Pure itself
            "com/legend/builtin/SystemMetamodel.java",
            // the prelude MODULE (SYSTEM_PRELUDE_DESIGN §10, 2026-09-08):
            // generated spec declarations, fixed Pure SOURCE parsed once at
            // class load — the same regime as the system metamodel
            "com/legend/builtin/Prelude.java",
            "com/legend/harness/HarnessSubstitution.java",
            "com/legend/ide/ModelOrchestrator.java",
            "com/legend/testing/Own.java",
            "com/legend/testing/Engine.java",
            "com/legend/testing/Platform.java",
            // the minimal harness parses the engine's corpus sources in the
            // PLATFORM dialect (their provenance); its namespace-guard test
            // parses a platform-dialect fixture the same way
            "com/legend/rcorpus/MinimalCorpus.java",
            "com/legend/rcorpus/LibraryPlatformNamespaceGuardTest.java",
            // the prelude GENERATOR parses the spec's declaration files in
            // the platform dialect (WORLD_MAP rule 2, 2026-09-04)
            "com/legend/generators/PreludeGeneratorTest.java",
            // the SIGNATURE generator reads the same declaration files (upstream
            // boundary batch 5): Pure.java's text is upstream's, parsed as such
            "com/legend/generators/NativeSignatureGeneratorTest.java",
            // the typing CENSUS parses legend-pure's platform packages —
            // the spec's own declaration files, the same provenance as the
            // generator (SYSTEM_PRELUDE_DESIGN §6, 2026-09-08)
            "com/legend/generators/SpecBodyCensusTest.java",
            // the census's RUNNING-WORLD pass parses the engine files a module
            // body runs against (its own spec file, the corpus's library
            // files) — COMPILE_EVERYTHING_HOMEWORK §6, batch 168
            "com/legend/generators/CensusWorlds.java",
            // the EAGER corpus compile probe (COMPILE_EVERYTHING_HOMEWORK §10) parses
            // the corpus world plus legend-pure's platform packages to measure what
            // closes — a measurement run by name, not a gate (batch 169)
            "com/legend/rcorpus/EagerCorpusCompileProbe.java",
            "com/legend/equivalence/Surfaces.java",
            // Phase 4 entry gate: channel B's front door IS a dialect
            // decision — the PCT sources are the M3 surface, parsed at
            // LEGEND_PLATFORM (the census names that fact deliberately)
            "com/legend/equivalence/PctParseCensusTest.java",
            // Phase 4: channel B parses the PCT M3 sources — the same
            // front-door dialect decision the census names
            "org/finos/legend/lite/pct/channelb/ChannelB.java");

    @Test
    void theCompilerLayerNeverTouchesTheParser() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path root : roots()) {
            try (var walk = Files.walk(root)) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".java"))
                        .toList()) {
                    String rel = rel(root, f);
                    if (!rel.contains("com/legend/compiler/")) {
                        continue;
                    }
                    int line = firstHit(f, "com.legend." + "parser");
                    if (line > 0) {
                        violations.add(rel + ":" + line);
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(),
                () -> "the COMPILER LAYER references the parser — there are"
                        + " no exceptions to this rule; move the code to a"
                        + " door (parser/harness) or delete it:\n  "
                        + String.join("\n  ", violations));
    }

    @Test
    void dialectIsConfinedToTheNamedClasses() throws IOException {
        // the list itself may never sanction the compiler layer
        for (String entry : DIALECT_CLASSES) {
            assertTrue(!entry.contains("com/legend/compiler/"),
                    () -> "DIALECT_CLASSES may not include compiler-layer"
                            + " classes: " + entry);
        }
        List<String> violations = new ArrayList<>();
        for (Path root : roots()) {
            try (var walk = Files.walk(root)) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".java"))
                        .toList()) {
                    String rel = rel(root, f);
                    if (rel.startsWith("com/legend/parser/")
                            || DIALECT_CLASSES.contains(rel)) {
                        continue;
                    }
                    int line = firstHit(f, "Dialect.LEGEND" + "_");
                    if (line < 0) {
                        line = firstHit(f, "com.legend.parser." + "Dialect");
                    }
                    if (line > 0) {
                        violations.add(rel + ":" + line);
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(),
                () -> "Dialect referenced outside the parser package and the"
                        + " named classes — name the class here (WITH its"
                        + " reason) or route through a door:\n  "
                        + String.join("\n  ", violations));
    }

    private static List<Path> roots() {
        List<Path> roots = new ArrayList<>();
        roots.add(Path.of("src/main/java"));
        roots.add(Path.of("src/test/java"));
        for (String sibling : new String[] {"../nlq/src", "../server/src",
                "../pct/src", "../parser-equivalence/src"}) {
            Path p = Path.of(sibling);
            if (Files.isDirectory(p)) {
                roots.add(p);
            }
        }
        return roots;
    }

    private static String rel(Path root, Path f) {
        String s = root.relativize(f).toString().replace(java.io.File.separatorChar, '/');
        int i = s.indexOf("java/");
        return i >= 0 ? s.substring(i + 5) : s;
    }

    /** First non-comment line containing {@code token}, or -1. */
    private static int firstHit(Path f, String token) throws IOException {
        List<String> lines = Files.readAllLines(f);
        boolean inBlockComment = false;
        for (int i = 0; i < lines.size(); i++) {
            String t = lines.get(i).trim();
            if (inBlockComment) {
                if (t.contains("*/")) {
                    inBlockComment = false;
                }
                continue;
            }
            if (t.startsWith("//") || t.startsWith("*")) {
                continue;
            }
            if (t.startsWith("/*")) {
                if (!t.contains("*/")) {
                    inBlockComment = true;
                }
                continue;
            }
            if (t.contains(token)) {
                return i + 1;
            }
        }
        return -1;
    }
}
