// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.NameResolver;
import com.legend.rcorpus.Corpus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link NameResolver#CORE_IMPORTS} — the implicit import group every Pure
 * element resolves through, walked FIRST-MATCH — is upstream's, as a SEQUENCE
 * (order is semantic), from two upstream files:
 * <ul>
 *   <li>legend-pure's {@code m3.pure}, {@code system::imports::coreImport} — the
 *       platform's group (29 packages);</li>
 *   <li>legend-engine's {@code CompileContext.META_IMPORTS} — the engine's
 *       group: the same 29 plus {@code metamodel::variant},
 *       {@code metamodel::relation} and {@code precisePrimitives}, INTERLEAVED
 *       at the engine's positions. The corpus is engine code, so the engine's
 *       sequence is the one we resolve with.</li>
 * </ul>
 * Held here: ours equals the engine's exactly, and pure's 29 are exactly the
 * engine's minus those three (the two upstream files even ORDER the shared
 * packages differently — pure lists {@code functions::relation} before
 * {@code functions::io}, the engine after {@code functions::multiplicity} —
 * which is why the engine's sequence, not a merge, is the one held).
 * {@code -Dimports.generate=1} rewrites the constant from the checkout.
 * Until the batch-5 audit (2026-09-11) the list was typed by hand with the
 * three engine additions APPENDED — a different first-match order from the
 * engine's.
 */
class CoreImportsParityTest {

    /** The engine's compile context (ENGINE_ROOT-relative); in the path manifest. */
    static final String COMPILE_CONTEXT =
            "legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/"
            + "legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/"
            + "language/pure/compiler/toPureGraph/CompileContext.java";

    static List<String> engineMetaImports() throws IOException {
        String java = Files.readString(Corpus.ENGINE_ROOT.resolve(COMPILE_CONTEXT), StandardCharsets.UTF_8);
        int at = java.indexOf("META_IMPORTS");
        assertTrue(at >= 0, "CompileContext.META_IMPORTS not found — upstream moved it");
        int end = java.indexOf(");", at);
        Matcher m = Pattern.compile("\"(meta::[A-Za-z0-9_:]+)\"").matcher(java.substring(at, end));
        List<String> out = new ArrayList<>();
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    static List<String> pureCoreImport() throws IOException {
        String m3 = Files.readString(PreludeGeneratorTest.pureRoot().resolve(PreludeGeneratorTest.M3_PURE),
                StandardCharsets.UTF_8);
        int at = m3.indexOf("coreImport @Root");
        assertTrue(at >= 0, "m3.pure system::imports::coreImport not found — upstream moved it");
        int end = m3.indexOf("\n}\n", at);
        Matcher m = Pattern.compile("'(meta::[A-Za-z0-9_:]+)'").matcher(m3.substring(at, end));
        List<String> out = new ArrayList<>();
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    @Test
    @DisplayName("CORE_IMPORTS is the engine's META_IMPORTS sequence; pure's coreImport is that set minus the engine's three")
    void coreImportsAreUpstreams() throws IOException {
        Assumptions.assumeTrue(Files.isDirectory(Corpus.ENGINE_ROOT), "legend-engine checkout not present");
        Assumptions.assumeTrue(Files.isDirectory(PreludeGeneratorTest.pureRoot()), "legend-pure checkout not present");
        List<String> engine = engineMetaImports();
        List<String> pure = pureCoreImport();
        if ("1".equals(System.getProperty("imports.generate"))) {
            generate(engine);
            return;
        }
        assertEquals(engine, NameResolver.CORE_IMPORTS,
                "CORE_IMPORTS drifted from CompileContext.META_IMPORTS — regenerate with -Dimports.generate=1");
        List<String> engineMinusThree = new ArrayList<>(engine);
        engineMinusThree.removeAll(List.of("meta::pure::metamodel::variant",
                "meta::pure::metamodel::relation", "meta::pure::precisePrimitives"));
        assertEquals(new java.util.TreeSet<>(pure), new java.util.TreeSet<>(engineMinusThree),
                "pure's coreImport is no longer the engine's META_IMPORTS minus variant/relation/precisePrimitives");
    }

    private static void generate(List<String> engine) throws IOException {
        Path src = CoreTree.main("com/legend/compiler/NameResolver.java");
        String text = Files.readString(src, StandardCharsets.UTF_8);
        int start = text.indexOf("CORE_IMPORTS = List.of(") + "CORE_IMPORTS = List.of(".length();
        int end = text.indexOf(");", start);
        StringBuilder sb = new StringBuilder("\n");
        for (int k = 0; k < engine.size(); k++) {
            sb.append("            \"").append(engine.get(k)).append('"').append(k + 1 < engine.size() ? ",\n" : "");
        }
        Files.writeString(src, text.substring(0, start) + sb + text.substring(end), StandardCharsets.UTF_8);
        System.out.println("[imports] regenerated " + engine.size() + " packages");
    }
}
