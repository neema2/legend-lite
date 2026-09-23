// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.testing.Repo;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The checks on {@link PreludeGenerator}: the committed prelude.pure is its
 *  current output, and the m3 reader prints every m3.pure class. */
class PreludeGeneratorTest {

    private static final Path OUT = CoreTree.resource("com/legend/builtin/prelude.pure");

    static Path engineRoot() {
        return com.legend.testing.Upstream.engine();
    }

    static Path pureRoot() {
        return com.legend.testing.Upstream.pure();
    }

    @Test
    @DisplayName("prelude.pure is the generator's current output (regenerate: bazel run //:update_generated)")
    void preludeIsCurrent() throws Exception {
        Path census = "1".equals(System.getProperty("prelude.census")) ? Repo.out("prelude-census.tsv") : null;
        String generated = PreludeGenerator.generate(engineRoot(), pureRoot(),
                SourceTree.of(CoreTree.CORE.resolve("src/main/java")), census);
        assertTrue(Files.exists(OUT), "prelude.pure missing — regenerate: bazel run //:update_generated");
        assertEquals(generated, Files.readString(OUT, StandardCharsets.UTF_8),
                "prelude.pure is stale: the spec moved or the file was edited by hand —"
                        + " regenerate: bazel run //:update_generated");
    }

    @Test
    @DisplayName("m3 reader: every class of m3.pure prints as a declaration (-Dprelude.m3=1 lists them)")
    void m3ReaderPrintsEveryClass() throws IOException {
        Path m3 = pureRoot().resolve(PreludeGenerator.M3_PURE);
        // never an assumption-skip (SkipCensusTest): the reference checkout is
        // this test class's hard default, exactly as preludeIsCurrent's
        assertTrue(Files.isRegularFile(m3), "m3.pure missing at " + m3);
        Map<String, String> decls = PreludeGenerator.m3Declarations(Files.readString(m3, StandardCharsets.UTF_8));
        for (Map.Entry<String, String> e : decls.entrySet()) {
            // every printed declaration parses through the platform's own door
            ParsedModel parsed = ElementParser.parse(e.getValue(), Dialect.LEGEND_PLATFORM);
            assertEquals(1, parsed.elements().size(), e.getKey());
            if ("1".equals(System.getProperty("prelude.m3"))) {
                System.out.println(e.getValue());
            }
        }
        assertTrue(decls.size() >= 85, "m3.pure declares 85 classes; read " + decls.size());
    }
}
