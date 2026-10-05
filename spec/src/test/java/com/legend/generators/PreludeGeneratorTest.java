// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
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


    static Path engineRoot() {
        return com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
    }

    static Path pureRoot() {
        return com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
    }

    @Test
    @DisplayName("the engine-library membership is the one interim row (charter D7)")
    void engineLibraryMembershipIsTheOneInterimRow() {
        // the membership list is the declared INTERIM for the namespace rule
        // (charter D7); it grows only with that rule's own work, never a row at a time
        org.junit.jupiter.api.Assertions.assertEquals(java.util.Set.of("meta::pure::functions::collection::removeAll"),
                PreludeGenerator.ENGINE_LIBRARY_FUNCTIONS.keySet());
    }

    @Test
    @DisplayName("m3 reader: every class of m3.pure prints as a declaration")
    void m3ReaderPrintsEveryClass() throws IOException {
        Path m3 = pureRoot().resolve(PreludeGenerator.M3_PURE);
        // never an assumption-skip (SkipCensusTest): the reference checkout is
        // this test class's hard default
        assertTrue(Files.isRegularFile(m3), "m3.pure missing at " + m3);
        Map<String, String> decls = PreludeGenerator.m3Declarations(Files.readString(m3, StandardCharsets.UTF_8));
        for (Map.Entry<String, String> e : decls.entrySet()) {
            // every printed declaration parses through the platform's own door
            ParsedModel parsed = ElementParser.parse(e.getValue(), Dialect.LEGEND_PLATFORM);
            assertEquals(1, parsed.elements().size(), e.getKey());
        }
        assertTrue(decls.size() >= 85, "m3.pure declares 85 classes; read " + decls.size());
    }
}
