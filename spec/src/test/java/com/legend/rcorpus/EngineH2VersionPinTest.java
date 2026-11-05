// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.rcorpus;

import com.legend.sql.dialect.RawSqlBoundary;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The raw-SQL boundary's H2 dialect level is legend-engine's own H2, read from the pinned engine's root POM
 *  ({@code h2.version}): {@code SELECT H2VERSION()} answers it, so the engine's version probe picks the branch its
 *  goldens were written on. Not lite's H2 jar (2.5.252 since 2026-11-05; until then the same 2.1.214, which this test
 *  pinned as {@code H2VersionPinTest} in core). */
class EngineH2VersionPinTest {

    @Test
    void h2VersionIsTheEnginesOwnH2() throws Exception {
        String pom = Files.readString(Corpus.ENGINE_ROOT.resolve("pom.xml"));
        Matcher m = Pattern.compile("<h2\\.version>([^<]+)</h2\\.version>").matcher(pom);
        assertTrue(m.find(), "the pinned legend-engine root POM names no h2.version");
        assertEquals(m.group(1), RawSqlBoundary.H2_DIALECT_VERSION,
                "legend-engine's H2 moved: re-pin RawSqlBoundary.H2_DIALECT_VERSION with the engine's matching extension");
    }
}
