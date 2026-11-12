// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.sql.SqlExpr;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The cast-and-case census held to its pins ({@link CastCaseCensus}; E-4, 2026-11-12). */
class CastCaseCensusTest {

    @Test
    void everyCastAndCaseIsCountedByItsReasonAndHeldToItsPins() throws Exception {
        Map<CastCaseCensus.Key, long[]> pinned = CastCaseCensus.read(Files.readString(
                com.legend.testing.Runfile.property("legend.lean_sql.cast_case_census"), StandardCharsets.UTF_8));
        Map<CastCaseCensus.Key, long[]> measured = CastCaseCensus.measure(
                com.legend.testing.Runfile.property("legend.lean_sql.channel_a"));
        List<String> drift = CastCaseCensus.drift(pinned, measured);
        assertTrue(drift.isEmpty(), "the casts and cases left their census (" + CastCaseCensus.PINS + "):\n  "
                + String.join("\n  ", drift));
    }

    /** The rule catches a cast more, and a cast or case cannot be made without a reason. */
    @Test
    void theRuleCatchesGrowth_andANodeWithNoReasonIsRefused() {
        Map<CastCaseCensus.Key, long[]> pinned = new TreeMap<>(Map.of(
                new CastCaseCensus.Key("DuckDB", "carrier"), new long[] {100, 10}));
        Map<CastCaseCensus.Key, long[]> grown = new TreeMap<>(Map.of(
                new CastCaseCensus.Key("DuckDB", "carrier"), new long[] {101, 10}));
        assertEquals(1, CastCaseCensus.drift(pinned, grown).size());
        assertTrue(CastCaseCensus.drift(pinned, pinned).isEmpty());
        assertThrows(NullPointerException.class,
                () -> new SqlExpr.Cast(new SqlExpr.NullLit(), com.legend.sql.SqlType.Scalar.VARCHAR, null));
        assertThrows(NullPointerException.class,
                () -> new SqlExpr.Case(List.of(), new SqlExpr.NullLit(), null));
    }
}
