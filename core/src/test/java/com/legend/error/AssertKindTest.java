// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The one catalog of a failed assert's words (item 3, 2026-11-21): a record of each kind, its sentence, its trip
 *  through the test plan's failure and back. */
class AssertKindTest {

    @Test
    void aRowFillsItsSlotsInOrder() {
        assertEquals("expected 3, got 4", AssertKind.EQUALS.fill(Map.of("expected", "3", "actual", "4")));
        assertEquals("expected [1, 2] not to contain 2",
                AssertKind.NOT_CONTAINS.fill(Map.of("collection", "[1, 2]", "value", "2")));
        assertEquals("expected at least one value, got none", AssertKind.NOT_EMPTY.fill(Map.of()));
    }

    @Test
    void aMissingOrExtraSlotIsRefusedByName() {
        assertThrows(IllegalArgumentException.class, () -> AssertKind.EQUALS.fill(Map.of("expected", "3")));
        assertThrows(IllegalArgumentException.class, () -> AssertKind.TRUE.fill(Map.of("expected", "3")));
        assertThrows(IllegalArgumentException.class, () -> AssertKind.EQUALS.record(Map.of("expected", "3")));
        assertThrows(IllegalArgumentException.class, () -> AssertKind.EQUALS.sentence(List.of("3")));
    }

    @Test
    void everyRowParses() {
        for (AssertKind s : AssertKind.values()) {
            StringBuilder back = new StringBuilder();
            for (AssertKind.Piece p : s.pieces()) {
                back.append(switch (p) {
                    case AssertKind.Text t -> t.text();
                    case AssertKind.Slot slot -> "{" + slot.name() + "}";
                });
            }
            assertEquals(s.template(), back.toString(), s.name());
        }
    }

    /** Every kind: a record by its slots' names holds them in the template's order, its sentence is the filled
     *  template, its code is found again by its qualified name, and an assert's failure rebuilt from it says the same
     *  sentence and holds the same record. */
    @Test
    void everyKindIsARecordWordedByTheCatalog() {
        for (AssertKind k : AssertKind.values()) {
            Map<String, String> slots = new java.util.LinkedHashMap<>();
            List<String> ordered = new ArrayList<>();
            for (String slot : k.slots()) {
                slots.put(slot, "<" + slot + ">");
                ordered.add("<" + slot + ">");
            }
            ErrorRecord r = k.record(slots);
            assertEquals(ordered, r.values(), k.name());
            assertEquals(k.arity(), r.values().size(), k.name());
            assertEquals(k.fill(slots), r.sentence(), k.name());
            assertEquals("assert." + k.name(), k.qualified(), k.name());
            assertSame(k, ErrorCode.named(k.qualified()), k.name());
            AssertFailed f = new AssertFailed(r);
            assertEquals(r.sentence(), f.getMessage(), k.name());
            assertSame(r, f.record(), k.name());
            assertNull(f.unjudgedReason(), k.name());
        }
    }

    @Test
    void anElementsFailureIsFramedByItsIndex() {
        ErrorRecord r = AssertKind.EQUALS.record(Map.of("expected", "1", "actual", "2"))
                .framed(new ErrorRecord.Frame(FrameKind.AT_INDEX, List.of("3")));
        assertEquals("expected 1, got 2 at index 3", r.sentence());
        assertEquals("expected 1, got 2 at index 3", new AssertFailed(r).getMessage());
    }

    @Test
    void anAssertsRecordIsAnAssertsCode() {
        ErrorRecord notAnAssert = ErrorRecord.of(ErrorKind.DIVIDE_BY_ZERO, List.of());
        assertThrows(IllegalArgumentException.class, () -> new AssertFailed(notAnAssert));
    }

    /** The platform functions' own asserts keep legend-engine's sentences (read from its test-data generator and CSV
     *  seed; lite has none of its own for them). */
    @Test
    void thePlatformsAssertsSayLegendEnginesWords() {
        assertEquals("Seed data generation currently supports queries with a single root (got: 2)",
                AssertKind.SEED_SINGLE_ROOT.record(Map.of("count", "2")).sentence());
        assertEquals("Table : T must have atleast 1 primary key or non-null column to utilise this feature",
                AssertKind.SEED_TABLE_KEYS.record(Map.of("table", "T")).sentence());
        assertEquals("There should be exactly 1 schema with name S found in the provided runtime database(s) [a::Db]"
                + " at the given connection.", AssertKind.CSV_SCHEMA_NOT_FOUND.record(Map.of("schema", "S",
                "databases", "a::Db")).sentence());
        assertEquals("Table \"T\" not found in Schema \"S\" in Database(s) a::Db",
                AssertKind.CSV_TABLE_NOT_FOUND.record(Map.of("table", "T", "schema", "S", "databases", "a::Db"))
                        .sentence());
        assertEquals("Column \"c\" not found in Table \"T\" in Database(s) a::Db",
                AssertKind.CSV_COLUMN_NOT_FOUND.record(Map.of("column", "c", "table", "T", "database", "a::Db"))
                        .sentence());
        assertTrue(AssertKind.SEED_ROOT_TABLE.arity() == 0 && AssertKind.SEED_SINGLE_ROOT_VIEW.arity() == 0);
    }
}
