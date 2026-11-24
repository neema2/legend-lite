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
        assertEquals("expected: 3, actual: 4", AssertKind.EQUALS.fill(Map.of("expected", "3", "actual", "4")));
        assertEquals("[1, 2] should not contain 2",
                AssertKind.NOT_CONTAINS.fill(Map.of("collection", "[1, 2]", "value", "2")));
        assertEquals("Expected non-empty collection", AssertKind.NOT_EMPTY.fill(Map.of()));
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
        assertEquals("expected: 1, actual: 2 at index 3", r.sentence());
        assertEquals("expected: 1, actual: 2 at index 3", new AssertFailed(r).getMessage());
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

    /** Every template pinned by hand (the S1 audit's L5: a mistyped template must fail here, not pass by reading the
     *  table it checks). A row changes only with a dated reason: its words are what every route says. */
    @Test
    void everyTemplateIsPinned() {
        Map<AssertKind, String> pinned = Map.ofEntries(
                Map.entry(AssertKind.EQUALS, "expected: {expected}, actual: {actual}"),
                Map.entry(AssertKind.EQUALS_KINDS, "expected: {expected} ({expectedKind}), actual: {actual} ({actualKind})"),
                Map.entry(AssertKind.NOT_EQUALS, "{expected} should not equal {actual}"),
                Map.entry(AssertKind.SAME_ELEMENTS, "expected the elements {expected} in any order, got {actual}"),
                Map.entry(AssertKind.SIZE, "expected size: {expected}, actual size: {actual}"),
                Map.entry(AssertKind.NOT_SIZE, "size should not equal: {size}"),
                Map.entry(AssertKind.CONTAINS, "{collection} does not contain {value}"),
                Map.entry(AssertKind.NOT_CONTAINS, "{collection} should not contain {value}"),
                Map.entry(AssertKind.EMPTY, "{collection} is not empty"),
                Map.entry(AssertKind.NOT_EMPTY, "Expected non-empty collection"),
                Map.entry(AssertKind.TRUE, "Assert failed"),
                Map.entry(AssertKind.FALSE, "Assert failed"),
                Map.entry(AssertKind.IS, "expected: {expected}, actual: {actual}"),
                Map.entry(AssertKind.IS_NOT, "Should not be identical to: {actual}"),
                Map.entry(AssertKind.SAME_INSTANCE, "expected the same instance"),
                Map.entry(AssertKind.INSTANCE_OF, "expected an instance of {type}, got {actual}"),
                Map.entry(AssertKind.TDS_EQUIVALENT, "expected the rows {expected} within {delta}, got {actual}"),
                Map.entry(AssertKind.WITHIN_TOLERANCE, "expected {expected} within {delta}, got {actual}"),
                Map.entry(AssertKind.JSON_EQUALS, "JSON strings don't represent semantically same object, expected: {expected}, actual: {actual}"),
                Map.entry(AssertKind.ERROR, "Execution error message mismatch. The actual message was {actual} where the expected message was:{expected}"),
                Map.entry(AssertKind.NO_ERROR, "Execution error message mismatch. The actual message was \"No error was thrown\" where the expected message was:{expected}"),
                Map.entry(AssertKind.ERROR_LINE, "Execution error line mismatch. Actual: {actual} where expected: {expected}"),
                Map.entry(AssertKind.ERROR_COLUMN, "Execution error column mismatch. Actual: {actual} where expected: {expected}"),
                Map.entry(AssertKind.ERROR_NO_POSITION, "expected the error at a source position, got an error with none"),
                Map.entry(AssertKind.MESSAGE, "{message}"),
                Map.entry(AssertKind.SEED_SINGLE_ROOT_VIEW, "Seed data generation currently supports queries with a single root table/view (unions are not supported)"),
                Map.entry(AssertKind.SEED_SINGLE_ROOT, "Seed data generation currently supports queries with a single root (got: {count})"),
                Map.entry(AssertKind.SEED_ROOT_TABLE, "Seed data generation currently supports queries with a root table (unions/views are not supported, got: RelationTree)"),
                Map.entry(AssertKind.SEED_TABLE_KEYS, "Table : {table} must have atleast 1 primary key or non-null column to utilise this feature"),
                Map.entry(AssertKind.CSV_SCHEMA_NOT_FOUND, "There should be exactly 1 schema with name {schema} found in the provided runtime database(s) [{databases}] at the given connection."),
                Map.entry(AssertKind.CSV_TABLE_NOT_FOUND, "Table \"{table}\" not found in Schema \"{schema}\" in Database(s) {databases}"),
                Map.entry(AssertKind.CSV_COLUMN_NOT_FOUND, "Column \"{column}\" not found in Table \"{table}\" in Database(s) {database}"));
        assertEquals(java.util.Set.of(AssertKind.values()), pinned.keySet(), "a kind added or removed is pinned here");
        for (AssertKind k : AssertKind.values()) {
            assertEquals(pinned.get(k), k.template(), k.name());
        }
    }

    /** The AT_INDEX frame's shape the database judge's SQL builds on (VerdictSql.atIndexFrame: the message, then the
     *  frame's text, the index last; the S1 audit's L4): no text before the inner failure, the index the frame's end. */
    @Test
    void theIndexFrameIsTheMessageThenItsIndex() {
        assertEquals("", FrameKind.AT_INDEX.wrap(List.of(""), "").replace(" at index ", ""));
        assertEquals("inner at index 7", FrameKind.AT_INDEX.wrap(List.of("7"), "inner"));
        assertTrue(FrameKind.AT_INDEX.wrap(List.of("7"), "inner").startsWith("inner"), "nothing before the failure");
    }
}
