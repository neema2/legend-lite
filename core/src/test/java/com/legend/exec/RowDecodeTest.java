// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runner's typed-row decode (the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md §6): a row the
 * database wrote as one JSON object read into cells by the plan's columns alone -- each kind its carrier, the one the
 * host adjudication layer reads; a token not of its column's kind refused by name.
 */
class RowDecodeTest {

    private static final Multiplicity ONE = new Multiplicity(1, 1);

    private static Column col(String name, String type, String slot) {
        return new Column(name, type, ONE, slot, null);
    }

    private static Object one(String type, String slot, String token) {
        return RowDecode.cells("{\"v\":" + token + "}", List.of(col("v", type, slot))).get(0);
    }

    @Test
    void eachPrimitiveKindDecodesToItsCarrier() {
        assertEquals(2L, one("Integer", "INTEGER", "2"));
        assertEquals(3.0d, one("Float", "DOUBLE", "3.0"));
        assertEquals(2.5e-7d, one("Float", "DOUBLE", "0.00000025"));
        assertEquals(new BigDecimal("2.50"), one("Decimal", "DECIMAL(10,2)", "2.50"));
        assertEquals(new BigDecimal("2.50"), one("Decimal(10,2)", "DECIMAL(10,2)", "2.50"));
        assertEquals("O'Brien", one("String", "VARCHAR", "\"O'Brien\""));
        assertEquals(Boolean.TRUE, one("Boolean", "BOOLEAN", "true"));
        assertEquals(PureDateLiteral.parse("2024-01-02"), one("StrictDate", "DATE", "\"2024-01-02\""));
        assertEquals(PureDateLiteral.parse("2024-01-02T10:30:00.123"),
                one("DateTime", "TIMESTAMP", "\"2024-01-02T10:30:00.123+0000\""));
        assertEquals(PureDateLiteral.parse("2024-01-02T10:30:00"), one("Date", "TIMESTAMP", "\"2024-01-02 10:30:00\""));
        assertNull(one("Integer", "INTEGER", "null"));
    }

    @Test
    void aNumberIsDecodedByItsSlot_orByItsTokenWithoutOne() {
        assertEquals(2L, one("Number", "BIGINT", "2"));
        assertEquals(2.5d, one("Number", "DOUBLE", "2.5"));
        assertEquals(new BigDecimal("2.5"), one("Number", "DECIMAL(3,1)", "2.5"));
        // no slot (a JSON cell): a whole number a long, any other its exact decimal -- the token's own digits
        assertEquals(2L, one("Number", null, "2"));
        assertEquals(new BigDecimal("2.50"), one("Number", null, "2.50"));
    }

    @Test
    void theSlotDecidesTheCarrierBeforeTheDeclaredType() {
        // a column the typer names String over a BIGINT slot reads as the database holds it (the legacy executor read
        // a driver's cell by the output's SQL type); a String over VARCHAR stays text
        assertEquals(11L, one("String", "BIGINT", "11"));
        assertEquals("11", one("String", "VARCHAR", "\"11\""));
        // a string token under a numeric slot is the column's own text (the engine's test store declares
        // interactionTable.ID INT over a VARCHAR column: the String-typed id reads '7'), unless the Pure type is
        // numeric, which parses a wire's quoted number
        assertEquals("7", one("String", "BIGINT", "\"7\""));
        assertEquals("7", one("meta::pure::metamodel::type::Any", "BIGINT", "\"7\""));
        assertEquals("7.5", one("String", "DOUBLE", "\"7.5\""));
        assertEquals(7L, one("Integer", "BIGINT", "\"7\""));
        assertEquals(new BigDecimal("7.5"), one("Decimal", "DECIMAL(3,1)", "\"7.5\""));
        assertEquals(PureDateLiteral.parse("2015-08-15T00:00:00.000000000"),
                one("Date", "DATE", "\"2015-08-15T00:00:00.000000000\""));
    }

    @Test
    void theSlotsLabelIsTheDecodeInstruction() {
        // a literal-spelled cell carries its own kind; a text-carried decimal or temporal parses
        assertEquals(new BigDecimal("7.345"), one("Any", "LITERAL", "\"7.345D\""));
        assertEquals(new BigDecimal("7.345"), one("Number", "DECIMAL_TEXT", "\"7.345D\""));
        assertEquals(PureDateLiteral.parse("2024-01-02T10:30"), one("Any", "TEMPORAL_TEXT", "\"2024-01-02T10:30\""));
    }

    @Test
    void anIntegerBeyondALongKeepsItsDigits() {
        assertEquals(new BigInteger("123456789012345678901234567890"),
                one("Integer", "HUGEINT", "\"123456789012345678901234567890\""));
        assertEquals(5L, one("Integer", "BIGINT", "5.0"));   // a scale-0 decimal under an integral slot
    }

    @Test
    void aClassInstanceIsItsStructure_anEnumItsName_aVariantItsText() {
        Object instance = one("my::Person", "JSON", "{\"__type\":\"my::Person\",\"name\":\"a\",\"age\":1,\"w\":1.5}");
        assertEquals(Map.of("__type", "my::Person", "name", "a", "age", 1L, "w", new BigDecimal("1.5")), instance);
        assertEquals("CITY", one("my::Kind", "VARCHAR", "\"CITY\""));
        assertEquals("{\"a\":[1,2.5,\"x\",null,true]}",
                one("meta::pure::metamodel::variant::Variant", "JSON", "{\"a\":[1,2.5,\"x\",null,true]}"));
        // the Any lane reads a token's own kind: a whole number a long, any other number a double
        assertEquals(1L, one("Any", "JSON", "1"));
        assertEquals(1.5d, one("Any", "JSON", "1.5"));
        assertEquals("x", one("Any", "JSON", "\"x\""));
        assertEquals(List.of(1L, "x"), one("Any", "JSON", "[1,\"x\"]"));
    }

    @Test
    void aTokenNotOfItsColumnsKindIsRefusedByName() {
        var refused = assertThrows(IllegalStateException.class, () -> one("Boolean", "BOOLEAN", "\"yes\""));
        assertTrue(refused.getMessage().contains("declared Boolean"), refused.getMessage());
        assertTrue(refused.getMessage().contains("\"yes\""), refused.getMessage());
        var missing = assertThrows(IllegalStateException.class,
                () -> RowDecode.cells("{\"w\":1}", List.of(col("v", "Integer", "INTEGER"), col("x", "Integer", null))));
        assertTrue(missing.getMessage().contains("no field for the plan's column 'v'"), missing.getMessage());
        var noObject = assertThrows(IllegalStateException.class,
                () -> RowDecode.cells("[1]", List.of(col("v", "Integer", "INTEGER"))));
        assertTrue(noObject.getMessage().contains("no JSON object"), noObject.getMessage());
    }

    @Test
    void columnsAreReadByName_orByPositionWhenNoNameMatches() {
        List<Column> cols = List.of(col("b", "Integer", "INTEGER"), col("a", "String", "VARCHAR"));
        assertEquals(List.of(2L, "x"), RowDecode.cells("{\"a\":\"x\",\"b\":2}", cols));
        // a value root's one column is named `value` by the plan and after its output by the wire
        assertEquals(List.of(2L), RowDecode.cells("{\"_v\":2}", List.of(col("value", "Integer", "INTEGER"))));
        ExecutionPlan.Column optional = new Column("v", "Integer", new Multiplicity(0, 1), "INTEGER", null);
        assertEquals(java.util.Collections.singletonList(null), RowDecode.cells("{\"v\":null}", List.of(optional)));
    }
}
