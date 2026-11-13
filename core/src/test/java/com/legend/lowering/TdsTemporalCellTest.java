// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A TDS LITERAL'S DATE CELL IS ISO-SHAPED OR REFUSED (the final audit, 2026-11-12: the class of the test-data routes'
 * literal). A typed literal is written into its statement as its text is, so a {@code #TDS} cell of an annotated
 * StrictDate, Date or DateTime column -- the query's own text, never lexed as a date -- becomes one only where it is a
 * date or date-time in its ISO shape; a quote, which would end the literal, or any other shape is refused by name. The
 * planning door refuses such a query before any statement is written.
 */
class TdsTemporalCellTest {

    @Test
    void anIsoCellIsItsTypedLiteral() {
        assertEquals(new SqlExpr.DateLit("2024-01-01"), Scalars.tdsCell("%2024-01-01", Type.Primitive.STRICT_DATE));
        assertEquals(new SqlExpr.DateLit("2024-01-01"), Scalars.tdsCell("2024-01-01", Type.Primitive.DATE));
        assertEquals(new SqlExpr.TimestampLit("2024-01-29T00:32:34.123456"),
                Scalars.tdsCell("2024-01-29T00:32:34.123456789+0000", Type.Primitive.DATE));
        assertEquals(new SqlExpr.TimestampLit("2024-01-29 00:32"),
                Scalars.tdsCell("2024-01-29 00:32", Type.Primitive.DATE_TIME));
    }

    @Test
    void anyOtherCellIsRefusedByName() {
        for (Type type : List.of(Type.Primitive.STRICT_DATE, Type.Primitive.DATE, Type.Primitive.DATE_TIME)) {
            for (String bad : List.of("2024-01-01' OR '1'='1", "01/02/2024", "2024-01-01'")) {
                IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                        () -> Scalars.tdsCell(bad, type));
                assertTrue(refused.getMessage().startsWith("a TDS cell '" + bad + "' of a " + type.typeName()
                        + " column is no ISO date"), refused.getMessage());
            }
        }
    }

    /** Through the planning door: a query whose TDS literal holds a quote in an annotated date column is refused by
     *  name, no plan made. */
    @Test
    void aQueryWhoseTdsDateCellHoldsAQuoteIsRefused() {
        String model = "###Relational\nDatabase x::DB ( Table t ( id INTEGER ) )\n###Connection\n"
                + "RelationalDatabaseConnection x::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }\n"
                + "###Runtime\nRuntime x::RT { mappings: []; connections: [ x::DB: [ c1: x::Conn ] ]; }\n";
        Throwable refused = assertThrows(RuntimeException.class, () -> com.legend.Compiler.query(
                com.legend.Compiler.compileModel(model), "|#TDS\n  d:StrictDate\n  2024-01-01' OR '1'='1\n#")
                .executionPlan("x::RT"));
        Throwable cause = refused;
        while (cause.getCause() != null && !(cause instanceof IllegalArgumentException)) {
            cause = cause.getCause();
        }
        assertTrue(String.valueOf(cause.getMessage()).contains("is no ISO date"), String.valueOf(refused));
    }
}
