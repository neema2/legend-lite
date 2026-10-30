// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.executionplan.ExecutionPlan.Precision;
import com.legend.executionplan.ExecutionPlan.TdsResult.Shape;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The written-precision FACT a plan column carries ({@link ColumnFacts#precision}), pinned as a table over (Pure
 * type, slot, result shape) -- the S1.4 audit (2026-10-29) found the five spellings pinned and the fact's assignment
 * not. The rule (the start page's S1.4, the decisions log D5 and D6): a literal carrier is WRITTEN; a day is DAY; a
 * relation's or a graph fetch's time-bearing cell is INSTANT (legend-engine's TDS, nine digits) whatever carried it;
 * a value root's keeps legend-pure's value, WRITTEN where the cell carries its precision (a temporal text) and
 * MINIMAL where the database computed it (register S118); any other column has no temporal of its own.
 */
class ColumnFactsTest {

    private static Precision fact(String type, SqlType slot, Shape shape) {
        return ColumnFacts.precision(type, slot, shape);
    }

    @Test
    void aTaggedColumnHasNoOnePrecision() {
        // a mixed value's column (TAGGED<kind slot, ...>): its slots' kinds carry the precision (DateTime(n)) and
        // the decoders present each cell at its own, so the column states none
        SqlType tagged = new SqlType.Tagged(List.of(new SqlType.Tagged.Slot("DateTime(3)", SqlType.Scalar.TIMESTAMP),
                new SqlType.Tagged.Slot("Integer", SqlType.Scalar.BIGINT)));
        for (Shape shape : Shape.values()) {
            assertNull(fact("Any", tagged, shape), shape.name());
        }
    }

    @Test
    void aDayIsDay_byTheTypeOrTheSlot() {
        for (Shape shape : Shape.values()) {
            assertEquals(Precision.DAY, fact("StrictDate", SqlType.Scalar.DATE, shape), shape.name());
            assertEquals(Precision.DAY, fact("StrictDate", SqlType.Scalar.VARCHAR, shape), shape.name());
            assertEquals(Precision.DAY, fact("DateTime", SqlType.Scalar.DATE, shape), shape.name());
            assertEquals(Precision.DAY, fact("Date", SqlType.Scalar.DATE, shape), shape.name());
        }
    }

    @Test
    void aTemporalTextIsWrittenAtAValueRoot_anInstantInARelationOrAGraph() {
        assertEquals(Precision.WRITTEN, fact("DateTime", SqlType.Scalar.TEMPORAL_TEXT, Shape.VALUE));
        assertEquals(Precision.INSTANT, fact("DateTime", SqlType.Scalar.TEMPORAL_TEXT, Shape.RELATION));
        assertEquals(Precision.INSTANT, fact("DateTime", SqlType.Scalar.TEMPORAL_TEXT, Shape.OBJECTS));
        assertEquals(Precision.WRITTEN, fact("Date", SqlType.Scalar.TEMPORAL_TEXT, Shape.VALUE));
    }

    @Test
    void aTimestampIsMinimalAtAValueRoot_anInstantInARelationOrAGraph() {
        for (SqlType slot : new SqlType[] {SqlType.Scalar.TIMESTAMP, SqlType.Scalar.TIMESTAMPTZ}) {
            assertEquals(Precision.MINIMAL, fact("DateTime", slot, Shape.VALUE), slot.toString());
            assertEquals(Precision.INSTANT, fact("DateTime", slot, Shape.RELATION), slot.toString());
            assertEquals(Precision.INSTANT, fact("DateTime", slot, Shape.OBJECTS), slot.toString());
            assertEquals(Precision.MINIMAL, fact("Date", slot, Shape.VALUE), slot.toString());
            assertEquals(Precision.INSTANT, fact("Date", slot, Shape.RELATION), slot.toString());
        }
        // a Date or DateTime type in any other carrier (a text slot, a JSON slot, no slot) is a time-bearing value too
        assertEquals(Precision.MINIMAL, fact("DateTime", SqlType.Scalar.VARCHAR, Shape.VALUE));
        assertEquals(Precision.INSTANT, fact("DateTime", SqlType.Scalar.VARCHAR, Shape.RELATION));
        assertEquals(Precision.MINIMAL, fact("Date", null, Shape.VALUE));
        assertEquals(Precision.INSTANT, fact("DateTime", null, Shape.OBJECTS));
    }

    @Test
    void anyOtherColumnHasNoTemporalOfItsOwn() {
        for (Shape shape : Shape.values()) {
            assertNull(fact("Integer", SqlType.Scalar.BIGINT, shape), shape.name());
            assertNull(fact("String", SqlType.Scalar.VARCHAR, shape), shape.name());
            assertNull(fact("Float", SqlType.Scalar.DOUBLE, shape), shape.name());
            assertNull(fact("Any", SqlType.Scalar.VARCHAR, shape), shape.name());
        }
    }
}
