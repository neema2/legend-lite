// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the variant lane's VALUE LAW and its ONE inverse
 * (MixedEncoding.variantElement / unwrapVariant — TDSNull-is-a-value
 * slice, 2026-08-24). The law: a [1]-stamped ROW CELL never vanishes —
 * its runtime NULL rides as the JSON null VALUE; everything else keeps
 * the bare TO_VARIANT wrap byte-identical. The inverse must understand
 * EVERY shape the emitter produces — six consumers ask the owner
 * instead of matching wrap shapes locally (the shape-matchers went
 * stale the first time the wrap changed; slice-1-3 audit).
 */
class VariantElementLawTest {

    private static final SqlExpr JSON_NULL =
            new SqlExpr.Cast(new SqlExpr.StringLit("null"), SqlType.Scalar.JSON, SqlExpr.Reason.CARRIER);

    private static TypedSpec stamped(int lower, Integer upper) {
        return new TypedVariable("c", new ExprType(Type.Primitive.STRING,
                new Multiplicity.Bounded(lower, upper)));
    }

    private static final SqlExpr COMPUTED =
            SqlExpr.Call.of(SqlFn.UPPER, new SqlExpr.StringLit("x"));

    @Test
    @DisplayName("[1] cell (cellSlots): COALESCE(TO_VARIANT(e), json-null)")
    void oneStampedCellGetsTheSlotLaw() {
        SqlExpr e = MixedEncoding.variantElement(stamped(1, 1), COMPUTED, true);
        assertEquals(new SqlExpr.Call(SqlFn.COALESCE, java.util.List.of(
                SqlExpr.Call.of(SqlFn.TO_VARIANT, COMPUTED), JSON_NULL)), e);
    }

    @Test
    @DisplayName("[1] outside cellSlots: bare TO_VARIANT, byte-identical")
    void plainCollectionsKeepTheBareWrap() {
        assertEquals(SqlExpr.Call.of(SqlFn.TO_VARIANT, COMPUTED),
                MixedEncoding.variantElement(stamped(1, 1), COMPUTED, false));
    }

    @Test
    @DisplayName("[0..1] cell: bare wrap — an EMPTY decays, pure law")
    void optionalCellsStillDecay() {
        assertEquals(SqlExpr.Call.of(SqlFn.TO_VARIANT, COMPUTED),
                MixedEncoding.variantElement(stamped(0, 1), COMPUTED, true));
    }

    @Test
    @DisplayName("[1] static NULL literal (TDSNull): the json-null value")
    void staticTdsNullIsTheJsonNullValue() {
        assertEquals(JSON_NULL, MixedEncoding.variantElement(
                stamped(1, 1), new SqlExpr.NullLit(), true));
    }

    @Test
    @DisplayName("[1] static literal: bare wrap (COALESCE is a no-op)")
    void staticLiteralsStayByteIdentical() {
        SqlExpr lit = new SqlExpr.StringLit("a");
        assertEquals(SqlExpr.Call.of(SqlFn.TO_VARIANT, lit),
                MixedEncoding.variantElement(stamped(1, 1), lit, true));
    }

    @Test
    @DisplayName("unwrapVariant inverts BOTH emitted shapes; identity otherwise")
    void oneInverseUnderstandsEveryEmittedShape() {
        SqlExpr bare = MixedEncoding.variantElement(stamped(1, 1), COMPUTED, false);
        SqlExpr slot = MixedEncoding.variantElement(stamped(1, 1), COMPUTED, true);
        assertEquals(COMPUTED, MixedEncoding.unwrapVariant(bare));
        assertEquals(COMPUTED, MixedEncoding.unwrapVariant(slot));
        assertTrue(MixedEncoding.variantWrapped(bare));
        assertTrue(MixedEncoding.variantWrapped(slot));
        // not wrapped: returned as-is, reported unwrapped
        assertSame(COMPUTED, MixedEncoding.unwrapVariant(COMPUTED));
        assertFalse(MixedEncoding.variantWrapped(COMPUTED));
        // a USER coalesce (not the emitter's shape) is NOT claimed
        SqlExpr userCoalesce = new SqlExpr.Call(SqlFn.COALESCE,
                java.util.List.of(COMPUTED, new SqlExpr.StringLit("d")));
        assertSame(userCoalesce, MixedEncoding.unwrapVariant(userCoalesce));
    }
}
