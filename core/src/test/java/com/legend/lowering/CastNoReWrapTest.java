// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * M4 §3.3 — the no-re-wrap decision: a LITERAL-marked value is a
 * self-describing Any carrier, so an Any-conformance over it emits NO
 * carrier cast ("labels distinguish carriers, casts never
 * re-carrier"). Decided on the value's STORED type fact — the typed
 * IR's clean read at CastPolicy's variant arm.
 */
class CastNoReWrapTest {

    private static TypedCast anyConformance(Multiplicity m) {
        Type variant = new Type.ClassType(PlatformTypes.VARIANT);
        Type any = new Type.ClassType(PlatformTypes.ANY);
        return new TypedCast(
                new TypedVariable("v", new ExprType(variant, m)),
                any, new ExprType(any, m), false);
    }

    @Test
    void literalMarkedValueKeepsItsMark() {
        SqlExpr scalar = new SqlExpr.Cast(new SqlExpr.StringLit("1"),
                SqlType.Scalar.LITERAL);
        assertSame(scalar, CastPolicy.lower(
                anyConformance(Multiplicity.Bounded.ONE), scalar, false, false));

        SqlExpr carried = new SqlExpr.Cast(
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("1"))),
                new SqlType.Array(SqlType.Scalar.LITERAL));
        assertSame(carried, CastPolicy.lower(
                anyConformance(Multiplicity.Bounded.ONE), carried, true, false));
    }

    @Test
    void anEmptyCastToAVariantIsTheTypedNull() {
        // []->cast(@Variant): the typed NULL (the cast types the empty; Postgres's jsonb operators refuse an
        // untyped NULL), 2026-10-17
        Type variant = new Type.ClassType(PlatformTypes.VARIANT);
        Type nil = new Type.ClassType(PlatformTypes.NIL);
        TypedCast c = new TypedCast(new TypedVariable("v", new ExprType(nil, new Multiplicity.Bounded(0, 0))),
                variant, new ExprType(variant, new Multiplicity.Bounded(0, 0)), false);
        SqlExpr.Cast cast = assertInstanceOf(SqlExpr.Cast.class,
                CastPolicy.lower(c, new SqlExpr.NullLit(), false, false));
        assertSame(SqlType.Scalar.JSON, cast.target());
        assertInstanceOf(SqlExpr.NullLit.class, cast.value());
    }

    @Test
    void unmarkedValueStillGetsTheCarrierCast() {
        // the control: without the LITERAL mark the variant arm's
        // carrier cast emits exactly as before
        SqlExpr plain = new SqlExpr.Column("t", "c");
        assertInstanceOf(SqlExpr.Cast.class, CastPolicy.lower(
                anyConformance(Multiplicity.Bounded.ONE), plain, false, false));
        assertInstanceOf(SqlExpr.Cast.class, CastPolicy.lower(
                anyConformance(Multiplicity.Bounded.ONE), plain, true, false));
    }
}
