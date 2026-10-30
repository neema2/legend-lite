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
 * CastPolicy's variant arm: an Any-conformance's carrier cast, and the typed NULL of an empty cast. (The no-re-wrap
 * test over a LITERAL-marked value, M4 §3.3, left with the spelling carrier, track L's L2, 2026-10-29.)
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
        // the variant arm's carrier cast over a plain wire value
        SqlExpr plain = new SqlExpr.Column("t", "c");
        assertInstanceOf(SqlExpr.Cast.class, CastPolicy.lower(
                anyConformance(Multiplicity.Bounded.ONE), plain, false, false));
        assertInstanceOf(SqlExpr.Cast.class, CastPolicy.lower(
                anyConformance(Multiplicity.Bounded.ONE), plain, true, false));
    }
}
