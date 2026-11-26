// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * THE TEXTS ONLY LEGEND-PURE WRITES, COMPOSED AT LOWERING (the separation census's B8 (i), 2026-11-17): the edge's
 * answer to the plan style's {@link com.legend.lowering.PureOnlyTexts} facet, each text built from the product's own
 * nodes -- what the edge's first spelling pass (PurePrintForms) wrote for the lowering's marker node until then, so
 * the SQL is byte-identical.
 */
final class EdgePureOnlyTexts implements com.legend.lowering.PureOnlyTexts {

    static final EdgePureOnlyTexts INSTANCE = new EdgePureOnlyTexts();

    private EdgePureOnlyTexts() {
    }

    /** {@code format}'s {@code %0<w>d} as legend-pure writes it ({@code -3} at width 5 is {@code -00003}): the digits
     *  padded, then signed -- printf's width would count the sign (moved here from the lowering, Scalars, 2026-11-15:
     *  phase D's D1.3; from the edge's spelling pass to its lowering facet, B8 (i), 2026-11-17). */
    @Override
    public SqlExpr formatPadded(SqlExpr x, SqlExpr.IntLit width) {
        SqlExpr padded = SqlExpr.Call.of(SqlFn.LPAD,
                new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.ABS, x), com.legend.sql.SqlType.Scalar.VARCHAR,
                        SqlExpr.Reason.PRINT),
                width, new SqlExpr.StringLit("0"));
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("-"), padded))),
                padded, SqlExpr.Reason.PRINT);
    }

    /** An instance's {@code toString}: legend-pure's anonymous-instance name, {@code @_} and the instance's identity
     *  (the edge's pass composed it from the INSTANCE_TEXT marker until B8, 2026-11-17). */
    @Override
    public SqlExpr instanceText(SqlExpr id) {
        return SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("@_"), id);
    }

    /** A user's {@code toRepresentation}: legend-pure's representation as the lowering writes it, as it is (the
     *  edge's pass unwrapped REPRESENTATION_TEXT to it until B8 (i), 2026-11-17). */
    @Override
    public SqlExpr representation(SqlExpr text) {
        return text;
    }

    // ---- legend-pure's representation (C5's step A, 2026-11-17: moved unchanged from the product's lowering.Repr;
    // ---- the product's own parts through lowering.ReprParts, the one facade)

    private static final com.legend.compiler.element.type.Type.Primitive STRING =
            com.legend.compiler.element.type.Type.Primitive.STRING;

    /** The TYPE-directed spelling (the {@code toRepresentation} native's rule): strings quote and backslash-escape,
     *  temporals take the {@code %} literal prefix, Decimal the {@code D} suffix; everything else is the toString
     *  spelling. */
    @Override
    public SqlExpr representationOf(com.legend.lowering.Styled styled, com.legend.compiler.element.type.Type t,
            SqlExpr x) {
        if (t == STRING) {
            // CAST first: identity for a real string, and keeps a DEAD size-1 branch (assertEquals' %r fast path over a
            // known-many collection) VALID SQL -- DuckDB type-checks dead branches
            SqlExpr sx = new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
            SqlExpr esc = SqlExpr.Call.of(SqlFn.REPLACE, SqlExpr.Call.of(SqlFn.REPLACE, SqlExpr.Call.of(SqlFn.REPLACE,
                    sx, new SqlExpr.StringLit("\\"), new SqlExpr.StringLit("\\\\")), new SqlExpr.StringLit("'"),
                    new SqlExpr.StringLit("\\'")), new SqlExpr.StringLit("\n"), new SqlExpr.StringLit("\\n"));
            return com.legend.lowering.ReprParts.cat(new SqlExpr.StringLit("'"),
                    com.legend.lowering.ReprParts.cat(esc, new SqlExpr.StringLit("'")));
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME
                && x.type() instanceof com.legend.sql.TypeFact.Typed ty
                && (ty.type() == com.legend.sql.SqlType.Scalar.TIMESTAMP
                        || ty.type() == com.legend.sql.SqlType.Scalar.TIMESTAMPTZ)) {
            // a timestamp as every output of one writes it -- upstream's toRepresentation writes
            // '%2020-01-01T10:00:00+0000' (the engine runner, 2026-11-04, the assert bar)
            return com.legend.lowering.ReprParts.cat(new SqlExpr.StringLit("%"), styled.dateTimes().dateTimeText(x, null));
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE
                || t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE
                || t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME) {
            return com.legend.lowering.ReprParts.cat(new SqlExpr.StringLit("%"),
                    com.legend.lowering.ReprParts.pureToString(styled, t, x));
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                || t instanceof com.legend.compiler.element.type.Type.PrecisionDecimal) {
            // a declared-precision Decimal IS a Decimal: its representation carries the D too
            return com.legend.lowering.ReprParts.cat(com.legend.lowering.ReprParts.pureToString(styled,
                    com.legend.compiler.element.type.Type.Primitive.DECIMAL, x), new SqlExpr.StringLit("D"));
        }
        // a Float (the edition program's step 8, 2026-11-26): the toString spelling, lite's own text of a Float, as
        // every text the program goes on computing with is (tenet 1 as the user amended it on 2026-11-25; the user's
        // ruling on the register row, 2026-11-26). Until then legend-pure's print composed in SQL (the node
        // FLOAT_SHORTEST_TEXT, deleted), and an integer-held Float column its digits
        // numbers, booleans, and the Any carrier: the toString spelling IS the representation
        return com.legend.lowering.ReprParts.pureToString(styled, t, x);
    }

    /** The NODE-directed spelling ({@code format('%r', x)}): a date literal prints its {@code %} form; strings quote
     *  with backslash-escapes. */
    @Override
    public SqlExpr formatR(com.legend.lowering.Styled styled,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedSpec typed, SqlExpr e) {
        if (typed != null) {
            SqlExpr dp = dateReprOf(styled, typed, e);
            if (dp != null) {
                return com.legend.lowering.ReprParts.cat(new SqlExpr.StringLit("%"), dp);
            }
            if (typed.info().type() instanceof com.legend.compiler.element.type.Type.Primitive p && p != STRING) {
                // a number, a Boolean, a Decimal: pure's representation by its type
                return representationOf(styled, p, e);
            }
        }
        // CAST first: identity for a real string; keeps a DEAD branch VALID SQL -- DuckDB type-checks dead branches
        SqlExpr se = new SqlExpr.Cast(e, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
        SqlExpr escaped = SqlExpr.Call.of(SqlFn.REPLACE, SqlExpr.Call.of(SqlFn.REPLACE, se,
                new SqlExpr.StringLit("\\"), new SqlExpr.StringLit("\\\\")), new SqlExpr.StringLit("'"),
                new SqlExpr.StringLit("\\'"));
        return com.legend.lowering.ReprParts.cat(new SqlExpr.StringLit("'"), escaped, new SqlExpr.StringLit("'"));
    }

    /** A date's representation, or null when not a date: a DateTime's the style's text ({@code Styled.dateTimes}),
     *  any other date literal's as written (moved with Repr from the lowering's {@code Scalars.dateReprOf}). */
    private static @com.legend.base.Nullable SqlExpr dateReprOf(com.legend.lowering.Styled styled,
            com.legend.compiler.spec.typed.TypedSpec typed, SqlExpr e) {
        if (typed.info().type() == com.legend.compiler.element.type.Type.Primitive.DATE_TIME) {
            return styled.dateTimes().dateTimeText(e,
                    typed instanceof com.legend.compiler.spec.typed.TypedCDate cd ? cd.value().canonicalText() : null);
        }
        return com.legend.lowering.ReprParts.dateLiteralText(typed);
    }
}
