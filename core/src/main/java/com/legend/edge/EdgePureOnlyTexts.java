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
        if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
            // legend-pure's print form, the judges' %r of a Float; a value the store holds as an integer its digits
            return heldAsInteger(x)
                    ? new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                    : floatPrint(x);
        }
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

    // ---- legend-pure's print of a Float, composed here from the product's own nodes (the edition program's step 8,
    // ---- slice 1, 2026-11-26: moved unchanged from the product's sql.dialect.FloatShortestText, whose node
    // ---- FLOAT_SHORTEST_TEXT only this class wrote; the node, its expansion pass and each dialect's arm are deleted)

    /**
     * legend-pure's print of the Float {@code x}, as SQL over the database's text of it ({@code CAST(x AS VARCHAR)}):
     * the database's shortest round-trip text ('1.5', '2.0') except where it chooses exponent notation -- those
     * re-render plain, a whole value below 1e38 through HUGEINT with {@code .0}, a fractional one from 1e-17 below
     * 2^53 through a DECIMAL(38,18) cast with its trailing zeros trimmed. Outside those windows the database's own
     * exponent form stays. NaN and the infinities by Java's names. Each database's own casts and functions, the
     * product's nodes as its dialects write them: a database whose text of a double is long writes it once per read
     * here (Snowflake and BigQuery read it once while the product's dialects spelled the node).
     */
    private static SqlExpr floatPrint(SqlExpr x) {
        SqlExpr s = new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
        // the exponent marker in either case: DuckDB and Postgres write 2.5e-07, H2 1.5E15
        SqlExpr hasExp = SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, SqlExpr.Call.of(SqlFn.LOWER, s), new SqlExpr.StringLit("e")),
                new SqlExpr.IntLit(0));
        // fraction-free values render through HUGEINT, exact plain digits for the whole [1e16, 1e38) band where the
        // DECIMAL(38,18) cast is not exact; every double from 2^53 is fraction-free
        SqlExpr fractionFree = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, x, SqlExpr.Call.of(SqlFn.FLOOR_RAW, x)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e38)));
        // the DECIMAL path only where the scale-18 cast is exact for short-decimal values: [1e-17, 2^53)
        SqlExpr inRange = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e-17)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(9.007199254740992e15)));
        // each cast reads the value only where its branch is taken (NULL elsewhere): a database that computes every
        // branch of a CASE (ClickHouse) otherwise failed the cast of a value the branch never takes
        SqlExpr intPlain = SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.Cast(new SqlExpr.Cast(onlyWhere(fractionFree, x), com.legend.sql.SqlType.Scalar.HUGEINT,
                        SqlExpr.Reason.PRINT), com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT),
                new SqlExpr.StringLit(".0"));
        SqlExpr plain = SqlExpr.Call.of(SqlFn.RTRIM,
                new SqlExpr.Cast(new SqlExpr.Cast(onlyWhere(inRange, x), new com.legend.sql.SqlType.Decimal(38, 18),
                        SqlExpr.Reason.PRINT), com.legend.sql.SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT),
                new SqlExpr.StringLit("0"));
        SqlExpr fixed = new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.ENDS_WITH, plain, new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.CONCAT, plain, new SqlExpr.StringLit("0")))),
                plain, SqlExpr.Reason.PRINT);
        // a whole value the database writes without a fraction (Postgres's double 3, an integer slot a Float declares)
        // takes .0: a Float's text always has a fraction
        SqlExpr bare = SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, hasExp),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, s, new SqlExpr.StringLit(".")),
                        new SqlExpr.IntLit(0)));
        // a double that is no number by Java's names, from the database's own (DuckDB writes nan, inf and -inf, H2 and
        // Postgres Java's): the spelled tokens compared, nothing computed
        SqlExpr lower = SqlExpr.Call.of(SqlFn.LOWER, s);
        java.util.List<SqlExpr.Case.When> whens = new java.util.ArrayList<>();
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IN, lower, text("nan"), text("-nan")), text("NaN")));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IN, lower, text("inf"), text("+inf"), text("infinity")),
                text("Infinity")));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IN, lower, text("-inf"), text("-infinity")),
                text("-Infinity")));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, hasExp, fractionFree), intPlain));
        whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, hasExp, inRange), fixed));
        whens.add(new SqlExpr.Case.When(bare, SqlExpr.Call.of(SqlFn.CONCAT, s, new SqlExpr.StringLit(".0"))));
        return new SqlExpr.Case(whens, s, SqlExpr.Reason.PRINT);
    }

    /** {@code x} where {@code guard} holds, else NULL. */
    private static SqlExpr onlyWhere(SqlExpr guard, SqlExpr x) {
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(guard, x)), null, SqlExpr.Reason.PRINT);
    }

    private static SqlExpr text(String s) {
        return new SqlExpr.StringLit(s);
    }

    /** Whether {@code x} is a Float the STORE holds as an integer: a physical column the store declares integral
     *  (legend-engine's test {@code tradeTable.quantity} INT under the validation showcase's Float), read off the scan's
     *  own column (origin PHYSICAL), never inferred. legend-pure's print of it is the integer the database holds, as
     *  legend-engine's cast of the column writes it. Moved here 2026-11-24 from the lowering ({@code ReprParts},
     *  {@code LiteralSpelling}): since B2a the product prints every Float by one rule, so this is the edge's question
     *  alone (B2a's end design audit, 2026-11-24). */
    private static boolean heldAsInteger(SqlExpr x) {
        return x instanceof SqlExpr.Column c && c.type() instanceof com.legend.sql.TypeFact.Typed t
                && (c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL
                        || c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL_QUOTED)
                && (t.type() == com.legend.sql.SqlType.Scalar.INTEGER || t.type() == com.legend.sql.SqlType.Scalar.BIGINT
                        || t.type() == com.legend.sql.SqlType.Scalar.HUGEINT);
    }
}
