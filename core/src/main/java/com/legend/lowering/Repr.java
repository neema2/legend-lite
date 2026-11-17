// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

/**
 * {@code toRepresentation}'s SQL spelling (One-Platform Plan Phase 4 —
 * the platform native's lowering owner; the host owner is
 * {@code testexecutionplan.Verdicts.repr}, and the {@code format} {@code %r}
 * directive rides the same emission): strings quote and
 * backslash-escape, temporals take the {@code %} literal prefix,
 * Decimal the {@code D} suffix; everything else is the toString
 * spelling.
 */
final class Repr {

    private Repr() {
    }

    /** The TYPE-directed spelling (the {@code toRepresentation} native's
     * rule). */
    static SqlExpr of(Type t, SqlExpr x) {
        if (t == Type.Primitive.STRING) {
            // CAST first: identity for a real string, and keeps a DEAD
            // size-1 branch (assertEquals' %r fast path over a known-many
            // collection) VALID SQL — DuckDB type-checks dead branches
            SqlExpr sx = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
            SqlExpr esc = SqlExpr.Call.of(SqlFn.REPLACE,
                    SqlExpr.Call.of(SqlFn.REPLACE,
                            SqlExpr.Call.of(SqlFn.REPLACE, sx,
                                    new SqlExpr.StringLit("\\"),
                                    new SqlExpr.StringLit("\\\\")),
                            new SqlExpr.StringLit("'"),
                            new SqlExpr.StringLit("\\'")),
                    new SqlExpr.StringLit("\n"),
                    new SqlExpr.StringLit("\\n"));
            return Scalars.cat(new SqlExpr.StringLit("'"),
                    Scalars.cat(esc, new SqlExpr.StringLit("'")));
        }
        if (t == Type.Primitive.DATE_TIME && x.type() instanceof com.legend.sql.TypeFact.Typed ty
                && (ty.type() == SqlType.Scalar.TIMESTAMP || ty.type() == SqlType.Scalar.TIMESTAMPTZ)) {
            // a timestamp as every output of one writes it (LiteralSpelling.dateTimeText: T, the fraction's digits,
            // +0000) -- upstream's toRepresentation writes '%2020-01-01T10:00:00+0000' (the engine runner, 2026-11-04,
            // the assert bar), where the VARCHAR cast wrote '2020-01-01 10:00:00'. A value carried another way (a
            // literal's own text, holding a written precision a timestamp cannot) keeps its text below
            return Scalars.cat(new SqlExpr.StringLit("%"), LiteralSpelling.dateTimeText(x));
        }
        if (t == Type.Primitive.DATE || t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE_TIME) {
            return Scalars.cat(new SqlExpr.StringLit("%"),
                    Scalars.pureToString(t, x));
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            // a declared-precision Decimal IS a Decimal: its representation carries the D too (2026-11-04, the
            // asserts-as-plan-nodes program: a literal 3.0D is typed Decimal(2,1), and the database judge's
            // per-element message printed 3.0 where upstream prints 3.0D)
            return Scalars.cat(Scalars.pureToString(Type.Primitive.DECIMAL, x),
                    new SqlExpr.StringLit("D"));
        }
        if (t == Type.Primitive.FLOAT) {
            // legend-pure's print form, the judges' %r of a Float (phase D's D1.2, 2026-11-15: the product's text of a
            // Float is lite's six decimals, FLOAT_TO_TEXT; an assert's message prints the value as the host judge
            // does, judge parity G2); a value the store holds as an integer its digits, as before
            return LiteralSpelling.heldAsInteger(x) ? new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                    : SqlExpr.Call.of(SqlFn.FLOAT_PRINT, x);
        }
        // numbers, booleans, and the Any carrier: the toString spelling
        // IS the representation
        return Scalars.pureToString(t, x);
    }

    /** A USER's {@code toRepresentation} of a value: {@link #of}, legend-pure's text, marked
     *  {@link SqlFn#REPRESENTATION_TEXT} -- the product refuses it by name (the user's ruling of 2026-11-16,
     *  docs/records/total-wall-edge-program-2026-11-16.md §10 item 8: "refuse in product but not in compatibility"),
     *  the compatibility edge writes it as it is. {@code format}'s {@code %r} is not marked yet: the platform's own
     *  asserts write their messages with it (the prelude's assertNotEq), until the judges' messages are Java at the
     *  edge (Phase 4b); a judge's message writes {@link #of} unmarked. */
    static SqlExpr userCall(Type t, SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.REPRESENTATION_TEXT, of(t, x), new SqlExpr.StringLit("toRepresentation"));
    }

    /** The {@code %r} directive's arm ({@code format('%r', x)}): the
     * NODE-directed spelling — a date literal prints its {@code %}
     * form; strings quote with backslash-escapes. */
    static SqlExpr of(@com.legend.base.Nullable TypedSpec typed, SqlExpr e) {
        if (typed != null) {
            SqlExpr dp = Scalars.dateReprOf(typed, e);
            if (dp != null) {
                return Scalars.cat(new SqlExpr.StringLit("%"), dp);
            }
            if (typed.info().type() instanceof Type.Primitive p && p != Type.Primitive.STRING) {
                // a number, a Boolean, a Decimal: pure's representation by its type (format('%r', [1]) is '1', a
                // Decimal's '3.0D' -- until 2026-11-04 every %r quoted as a String: the asserts-as-plan-nodes program
                // reached it through assertNotEq's message, '%r should not equal %r')
                return of(p, e);
            }
        }
        // CAST first: identity for a real string; keeps a DEAD branch
        // (assertEquals' %r fast path inlined over a known-many
        // collection) VALID SQL — DuckDB type-checks dead branches
        // (Phase 4 channel B surfaced this on 169 essential tests)
        SqlExpr se = new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR, SqlExpr.Reason.PRINT);
        SqlExpr escaped = SqlExpr.Call.of(SqlFn.REPLACE,
                SqlExpr.Call.of(SqlFn.REPLACE, se,
                        new SqlExpr.StringLit("\\"),
                        new SqlExpr.StringLit("\\\\")),
                new SqlExpr.StringLit("'"), new SqlExpr.StringLit("\\'"));
        return Scalars.cat(new SqlExpr.StringLit("'"), escaped,
                new SqlExpr.StringLit("'"));
    }

    /** A value's text in an assert's message under LITE'S OWN RULES (the user's judge model, 2026-11-17: the host
     *  judge's {@code testexec.LiteWords.value} in SQL, the two judges one text): a String quoted JSON-style (a
     *  backslash, a quote, a newline, a tab and a carriage return escaped); a Float its product text (S311,
     *  {@code FLOAT_TO_TEXT}); a DateTime ISO 8601 in UTC, six fraction digits and Z (S312, {@code DATETIME_TO_TEXT});
     *  a date its day; a Decimal its plain digits ({@code DECIMAL_TO_TEXT}); every other primitive its toString. */
    static SqlExpr lite(Type t, SqlExpr x) {
        if (t == Type.Primitive.STRING) {
            SqlExpr esc = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
            String[][] pairs = {{"\\", "\\\\"}, {"\"", "\\\""}, {"\n", "\\n"}, {"\t", "\\t"},
                    {"\r", "\\r"}};
            for (String[] p : pairs) {
                esc = SqlExpr.Call.of(SqlFn.REPLACE, esc, new SqlExpr.StringLit(p[0]), new SqlExpr.StringLit(p[1]));
            }
            return Scalars.cat(new SqlExpr.StringLit("\""), Scalars.cat(esc, new SqlExpr.StringLit("\"")));
        }
        if (t == Type.Primitive.DATE_TIME) {
            // a value carried as its text (a literal's channel) read as the timestamp it names first
            boolean timestamp = x.type() instanceof com.legend.sql.TypeFact.Typed ty
                    && (ty.type() == SqlType.Scalar.TIMESTAMP || ty.type() == SqlType.Scalar.TIMESTAMPTZ);
            return SqlExpr.Call.of(SqlFn.DATETIME_TO_TEXT, timestamp ? x
                    : new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE));
        }
        if (t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE) {
            return new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        }
        if (t instanceof Type.PrecisionDecimal) {
            return Scalars.pureToString(Type.Primitive.DECIMAL, x);
        }
        return Scalars.pureToString(t, x);
    }
}
