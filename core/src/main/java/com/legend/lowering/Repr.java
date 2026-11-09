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
            SqlExpr sx = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR);
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
        // numbers, booleans, and the Any carrier: the toString spelling
        // IS the representation
        return Scalars.pureToString(t, x);
    }

    /** The {@code %r} directive's arm ({@code format('%r', x)}): the
     * NODE-directed spelling — a date literal prints its {@code %}
     * form; strings quote with backslash-escapes. */
    static SqlExpr of(@com.legend.base.Nullable TypedSpec typed, SqlExpr e) {
        if (typed != null) {
            SqlExpr dp = Scalars.datePrintOf(typed, e);
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
        SqlExpr se = new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR);
        SqlExpr escaped = SqlExpr.Call.of(SqlFn.REPLACE,
                SqlExpr.Call.of(SqlFn.REPLACE, se,
                        new SqlExpr.StringLit("\\"),
                        new SqlExpr.StringLit("\\\\")),
                new SqlExpr.StringLit("'"), new SqlExpr.StringLit("\\'"));
        return Scalars.cat(new SqlExpr.StringLit("'"), escaped,
                new SqlExpr.StringLit("'"));
    }
}
