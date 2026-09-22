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
 * {@code exec.PureAsserts.repr}, and the {@code format} {@code %r}
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
        if (t == Type.Primitive.DATE || t == Type.Primitive.STRICT_DATE
                || t == Type.Primitive.DATE_TIME) {
            return Scalars.cat(new SqlExpr.StringLit("%"),
                    Scalars.pureToString(t, x));
        }
        if (t == Type.Primitive.DECIMAL) {
            return Scalars.cat(Scalars.pureToString(t, x),
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
