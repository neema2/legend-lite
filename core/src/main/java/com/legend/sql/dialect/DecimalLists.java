// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;
import java.util.ArrayList;
import java.util.List;

/**
 * A LIST OF DECIMALS AT ONE SCALE, the widest (register S400, the user's ruling of 2026-11-22: "I think the right way in
 * product is that they all widen to max one"): each element of a list literal the plan types as a Decimal list is cast
 * to the list's element type ({@code SqlTyping}'s union: the widest integer digits and the widest scale), so every
 * database writes one scale -- one that unifies a list's DECIMALs itself (DuckDB) and one that keeps each element's (a
 * JSON array's numbers, Postgres's numeric[]) alike: {@code [1.0D, 10.125D]} is {@code [1.000, 10.125]} on every
 * database. Exact: a value is never rounded; a widening past 38 digits is refused by name. A pass of the base renderer,
 * run before the dialect's own.
 */
final class DecimalLists {

    private DecimalLists() {
    }

    /** The base renderer's pass: each Decimal list literal's elements at the list's one type. */
    static final class Widening extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (!(e instanceof SqlExpr.ArrayLit a) || !(a.type() instanceof com.legend.sql.TypeFact.Typed t)
                    || !(t.type() instanceof SqlType.Array arr) || !(arr.element() instanceof SqlType.Decimal d)) {
                return e;
            }
            int whole = 0;
            int scale = 0;
            boolean mixed = false;
            for (SqlExpr x : a.elements()) {
                if (x.type() instanceof com.legend.sql.TypeFact.Typed xt && xt.type() instanceof SqlType.Decimal xd) {
                    whole = Math.max(whole, xd.precision() - xd.scale());
                    scale = Math.max(scale, xd.scale());
                    mixed |= !xd.equals(d);
                }
            }
            if (!mixed) {
                return e;
            }
            if (whole + scale > 38) {
                throw new com.legend.sql.DialectCapability("a list of Decimals whose one type needs " + (whole + scale)
                        + " digits (" + whole + " before the point, " + scale + " after); a DECIMAL holds 38: refused,"
                        + " never rounded");
            }
            List<SqlExpr> widened = new ArrayList<>(a.elements().size());
            for (SqlExpr x : a.elements()) {
                widened.add(x.type() instanceof com.legend.sql.TypeFact.Typed xt && xt.type().equals(d) ? x
                        : new SqlExpr.Cast(x, d, SqlExpr.Reason.DECLARED));
            }
            return new SqlExpr.ArrayLit(widened, a.type());
        }
    }
}
