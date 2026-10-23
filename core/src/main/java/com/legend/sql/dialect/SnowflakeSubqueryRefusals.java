// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.HashSet;
import java.util.Set;

/**
 * The subquery shapes Snowflake cannot evaluate that the passes before this one leave, refused by name rather than
 * sent (each "Unsupported subquery type cannot be evaluated" on the IQ scale program's Snowflake lane, 2026-10-23):
 * <ul>
 *   <li>the island's two-valued NOT IN or ALL ({@link NotExistsAsAll}'s form) anywhere but a WHERE's conjunct, where
 *       Snowflake's ALL would answer an unknown the NOT EXISTS answers false for ({@code (x NOT IN (...)) IS
 *       UNKNOWN});</li>
 *   <li>a scalar subquery that reads a derived table its correlation sorts and limits (SQL Anywhere's {@code (SELECT
 *       TOP 1 ... WHERE u.t_id = t.id ORDER BY ...)}).</li>
 * </ul>
 */
final class SnowflakeSubqueryRefusals extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.Call not && not.fn() == SqlFn.NOT && not.args().size() == 1
                && NotExistsAsAll.isTwoValuedQuantified(not)) {
            throw new DialectCapability("a NOT IN or ALL whose unknown is read (here, not as a WHERE's condition)"
                    + " reached Snowflake, which evaluates the NOT EXISTS it is written as only there");
        }
        if (e instanceof SqlExpr.ScalarSubquery s && s.subquery() instanceof SqlSelect q
                && q.from() instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect in
                && (in.limit() != null || in.offset() != null) && correlated(in)) {
            throw new DialectCapability("a correlated scalar subquery that sorts and limits its rows (TOP in a"
                    + " subquery) reached Snowflake, which evaluates no correlated subquery of a row limit");
        }
        return e;
    }

    /** Whether {@code q}'s WHERE reads a column of a table its FROM does not name. */
    private static boolean correlated(SqlSelect q) {
        if (q.where() == null) {
            return false;
        }
        Set<String> own = new HashSet<>();
        aliases(q.from(), own);
        boolean[] found = {false};
        new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr c) {
                found[0] |= c instanceof SqlExpr.Column col && col.table() != null && !own.contains(col.table());
                return c;
            }

            SqlExpr run(SqlExpr root) {
                return rewriteExpr(root);
            }
        }.run(q.where());
        return found[0];
    }

    private static void aliases(SqlSource s, Set<String> out) {
        if (s instanceof SqlSource.Join j) {
            aliases(j.left(), out);
            aliases(j.right(), out);
        } else if (!(s instanceof SqlSource.Dual) && s.alias() != null) {
            out.add(s.alias());
        }
    }
}
