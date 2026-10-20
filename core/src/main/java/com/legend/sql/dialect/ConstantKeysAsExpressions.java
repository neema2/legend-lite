// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.List;

/**
 * A constant GROUP BY or ORDER BY key renders as a typed expression, {@code CAST(<lit> AS <type>)}
 * (found driving DataCube Live on Postgres, 2026-10-02: its root row groups by {@code '[ROOT]'}).
 *
 * <p>Postgres reads a bare constant in those clauses by SQL-92's rule: an integer is a column
 * POSITION ({@code GROUP BY 1} groups by the first output, silently another query), and any other
 * constant is refused ("non-integer constant in GROUP BY"). A cast is an expression, so the key
 * means what the plan says: one group over the input, and none over an empty input, as DuckDB
 * and H2 answer a constant key (measured on PostgreSQL 17). A NULL key, which has no type of its
 * own, is grouped by as an integer's NULL: every constant key is one group alike (IQ island,
 * 2026-10-17: IQ's {@code GROUP BY NULL}).
 *
 * <p>DuckDB reads an integer GROUP BY key as a position too ({@code GROUP BY 2} names the second
 * output: IQ island, 2026-10-17, IQ's {@code GROUP BY 2}, a constant there): its dialect runs
 * this pass over GROUP BY's integers alone ({@link #groupByOnly}), where any other constant key and
 * its ORDER BY keep what they say.
 */
final class ConstantKeysAsExpressions extends SqlRewriter {

    private final boolean orderToo;
    /** The database a refusal names (2026-10-18, IQ landing 5's review: DuckDB's was named Postgres). */
    private final String database;

    ConstantKeysAsExpressions() {
        this(true, "Postgres");
    }

    private ConstantKeysAsExpressions(boolean orderToo, String database) {
        this.orderToo = orderToo;
        this.database = database;
    }

    /** The pass over GROUP BY keys alone, on DuckDB. */
    static ConstantKeysAsExpressions groupByOnly() {
        return new ConstantKeysAsExpressions(false, "DuckDB");
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        List<SqlExpr> group = s.groupBy();
        List<SqlExpr> g = new ArrayList<>(group.size());
        boolean changed = false;
        for (SqlExpr e : group) {
            // DuckDB's mode: only an integer is a position there; any other constant key stays as it is
            SqlExpr k = orderToo || e instanceof SqlExpr.IntLit ? typedKey(e, "GROUP BY") : e;
            changed |= k != e;
            g.add(k);
        }
        List<SqlSelect.SortKey> ob = new ArrayList<>(s.orderBy().size());
        for (SqlSelect.SortKey k : s.orderBy()) {
            SqlExpr e = orderToo ? typedKey(k.expr(), "ORDER BY") : k.expr();
            changed |= e != k.expr();
            ob.add(e == k.expr() ? k : new SqlSelect.SortKey(e, k.ascending(), k.nullOrder(), k.outputName()));
        }
        return !changed ? s : new SqlSelect(s.projections(), s.distinct(), s.from(), s.where(), g,
                s.having(), s.qualify(), ob, s.limit(), s.offset(), s.outputs());
    }

    /** A literal key as {@code CAST(<lit> AS <its own type>)}; any other key as it is. */
    private SqlExpr typedKey(SqlExpr e, String clause) {
        if (e instanceof SqlExpr.NullLit && clause.equals("GROUP BY")) {
            return new SqlExpr.Cast(e, SqlType.Scalar.INTEGER);
        }
        if (e instanceof SqlExpr.NullLit) {
            throw new DialectCapability("a NULL " + clause + " key reached " + database + ", which"
                    + " refuses a constant key and has no type to cast it to");
        }
        // DATE '…' and TIMESTAMP '…' are typed literals already, which Postgres reads as
        // expressions
        boolean constant = e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit
                || e instanceof SqlExpr.FloatLit || e instanceof SqlExpr.DecimalLit
                || e instanceof SqlExpr.BoolLit;
        if (!constant) return e;
        if (!(e.type() instanceof TypeFact.Typed typed)) {
            throw new DialectCapability("an untyped constant " + clause + " key reached " + database + ", which"
                    + " reads a constant key otherwise and has no type to cast it to");
        }
        return new SqlExpr.Cast(e, typed.type());
    }
}
