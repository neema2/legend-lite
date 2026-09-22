// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Structural analysis of literal-DECODE case chains (the mapping's enum
 * decode emission: every branch {@code col = literal -> 'NAME'}, null
 * terminal). Pure {@link SqlExpr} shape work — shared by the lowering's
 * toSourceValues inversion, the dialect's enum selector template, and
 * the plan text's computed-column typing.
 */
public final class DecodeShapes {

    private DecodeShapes() {
    }

    /** The chain's (condition, literal) branches; empty when {@code e}
     * is not a literal-decode case (nested via otherwise). Optional, not
     * a null sentinel: the consumers live in packages the null gate does
     * not check yet — the type forces handling there TODAY. */
    public static Optional<List<SqlExpr.Case.When>> flattenDecode(SqlExpr e) {
        List<SqlExpr.Case.When> out = new ArrayList<>();
        SqlExpr cur = e;
        while (cur instanceof SqlExpr.Case c) {
            for (var w : c.whens()) {
                if (!(w.then() instanceof SqlExpr.StringLit)) {
                    return Optional.empty();
                }
                out.add(w);
            }
            if (c.otherwise() == null) {
                return out.isEmpty() ? Optional.empty() : Optional.of(out);
            }
            cur = c.otherwise();
        }
        return cur instanceof SqlExpr.NullLit && !out.isEmpty()
                ? Optional.of(out) : Optional.empty();
    }

    /** The ONE source expression every branch condition compares
     * ({@code src = literal}, or an OR of such equalities — the
     * multi-source-value branch: {@code src = 'FTC' OR src = 'FTO'});
     * empty otherwise. */
    public static Optional<SqlExpr> sourceExpr(SqlExpr e) {
        Optional<List<SqlExpr.Case.When>> flat = flattenDecode(e);
        if (flat.isEmpty()) {
            return Optional.empty();
        }
        SqlExpr src = null;
        for (var w : flat.get()) {
            SqlExpr left = conditionSource(w.condition());
            if (left == null) {
                return Optional.empty();
            }
            if (src == null) {
                src = left;
            } else if (!src.equals(left)) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(src);
    }

    /** The shared LHS of an equality (or OR-tree of equalities over the
     * SAME lhs); null when the condition is any other shape. */
    private static @com.legend.base.Nullable SqlExpr conditionSource(SqlExpr cond) {
        if (cond instanceof SqlExpr.Call c && c.fn() == SqlFn.EQUAL
                && c.args().size() == 2) {
            return c.args().get(0);
        }
        if (cond instanceof SqlExpr.Call o && o.fn() == SqlFn.OR) {
            SqlExpr src = null;
            for (SqlExpr arm : o.args()) {
                SqlExpr a = conditionSource(arm);
                if (a == null || (src != null && !src.equals(a))) {
                    return null;
                }
                src = a;
            }
            return src;
        }
        return null;
    }

    /** {@link #sourceExpr} narrowed to a raw store COLUMN. */
    public static Optional<SqlExpr.Column> sourceColumn(SqlExpr e) {
        return sourceExpr(e)
                .filter(SqlExpr.Column.class::isInstance)
                .map(SqlExpr.Column.class::cast);
    }

    /** {@code e} with every interior literal-decode chain replaced by
     * its source column; {@code e} itself when nothing rewrites. */
    public static SqlExpr stripDecodes(SqlExpr e) {
        Optional<SqlExpr.Column> src = sourceColumn(e);
        if (src.isPresent()) {
            return src.get();
        }
        switch (e) {
            case SqlExpr.Call c -> {
                List<SqlExpr> args = new ArrayList<>();
                boolean ch = false;
                for (SqlExpr a : c.args()) {
                    SqlExpr r = stripDecodes(a);
                    ch |= r != a;
                    args.add(r);
                }
                return ch ? new SqlExpr.Call(c.fn(), args) : e;
            }
            case SqlExpr.Case cs -> {
                List<SqlExpr.Case.When> ws = new ArrayList<>();
                boolean ch = false;
                for (var w : cs.whens()) {
                    SqlExpr cnd = stripDecodes(w.condition());
                    SqlExpr th = stripDecodes(w.then());
                    ch |= cnd != w.condition() || th != w.then();
                    ws.add(new SqlExpr.Case.When(cnd, th));
                }
                SqlExpr ow = cs.otherwise() == null ? null
                        : stripDecodes(cs.otherwise());
                ch |= ow != cs.otherwise();
                return ch ? new SqlExpr.Case(ws, ow) : e;
            }
            case SqlExpr.Cast ct -> {
                SqlExpr v = stripDecodes(ct.value());
                return v != ct.value()
                        ? new SqlExpr.Cast(v, ct.target(), ct.conform()) : e;
            }
            case SqlExpr.Group g -> {
                SqlExpr i = stripDecodes(g.inner());
                return i != g.inner() ? new SqlExpr.Group(i) : e;
            }
            default -> {
                return e;
            }
        }
    }

}
