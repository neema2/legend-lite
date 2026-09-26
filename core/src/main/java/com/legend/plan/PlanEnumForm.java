// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's PLAN-TEXT form of enum-mapped columns. Execution decodes
 * enum source values in SQL (the if-chain CASE — Java orchestrates, the
 * database executes), but the engine's relational plan keeps the RAW
 * store column and pushes the decode host-side: the TDS tuple carries
 * the enumeration-mapping id, projections spell the raw column, and an
 * enum-typed plan PARAMETER in a filter spells the freemarker
 * {@code equalEnumOperationSelector} template over the raw column.
 * This pass rewrites the lowered SQL IR into that form — plan-surface
 * only, never on the execution path.
 */
public final class PlanEnumForm {

    private PlanEnumForm() {
    }

    /** {@code rt}: the terminal's relation type — its enum-typed columns
     * name which projections reduce to their raw store column. */
    public static SqlSelect apply(SqlSelect sel, Type.RelationType rt) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        boolean changed = false;
        for (int i = 0; i < sel.projections().size(); i++) {
            SqlSelect.Projection p = sel.projections().get(i);
            boolean isEnum = i < rt.columns().size()
                    && rt.columns().get(i).type() instanceof Type.EnumType;
            SqlExpr.Column raw;
            // only the DECODE CASE reduces — computed enum projections
            // (monthOf's formatdatetime) keep their expression
            if (isEnum && p.expr() instanceof SqlExpr.Case
                    && (raw = singleColumnIn(p.expr())) != null) {
                ps.add(new SqlSelect.Projection(raw, p.outputName(),
                        p.out()));
                changed = true;
            } else if (isEnum && com.legend.sql.DecodeShapes
                    .stripDecodes(p.expr()) instanceof SqlExpr st
                    && st != p.expr()) {
                // computed enum projection (e.g. an if over the enum
                // value): interior decode chains reduce to their raw
                // store column — the plan keeps enum columns raw
                ps.add(new SqlSelect.Projection(st, p.outputName(),
                        p.out()));
                changed = true;
            } else {
                ps.add(p);
            }
        }
        SqlExpr where = sel.where() == null ? null
                : rewritePredicate(sel.where());
        changed |= where != sel.where();
        if (!changed) {
            return sel;
        }
        return new SqlSelect(ps, sel.distinct(), sel.from(), where,
                sel.groupBy(), sel.having(), sel.qualify(), sel.orderBy(),
                sel.limit(), sel.offset(), sel.outputs());
    }

    /** Enum-parameter comparisons collapse to ONE canonical shape —
     * {@code rawColumn = param} (the selector template spells both the
     * {@code =} and {@code in} arms); {@code !=}/{@code not in} wrap it
     * in NOT. */
    private static SqlExpr rewritePredicate(SqlExpr e) {
        if (!(e instanceof SqlExpr.Call c)) {
            return e instanceof SqlExpr.Group g
                    ? regroup(g, rewritePredicate(g.inner())) : e;
        }
        switch (c.fn()) {
            case AND, OR, NOT -> {
                List<SqlExpr> args = new ArrayList<>();
                boolean changed = false;
                for (SqlExpr a : c.args()) {
                    SqlExpr r = rewritePredicate(a);
                    changed |= r != a;
                    args.add(r);
                }
                // pure != three-valued lowering ORs NULL-keeper guards
                // beside not(pred) — engine text spells the bare selector
                if (c.fn() == SqlFn.OR) {
                    SqlExpr.PlanParam p = args.stream()
                            .map(PlanEnumForm::selectorParam)
                            .filter(java.util.Objects::nonNull)
                            .findFirst().orElse(null);
                    if (p != null) {
                        List<SqlExpr> kept = new ArrayList<>();
                        for (SqlExpr a : args) {
                            if (selectorParam(a) == null
                                    && nullGuardOver(a, p)) {
                                changed = true;
                            } else {
                                kept.add(a);
                            }
                        }
                        if (kept.size() == 1) {
                            return kept.get(0);
                        }
                        args = kept;
                    }
                }
                return changed ? new SqlExpr.Call(c.fn(), args) : c;
            }
            case EQUAL, NOT_EQUAL -> {
                SqlExpr.PlanParam p = enumParam(c.args().get(1));
                SqlExpr other = c.args().get(0);
                if (p == null) {
                    p = enumParam(other);
                    other = c.args().get(1);
                }
                if (p == null) {
                    return c;
                }
                SqlExpr base = new SqlExpr.Call(SqlFn.EQUAL,
                        List.of(reduce(other), p));
                return c.fn() == SqlFn.EQUAL ? base
                        : new SqlExpr.Call(SqlFn.NOT, List.of(base));
            }
            case IN -> {
                if (c.args().size() == 2
                        && enumParam(c.args().get(1)) != null) {
                    return new SqlExpr.Call(SqlFn.EQUAL, List.of(
                            reduce(c.args().get(0)), c.args().get(1)));
                }
                return c;
            }
            case COALESCE -> {
                // execution null-guard (COALESCE(pred, false)) — engine
                // text drops it when the guarded predicate is the enum
                // selector shape
                SqlExpr r = rewritePredicate(c.args().get(0));
                if (r != c.args().get(0) && r instanceof SqlExpr.Call rc
                        && rc.fn() == SqlFn.EQUAL
                        && enumParam(rc.args().get(1)) != null) {
                    return r;
                }
                return c;
            }
            default -> {
                return c;
            }
        }
    }

    /** The enum parameter when {@code e} is the rewritten selector
     * shape ({@code col = p} or {@code not(col = p)}), else null. */
    private static SqlExpr.@com.legend.base.Nullable PlanParam selectorParam(SqlExpr e) {
        if (e instanceof SqlExpr.Call n && n.fn() == SqlFn.NOT
                && n.args().size() == 1) {
            return selectorParam(n.args().get(0));
        }
        return e instanceof SqlExpr.Call c && c.fn() == SqlFn.EQUAL
                && c.args().size() == 2
                ? enumParam(c.args().get(1)) : null;
    }

    /** A three-valued NULL-keeper disjunct over the parameter — ANDs of
     * is-null / is-not-null tests on the param or its decode CASE. */
    private static boolean nullGuardOver(SqlExpr e, SqlExpr.PlanParam p) {
        return switch (e) {
            case SqlExpr.Group g -> nullGuardOver(g.inner(), p);
            case SqlExpr.Call c when c.fn() == SqlFn.AND ->
                    c.args().stream().allMatch(x -> nullGuardOver(x, p));
            case SqlExpr.Call c when c.fn() == SqlFn.IS_NULL
                    || c.fn() == SqlFn.IS_NOT_NULL -> {
                SqlExpr a = c.args().get(0);
                yield a instanceof SqlExpr.PlanParam pp
                        ? pp.name().equals(p.name())
                        : a instanceof SqlExpr.Case
                                || a instanceof SqlExpr.Column;
            }
            default -> false;
        };
    }

    private static SqlExpr regroup(SqlExpr.Group g, SqlExpr inner) {
        return inner == g.inner() ? g : new SqlExpr.Group(inner);
    }

    private static SqlExpr.@com.legend.base.Nullable PlanParam enumParam(SqlExpr e) {
        return e instanceof SqlExpr.PlanParam p && p.enumMapFn() != null
                ? p : null;
    }

    /** An enum-decode CASE reduced to its raw store column (comparison
     * against the parameter happens over source values). */
    private static SqlExpr reduce(SqlExpr e) {
        if (e instanceof SqlExpr.Case) {
            SqlExpr.Column raw = singleColumnIn(e);
            if (raw != null) {
                return raw;
            }
        }
        return e;
    }

    /** The single distinct Column reference inside an expression, or
     * null when it reads zero or several. */
    static SqlExpr.@com.legend.base.Nullable Column singleColumnIn(SqlExpr e) {
        java.util.LinkedHashSet<String> keys = new java.util.LinkedHashSet<>();
        List<SqlExpr.Column> found = new ArrayList<>();
        collectColumns(e, keys, found);
        return keys.size() == 1 ? found.get(0) : null;
    }

    private static void collectColumns(SqlExpr e,
            java.util.Set<String> keys, List<SqlExpr.Column> out) {
        if (e instanceof SqlExpr.Column c) {
            if (keys.add(c.table() + "." + c.name())) {
                out.add(c);
            }
            return;
        }
        switch (e) {
            case SqlExpr.Call c2 -> c2.args().forEach(x ->
                    collectColumns(x, keys, out));
            case SqlExpr.Case cs -> {
                cs.whens().forEach(w -> {
                    collectColumns(w.condition(), keys, out);
                    collectColumns(w.then(), keys, out);
                });
                if (cs.otherwise() != null) {
                    collectColumns(cs.otherwise(), keys, out);
                }
            }
            default -> e.children().forEach(x -> collectColumns(x, keys, out));
        }
    }
}
