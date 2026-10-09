// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;
import java.util.Optional;

/**
 * An enumeration parameter compared with a mapped column, in a statement that binds it (a lite plan's,
 * docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's decisions): the database translates the parameter's NAME at
 * that place, through a value table of that place's decode —
 * {@code col IN (SELECT code FROM (VALUES ('A', 'ACTIVE'), ...) AS _enum(code, name) WHERE name = ?)} — so the
 * comparison reads the stored column and keeps its index, and a name stored under two codes matches both (measured:
 * docs/execution-plan-boundary-2026-10-05/probes/enum-index-results.txt). The decoded comparison the lowering writes
 * ({@code CASE WHEN col = 'A' ... END = ?}) answers the same rows and reads every one. {@code !=}'s comparison becomes
 * the table's NOT IN, the null arms beside it as they are; a list of names ({@code in}, {@code contains}) filters the
 * table by {@code name = ANY(?)}. The legacy plan has its own form ({@code PlanEnumForm}).
 */
public final class EnumValueTables extends SqlRewriter {

    private static final String ALIAS = "_enum";

    private EnumValueTables() {
    }

    /** {@code q} with each enumeration parameter's comparison against a decoded column read through a value table. */
    public static SqlQuery apply(SqlQuery q) {
        return new EnumValueTables().rewrite(q);
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        // a list's membership: `decode IN ($names)`, `$names->contains(decode)`
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.IN && c.args().size() == 2 && enumParam(c.args().get(1))) {
            return tabled(c.args().get(0), (SqlExpr.PlanParam) c.args().get(1), e);
        }
        if (e instanceof SqlExpr.Membership m && enumParam(m.collection())) {
            return tabled(m.needle(), (SqlExpr.PlanParam) m.collection(), e);
        }
        // one value's comparison: `decode = $name`, `decode <> $name` (either side)
        if (!(e instanceof SqlExpr.Call c) || c.args().size() != 2
                || (c.fn() != SqlFn.EQUAL && c.fn() != SqlFn.NOT_EQUAL)) {
            return e;
        }
        int param = enumParam(c.args().get(1)) ? 1 : enumParam(c.args().get(0)) ? 0 : -1;
        if (param < 0) {
            return e;
        }
        SqlExpr in = tabled(c.args().get(1 - param), (SqlExpr.PlanParam) c.args().get(param), e);
        return in == e || c.fn() == SqlFn.EQUAL ? in : new SqlExpr.Call(SqlFn.NOT, List.of(in));
    }

    private static boolean enumParam(SqlExpr e) {
        return e instanceof SqlExpr.PlanParam p && p.kind() == SqlExpr.PlanParam.Kind.ENUM;
    }

    /** {@code decoded}'s source column IN the value table of its decode, its names filtered by {@code param} (one
     *  name, or a list's: {@code name IN param}, which the dialect binds as one array); {@code unchanged} when
     *  {@code decoded} is not a decode of literal codes. */
    private static SqlExpr tabled(SqlExpr decoded, SqlExpr.PlanParam param, SqlExpr unchanged) {
        Optional<SqlExpr> source = DecodeShapes.sourceExpr(decoded);
        Optional<List<List<SqlExpr>>> pairs = DecodeShapes.codesAndNames(decoded);
        if (source.isEmpty() || pairs.isEmpty()
                || !(pairs.get().get(0).get(0).type() instanceof TypeFact.Typed code)) {
            return unchanged;
        }
        List<OutputCol> columns = List.of(new OutputCol("code", code.type(), false),
                new OutputCol("name", SqlType.Scalar.VARCHAR, false));
        SqlExpr name = SqlExpr.Column.of(ALIAS, columns, "name");
        boolean list = param.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Array;
        SqlSelect table = SqlSelect.starOf(new SqlSource.Values(pairs.get(), List.of("code", "name"), ALIAS, columns))
                .withProjections(List.of(new SqlSelect.Projection(SqlExpr.Column.of(ALIAS, columns, "code"), "code",
                        columns.get(0))))
                .withWhere(SqlExpr.Call.of(list ? SqlFn.IN : SqlFn.EQUAL, name, param));
        return new SqlExpr.InSubquery(source.get(), table);
    }
}
