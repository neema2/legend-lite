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
 * the table's NOT IN; the null arms beside it stay as they are. The legacy plan has its own form ({@code PlanEnumForm}).
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
        if (!(e instanceof SqlExpr.Call c) || c.args().size() != 2
                || (c.fn() != SqlFn.EQUAL && c.fn() != SqlFn.NOT_EQUAL)) {
            return e;
        }
        int param = enumParam(c.args().get(1)) ? 1 : enumParam(c.args().get(0)) ? 0 : -1;
        if (param < 0) {
            return e;
        }
        SqlExpr decoded = c.args().get(1 - param);
        Optional<SqlExpr> source = DecodeShapes.sourceExpr(decoded);
        Optional<List<List<SqlExpr>>> pairs = DecodeShapes.codesAndNames(decoded);
        if (source.isEmpty() || pairs.isEmpty()
                || !(pairs.get().get(0).get(0).type() instanceof TypeFact.Typed code)) {
            return e;
        }
        SqlExpr in = new SqlExpr.InSubquery(source.get(), table(pairs.get(), code.type(),
                (SqlExpr.PlanParam) c.args().get(param)));
        return c.fn() == SqlFn.EQUAL ? in : new SqlExpr.Call(SqlFn.NOT, List.of(in));
    }

    private static boolean enumParam(SqlExpr e) {
        return e instanceof SqlExpr.PlanParam p && p.kind() == SqlExpr.PlanParam.Kind.ENUM;
    }

    /** {@code SELECT code FROM (VALUES (code, name), ...) AS _enum(code, name) WHERE name = param}. */
    private static SqlSelect table(List<List<SqlExpr>> rows, SqlType codeType, SqlExpr.PlanParam param) {
        List<OutputCol> columns = List.of(new OutputCol("code", codeType, false),
                new OutputCol("name", SqlType.Scalar.VARCHAR, false));
        SqlSource values = new SqlSource.Values(rows, List.of("code", "name"), ALIAS, columns);
        return SqlSelect.starOf(values)
                .withProjections(List.of(new SqlSelect.Projection(SqlExpr.Column.of(ALIAS, columns, "code"), "code",
                        columns.get(0))))
                .withWhere(SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of(ALIAS, columns, "name"), param));
    }
}
