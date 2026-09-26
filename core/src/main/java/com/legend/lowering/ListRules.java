// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.Map;

/** The list-PRODUCING natives' scalar rules: range and repeat. */
final class ListRules {

    private ListRules() {
    }

    static void register(Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        // repeat(e, n): n copies of one value — a semantic node the
        // dialects spell (DuckDB: list_transform over range(n))
        for (com.legend.model.FunctionId f : Pure.AT_COLLECTION_REPEAT) {
            rules.put(f, (n, args) -> new SqlExpr.Call(SqlFn.REPEAT_VALUE, args));
        }
        // range(start, stop, step): a ZERO step raises real pure's message
        for (com.legend.model.FunctionId f : Pure.AT_COLLECTION_RANGE) {
            rules.put(f, (n, args) -> args.size() < 3
                    ? new SqlExpr.Call(SqlFn.RANGE_FN, args)
                    : Scalars.guarded(SqlExpr.Call.of(SqlFn.EQUAL, args.get(2), new SqlExpr.IntLit(0)),
                            new SqlExpr.StringLit("range step must not be 0"),
                            new SqlExpr.Call(SqlFn.RANGE_FN, args)));
        }
    }
}
