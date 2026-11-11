// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.Map;

/**
 * The lowering of a {@link com.legend.compiler.spec.typed.TypedTableSwap} (B2d.2 of the total-wall program, 2026-11-11):
 * the source's statement with each physical table it reads ({@link SqlSource.Table}, by the name the database knows it
 * by) whose name the swap holds read from that statement instead -- a derived table under the table's own alias, so
 * every column the source reads it by is the same. Structural: no SQL is written here, the statements are the
 * planner's.
 */
final class TableSwaps {

    private TableSwaps() {
    }

    /** {@code lowered} with each table {@code tables} names read from its statement. */
    static SqlSelect swapped(SqlSelect lowered, Map<String, SqlQuery> tables) {
        SqlQuery out = new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Table t && !t.call() && tables.containsKey(t.name())) {
                    return new SqlSource.Subselect(tables.get(t.name()), t.alias(), null);
                }
                return s;
            }
        }.rewriteRoot(lowered);
        if (!(out instanceof SqlSelect select)) {
            throw new IllegalStateException("a swapped relation lowered to " + out.getClass().getSimpleName()
                    + ", not a select");
        }
        return select;
    }
}
