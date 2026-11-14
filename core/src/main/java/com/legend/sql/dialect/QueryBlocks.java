// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * A STATEMENT'S QUERY BLOCKS, the scope of its aliases (2026-11-05): a source alias is visible in the block that
 * defines it -- a common table expression's query, the statement's body, a top-level union's branch -- and in the
 * blocks nested inside that one, never in a sibling block. A pass that maps alias names to their sources maps them
 * per block: the lowering keeps aliases unique within one lowered statement, but a statement COMPOSED of several (the
 * database judge's fused verdict statement: each assert's rows a block, each lowered on its own, each counting its
 * aliases from {@code t0}) holds the same alias in two blocks. A statement-wide map then resolved one block's
 * {@code "t3"} to the other's (H2's corpus: {@code Column "t3.bookId" not found}, the split rung's tripwire's first
 * catch).
 */
final class QueryBlocks {

    private QueryBlocks() {
    }

    /** {@code q} with {@code perBlock} applied to each of its top-level blocks: each common table expression's query
     *  and the body, a union's branches each, recursively; {@code q} itself where nothing changed. */
    static SqlQuery each(SqlQuery q, UnaryOperator<SqlQuery> perBlock) {
        if (q instanceof SqlWith w) {
            List<SqlWith.Cte> ctes = new ArrayList<>(w.ctes().size());
            boolean changed = false;
            for (SqlWith.Cte c : w.ctes()) {
                SqlQuery q2 = each(c.query(), perBlock);
                changed |= q2 != c.query();
                ctes.add(q2 == c.query() ? c : new SqlWith.Cte(c.name(), q2, c.materialized()));
            }
            SqlQuery body = each(w.body(), perBlock);
            return !changed && body == w.body() ? w : w.with(ctes, body);
        }
        if (q instanceof SqlUnion u) {
            List<SqlQuery> branches = new ArrayList<>(u.branches().size());
            boolean changed = false;
            for (SqlQuery b : u.branches()) {
                SqlQuery b2 = each(b, perBlock);
                changed |= b2 != b;
                branches.add(b2);
            }
            return changed ? new SqlUnion(branches, u.all(), u.outputs()) : u;
        }
        return perBlock.apply(q);
    }
}
