// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * A union of more than {@link #FAN_OUT} branches as a union of groups, each group's branches read through a derived
 * table ({@code SELECT * FROM (b1 UNION ALL ... b64) AS u1 UNION ALL SELECT * FROM (...) AS u2}), recursively. A
 * database parses {@code b1 UNION ALL b2 UNION ALL ...} as a tree as deep as it has branches, and DuckDB refuses one
 * past its depth of 1,000 (the IQ scale program's Q10 and Q17, 2026-10-23: its scale probe's UNION ALL of 2,000);
 * grouped, the tree is {@link #FAN_OUT} deep per level and a level per {@link #FAN_OUT}-fold. A union, deduplicating
 * or not, is associative, so the grouping keeps its rows; the union's columns are its first branch's in both forms,
 * and a derived table, unlike a parenthesized set operation, every dialect reads (SQLite has none of the latter).
 */
final class BalancedUnions extends SqlRewriter {

    /** The most branches one union is written with. */
    static final int FAN_OUT = 64;

    @Override
    protected SqlQuery union(SqlUnion u) {
        if (u.branches().size() <= FAN_OUT) {
            return u;
        }
        List<SqlQuery> level = u.branches();
        int group = 0;
        while (level.size() > FAN_OUT) {
            List<SqlQuery> groups = new ArrayList<>();
            for (int i = 0; i < level.size(); i += FAN_OUT) {
                List<SqlQuery> part = level.subList(i, Math.min(level.size(), i + FAN_OUT));
                groups.add(part.size() == 1 ? part.get(0) : SqlSelect.starOf(new SqlSource.Subselect(
                        new SqlUnion(List.copyOf(part), u.all(), u.outputs()), "u" + ++group, null)));
            }
            level = groups;
        }
        return new SqlUnion(level, u.all(), u.outputs());
    }
}
