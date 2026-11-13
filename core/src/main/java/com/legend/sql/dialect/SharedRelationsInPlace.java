// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.AliasPrefix;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A relation the statement reads more than once, written back where each read stands (2026-11-04): the lowering
 * writes it once, a common table expression {@code __let<n>} ({@link SqlWith#SHARED_PREFIX}) at the statement's head
 * that each read names ({@code lowering.SharedRelations}). This pass is for a statement that cannot hold it there:
 * MongoDB's pipeline has no common table expressions, and the database judge composes a side into a verdict of its own
 * ({@link #all}). (H2 2.1 and 2.2 lost a placeholder's value inside a common table expression another one reads, and a
 * shared relation that bound one was written in place there; H2 2.5.252, main's since 2026-11-05, keeps it, and that
 * case went on 2026-11-06.) Such an expression is a derived table at each read, as the lowering wrote it before
 * it was shared, and the statement holds as many copies as reads. Each copy's aliases are prefixed ({@link
 * AliasPrefix}): a statement's aliases are unique ({@code SourceSpelling}). An expression read by another written in
 * place is written in place inside it first, in the WITH's order.
 */
public final class SharedRelationsInPlace extends SqlRewriter {

    private final int[] copies = {0};

    private SharedRelationsInPlace() {
    }

    /** Every shared relation written in place: a database with no common table expressions; and a statement the
     *  database judge composes into a verdict of its own (BodyDriver.wrapSide, 2026-11-06), where it is a derived
     *  table, which cannot hold a WITH of its own on every database. */
    public static SharedRelationsInPlace all() {
        return new SharedRelationsInPlace();
    }

    /** Every WITH, at any depth, after its parts are rewritten. */
    @Override
    protected SqlQuery with(SqlWith w) {
        Map<String, SqlQuery> inPlace = new HashMap<>();
        List<SqlWith.Cte> kept = new ArrayList<>();
        for (SqlWith.Cte c : w.ctes()) {
            SqlQuery query = substituted(c.query(), inPlace);
            if (c.name().startsWith(SqlWith.SHARED_PREFIX)) {
                inPlace.put(c.name(), query);
            } else {
                kept.add(new SqlWith.Cte(c.name(), query, c.materialized()));
            }
        }
        if (inPlace.isEmpty()) {
            return w;
        }
        SqlQuery body = substituted(w.body(), inPlace);
        return kept.isEmpty() ? body : w.with(kept, body);
    }

    /** {@code q} with each read of an expression in {@code inPlace} its query, a derived table under the read's
     *  alias, the copy's own aliases prefixed. */
    private SqlQuery substituted(SqlQuery q, Map<String, SqlQuery> inPlace) {
        if (inPlace.isEmpty()) {
            return q;
        }
        return new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Cte c && inPlace.containsKey(c.name())) {
                    SqlQuery copy = AliasPrefix.apply("s" + copies[0]++ + "_", inPlace.get(c.name()));
                    return new SqlSource.Subselect(copy, c.alias(), null);
                }
                return s;
            }
        }.rewrite(q);
    }
}
