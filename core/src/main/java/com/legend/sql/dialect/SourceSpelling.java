// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE DEFINITION OWNS THE SPELLING (Phase 1, batch 135 — the audit's
 * "quoted alias, unquoted reference" renderer defect, 128 H2 tests). On a
 * case-sensitive session a column reference must spell exactly as the
 * source it reads DECLARED the name: a subselect's projection label
 * ({@code AS "id"}) is a quoted, case-exact identity, a table's column is
 * the DDL's bare spelling. The lowering stamps references at construction
 * with an {@link OutputCol.Origin}; where a reference was built from a
 * TABLE's output list but addressed at a SUBSELECT alias (a filtered set
 * behind a join), the stamp says PHYSICAL and the rendered {@code "t3".id}
 * folds to {@code ID} against the label {@code "id"}. Rather than hunt
 * every construction site, this pass re-derives each reference's origin
 * from its SOURCE: aliases are unique per lowered statement, so one alias →
 * outputs scope over each of the statement's query BLOCKS is correct
 * (correlated outer references included; {@link QueryBlocks}: a statement
 * composed of several lowered ones -- the fused verdict statement -- holds
 * one alias in two blocks, 2026-11-05). (A raw SQL source's labels are born
 * DERIVED at the lowering: the query's own spelling, not DDL.) A name the
 * source does not declare keeps its stamp.
 */
final class SourceSpelling extends SqlRewriter {

    /** alias → declared outputs, over the query block being rewritten. */
    private final Map<String, List<OutputCol>> scope = new HashMap<>();

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        return QueryBlocks.each(q, this::block);
    }

    /** One query block: its aliases' outputs, then its references re-derived from them. */
    private SqlQuery block(SqlQuery q) {
        scope.clear();
        new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (!(s instanceof SqlSource.Join) && !(s instanceof SqlSource.Dual)) {
                    scope.put(s.alias(), s.outputs());
                }
                return s;
            }
        }.rewrite(q);
        return rewrite(q);
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (!(e instanceof SqlExpr.Column c) || c.table() == null) {
            return e;
        }
        List<OutputCol> outs = scope.get(c.table());
        if (outs == null) {
            return e;
        }
        for (OutputCol oc : outs) {
            if (oc.name().equals(c.name())) {
                return oc.origin() == c.origin() ? e
                        : SqlExpr.Column.of(c.table(), oc);
            }
        }
        return e;
    }
}
