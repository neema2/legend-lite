// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTableSwap;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

/**
 * The lowering of a {@link com.legend.compiler.spec.typed.TypedTableSwap} (B2d.2 of the total-wall program, 2026-11-11;
 * keyed by typed identity since the audit's M1, the same day): each table the source reads by its typed reference
 * ({@link TypedTableReference}) whose store and physical name the swap reads from the slice (a table of the reloaded
 * database or of one it includes, whatever database names it) read from the slice's statement for that table instead -- the scan recorded by its alias where the lowering makes it ({@link #rows}), and once the
 * source is lowered its table replaced by a derived table under that alias ({@link #swapped}), so every column the
 * source reads it by is the same. Anything the slice cannot answer is refused by name: a reloaded database's table the
 * slice does not hold, a table of another database that shares a reloaded table's physical name (on one connection
 * upstream's reload replaced it), and, read by name rather than by a typed reference, a reloaded table left in the
 * lowered statement; the Lowerer refuses a raw, URL, catalog or frame source and a tabular function under a swap.
 * Structural: no SQL is written here, the statements are the planner's.
 */
final class TableSwaps {

    private TableSwaps() {
    }

    /** The slice's rows the typed table {@code t} is read from under {@code swap}, or null where the reload does not
     *  replace it (a table of another database, read as it is); one that shares the physical name of a table the
     *  reload replaces refused by name. */
    static @com.legend.base.Nullable SqlQuery rows(TypedTableReference t, TypedTableSwap swap) {
        SqlQuery rows = swap.tables().get(t.table());
        if (swap.reads().contains(t.store() + "|" + t.table())) {
            if (rows == null) {
                throw new IllegalStateException("a reloaded slice reads " + t.table() + " of " + t.store()
                        + " and holds no rows for it");
            }
            return rows;
        }
        if (rows != null) {
            throw new NotImplementedException("the query run over the test-data generator's reloaded slice reads "
                    + t.table() + " of " + t.store() + ", a table the reload does not replace by the physical name of"
                    + " one it does: not planned (B2d)");
        }
        return null;
    }

    /** A source the slice cannot answer, under {@code swap}: refused by name. */
    static NotImplementedException refused(String what) {
        return new NotImplementedException("the query run over the test-data generator's reloaded slice reads " + what
                + ", which the read-only reload cannot answer: not planned (B2d)");
    }

    /** {@code lowered}, the source of {@code swap} lowered, with each scan recorded in {@code byAlias} read from its
     *  rows: a derived table under the scan's alias. A reloaded table left read by name -- not by a recorded scan (a
     *  calendar's table, a name a post-processor wrote) -- is refused by name. */
    static SqlSelect swapped(SqlSelect lowered, TypedTableSwap swap, java.util.Map<String, SqlQuery> byAlias) {
        SqlQuery out = new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Table t && !t.call()) {
                    SqlQuery rows = byAlias.get(t.alias());
                    if (rows != null && swap.tables().get(t.name()) == rows) {
                        return new SqlSource.Subselect(rows, t.alias(), null);
                    }
                    if (swap.tables().containsKey(t.name())) {
                        throw refused("the table " + t.name() + " by its name");
                    }
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
