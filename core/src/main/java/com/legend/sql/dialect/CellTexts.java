// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * THE PRODUCT'S TEXT OF A CELL ({@link SqlFn#CELL_TEXT}; the user's ruling of 2026-11-10, D_BRIEF §1: "makeString
 * skips empty elements", and a TDS cell's toString empty where the cell is, relayed 2026-11-15; phase D's D1.3): the
 * text alone, EMPTY where the cell is ({@link Rest}) -- a joined element that is empty is a
 * {@code NULL}, which a list's {@code STRING_AGG} skips; a row's cells joined (a literal list) are one
 * {@link SqlFn#CONCAT_WS}, which skips an empty cell with its separator (Phase 3, 2026-11-16; until then each present
 * cell after the separator, the first separator cut, {@link #eachAfterSeparator}: H2's and SQLite's literal join is a
 * CONCAT chain, which kept an empty cell's separator, {@code 2,}, measured 2026-11-15). A pass of the base renderer, run after the compatibility edge's
 * spelling passes (which write legend-engine's {@code TDSNull} for it) and before every other.
 */
final class CellTexts {

    private CellTexts() {
    }

    /** The base renderer's pass, at the join (the walk is bottom-up: a hook sees its operands rewritten, so the join
     *  reads its elements' {@link SqlFn#CELL_TEXT}s, which no other node holds; one reaching a dialect is refused). */
    static final class Expansion extends com.legend.sql.SqlRewriter {
        /** The row joins this pass wrote, by identity: the only operands whose wrapping COALESCE it drops. */
        private final java.util.Set<SqlExpr> joinedRow = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());

        @Override
        protected SqlExpr expr(SqlExpr e) {
            // the join's COALESCE(.., '') (the lowering's, for a STRING_AGG over no rows) around a row's join already
            // written below: the join is never NULL (CONCAT_WS's separator is a String[1]; one cell's COALESCE), so
            // the wrapper goes (Phase 3, 2026-11-17: the probe's SQL showed it)
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.COALESCE && c.args().size() == 2
                    && c.args().get(1) instanceof SqlExpr.StringLit empty && empty.value().isEmpty()
                    && c.args().get(0) instanceof SqlExpr.Call j && joinedRow.contains(j)) {
                return j;
            }
            if (!(e instanceof SqlExpr.ReduceCollection rc && rc.reducer() == com.legend.sql.SqlAgg.Fn.STRING_AGG
                    && rc.extras().size() == 1)) {
                return e;
            }
            // a list's elements joined (makeString): each element's text, the list's STRING_AGG skipping an empty one
            if (rc.collection() instanceof SqlExpr.Call t && t.fn() == SqlFn.LIST_TRANSFORM && t.args().size() == 2
                    && t.args().get(1) instanceof SqlExpr.Lambda l && isJoined(l.body())) {
                return new SqlExpr.ReduceCollection(rc.reducer(), SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        t.args().get(0), new SqlExpr.Lambda(l.params(), operand(l.body()))), rc.extras());
            }
            // a row's cells joined (a literal list of CELL_TEXTs, the one join whose elements may be empty: a Pure
            // list holds no empty, a collected column is compacted when it is collected): each present cell, an empty
            // one skipped with its separator -- one CONCAT_WS (Phase 3 of
            // docs/records/total-wall-edge-program-2026-11-16.md, 2026-11-16; a database whose CONCAT_WS does not skip
            // a NULL writes its own form, {@link #eachAfterSeparator} where it has none), one cell its text or empty.
            // A literal list's join was a CONCAT chain on the databases that fold it (H2, SQLite: CarrierStrategies),
            // where an empty cell kept its separator (2026-11-15, 2,)
            if (rc.collection() instanceof SqlExpr.ArrayLit cells && !cells.elements().isEmpty()
                    && cells.elements().stream().allMatch(Expansion::isJoined)) {
                java.util.List<SqlExpr> parts = new java.util.ArrayList<>(cells.elements().size() + 1);
                parts.add(rc.extras().get(0));
                for (SqlExpr cell : cells.elements()) {
                    parts.add(operand(cell));
                }
                SqlExpr.Call join = parts.size() == 2
                        ? SqlExpr.Call.of(SqlFn.COALESCE, parts.get(1), new SqlExpr.StringLit(""))
                        : new SqlExpr.Call(SqlFn.CONCAT_WS, parts);
                joinedRow.add(join);
                return join;
            }
            return e;
        }

        private static boolean isJoined(SqlExpr e) {
            return e instanceof SqlExpr.Call c && c.fn() == SqlFn.CELL_TEXT;
        }

        private static SqlExpr operand(SqlExpr joined) {
            return ((SqlExpr.Call) joined).args().get(0);
        }
    }

    /** {@link SqlFn#CONCAT_WS}'s parts joined where the database has no separator join (D1.3's form, 2026-11-15):
     *  each present part after the separator, the first separator cut. */
    static SqlExpr eachAfterSeparator(SqlExpr sep, java.util.List<SqlExpr> parts) {
        SqlExpr out = afterSeparator(sep, parts.get(0));
        for (SqlExpr text : parts.subList(1, parts.size())) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, afterSeparator(sep, text));
        }
        return SqlExpr.Call.of(SqlFn.SUBSTRING, out, SqlExpr.Call.of(SqlFn.PLUS,
                SqlExpr.Call.of(SqlFn.LENGTH, sep), new SqlExpr.IntLit(1)));
    }

    private static SqlExpr afterSeparator(SqlExpr sep, SqlExpr text) {
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, text),
                new SqlExpr.StringLit(""))), SqlExpr.Call.of(SqlFn.CONCAT, sep, text), SqlExpr.Reason.PRINT);
    }

    /** The base renderer's pass after {@link Expansion}: a {@link SqlFn#CELL_TEXT} no join read (a TDS cell's
     *  {@code toString}), its text -- {@code NULL}, empty, where the cell is. */
    static final class Rest extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            return e instanceof SqlExpr.Call c && c.fn() == SqlFn.CELL_TEXT ? c.args().get(0) : e;
        }
    }
}
