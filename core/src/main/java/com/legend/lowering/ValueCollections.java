// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * VALUE-COLLECTION select builders — a relation consumed as a LIST value
 * (contains/in/makeString tails, the assert idiom
 * {@code $result.values.rows.values}). Pure SQL-IR construction; the
 * Lowerer supplies the lowered relation and the enclosing-scope channel.
 */
final class ValueCollections {

    private ValueCollections() {
    }

    /** The single-column {@code value} projection a relation-map value
     * read lowers through (both Lowerer map arms — root and collect):
     * cell type = the map node's result type, cell mult = the mapper's
     * per-cell stamp, outer mult ONE (the relation VALUE). */
    static com.legend.compiler.spec.typed.TypedProject valueColumnProject(
            TypedSpec source, TypedLambda ml,
            com.legend.compiler.element.type.Type cellType,
            com.legend.compiler.element.type.Multiplicity colMult) {
        return new com.legend.compiler.spec.typed.TypedProject(source,
                List.of(new com.legend.compiler.spec.typed.TypedFuncCol(
                        "value", ml)),
                new com.legend.compiler.element.type.ExprType(
                        Type.relation(new Type.RelationType(List.of(
                                new Type.RelationType.Column("value",
                                        cellType, colMult)))),
                        com.legend.compiler.element.type
                                .Multiplicity.Bounded.ONE));
    }

    /** The relation-map VALUE-COLLECTION collect (Lowerer's non-root
     * map arm): LIST-aggregate the projected {@code value} column to
     * one list value; a collection mapper flattens one level, and
     * scalar-STAMPED cells re-box as {@code [cell]} so the flatten
     * contract holds (C1). */
    static SqlExpr collectAsList(SqlSelect proj, boolean collMapper,
            boolean scalarCells, String sub) {
        SqlExpr cellRead = SqlExpr.Column.of(sub, proj.outputs(), "value");
        SqlExpr collected = scalarCells
                ? new SqlExpr.ArrayLit(List.of(cellRead)) : cellRead;
        SqlSelect agg = SqlSelect.starOf(new SqlSource.Subselect(proj, sub, null))
                .withProjections(List.of(new SqlSelect.Projection(
                        new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                collected), false, List.of()),
                        null,
                        new OutputCol("value",
                                SqlType.Scalar.VARCHAR, true))));
        SqlExpr listed = new SqlExpr.ScalarSubquery(agg);
        return collMapper
                ? SqlExpr.Call.of(SqlFn.LIST_FLATTEN, listed)
                : listed;
    }

    /** {@code SELECT LIST(col)} over {@code rel} — the single-column
     * value collection. */
    static SqlSelect columnList(SqlSelect rel, String col, String sub) {
        return SqlSelect.starOf(new SqlSource.Subselect(rel, sub, null))
                .withProjections(List.of(new SqlSelect.Projection(
                        new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                SqlExpr.Column.of(sub, rel.outputs(),
                                        col)),
                                false, java.util.List.of()),
                        null,
                        new OutputCol(col, SqlType.Scalar.VARCHAR,
                                true))));
    }

    /** {@code SELECT flatten(LIST([cells...]))} over {@code rel} — a
     * MULTI-column relation as a ROW-MAJOR value collection. Cells ride
     * the LITERAL carrier when every column kind SPELLS (conform BY
     * EMISSION — the same grammar owner the claimed Any-LUB expected
     * side uses, so grid asserts byte-compare; the OutputCol carries
     * the label and egress decodes typed). An unspellable column kind
     * (enum) keeps the whole list on the VARIANT carrier. */
    static SqlSelect rowMajorCellList(SqlSelect rel, Type.RelationType rt,
            String sub) {
        List<SqlExpr> cells = new ArrayList<>();
        boolean allSpell = true;
        for (Type.Column c : rt.columns()) {
            // WIRE columns by construction — DateTime cells spell the
            // engine's value-read decode (NINE subsecond digits,
            // disagree-9 burn); written-literal spellings never pass
            // through this caller
            SqlExpr s = MixedEncoding.spellByKind(c.type(),
                    SqlExpr.Column.of(sub, rel.outputs(), c.name()),
                    com.legend.sql.DateFmt.ISO_NANO);
            if (s == null) {
                allSpell = false;
                break;
            }
            cells.add(s);
        }
        if (!allSpell) {
            cells.clear();
            for (Type.Column c : rt.columns()) {
                cells.add(SqlExpr.Call.of(SqlFn.TO_VARIANT,
                        SqlExpr.Column.of(sub, rel.outputs(), c.name())));
            }
        }
        return SqlSelect.starOf(new SqlSource.Subselect(rel, sub, null))
                .withProjections(List.of(new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.LIST_FLATTEN,
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                        new SqlExpr.ArrayLit(cells)),
                                        false, java.util.List.of())),
                        null,
                        new OutputCol("value",
                                allSpell ? SqlType.Scalar.LITERAL
                                        : SqlType.Scalar.VARCHAR,
                                true))));
    }

    /** Delegates to the canonical reader on the node itself. */
    static @com.legend.base.Nullable String autoMapHop(TypedSpec spec) {
        return com.legend.compiler.spec.typed.TypedMap.singleHopProperty(spec);
    }

    /** The C1 SINGLETON-COLLAPSE predicate — one statement of the rule
     * both collection arms share (D4: it was restated inline in each):
     * a one-element collection whose stamp admits at most one value IS
     * its element; only the CARRIER differs per arm (plain vs the
     * Any-LUB variant wrap). */
    static boolean c1Singleton(TypedCollection tc) {
        return tc.elements().size() == 1 && Stamps.atMostOne(tc);
    }

    // isRowCells (the TDSRow-cells SHAPE-MATCHER) DELETED (TDSNull-is-a-
    // value slice): the fact is CONSTRUCTION-DECLARED now —
    // TypedCollection.rowCells(), set only by the Typer's rowCells()
    // synthesis. Recognizing our own synthesis by shape at consumption
    // was the sniffing disease applied to ourselves; the distinction the
    // roster test carried ($r.values vs a hand-written cell list — engine
    // joinStrings semantics, bare columns, no TDSNull) is the same
    // distinction the declaration carries, minus the accidental-match
    // hole.

    /** The rowCells makeString/joinStrings join as a STATIC CONCAT
     * interleave: (start?) c1 (sep c2 …) (end?) — args beyond the
     * collection arrive lowered in order ([sep] or [start, sep, end]). */
    static com.legend.sql.SqlExpr rowCellsJoin(
            java.util.List<com.legend.sql.SqlExpr> cells,
            java.util.List<com.legend.sql.SqlExpr> rest) {
        com.legend.sql.SqlExpr sep = rest.size() == 1 ? rest.get(0)
                : rest.size() == 3 ? rest.get(1) : null;
        java.util.List<com.legend.sql.SqlExpr> parts = new ArrayList<>();
        if (rest.size() == 3) {
            parts.add(rest.get(0));
        }
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0 && sep != null) {
                parts.add(sep);
            }
            parts.add(cells.get(i));
        }
        if (rest.size() == 3) {
            parts.add(rest.get(2));
        }
        return parts.size() == 1 ? parts.get(0)
                : parts.isEmpty() ? new com.legend.sql.SqlExpr.StringLit("")
                : new com.legend.sql.SqlExpr.Call(
                        com.legend.sql.SqlFn.CONCAT, parts);
    }

    static boolean isCollectionMapper(TypedLambda ml) {
        // Collection mapper iff the lowered value is a SQL LIST, which is
        // exactly a TypedCollection body (list_value carrier). A loose
        // declared multiplicity over a non-collection body still lowers to
        // a plain scalar column — wrapping it in UNNEST/flatten is a type
        // error, not a flatten. Casts distribute element-wise over
        // collections (the value stays a LIST) — look through them
        // ($x.values->cast(@StrictDate), calendar DateRange).
        TypedSpec last = ml.body().get(ml.body().size() - 1);
        while (last instanceof TypedCast tc) {
            last = tc.source();
        }
        // a JSON tree read that yields the element/member LIST (the
        // variant lane's list value) is a collection mapper too: pure's
        // $rows.keyValuePairs / $arrays.values flatten
        if (last instanceof com.legend.compiler.spec.typed.TypedJsonAccess ja) {
            return ja.op() == com.legend.compiler.spec.typed.TypedJsonAccess.Op.MEMBERS
                    || ja.op() == com.legend.compiler.spec.typed.TypedJsonAccess.Op.ELEMENTS;
        }
        return last instanceof TypedCollection;
    }
}
