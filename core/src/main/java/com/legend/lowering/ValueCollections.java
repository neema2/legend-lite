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

    /**
     * A MULTI-column relation as a ROW-MAJOR value collection: a TAGGED LIST over the relation's CELLS (track L,
     * 2026-10-29; the spelling carrier's text flatten, {@link #rowMajorCellList}, until then). The relation's rows
     * are numbered once ({@code ROW_NUMBER() OVER ()}, the scan's order, as the text flatten aggregated it) and
     * crossed with the column indexes {@code 1..w} (a VALUES table), one row per cell in row-major order: its
     * position {@code (rn - 1) * w + i}, its tag (the channel of the column's kind, {@link MixedEncoding#columnChannel}),
     * its comparable ({@link MixedEncoding#rankedComparableOf}) and every channel's value -- the cell, in its slot,
     * where the column is the channel's, NULL elsewhere. The list's arrays are ordered aggregates over the cells, its
     * positions {@code 1..n}, and it carries the cells as its rows, so a root reads them in order and the list's
     * equality compares channel arrays: lambda-free, every database. Null where a column's kind has no channel (a
     * class; the caller keeps the variant lane).
     */
    static SqlExpr.@com.legend.base.Nullable TaggedList rowMajorTaggedList(SqlSelect rel, Type.RelationType rt,
            java.util.function.Supplier<String> alias) {
        int w = rt.columns().size();
        List<SqlType.Tagged.Slot> channels = new ArrayList<>();
        List<Integer> channelOf = new ArrayList<>(w);
        var cellOf = SqlExpr.Column.lookup(null, rel.outputs());
        for (Type.Column c : rt.columns()) {
            SqlType.Tagged.Slot ch = MixedEncoding.columnChannel(c.type(),
                    cellOf.apply(c.name()).type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null);
            if (ch == null) {
                return null;
            }
            if (!channels.contains(ch)) {
                channels.add(ch);
            }
            channelOf.add(channels.indexOf(ch));
        }
        // the rows numbered ONCE and DETERMINISTICALLY: the list's arrays are aggregates over the cells, each its
        // own scan of the relation, so the numbering must come out the same on every scan (the L1 audit,
        // 2026-10-29: ROW_NUMBER() OVER () over an unordered scan may not) -- by the relation's own sort keys where
        // it has them, else by the order its source carries where that order can be read outside it (an ordered
        // subselect, Sorts.carriedName), then by every column (equal rows are interchangeable: a relation of no
        // order is read in all-columns order, the closest thing to an arrival order no database promises;
        // register S124). A sorted relation keeps its order.
        String inner = alias.get();
        var innerCell = SqlExpr.Column.lookup(inner, rel.outputs());
        List<SqlSelect.Projection> numbered = new ArrayList<>();
        List<SqlSelect.SortKey> order = new ArrayList<>(carriedOrder(rel, innerCell));
        for (OutputCol oc : rel.outputs()) {
            numbered.add(new SqlSelect.Projection(innerCell.apply(oc.name()), oc.name(), oc));
            order.add(new SqlSelect.SortKey(innerCell.apply(oc.name()), true, null, null));
        }
        OutputCol rnOut = new OutputCol("__rn", SqlType.Scalar.BIGINT, false);
        SqlSelect numberedRel;
        boolean star = rel.projections().isEmpty() || rel.projections().stream().anyMatch(
                pr -> pr.expr() instanceof SqlExpr.Star || pr.expr() instanceof SqlExpr.StarExcept);
        if (!rel.orderBy().isEmpty() && !rel.distinct()) {
            // the relation's own sort keys are in scope inside it: the number is one of its projections; a star
            // frame's projections are spelled out first (one per output, the source's column: the L2 audit,
            // 2026-10-29, a sorted star frame -- a sorted table, a sorted raw grid -- threw here)
            List<SqlSelect.Projection> shown = star ? spelledOut(rel) : rel.projections();
            List<SqlSelect.SortKey> keys = new ArrayList<>(rel.orderBy());
            for (SqlSelect.Projection pr : shown) {
                keys.add(new SqlSelect.SortKey(pr.expr(), true, null, null));
            }
            List<SqlSelect.Projection> own = new ArrayList<>(shown);
            own.add(new SqlSelect.Projection(new SqlExpr.WindowCall(
                    new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), keys, null), "__rn", rnOut));
            List<OutputCol> outs = new ArrayList<>(rel.outputs());
            outs.add(rnOut);
            SqlSelect numberedInside = rel.withProjections(own, outs);
            var nCellInner = SqlExpr.Column.lookup(inner, numberedInside.outputs());
            List<SqlSelect.Projection> pass = new ArrayList<>();
            for (OutputCol oc : numberedInside.outputs()) {
                pass.add(new SqlSelect.Projection(nCellInner.apply(oc.name()), oc.name(), oc));
            }
            numberedRel = SqlSelect.starOf(new SqlSource.Subselect(numberedInside, inner, null)).withProjections(pass);
        } else {
            numbered.add(new SqlSelect.Projection(new SqlExpr.WindowCall(
                    new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), order, null), "__rn", rnOut));
            numberedRel = SqlSelect.starOf(new SqlSource.Subselect(rel, inner, null)).withProjections(numbered);
        }
        // crossed with the column indexes: one row per cell
        String n = alias.get();
        String cAlias = alias.get();
        OutputCol iOut = new OutputCol("__i", SqlType.Scalar.INTEGER, false);
        List<List<SqlExpr>> indexRows = new ArrayList<>(w);
        for (int i = 1; i <= w; i++) {
            indexRows.add(List.of(new SqlExpr.IntLit(i)));
        }
        SqlSource.Values index = new SqlSource.Values(indexRows, List.of("__i"), cAlias, List.of(iOut));
        SqlSource from = new SqlSource.Join(new SqlSource.Subselect(numberedRel, n, null), index,
                SqlSource.Join.Kind.CROSS, null);
        var nCell = SqlExpr.Column.lookup(n, numberedRel.outputs());
        SqlExpr i = SqlExpr.Column.of(cAlias, iOut);
        SqlExpr rn = nCell.apply("__rn");
        SqlExpr pos = SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES,
                SqlExpr.Call.of(SqlFn.MINUS, rn, new SqlExpr.IntLit(1)), new SqlExpr.IntLit(w)), i);
        List<SqlExpr.Case.When> tagArms = new ArrayList<>(w);
        List<SqlExpr> ranks = new ArrayList<>(w);
        java.util.Set<Integer> groups = new java.util.HashSet<>();
        for (int k = 0; k < w; k++) {
            Type.Column c = rt.columns().get(k);
            tagArms.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, i, new SqlExpr.IntLit(k + 1)),
                    new SqlExpr.IntLit(channelOf.get(k) + 1)));
            SqlExpr rank = MixedEncoding.rankedComparableOf(c.type(), null, nCell.apply(c.name()));
            if (rank == null) {
                return null;
            }
            ranks.add(rank);
            groups.add((int) ((SqlExpr.IntLit) ((SqlExpr.StructLit) rank).fields().get(0).value()).value());
        }
        // one comparison group of numbers or dates: the group's own channel (MixedEncoding.taggedList's rule)
        if (groups.size() == 1) {
            int g = groups.iterator().next();
            if (g == 0 || g == 1) {
                for (int k = 0; k < w; k++) {
                    ranks.set(k, ((SqlExpr.StructLit) ranks.get(k)).fields().get(g == 0 ? 1 : 2).value());
                }
            }
        }
        // the comparable's channels as SCALAR columns of the cells (a struct reaches no cell: H2 has none; the rank
        // struct is built in the comparable's own aggregate, rendered only where a sort reads it)
        boolean struct = ranks.get(0) instanceof SqlExpr.StructLit;
        List<SqlSelect.Projection> cells = new ArrayList<>();
        List<OutputCol> cellOuts = new ArrayList<>();
        OutputCol posOut = new OutputCol(SqlExpr.TaggedList.ROW_POSITION, SqlType.Scalar.BIGINT, false);
        OutputCol tagOut = new OutputCol(SqlExpr.TaggedList.ROW_TAG, SqlType.Scalar.INTEGER, false);
        cells.add(new SqlSelect.Projection(pos, posOut.name(), posOut));
        cells.add(new SqlSelect.Projection(new SqlExpr.Case(tagArms, null), tagOut.name(), tagOut));
        cellOuts.add(posOut);
        cellOuts.add(tagOut);
        List<String> rankFields = struct ? List.of("g", "n", "d", "s") : List.of(SqlExpr.TaggedList.ROW_COMPARABLE);
        // the one-group comparable's type is the group's (numbers a DOUBLE, dates a TIMESTAMP), never read off a
        // column's rank, which a partial date leaves NULL (the L1 audit, 2026-10-29)
        List<SqlType> rankTypes = struct
                ? List.of(SqlType.Scalar.INTEGER, SqlType.Scalar.DOUBLE, SqlType.Scalar.TIMESTAMP,
                        SqlType.Scalar.VARCHAR)
                : List.of(groups.iterator().next() == 0 ? SqlType.Scalar.DOUBLE : SqlType.Scalar.TIMESTAMP);
        for (int f = 0; f < rankFields.size(); f++) {
            List<SqlExpr.Case.When> arms = new ArrayList<>(w);
            for (int k = 0; k < w; k++) {
                SqlExpr field = struct ? ((SqlExpr.StructLit) ranks.get(k)).fields().get(f).value() : ranks.get(k);
                arms.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, i, new SqlExpr.IntLit(k + 1)), field));
            }
            String name = struct ? SqlExpr.TaggedList.ROW_COMPARABLE + "_" + rankFields.get(f)
                    : SqlExpr.TaggedList.ROW_COMPARABLE;
            OutputCol out = new OutputCol(name, rankTypes.get(f), true);
            cells.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.Case(arms, null), rankTypes.get(f)),
                    name, out));
            cellOuts.add(out);
        }
        for (int j = 0; j < channels.size(); j++) {
            SqlType.Tagged.Slot ch = channels.get(j);
            List<SqlExpr.Case.When> arms = new ArrayList<>();
            for (int k = 0; k < w; k++) {
                if (channelOf.get(k) == j) {
                    arms.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, i, new SqlExpr.IntLit(k + 1)),
                            MixedEncoding.columnCell(ch, nCell.apply(rt.columns().get(k).name()))));
                }
            }
            OutputCol chOut = new OutputCol(SqlExpr.TaggedList.ROW_CHANNEL(j), ch.type(), true);
            cells.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.Case(arms, null), ch.type()),
                    chOut.name(), chOut));
            cellOuts.add(chOut);
        }
        SqlSelect cellsRel = new SqlSelect(cells, false, from, null, List.of(), null, null, List.of(), null, null,
                cellOuts);
        // the arrays: ordered aggregates over the cells
        String v = alias.get();
        var vCell = SqlExpr.Column.lookup(v, cellsRel.outputs());
        java.util.function.Function<String, SqlExpr> agg = name -> new SqlExpr.ScalarSubquery(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.OrderedListAgg(vCell.apply(name),
                        vCell.apply(SqlExpr.TaggedList.ROW_POSITION)), "s", null)),
                false, new SqlSource.Subselect(cellsRel, v, null), null, List.of(), null, null, List.of(), null,
                null, List.of()));
        OutputCol countOut = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        SqlExpr count = new SqlExpr.ScalarSubquery(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()),
                        "__n", countOut)),
                false, new SqlSource.Subselect(cellsRel, v, null), null, List.of(), null, null, List.of(), null,
                null, List.of(countOut)));
        SqlExpr positions = SqlExpr.Call.of(SqlFn.RANGE_FN, new SqlExpr.IntLit(1),
                SqlExpr.Call.of(SqlFn.PLUS, count, new SqlExpr.IntLit(1)));
        List<SqlExpr.TaggedList.Channel> listChannels = new ArrayList<>(channels.size());
        for (int j = 0; j < channels.size(); j++) {
            listChannels.add(new SqlExpr.TaggedList.Channel(channels.get(j).kind(), channels.get(j).type(),
                    agg.apply(SqlExpr.TaggedList.ROW_CHANNEL(j))));
        }
        SqlExpr comparable;
        if (struct) {
            List<SqlExpr.StructLit.Field> fields = new ArrayList<>(4);
            for (int f = 0; f < rankFields.size(); f++) {
                fields.add(new SqlExpr.StructLit.Field(rankFields.get(f),
                        vCell.apply(SqlExpr.TaggedList.ROW_COMPARABLE + "_" + rankFields.get(f)), rankTypes.get(f)));
            }
            comparable = new SqlExpr.ScalarSubquery(new SqlSelect(
                    List.of(new SqlSelect.Projection(new SqlExpr.OrderedListAgg(new SqlExpr.StructLit(fields),
                            vCell.apply(SqlExpr.TaggedList.ROW_POSITION)), "s", null)),
                    false, new SqlSource.Subselect(cellsRel, v, null), null, List.of(), null, null, List.of(), null,
                    null, List.of()));
        } else {
            comparable = agg.apply(SqlExpr.TaggedList.ROW_COMPARABLE);
        }
        return new SqlExpr.TaggedList(positions, agg.apply(SqlExpr.TaggedList.ROW_TAG), comparable, listChannels,
                new SqlExpr.ScalarSubquery(cellsRel));
    }

    /** A star frame's projections spelled out: one per output, the source's own column (the frame reads it so). */
    private static List<SqlSelect.Projection> spelledOut(SqlSelect rel) {
        List<SqlSelect.Projection> out = new ArrayList<>(rel.outputs().size());
        for (OutputCol oc : rel.outputs()) {
            SqlExpr.Column c = Fold.sourceColumn(rel.from(), oc.name());
            if (c == null) {
                throw new IllegalStateException("the cells of a sorted relation: its column '" + oc.name()
                        + "' cannot be read from its source " + rel.from().getClass().getSimpleName()
                        + " to number its rows by its own order");
            }
            out.add(new SqlSelect.Projection(c, oc.name(), oc));
        }
        return out;
    }

    /**
     * The order a relation of NO sort keys of its own still carries, read outside it as columns of {@code innerCell}
     * (the subselect the numbering ranges over): a DISTINCT frame's own ORDER BY (the number cannot stand among its
     * projections, which would undo the distinct), and a star frame's ordered SOURCE ({@link Sorts#carriedName}: the
     * name the source shows each key under, as a sort over a sorted subselect carries them). A key that cannot be
     * read outside is refused by name: the order would be dropped silently.
     */
    private static List<SqlSelect.SortKey> carriedOrder(SqlSelect rel,
            java.util.function.Function<String, SqlExpr.Column> innerCell) {
        List<SqlSelect.SortKey> out = new ArrayList<>();
        boolean star = rel.projections().isEmpty() || rel.projections().stream().anyMatch(
                pr -> pr.expr() instanceof SqlExpr.Star || pr.expr() instanceof SqlExpr.StarExcept);
        SqlSelect ordered;
        if (!rel.orderBy().isEmpty() && rel.distinct()) {
            ordered = rel;
        } else if (rel.orderBy().isEmpty() && star && rel.from() instanceof SqlSource.Subselect sub
                && sub.inner() instanceof SqlSelect inside && !inside.orderBy().isEmpty()) {
            ordered = inside;
        } else {
            return out;
        }
        for (SqlSelect.SortKey k : ordered.orderBy()) {
            String name = Sorts.carriedName(ordered, k);
            if (name == null || rel.outputs().stream().noneMatch(oc -> oc.name().equals(name))) {
                throw new com.legend.error.NotImplementedException("the cells of a sorted relation whose sort key "
                        + k.expr() + " cannot be read outside it (no output shows it): its rows cannot be numbered"
                        + " in its order");
            }
            out.add(new SqlSelect.SortKey(innerCell.apply(name), k.ascending(), k.nullOrder(), null));
        }
        return out;
    }

    /** {@code SELECT flatten(LIST([cells...]))} over {@code rel}: a MULTI-column relation as a ROW-MAJOR value
     *  collection on the VARIANT carrier, for a column of a kind no tagged channel holds (a class; the tagged list,
     *  {@link #rowMajorTaggedList}, carries every other). The LITERAL spelling carrier stood here until track L's
     *  L2, 2026-10-29. */
    static SqlSelect rowMajorCellList(SqlSelect rel, Type.RelationType rt,
            String sub) {
        List<SqlExpr> cells = new ArrayList<>();
        var column = SqlExpr.Column.lookup(sub, rel.outputs());
        for (Type.Column c : rt.columns()) {
            cells.add(SqlExpr.Call.of(SqlFn.TO_VARIANT, column.apply(c.name())));
        }
        return SqlSelect.starOf(new SqlSource.Subselect(rel, sub, null))
                .withProjections(List.of(new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.LIST_FLATTEN,
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(
                                        new SqlExpr.ArrayLit(cells)),
                                        false, java.util.List.of())),
                        null,
                        new OutputCol("value", SqlType.Scalar.VARCHAR, true))));
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
