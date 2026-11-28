// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;

/**
 * THE DATABASE JUDGE'S STATEMENTS (item 3, 2026-11-20 to 2026-11-23): an assert's two sides, planned (never executed on
 * their own), composed into typed verdict rows the database answers -- each side's values as typed columns, the verdict
 * the database's comparison of them ({@code __verdict}, never NULL: P-19), a failure's values carried as typed cells
 * for the record the host's builders write ({@code AssertRecords}), so both judges write one record from the same
 * values (register S398's catalog). No failure sentence is planned here, and no Java compares a value. A shape with
 * no typed form is refused by name by the planner that asked for it.
 */
public final class VerdictSql {

    private VerdictSql() {
    }

    public static final String VERDICT = "__verdict";
    /** A caller's own failure message, read where its assert fails ({@link #messageRow}): the value its lambda
     *  computed. */
    public static final String MESSAGE = "__message";

    /** One canon-wrapped side: the wrapped plan, the name of the canon column that decides (one of
     *  {@code __canon<i>}), whether the side is a collection, and whether its elements order by canon text (the
     *  multiset forms) or by arrival (an ordered assert). {@code decimalByValue}: a Decimal side compared by its value,
     *  the scale dropped from its canon (register S139). */
    public record Side(SqlQuery wrapped, String canonColumn, boolean many,
            boolean byCanonText, boolean isFloat, boolean decimalByValue) {
        public Side(SqlQuery wrapped, String canonColumn, boolean many, boolean byCanonText, boolean isFloat) {
            this(wrapped, canonColumn, many, byCanonText, isFloat, false);
        }
    }

    /** A Decimal canon's text with its scale dropped ({@code 0.30} and {@code 0.3} one value, {@code 1.00} and
     *  {@code 1.0} one): the trailing zeros of a fraction cut, then a bare point (S139). */
    private static SqlExpr decimalValue(SqlExpr canon) {
        SqlExpr trimmed = SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, canon,
                new SqlExpr.StringLit("0+$"), new SqlExpr.StringLit("")), new SqlExpr.StringLit("[.]$"),
                new SqlExpr.StringLit(""));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, canon, new SqlExpr.StringLit(".")), new SqlExpr.IntLit(0)), trimmed)),
                canon, SqlExpr.Reason.JUDGE);
    }

    private static SqlExpr canonOf(Side s, SqlExpr canon) {
        return s.decimalByValue() ? decimalValue(canon) : canon;
    }

    private static final String C = "__c";
    private static final String RN = SideEqualities.RN;
    /** The NAMED wrapped grid(s) a grid verdict reads (see {@link SideEqualities#over}). */
    private static final String GRID = "__wg";
    private static final String GRID_E = SideEqualities.GRID_E;
    private static final String GRID_A = SideEqualities.GRID_A;
    private static final String V = "__v";
    /** A row's element as the failure message writes it ({@link Message}); and {@code __o}, the value legend-pure's
     *  sorted form ordered by, unread since item 3's S0 (2026-11-21; its projection goes with the message SQL, S4). */
    private static final String W = "__w";
    private static final String O = SideEqualities.O;

    /** A TYPED verdict row and the schema of its columns (item 3's S4, 2026-11-21): what the database judge defers. */
    public record TypedRow(SqlQuery row, Type.RelationType schema) {
    }

    /** A typed member's expected values: a list in the side's own order, written where the verdict fails. */
    public static final String TYPED_EXPECTED = "__e";
    /** A typed member's actual values. */
    public static final String TYPED_ACTUAL = "__a";
    private static final String LISTED = SideEqualities.LISTED;

    // ---- the equality families: the product's equality compares the two sides ({@link SideEqualities}); the member
    // here is its verdict under the assert's polarity and each side's values riding the failure, for the record ---------

    /** The names a comparison's held values ride a failure under: the expected's, the actual's, then an element's
     *  index (a quantified equality's first failing element). */
    private static final List<String> HELD = List.of(TYPED_EXPECTED, TYPED_ACTUAL, "__i");

    /** A comparison the product's equality made over two sides ({@link SideEqualities.Compared}) as a typed member's
     *  row: the comparison under the assert's polarity ({@code want}: the sides are to be the same), each side's
     *  values written where the verdict fails. Null where the sides have no typed comparison. */
    public static @com.legend.base.Nullable TypedRow typed(SideEqualities.@com.legend.base.Nullable Compared compared,
            boolean want) {
        return typed(compared, want, false);
    }

    /** {@link #typed(SideEqualities.Compared, boolean)}, its verdict computed once beneath the row ({@code once}). */
    public static @com.legend.base.Nullable TypedRow typed(SideEqualities.@com.legend.base.Nullable Compared compared,
            boolean want, boolean once) {
        if (compared == null) {
            return null;
        }
        List<Valued> values = new ArrayList<>();
        for (int i = 0; i < compared.held().size(); i++) {
            SideEqualities.Held held = compared.held().get(i);
            values.add(new Valued(HELD.get(i), held.value(), held.type(), held.kind(), held.many()));
        }
        return typedRow(compared.ctes(), compared.from(), compared.holds(want), values, once);
    }

    /** {@code assertContains}'s member where the values carry their kinds ({@link SideEqualities#containsTagged}). */
    public static @com.legend.base.Nullable TypedRow typedContainsTagged(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, boolean want) {
        return typed(SideEqualities.containsTagged(collWrapped, collKind, valWrapped, valKind), want);
    }

    /** {@code assertContains}'s member over typed values ({@link SideEqualities#contains}). */
    public static @com.legend.base.Nullable TypedRow typedContains(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, boolean want) {
        return typed(SideEqualities.contains(collWrapped, collKind, valWrapped, valKind), want);
    }

    /** A condition's {@code $need->forAll(n | $have->contains($n))} fold ({@link SideEqualities#subset}); its record
     *  the condition's, fixed at planning. */
    public static @com.legend.base.Nullable TypedRow typedSubset(SqlQuery needWrapped, SqlQuery haveWrapped,
            Type kind, boolean wantTrue) {
        return typed(SideEqualities.subset(needWrapped, haveWrapped, kind), wantTrue);
    }

    /** A quantified equality judged element by element ({@link SideEqualities#perElement}): where an element fails,
     *  the first failing one's two operands and its index ride the failure. */
    public static @com.legend.base.Nullable TypedRow typedPerElement(SqlQuery pairs, Type expectedKind,
            Type actualKind, boolean neverEqual) {
        return typed(SideEqualities.perElement(pairs, expectedKind, actualKind, neverEqual), true);
    }

    /** {@code assertEquals} and {@code assertNotEquals} over two tables ({@link SideEqualities#grids}). */
    public static @com.legend.base.Nullable TypedRow typedGridPair(SideEqualities.GridSide e, List<Type> eKinds,
            SideEqualities.GridSide a, List<Type> aKinds, boolean multiset, boolean wantEqual, boolean schemasDiffer,
            Type eRows, Type aRows) {
        return typed(SideEqualities.grids(e, eKinds, a, aKinds, multiset, schemasDiffer, eRows, aRows), wantEqual);
    }


    private static final String K = SideEqualities.K;

    /** {@code assert} / {@code assertFalse}'s TYPED member (item 3's S4): the condition's one Boolean as the side's plan
     *  types it, the verdict its {@code IS NOT DISTINCT FROM} the wanted truth; no value rides the failure (its record
     *  is fixed at planning). Null where the side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedCondition(SqlQuery condWrapped, boolean wantTrue) {
        SqlQuery rows = SideEqualities.valueRows(condWrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery one = SideEqualities.single("__c", rows);
        return typedRow(List.of(new SqlWith.Cte("__c", rows)), new SqlSource.Subselect(one, "c", null),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("c", one.outputs(), SINGLE),
                        new SqlExpr.BoolLit(wantTrue)), List.of());
    }

    /** One side of an assert planned as the condition it means ({@link #typedOverValues}): the name it stands under
     *  in the condition, the column its ONE value rides the failure under, its plan, and its Pure type. */
    public record ValueSide(String name, String column, SqlQuery wrapped, Type kind) {
        /** An assert's two sides of one type, named as its condition names them
         *  ({@code compiler.spec.VerdictQueries.EXPECTED}, {@code ACTUAL}), each under its failure column. */
        public static List<ValueSide> pair(SqlQuery expected, SqlQuery actual, Type kind) {
            return List.of(
                    new ValueSide(com.legend.compiler.spec.VerdictQueries.EXPECTED, TYPED_EXPECTED, expected, kind),
                    new ValueSide(com.legend.compiler.spec.VerdictQueries.ACTUAL, TYPED_ACTUAL, actual, kind));
        }
    }

    /**
     * AN ASSERT AS THE PRODUCT'S OWN CONDITION OVER ITS SIDES' VALUES (the equality program's first slice, 2026-11-26;
     * docs/records/edition-homework/r3/research-step6-equality.md section 4): each side's ONE value read once
     * ({@link #single}) and bound to a column; the verdict {@code holds} over those columns by the sides' names,
     * which the caller has the PRODUCT lower from the Pure condition the assert means
     * ({@code planner.SidePlanner.over}), so no comparison written here is the judge's own; each value rides the
     * failure under its side's column, so the values compared are the values recorded. {@code assertSize}'s member
     * since that day ({@code size(c) == n}: the product's count and the product's {@code ==}; until then
     * {@code typedSize} and {@code typedSizeOfGraph}, the judge's own count of a side's rows or of a graph's root
     * objects and its own IS NOT DISTINCT FROM). Null where a side has no typed value column.
     */
    public static @com.legend.base.Nullable TypedRow typedOverValues(List<ValueSide> sides,
            java.util.function.Function<java.util.Map<String, SqlExpr>, SqlExpr> holds) {
        List<SqlWith.Cte> ctes = new ArrayList<>();
        java.util.Map<String, SqlExpr> bound = new java.util.LinkedHashMap<>();
        List<Valued> values = new ArrayList<>();
        SqlSource from = null;
        for (int i = 0; i < sides.size(); i++) {
            ValueSide side = sides.get(i);
            SqlQuery rows = SideEqualities.valueRows(side.wrapped());
            if (rows == null) {
                return null;
            }
            SqlQuery one = SideEqualities.single("__v" + i, rows);
            ctes.add(new SqlWith.Cte("__v" + i, rows));
            SqlSource read = new SqlSource.Subselect(one, "v" + i, null);
            from = from == null ? read : SideEqualities.cross(from, read);
            SqlExpr value = SqlExpr.Column.of("v" + i, one.outputs(), SINGLE);
            bound.put(side.name(), value);
            values.add(new Valued(side.column(), value, one.outputs().get(0).type(), side.kind(), false));
        }
        if (from == null) {
            throw new IllegalArgumentException("a condition over no side");
        }
        return typedRow(ctes, from, holds.apply(java.util.Collections.unmodifiableMap(bound)), values);
    }

    /** {@code assertEmpty} over a graph side, TYPED (item 3's S4, the last families): its count of root objects zero;
     *  its documents ride the failure as JSON, their root objects read as instances where the record is built, as the
     *  host reads a graph side ({@code Family.EMPTY_GRAPH}; no row per element, which every database spells). Null where
     *  the graph's plan has no value column. */
    public static @com.legend.base.Nullable TypedRow typedEmptyGraph(SqlQuery graphPlan, Type kind) {
        SqlQuery raw = SideEqualities.documentTexts(graphPlan);
        SqlQuery rows = raw == null ? null : SideEqualities.valueRows(raw);
        if (rows == null) {
            return null;
        }
        SqlQuery docs = SideEqualities.jsonDocuments("__xg", rows);
        SqlQuery list = SideEqualities.listed("__dg", docs);
        return typedRow(List.of(new SqlWith.Cte("__xg", rows), new SqlWith.Cte("__dg", docs)),
                new SqlSource.Subselect(list, "a", null), SqlExpr.Call.of(SqlFn.EQUAL, graphCount(graphPlan),
                        new SqlExpr.IntLit(0)), List.of(listValue(TYPED_ACTUAL, "a", list, kind)));
    }

    /** {@code assertNotEmpty} over a graph side, TYPED (item 3's S4): its count of root objects not zero -- a failure
     *  names no value, the side being empty. */
    public static TypedRow typedNotEmptyGraph(SqlQuery graphPlan) {
        return typedRow(List.of(), new SqlSource.Dual(), SqlExpr.Call.of(SqlFn.GREATER, graphCount(graphPlan),
                new SqlExpr.IntLit(0)), List.of(new Valued(TYPED_ACTUAL, SideEqualities.nullOf(new SqlType.Array(SqlType.Scalar.VARCHAR)),
                new SqlType.Array(SqlType.Scalar.VARCHAR), Type.Primitive.STRING, true)));
    }

    /** A graph side's count of root objects, as a scalar subquery over the fold (item 3's S4: a graph's size, typed). */
    private static SqlExpr graphCount(SqlQuery graphPlan) {
        // the canon wrap over a graph side is a pass-through select (the
        // document column beside its canon) around the fold: descend to it
        SqlQuery fold = graphPlan;
        for (int depth = 0; depth < 4 && fold instanceof SqlSelect w
                && !w.projections().isEmpty()
                && w.projections().get(0).expr() instanceof SqlExpr.Column
                && w.from() instanceof SqlSource.Subselect inner; depth++) {
            fold = inner.inner();
        }
        if (!(fold instanceof SqlSelect ps) || ps.projections().size() != 1
                || !ps.groupBy().isEmpty()) {
            throw new IllegalStateException("graph side: not the fold's one-projection select: "
                    + fold.getClass().getSimpleName()
                    + (fold instanceof SqlSelect gs ? " projections=" + gs.projections().stream()
                            .map(pr -> pr.alias() + ":" + pr.expr().getClass().getSimpleName()).toList()
                            + " groupBy=" + gs.groupBy().size() + " from=" + gs.from().getClass().getSimpleName()
                            : ""));
        }
        SqlExpr top = ps.projections().get(0).expr();
        while (true) {
            if (top instanceof SqlExpr.Cast c) {
                top = c.value();
            } else if (top instanceof SqlExpr.Call k && k.fn() == SqlFn.COALESCE
                    && !k.args().isEmpty()) {
                top = k.args().get(0);
            } else {
                break;
            }
        }
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        SqlSelect.Projection countStar = new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__n", n);
        // a value that aggregates its rows into an array (the array itself, or any form a style writes over it -- the
        // edge's one-object-bare choice among them): the same rows, counted the same way (one object bare is one, the
        // host rule's 1). Read by the value's own structure, never by the edge's shape (L-A.foldedAggregate, the
        // lens audit, 2026-11-18)
        if (aggregatesRowsIntoAnArray(top)) {
            return new SqlExpr.ScalarSubquery(ps.withProjections(List.of(countStar)));
        }
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSelect first = new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                ps.distinct(), ps.from(), ps.where(), ps.groupBy(), ps.having(), ps.qualify(),
                ps.orderBy(), 1L, ps.offset(), List.of(one));
        return new SqlExpr.ScalarSubquery(new SqlSelect(List.of(countStar), false,
                new SqlSource.Subselect(first, "w", null), null, List.of(), null, null,
                List.of(), null, null, List.of(n)));
    }

    /** Whether {@code e} holds the array aggregate of its select's rows outside any subquery. */
    private static boolean aggregatesRowsIntoAnArray(SqlExpr e) {
        return e instanceof SqlExpr.JsonArrayAgg
                || e.children().stream().anyMatch(VerdictSql::aggregatesRowsIntoAnArray);
    }

    /** {@code assertEmpty} / {@code assertNotEmpty}'s TYPED member (item 3's S4): the side's values as one list (an empty
     *  side's NULL); the verdict its emptiness under {@code wantEmpty}; the list rides the failure. */
    public static @com.legend.base.Nullable TypedRow typedEmpty(SqlQuery wrapped, Type kind, boolean wantEmpty) {
        if (PlatformTypes.isNil(kind)) {
            // a side the types say holds nothing (Nil): empty, a fact of its type -- decided, as the host decides it
            return typedRow(List.of(), new SqlSource.Dual(), new SqlExpr.BoolLit(wantEmpty), List.of(new Valued(
                    TYPED_ACTUAL, SideEqualities.nullOf(new SqlType.Array(SqlType.Scalar.VARCHAR)),
                    new SqlType.Array(SqlType.Scalar.VARCHAR), kind, true)));
        }
        SqlQuery rows = SideEqualities.valueRows(wrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery list = SideEqualities.listed("__a", rows);
        SqlExpr empty = SideEqualities.noRows("__a", rows);
        return typedRow(List.of(new SqlWith.Cte("__a", rows)), new SqlSource.Subselect(list, "a", null),
                wantEmpty ? empty : SqlExpr.Call.of(SqlFn.NOT, empty),
                List.of(listValue(TYPED_ACTUAL, "a", list, kind)));
    }

    /** {@code assertEmpty} / {@code assertNotEmpty} over a tagged side (the audit of 946d16f2: a mixed list's filter):
     *  its emptiness the verdict, its values -- each its kind and its value -- riding the failure
     *  ({@code Family.EMPTY_TAGGED}). Null where the side has no kinded reading. */
    public static @com.legend.base.Nullable TypedRow typedEmptyTagged(SqlQuery wrapped, Type kind, boolean wantEmpty) {
        java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
        SqlQuery h = SideEqualities.kindsOf(wrapped, kind, held, true) ? SideEqualities.kindedRows(wrapped, kind, held, true) : null;
        if (h == null) {
            return null;
        }
        SqlQuery list = SideEqualities.listed("__hg", SideEqualities.structRows(SideEqualities.cteRows("__gg", h)));
        SqlExpr empty = SideEqualities.noRows("__gg", h);
        return typedRow(List.of(new SqlWith.Cte("__gg", h), new SqlWith.Cte("__hg", SideEqualities.structRows(SideEqualities.cteRows("__gg", h)))),
                new SqlSource.Subselect(list, "a", null), wantEmpty ? empty : SqlExpr.Call.of(SqlFn.NOT, empty),
                List.of(listValue(TYPED_ACTUAL, "a", list, kind)));
    }

    /** {@code assertInstanceOf}'s TYPED member where the value's static type decided it (item 3's S4): the decided
     *  verdict, then the value's typed list and the type's name riding the failure for the host's record
     *  ({@code AssertKind.INSTANCE_OF}). Null where the value has no typed column. */
    public static @com.legend.base.Nullable TypedRow typedInstanceOf(SqlQuery wrapped, Type kind, boolean holds,
            String type) {
        if (holds) {
            // decided true: no failure to record, so no value read (a holding assert need not read it)
            return typedRow(List.of(), new SqlSource.Dual(), new SqlExpr.BoolLit(true), List.of(new Valued(TYPED_ACTUAL,
                    SideEqualities.nullOf(new SqlType.Array(SqlType.Scalar.VARCHAR)), new SqlType.Array(SqlType.Scalar.VARCHAR), kind,
                    true), new Valued("__t", new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING,
                    false)));
        }
        SqlQuery rows = SideEqualities.valueRows(wrapped);
        if (rows == null || !(kind instanceof Type.Primitive) && !(rows.outputs().get(0).type() instanceof SqlType.Struct)) {
            // an instance's value rides as its struct, the host's reading; carried another way, no typed reading here
            return null;
        }
        SqlQuery list = SideEqualities.listed("__a", rows);
        return typedRow(List.of(new SqlWith.Cte("__a", rows)), new SqlSource.Subselect(list, "a", null),
                new SqlExpr.BoolLit(holds), List.of(listValue(TYPED_ACTUAL, "a", list, kind), new Valued("__t",
                        new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING, false)));
    }

    /** A grid's column {@code column} as a side of its values, in the grid's own order (item 3's S4: a one-column grid
     *  against a list). */
    public static @com.legend.base.Nullable SqlQuery gridColumn(SqlQuery grid, String column) {
        // the column the grid declares, with its own type; one it does not declare is no reading of the grid (refused
        // by name, as gridCells refuses it), never a text guessed for it (the audit of 8477c703)
        OutputCol declared = grid.outputs().stream().filter(o -> o.name().equals(column)).findFirst().orElse(null);
        if (declared == null) {
            return null;
        }
        SqlExpr v = SqlExpr.Column.of("w", declared);
        OutputCol out = new OutputCol("value", declared.type(), true);
        return new SqlSelect(List.of(new SqlSelect.Projection(v, "value", out)), false,
                new SqlSource.Subselect(grid, "w", null), null, List.of(), null, null, List.of(), null, null,
                List.of(out));
    }

    /** A grid of more than one column as its CELLS, row-major (the product's flatten of a relation in a value's place,
     *  {@link ValueCollections#rowMajorTaggedList}): one tagged value per cell, its kind its column's, in the grid's
     *  own order -- a list {@link #typedTagged} compares. Null where the grid is no select or a column's kind has no
     *  channel (a class). */
    public static @com.legend.base.Nullable SqlQuery gridCells(SqlQuery grid, Type.RelationType schema) {
        if (!(grid instanceof SqlSelect gs)) {
            return null;
        }
        // the grid's own order first: an unsorted select over a source whose plan defines an order is numbered by that
        // order, as a one-column grid's values are (valueRows) -- the flatten's numbering keyed by it, never by the
        // cells' values (the audit of 3f2fe452: a descending sort was read ascending)
        SqlSelect rel = gs;
        if (gs.orderBy().isEmpty() && SideEqualities.plainProjection(gs) && gs.from() instanceof SqlSource.Subselect sub
                && SideOrdinal.defined(sub.inner()) instanceof SqlQuery inner) {
            // the grid's columns by name over the numbered source (a star would read the ordinal too: H2 refuses the
            // duplicate column), its rows sorted by the ordinal
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (Type.Column c : schema.columns()) {
                // each column the numbered source declares, with its own type; a column it does not declare is no
                // reading of this grid (refused by name, as an untyped grid is), never a text guessed for it
                OutputCol declared = inner.outputs().stream().filter(o -> o.name().equals(c.name())).findFirst()
                        .orElse(null);
                if (declared == null) {
                    return null;
                }
                SqlExpr col = SqlExpr.Column.of(sub.alias(), declared);
                ps.add(new SqlSelect.Projection(col, c.name(), new OutputCol(c.name(), declared.type(), true)));
            }
            rel = new SqlSelect(ps, false, new SqlSource.Subselect(inner, sub.alias(), sub.frameName()), null,
                    List.of(), null, null, List.of(new SqlSelect.SortKey(SqlExpr.Column.of(sub.alias(), inner.outputs(),
                    SideOrdinal.ORD), true, null, null)), null, null,
                    ps.stream().map(p -> java.util.Objects.requireNonNull(p.out())).toList());
        }
        int[] next = {0};
        SqlExpr.TaggedList cells = ValueCollections.rowMajorTaggedList(rel, schema, () -> "__gc" + next[0]++);
        return cells == null ? null : TaggedLists.root(cells, true, "__gc" + next[0]++);
    }

    /**
     * {@code assertJsonStringsEqual}'s TYPED member (item 3's S4, the last families): the golden's canonical text
     * ({@code goldenCanon}: compact, its keys sorted) against the document's ({@code actualCanon}, built with its keys
     * sorted) as two Strings the database compares -- or, where the document's root has no defined order
     * ({@code goldenElements}: the golden root's elements), the two roots' elements as a multiset of texts; the
     * golden's text and the document's as its plan writes it ({@code actualRaw}) ride the failure, the host's record
     * ({@code AssertRecords.jsonDiffers}). Null where a side has no typed value column.
     */
    public static @com.legend.base.Nullable TypedRow typedJson(SqlQuery goldenCanon, SqlQuery actualCanon,
            SqlQuery actualRaw, String golden, @com.legend.base.Nullable SqlQuery goldenElements) {
        SqlQuery aCan = SideEqualities.valueRows(actualCanon);
        SqlQuery aRaw = SideEqualities.valueRows(actualRaw);
        SqlQuery eCan = goldenElements == null ? SideEqualities.valueRows(goldenCanon) : SideEqualities.valueRows(goldenElements);
        if (aCan == null || aRaw == null || eCan == null) {
            return null;
        }
        List<SqlWith.Cte> ctes = new ArrayList<>(List.of(new SqlWith.Cte("__a", aCan), new SqlWith.Cte("__r", aRaw),
                new SqlWith.Cte("__e", eCan)));
        SqlQuery rawList = SideEqualities.listed("__r", aRaw);
        SqlExpr holds;
        SqlSource from;
        if (goldenElements == null) {
            SqlQuery e1 = SideEqualities.single("__e", eCan);
            SqlQuery a1 = SideEqualities.single("__a", aCan);
            holds = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                    new SqlExpr.Cast(SqlExpr.Column.of("e", e1.outputs(), SINGLE), SqlType.Scalar.VARCHAR,
                            SqlExpr.Reason.JUDGE),
                    new SqlExpr.Cast(SqlExpr.Column.of("a", a1.outputs(), SINGLE), SqlType.Scalar.VARCHAR,
                            SqlExpr.Reason.JUDGE));
            from = SideEqualities.cross(new SqlSource.Subselect(e1, "e", null), new SqlSource.Subselect(a1, "a", null));
        } else {
            // the document's root elements, each its canonical text, in the document's order
            OutputCol vOut = new OutputCol("value", SqlType.Scalar.JSON, true);
            SqlExpr doc = new SqlExpr.Cast(SqlExpr.Column.of("d", aCan.outputs(), O), SqlType.Scalar.JSON,
                    SqlExpr.Reason.JUDGE);
            SqlSelect elements = CollectionRelations.rows(SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, doc), "value", vOut,
                    List.of(vOut), new SqlSource.Table("__a", "d", aCan.outputs(), false, java.util.Map.of(),
                            com.legend.sql.NameQuoting.BARE));
            OutputCol oOut = new OutputCol(O, SqlType.Scalar.VARCHAR, false);
            OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
            SqlQuery aEl = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("w",
                    List.of(vOut), "value"), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), O, oOut),
                    new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER,
                            List.of()), List.of(), List.of(), null), RN, rnOut)), false,
                    new SqlSource.Subselect(elements, "w", null), null, List.of(), null, null, List.of(), null, null,
                    List.of(oOut, rnOut));
            ctes.add(new SqlWith.Cte("__ae", aEl));
            SqlQuery eList = SideEqualities.listed("__e", eCan);
            SqlQuery aList = SideEqualities.listed("__ae", aEl);
            holds = SqlExpr.Call.of(SqlFn.SAME_IN_ANY_ORDER, SqlExpr.Column.of("e", eList.outputs(), LISTED),
                    SqlExpr.Column.of("a", aList.outputs(), LISTED), new SqlExpr.ScalarSubquery(SideEqualities.cteRows("__e", eCan)),
                    new SqlExpr.ScalarSubquery(SideEqualities.cteRows("__ae", aEl)));
            from = SideEqualities.cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a", null));
        }
        return typedRow(ctes, SideEqualities.cross(from, new SqlSource.Subselect(rawList, "r", null)), holds, List.of(
                new Valued(TYPED_EXPECTED, new SqlExpr.StringLit(golden), SqlType.Scalar.VARCHAR, Type.Primitive.STRING,
                        false),
                listValue(TYPED_ACTUAL, "r", rawList, Type.Primitive.STRING)));
    }

    /** {@code assertInstanceOf}'s TYPED member where the database decides it (item 3's S4, the last families): the
     *  condition's one Boolean ({@code instanceOf}'s native, planned as a side) the verdict, the value's list and the
     *  type's name riding its failure, as {@link #typedInstanceOf}'s -- a tagged value (a Number among candidates) as
     *  its {@code {k, v0, ...}} struct ({@code Family.INSTANCE_OF_TAGGED}). Null where either has no typed reading. */
    public static @com.legend.base.Nullable TypedRow typedInstanceOfHeld(SqlQuery condWrapped, SqlQuery valWrapped,
            Type kind, String type) {
        SqlQuery cRows = SideEqualities.valueRows(condWrapped);
        if (cRows == null || valWrapped.outputs().isEmpty()) {
            return null;
        }
        List<SqlWith.Cte> ctes = new ArrayList<>(List.of(new SqlWith.Cte("__c", cRows)));
        SqlQuery vList;
        if (valWrapped.outputs().get(0).type() instanceof SqlType.Tagged) {
            java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
            SqlQuery h = SideEqualities.kindsOf(valWrapped, kind, held, true) ? SideEqualities.kindedRows(valWrapped, kind, held, true) : null;
            if (h == null) {
                return null;
            }
            ctes.add(new SqlWith.Cte("__gv", h));
            ctes.add(new SqlWith.Cte("__hv", SideEqualities.structRows(SideEqualities.cteRows("__gv", h))));
            vList = SideEqualities.listed("__hv", SideEqualities.structRows(SideEqualities.cteRows("__gv", h)));
        } else {
            SqlQuery vRows = SideEqualities.valueRows(valWrapped);
            if (vRows == null) {
                return null;
            }
            ctes.add(new SqlWith.Cte("__v", vRows));
            vList = SideEqualities.listed("__v", vRows);
        }
        SqlQuery one = SideEqualities.single("__c", cRows);
        return typedRow(ctes, SideEqualities.cross(new SqlSource.Subselect(one, "c", null), new SqlSource.Subselect(vList, "v", null)),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("c", one.outputs(), SINGLE),
                        new SqlExpr.BoolLit(true)), List.of(listValue(TYPED_ACTUAL, "v", vList, kind), new Valued("__t",
                        new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING, false)));
    }

    /** {@code assertEqWithinTolerance}'s TYPED member (item 3's S4): the three numbers as their plans type them, the
     *  verdict {@code |e - a| <= t} computed in {@code arithmetic} (a NULL operand fails); each number rides the failure
     *  as its own kind. Null where a side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedTolerance(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, SqlQuery tWrapped, Type tKind, SqlType arithmetic) {
        SqlQuery eRows = SideEqualities.valueRows(eWrapped);
        SqlQuery aRows = SideEqualities.valueRows(aWrapped);
        SqlQuery tRows = SideEqualities.valueRows(tWrapped);
        if (eRows == null || aRows == null || tRows == null) {
            return null;
        }
        SqlQuery ev = SideEqualities.single("__e", eRows);
        SqlQuery av = SideEqualities.single("__a", aRows);
        SqlQuery tv = SideEqualities.single("__t", tRows);
        SqlExpr e = SqlExpr.Column.of("e", ev.outputs(), SINGLE);
        SqlExpr a = SqlExpr.Column.of("a", av.outputs(), SINGLE);
        SqlExpr t = SqlExpr.Column.of("t", tv.outputs(), SINGLE);
        SqlExpr within = SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS,
                new SqlExpr.Cast(e, arithmetic, SqlExpr.Reason.JUDGE), new SqlExpr.Cast(a, arithmetic,
                        SqlExpr.Reason.JUDGE))), new SqlExpr.Cast(t, arithmetic, SqlExpr.Reason.JUDGE));
        return typedRow(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows),
                        new SqlWith.Cte("__t", tRows)),
                SideEqualities.cross(SideEqualities.cross(new SqlSource.Subselect(ev, "e", null), new SqlSource.Subselect(av, "a", null)),
                        new SqlSource.Subselect(tv, "t", null)),
                SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false)), List.of(
                        new Valued(TYPED_EXPECTED, e, ev.outputs().get(0).type(), eKind, false),
                        new Valued(TYPED_ACTUAL, a, av.outputs().get(0).type(), aKind, false),
                        new Valued("__t", t, tv.outputs().get(0).type(), tKind, false)));
    }

    /** A quantified {@code assert} / {@code assertFalse}'s vector form, TYPED (item 3's S4): the predicate's Booleans
     *  as the vector's plan types them; the verdict that none is otherwise than {@code wantTrue}; its record fixed at
     *  planning, as the host's vector judgment's. Null where the vector has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedAllOf(SqlQuery vectorWrapped, boolean wantTrue) {
        SqlQuery rows = SideEqualities.valueRows(vectorWrapped);
        if (rows == null) {
            return null;
        }
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        SqlSelect otherwise = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of("__v", rows.outputs(), RN)), false, List.of()), K, kOut)), false,
                new SqlSource.Table("__v", "__v", rows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                        SqlExpr.Column.of("__v", rows.outputs(), O), new SqlExpr.BoolLit(wantTrue))),
                List.of(), null, null, List.of(), null, null, List.of(kOut));
        return typedRow(List.of(new SqlWith.Cte("__v", rows)), new SqlSource.Subselect(otherwise, "o", null),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("o", otherwise.outputs(), K), new SqlExpr.IntLit(0)),
                List.of());
    }

    /** A typed member's value: its column, its expression, its SQL and Pure types, a list or one. */
    private record Valued(String name, SqlExpr expr, SqlType type, Type kind, boolean many) {
    }

    /** A listed side's list ({@link SideEqualities#listed}) as a typed member's value. */
    private static Valued listValue(String name, String alias, SqlQuery listed, Type kind) {
        return new Valued(name, SqlExpr.Column.of(alias, listed.outputs(), LISTED), listed.outputs().get(0).type(), kind,
                true);
    }

    /**
     * A TYPED member's row (item 3's S4): its verdict {@code holds}, then each of {@code values}, written only where
     * the verdict fails -- one row over {@code from}, the members' CTEs at its head -- and the schema of its columns.
     */
    private static TypedRow typedRow(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr holds, List<Valued> values) {
        return typedRow(ctes, from, holds, values, false);
    }

    /** {@link #typedRow(List, SqlSource, SqlExpr, List)}; {@code once}: the verdict is computed once beneath the
     *  row whatever it is -- the product's own comparison of two bound sides, which may be more than a column's
     *  test (two date-times read as instants) and is written three times otherwise (the review of slice 6). */
    private static TypedRow typedRow(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr holds, List<Valued> values,
            boolean once) {
        if (once || comparesSides(holds)) {
            // the comparison node is computed ONCE, beneath the row that writes the values only where it fails (its
            // portable form is a subquery a database would run once per reference)
            List<SqlSelect.Projection> ips = new ArrayList<>();
            List<OutputCol> iOuts = new ArrayList<>();
            OutputCol hOut = new OutputCol("__h", SqlType.Scalar.BOOLEAN, false);
            ips.add(new SqlSelect.Projection(holds, "__h", hOut));
            iOuts.add(hOut);
            for (int i = 0; i < values.size(); i++) {
                OutputCol x = new OutputCol("__x" + i, values.get(i).type(), true);
                ips.add(new SqlSelect.Projection(values.get(i).expr(), x.name(), x));
                iOuts.add(x);
            }
            SqlSelect inner = new SqlSelect(ips, false, from, null, List.of(), null, null, List.of(), null, null,
                    iOuts);
            List<Valued> lifted = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                Valued v = values.get(i);
                lifted.add(new Valued(v.name(), SqlExpr.Column.of("w", iOuts, "__x" + i), v.type(), v.kind(), v.many()));
            }
            from = new SqlSource.Subselect(inner, "w", null);
            holds = SqlExpr.Column.of("w", iOuts, "__h");
            values = lifted;
        }
        OutputCol vOut = new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false);
        List<SqlSelect.Projection> ps = new ArrayList<>(List.of(new SqlSelect.Projection(holds, VERDICT, vOut)));
        List<OutputCol> outs = new ArrayList<>(List.of(vOut));
        List<Type.RelationType.Column> cols = new ArrayList<>(List.of(new Type.RelationType.Column(VERDICT,
                Type.Primitive.BOOLEAN, com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
        for (Valued v : values) {
            OutputCol o = new OutputCol(v.name(), v.type(), true);
            ps.add(new SqlSelect.Projection(whereFails(holds, v.expr()), v.name(), o));
            outs.add(o);
            cols.add(new Type.RelationType.Column(v.name(), v.kind(), v.many()
                    ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                    : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE));
        }
        SqlSelect row = new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null, outs);
        return new TypedRow(nanos(ctes.isEmpty() ? row : new SqlWith(ctes, row)), new Type.RelationType(cols));
    }

    /** {@code member} with its judge's instants read to the nanosecond where a value it compares carries a digit of a
     *  second past the microsecond -- a literal's, a fact of the plan, never a row's (register S399; S181 and the
     *  user's rule of 2026-11-22: "do nanoseconds typed for whatever range allows, refuse if out of range"): every
     *  {@code JUDGE} cast to TIMESTAMP becomes {@code JUDGE_NANOS}, which a database whose date-time holds
     *  microseconds reads in its nanosecond type where it has one, refusing by name a value outside the days that
     *  type holds (DuckDB's); an assert with no such value is read to the microsecond, every year. */
    private static SqlQuery nanos(SqlQuery member) {
        boolean[] carries = {false};
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                carries[0] |= digitsPastMicros(e);
                return e;
            }
        }.rewrite(member);
        if (!carries[0]) {
            return member;
        }
        return new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                return e instanceof SqlExpr.Cast c && c.reason() == SqlExpr.Reason.JUDGE
                        && c.target() == SqlType.Scalar.TIMESTAMP
                        ? new SqlExpr.Cast(c.value(), SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE_NANOS) : e;
            }
        }.rewrite(member);
    }

    /** Whether {@code e} is a date-time literal -- typed, or its text carrier ({@code TEMPORAL_TEXT}, a root literal's
     *  written precision: RootLiterals) -- with a nonzero digit of a second past the sixth. A String literal is none,
     *  whatever it spells (the precision audit's M2, 2026-11-22: a filter's text switched an assert to nanoseconds). */
    private static boolean digitsPastMicros(SqlExpr e) {
        if (e instanceof SqlExpr.Call ns && ns.fn() == SqlFn.FROM_EPOCH_NS) {
            // a date-time computed to the nanosecond (an adjust by nanoseconds: DateShifts' epoch count): a fact of the
            // plan too (the instances audit's M-a, 2026-11-22: its value, a TIMESTAMP_NS, was refused where DuckDB
            // compares it)
            return true;
        }
        if (e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.TEMPORAL_TEXT
                && !(c.value() instanceof SqlExpr.StringLit)) {
            // a literal collection's carrier: its elements' texts inside (LiteralSpelling.staticTemporalText; the
            // instances audit's H-A, 2026-11-22: a list of nanosecond literals was read to the microsecond)
            boolean[] found = {false};
            new com.legend.sql.SqlRewriter() {
                @Override
                protected SqlExpr expr(SqlExpr x) {
                    found[0] |= x instanceof SqlExpr.StringLit sl && fractionDigits(sl.value()) > 6;
                    return x;
                }
            }.rewrite(SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(
                    new SqlSelect.Projection(c.value(), "v", null))));
            return found[0];
        }
        String text = e instanceof SqlExpr.TimestampLit t ? t.iso()
                : e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.TEMPORAL_TEXT
                        && c.value() instanceof SqlExpr.StringLit s ? s.value() : null;
        return text != null && fractionDigits(text) > 6;
    }

    /** The significant digits of a second a date-time's text writes (its trailing zeros no digit). */
    private static int fractionDigits(String text) {
        if (text.indexOf('.') < 0) {
            return 0;
        }
        int at = text.indexOf('.') + 1;
        int end = at;
        while (end < text.length() && Character.isDigit(text.charAt(end))) {
            end++;
        }
        String digits = text.substring(at, end);
        int significant = digits.length();
        while (significant > 0 && digits.charAt(significant - 1) == '0') {
            significant--;
        }
        return significant;
    }

    /** Whether {@code holds} is the sides' comparison node, or its negation. */
    private static boolean comparesSides(SqlExpr holds) {
        SqlExpr h = holds instanceof SqlExpr.Call n && n.fn() == SqlFn.NOT ? n.args().get(0) : holds;
        return h instanceof SqlExpr.Call c && (c.fn() == SqlFn.SAME_IN_ORDER || c.fn() == SqlFn.SAME_IN_ANY_ORDER);
    }

    private static final String SINGLE = "__v";

    /** {@code value} where the verdict {@code holds} not, else NULL: a typed member's values ride its failure alone. */
    private static SqlExpr whereFails(SqlExpr holds, SqlExpr value) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(holds, new SqlExpr.NullLit())), value,
                SqlExpr.Reason.JUDGE);
    }

    // ── the ONE-LINE families (leg 3.1c): size / empty / contains / a boolean
    // condition / the tolerance assert / the forAll-contains subset — each a
    // predicate over the same row sources, returned in the same verdict row.

    /** {@code assertSize}: the side's row count against {@code n} (the size
     * side's one canon text as a BIGINT); {@code envelope} = the read is a
     * relation-rooted execute's {@code .values}, which holds ONE TDS. */

    /** A GRAPH-shaped side (a class collection serialized as ONE JSON
     * document): its size is the number of ROOT ROWS the fold aggregates —
     * the host rule ({@code p instanceof List ? size : 1}) read off the
     * PLAN, not the document. An array-wrapped root ({@code JsonArrayAgg}
     * under the fold's VARCHAR cast / empty-array COALESCE) counts the rows
     * under the aggregate; a bare-object root is one document when a row
     * exists and NULL (0) otherwise. No JSON function on any dialect, no
     * document built to be measured. A plan that is not the fold's
     * one-projection select is a construction fault, loud. */
    /** A graph plan's ROOT ROWS (rung 12): the fold's own from / where / caps
     * projecting the root table's PHYSICAL columns ({@code root.*} — the
     * store's declared list can exceed what a seeded table carries; a reader
     * that names an absent column fails exactly as it would over the table).
     * Null when the plan is not a fold over a single root TABLE. */
    public static @com.legend.base.Nullable SqlSelect classExtentRows(SqlQuery graphPlan) {
        SqlQuery fold = graphPlan;
        for (int depth = 0; depth < 4 && fold instanceof SqlSelect w
                && !w.projections().isEmpty()
                && w.projections().get(0).expr() instanceof SqlExpr.Column
                && w.from() instanceof SqlSource.Subselect inner; depth++) {
            fold = inner.inner();
        }
        if (!(fold instanceof SqlSelect ps) || ps.projections().size() != 1
                || !ps.groupBy().isEmpty()) {
            return null;
        }
        SqlSource leftmost = ps.from();
        while (leftmost instanceof SqlSource.Join j) {
            leftmost = j.left();
        }
        if (!(leftmost instanceof SqlSource.Table root) || root.outputs().isEmpty()) {
            return null;
        }
        List<SqlSelect.Projection> ps2 = List.of(
                new SqlSelect.Projection(new SqlExpr.Star(root.alias()), null, null));
        return new SqlSelect(ps2, ps.distinct(), ps.from(), ps.where(), ps.groupBy(), ps.having(),
                ps.qualify(), ps.orderBy(), ps.limit(), ps.offset(), root.outputs());
    }

    /** {@code assertEmpty} / {@code assertNotEmpty}: the side's row count. {@code written}: the rows carry each element
     *  as the failure message writes it ({@link #writtenRows}), else the message names the count alone. */

    /** {@code assertContains}: some element's canon equals the value's. {@code valueWritten}: the value's message
     *  (its kind and the planning style), where the collection's rows are {@link #writtenRows} and the message writes
     *  both; null, the canons'. */

    /**
     * A quantified EQUALITY judged element by element (the asserts-as-plan-nodes program, step 5, 2026-11-04):
     * {@code pairs}, the plan of {@code source->map(x | pair(expected, actual))} -- each element's two operands one row's
     * {@code {first, second}}, in the source's order -- and the verdict that every element's operands are equal by the
     * database judge's own spelling of their kinds ({@link LiteralSpelling#leaf}: a Decimal's scale kept); a failure's
     * texts the FIRST failing element's, each as upstream's {@code %r} writes it ({@link Repr}), so the row's message is
     * upstream's {@code '\nexpected: %r\nactual:   %r'} for that element. {@code neverEqual}: the two kinds are never
     * equal under the run's rules (register S127): every element fails, an empty source holds. {@code floats}: both are
     * Floats, two finite doubles within 2 ULP equal too (S15, as the scalar equality's leniency). Null where a kind has no
     * spelling (the caller declines by name). One statement: the pairs read once, numbered in their own select.
     */

    /** {@code assertEqWithinTolerance(e, a, tol)}: {@code |e − a| ≤ tol} over the three sides' canon texts, computed
     *  in {@code arithmetic} -- the sides' own kind (the user's ruling, 2026-11-03: never more lenient than upstream's
     *  exact {@code abs($e-$a) <= abs($delta)}): BIGINT for Integers, DECIMAL for Decimals, DOUBLE where a side is a
     *  Float (the database's Float). */

    /**
     * {@code assertTdsEquivalent}'s TYPED member (item 3's S4, tables): the verdict the database computes over each
     * grid's typed cells, as before -- the same count of cells, and no pair of cells joined by position that is not
     * within: two numbers within {@code |delta|}, two dates within {@code |timeDelta|} seconds, any other two the same,
     * two NULLs alike (the host's {@code TdsCompare.tdsEquivalent}) -- or false at planning where the two grids'
     * columns differ ({@code columnsDiffer}). Where it fails, each grid's rows ride back as a list of row structs in its
     * order, and the delta, for the host's record ({@code AssertKind.TDS_EQUIVALENT}); until then the row's sentence
     * written in SQL. {@code eRows}, {@code aRows}: each grid's relation type, the kind its rows decode by.
     */
    public static @com.legend.base.Nullable TypedRow typedTdsEquivalent(SideEqualities.GridSide one, SideEqualities.GridSide two, List<Type> kinds,
            List<Type> twoKinds, SqlQuery deltaRows, SqlQuery timeDeltaRows, boolean columnsDiffer, Type eRows,
            Type aRows) {
        if (!SideEqualities.carriesCells(one) || !SideEqualities.carriesCells(two)) {
            return null;
        }
        SqlQuery e = toleranceCells(one, kinds, GRID_E);
        SqlQuery a = toleranceCells(two, twoKinds, GRID_A);
        SqlExpr delta = new SqlExpr.Cast(scalarOver("__d", col("__d", C), "__one",
                SqlType.Scalar.VARCHAR, 1L), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
        SqlExpr timeDelta = new SqlExpr.Cast(scalarOver("__t", col("__t", C), "__one",
                SqlType.Scalar.VARCHAR, 1L), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
        SqlExpr en = SqlExpr.Column.of("__e", "__n", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr an = SqlExpr.Column.of("__a", "__n", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr es = SqlExpr.Column.of("__e", "__s", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr as = SqlExpr.Column.of("__a", "__s", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr within = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, en),
                                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, an)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, en, an)),
                                        SqlExpr.Call.of(SqlFn.ABS, delta))),
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, es),
                                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, as)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, es, as)),
                                        SqlExpr.Call.of(SqlFn.ABS, timeDelta)))),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__e", C), col("__a", C)));
        // the BAD pairs: cells joined by position that are not within
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlSelect bad = new SqlSelect(List.of(new SqlSelect.Projection(col("__e", RN), RN, rnOut)), false,
                new SqlSource.Join(cte("__e"), cte("__a"), SqlSource.Join.Kind.INNER,
                        SqlExpr.Call.of(SqlFn.EQUAL, col("__e", RN), col("__a", RN))),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false))),
                List.of(), null, null, List.of(), null, null, List.of(rnOut));
        SqlExpr holds = columnsDiffer ? new SqlExpr.BoolLit(false) : SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, count("__e"), count("__a")),
                SqlExpr.Call.of(SqlFn.EQUAL, count("__p"), new SqlExpr.IntLit(0)));
        OutputCol hOut = new OutputCol(SAME_H, SqlType.Scalar.BOOLEAN, false);
        SqlSelect same = new SqlSelect(List.of(new SqlSelect.Projection(holds, SAME_H, hOut)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(hOut));
        SqlQuery eList = SideEqualities.listed("__ge", SideEqualities.rowStructs(one, GRID_E, kinds));
        SqlQuery aList = SideEqualities.listed("__ga", SideEqualities.rowStructs(two, GRID_A, twoKinds));
        OutputCol dOut = new OutputCol("__dv", SqlType.Scalar.DOUBLE, true);
        SqlSelect deltaValue = new SqlSelect(List.of(new SqlSelect.Projection(delta, "__dv", dOut)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(dOut));
        List<SqlWith.Cte> ctes = List.of(new SqlWith.Cte(GRID_E, one.wrapped()), new SqlWith.Cte(GRID_A, two.wrapped()),
                new SqlWith.Cte("__e", e), new SqlWith.Cte("__a", a), new SqlWith.Cte("__d", deltaRows),
                new SqlWith.Cte("__t", timeDeltaRows), new SqlWith.Cte("__p", bad),
                new SqlWith.Cte("__ge", SideEqualities.rowStructs(one, GRID_E, kinds)),
                new SqlWith.Cte("__ga", SideEqualities.rowStructs(two, GRID_A, twoKinds)));
        return typedRow(ctes, SideEqualities.cross(SideEqualities.cross(SideEqualities.cross(new SqlSource.Subselect(same, "q", null),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                        new SqlSource.Subselect(deltaValue, "d", null)),
                SqlExpr.Column.of("q", same.outputs(), SAME_H), List.of(listValue(TYPED_EXPECTED, "e", eList, eRows),
                        listValue(TYPED_ACTUAL, "a", aList, aRows), new Valued("__dv", SqlExpr.Column.of("d",
                                deltaValue.outputs(), "__dv"), SqlType.Scalar.DOUBLE, Type.Primitive.FLOAT, false)));
    }

    private static final String SAME_H = "__holds";

    /** A grid's cells row-major with a NUMERIC value ({@code __n}, any
     * numeric kind) and a TEMPORAL value in epoch seconds ({@code __s}). */
    private static SqlQuery toleranceCells(SideEqualities.GridSide grid, List<Type> kinds, String named) {
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol nOut = new OutputCol("__n", SqlType.Scalar.DOUBLE, true);
        OutputCol sOut = new OutputCol("__s", SqlType.Scalar.DOUBLE, true);
        List<SqlSelect.Projection> values = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size()))
                : List.of();
        List<SqlExpr> cells = new ArrayList<>();
        List<SqlExpr> ns = new ArrayList<>();
        List<SqlExpr> secs = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(gridCell(grid, i));
            Type k = i < kinds.size() ? kinds.get(i) : null;
            String alias = i < values.size() ? values.get(i).alias() : null;
            boolean numeric = k == Type.Primitive.INTEGER || k == Type.Primitive.FLOAT
                    || k == Type.Primitive.DECIMAL || k == Type.Primitive.NUMBER
                    || k instanceof Type.PrecisionDecimal;
            boolean temporal = k == Type.Primitive.DATE_TIME || k == Type.Primitive.STRICT_DATE
                    || k == Type.Primitive.DATE;
            // a cell the side carries as a temporal value is read as one (a literal grid's timestamp: a text cast of
            // it is no timestamp's on Trino), any other as the text it arrives as
            SqlExpr raw = alias == null ? null
                    : temporal && carriedTemporal(grid.wrapped().outputs(), alias)
                            ? SqlExpr.Column.of("w", grid.wrapped().outputs(), alias)
                            : SqlExpr.Column.of("w", alias, SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED);
            ns.add(raw != null && numeric ? new SqlExpr.Cast(raw, SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                    : new SqlExpr.NullLit());
            // a temporal cell casts to a timestamp for its epoch: a computed
            // one arrives DECODED as text (the fetch conformance: nine-digit
            // temporals)
            secs.add(raw != null && temporal
                    ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.EPOCH_SECONDS,
                            new SqlExpr.Cast(raw, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE)), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                    : new SqlExpr.NullLit());
        }
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(pick(cells), C, cOut),
                        new SqlSelect.Projection(position(grid.width()), RN, rnOut),
                        new SqlSelect.Projection(pick(ns), "__n", nOut),
                        new SqlSelect.Projection(pick(secs), "__s", sOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut, rnOut, nOut, sOut));
    }

    /** Whether {@code name} among {@code outs} is a date or a timestamp value (not its text). */
    private static boolean carriedTemporal(List<OutputCol> outs, String name) {
        return outs.stream().anyMatch(o -> o.name().equals(name)
                && (o.type() == SqlType.Scalar.TIMESTAMP || o.type() == SqlType.Scalar.TIMESTAMPTZ
                        || o.type() == SqlType.Scalar.DATE));
    }

    private static final String P = "__p";

    /** {@code plan}'s first output as a one-row relation {@code (value)} (a one-row seed LEFT JOINed to the plan, so an
     *  empty plan is one NULL row, as {@link #scalarRow}'s): a rendered text's value, no canon (2026-11-04). */
    public static SqlQuery firstValueRow(SqlQuery plan) {
        OutputCol first = plan.outputs().get(0);
        OutputCol vOut = new OutputCol("value", first.type(), true);
        OutputCol seedOut = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSource seed = new SqlSource.Subselect(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", seedOut)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                List.of(seedOut)), "__one", null);
        SqlSource from = new SqlSource.Join(seed, new SqlSource.Subselect(plan, "w", null), SqlSource.Join.Kind.LEFT,
                new SqlExpr.BoolLit(true));
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("w", plan.outputs(), first.name()),
                "value", vOut)), false, from, null, List.of(), null, null, List.of(), null, null, List.of(vOut));
    }

    /** A side's rows for the predicate forms: a grid's row canons, or a
     * scalar / collection side's canons (NULL values dropped). */
    public static SqlQuery sideRows(SqlQuery wrapped, boolean grid, String canonColumn, boolean many) {
        return grid ? gridRowCanons(wrapped)
                : canonRows(new Side(wrapped, canonColumn, many, false, false));
    }

    /** A HOST-CONSTANT side (a compile-time fact the pipeline answered
     * without SQL — a generated seed-data string, a rendered DDL text, a
     * folded literal) bound as a VALUES relation, so the comparison still
     * happens in the database (TWO_DESIGN_LEGS §2.6 blocker 1:
     * database-ADJUDICATED, not database-computed — the ledger says
     * which). Null when a value has no literal spelling here (the caller
     * reports it unjudged by kind). */
    public static @com.legend.base.Nullable SqlQuery constantPlan(List<Object> values) {
        List<List<SqlExpr>> rows = new ArrayList<>();
        SqlType type = SqlType.Scalar.VARCHAR;   // the EMPTY constant's column kind
        for (Object v : values) {
            SqlExpr lit;
            SqlType t;
            switch (v) {
                case String str -> { lit = new SqlExpr.StringLit(str); t = SqlType.Scalar.VARCHAR; }
                case Long l -> { lit = new SqlExpr.IntLit(l); t = SqlType.Scalar.BIGINT; }
                case Integer i -> { lit = new SqlExpr.IntLit(i); t = SqlType.Scalar.BIGINT; }
                case Boolean b -> { lit = new SqlExpr.BoolLit(b); t = SqlType.Scalar.BOOLEAN; }
                case null, default -> { return null; }
            }
            if (!rows.isEmpty() && type != t) {
                return null;   // a mixed constant collection has no one column type
            }
            type = t;
            rows.add(List.of(lit));
        }
        OutputCol out = new OutputCol("value", type, rows.isEmpty());
        SqlSource src = rows.isEmpty()
                ? new SqlSource.Subselect(new SqlSelect(
                        List.of(new SqlSelect.Projection(new SqlExpr.NullLit(), "value", out)),
                        false, new SqlSource.Dual(),
                        new SqlExpr.BoolLit(false), List.of(), null, null, List.of(),
                        null, null, List.of(out)), "k", null)
                : new SqlSource.Values(rows, List.of("value"), "k", List.of(out));
        return new SqlSelect(
                List.of(new SqlSelect.Projection(SqlExpr.Column.of("k", out), "value", out)),
                false, src, null, List.of(), null, null, List.of(), null, null, List.of(out));
    }

    /** A fused statement's column naming the member a row is ({@link #batch}). */
    public static final String MEMBER = "__m";

    /** A fused verdict statement ({@link #batch}): the statement, its columns' schema, and each member's row as the
     *  fused columns that hold it, in the member's own order. */
    public record Batch(SqlQuery query, Type.RelationType schema, List<List<Integer>> layout) {
        public Batch {
            layout = layout.stream().map(List::copyOf).toList();
        }
    }

    /** A fused statement's shared column: the members' values at {@code position} of one SQL type and one Pure type
     *  share it -- a member read to the nanosecond ({@code nanos}: {@link #nanos}) only with another such, so a
     *  nanosecond date-time never shares a column with another assert's year past what a nanosecond timestamp holds
     *  (the precision audit's H3, 2026-11-22: DuckDB unified the column to TIMESTAMP_NS and refused the year). */
    private record Slot(int position, SqlType sql, Type kind, com.legend.compiler.element.type.Multiplicity many,
            boolean nanos) {
    }

    /** Whether {@code member} reads a date-time to the nanosecond ({@code JUDGE_NANOS}, {@link #nanos}). */
    private static boolean readsNanos(SqlQuery member) {
        boolean[] found = {false};
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                found[0] |= e instanceof SqlExpr.Cast c && c.reason() == SqlExpr.Reason.JUDGE_NANOS;
                return e;
            }
        }.rewrite(member);
        return found[0];
    }

    /** LEG 3.4: several verdict statements as ONE. Every statement's CTEs are hoisted to ONE top-level {@code WITH}
     * under per-statement names ({@code __e} of statement 3 is {@code __e_3}; the CTE's own alias is unchanged, so its
     * column references stand). The statement is TALL (item 3's S4, 2026-11-22, the user: "Don't we just need 1 column
     * per type that each assert reuses?"): a row per member, a {@code UNION ALL} branch, its {@link #MEMBER} number
     * first; then ONE column per value position and type that some member writes -- each member's value at position
     * {@code j} of SQL type {@code T} and Pure type {@code P} in the column all such values share, NULL in the columns
     * it does not write. So the row's width is the types the statement uses, never its asserts' count, and no
     * database's join limit applies (until then one wide row, a derived table per member CROSS JOINed: SQLite joins at
     * most 64 tables, MariaDB and MySQL 61 -- the S4 audit's MEDIUM 2). Each member's columns are the {@code layout}
     * the plan carries. (A {@code WITH} inside a derived table is NOT this shape: H2 blew its heap on one such branch,
     * a metamodel read with JSON aggregation -- the Linux-independent catch of 2026-09-20; the members' CTEs are
     * hoisted.) */
    public static Batch batch(List<SqlQuery> statements, List<Type.RelationType> schemas,
            java.util.Map<String, SqlQuery> frames) {
        // leg 3.4 step 2: the frames the sides reference, DEFINED once at the
        // head (MATERIALIZED: every side sees the same rows), ahead of all
        List<SqlWith.Cte> ctes = new ArrayList<>();
        java.util.Set<String> used = new java.util.LinkedHashSet<>();
        for (SqlQuery st : statements) {
            used.addAll(com.legend.sql.FrameCtes.referenced(st));
        }
        for (var f : frames.entrySet()) {
            if (used.contains(f.getKey())) {
                ctes.add(new SqlWith.Cte(f.getKey(), f.getValue(), true));
            }
        }
        // A CTE whose body is IDENTICAL to one already hoisted (after the
        // renames) is not hoisted again: its name maps to the first. The asserts
        // of one body often read the same relation — each spelled its own copy
        // of the formatted grid (a 30 KB canon each, 2026-09-23). Walking in
        // order makes it compound: once two grids are one, the cell stacks over
        // them are identical too. Frames stay as they are (named, materialized).
        java.util.Map<SqlQuery, String> hoisted = new java.util.HashMap<>();
        List<SqlQuery> bodies = new ArrayList<>(statements.size());
        List<Slot> slots = new ArrayList<>();
        List<List<OutputCol>> owned = new ArrayList<>(statements.size());
        List<List<Integer>> layout = new ArrayList<>(statements.size());
        for (int i = 0; i < statements.size(); i++) {
            SqlQuery st = statements.get(i);
            SqlQuery body = st;
            if (st instanceof SqlWith w) {
                java.util.Map<String, String> names = new java.util.LinkedHashMap<>();
                RenameCtes rename = new RenameCtes(names);
                for (SqlWith.Cte c : w.ctes()) {
                    SqlQuery q = rename.rewriteRoot(c.query());
                    String same = hoisted.get(q);
                    if (same != null && !c.materialized()) {
                        names.put(c.name(), same);
                        continue;
                    }
                    String name = c.name() + "_" + i;
                    names.put(c.name(), name);
                    hoisted.putIfAbsent(q, name);
                    ctes.add(new SqlWith.Cte(name, q, c.materialized()));
                }
                body = rename.rewriteRoot(w.body());
            }
            bodies.add(body);
            // the member's own columns, in its schema's order, by name
            // each in the shared column of its position and types
            List<OutputCol> mine = new ArrayList<>();
            List<Integer> at = new ArrayList<>();
            boolean nanos = readsNanos(st);
            List<Type.RelationType.Column> cols = schemas.get(i).columns();
            for (int j = 0; j < cols.size(); j++) {
                Type.RelationType.Column c = cols.get(j);
                OutputCol o = body.outputs().stream().filter(x -> x.name().equals(c.name())).findFirst()
                        .orElseThrow(() -> new IllegalStateException("a verdict row without its " + c.name()));
                Slot slot = new Slot(j, o.type(), c.type(), c.multiplicity(), nanos);
                int k = slots.indexOf(slot);
                if (k < 0) {
                    slots.add(slot);
                    k = slots.size() - 1;
                }
                mine.add(o);
                at.add(k + 1);   // the fused columns: the member's number first
            }
            owned.add(mine);
            layout.add(at);
        }
        List<OutputCol> outs = new ArrayList<>(List.of(new OutputCol(MEMBER, SqlType.Scalar.BIGINT, false)));
        List<Type.RelationType.Column> schema = new ArrayList<>(List.of(new Type.RelationType.Column(MEMBER,
                Type.Primitive.INTEGER, com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
        for (int k = 0; k < slots.size(); k++) {
            Slot slot = slots.get(k);
            outs.add(new OutputCol("__s" + k, slot.sql(), true));
            // a slot holds NULL in every branch that writes none: at most one value, or a list (the tall-row audit's
            // LOW 3: until then the member's own multiplicity, [1] for a column another member leaves NULL)
            schema.add(new Type.RelationType.Column("__s" + k, slot.kind(), slot.many().isMany()
                    ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                    : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE));
        }
        List<SqlQuery> branches = new ArrayList<>(bodies.size());
        for (int i = 0; i < bodies.size(); i++) {
            SqlQuery body = bodies.get(i);
            java.util.Map<Integer, OutputCol> byColumn = new java.util.HashMap<>();
            for (int j = 0; j < owned.get(i).size(); j++) {
                byColumn.put(layout.get(i).get(j), owned.get(i).get(j));
            }
            List<SqlSelect.Projection> ps = new ArrayList<>(outs.size());
            ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(i), MEMBER, outs.get(0)));
            for (int c = 1; c < outs.size(); c++) {
                OutputCol own = byColumn.get(c);
                ps.add(new SqlSelect.Projection(own == null ? SideEqualities.nullOf(outs.get(c).type())
                        : SqlExpr.Column.of("v", body.outputs(), own.name()), outs.get(c).name(), outs.get(c)));
            }
            branches.add(new SqlSelect(ps, false, new SqlSource.Subselect(body, "v", null), null, List.of(), null,
                    null, List.of(), null, null, outs));
        }
        SqlQuery union = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        return new Batch(ctes.isEmpty() ? union : new SqlWith(ctes, union), new Type.RelationType(schema), layout);
    }

    /** CTE references renamed (a table source named after a CTE; the
     * alias — what column references spell — is kept). */
    private static final class RenameCtes extends com.legend.sql.SqlRewriter {
        private final java.util.Map<String, String> names;

        RenameCtes(java.util.Map<String, String> names) {
            this.names = names;
        }

        @Override
        protected SqlSource source(SqlSource s) {
            if (s instanceof SqlSource.Table t) {
                String renamed = names.get(t.name());
                if (renamed != null) {
                    return new SqlSource.Table(renamed, t.alias(), t.outputs(), t.call(), t.storedTypes(), t.quoting());
                }
            }
            return s;
        }
    }

    /** {@code side}'s rows in their branches' order where it is a UNION ALL (directly or under pass-through selects),
     *  for the HOST judge's read of a side: the order the database judge numbers the same side by
     *  ({@code SideOrdinal.inBranchOrder}, 2026-11-06); {@code side} itself otherwise. */
    public static SqlQuery inBranchOrder(SqlQuery side) {
        return SideOrdinal.inBranchOrder(side);
    }

    /** What running an assertError's function raised, as its verdict statement reads it: its message, line and column,
     *  each a value or absent (SQL NULL) -- the runner's bound parameters ({@link #bound}), or constants where the
     *  function raised while it was planned ({@link #raised}). */
    public record Raised(SqlExpr message, SqlExpr line, SqlExpr column) {

        /** The outcome the runner binds once it ran the function: the three parameters the plan names. */
        public static Raised bound(String messageParameter, String lineParameter, String columnParameter) {
            return new Raised(param(messageParameter, SqlType.Scalar.VARCHAR),
                    param(lineParameter, SqlType.Scalar.BIGINT), param(columnParameter, SqlType.Scalar.BIGINT));
        }

        /** An error raised while the function was planned: its message, and its position where it carried one. */
        public static Raised raised(String message, @com.legend.base.Nullable Long line,
                @com.legend.base.Nullable Long column) {
            return new Raised(new SqlExpr.StringLit(message), lineOrNull(line), lineOrNull(column));
        }

        private static SqlExpr lineOrNull(@com.legend.base.Nullable Long n) {
            return n == null ? new SqlExpr.NullLit(com.legend.sql.SqlTyping.typed(SqlType.Scalar.BIGINT))
                    : new SqlExpr.IntLit(n);
        }

        private static SqlExpr param(String name, SqlType type) {
            return new SqlExpr.PlanParam(name, type == SqlType.Scalar.VARCHAR ? SqlExpr.PlanParam.ValueType.STRING
                    : SqlExpr.PlanParam.ValueType.OTHER, true, com.legend.sql.SqlTyping.typed(type), null);
        }
    }

    /**
     * ASSERTERROR'S VERDICT ROW, TYPED (the user's ruling S135, 2026-11-05: the host catches, the database judges; item
     * 3's S4, the last families): what the function raised ({@code raised}, read once through a one-row derived table)
     * compared with the expected message, line and column BY THE DATABASE -- nothing raised holds only where the assert
     * expects upstream's own "nothing was raised" text ({@code passesUnraised}, decided at planning; item 3's S8, Q-B);
     * else the message, then the line, then the column, each only where it is expected, an expected position over a
     * raise that carried none failing, as upstream's {@code si.line->toOne()} does -- the row its verdict and the three
     * raised values ({@link #errorSchema}), the run's record the host's builders' ({@code AssertErrorRuns}). No word is
     * in the statement.
     */
    public static SqlQuery assertError(String expected, @com.legend.base.Nullable Long line,
            @com.legend.base.Nullable Long column, Raised raised, boolean passesUnraised) {
        OutputCol mOut = new OutputCol("m", SqlType.Scalar.VARCHAR, true);
        OutputCol lOut = new OutputCol("l", SqlType.Scalar.BIGINT, true);
        OutputCol cOut = new OutputCol("c", SqlType.Scalar.BIGINT, true);
        SqlSelect outcome = new SqlSelect(List.of(new SqlSelect.Projection(raised.message(), "m", mOut),
                new SqlSelect.Projection(raised.line(), "l", lOut), new SqlSelect.Projection(raised.column(), "c", cOut)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                List.of(mOut, lOut, cOut));
        SqlExpr m = SqlExpr.Column.of("o", mOut);
        SqlExpr l = SqlExpr.Column.of("o", lOut);
        SqlExpr c = SqlExpr.Column.of("o", cOut);
        SqlExpr matched = SqlExpr.Call.of(SqlFn.EQUAL, m, new SqlExpr.StringLit(expected));
        if (line != null || column != null) {
            // a position is its line and its column, both carried (AssertRecords.errorFailure reads the two)
            matched = SqlExpr.Call.of(SqlFn.AND, matched, SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, l),
                    SqlExpr.Call.of(SqlFn.IS_NOT_NULL, c)));
        }
        if (line != null) {
            matched = SqlExpr.Call.of(SqlFn.AND, matched, SqlExpr.Call.of(SqlFn.EQUAL, l, new SqlExpr.IntLit(line)));
        }
        if (column != null) {
            matched = SqlExpr.Call.of(SqlFn.AND, matched, SqlExpr.Call.of(SqlFn.EQUAL, c, new SqlExpr.IntLit(column)));
        }
        SqlExpr holds = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, m),
                new SqlExpr.BoolLit(passesUnraised))), SqlExpr.Call.of(SqlFn.COALESCE, matched,
                new SqlExpr.BoolLit(false)), SqlExpr.Reason.JUDGE);
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(holds, VERDICT, new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(m, "m", mOut), new SqlSelect.Projection(l, "l", lOut),
                new SqlSelect.Projection(c, "c", cOut));
        return new SqlSelect(ps, false, new SqlSource.Subselect(outcome, "o", null), null, List.of(), null, null,
                List.of(), null, null, ps.stream().map(SqlSelect.Projection::out).toList());
    }

    /** {@link #assertError}'s row: its verdict, then the raised message, line and column (each absent where none). */
    public static com.legend.compiler.element.type.Type.RelationType errorSchema() {
        var one = new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1);
        var opt = new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1);
        return new com.legend.compiler.element.type.Type.RelationType(List.of(
                new com.legend.compiler.element.type.Type.RelationType.Column(VERDICT,
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one),
                new com.legend.compiler.element.type.Type.RelationType.Column("m",
                        com.legend.compiler.element.type.Type.Primitive.STRING, opt),
                new com.legend.compiler.element.type.Type.RelationType.Column("l",
                        com.legend.compiler.element.type.Type.Primitive.INTEGER, opt),
                new com.legend.compiler.element.type.Type.RelationType.Column("c",
                        com.legend.compiler.element.type.Type.Primitive.INTEGER, opt)));
    }

    /** A caller's failure message for an assert (the asserts-as-plan-nodes program, step 2, 2026-11-04): a constant the
     *  plan spells ({@code literal}), or the one-row relation of its {@code value} the database computes
     *  ({@link #firstValueRow}). */
    public record FailureMessage(@com.legend.base.Nullable String literal, @com.legend.base.Nullable SqlQuery row) {
        public FailureMessage {
            if ((literal == null) == (row == null)) {
                throw new IllegalArgumentException("a failure message is a literal or a row, one of the two");
            }
        }
    }

    /** A caller's message READ where its assert fails (the assert bar, 2026-11-04): {@code textRow}, the message's one
     *  value, as a self-contained verdict row that holds nothing and whose message is that value -- the statement the
     *  database judge's batch sends at the failing assert's place alone, so a message that would raise where the assert
     *  holds is never sent (upstream evaluates it only where the assert fails). */
    public static SqlQuery messageRow(SqlQuery textRow) {
        // the caller's message, a value the program computed: its text alone (item 3's S4: until then a seven-column
        // verdict row whose message column carried it), JSON null spelled null as upstream's toString writes it
        SqlExpr text = SqlExpr.Call.of(SqlFn.COALESCE, asText(SqlExpr.Column.of("rm", textRow.outputs(), "value")),
                new SqlExpr.StringLit("null"));
        OutputCol out = new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(text, MESSAGE, out)), false,
                new SqlSource.Subselect(textRow, "rm", null), null, List.of(), null, null, List.of(), null, null,
                List.of(out));
    }

    /** {@link #messageRow}'s row: the caller's message, one String. */
    public static com.legend.compiler.element.type.Type.RelationType messageSchema() {
        return new com.legend.compiler.element.type.Type.RelationType(List.of(
                new com.legend.compiler.element.type.Type.RelationType.Column(MESSAGE,
                        com.legend.compiler.element.type.Type.Primitive.STRING,
                        new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1))));
    }

    /** {@code SELECT canon AS __c, row_number() OVER () AS __rn FROM (wrapped) w}
     * — the side reduced to its deciding canon texts in arrival order. */
    /** The wrap's projection named {@code alias}, or null. */
    private static SqlSelect.@com.legend.base.Nullable Projection projectionOf(SqlSelect ws, String alias) {
        for (SqlSelect.Projection p : ws.projections()) {
            if (alias.equals(p.alias())) {
                return p;
            }
        }
        return null;
    }

    /** TEXT by contract: a cast to VARCHAR unless the expression is one already. */

    private static SqlExpr asText(SqlExpr e) {
        return e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.VARCHAR ? e
                : new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
    }


    private static SqlQuery canonRows(Side s) {
        return canonRows(s, false);
    }

    /** {@code withValues}: the PAIR may be judged with the 2-ULP leniency,
     * so this side carries the value column — its Float value, or a typed
     * NULL when it has none (the column belongs to the pair, not the side). */
    private static SqlQuery canonRows(Side s, boolean withValues) {
        SqlSelect ws = (SqlSelect) s.wrapped();
        if (!SideEqualities.plainProjection(ws)) {
            return canonRowsLayered(s, ws, withValues);
        }
        SqlExpr valueRef = ws.projections().get(0).expr();
        SqlExpr canon = canonOf(s, asText(java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column").expr()));
        // pure has no null VALUE: a NULL row of a value collection is dropped
        // and a NULL scalar reads as the EMPTY collection — the side drops the
        // row, so an empty [] frames '[]' and never counts as a null canon
        // cell (a NULL canon over a non-null value stays unjudged)
        SqlExpr where = SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef);
        // a canon-ordered side re-orders by __c in the aggregate; the wrap's
        // own ORDER BY is the row number's window otherwise
        List<SqlSelect.SortKey> order = s.byCanonText() ? List.of() : ws.orderBy();
        SqlSource from = ws.from();
        if (!s.byCanonText() && order.isEmpty() && from instanceof SqlSource.Subselect sub
                && SideOrdinal.defined(sub.inner()) instanceof SqlQuery ordered) {
            // the order the side's own plan defines (its sort, a literal's rows), numbered inside it (SideOrdinal),
            // never its rows' arrival through the derived table, which a database need not keep: Snowflake numbered
            // sort's testSimpleSortShared [2, 3, 1, 1, 5, 4, 2] by ORDER BY NULL at 81960bf7 (and Trino, the residue
            // GATES records), where 9dfb0ae1 had fixed a layered side only (2026-10-22)
            from = new SqlSource.Subselect(ordered, sub.alias(), sub.frameName());
            order = List.of(new SqlSelect.SortKey(SqlExpr.Column.of(sub.alias(), ordered.outputs(), SideOrdinal.ORD),
                    true, null, null));
        }
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), order, null);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(canon, C, cOut));
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (withValues) {
            OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
            ps.add(new SqlSelect.Projection(valueColumn(valueRef, s.isFloat()), V, vOut));
            outs.add(vOut);
        }
        return new SqlSelect(ps, false, from, where, List.of(), null, null, List.of(),
                null, null, List.copyOf(outs));
    }

    /** A wrap that must stay a layer (an aggregate value, a filtered or
     * capped wrap): trimmed to its value and the ONE canon the side reads,
     * the rows read over it under {@code w}. */
    /** The pair's value column for one side: the Float value as DOUBLE, or a
     * typed NULL for a side that has none. */
    private static SqlExpr valueColumn(SqlExpr valueRef, boolean isFloat) {
        return isFloat ? new SqlExpr.Cast(valueRef, SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                : new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
    }

    private static SqlQuery canonRowsLayered(Side s, SqlSelect ws, boolean withValues) {
        SqlSelect.Projection value = ws.projections().get(0);
        SqlSelect.Projection chosen = java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column");
        List<SqlSelect.Projection> kept = chosen == value ? List.of(value) : List.of(value, chosen);
        List<OutputCol> keptOuts = kept.stream().map(SqlSelect.Projection::out)
                .filter(java.util.Objects::nonNull).toList();
        // a canon-ordered side re-orders by __c in the aggregate; the wrap's
        // own ORDER BY stays for an ordered compare (the row number follows it)
        List<SqlSelect.SortKey> order = s.byCanonText() ? List.of() : ws.orderBy();
        SqlSelect trimmed = new SqlSelect(kept, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                ws.having(), ws.qualify(), order, ws.limit(), ws.offset(), keptOuts);
        // an ordered side's rows numbered by its own order, inside it (SideOrdinal), never by their arrival through
        // the derived table: a database need not keep a derived table's order (Snowflake's numbering of the arrival,
        // ORDER BY NULL, and Trino's lost sort's testSimpleSortShared and testSimpleSort_MultipleExpressions, measured
        // 2026-10-21; the grid sides' own fix of 2026-10-20)
        SqlQuery read = order.isEmpty() ? trimmed : SideOrdinal.withOrdinal(trimmed);
        List<OutputCol> readOuts = read.outputs();
        SqlExpr canon = canonOf(s, asText(SqlExpr.Column.of("w", readOuts, s.canonColumn())));
        SqlExpr valueRef = SqlExpr.Column.of("w", readOuts, java.util.Objects.requireNonNull(value.alias()));
        SqlExpr rn = SideEqualities.rowNumber(readOuts);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(canon, C, cOut));
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (withValues) {
            OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
            ps.add(new SqlSelect.Projection(valueColumn(valueRef, s.isFloat()), V, vOut));
            outs.add(vOut);
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(read, "w", null),
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef), List.of(), null, null, List.of(),
                null, null, List.copyOf(outs));
    }

    /** A grid side's row canons in arrival order: {@code SELECT
     * CAST(w.__rowcanon AS VARCHAR) AS __c, row_number() OVER () AS __rn}. */
    private static SqlQuery gridRowCanons(SqlQuery wrapped) {
        return rowsOf(new SqlExpr.Cast(
                SqlExpr.Column.of("w", wrapped.outputs(), CanonicalRenderSql.ROW_CANON),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), wrapped);
    }

    /** A grid's row canons; under a column's empty-is-NULL equivalence the
     * row canon is rebuilt from the cell canons ({@code __cell<i>} joined by
     * the cell separator, as the wrap builds {@code __rowcanon}). */

    private static SqlQuery gridRowCanons(SideEqualities.GridSide grid, String named) {
        if (grid.emptyIsNull().stream().noneMatch(b -> b)) {
            return rowsOf(new SqlExpr.Cast(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), CanonicalRenderSql.ROW_CANON),
                    SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), new SqlExpr.NullLit(), SideEqualities.over(grid, named), null);
        }
        SqlExpr row = null;
        for (int i = 0; i < grid.width(); i++) {
            SqlExpr cell = gridCell(grid, i);
            row = row == null ? cell : SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, row,
                            new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP)), cell);
        }
        return rowsOf(java.util.Objects.requireNonNull(row), new SqlExpr.NullLit(), SideEqualities.over(grid, named), null);
    }

    /** One cell canon of a grid, the empty String canon ({@code ''}) read as
     * the sentinel where the column's grammar conflates it with NULL. */
    private static SqlExpr gridCell(SideEqualities.GridSide grid, int i) {
        SqlExpr cell = new SqlExpr.Cast(SqlExpr.Column.of("w", grid.wrapped().outputs(),
                CanonicalRenderSql.CELL_CANON + i), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        if (!grid.emptyIsNullAt(i)) {
            return cell;
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, cell, new SqlExpr.StringLit("''")),
                new SqlExpr.StringLit(CanonicalRenderSql.NULL_CELL))), cell, SqlExpr.Reason.JUDGE);
    }

    /** {@code SELECT <canon> AS __c, row_number() OVER () AS __rn, <value> AS __v
     * FROM (source) w} — {@code value} the cell's DOUBLE value when the cell
     * IS a Float (the leniency's operand), NULL otherwise. */
    private static SqlQuery rowsOf(SqlExpr canon, SqlQuery source) {
        return rowsOf(canon, new SqlExpr.NullLit(), source, null);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlQuery source,
            @com.legend.base.Nullable SqlExpr where) {
        return rowsOf(canon, value, new SqlSource.Subselect(source, "w", null), where);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlSource source,
            @com.legend.base.Nullable SqlExpr where) {
        SqlExpr rn = SideEqualities.rowNumber(source.outputs());
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(canon, C, cOut),
                        new SqlSelect.Projection(rn, RN, rnOut),
                        new SqlSelect.Projection(value, V, vOut)),
                false, source, where,
                List.of(), null, null, List.of(), null, null,
                List.of(cOut, rnOut, vOut));
    }

    // ── THE UNPIVOT (2026-09-23): a grid as one row per cell, reading the grid
    // ONCE. It was one UNION ALL branch per column, each re-reading the grid
    // and numbering its rows on its own: width references to the grid, which
    // H2 re-expands per reference while planning (an 8-column grid needed
    // gigabytes to plan), and width row numberings that had to agree on
    // arrival order. Now: the grid's rows numbered once (__r), cross-joined
    // with the column positions 1..width (k.__i); each output row picks its
    // cell by position.

    private static final String ROW = "__r";
    private static final String POS = "__i";

    /** {@code (SELECT w.*, ROW_NUMBER() OVER () AS __r FROM <grid> w) w
     * CROSS JOIN (VALUES 1, …, width) k(__i)}. */
    private static SqlSource unpivot(SideEqualities.GridSide grid, String named) {
        List<OutputCol> outs = new ArrayList<>(grid.wrapped().outputs());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : grid.wrapped().outputs()) {
            ps.add(new SqlSelect.Projection(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), o.name()), o.name(), o));
        }
        OutputCol r = new OutputCol(ROW, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(SideEqualities.rowNumber(grid.wrapped().outputs()), ROW, r));
        outs.add(r);
        SqlSource rows = new SqlSource.Subselect(new SqlSelect(ps, false, SideEqualities.over(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.copyOf(outs)), "w", null);
        List<List<SqlExpr>> positions = new ArrayList<>();
        for (int p = 1; p <= grid.width(); p++) {
            positions.add(List.of(new SqlExpr.IntLit(p)));
        }
        SqlSource k = new SqlSource.Values(positions, List.of(POS), "k",
                List.of(new OutputCol(POS, SqlType.Scalar.INTEGER, false)));
        return new SqlSource.Join(rows, k, SqlSource.Join.Kind.CROSS, null);
    }

    private static SqlExpr pos() {
        return SqlExpr.Column.of("k", List.of(new OutputCol(POS, SqlType.Scalar.INTEGER, false)), POS);
    }

    /** {@code CASE WHEN k.__i = 1 THEN e1 … END}: the cell at this row's position
     * (NULL when every candidate is NULL). */
    private static SqlExpr pick(List<SqlExpr> perColumn) {
        if (perColumn.stream().allMatch(e -> e instanceof SqlExpr.NullLit)) {
            return new SqlExpr.NullLit();
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (int i = 0; i < perColumn.size(); i++) {
            whens.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, pos(), new SqlExpr.IntLit(i + 1)), perColumn.get(i)));
        }
        return new SqlExpr.Case(whens, null, SqlExpr.Reason.JUDGE);
    }

    /** {@code (w.__r - 1) * width + k.__i}: the cell's row-major position. */
    private static SqlExpr position(int width) {
        SqlExpr r = SqlExpr.Column.of("w", ROW, SqlType.Scalar.BIGINT, false, OutputCol.Origin.DERIVED);
        return SqlExpr.Call.of(SqlFn.PLUS,
                SqlExpr.Call.of(SqlFn.TIMES,
                        SqlExpr.Call.of(SqlFn.MINUS, r, new SqlExpr.IntLit(1)),
                        new SqlExpr.IntLit(width)),
                pos());
    }

    private static List<OutputCol> cteOutputs() {
        return List.of(new OutputCol(C, SqlType.Scalar.VARCHAR, true),
                new OutputCol(RN, SqlType.Scalar.BIGINT, false),
                new OutputCol(V, SqlType.Scalar.DOUBLE, true),
                new OutputCol(W, SqlType.Scalar.VARCHAR, true),
                new OutputCol(O, SqlType.Scalar.VARCHAR, true));
    }

    private static SqlSource cte(String name) {
        return new SqlSource.Table(name, name, cteOutputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    private static SqlExpr col(String cteName, String col) {
        return SqlExpr.Column.of(cteName, cteOutputs(), col);
    }

    /** {@code (SELECT <agg> FROM cte)} as a scalar. */
    private static SqlExpr scalarOver(String cteName, SqlExpr projected,
            String alias, SqlType type, @com.legend.base.Nullable Long limit) {
        OutputCol out = new OutputCol(alias, type, true);
        return new SqlExpr.ScalarSubquery(new SqlSelect(
                List.of(new SqlSelect.Projection(projected, alias, out)),
                false, cte(cteName), null, List.of(), null, null,
                limit == null ? List.of()
                        : List.of(new SqlSelect.SortKey(col(cteName, RN), true, null, null)),
                limit, null, List.of(out)));
    }

    private static SqlExpr count(String cteName) {
        return scalarOver(cteName,
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col(cteName, RN)),
                        false, List.of()),
                "__n", SqlType.Scalar.BIGINT, null);
    }

    /** {@code (SELECT count(*) FROM cte WHERE strpos(__c, marker) > 0) > 0}. */
    /** {@code (SELECT count(*) FROM cte WHERE __c IS NULL) > 0}. */
    /** The spec's side framing in SQL. */
}
