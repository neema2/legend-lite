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
    private static final String RN = "__rn";
    /** The NAMED wrapped grid(s) a grid verdict reads (see {@link #over}). */
    private static final String GRID = "__wg";
    private static final String GRID_E = "__we";
    private static final String GRID_A = "__wa";
    private static final String V = "__v";
    /** A row's element as the failure message writes it ({@link Message}); and {@code __o}, the value legend-pure's
     *  sorted form ordered by, unread since item 3's S0 (2026-11-21; its projection goes with the message SQL, S4). */
    private static final String W = "__w";
    private static final String O = "__o";

    /** A TYPED verdict row and the schema of its columns (item 3's S4, 2026-11-21): what the database judge defers. */
    public record TypedRow(SqlQuery row, Type.RelationType schema) {
    }

    /** A typed member's expected values: a list in the side's own order, written where the verdict fails. */
    public static final String TYPED_EXPECTED = "__e";
    /** A typed member's actual values. */
    public static final String TYPED_ACTUAL = "__a";
    private static final String LISTED = "__l";

    /**
     * {@code assertEquals}'s and {@code assertSameElements}' TYPED member (item 3's S4, 2026-11-21, the user's Q-A: the
     * database decides over typed values): each side's values as its plan types them ({@link #valueRows}), folded to ONE
     * list in the side's own order; the verdict their {@code IS NOT DISTINCT FROM} -- two sorted lists where the order is
     * incidental ({@code multiset}) -- under the assert's polarity; each side's list written only where the verdict
     * fails, for the failure's record, which the run builds with the host judge's builders. The expected side is read
     * before the actual, as the host's side batch reads them. Null where a side's value has no type the row can carry.
     */
    public static @com.legend.base.Nullable TypedRow typedEquals(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean wantEqual, boolean emptyOnly) {
        SqlQuery eRaw = valueRows(eWrapped);
        SqlQuery aRaw = valueRows(aWrapped);
        // each side computed once: where its values are read as instants, the instants read its rows' table (__xe,
        // __xa), which the record lists
        SqlQuery eRows = readOnce("__xe", eRaw, eKind);
        SqlQuery aRows = readOnce("__xa", aRaw, aKind);
        boolean bothEmptyOnly = emptyOnly || !eKind.equals(aKind);
        // the carriers matter only where the values are compared: two kinds lite never calls equal compare no value
        if (eRows == null || aRows == null || !bothEmptyOnly && !oneCarrier(eRows, aRows)) {
            return null;
        }
        SqlQuery eList = listed("__e", eRows);
        SqlQuery aList = listed("__a", aRows);
        // the comparison over the typed values themselves, ONE node each dialect writes (the user, 2026-11-22: "Yes on
        // list compare"): over the two lists where its database compares lists, else the portable form over the rows
        // (H2 has no list equality, no EXCEPT ALL, no FULL OUTER JOIN; M1), the dialect's (SameValuesForm)
        SqlExpr eL = SqlExpr.Column.of("e", eList.outputs(), LISTED);
        SqlExpr aL = SqlExpr.Column.of("a", aList.outputs(), LISTED);
        // two kinds lite never calls equal (register S127: an Integer and a Decimal, the type part of the value), or an
        // empty side's (Nil, {@code emptyOnly}): the sides are the same only where both are empty, an empty side's list
        // NULL
        SqlExpr holds = bothEmptyOnly
                ? SqlExpr.Call.of(SqlFn.AND, noRows("__e", eRows), noRows("__a", aRows))
                : SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER, eL, aL,
                        new SqlExpr.ScalarSubquery(rowsOf("__e", eRows)), new SqlExpr.ScalarSubquery(rowsOf("__a", aRows)));
        // the record's values as each side carries them, not the instants compared (the precision audit's M4: a
        // record of the instant named another precision than the host's; H3: a nanosecond instant shared a fused
        // column with another assert's year past 2262)
        List<SqlWith.Cte> ctes = new ArrayList<>();
        SqlSource from = cross(new SqlSource.Subselect(aList, "a", null), new SqlSource.Subselect(eList, "e", null));
        SqlQuery eHeld = eList;
        SqlQuery aHeld = aList;
        String eAt = "e";
        String aAt = "a";
        if (eRows != eRaw && eRaw != null) {
            ctes.add(new SqlWith.Cte("__xe", eRaw));
            eHeld = listed("__xe", eRaw);
            eAt = "xe";
            from = cross(from, new SqlSource.Subselect(eHeld, eAt, null));
        }
        if (aRows != aRaw && aRaw != null) {
            ctes.add(new SqlWith.Cte("__xa", aRaw));
            aHeld = listed("__xa", aRaw);
            aAt = "xa";
            from = cross(from, new SqlSource.Subselect(aHeld, aAt, null));
        }
        ctes.add(new SqlWith.Cte("__e", eRows));
        ctes.add(new SqlWith.Cte("__a", aRows));
        return typedRow(ctes, from, wantEqual ? holds : SqlExpr.Call.of(SqlFn.NOT, holds), List.of(
                listValue(TYPED_EXPECTED, eAt, eHeld, eKind), listValue(TYPED_ACTUAL, aAt, aHeld, aKind)));
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over two sides of lists or maps (a nested
     * {@code List<T>}, a {@code Map<K, V>}; item 3's S4, 2026-11-22): each value compared as its database holds it -- a
     * list as itself, in order; a map as its entries sorted by key ({@code [{k, v}, ...]}: a map has no order, the host
     * compares its keys as a set) -- by the one list node, typed; where the database carries the collection as JSON the
     * node is refused by name ({@code SameValuesForm}: typed values only). Each side's values ride the failure as
     * carried. Null where a side carries no list or map, the two carry different types, or an element is no scalar.
     */
    public static @com.legend.base.Nullable TypedRow typedCollections(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean wantEqual) {
        SqlQuery eRaw = valueRows(eWrapped);
        SqlQuery aRaw = valueRows(aWrapped);
        if (eRaw == null || aRaw == null || !eRaw.outputs().get(0).type().equals(aRaw.outputs().get(0).type())) {
            return null;
        }
        SqlType t = eRaw.outputs().get(0).type();
        boolean list = t instanceof SqlType.Array ar && scalar(ar.element());
        boolean map = t instanceof SqlType.Map mp && scalar(mp.key()) && scalar(mp.value());
        if (!list && !map) {
            return null;
        }
        SqlQuery eRows = map ? entryRows("__xe", eRaw, (SqlType.Map) t) : cteRows("__xe", eRaw);
        SqlQuery aRows = map ? entryRows("__xa", aRaw, (SqlType.Map) t) : cteRows("__xa", aRaw);
        SqlQuery eList = listed("__e", eRows);
        SqlQuery aList = listed("__a", aRows);
        SqlQuery eHeld = listed("__xe", eRaw);
        SqlQuery aHeld = listed("__xa", aRaw);
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("e", eList.outputs(), LISTED), SqlExpr.Column.of("a", aList.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__e", eRows)), new SqlExpr.ScalarSubquery(rowsOf("__a", aRows)));
        return typedRow(List.of(new SqlWith.Cte("__xe", eRaw), new SqlWith.Cte("__xa", aRaw),
                        new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a",
                        null)), new SqlSource.Subselect(eHeld, "xe", null)), new SqlSource.Subselect(aHeld, "xa",
                        null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "xe", eHeld, eKind), listValue(TYPED_ACTUAL, "xa", aHeld, aKind)));
    }

    /** Whether {@code t} is a scalar value's type (a collection's element a typed member compares as it is). */
    private static boolean scalar(SqlType t) {
        return t instanceof SqlType.Scalar sc && sc != SqlType.Scalar.JSON || t instanceof SqlType.Decimal;
    }

    /** A side's maps ({@code rows}, read as {@code cte}) as their entries sorted by key, each a {@code {k, v}} struct:
     *  the value a map is compared as (the order its entries were written in no part of it). */
    private static SqlQuery entryRows(String cte, SqlQuery rows, SqlType.Map t) {
        SqlExpr m = SqlExpr.Column.of(cte, rows.outputs(), O);
        SqlExpr k = SqlExpr.Column.derived(null, "__k");
        SqlExpr entry = new SqlExpr.StructLit(List.of(new SqlExpr.StructLit.Field("k", k, t.key()),
                new SqlExpr.StructLit.Field("v", CanonicalRenderSql.valueAtKey(m), t.value())));
        SqlExpr entries = CanonicalRenderSql.sortedEntries(m, entry);
        SqlType et = new SqlType.Array(new SqlType.Struct(List.of(new SqlType.Struct.Field("k", t.key()),
                new SqlType.Struct.Field("v", t.value()))));
        OutputCol oOut = new OutputCol(O, et, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(entries, O, oOut), new SqlSelect.Projection(
                SqlExpr.Column.of(cte, rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, cte,
                rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null,
                null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /** {@code raw}, a side's typed rows, as the values compared: itself, or, where {@link #instants} reads them as
     *  the judge's instants, those instants over the rows' table {@code cte} (which the caller defines and the record
     *  lists), so the side's plan is computed once (the precision fix's round: a second copy made the database compute
     *  each side twice, its errors in another order). */
    private static @com.legend.base.Nullable SqlQuery readOnce(String cte,
            @com.legend.base.Nullable SqlQuery raw, Type kind) {
        if (raw == null || instants(raw, kind) == raw) {
            return raw == null ? null : instants(raw, kind);
        }
        return instants(cteRows(cte, raw), kind);
    }

    /** The rows of {@code cte}, a side's typed rows ({@code __o}, {@code __rn}), read back. */
    private static SqlQuery cteRows(String cte, SqlQuery rows) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : rows.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of(cte, rows.outputs(), o.name()), o.name(), o));
        }
        return new SqlSelect(ps, false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, rows.outputs());
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over a TAGGED side (item 3's S4, 2026-11-22):
     * a value whose kind the database decided among candidates (a Number's max, a mixed list's elements; the plan's
     * {@link SqlType.Tagged}: its tag and one slot per kind), against another tagged side or one of a typed class. Each
     * value is a row of its kind's index ({@code k}, into the kinds of both sides, one order) and one typed column per
     * kind ({@code v0, v1, ...}, the value in its kind's column, NULL in the others), compared by the one list node --
     * two values the same only where their kinds and their values are (register S127: an Integer is never a Float;
     * S139 a Decimal by its value; S399 a DateTime of any written precision one kind). Each side's rows ride the failure
     * as structs, the record un-tagging each to its kind's value. Null where a side carries no tagged or typed value,
     * or a kind (an enumeration's) no column holds.
     */
    public static @com.legend.base.Nullable TypedRow typedTagged(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean wantEqual) {
        // the values compared by KIND (a Decimal of any scale by its value, S139); the values recorded by kind AND SQL
        // type, each Decimal in a column of its own scale, so none is rescaled in the record (batch 3's audit, H1)
        java.util.TreeMap<String, SqlType> kinds = new java.util.TreeMap<>();
        java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
        if (!kindsOf(eWrapped, eKind, kinds, false) || !kindsOf(aWrapped, aKind, kinds, false)
                || !kindsOf(eWrapped, eKind, held, true) || !kindsOf(aWrapped, aKind, held, true)) {
            return null;
        }
        SqlQuery eCells = kindedRows(eWrapped, eKind, kinds, false);
        SqlQuery aCells = kindedRows(aWrapped, aKind, kinds, false);
        SqlQuery eHeld = kindedRows(eWrapped, eKind, held, true);
        SqlQuery aHeld = kindedRows(aWrapped, aKind, held, true);
        if (eCells == null || aCells == null || eHeld == null || aHeld == null) {
            return null;
        }
        SqlQuery eCmp = listed("__ke", structRows(rowsOf("__ce", eCells)));
        SqlQuery aCmp = listed("__ka", structRows(rowsOf("__ca", aCells)));
        SqlQuery eList = listed("__he", structRows(rowsOf("__ge", eHeld)));
        SqlQuery aList = listed("__ha", structRows(rowsOf("__ga", aHeld)));
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__ce", eCells)), new SqlExpr.ScalarSubquery(rowsOf("__ca", aCells)));
        return typedRow(List.of(new SqlWith.Cte("__ce", eCells), new SqlWith.Cte("__ca", aCells),
                        new SqlWith.Cte("__ke", structRows(rowsOf("__ce", eCells))),
                        new SqlWith.Cte("__ka", structRows(rowsOf("__ca", aCells))),
                        new SqlWith.Cte("__ge", eHeld), new SqlWith.Cte("__ga", aHeld),
                        new SqlWith.Cte("__he", structRows(rowsOf("__ge", eHeld))),
                        new SqlWith.Cte("__ha", structRows(rowsOf("__ga", aHeld)))),
                cross(cross(cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "e", eList, eKind), listValue(TYPED_ACTUAL, "a", aList, aKind)));
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over two sides of instances of one class -- a
     * model class's or a Pair's, each value a struct (item 3's S4, instances, 2026-11-22; the user's Q-A): each value
     * ONE row of typed leaves, as the host restricts it ({@code HostJudgments.restrictOne}: the classifier, then the
     * keys, upstream's {@code EqualityUtilities}) -- a keyed class its {@code __type} where it carries one and each key
     * property, a nested keyed instance's leaves in its place; a keyless class its {@code __type} and its identity,
     * {@code __id} (register S111; read where the product tracks it, else raised by name: {@link
     * CanonicalRenderSql#trackedId}) -- compared by the one list node as the grids are, in order or as a multiset of
     * rows. Each leaf is read as its layout types it (a dialect carrying a struct as JSON reads the field by its type), a
     * date-time through the judge's instant. Where the verdict fails, each side's instances ride back as they are for the
     * host's record. Null where a side carries no struct, the two layouts' leaves differ in kind, or a leaf has no typed
     * reading here: a to-many key, a key slot of a polymorphic or Any class, a JSON field, a keyless class with no
     * identity.
     */
    public static @com.legend.base.Nullable TypedRow typedInstances(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys, boolean multiset,
            boolean wantEqual) {
        SqlQuery eRows = valueRows(eWrapped);
        SqlQuery aRows = valueRows(aWrapped);
        if (eRows == null || aRows == null || !(eRows.outputs().get(0).type() instanceof SqlType.Struct es)
                || !(aRows.outputs().get(0).type() instanceof SqlType.Struct as)) {
            return null;
        }
        List<List<Leaf>> both = pairedLeaves(es, as, keys);
        if (both == null) {
            return null;
        }
        SqlQuery ce = leafRows("__ie", eRows, both.get(0), tracked(eWrapped));
        SqlQuery ca = leafRows("__ia", aRows, both.get(1), tracked(aWrapped));
        SqlQuery eCmp = listed("__ke", structRows(rowsOf("__ce", ce)));
        SqlQuery aCmp = listed("__ka", structRows(rowsOf("__ca", ca)));
        // the record's instances as the host's reads them: a keyed class restricted to its classifier and its keys
        // (HostJudgments.restrictOne), a keyless one whole
        SqlQuery eHeld = keys == null ? eRows : restrictedRows("__ie", eRows, es, keys);
        SqlQuery aHeld = keys == null ? aRows : restrictedRows("__ia", aRows, as, keys);
        SqlQuery eList = listed("__re", eHeld);
        SqlQuery aList = listed("__ra", aHeld);
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__ce", ce)), new SqlExpr.ScalarSubquery(rowsOf("__ca", ca)));
        return typedRow(List.of(new SqlWith.Cte("__ie", eRows), new SqlWith.Cte("__ia", aRows),
                        new SqlWith.Cte("__re", eHeld), new SqlWith.Cte("__ra", aHeld),
                        new SqlWith.Cte("__ce", ce), new SqlWith.Cte("__ca", ca),
                        new SqlWith.Cte("__ke", structRows(rowsOf("__ce", ce))),
                        new SqlWith.Cte("__ka", structRows(rowsOf("__ca", ca)))),
                cross(cross(cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "e", eList, eKind), listValue(TYPED_ACTUAL, "a", aList, aKind)));
    }

    /** Two layouts' leaves as the host restricts them, path by path of one kind ({@link #leaves}); null where a leaf
     *  has no typed reading or the two differ. */
    private static @com.legend.base.Nullable List<List<Leaf>> pairedLeaves(SqlType.Struct es, SqlType.Struct as,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        List<Leaf> eLeaves = new ArrayList<>();
        List<Leaf> aLeaves = new ArrayList<>();
        if (!leaves(es, keys, List.of(), eLeaves) || !leaves(as, keys, List.of(), aLeaves)
                || eLeaves.size() != aLeaves.size() || eLeaves.isEmpty()) {
            return null;
        }
        for (int i = 0; i < eLeaves.size(); i++) {
            Leaf el = eLeaves.get(i);
            Leaf al = aLeaves.get(i);
            if (!el.path().equals(al.path()) || !java.util.Objects.equals(leafClass(el.type()), leafClass(al.type()))) {
                return null;
            }
        }
        return List.of(eLeaves, aLeaves);
    }

    /**
     * {@code assertContains}' TYPED member over instances of one class -- a model class's or a Pair's, each a struct
     * (item 3's S4, the last families): the collection's instances and the value's as rows of their typed leaves, as
     * {@link #typedInstances} reads them (the host's {@code Equality}: the classifier, then the keys or the identity);
     * the verdict whether some instance's leaves are each {@code IS NOT DISTINCT FROM} the value's, under {@code want};
     * both sides' instances ride the failure whole, as the host's record writes them. Null where {@link
     * #typedInstances} has no reading.
     */
    public static @com.legend.base.Nullable TypedRow typedContainsInstances(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys,
            boolean want) {
        SqlQuery cRows = valueRows(collWrapped);
        SqlQuery vRows = valueRows(valWrapped);
        if (cRows == null || vRows == null || !(cRows.outputs().get(0).type() instanceof SqlType.Struct cs)
                || !(vRows.outputs().get(0).type() instanceof SqlType.Struct vs)) {
            return null;
        }
        List<List<Leaf>> both = pairedLeaves(cs, vs, keys);
        if (both == null) {
            return null;
        }
        SqlQuery cl = leafRows("__xc", cRows, both.get(0), tracked(collWrapped));
        SqlQuery vl = leafRows("__xv", vRows, both.get(1), tracked(valWrapped));
        SqlExpr on = null;
        for (int i = 0; i < both.get(0).size(); i++) {
            SqlExpr same = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("__c", cl.outputs(), "c" + i),
                    SqlExpr.Column.of("__v", vl.outputs(), "c" + i));
            on = on == null ? same : SqlExpr.Call.of(SqlFn.AND, on, same);
        }
        OutputCol mOut = new OutputCol("__m", SqlType.Scalar.BIGINT, false);
        SqlSource matched = new SqlSource.Join(
                new SqlSource.Table("__c", "__c", cl.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                new SqlSource.Table("__v", "__v", vl.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                SqlSource.Join.Kind.INNER, java.util.Objects.requireNonNull(on));
        SqlSelect m = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                false, List.of()), "__m", mOut)), false, matched, null, List.of(), null, null, List.of(), null, null,
                List.of(mOut));
        SqlExpr some = SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Column.of("m", m.outputs(), "__m"),
                new SqlExpr.IntLit(0));
        SqlQuery cList = listed("__xc", cRows);
        SqlQuery vList = listed("__xv", vRows);
        return typedRow(List.of(new SqlWith.Cte("__xc", cRows), new SqlWith.Cte("__xv", vRows),
                        new SqlWith.Cte("__c", cl), new SqlWith.Cte("__v", vl)),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                want ? some : SqlExpr.Call.of(SqlFn.NOT, some), List.of(
                        listValue(TYPED_EXPECTED, "c", cList, collKind), listValue(TYPED_ACTUAL, "v", vList, valKind)));
    }

    /** A side's keyed instances ({@code rows}, read as {@code cte}) each restricted to its classifier and its keys, a
     *  nested keyed instance restricted in its place -- the value the host's record writes
     *  ({@code HostJudgments.restrictOne}). */
    private static SqlQuery restrictedRows(String cte, SqlQuery rows, SqlType.Struct layout,
            com.legend.compiler.element.EqualityKeys keys) {
        SqlExpr held = restricted(SqlExpr.Column.of("i", rows.outputs(), O), layout, keys);
        OutputCol oOut = new OutputCol(O, held.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : layout,
                false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(held, O, oOut), new SqlSelect.Projection(
                SqlExpr.Column.of("i", rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, "i",
                rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null,
                null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /** {@code v}, an instance of layout {@code st}, as a struct of its classifier and its key properties. */
    private static SqlExpr restricted(SqlExpr v, SqlType.Struct st, com.legend.compiler.element.EqualityKeys keys) {
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
        SqlType type = fieldType(st, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE);
        if (type != null) {
            fields.add(new SqlExpr.StructLit.Field(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE,
                    new SqlExpr.StructGet(v, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE), type));
        }
        for (com.legend.compiler.element.EqualityKeys.Key k : keys.keys()) {
            SqlType ft = java.util.Objects.requireNonNull(fieldType(st, k.name()), "a key's field (leaves read it)");
            SqlExpr field = new SqlExpr.StructGet(v, k.name());
            // a nested keyed instance restricted in its place, an absent one NULL (the instances audit's M-b)
            SqlExpr value = ft instanceof SqlType.Struct nested && k.nested() != null
                    ? new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, field),
                            nullOf(nested))), restricted(field, nested, k.nested()), SqlExpr.Reason.JUDGE) : field;
            SqlType vt = value.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : ft;
            fields.add(new SqlExpr.StructLit.Field(k.name(), value, vt));
        }
        return new SqlExpr.StructLit(fields);
    }

    /** One typed leaf of an instance: the field path from the instance down to it, and its layout's SQL type. */
    private record Leaf(List<String> path, SqlType type) {
    }

    /** The leaves of a struct of layout {@code st} as the host restricts it, into {@code out} (path-prefixed by
     *  {@code at}); false where one has no typed reading. */
    private static boolean leaves(SqlType.Struct st, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys
            keys, List<String> at, List<Leaf> out) {
        SqlType type = fieldType(st, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE);
        if (type != null) {
            out.add(new Leaf(append(at, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE), type));
        }
        if (keys == null) {
            // a keyless class: its identity, tracked where the product tracks it (S111)
            SqlType id = fieldType(st, com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID);
            if (id == null || type == null) {
                return false;
            }
            out.add(new Leaf(append(at, com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID), id));
            return true;
        }
        for (com.legend.compiler.element.EqualityKeys.Key k : keys.keys()) {
            SqlType ft = fieldType(st, k.name());
            if (ft == null || k.many()) {
                return false;
            }
            if (ft instanceof SqlType.Struct nested) {
                // a nested keyed instance's own leaves; a slot whose class is keyless, polymorphic or Any has none here
                if (k.nested() == null || !leaves(nested, k.nested(), append(at, k.name()), out)) {
                    return false;
                }
            } else if (leafClass(ft) == null) {
                return false;
            } else {
                out.add(new Leaf(append(at, k.name()), ft));
            }
        }
        return true;
    }

    /** The kind a leaf's SQL type holds, as the host reads it, or null where it has no typed reading here (a JSON
     *  value, a list, a map, a struct). Two leaves compare only where their classes agree. */
    private static @com.legend.base.Nullable String leafClass(SqlType t) {
        return switch (t) {
            case SqlType.Decimal d -> "Decimal";
            case SqlType.Scalar sc -> switch (sc) {
                case BOOLEAN -> "Boolean";
                case INTEGER, BIGINT, HUGEINT -> "Integer";
                case DOUBLE -> "Float";
                case VARCHAR -> "String";
                case DATE -> "StrictDate";
                case TIMESTAMP -> "DateTime";
                default -> null;
            };
            default -> null;
        };
    }

    private static @com.legend.base.Nullable SqlType fieldType(SqlType.Struct st, String name) {
        for (SqlType.Struct.Field f : st.fields()) {
            if (f.name().equals(name)) {
                return f.type();
            }
        }
        return null;
    }

    /** Whether every object {@code side} holds has an identity the product tracks, proven from its plan alone: no read
     *  of a table its own WITH does not define, of a catalog, a URL, raw SQL or a grid's report; no cast to a
     *  struct; and every struct literal an object carrying a literal id ({@code ClassLayouts.SYNTHETIC_ID}) -- an
     *  object built in the body, never one read from a store, built per row or in a fold's reducer (those carry no
     *  literal id: docs/OBJECT_IDENTITY_2026_10_20.md section 4). There the identity's guard can never raise, so it is
     *  left out and the member fuses with the asserts beside it ({@code VerdictBatch.raisingRuns}: the PCT grammar
     *  ratchet's 179 -> 180 of item 3's S4, the user's word of 2026-11-23, "fold in the instance fusing"). Any other
     *  shape keeps the guard. */
    static boolean tracked(SqlQuery side) {
        java.util.Set<String> defined = new java.util.HashSet<>();
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlQuery with(SqlWith w) {
                w.ctes().forEach(c -> defined.add(c.name()));
                return w;
            }
        }.rewrite(side);
        List<String> untracked = new ArrayList<>();
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Table t && (t.call() || !defined.contains(t.name()))
                        || s instanceof SqlSource.Cte c && !defined.contains(c.name())
                        || s instanceof SqlSource.Catalog || s instanceof SqlSource.SourceUrl
                        || s instanceof SqlSource.RawSql || s instanceof SqlSource.GridReport) {
                    untracked.add("a read of rows its plan does not build");
                }
                return s;
            }

            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlExpr.Cast c && c.target() instanceof SqlType.Struct
                        || e instanceof SqlExpr.StructLit l && l.fields().stream().noneMatch(f -> f.name().equals(
                        com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID) && f.value() instanceof SqlExpr.StringLit)) {
                    untracked.add("an object with no literal id");
                }
                return e;
            }
        }.rewrite(side);
        return untracked.isEmpty();
    }

    private static List<String> append(List<String> at, String name) {
        List<String> out = new ArrayList<>(at);
        out.add(name);
        return out;
    }

    /** A side's instances ({@code rows}, read as {@code cte}) as rows of their typed leaves ({@code c0, c1, ...,
     *  __rn}): each leaf the field read down its path, cast to its layout's type (a dialect carrying a struct as JSON
     *  reads the field by it), a date-time as the judge's instant, an identity where the product tracks it -- read
     *  plain where {@code tracked} (the side's plan proves it, {@link #tracked}), else behind the guard that raises
     *  where it does not ({@code CanonicalRenderSql.trackedId}). */
    private static SqlQuery leafRows(String cte, SqlQuery rows, List<Leaf> leaves, boolean tracked) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        for (int i = 0; i < leaves.size(); i++) {
            Leaf leaf = leaves.get(i);
            SqlExpr v = SqlExpr.Column.of("i", rows.outputs(), O);
            for (String step : leaf.path()) {
                v = new SqlExpr.StructGet(v, step);
            }
            String last = leaf.path().get(leaf.path().size() - 1);
            SqlExpr read = last.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)
                    ? new SqlExpr.Cast(tracked ? v : CanonicalRenderSql.trackedId(((SqlExpr.StructGet) v).source()),
                            SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                    : new SqlExpr.Cast(v, leaf.type(), SqlExpr.Reason.JUDGE);
            SqlType type = last.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID) ? SqlType.Scalar.VARCHAR
                    : leaf.type();
            OutputCol out = new OutputCol("c" + i, type, true);
            ps.add(new SqlSelect.Projection(read, out.name(), out));
            outs.add(out);
        }
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(SqlExpr.Column.of("i", rows.outputs(), RN), RN, rnOut));
        outs.add(rnOut);
        return new SqlSelect(ps, false, new SqlSource.Table(cte, "i", rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** Whether a tagged side ({@code wrapped}'s first column a {@link SqlType.Tagged}) or a side of one typed class
     *  states its kinds, each with the SQL type that holds it, into {@code into}, keyed by {@link #kindKey}. */
    private static boolean kindsOf(SqlQuery wrapped, Type kind, java.util.Map<String, SqlType> into, boolean byType) {
        if (wrapped.outputs().isEmpty() || wrapped.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT) {
            // (a Decimal the root writes as its text has no typed reading against a tagged side's values: oneCarrier's
            // rule, the audit of 3f2fe452 -- its text was joined against a number)
            return false;
        }
        if (wrapped.outputs().get(0).type() instanceof SqlType.Tagged tg) {
            for (SqlType.Tagged.Slot slot : tg.slots()) {
                if (MixedEncoding.enumerationKind(slot.kind()) != null) {
                    return false;
                }
                if (!byType && slot.kind().equals("Date")) {
                    // a Date slot holds its value written: each one of the four date kinds, as a written side's
                    for (java.util.Map.Entry<String, SqlType> k : DATE_KINDS.entrySet()) {
                        into.putIfAbsent(kindKey(k.getKey(), k.getValue(), false), k.getValue());
                    }
                    continue;
                }
                // a DateTime compared at the judge's instant (S399): its compare column a TIMESTAMP whatever its slot
                into.putIfAbsent(kindKey(slot.kind(), slot.type(), byType), !byType && instant(slot.kind())
                        ? SqlType.Scalar.TIMESTAMP : slot.type());
            }
            return true;
        }
        if (wrapped.outputs().get(0).type() == SqlType.Scalar.JSON) {
            // a value carried as JSON (an Any): each value its JSON kind's -- a String, an Integer, a Float, a Boolean --
            // as the product reads a Variant (CastPolicy), compared as that kind's value; recorded as carried
            if (byType) {
                into.putIfAbsent(kindKey("Any", SqlType.Scalar.JSON, true), SqlType.Scalar.JSON);
            } else {
                JSON_KINDS.forEach((k, t) -> into.putIfAbsent(kindKey(k, t, false), t));
            }
            return true;
        }
        if (writtenDate(kind, wrapped)) {
            // a Date written as its text: each value one of the four date kinds, as its text says (S399), compared as
            // its kind's value and recorded as it is written
            SqlType carried = wrapped.outputs().get(0).type();
            for (java.util.Map.Entry<String, SqlType> k : DATE_KINDS.entrySet()) {
                into.putIfAbsent(writtenKey(k.getKey(), carried, byType), byType ? carried : k.getValue());
            }
            return true;
        }
        Type t = typedClass(kind);
        if (t == null || t instanceof Type.EnumType || PlatformTypes.isNil(t)) {
            return false;
        }
        SqlType held = byType ? wrapped.outputs().get(0).type() : heldType(wrapped, t);
        into.putIfAbsent(kindKey(kindText(t), held, byType), held);
        return true;
    }

    /** The kinds a Date written as its text may be, each with the SQL type its value is compared in (S399): a year and
     *  a year-month by their components, typed as integers (a year itself; a year-month its year times twelve plus its
     *  month), a day the database's DATE, a date-time the judge's instant (the precision audit's H1, 2026-11-22: a
     *  year's and a year-month's text were compared as text). */
    private static final java.util.Map<String, SqlType> DATE_KINDS = java.util.Map.of("Year", SqlType.Scalar.BIGINT,
            "YearMonth", SqlType.Scalar.BIGINT, "StrictDate", SqlType.Scalar.DATE, "DateTime", SqlType.Scalar.TIMESTAMP);

    /** The kinds a value carried as JSON may be, each with the SQL type its value is compared in. */
    private static final java.util.Map<String, SqlType> JSON_KINDS = java.util.Map.of("String", SqlType.Scalar.VARCHAR,
            "Integer", SqlType.Scalar.BIGINT, "Float", SqlType.Scalar.DOUBLE, "Boolean", SqlType.Scalar.BOOLEAN);

    /** A JSON value's kind's name ({@link #JSON_KINDS}) where {@code jsonType} (its JSON_TYPE) answers it. */
    private static SqlExpr jsonKindIs(String kind, SqlExpr jsonType) {
        return switch (kind) {
            case "String" -> SqlExpr.Call.of(SqlFn.EQUAL, jsonType, new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.STRING));
            case "Integer" -> SqlExpr.Call.of(SqlFn.OR,
                    SqlExpr.Call.of(SqlFn.EQUAL, jsonType, new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.INTEGER)),
                    SqlExpr.Call.of(SqlFn.EQUAL, jsonType,
                            new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.UNSIGNED_INTEGER)));
            case "Float" -> SqlExpr.Call.of(SqlFn.EQUAL, jsonType, new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.FLOAT));
            case "Boolean" -> SqlExpr.Call.of(SqlFn.EQUAL, jsonType,
                    new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.BOOLEAN));
            default -> throw new IllegalArgumentException("no JSON kind is " + kind);
        };
    }

    /** A JSON value of {@code kind} read as that kind's value, as the product reads a Variant ({@code CastPolicy}): a
     *  String its root's text, a number or a Boolean its cast. */
    private static SqlExpr jsonValue(String kind, SqlExpr v) {
        return kind.equals("String")
                ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.VARIANT_GET, v, new SqlExpr.StringLit("$")),
                        SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                : new SqlExpr.Cast(v, java.util.Objects.requireNonNull(JSON_KINDS.get(kind)), SqlExpr.Reason.JUDGE);
    }

    /** Whether a side is a {@code Date} carried as its written text ({@code JudgmentFacts}' the same rule). */
    private static boolean writtenDate(Type kind, SqlQuery wrapped) {
        return kind == Type.Primitive.DATE && !wrapped.outputs().isEmpty()
                && (wrapped.outputs().get(0).type() == SqlType.Scalar.TEMPORAL_TEXT
                        || wrapped.outputs().get(0).type() == SqlType.Scalar.VARCHAR);
    }

    /** The kind a Date's written text names, read from its STRUCTURE -- a time after the day (a {@code T} or a space)
     *  a date-time; else by its dashes after the year's sign: none a year, one a year-month, two a day -- as the kind
     *  key's index. Never its length (the precision audit's H1, 2026-11-22: a year of three or five digits, or a
     *  negative one, took another kind's length). */
    private static SqlExpr dateKindOfText(SqlExpr text, java.util.function.Function<String, Integer> index) {
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (String k : List.of("DateTime", "Year", "YearMonth")) {
            whens.add(new SqlExpr.Case.When(writtenKindIs(k, text), new SqlExpr.IntLit(index.apply(k))));
        }
        return new SqlExpr.Case(whens, new SqlExpr.IntLit(index.apply("StrictDate")), SqlExpr.Reason.JUDGE);
    }

    /** Whether a Date's written {@code text} is of {@code kind} ({@link #dateKindOfText}). */
    private static SqlExpr writtenKindIs(String kind, SqlExpr text) {
        SqlExpr body = unsignedYear(text);
        SqlExpr time = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, body, new SqlExpr.StringLit("T")),
                        new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, body, new SqlExpr.StringLit(" ")),
                        new SqlExpr.IntLit(0)));
        SqlExpr dashes = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, body), SqlExpr.Call.of(SqlFn.LENGTH,
                SqlExpr.Call.of(SqlFn.REPLACE, body, new SqlExpr.StringLit("-"), new SqlExpr.StringLit(""))));
        return switch (kind) {
            case "DateTime" -> time;
            case "Year" -> SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, time),
                    SqlExpr.Call.of(SqlFn.EQUAL, dashes, new SqlExpr.IntLit(0)));
            case "YearMonth" -> SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, time),
                    SqlExpr.Call.of(SqlFn.EQUAL, dashes, new SqlExpr.IntLit(1)));
            case "StrictDate" -> SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, time),
                    SqlExpr.Call.of(SqlFn.EQUAL, dashes, new SqlExpr.IntLit(2)));
            default -> throw new IllegalArgumentException("no written date's kind is " + kind);
        };
    }

    /** {@code text} without the sign before its year. */
    private static SqlExpr unsignedYear(SqlExpr text) {
        SqlExpr first = SqlExpr.Call.of(SqlFn.LEFT, text, new SqlExpr.IntLit(1));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("-")),
                SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("+"))),
                SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(2)))), text, SqlExpr.Reason.JUDGE);
    }

    /** A Date's written {@code text} of {@code kind} as the value it is compared as ({@link #DATE_KINDS}): a year its
     *  number; a year-month its year times twelve plus its month; a day the database's DATE; a date-time the judge's
     *  instant (an hour's text read at its minute). */
    private static SqlExpr writtenDateValue(String kind, SqlExpr text) {
        return switch (kind) {
            case "Year" -> new SqlExpr.Cast(text, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE);
            case "YearMonth" -> {
                SqlExpr body = unsignedYear(text);
                SqlExpr dash = SqlExpr.Call.of(SqlFn.STRPOS, body, new SqlExpr.StringLit("-"));
                // the year with its sign: the text up to the dash that ends it
                SqlExpr year = SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(1), SqlExpr.Call.of(
                        SqlFn.MINUS, SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(
                                SqlFn.LENGTH, text), SqlExpr.Call.of(SqlFn.LENGTH, body)), dash), new SqlExpr.IntLit(1)));
                SqlExpr month = SqlExpr.Call.of(SqlFn.SUBSTRING, body, SqlExpr.Call.of(SqlFn.PLUS, dash,
                        new SqlExpr.IntLit(1)));
                yield SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES,
                        new SqlExpr.Cast(year, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE), new SqlExpr.IntLit(12)),
                        new SqlExpr.Cast(month, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE));
            }
            case "StrictDate" -> new SqlExpr.Cast(text, SqlType.Scalar.DATE, SqlExpr.Reason.JUDGE);
            case "DateTime" -> new SqlExpr.Cast(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, text, new SqlExpr.StringLit(":")),
                            new SqlExpr.IntLit(0)),
                    SqlExpr.Call.of(SqlFn.CONCAT, text, new SqlExpr.StringLit(":00")))), text, SqlExpr.Reason.JUDGE),
                    SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE);
            default -> throw new IllegalArgumentException("no written date's kind is " + kind);
        };
    }

    /** The SQL type a typed side's values are compared in: a date's the database's DATE or TIMESTAMP ({@link #instants}
     *  reads a written one), any other its column's. */
    private static SqlType heldType(SqlQuery wrapped, Type t) {
        return t == Type.Primitive.DATE_TIME ? SqlType.Scalar.TIMESTAMP
                : t == Type.Primitive.STRICT_DATE ? SqlType.Scalar.DATE : wrapped.outputs().get(0).type();
    }

    /** Whether a slot's kind is a DateTime's (of any written precision), compared at the judge's instant. */
    private static boolean instant(String slotKind) {
        return MixedEncoding.writtenDigits(slotKind) != null || slotKind.equals("DateTime");
    }

    /** A written Date's kind's column key: its compared kind's ({@link #DATE_KINDS}), and where the column is the
     *  record's, the text it is written in. */
    private static String writtenKey(String kind, SqlType carried, boolean byType) {
        return kindKey(kind, byType ? carried : java.util.Objects.requireNonNull(DATE_KINDS.get(kind)), byType);
    }

    /** A kind's column key: its name (a DateTime of any written precision one kind, register S399), and where the
     *  column is the record's ({@code byType}) its SQL type too. */
    private static String kindKey(String slotKind, SqlType type, boolean byType) {
        String kind = MixedEncoding.writtenDigits(slotKind) != null ? "DateTime" : slotKind;
        return byType ? kind + "|" + type : kind;
    }

    /** A side's values as kinded rows ({@link #typedTagged}): {@code k}, the index of its kind's key in
     *  {@code columns}, then {@code v0..vn} one per key, then {@code __rn}, numbered in the side's own order. A tagged
     *  side's tag names its slot; a typed side's every value is its one kind's. */
    private static @com.legend.base.Nullable SqlQuery kindedRows(SqlQuery wrapped, Type kind,
            java.util.SortedMap<String, SqlType> columns, boolean byType) {
        List<String> order = new ArrayList<>(columns.keySet());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        OutputCol kOut = new OutputCol("k", SqlType.Scalar.BIGINT, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlQuery rows;
        SqlExpr kOf;
        java.util.function.Function<String, @com.legend.base.Nullable SqlExpr> valueOf;
        if (wrapped.outputs().get(0).type() instanceof SqlType.Tagged tg) {
            // the tag and its slots, in the side's own order (valueRows: a wrap's ORDER BY numbered, never lost), each
            // slot the column its name says (batch 4's audit, L2: read by position, so checked by name)
            String tagName = wrapped.outputs().get(0).name();
            for (int i = 0; i < tg.slots().size(); i++) {
                if (wrapped.outputs().size() <= i + 1
                        || !wrapped.outputs().get(i + 1).name().equals(SqlType.Tagged.slotColumn(tagName, i))) {
                    return null;
                }
            }
            rows = valueRows(wrapped, 1 + tg.slots().size());
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            SqlExpr tag = SqlExpr.Column.of("r", r.outputs(), O);
            List<SqlExpr.Case.When> ks = new ArrayList<>();
            for (int i = 0; i < tg.slots().size(); i++) {
                SqlType.Tagged.Slot slot = tg.slots().get(i);
                SqlExpr at = SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(i));
                if (!byType && slot.kind().equals("Date")) {
                    // a Date slot's kind is its written value's (dateKindOfText)
                    SqlExpr text = SqlExpr.Column.of("r", r.outputs(), "__s" + (i + 1));
                    for (String k : List.of("DateTime", "Year", "YearMonth", "StrictDate")) {
                        ks.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, at, writtenKindIs(k, text)),
                                new SqlExpr.IntLit(order.indexOf(kindKey(k, java.util.Objects.requireNonNull(
                                        DATE_KINDS.get(k)), false)))));
                    }
                    continue;
                }
                ks.add(new SqlExpr.Case.When(at,
                        new SqlExpr.IntLit(order.indexOf(kindKey(slot.kind(), slot.type(), byType)))));
            }
            kOf = new SqlExpr.Case(ks, null, SqlExpr.Reason.JUDGE);
            valueOf = key -> {
                List<SqlExpr.Case.When> vs = new ArrayList<>();
                for (int i = 0; i < tg.slots().size(); i++) {
                    SqlType.Tagged.Slot slot = tg.slots().get(i);
                    if (!byType && slot.kind().equals("Date")) {
                        for (String k : DATE_KINDS.keySet()) {
                            if (kindKey(k, java.util.Objects.requireNonNull(DATE_KINDS.get(k)), false).equals(key)) {
                                SqlExpr text = SqlExpr.Column.of("r", r.outputs(), "__s" + (i + 1));
                                vs.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.EQUAL,
                                        tag, new SqlExpr.IntLit(i)), writtenKindIs(k, text)),
                                        writtenDateValue(k, text)));
                            }
                        }
                        continue;
                    }
                    if (kindKey(slot.kind(), slot.type(), byType).equals(key)) {
                        SqlExpr held = SqlExpr.Column.of("r", r.outputs(), "__s" + (i + 1));
                        vs.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(i)),
                                !byType && instant(slot.kind()) ? new SqlExpr.Cast(held, SqlType.Scalar.TIMESTAMP,
                                        SqlExpr.Reason.JUDGE) : held));
                    }
                }
                return vs.isEmpty() ? null : new SqlExpr.Case(vs, nullOf(java.util.Objects.requireNonNull(
                        columns.get(key), "a kind's SQL type")), SqlExpr.Reason.JUDGE);
            };
        } else if (wrapped.outputs().get(0).type() == SqlType.Scalar.JSON) {
            rows = valueRows(wrapped);
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            SqlExpr v = SqlExpr.Column.of("r", r.outputs(), O);
            if (byType) {
                // recorded as carried: the JSON value itself
                kOf = new SqlExpr.IntLit(order.indexOf(kindKey("Any", SqlType.Scalar.JSON, true)));
                valueOf = key -> key.equals(kindKey("Any", SqlType.Scalar.JSON, true)) ? v : null;
            } else {
                SqlExpr jt = SqlExpr.Call.of(SqlFn.JSON_TYPE, v);
                List<SqlExpr.Case.When> ks = new ArrayList<>();
                ks.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, jt,
                        new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.NULL)), new SqlExpr.NullLit()));
                for (String k : JSON_KINDS.keySet()) {
                    ks.add(new SqlExpr.Case.When(jsonKindIs(k, jt), new SqlExpr.IntLit(order.indexOf(kindKey(k,
                            java.util.Objects.requireNonNull(JSON_KINDS.get(k)), false)))));
                }
                // an object or an array: no typed value here, refused by name
                kOf = new SqlExpr.Case(ks, SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(
                        com.legend.sql.RaiseKind.JUDGED_TREE_NOT_TYPED, List.of())), SqlExpr.Reason.JUDGE);
                valueOf = key -> {
                    for (String k : JSON_KINDS.keySet()) {
                        if (kindKey(k, java.util.Objects.requireNonNull(JSON_KINDS.get(k)), false).equals(key)) {
                            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(jsonKindIs(k, jt), jsonValue(k, v))),
                                    nullOf(java.util.Objects.requireNonNull(columns.get(key), "a kind's SQL type")),
                                    SqlExpr.Reason.JUDGE);
                        }
                    }
                    return null;
                };
            }
        } else if (writtenDate(kind, wrapped)) {
            rows = valueRows(wrapped);
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            SqlExpr text = SqlExpr.Column.of("r", r.outputs(), O);
            SqlType carried = r.outputs().get(0).type();
            kOf = dateKindOfText(text, name -> order.indexOf(writtenKey(name, carried, byType)));
            valueOf = key -> {
                for (String k : DATE_KINDS.keySet()) {
                    if (writtenKey(k, carried, byType).equals(key)) {
                        // compared as its kind's value; recorded as it is written
                        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(writtenKindIs(k, text),
                                byType ? text : writtenDateValue(k, text))), nullOf(java.util.Objects.requireNonNull(
                                columns.get(key), "a kind's SQL type")), SqlExpr.Reason.JUDGE);
                    }
                }
                return null;
            };
        } else {
            Type t = java.util.Objects.requireNonNull(typedClass(kind), "a typed side's class");
            // compared at the judge's instant; recorded as it is held (the precision audit's M4)
            rows = byType ? valueRows(wrapped) : instants(valueRows(wrapped), t);
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            String own = kindKey(kindText(t), byType ? wrapped.outputs().get(0).type() : heldType(wrapped, t),
                    byType);
            kOf = new SqlExpr.IntLit(order.indexOf(own));
            valueOf = key -> key.equals(own) ? SqlExpr.Column.of("r", r.outputs(), O) : null;
        }
        ps.add(new SqlSelect.Projection(kOf, "k", kOut));
        outs.add(kOut);
        for (int j = 0; j < order.size(); j++) {
            SqlType vt = java.util.Objects.requireNonNull(columns.get(order.get(j)), "a kind's SQL type");
            SqlExpr v = valueOf.apply(order.get(j));
            OutputCol out = new OutputCol("v" + j, vt, true);
            ps.add(new SqlSelect.Projection(v == null ? nullOf(vt) : v, out.name(), out));
            outs.add(out);
        }
        ps.add(new SqlSelect.Projection(SqlExpr.Column.of("r", rows.outputs(), RN), RN, rnOut));
        outs.add(rnOut);
        return new SqlSelect(ps, false, new SqlSource.Subselect(rows, "r", null), null, List.of(), null, null,
                List.of(), null, null, outs);
    }

    /** The kind the database judge compares {@code kind}'s values as, typed (item 3's S4), or null where it does not
     *  yet: a String, an Integer, a Float, a Boolean as itself; every Decimal, a declared precision's too, as a Decimal
     *  (compared by value, register S139); an enumeration as itself (its values' names); a StrictDate and a DateTime as
     *  themselves, compared by their instant (register S399; a {@code Date} side's kind is its column's,
     *  {@code JudgmentFacts.typedPair}); an empty side's kind (Nil, a side the types say holds nothing) as itself.
     *  Null for any other kind (an instance, a generic type, a table: their own typed members). */
    public static @com.legend.base.Nullable Type typedClass(@com.legend.base.Nullable Type kind) {
        if (kind == Type.Primitive.STRING || kind == Type.Primitive.INTEGER || kind == Type.Primitive.FLOAT
                || kind == Type.Primitive.BOOLEAN || kind instanceof Type.EnumType || kind == Type.Primitive.STRICT_DATE
                || kind == Type.Primitive.DATE_TIME || kind != null && PlatformTypes.isNil(kind)) {
            return kind;
        }
        return kind == Type.Primitive.DECIMAL || kind instanceof Type.PrecisionDecimal ? Type.Primitive.DECIMAL : null;
    }

    /**
     * A date-time side's typed rows as the database's TIMESTAMPs (register S399, the user's ruling of 2026-11-22: the
     * product compares a date-time by its instant, the database comparing). A side carried as its written text (a
     * literal's TEMPORAL_TEXT, which keeps how it was written for its print) is parsed by the database: every written
     * precision parses as an instant but an hour's, which no database parses, so it is read at its minute (probed on
     * DuckDB 1.5.6 and H2 2.5.252, 2026-11-22). Any other side's rows as they are; null stays null.
     */
    private static @com.legend.base.Nullable SqlQuery instants(@com.legend.base.Nullable SqlQuery rows, Type kind) {
        if (rows == null || kind != Type.Primitive.DATE_TIME && kind != Type.Primitive.STRICT_DATE) {
            return rows;
        }
        SqlType carried = rows.outputs().get(0).type();
        if (carried == SqlType.Scalar.DATE || carried == SqlType.Scalar.TIMESTAMPTZ
                || carried == SqlType.Scalar.TIMESTAMP && kind != Type.Primitive.DATE_TIME) {
            return rows;
        }
        if (carried != SqlType.Scalar.TEMPORAL_TEXT && carried != SqlType.Scalar.VARCHAR
                && carried != SqlType.Scalar.TIMESTAMP) {
            // a date the side carries in no form the database reads as one (a JSON carrier: the dates audit's LOW 6,
            // 2026-11-22): no typed member
            return null;
        }
        if (kind == Type.Primitive.STRICT_DATE) {
            // a day written as its text, read as the database's DATE, the kind a stored day's column holds
            OutputCol dOut = new OutputCol(O, SqlType.Scalar.DATE, false);
            return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("t", rows.outputs(),
                    O), SqlType.Scalar.DATE, SqlExpr.Reason.JUDGE), O, dOut), new SqlSelect.Projection(
                    SqlExpr.Column.of("t", rows.outputs(), RN), RN, rows.outputs().get(1))), false,
                    new SqlSource.Subselect(rows, "t", null), null, List.of(), null, null, List.of(), null, null,
                    List.of(dOut, rows.outputs().get(1)));
        }
        SqlExpr v = SqlExpr.Column.of("t", rows.outputs(), O);
        // every date-time read as the judge's instant (the dialect's JUDGE cast to TIMESTAMP: a value the database
        // types as a date-time, as it holds it, every digit; SQLite's, a text, as the one text of its instant; a text
        // with more digits than the database's date-time reads refused by name -- S399, S181, the user's rule of
        // 2026-11-22) --
        // a stored one as it is held, a written one from its text: an hour's text read at its minute; a day's text (a
        // DateTime over a column that holds days: the egress writes its day alone, the dates audit's MEDIUM 4) read
        // at its midnight, as it is
        SqlExpr written = carried == SqlType.Scalar.TIMESTAMP ? v : new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, v, new SqlExpr.StringLit(":")),
                        new SqlExpr.IntLit(0)), SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.LENGTH, v),
                        new SqlExpr.IntLit(10))),
                SqlExpr.Call.of(SqlFn.CONCAT, v, new SqlExpr.StringLit(":00")))), v, SqlExpr.Reason.JUDGE);
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.TIMESTAMP, false);
        OutputCol rnOut = rows.outputs().get(1);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(written, SqlType.Scalar.TIMESTAMP,
                SqlExpr.Reason.JUDGE), O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t", rows.outputs(), RN), RN,
                rnOut)), false, new SqlSource.Subselect(rows, "t", null), null, List.of(), null, null, List.of(), null,
                null, List.of(oOut, rnOut));
    }

    /** Whether a side's typed rows CTE ({@code cte}) holds no row: emptiness by the rows, never by the list (a database's
     *  list of no rows is NULL on one and {@code []} on another: SQLite's json_group_array; batch 2's audit, HIGH 1). */
    private static SqlExpr noRows(String cte, SqlQuery rows) {
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        return SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.IntLit(1), "__one", one)), false, new SqlSource.Table(cte, cte, rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null,
                null, List.of(one))));
    }

    /** Whether two sides' typed rows carry their values alike: a Decimal the root writes as its text (a store-read
     *  Decimal's {@code DECIMAL_TEXT}, the value lane's egress) against a decimal value does not compare -- no typed
     *  member for the pair (the caller refuses it by name). */
    private static boolean oneCarrier(SqlQuery e, SqlQuery a) {
        return (e.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT)
                == (a.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT);
    }

    /** A typed rows CTE read whole: its {@code __o} and {@code __rn}, the rows the portable comparison reads. */
    private static SqlQuery rowsOf(String cte, SqlQuery rows) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : rows.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of(cte, rows.outputs(), o.name()), o.name(), o));
        }
        return new SqlSelect(ps, false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, rows.outputs());
    }

    private static final String K = "__k";

    /** {@code assert} / {@code assertFalse}'s TYPED member (item 3's S4): the condition's one Boolean as the side's plan
     *  types it, the verdict its {@code IS NOT DISTINCT FROM} the wanted truth; no value rides the failure (its record
     *  is fixed at planning). Null where the side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedCondition(SqlQuery condWrapped, boolean wantTrue) {
        SqlQuery rows = valueRows(condWrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery one = single("__c", rows);
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
            SqlQuery rows = valueRows(side.wrapped());
            if (rows == null) {
                return null;
            }
            SqlQuery one = single("__v" + i, rows);
            ctes.add(new SqlWith.Cte("__v" + i, rows));
            SqlSource read = new SqlSource.Subselect(one, "v" + i, null);
            from = from == null ? read : cross(from, read);
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
        SqlQuery raw = documentTexts(graphPlan);
        SqlQuery rows = raw == null ? null : valueRows(raw);
        if (rows == null) {
            return null;
        }
        SqlQuery docs = jsonDocuments("__xg", rows);
        SqlQuery list = listed("__dg", docs);
        return typedRow(List.of(new SqlWith.Cte("__xg", rows), new SqlWith.Cte("__dg", docs)),
                new SqlSource.Subselect(list, "a", null), SqlExpr.Call.of(SqlFn.EQUAL, graphCount(graphPlan),
                        new SqlExpr.IntLit(0)), List.of(listValue(TYPED_ACTUAL, "a", list, kind)));
    }

    /** {@code assertNotEmpty} over a graph side, TYPED (item 3's S4): its count of root objects not zero -- a failure
     *  names no value, the side being empty. */
    public static TypedRow typedNotEmptyGraph(SqlQuery graphPlan) {
        return typedRow(List.of(), new SqlSource.Dual(), SqlExpr.Call.of(SqlFn.GREATER, graphCount(graphPlan),
                new SqlExpr.IntLit(0)), List.of(new Valued(TYPED_ACTUAL, nullOf(new SqlType.Array(SqlType.Scalar.VARCHAR)),
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
                    TYPED_ACTUAL, nullOf(new SqlType.Array(SqlType.Scalar.VARCHAR)),
                    new SqlType.Array(SqlType.Scalar.VARCHAR), kind, true)));
        }
        SqlQuery rows = valueRows(wrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery list = listed("__a", rows);
        SqlExpr empty = noRows("__a", rows);
        return typedRow(List.of(new SqlWith.Cte("__a", rows)), new SqlSource.Subselect(list, "a", null),
                wantEmpty ? empty : SqlExpr.Call.of(SqlFn.NOT, empty),
                List.of(listValue(TYPED_ACTUAL, "a", list, kind)));
    }

    /** {@code assertEmpty} / {@code assertNotEmpty} over a tagged side (the audit of 946d16f2: a mixed list's filter):
     *  its emptiness the verdict, its values -- each its kind and its value -- riding the failure
     *  ({@code Family.EMPTY_TAGGED}). Null where the side has no kinded reading. */
    public static @com.legend.base.Nullable TypedRow typedEmptyTagged(SqlQuery wrapped, Type kind, boolean wantEmpty) {
        java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
        SqlQuery h = kindsOf(wrapped, kind, held, true) ? kindedRows(wrapped, kind, held, true) : null;
        if (h == null) {
            return null;
        }
        SqlQuery list = listed("__hg", structRows(rowsOf("__gg", h)));
        SqlExpr empty = noRows("__gg", h);
        return typedRow(List.of(new SqlWith.Cte("__gg", h), new SqlWith.Cte("__hg", structRows(rowsOf("__gg", h)))),
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
                    nullOf(new SqlType.Array(SqlType.Scalar.VARCHAR)), new SqlType.Array(SqlType.Scalar.VARCHAR), kind,
                    true), new Valued("__t", new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING,
                    false)));
        }
        SqlQuery rows = valueRows(wrapped);
        if (rows == null || !(kind instanceof Type.Primitive) && !(rows.outputs().get(0).type() instanceof SqlType.Struct)) {
            // an instance's value rides as its struct, the host's reading; carried another way, no typed reading here
            return null;
        }
        SqlQuery list = listed("__a", rows);
        return typedRow(List.of(new SqlWith.Cte("__a", rows)), new SqlSource.Subselect(list, "a", null),
                new SqlExpr.BoolLit(holds), List.of(listValue(TYPED_ACTUAL, "a", list, kind), new Valued("__t",
                        new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING, false)));
    }

    /** Two sides of instances of one class carried as JSON documents' text (item 3's S4, the last families: a
     *  metamodel's SetImplementation, a Table -- a graph's array of its root objects), each side its documents' root
     *  elements, as the host reads a graph side ({@code Verdicts.decodeSideValues}): their texts compared -- one
     *  serializer writes both, so one instance is one text -- in order or as a multiset; each side's elements ride the
     *  failure as JSON, read as instances, as the host's record writes them. Null where a side has no value column. */
    public static @com.legend.base.Nullable TypedRow typedInstanceTexts(SqlQuery eWrapped, Type eKind,
            SqlQuery aWrapped, Type aKind, boolean multiset, boolean wantEqual) {
        SqlQuery eRaw = documentTexts(eWrapped);
        SqlQuery aRaw = documentTexts(aWrapped);
        SqlQuery eRows = eRaw == null ? null : valueRows(eRaw);
        SqlQuery aRows = aRaw == null ? null : valueRows(aRaw);
        if (eRows == null || aRows == null) {
            return null;
        }
        if (!multiset) {
            // in order: the documents' texts in order, each document riding the failure whole, its root objects read
            // where the record is built (Family.EQUALS_DOCUMENTS) -- no row per element, which every database spells
            SqlQuery eList = listed("__xe", eRows);
            SqlQuery aList = listed("__xa", aRows);
            SqlQuery eDocs = listed("__de", jsonDocuments("__xe", eRows));
            SqlQuery aDocs = listed("__da", jsonDocuments("__xa", aRows));
            SqlExpr same = SqlExpr.Call.of(SqlFn.SAME_IN_ORDER, SqlExpr.Column.of("e", eList.outputs(), LISTED),
                    SqlExpr.Column.of("a", aList.outputs(), LISTED), new SqlExpr.ScalarSubquery(rowsOf("__xe", eRows)),
                    new SqlExpr.ScalarSubquery(rowsOf("__xa", aRows)));
            return typedRow(List.of(new SqlWith.Cte("__xe", eRows), new SqlWith.Cte("__xa", aRows),
                            new SqlWith.Cte("__de", jsonDocuments("__xe", eRows)),
                            new SqlWith.Cte("__da", jsonDocuments("__xa", aRows))),
                    cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a",
                            null)), new SqlSource.Subselect(eDocs, "de", null)), new SqlSource.Subselect(aDocs, "da", null)),
                    wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                            listValue(TYPED_EXPECTED, "de", eDocs, eKind), listValue(TYPED_ACTUAL, "da", aDocs, aKind)));
        }
        SqlQuery eEl = rootElements("__xe", eRows);
        SqlQuery aEl = rootElements("__xa", aRows);
        SqlQuery eTexts = elementTexts("__ee", eEl);
        SqlQuery aTexts = elementTexts("__ea", aEl);
        SqlQuery eList = listed("__te", eTexts);
        SqlQuery aList = listed("__ta", aTexts);
        SqlQuery eDocs = listed("__ee", eEl);
        SqlQuery aDocs = listed("__ea", aEl);
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("e", eList.outputs(), LISTED), SqlExpr.Column.of("a", aList.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__te", eTexts)), new SqlExpr.ScalarSubquery(rowsOf("__ta", aTexts)));
        return typedRow(List.of(new SqlWith.Cte("__xe", eRows), new SqlWith.Cte("__xa", aRows),
                        new SqlWith.Cte("__ee", eEl), new SqlWith.Cte("__ea", aEl),
                        new SqlWith.Cte("__te", eTexts), new SqlWith.Cte("__ta", aTexts)),
                cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a", null)),
                        new SqlSource.Subselect(eDocs, "de", null)), new SqlSource.Subselect(aDocs, "da", null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "de", eDocs, eKind), listValue(TYPED_ACTUAL, "da", aDocs, aKind)));
    }

    /** A side's document texts ({@code rows}, read as {@code cte}) each as its JSON document, in order. */
    private static SqlQuery jsonDocuments(String cte, SqlQuery rows) {
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.JSON, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("t", rows.outputs(), O),
                SqlType.Scalar.JSON, SqlExpr.Reason.JUDGE), O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t",
                rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, "t", rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(oOut, rnOut));
    }

    /** A side's documents ({@code rows}, read as {@code cte}) as their root arrays' elements, each JSON, in order. */
    private static SqlQuery rootElements(String cte, SqlQuery rows) {
        OutputCol vOut = new OutputCol("value", SqlType.Scalar.JSON, true);
        SqlSelect elements = CollectionRelations.rows(SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, new SqlExpr.Cast(
                SqlExpr.Column.of("t", rows.outputs(), O), SqlType.Scalar.JSON, SqlExpr.Reason.JUDGE)), "value", vOut,
                List.of(vOut), new SqlSource.Table(cte, "t", rows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE));
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.JSON, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("w", List.of(vOut), "value"), O, oOut),
                new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                        List.of(), List.of(), null), RN, rnOut)), false, new SqlSource.Subselect(elements, "w", null),
                null, List.of(), null, null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /** Elements' rows ({@code rows}, read as {@code cte}) each as its JSON text, in order. */
    private static SqlQuery elementTexts(String cte, SqlQuery rows) {
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.VARCHAR, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("t", rows.outputs(), O),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t",
                rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, "t", rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(oOut, rnOut));
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
        if (gs.orderBy().isEmpty() && plainProjection(gs) && gs.from() instanceof SqlSource.Subselect sub
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

    /** {@code wrapped}'s first column as its text, in the side's own order. */
    private static @com.legend.base.Nullable SqlQuery documentTexts(SqlQuery wrapped) {
        if (wrapped.outputs().isEmpty()) {
            return null;
        }
        OutputCol out = new OutputCol("value", SqlType.Scalar.VARCHAR, true);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("w", wrapped.outputs(),
                wrapped.outputs().get(0).name()), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), "value", out)), false,
                new SqlSource.Subselect(wrapped, "w", null), null, List.of(), null, null, List.of(), null, null,
                List.of(out));
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
        SqlQuery aCan = valueRows(actualCanon);
        SqlQuery aRaw = valueRows(actualRaw);
        SqlQuery eCan = goldenElements == null ? valueRows(goldenCanon) : valueRows(goldenElements);
        if (aCan == null || aRaw == null || eCan == null) {
            return null;
        }
        List<SqlWith.Cte> ctes = new ArrayList<>(List.of(new SqlWith.Cte("__a", aCan), new SqlWith.Cte("__r", aRaw),
                new SqlWith.Cte("__e", eCan)));
        SqlQuery rawList = listed("__r", aRaw);
        SqlExpr holds;
        SqlSource from;
        if (goldenElements == null) {
            SqlQuery e1 = single("__e", eCan);
            SqlQuery a1 = single("__a", aCan);
            holds = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                    new SqlExpr.Cast(SqlExpr.Column.of("e", e1.outputs(), SINGLE), SqlType.Scalar.VARCHAR,
                            SqlExpr.Reason.JUDGE),
                    new SqlExpr.Cast(SqlExpr.Column.of("a", a1.outputs(), SINGLE), SqlType.Scalar.VARCHAR,
                            SqlExpr.Reason.JUDGE));
            from = cross(new SqlSource.Subselect(e1, "e", null), new SqlSource.Subselect(a1, "a", null));
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
            SqlQuery eList = listed("__e", eCan);
            SqlQuery aList = listed("__ae", aEl);
            holds = SqlExpr.Call.of(SqlFn.SAME_IN_ANY_ORDER, SqlExpr.Column.of("e", eList.outputs(), LISTED),
                    SqlExpr.Column.of("a", aList.outputs(), LISTED), new SqlExpr.ScalarSubquery(rowsOf("__e", eCan)),
                    new SqlExpr.ScalarSubquery(rowsOf("__ae", aEl)));
            from = cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a", null));
        }
        return typedRow(ctes, cross(from, new SqlSource.Subselect(rawList, "r", null)), holds, List.of(
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
        SqlQuery cRows = valueRows(condWrapped);
        if (cRows == null || valWrapped.outputs().isEmpty()) {
            return null;
        }
        List<SqlWith.Cte> ctes = new ArrayList<>(List.of(new SqlWith.Cte("__c", cRows)));
        SqlQuery vList;
        if (valWrapped.outputs().get(0).type() instanceof SqlType.Tagged) {
            java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
            SqlQuery h = kindsOf(valWrapped, kind, held, true) ? kindedRows(valWrapped, kind, held, true) : null;
            if (h == null) {
                return null;
            }
            ctes.add(new SqlWith.Cte("__gv", h));
            ctes.add(new SqlWith.Cte("__hv", structRows(rowsOf("__gv", h))));
            vList = listed("__hv", structRows(rowsOf("__gv", h)));
        } else {
            SqlQuery vRows = valueRows(valWrapped);
            if (vRows == null) {
                return null;
            }
            ctes.add(new SqlWith.Cte("__v", vRows));
            vList = listed("__v", vRows);
        }
        SqlQuery one = single("__c", cRows);
        return typedRow(ctes, cross(new SqlSource.Subselect(one, "c", null), new SqlSource.Subselect(vList, "v", null)),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("c", one.outputs(), SINGLE),
                        new SqlExpr.BoolLit(true)), List.of(listValue(TYPED_ACTUAL, "v", vList, kind), new Valued("__t",
                        new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING, false)));
    }

    /**
     * {@code assertContains} / {@code assertNotContains}' TYPED member over tagged values (item 3's S4, the audit of
     * 946d16f2: a mixed list, an Any): each value its kind and its value ({@link #kindedRows}, as {@link #typedTagged}
     * compares them); the verdict whether some element has the value's kind and a value {@code IS NOT DISTINCT FROM} it,
     * under {@code want}; both sides' tagged values ride the failure ({@code Family.CONTAINS_TAGGED}). Null where a
     * side has no kinded reading.
     */
    public static @com.legend.base.Nullable TypedRow typedContainsTagged(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, boolean want) {
        java.util.TreeMap<String, SqlType> kinds = new java.util.TreeMap<>();
        java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
        if (!kindsOf(collWrapped, collKind, kinds, false) || !kindsOf(valWrapped, valKind, kinds, false)
                || !kindsOf(collWrapped, collKind, held, true) || !kindsOf(valWrapped, valKind, held, true)) {
            return null;
        }
        SqlQuery cc = kindedRows(collWrapped, collKind, kinds, false);
        SqlQuery vc = kindedRows(valWrapped, valKind, kinds, false);
        SqlQuery ch = kindedRows(collWrapped, collKind, held, true);
        SqlQuery vh = kindedRows(valWrapped, valKind, held, true);
        if (cc == null || vc == null || ch == null || vh == null) {
            return null;
        }
        SqlExpr on = SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("__c", cc.outputs(), "k"),
                SqlExpr.Column.of("__v", vc.outputs(), "k"));
        for (int j = 0; j < kinds.size(); j++) {
            on = SqlExpr.Call.of(SqlFn.AND, on, SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                    SqlExpr.Column.of("__c", cc.outputs(), "v" + j), SqlExpr.Column.of("__v", vc.outputs(), "v" + j)));
        }
        OutputCol mOut = new OutputCol("__m", SqlType.Scalar.BIGINT, false);
        SqlSelect m = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                false, List.of()), "__m", mOut)), false, new SqlSource.Join(new SqlSource.Table("__c", "__c",
                cc.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), new SqlSource.Table("__v", "__v",
                vc.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), SqlSource.Join.Kind.INNER, on),
                null, List.of(), null, null, List.of(), null, null, List.of(mOut));
        SqlExpr some = SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Column.of("m", m.outputs(), "__m"),
                new SqlExpr.IntLit(0));
        SqlQuery cList = listed("__hc", structRows(rowsOf("__gc", ch)));
        SqlQuery vList = listed("__hv", structRows(rowsOf("__gv", vh)));
        return typedRow(List.of(new SqlWith.Cte("__c", cc), new SqlWith.Cte("__v", vc), new SqlWith.Cte("__gc", ch),
                        new SqlWith.Cte("__gv", vh), new SqlWith.Cte("__hc", structRows(rowsOf("__gc", ch))),
                        new SqlWith.Cte("__hv", structRows(rowsOf("__gv", vh)))),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                want ? some : SqlExpr.Call.of(SqlFn.NOT, some), List.of(
                        listValue(TYPED_EXPECTED, "c", cList, collKind), listValue(TYPED_ACTUAL, "v", vList, valKind)));
    }

    /** {@code assertContains} / {@code assertNotContains}' TYPED member (item 3's S4): the collection's values and the
     *  value's, typed; the verdict whether some element {@code IS NOT DISTINCT FROM} the value, under {@code want}; both
     *  lists ride the failure, the collection's first. */
    public static @com.legend.base.Nullable TypedRow typedContains(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, boolean want) {
        // a date-time compared by its instant (register S399), as equality's sides are (the dates audit, 2026-11-22)
        SqlQuery cRaw = valueRows(collWrapped);
        SqlQuery vRaw = valueRows(valWrapped);
        SqlQuery cRows = cRaw == null ? null : instants(cteRows("__xc", cRaw), collKind);
        SqlQuery vRows = vRaw == null ? null : instants(cteRows("__xv", vRaw), valKind);
        if (cRaw == null || vRaw == null || cRows == null || vRows == null || !oneCarrier(cRows, vRows)) {
            return null;
        }
        // the record's values as each side carries them, not the instants compared (the precision audit's M4)
        SqlQuery cList = listed("__xc", cRaw);
        SqlQuery vList = listed("__xv", vRaw);
        OutputCol mOut = new OutputCol("__m", SqlType.Scalar.BIGINT, false);
        SqlSource matched = new SqlSource.Join(
                new SqlSource.Table("__c", "__c", cRows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE),
                new SqlSource.Table("__v", "__v", vRows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("__c", cRows.outputs(), O),
                        SqlExpr.Column.of("__v", vRows.outputs(), O)));
        SqlSelect m = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                false, List.of()), "__m", mOut)), false, matched, null, List.of(), null, null, List.of(), null, null,
                List.of(mOut));
        SqlExpr some = SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Column.of("m", m.outputs(), "__m"),
                new SqlExpr.IntLit(0));
        return typedRow(List.of(new SqlWith.Cte("__xc", cRaw), new SqlWith.Cte("__xv", vRaw),
                        new SqlWith.Cte("__c", cRows), new SqlWith.Cte("__v", vRows)),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                want ? some : SqlExpr.Call.of(SqlFn.NOT, some), List.of(
                        listValue(TYPED_EXPECTED, "c", cList, collKind), listValue(TYPED_ACTUAL, "v", vList, valKind)));
    }

    /** {@code assertEqWithinTolerance}'s TYPED member (item 3's S4): the three numbers as their plans type them, the
     *  verdict {@code |e - a| <= t} computed in {@code arithmetic} (a NULL operand fails); each number rides the failure
     *  as its own kind. Null where a side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedTolerance(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, SqlQuery tWrapped, Type tKind, SqlType arithmetic) {
        SqlQuery eRows = valueRows(eWrapped);
        SqlQuery aRows = valueRows(aWrapped);
        SqlQuery tRows = valueRows(tWrapped);
        if (eRows == null || aRows == null || tRows == null) {
            return null;
        }
        SqlQuery ev = single("__e", eRows);
        SqlQuery av = single("__a", aRows);
        SqlQuery tv = single("__t", tRows);
        SqlExpr e = SqlExpr.Column.of("e", ev.outputs(), SINGLE);
        SqlExpr a = SqlExpr.Column.of("a", av.outputs(), SINGLE);
        SqlExpr t = SqlExpr.Column.of("t", tv.outputs(), SINGLE);
        SqlExpr within = SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS,
                new SqlExpr.Cast(e, arithmetic, SqlExpr.Reason.JUDGE), new SqlExpr.Cast(a, arithmetic,
                        SqlExpr.Reason.JUDGE))), new SqlExpr.Cast(t, arithmetic, SqlExpr.Reason.JUDGE));
        return typedRow(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows),
                        new SqlWith.Cte("__t", tRows)),
                cross(cross(new SqlSource.Subselect(ev, "e", null), new SqlSource.Subselect(av, "a", null)),
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
        SqlQuery rows = valueRows(vectorWrapped);
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

    /** The {@code $need->forAll(n | $have->contains($n))} subset fold of a condition, TYPED (item 3's S4): no needed
     *  value without a have that is not distinct from it, under {@code wantTrue}; its record the condition's, fixed at
     *  planning; a date-time by its instant ({@code kind}, the sides' one typed class). Null where a side has no typed
     *  value column. */
    public static @com.legend.base.Nullable TypedRow typedSubset(SqlQuery needWrapped, SqlQuery haveWrapped,
            Type kind, boolean wantTrue) {
        SqlQuery nRows = instants(valueRows(needWrapped), kind);
        SqlQuery hRows = instants(valueRows(haveWrapped), kind);
        if (nRows == null || hRows == null || !oneCarrier(nRows, hRows)) {
            return null;
        }
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlExpr present = new SqlExpr.Exists(new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1),
                "__one", one)), false, new SqlSource.Table("__h", "__h", hRows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                SqlExpr.Column.of("__h", hRows.outputs(), O), SqlExpr.Column.of("__n", nRows.outputs(), O)), List.of(),
                null, null, List.of(), null, null, List.of(one)));
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        SqlSelect missing = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of("__n", nRows.outputs(), RN)), false, List.of()), K, kOut)), false,
                new SqlSource.Table("__n", "__n", nRows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), SqlExpr.Call.of(SqlFn.NOT, present), List.of(), null, null,
                List.of(), null, null, List.of(kOut));
        SqlExpr none = SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("m", missing.outputs(), K),
                new SqlExpr.IntLit(0));
        return typedRow(List.of(new SqlWith.Cte("__n", nRows), new SqlWith.Cte("__h", hRows)),
                new SqlSource.Subselect(missing, "m", null), wantTrue ? none : SqlExpr.Call.of(SqlFn.NOT, none),
                List.of());
    }

    /**
     * A quantified equality judged element by element, TYPED (item 3's S4): {@code pairs}' rows, each element's two
     * operands one {@code {first, second}} struct in the source's order; each element's verdict its two typed operands
     * not distinct ({@code neverEqual}: kinds lite never calls equal, S127); the verdict that none fails, and where one
     * does, the FIRST failing element's zero-based index and its two operands ride the failure. Null where the pairs'
     * struct states no field types.
     */
    public static @com.legend.base.Nullable TypedRow typedPerElement(SqlQuery pairs, Type expectedKind,
            Type actualKind, boolean neverEqual) {
        OutputCol valueOut = pairs.outputs().get(0);
        if (!(valueOut.type() instanceof SqlType.Struct st) || st.fields().size() != 2) {
            return null;
        }
        SqlType ft = st.fields().stream().filter(f -> f.name().equals("first")).map(SqlType.Struct.Field::type)
                .findFirst().orElse(null);
        SqlType sdt = st.fields().stream().filter(f -> f.name().equals("second")).map(SqlType.Struct.Field::type)
                .findFirst().orElse(null);
        if (ft == null || sdt == null) {
            return null;
        }
        SqlQuery numbered = SideOrdinal.withOrdinal(pairs);
        SqlExpr value = SqlExpr.Column.of("__s", numbered.outputs(), valueOut.name());
        SqlExpr first = SqlExpr.StructGet.of(value, "first");
        SqlExpr second = SqlExpr.StructGet.of(value, "second");
        SqlExpr ok = neverEqual ? new SqlExpr.BoolLit(false) : SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, first, second);
        OutputCol okOut = new OutputCol("__ok", SqlType.Scalar.BOOLEAN, false);
        OutputCol fOut = new OutputCol("__f", ft, true);
        OutputCol sOut = new OutputCol("__x", sdt, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<OutputCol> pe = List.of(okOut, fOut, sOut, rnOut);
        SqlQuery rows = new SqlSelect(List.of(new SqlSelect.Projection(ok, "__ok", okOut),
                new SqlSelect.Projection(first, "__f", fOut), new SqlSelect.Projection(second, "__x", sOut),
                new SqlSelect.Projection(SqlExpr.Column.of("__s", numbered.outputs(), SideOrdinal.ORD), RN, rnOut)),
                false, new SqlSource.Subselect(numbered, "__s", null), null, List.of(), null, null, List.of(), null,
                null, pe);
        SqlExpr failingAt = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Column.of("__pe", pe, "__ok")),
                SqlExpr.Column.of("__pe", pe, RN))), null, SqlExpr.Reason.JUDGE);
        OutputCol fi = new OutputCol("__fi", SqlType.Scalar.BIGINT, true);
        OutputCol fe = new OutputCol("__fe", ft, true);
        OutputCol fa = new OutputCol("__fa", sdt, true);
        SqlSelect b = new SqlSelect(List.of(
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(failingAt), false, List.of()),
                        "__fi", fi),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.ARG_MIN,
                        List.of(SqlExpr.Column.of("__pe", pe, "__f"), failingAt), false, List.of()), "__fe", fe),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.ARG_MIN,
                        List.of(SqlExpr.Column.of("__pe", pe, "__x"), failingAt), false, List.of()), "__fa", fa)),
                false, new SqlSource.Table("__pe", "__pe", pe, false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(fi, fe, fa));
        SqlExpr firstFailing = SqlExpr.Column.of("b", b.outputs(), "__fi");
        return typedRow(List.of(new SqlWith.Cte("__pe", rows)), new SqlSource.Subselect(b, "b", null),
                SqlExpr.Call.of(SqlFn.IS_NULL, firstFailing), List.of(
                        new Valued(TYPED_EXPECTED, SqlExpr.Column.of("b", b.outputs(), "__fe"), ft, expectedKind, false),
                        new Valued(TYPED_ACTUAL, SqlExpr.Column.of("b", b.outputs(), "__fa"), sdt, actualKind, false),
                        new Valued("__i", SqlExpr.Call.of(SqlFn.MINUS, firstFailing, new SqlExpr.IntLit(1)),
                                SqlType.Scalar.BIGINT, Type.Primitive.INTEGER, false)));
    }

    /** A typed member's value: its column, its expression, its SQL and Pure types, a list or one. */
    private record Valued(String name, SqlExpr expr, SqlType type, Type kind, boolean many) {
    }

    /** A listed side's list ({@link #listed}) as a typed member's value. */
    private static Valued listValue(String name, String alias, SqlQuery listed, Type kind) {
        return new Valued(name, SqlExpr.Column.of(alias, listed.outputs(), LISTED), listed.outputs().get(0).type(), kind,
                true);
    }

    /**
     * A TYPED member's row (item 3's S4): its verdict {@code holds}, then each of {@code values}, written only where
     * the verdict fails -- one row over {@code from}, the members' CTEs at its head -- and the schema of its columns.
     */
    private static TypedRow typedRow(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr holds, List<Valued> values) {
        if (comparesSides(holds)) {
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

    private static SqlSource cross(SqlSource left, SqlSource right) {
        return new SqlSource.Join(left, right, SqlSource.Join.Kind.CROSS, null);
    }

    private static final String SINGLE = "__v";

    /** A side's typed rows ({@code cte}) as its one value: {@code __v}, the least of its values (a one-valued side's
     *  value; NULL where it has none). */
    private static SqlQuery single(String cte, SqlQuery rows) {
        OutputCol o = new OutputCol(SINGLE, rows.outputs().get(0).type(), true);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN,
                List.of(SqlExpr.Column.of(cte, rows.outputs(), O)), false, List.of()), SINGLE, o)), false,
                new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of(o));
    }

    /** {@code value} where the verdict {@code holds} not, else NULL: a typed member's values ride its failure alone. */
    private static SqlExpr whereFails(SqlExpr holds, SqlExpr value) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(holds, new SqlExpr.NullLit())), value,
                SqlExpr.Reason.JUDGE);
    }

    /** A side's typed rows ({@code cte}: {@code __o}, {@code __rn}) folded to one row: {@code __l}, its values in its own
     *  order -- the LIST reducer, which each database carries as it carries a list (H2: a JSON array) and exec decodes
     *  as any list result. The list rides the failure's record; no verdict reads it (emptiness is the rows', noRows). */
    private static SqlQuery listed(String cte, SqlQuery rows) {
        OutputCol o = rows.outputs().get(0);
        OutputCol l = new OutputCol(LISTED, new SqlType.Array(o.type()), true);
        // never NULL: a typed rows CTE drops a NULL value (valueRows), so the list needs no null clause (H2's)
        SqlExpr value = SqlExpr.Column.of(cte, O, o.type(), false, OutputCol.Origin.DERIVED);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.LIST,
                List.of(value), false,
                List.of(new SqlSelect.SortKey(SqlExpr.Column.of(cte, rows.outputs(), RN), true, null, null))),
                LISTED, l)), false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of(l));
    }

    /**
     * A side's TYPED rows (item 3's S4): {@code __o}, each value as the side's plan types it, and {@code __rn}, its place
     * in the side's own order -- numbered as {@link #canonRows} numbers them, with no canon; a NULL value dropped (Pure
     * has no null value). Null where the value's column states no type.
     */
    private static @com.legend.base.Nullable SqlQuery valueRows(SqlQuery wrapped) {
        return valueRows(wrapped, 1);
    }

    /** {@link #valueRows} carrying the side's first {@code width} columns: {@code __o}, then {@code __s1, __s2, ...}
     *  (a tagged value's slots beside its tag), then {@code __rn}; the rows those whose first column is not NULL. */
    private static @com.legend.base.Nullable SqlQuery valueRows(SqlQuery wrapped, int width) {
        if (!(wrapped instanceof SqlSelect ws) || ws.projections().size() < width) {
            return null;
        }
        List<SqlSelect.Projection> cols = ws.projections().subList(0, width);
        for (SqlSelect.Projection p : cols) {
            if (p.out() == null || p.alias() == null) {
                return null;
            }
        }
        List<OutputCol> outs = new ArrayList<>();
        for (int i = 0; i < width; i++) {
            outs.add(new OutputCol(i == 0 ? O : "__s" + i, java.util.Objects.requireNonNull(cols.get(i).out()).type(),
                    i > 0));
        }
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<OutputCol> all = new ArrayList<>(outs);
        all.add(rnOut);
        if (!plainProjection(ws)) {
            // a wrap that stays a layer (an aggregate value, a filtered or capped wrap): its values, its own order
            List<OutputCol> kept = cols.stream().map(p -> java.util.Objects.requireNonNull(p.out())).toList();
            SqlSelect trimmed = new SqlSelect(cols, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                    ws.having(), ws.qualify(), ws.orderBy(), ws.limit(), ws.offset(), kept);
            SqlQuery read = ws.orderBy().isEmpty() ? trimmed : SideOrdinal.withOrdinal(trimmed);
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (int i = 0; i < width; i++) {
                ps.add(new SqlSelect.Projection(SqlExpr.Column.of("w", read.outputs(),
                        java.util.Objects.requireNonNull(cols.get(i).alias())), outs.get(i).name(), outs.get(i)));
            }
            ps.add(new SqlSelect.Projection(rowNumber(read.outputs()), RN, rnOut));
            return new SqlSelect(ps, false, new SqlSource.Subselect(read, "w", null), SqlExpr.Call.of(
                    SqlFn.IS_NOT_NULL, SqlExpr.Column.of("w", read.outputs(), java.util.Objects.requireNonNull(
                            cols.get(0).alias()))), List.of(), null,
                    null, List.of(), null, null, all);
        }
        List<SqlSelect.SortKey> order = ws.orderBy();
        SqlSource from = ws.from();
        if (order.isEmpty() && from instanceof SqlSource.Subselect sub
                && SideOrdinal.defined(sub.inner()) instanceof SqlQuery ordered) {
            // the order the side's own plan defines, numbered inside it (SideOrdinal), as canonRows numbers it
            from = new SqlSource.Subselect(ordered, sub.alias(), sub.frameName());
            order = List.of(new SqlSelect.SortKey(SqlExpr.Column.of(sub.alias(), ordered.outputs(), SideOrdinal.ORD),
                    true, null, null));
        }
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), order,
                null);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (int i = 0; i < width; i++) {
            ps.add(new SqlSelect.Projection(cols.get(i).expr(), outs.get(i).name(), outs.get(i)));
        }
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        return new SqlSelect(ps, false, from, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, cols.get(0).expr()), List.of(), null,
                null, List.of(), null, null, all);
    }

    // ── the GRID forms (leg 3.1b): a TABULAR side rides the grid wrap
    // (CanonicalRenderSql.wrapTdsCanon: __rowcanon + __cell<i>); its value
    // PEER (a literal list) is framed into rows of the grid's width, or
    // compared as a loose cell pool (the sameElements form) — the same
    // rules TdsCompare.peerRowCanons / tdsCellCanons apply in Java.

    /** A grid side: its wrapped plan and its width (columns). */
    /** {@code floatColumns}: per column, whether it is DECLARED Float —
     * the 2-ULP leniency's operand columns. */
    /** {@code emptyIsNull}: per column, whether the assert's own grammar
     * conflates the empty string with NULL ({@code toCSV} prints both as
     * an empty cell) — the verdict then judges under that equivalence: a
     * String cell's empty canon reads as the TDSNull sentinel. */
    public record GridSide(SqlQuery wrapped, int width, List<Boolean> floatColumns,
            List<Boolean> emptyIsNull) {
        public GridSide(SqlQuery wrapped, int width, List<Boolean> floatColumns) {
            this(wrapped, width, floatColumns, List.of());
        }
        boolean emptyIsNullAt(int i) {
            return i < emptyIsNull.size() && emptyIsNull.get(i);
        }
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
    public static @com.legend.base.Nullable TypedRow typedTdsEquivalent(GridSide one, GridSide two, List<Type> kinds,
            List<Type> twoKinds, SqlQuery deltaRows, SqlQuery timeDeltaRows, boolean columnsDiffer, Type eRows,
            Type aRows) {
        if (!carriesCells(one) || !carriesCells(two)) {
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
        SqlQuery eList = listed("__ge", rowStructs(one, GRID_E, kinds));
        SqlQuery aList = listed("__ga", rowStructs(two, GRID_A, twoKinds));
        OutputCol dOut = new OutputCol("__dv", SqlType.Scalar.DOUBLE, true);
        SqlSelect deltaValue = new SqlSelect(List.of(new SqlSelect.Projection(delta, "__dv", dOut)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(dOut));
        List<SqlWith.Cte> ctes = List.of(new SqlWith.Cte(GRID_E, one.wrapped()), new SqlWith.Cte(GRID_A, two.wrapped()),
                new SqlWith.Cte("__e", e), new SqlWith.Cte("__a", a), new SqlWith.Cte("__d", deltaRows),
                new SqlWith.Cte("__t", timeDeltaRows), new SqlWith.Cte("__p", bad),
                new SqlWith.Cte("__ge", rowStructs(one, GRID_E, kinds)),
                new SqlWith.Cte("__ga", rowStructs(two, GRID_A, twoKinds)));
        return typedRow(ctes, cross(cross(cross(new SqlSource.Subselect(same, "q", null),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                        new SqlSource.Subselect(deltaValue, "d", null)),
                SqlExpr.Column.of("q", same.outputs(), SAME_H), List.of(listValue(TYPED_EXPECTED, "e", eList, eRows),
                        listValue(TYPED_ACTUAL, "a", aList, aRows), new Valued("__dv", SqlExpr.Column.of("d",
                                deltaValue.outputs(), "__dv"), SqlType.Scalar.DOUBLE, Type.Primitive.FLOAT, false)));
    }

    private static final String SAME_H = "__holds";

    /** A grid's rows as row structs ({@code __o}, its cells as fields {@code c0, c1, ...}, each typed as its column
     *  declares ({@link #typedCell}); a struct is never NULL, whatever its cells) numbered as its cells are
     *  ({@code __rn}), read through its CTE. */
    private static SqlQuery rowStructs(GridSide grid, String named, List<Type> kinds) {
        return structRows(typedCells(grid, named, kinds, null));
    }

    /** {@code cells}' rows ({@code c0, c1, ..., __rn}) as row structs: {@code __o} and {@code __rn}. */
    private static SqlQuery structRows(SqlQuery cells) {
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
        List<SqlType.Struct.Field> types = new ArrayList<>();
        for (OutputCol o : cells.outputs()) {
            if (!o.name().equals(RN)) {
                fields.add(new SqlExpr.StructLit.Field(o.name(), SqlExpr.Column.of("k", cells.outputs(), o.name()),
                        o.type()));
                types.add(new SqlType.Struct.Field(o.name(), o.type()));
            }
        }
        OutputCol oOut = new OutputCol(O, new SqlType.Struct(types), false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.StructLit(fields), O, oOut),
                new SqlSelect.Projection(SqlExpr.Column.of("k", cells.outputs(), RN), RN, rnOut)), false,
                new SqlSource.Subselect(cells, "k", null), null, List.of(), null, null, List.of(), null, null,
                List.of(oOut, rnOut));
    }

    /**
     * A grid's rows as typed cells, one column each ({@code c0, c1, ...}), numbered ({@code __rn}) in the grid's order,
     * read through its CTE ({@code named}): each cell as its plan types it, and, where {@code tagged} names
     * a column, its KIND in place of its value ({@code CASE WHEN cell IS NULL THEN NULL ELSE 'Integer' END}): two cells
     * of kinds lite never calls equal (register S127) alike only where both are NULL, the database comparing.
     */
    private static SqlQuery typedCells(GridSide grid, String named, List<Type> kinds,
            @com.legend.base.Nullable List<Boolean> tagged) {
        List<SqlSelect.Projection> data = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size())) : List.of();
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            SqlExpr cell = typedCell(grid, data.get(i));
            SqlType type = java.util.Objects.requireNonNull(data.get(i).out(), "a grid's data column with no type").type();
            if (tagged != null && i < tagged.size() && tagged.get(i)) {
                cell = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, cell),
                        new SqlExpr.NullLit())), new SqlExpr.StringLit(kindText(kinds.get(i))), SqlExpr.Reason.JUDGE);
                type = SqlType.Scalar.VARCHAR;
            }
            OutputCol out = new OutputCol("c" + i, type, true);
            ps.add(new SqlSelect.Projection(cell, out.name(), out));
            outs.add(out);
        }
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(rowNumber(grid.wrapped().outputs()), RN, rnOut));
        outs.add(rnOut);
        return new SqlSelect(ps, false, over(grid, named), null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** A grid's data column's cell as its plan types it: the carrier the host's rows read too (the table slice's
     *  audit, M1, 2026-11-22: a Float held as a DECIMAL cast to a DOUBLE wrote 1.5 where the host writes 1.50). */
    private static SqlExpr typedCell(GridSide grid, SqlSelect.Projection p) {
        return SqlExpr.Column.of("w", grid.wrapped().outputs(),
                java.util.Objects.requireNonNull(p.alias(), "a grid's data column with no name"));
    }

    /**
     * {@code assertEquals} and {@code assertNotEquals} over two GRIDS, TYPED (item 3's S4, tables, 2026-11-22): each
     * grid's rows as typed cells ({@link #typedCells}), compared row by row as the host's {@code TdsCompare.grids}
     * compares them -- in the grid's order where the actual is sorted, else as a multiset of rows ({@code multiset}) --
     * by the one list node ({@link SqlFn#SAME_IN_ORDER}, {@link SqlFn#SAME_IN_ANY_ORDER}); false where the two grids'
     * columns differ in name or number ({@code schemasDiffer}, a fact of the types); a column whose two kinds lite never
     * calls equal compared by its kind tag. Where the verdict fails, each grid's rows ride back as typed row structs
     * for the host's record ({@code AssertKind.EQUALS} over the rows, {@code NOT_EQUALS}). Null where a column has no
     * typed class ({@link #typedClass}), or a date column's cells are no date the database holds.
     */
    public static @com.legend.base.Nullable TypedRow typedGridPair(GridSide e, List<Type> eKinds, GridSide a,
            List<Type> aKinds, boolean multiset, boolean wantEqual, boolean schemasDiffer, Type eRows, Type aRows) {
        if (!typedGrid(e, eKinds) || !typedGrid(a, aKinds)) {
            return null;
        }
        List<Boolean> tagged = new ArrayList<>();
        for (int i = 0; i < eKinds.size(); i++) {
            tagged.add(!schemasDiffer && i < aKinds.size()
                    && !java.util.Objects.equals(typedClass(eKinds.get(i)), typedClass(aKinds.get(i))));
        }
        boolean anyTagged = tagged.contains(true);
        SqlQuery ce = typedCells(e, GRID_E, eKinds, tagged);
        SqlQuery ca = typedCells(a, GRID_A, aKinds, tagged);
        List<SqlWith.Cte> ctes = new ArrayList<>(List.of(new SqlWith.Cte(GRID_E, e.wrapped()),
                new SqlWith.Cte(GRID_A, a.wrapped()), new SqlWith.Cte("__ce", ce), new SqlWith.Cte("__ca", ca),
                new SqlWith.Cte("__ke", structRows(rowsOf("__ce", ce))),
                new SqlWith.Cte("__ka", structRows(rowsOf("__ca", ca)))));
        SqlQuery eCmp = listed("__ke", structRows(rowsOf("__ce", ce)));
        SqlQuery aCmp = listed("__ka", structRows(rowsOf("__ca", ca)));
        SqlQuery eList = eCmp;
        SqlQuery aList = aCmp;
        if (anyTagged) {
            // the record's rows are the cells themselves, not their tags
            ctes.add(new SqlWith.Cte("__ge", rowStructs(e, GRID_E, eKinds)));
            ctes.add(new SqlWith.Cte("__ga", rowStructs(a, GRID_A, aKinds)));
            eList = listed("__ge", rowStructs(e, GRID_E, eKinds));
            aList = listed("__ga", rowStructs(a, GRID_A, aKinds));
        }
        SqlExpr same = schemasDiffer ? new SqlExpr.BoolLit(false) : SqlExpr.Call.of(multiset
                        ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__ce", ce)), new SqlExpr.ScalarSubquery(rowsOf("__ca", ca)));
        SqlSource from = cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null));
        if (anyTagged) {
            from = cross(cross(from, new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a",
                    null));
        }
        return typedRow(ctes, from, wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                listValue(TYPED_EXPECTED, anyTagged ? "e" : "ke", eList, eRows),
                listValue(TYPED_ACTUAL, anyTagged ? "a" : "ka", aList, aRows)));
    }

    /** Whether a grid's plan names and types each of its data columns, the cells a typed member reads. */
    private static boolean carriesCells(GridSide grid) {
        return grid.wrapped() instanceof SqlSelect ws && ws.projections().size() >= grid.width()
                && ws.projections().subList(0, grid.width()).stream().allMatch(p -> p.alias() != null
                        && p.out() != null);
    }

    /** Whether each of a grid's columns has a typed class the cells carry: a date column's cells a DATE or TIMESTAMP,
     *  never a text, and no column empty-is-NULL (toCSV's equivalence: no typed form, refused by name). */
    private static boolean typedGrid(GridSide grid, List<Type> kinds) {
        if (!carriesCells(grid) || kinds.size() != grid.width()) {
            return false;
        }
        List<SqlSelect.Projection> data = ((SqlSelect) grid.wrapped()).projections().subList(0, grid.width());
        for (int i = 0; i < data.size(); i++) {
            Type k = typedClass(kinds.get(i));
            SqlSelect.Projection p = data.get(i);
            if (k == null || grid.emptyIsNullAt(i)) {
                return false;
            }
            SqlType t = java.util.Objects.requireNonNull(p.out(), "a grid's data column with no type").type();
            if (k == Type.Primitive.STRICT_DATE && t != SqlType.Scalar.DATE
                    || k == Type.Primitive.DATE_TIME && t != SqlType.Scalar.TIMESTAMP) {
                return false;
            }
        }
        return true;
    }

    /** A grid's cells row-major with a NUMERIC value ({@code __n}, any
     * numeric kind) and a TEMPORAL value in epoch seconds ({@code __s}). */
    private static SqlQuery toleranceCells(GridSide grid, List<Type> kinds, String named) {
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
                ps.add(new SqlSelect.Projection(own == null ? nullOf(outs.get(c).type())
                        : SqlExpr.Column.of("v", body.outputs(), own.name()), outs.get(c).name(), outs.get(c)));
            }
            branches.add(new SqlSelect(ps, false, new SqlSource.Subselect(body, "v", null), null, List.of(), null,
                    null, List.of(), null, null, outs));
        }
        SqlQuery union = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        return new Batch(ctes.isEmpty() ? union : new SqlWith(ctes, union), new Type.RelationType(schema), layout);
    }

    /** A slot's NULL where a member writes none: typed where the renderer leaves a typed slot's NULL bare -- a Decimal,
     *  a TIME -- since Postgres types a UNION's column pairwise from its left, and two bare NULL branches first make it
     *  text, which a numeric branch then fails to match (the S4 tall-row audit's MEDIUM, 2026-11-22). The renderer
     *  types every other scalar slot's NULL itself; a list's, a struct's and a label carrier's stay bare, as it writes
     *  them. */
    private static SqlExpr nullOf(SqlType slot) {
        return slot instanceof SqlType.Decimal || slot == SqlType.Scalar.TIME
                ? new SqlExpr.Cast(new SqlExpr.NullLit(), slot, SqlExpr.Reason.JUDGE) : new SqlExpr.NullLit();
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

    /** A kind's name as a typed row records it: a primitive's short name (a Decimal of a declared precision a Decimal),
     *  any other type its own name. */
    public static String kindText(Type t) {
        return t instanceof Type.PrecisionDecimal ? "Decimal" : t.typeName();
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


    /** A many or optional side as rows {@code (__c, __rn[, __v])} at ONE
     * level: the chosen canon expression spliced over the plan; the wrap's
     * canonical order rides the row number's own window. */
    /** A wrap that only projects row expressions over its source: no
     * aggregate or window in a projection, no where / group / having /
     * qualify / distinct / limit / offset — its projections can be spliced
     * into a reader without changing what they compute. A JSON-document
     * side ({@code to_json(list(…))}) is NOT one: its value is an aggregate
     * over the plan's rows and must stay a layer. */
    private static boolean plainProjection(SqlSelect ws) {
        if (ws.where() != null || !ws.groupBy().isEmpty() || ws.having() != null
                || ws.qualify() != null || ws.distinct() || ws.limit() != null || ws.offset() != null) {
            return false;
        }
        boolean[] aggregate = {false};
        SqlSelect projectionsOnly = new SqlSelect(ws.projections(), false, new SqlSource.Dual(),
                null, List.of(), null, null, List.of(), null, null, List.of());
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlAgg || e instanceof SqlExpr.WindowCall
                        || e instanceof SqlExpr.JsonArrayAgg || e instanceof SqlExpr.ScalarSubquery
                        || e instanceof SqlExpr.Exists
                        || SqlExpr.expandsRows(e)) {
                    // a value that changes the rows (an aggregate, a window, a subquery, an UNNEST's row per element):
                    // the wrap stays a layer, its values read from outside it
                    aggregate[0] = true;
                }
                return e;
            }
        }.rewriteRoot(projectionsOnly);
        return !aggregate[0];
    }

    private static SqlQuery canonRows(Side s) {
        return canonRows(s, false);
    }

    /** {@code withValues}: the PAIR may be judged with the 2-ULP leniency,
     * so this side carries the value column — its Float value, or a typed
     * NULL when it has none (the column belongs to the pair, not the side). */
    private static SqlQuery canonRows(Side s, boolean withValues) {
        SqlSelect ws = (SqlSelect) s.wrapped();
        if (!plainProjection(ws)) {
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
        SqlExpr rn = rowNumber(readOuts);
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

    private static SqlQuery gridRowCanons(GridSide grid, String named) {
        if (grid.emptyIsNull().stream().noneMatch(b -> b)) {
            return rowsOf(new SqlExpr.Cast(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), CanonicalRenderSql.ROW_CANON),
                    SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), new SqlExpr.NullLit(), over(grid, named), null);
        }
        SqlExpr row = null;
        for (int i = 0; i < grid.width(); i++) {
            SqlExpr cell = gridCell(grid, i);
            row = row == null ? cell : SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, row,
                            new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP)), cell);
        }
        return rowsOf(java.util.Objects.requireNonNull(row), new SqlExpr.NullLit(), over(grid, named), null);
    }

    /** One cell canon of a grid, the empty String canon ({@code ''}) read as
     * the sentinel where the column's grammar conflates it with NULL. */
    private static SqlExpr gridCell(GridSide grid, int i) {
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

    /** A grid side read through its NAMED CTE, aliased {@code w} as every
     * cell and row reference spells it. A grid's rows and each of its
     * columns' cell branches read the one wrapped plan; inlined, the plan —
     * every formatted cell and the row canon — was spelled once per reader:
     * width + 1 copies per side (columnValueDifferenceTest, 8 columns: a
     * 1.2 MB statement; H2's parser, which copies the token list per
     * subquery, needed over 4 GB to read it — 2026-09-23). */
    private static SqlSource over(GridSide grid, String named) {
        return new SqlSource.Table(named, "w", grid.wrapped().outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlSource source,
            @com.legend.base.Nullable SqlExpr where) {
        SqlExpr rn = rowNumber(source.outputs());
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

    /** The rows of a source read as {@code w}, numbered: a grid side's by its ordinal (SideOrdinal: the order its plan
     *  defines, never its rows' arrival through the verdict's selects, which a database need not keep: MariaDB and
     *  Trino did not, 2026-10-20); any other source's by arrival. */
    private static SqlExpr rowNumber(List<OutputCol> outs) {
        List<SqlSelect.SortKey> by = outs.stream().anyMatch(o -> o.name().equals(SideOrdinal.ORD))
                ? List.of(new SqlSelect.SortKey(SqlExpr.Column.of("w", outs, SideOrdinal.ORD), true, null, null))
                : List.of();
        return new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), by, null);
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
    private static SqlSource unpivot(GridSide grid, String named) {
        List<OutputCol> outs = new ArrayList<>(grid.wrapped().outputs());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : grid.wrapped().outputs()) {
            ps.add(new SqlSelect.Projection(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), o.name()), o.name(), o));
        }
        OutputCol r = new OutputCol(ROW, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(rowNumber(grid.wrapped().outputs()), ROW, r));
        outs.add(r);
        SqlSource rows = new SqlSource.Subselect(new SqlSelect(ps, false, over(grid, named), null,
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
