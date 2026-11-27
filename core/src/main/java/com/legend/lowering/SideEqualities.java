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
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;

/**
 * THE PRODUCT'S EQUALITY OVER TWO SIDES COMPUTED ONCE (the edition program's step 6, 2026-11-27: the one equality).
 * Where {@link Equalities} compares two values inside an expression, this compares two whole SIDES, each a plan of its
 * own bound once as the rows of a statement: equal means the same type and the same value, a type known only when the
 * query runs read from the side's rows (the kind beside the value); a Decimal by its number, a date-time by its
 * instant; lists in order or as a multiset; instances by their run-time class and then their keys or their identity;
 * tables by their columns and typed cells. Each member answers a {@link Compared}: the statement's CTEs, the source
 * its one row reads, whether the two sides are the same, and each side's values as a record may list them.
 *
 * <p>It was the database judge's own comparison ({@code VerdictSql}'s typed members, item 3 of the compatibility
 * program), moved here as it stood: a test's assert is judged by the product's equality, and the judge keeps only the
 * failure's record. A side's rows are a statement's, so every database compares them (no list is compared where a
 * database has no list equality).
 */
public final class SideEqualities {

    private SideEqualities() {
    }

    static final String RN = "__rn";
    static final String O = "__o";
    static final String LISTED = "__l";
    static final String GRID_E = "__we";
    static final String GRID_A = "__wa";
    static final String K = "__k";

    /** Two sides compared: the CTEs that hold each side's rows, the source the comparison's one row reads,
     *  {@code same} whether the two sides are the same, and {@code held} each side's values as a record may list them
     *  (the expected's, then the actual's; then any other the member names). */
    public record Compared(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr same, List<Held> held) {
    }

    /** A side's values as a record lists them: the expression that reads them, its SQL type, the Pure kind they are
     *  read as, and whether it is a list or one value. */
    public record Held(SqlExpr value, SqlType type, Type kind, boolean many) {
    }

    /** A listed side's list ({@link #listed}) as its held values. */
    private static Held held(String alias, SqlQuery listed, Type kind) {
        return new Held(SqlExpr.Column.of(alias, listed.outputs(), LISTED), listed.outputs().get(0).type(), kind, true);
    }

    /**
     * {@code assertEquals}'s and {@code assertSameElements}' TYPED member (item 3's S4, 2026-11-21, the user's Q-A: the
     * database decides over typed values): each side's values as its plan types them ({@link #valueRows}), folded to ONE
     * list in the side's own order; the verdict their {@code IS NOT DISTINCT FROM} -- two sorted lists where the order is
     * incidental ({@code multiset}) -- under the assert's polarity; each side's list written only where the verdict
     * fails, for the failure's record, which the run builds with the host judge's builders. The expected side is read
     * before the actual, as the host's side batch reads them. Null where a side's value has no type the row can carry.
     */
    public static @com.legend.base.Nullable Compared values(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean emptyOnly) {
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
                        new SqlExpr.ScalarSubquery(cteRows("__e", eRows)), new SqlExpr.ScalarSubquery(cteRows("__a", aRows)));
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
        return new Compared(ctes, from, holds, List.of(
                held(eAt, eHeld, eKind), held(aAt, aHeld, aKind)));
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over two sides of lists or maps (a nested
     * {@code List<T>}, a {@code Map<K, V>}; item 3's S4, 2026-11-22): each value compared as its database holds it -- a
     * list as itself, in order; a map as its entries sorted by key ({@code [{k, v}, ...]}: a map has no order, the host
     * compares its keys as a set) -- by the one list node, typed; where the database carries the collection as JSON the
     * node is refused by name ({@code SameValuesForm}: typed values only). Each side's values ride the failure as
     * carried. Null where a side carries no list or map, the two carry different types, or an element is no scalar.
     */
    public static @com.legend.base.Nullable Compared collections(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset) {
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
                new SqlExpr.ScalarSubquery(cteRows("__e", eRows)), new SqlExpr.ScalarSubquery(cteRows("__a", aRows)));
        return new Compared(List.of(new SqlWith.Cte("__xe", eRaw), new SqlWith.Cte("__xa", aRaw),
                        new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a",
                        null)), new SqlSource.Subselect(eHeld, "xe", null)), new SqlSource.Subselect(aHeld, "xa",
                        null)),
                same, List.of(
                        held("xe", eHeld, eKind), held("xa", aHeld, aKind)));
    }

    /** Whether {@code t} is a scalar value's type (a collection's element a typed member compares as it is). */
    static boolean scalar(SqlType t) {
        return t instanceof SqlType.Scalar sc && sc != SqlType.Scalar.JSON || t instanceof SqlType.Decimal;
    }

    /** A side's maps ({@code rows}, read as {@code cte}) as their entries sorted by key, each a {@code {k, v}} struct:
     *  the value a map is compared as (the order its entries were written in no part of it). */
    static SqlQuery entryRows(String cte, SqlQuery rows, SqlType.Map t) {
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
    static @com.legend.base.Nullable SqlQuery readOnce(String cte,
            @com.legend.base.Nullable SqlQuery raw, Type kind) {
        if (raw == null || instants(raw, kind) == raw) {
            return raw == null ? null : instants(raw, kind);
        }
        return instants(cteRows(cte, raw), kind);
    }

    /** The rows of {@code cte}, a side's typed rows ({@code __o}, {@code __rn}), read back. */
    static SqlQuery cteRows(String cte, SqlQuery rows) {
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
    public static @com.legend.base.Nullable Compared tagged(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset) {
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
        SqlQuery eCmp = listed("__ke", structRows(cteRows("__ce", eCells)));
        SqlQuery aCmp = listed("__ka", structRows(cteRows("__ca", aCells)));
        SqlQuery eList = listed("__he", structRows(cteRows("__ge", eHeld)));
        SqlQuery aList = listed("__ha", structRows(cteRows("__ga", aHeld)));
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(cteRows("__ce", eCells)), new SqlExpr.ScalarSubquery(cteRows("__ca", aCells)));
        return new Compared(List.of(new SqlWith.Cte("__ce", eCells), new SqlWith.Cte("__ca", aCells),
                        new SqlWith.Cte("__ke", structRows(cteRows("__ce", eCells))),
                        new SqlWith.Cte("__ka", structRows(cteRows("__ca", aCells))),
                        new SqlWith.Cte("__ge", eHeld), new SqlWith.Cte("__ga", aHeld),
                        new SqlWith.Cte("__he", structRows(cteRows("__ge", eHeld))),
                        new SqlWith.Cte("__ha", structRows(cteRows("__ga", aHeld)))),
                cross(cross(cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                same, List.of(
                        held("e", eList, eKind), held("a", aList, aKind)));
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
    public static @com.legend.base.Nullable Compared instances(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys, boolean multiset) {
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
        SqlQuery eCmp = listed("__ke", structRows(cteRows("__ce", ce)));
        SqlQuery aCmp = listed("__ka", structRows(cteRows("__ca", ca)));
        // the record's instances as the host's reads them: a keyed class restricted to its classifier and its keys
        // (HostJudgments.restrictOne), a keyless one whole
        SqlQuery eHeld = keys == null ? eRows : restrictedRows("__ie", eRows, es, keys);
        SqlQuery aHeld = keys == null ? aRows : restrictedRows("__ia", aRows, as, keys);
        SqlQuery eList = listed("__re", eHeld);
        SqlQuery aList = listed("__ra", aHeld);
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(cteRows("__ce", ce)), new SqlExpr.ScalarSubquery(cteRows("__ca", ca)));
        return new Compared(List.of(new SqlWith.Cte("__ie", eRows), new SqlWith.Cte("__ia", aRows),
                        new SqlWith.Cte("__re", eHeld), new SqlWith.Cte("__ra", aHeld),
                        new SqlWith.Cte("__ce", ce), new SqlWith.Cte("__ca", ca),
                        new SqlWith.Cte("__ke", structRows(cteRows("__ce", ce))),
                        new SqlWith.Cte("__ka", structRows(cteRows("__ca", ca)))),
                cross(cross(cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                same, List.of(
                        held("e", eList, eKind), held("a", aList, aKind)));
    }

    /** Two layouts' leaves as the host restricts them, path by path of one kind ({@link #leaves}); null where a leaf
     *  has no typed reading or the two differ. */
    static @com.legend.base.Nullable List<List<Leaf>> pairedLeaves(SqlType.Struct es, SqlType.Struct as,
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
    public static @com.legend.base.Nullable Compared containsInstances(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
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
        return new Compared(List.of(new SqlWith.Cte("__xc", cRows), new SqlWith.Cte("__xv", vRows),
                        new SqlWith.Cte("__c", cl), new SqlWith.Cte("__v", vl)),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                some, List.of(
                        held("c", cList, collKind), held("v", vList, valKind)));
    }

    /** A side's keyed instances ({@code rows}, read as {@code cte}) each restricted to its classifier and its keys, a
     *  nested keyed instance restricted in its place -- the value the host's record writes
     *  ({@code HostJudgments.restrictOne}). */
    static SqlQuery restrictedRows(String cte, SqlQuery rows, SqlType.Struct layout,
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
    static SqlExpr restricted(SqlExpr v, SqlType.Struct st, com.legend.compiler.element.EqualityKeys keys) {
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
    record Leaf(List<String> path, SqlType type) {
    }

    /** The leaves of a struct of layout {@code st} as the host restricts it, into {@code out} (path-prefixed by
     *  {@code at}); false where one has no typed reading. */
    static boolean leaves(SqlType.Struct st, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys
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
    static @com.legend.base.Nullable String leafClass(SqlType t) {
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

    static @com.legend.base.Nullable SqlType fieldType(SqlType.Struct st, String name) {
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

    static List<String> append(List<String> at, String name) {
        List<String> out = new ArrayList<>(at);
        out.add(name);
        return out;
    }

    /** A side's instances ({@code rows}, read as {@code cte}) as rows of their typed leaves ({@code c0, c1, ...,
     *  __rn}): each leaf the field read down its path, cast to its layout's type (a dialect carrying a struct as JSON
     *  reads the field by it), a date-time as the judge's instant, an identity where the product tracks it -- read
     *  plain where {@code tracked} (the side's plan proves it, {@link #tracked}), else behind the guard that raises
     *  where it does not ({@code CanonicalRenderSql.trackedId}). */
    static SqlQuery leafRows(String cte, SqlQuery rows, List<Leaf> leaves, boolean tracked) {
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
    static boolean kindsOf(SqlQuery wrapped, Type kind, java.util.Map<String, SqlType> into, boolean byType) {
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
    static final java.util.Map<String, SqlType> DATE_KINDS = java.util.Map.of("Year", SqlType.Scalar.BIGINT,
            "YearMonth", SqlType.Scalar.BIGINT, "StrictDate", SqlType.Scalar.DATE, "DateTime", SqlType.Scalar.TIMESTAMP);

    /** The kinds a value carried as JSON may be, each with the SQL type its value is compared in. */
    static final java.util.Map<String, SqlType> JSON_KINDS = java.util.Map.of("String", SqlType.Scalar.VARCHAR,
            "Integer", SqlType.Scalar.BIGINT, "Float", SqlType.Scalar.DOUBLE, "Boolean", SqlType.Scalar.BOOLEAN);

    /** A JSON value's kind's name ({@link #JSON_KINDS}) where {@code jsonType} (its JSON_TYPE) answers it. */
    static SqlExpr jsonKindIs(String kind, SqlExpr jsonType) {
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
    static SqlExpr jsonValue(String kind, SqlExpr v) {
        return kind.equals("String")
                ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.VARIANT_GET, v, new SqlExpr.StringLit("$")),
                        SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                : new SqlExpr.Cast(v, java.util.Objects.requireNonNull(JSON_KINDS.get(kind)), SqlExpr.Reason.JUDGE);
    }

    /** Whether a side is a {@code Date} carried as its written text ({@code JudgmentFacts}' the same rule). */
    static boolean writtenDate(Type kind, SqlQuery wrapped) {
        return kind == Type.Primitive.DATE && !wrapped.outputs().isEmpty()
                && (wrapped.outputs().get(0).type() == SqlType.Scalar.TEMPORAL_TEXT
                        || wrapped.outputs().get(0).type() == SqlType.Scalar.VARCHAR);
    }

    /** The kind a Date's written text names, read from its STRUCTURE -- a time after the day (a {@code T} or a space)
     *  a date-time; else by its dashes after the year's sign: none a year, one a year-month, two a day -- as the kind
     *  key's index. Never its length (the precision audit's H1, 2026-11-22: a year of three or five digits, or a
     *  negative one, took another kind's length). */
    static SqlExpr dateKindOfText(SqlExpr text, java.util.function.Function<String, Integer> index) {
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (String k : List.of("DateTime", "Year", "YearMonth")) {
            whens.add(new SqlExpr.Case.When(writtenKindIs(k, text), new SqlExpr.IntLit(index.apply(k))));
        }
        return new SqlExpr.Case(whens, new SqlExpr.IntLit(index.apply("StrictDate")), SqlExpr.Reason.JUDGE);
    }

    /** Whether a Date's written {@code text} is of {@code kind} ({@link #dateKindOfText}). */
    static SqlExpr writtenKindIs(String kind, SqlExpr text) {
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
    static SqlExpr unsignedYear(SqlExpr text) {
        SqlExpr first = SqlExpr.Call.of(SqlFn.LEFT, text, new SqlExpr.IntLit(1));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("-")),
                SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("+"))),
                SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(2)))), text, SqlExpr.Reason.JUDGE);
    }

    /** A Date's written {@code text} of {@code kind} as the value it is compared as ({@link #DATE_KINDS}): a year its
     *  number; a year-month its year times twelve plus its month; a day the database's DATE; a date-time the judge's
     *  instant (an hour's text read at its minute). */
    static SqlExpr writtenDateValue(String kind, SqlExpr text) {
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
    static SqlType heldType(SqlQuery wrapped, Type t) {
        return t == Type.Primitive.DATE_TIME ? SqlType.Scalar.TIMESTAMP
                : t == Type.Primitive.STRICT_DATE ? SqlType.Scalar.DATE : wrapped.outputs().get(0).type();
    }

    /** Whether a slot's kind is a DateTime's (of any written precision), compared at the judge's instant. */
    static boolean instant(String slotKind) {
        return MixedEncoding.writtenDigits(slotKind) != null || slotKind.equals("DateTime");
    }

    /** A written Date's kind's column key: its compared kind's ({@link #DATE_KINDS}), and where the column is the
     *  record's, the text it is written in. */
    static String writtenKey(String kind, SqlType carried, boolean byType) {
        return kindKey(kind, byType ? carried : java.util.Objects.requireNonNull(DATE_KINDS.get(kind)), byType);
    }

    /** A kind's column key: its name (a DateTime of any written precision one kind, register S399), and where the
     *  column is the record's ({@code byType}) its SQL type too. */
    static String kindKey(String slotKind, SqlType type, boolean byType) {
        String kind = MixedEncoding.writtenDigits(slotKind) != null ? "DateTime" : slotKind;
        return byType ? kind + "|" + type : kind;
    }

    /** A side's values as kinded rows ({@link #typedTagged}): {@code k}, the index of its kind's key in
     *  {@code columns}, then {@code v0..vn} one per key, then {@code __rn}, numbered in the side's own order. A tagged
     *  side's tag names its slot; a typed side's every value is its one kind's. */
    static @com.legend.base.Nullable SqlQuery kindedRows(SqlQuery wrapped, Type kind,
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
    static @com.legend.base.Nullable SqlQuery instants(@com.legend.base.Nullable SqlQuery rows, Type kind) {
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
        // types as a date-time, as it holds it, every digit; SQLite's, a text, in one form at nine digits; a text with
        // more digits than the database's date-time reads refused by name -- S399, S181, the user's rule of
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
    static SqlExpr noRows(String cte, SqlQuery rows) {
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        return SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.IntLit(1), "__one", one)), false, new SqlSource.Table(cte, cte, rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null,
                null, List.of(one))));
    }

    /** Whether two sides' typed rows carry their values alike: a Decimal the root writes as its text (a store-read
     *  Decimal's {@code DECIMAL_TEXT}, the value lane's egress) against a decimal value does not compare -- no typed
     *  member for the pair (the caller refuses it by name). */
    static boolean oneCarrier(SqlQuery e, SqlQuery a) {
        return (e.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT)
                == (a.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT);
    }

    /**
     * {@code assertContains} / {@code assertNotContains}' TYPED member over tagged values (item 3's S4, the audit of
     * 946d16f2: a mixed list, an Any): each value its kind and its value ({@link #kindedRows}, as {@link #typedTagged}
     * compares them); the verdict whether some element has the value's kind and a value {@code IS NOT DISTINCT FROM} it,
     * under {@code want}; both sides' tagged values ride the failure ({@code Family.CONTAINS_TAGGED}). Null where a
     * side has no kinded reading.
     */
    public static @com.legend.base.Nullable Compared containsTagged(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind) {
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
        SqlQuery cList = listed("__hc", structRows(cteRows("__gc", ch)));
        SqlQuery vList = listed("__hv", structRows(cteRows("__gv", vh)));
        return new Compared(List.of(new SqlWith.Cte("__c", cc), new SqlWith.Cte("__v", vc), new SqlWith.Cte("__gc", ch),
                        new SqlWith.Cte("__gv", vh), new SqlWith.Cte("__hc", structRows(cteRows("__gc", ch))),
                        new SqlWith.Cte("__hv", structRows(cteRows("__gv", vh)))),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                some, List.of(
                        held("c", cList, collKind), held("v", vList, valKind)));
    }

    /** {@code assertContains} / {@code assertNotContains}' TYPED member (item 3's S4): the collection's values and the
     *  value's, typed; the verdict whether some element {@code IS NOT DISTINCT FROM} the value, under {@code want}; both
     *  lists ride the failure, the collection's first. */
    public static @com.legend.base.Nullable Compared contains(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind) {
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
        return new Compared(List.of(new SqlWith.Cte("__xc", cRaw), new SqlWith.Cte("__xv", vRaw),
                        new SqlWith.Cte("__c", cRows), new SqlWith.Cte("__v", vRows)),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                some, List.of(
                        held("c", cList, collKind), held("v", vList, valKind)));
    }

    /** The {@code $need->forAll(n | $have->contains($n))} subset fold of a condition, TYPED (item 3's S4): no needed
     *  value without a have that is not distinct from it, under {@code wantTrue}; its record the condition's, fixed at
     *  planning; a date-time by its instant ({@code kind}, the sides' one typed class). Null where a side has no typed
     *  value column. */
    public static @com.legend.base.Nullable Compared subset(SqlQuery needWrapped, SqlQuery haveWrapped,
            Type kind) {
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
        return new Compared(List.of(new SqlWith.Cte("__n", nRows), new SqlWith.Cte("__h", hRows)),
                new SqlSource.Subselect(missing, "m", null), none,
                List.of());
    }

    /**
     * A quantified equality judged element by element, TYPED (item 3's S4): {@code pairs}' rows, each element's two
     * operands one {@code {first, second}} struct in the source's order; each element's verdict its two typed operands
     * not distinct ({@code neverEqual}: kinds lite never calls equal, S127); the verdict that none fails, and where one
     * does, the FIRST failing element's zero-based index and its two operands ride the failure. Null where the pairs'
     * struct states no field types.
     */
    public static @com.legend.base.Nullable Compared perElement(SqlQuery pairs, Type expectedKind,
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
        return new Compared(List.of(new SqlWith.Cte("__pe", rows)), new SqlSource.Subselect(b, "b", null),
                SqlExpr.Call.of(SqlFn.IS_NULL, firstFailing), List.of(
                        new Held(SqlExpr.Column.of("b", b.outputs(), "__fe"), ft, expectedKind, false),
                        new Held(SqlExpr.Column.of("b", b.outputs(), "__fa"), sdt, actualKind, false),
                        new Held(SqlExpr.Call.of(SqlFn.MINUS, firstFailing, new SqlExpr.IntLit(1)),
                                SqlType.Scalar.BIGINT, Type.Primitive.INTEGER, false)));
    }

    static SqlSource cross(SqlSource left, SqlSource right) {
        return new SqlSource.Join(left, right, SqlSource.Join.Kind.CROSS, null);
    }

    /** A side's typed rows ({@code cte}: {@code __o}, {@code __rn}) folded to one row: {@code __l}, its values in its own
     *  order -- the LIST reducer, which each database carries as it carries a list (H2: a JSON array) and exec decodes
     *  as any list result. The list rides the failure's record; no verdict reads it (emptiness is the rows', noRows). */
    static SqlQuery listed(String cte, SqlQuery rows) {
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
    static @com.legend.base.Nullable SqlQuery valueRows(SqlQuery wrapped) {
        return valueRows(wrapped, 1);
    }

    /** {@link #valueRows} carrying the side's first {@code width} columns: {@code __o}, then {@code __s1, __s2, ...}
     *  (a tagged value's slots beside its tag), then {@code __rn}; the rows those whose first column is not NULL. */
    static @com.legend.base.Nullable SqlQuery valueRows(SqlQuery wrapped, int width) {
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

    /** A grid's rows as row structs ({@code __o}, its cells as fields {@code c0, c1, ...}, each typed as its column
     *  declares ({@link #typedCell}); a struct is never NULL, whatever its cells) numbered as its cells are
     *  ({@code __rn}), read through its CTE. */
    static SqlQuery rowStructs(GridSide grid, String named, List<Type> kinds) {
        return structRows(typedCells(grid, named, kinds, null));
    }

    /** {@code cells}' rows ({@code c0, c1, ..., __rn}) as row structs: {@code __o} and {@code __rn}. */
    static SqlQuery structRows(SqlQuery cells) {
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
    static SqlQuery typedCells(GridSide grid, String named, List<Type> kinds,
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
    static SqlExpr typedCell(GridSide grid, SqlSelect.Projection p) {
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
    public static @com.legend.base.Nullable Compared grids(GridSide e, List<Type> eKinds, GridSide a,
            List<Type> aKinds, boolean multiset, boolean schemasDiffer, Type eRows, Type aRows) {
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
                new SqlWith.Cte("__ke", structRows(cteRows("__ce", ce))),
                new SqlWith.Cte("__ka", structRows(cteRows("__ca", ca)))));
        SqlQuery eCmp = listed("__ke", structRows(cteRows("__ce", ce)));
        SqlQuery aCmp = listed("__ka", structRows(cteRows("__ca", ca)));
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
                new SqlExpr.ScalarSubquery(cteRows("__ce", ce)), new SqlExpr.ScalarSubquery(cteRows("__ca", ca)));
        SqlSource from = cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null));
        if (anyTagged) {
            from = cross(cross(from, new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a",
                    null));
        }
        return new Compared(ctes, from, same, List.of(
                held(anyTagged ? "e" : "ke", eList, eRows),
                held(anyTagged ? "a" : "ka", aList, aRows)));
    }

    /** Whether a grid's plan names and types each of its data columns, the cells a typed member reads. */
    static boolean carriesCells(GridSide grid) {
        return grid.wrapped() instanceof SqlSelect ws && ws.projections().size() >= grid.width()
                && ws.projections().subList(0, grid.width()).stream().allMatch(p -> p.alias() != null
                        && p.out() != null);
    }

    /** Whether each of a grid's columns has a typed class the cells carry: a date column's cells a DATE or TIMESTAMP,
     *  never a text, and no column empty-is-NULL (toCSV's equivalence: no typed form, refused by name). */
    static boolean typedGrid(GridSide grid, List<Type> kinds) {
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

    /** A slot's NULL where a member writes none: typed where the renderer leaves a typed slot's NULL bare -- a Decimal,
     *  a TIME -- since Postgres types a UNION's column pairwise from its left, and two bare NULL branches first make it
     *  text, which a numeric branch then fails to match (the S4 tall-row audit's MEDIUM, 2026-11-22). The renderer
     *  types every other scalar slot's NULL itself; a list's, a struct's and a label carrier's stay bare, as it writes
     *  them. */
    static SqlExpr nullOf(SqlType slot) {
        return slot instanceof SqlType.Decimal || slot == SqlType.Scalar.TIME
                ? new SqlExpr.Cast(new SqlExpr.NullLit(), slot, SqlExpr.Reason.JUDGE) : new SqlExpr.NullLit();
    }

    /** A kind's name as a typed row records it: a primitive's short name (a Decimal of a declared precision a Decimal),
     *  any other type its own name. */
    public static String kindText(Type t) {
        return t instanceof Type.PrecisionDecimal ? "Decimal" : t.typeName();
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
    static boolean plainProjection(SqlSelect ws) {
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

    /** A grid side read through its NAMED CTE, aliased {@code w} as every
     * cell and row reference spells it. A grid's rows and each of its
     * columns' cell branches read the one wrapped plan; inlined, the plan —
     * every formatted cell and the row canon — was spelled once per reader:
     * width + 1 copies per side (columnValueDifferenceTest, 8 columns: a
     * 1.2 MB statement; H2's parser, which copies the token list per
     * subquery, needed over 4 GB to read it — 2026-09-23). */
    static SqlSource over(GridSide grid, String named) {
        return new SqlSource.Table(named, "w", grid.wrapped().outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    /** The rows of a source read as {@code w}, numbered: a grid side's by its ordinal (SideOrdinal: the order its plan
     *  defines, never its rows' arrival through the verdict's selects, which a database need not keep: MariaDB and
     *  Trino did not, 2026-10-20); any other source's by arrival. */
    static SqlExpr rowNumber(List<OutputCol> outs) {
        List<SqlSelect.SortKey> by = outs.stream().anyMatch(o -> o.name().equals(SideOrdinal.ORD))
                ? List.of(new SqlSelect.SortKey(SqlExpr.Column.of("w", outs, SideOrdinal.ORD), true, null, null))
                : List.of();
        return new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), by, null);
    }
}
