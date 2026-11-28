// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
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
    public record Compared(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr same, List<Held> held,
            @com.legend.base.Nullable Compared alone) {
        /** A comparison whose record's values are the values compared: nothing to leave out ({@link #answerOnly}). */
        public Compared(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr same, List<Held> held) {
            this(ctes, from, same, held, null);
        }

        /** The comparison as asked: whether the two sides are the same ({@code want}), or are not. */
        public SqlExpr holds(boolean want) {
            return want ? same : SqlExpr.Call.of(SqlFn.NOT, same);
        }

        /** The comparison with none of what only a failure's record reads ({@code alone}): for a reader that asks
         *  whether the two sides are the same and lists nothing -- a query's answer ({@code planner.RootComparison}).
         *  A member that reads its record's values apart from the values it compares (a mixed list's by kind and SQL
         *  type, an instance's restricted to its keys, a list's as it is held) gives the form without them; measured
         *  2026-11-27, they were a third to a half of such a query's statement. */
        public Compared answerOnly() {
            return alone == null ? this : alone;
        }
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
        // an Integer side against a Decimal side: two EXACT numbers, equal by their value (the user's ruling of
        // 2026-11-27, "Yes I like the rule": exact with exact by value, a Float only with a Float) -- the Integer
        // side's values read as Decimals, so the two lists hold one type and the database compares the numbers
        boolean exactPair = !eKind.equals(aKind) && exactNumber(eKind)
                && exactNumber(aKind);
        boolean bothEmptyOnly = emptyOnly || !eKind.equals(aKind) && !exactPair;
        if (!bothEmptyOnly && eRows != null && aRows != null && exactNumber(eKind) && exactNumber(aKind)) {
            // (two Decimal sides too: one the plan types and one a store read carries as its text are compared as
            // that text -- until the edition program's step 6 such a pair had no typed comparison)
            SqlQuery e = besideDecimals("__xe", eRows, aRows);
            aRows = besideDecimals("__xa", aRows, eRows);
            eRows = e;
        }
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
        // two kinds lite never calls equal (register S127: an exact number and a Float, the type part of the value),
        // or an empty side's (Nil, {@code emptyOnly}): the sides are the same only where both are empty, an empty
        // side's list NULL
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
                held(eAt, eHeld, eKind), held(aAt, aHeld, aKind)),
                bothEmptyOnly ? sameOnlyWhereBothEmpty(ctes, eRows, aRows) : null);
    }

    /** A side bound once: the expression that holds its value where the comparison is written (its one list, or
     *  its one value), its typed rows, for a database that compares rows, and whether every instance it holds has
     *  an identity the product tracks, proven from the side's own plan ({@link #tracked}; the bound rows read a
     *  named table, which proves nothing). */
    public record Bound(SqlExpr value, SqlQuery rows, boolean tracked) {
    }

    private static final String SINGLE = "__v";

    /** A side's typed rows ({@code cte}) as its one value: {@code __v}, the least of its values (a one-valued side's
     *  value; NULL where it has none). */
    static SqlQuery single(String cte, SqlQuery rows) {
        OutputCol o = new OutputCol(SINGLE, rows.outputs().get(0).type(), true);
        if (o.type() instanceof SqlType.Struct) {
            // an instance has no least, and not every database folds structs: the side's one row, read by a
            // sub-query (step 6B part 2, objects; H2 2.5 has no ANY_VALUE and its MIN wants a scalar)
            OutputCol held = new OutputCol(O, o.type(), true);
            SqlSelect one = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of(cte, rows.outputs(), O),
                    O, held)), false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                    com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), 1L, null, List.of(held));
            return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.ScalarSubquery(one), SINGLE, o)), false,
                    new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(o));
        }
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN,
                List.of(SqlExpr.Column.of(cte, rows.outputs(), O)), false, List.of()), SINGLE, o)), false,
                new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of(o));
    }

    /** Whether a value of {@code kind} carried as {@code held} is carried as that kind's own SQL type (a whole
     *  number, a Decimal, a Float, a String, a Boolean, a day as the database's DATE), or, a date-time, as the
     *  database's TIMESTAMP or its written text: the carriers the product's comparison of two values reads by
     *  their Pure type alone. */
    private static boolean heldAsItsKind(Type kind, SqlType held) {
        if (kind == Type.Primitive.DATE_TIME) {
            return held == SqlType.Scalar.TIMESTAMP || held == SqlType.Scalar.TEMPORAL_TEXT;
        }
        return (kind == Type.Primitive.INTEGER || kind == Type.Primitive.DECIMAL || kind == Type.Primitive.FLOAT
                || kind == Type.Primitive.STRING || kind == Type.Primitive.BOOLEAN
                || kind == Type.Primitive.STRICT_DATE) && kind.equals(Type.kindOfSqlType(held));
    }

    /** {@code list}, a side's folded list ({@link #listed}: no list where the side has no row), as a list always:
     *  the empty list of its type where it is none. */
    private static SqlExpr orTheEmptyList(SqlExpr list, SqlQuery listed) {
        return SqlExpr.Call.of(SqlFn.COALESCE, list, new SqlExpr.Cast(CanonicalRenderSql.emptyArray(),
                listed.outputs().get(0).type(), SqlExpr.Reason.OPERATOR));
    }

    /** {@link #listed} with the side's one value beside its list ({@code __v}, as {@link #single} reads it): one
     *  read of the rows for both. */
    private static SqlQuery listedWithItsOne(String cte, SqlQuery rows) {
        SqlSelect list = (SqlSelect) listed(cte, rows);
        SqlSelect one = (SqlSelect) single(cte, rows);
        List<SqlSelect.Projection> both = new ArrayList<>(list.projections());
        both.addAll(one.projections());
        List<OutputCol> outs = new ArrayList<>(list.outputs());
        outs.addAll(one.outputs());
        return new SqlSelect(both, false, list.from(), null, List.of(), null, null, List.of(), null, null, outs);
    }

    /**
     * AN ASSERT OF TWO SIDES AS THE PRODUCT'S OWN COMPARISON OF THEM, EACH SIDE BOUND ONCE (step 6B, slice 6): each
     * side's values as its plan types them ({@link #valueRows}), computed once as a common table; its one list read
     * from it ({@link #listed}), and, for a side that holds at most one value, that value ({@link #single});
     * {@code same}, the PRODUCT'S lowering of the comparison over the two bound sides, is the verdict -- no reading
     * of the two is decided here (until this slice {@link #values} decided for itself how a date, an exact number
     * beside a Decimal and two kinds never equal are read: a second set of the decisions {@code Equalities} makes).
     * Each side's list rides the failure's record, as {@link #values} records it. Null where a side's value has no
     * type the row can carry.
     *
     * @param eOne whether the expected side holds at most one value: bound as that value, as the product lowers
     *             such a side
     * @param aOne the same of the actual side
     */
    public static @com.legend.base.Nullable Compared boundOnce(SqlQuery eWrapped, Type eKind, boolean eOne,
            SqlQuery aWrapped, Type aKind, boolean aOne, java.util.function.BiFunction<Bound, Bound, SqlExpr> same) {
        return boundOnce(eWrapped, eKind, eOne, aWrapped, aKind, aOne, false, same);
    }

    /** {@link #boundOnce(SqlQuery, Type, boolean, SqlQuery, Type, boolean, java.util.function.BiFunction)};
     *  {@code typedAtRunTime}: a side is declared {@code Number} (a type its value fills in when the query runs), so
     *  the two may be held in two kinds -- the product's comparison reads the kind each holds off the query's text
     *  and answers "never the same, both evaluated" for two families (step 6B part 2). */
    public static @com.legend.base.Nullable Compared boundOnce(SqlQuery eWrapped, Type eKind, boolean eOne,
            SqlQuery aWrapped, Type aKind, boolean aOne, boolean typedAtRunTime,
            java.util.function.BiFunction<Bound, Bound, SqlExpr> same) {
        SqlQuery eRows = valueRows(eWrapped);
        SqlQuery aRows = valueRows(aWrapped);
        // (each side held as its own kind's SQL type, the two of ONE kind: a Decimal a store read carries as its
        // text, a JSON-carried value, a day carried as a text, an Integer beside a Decimal are read by the member
        // that reads them, values -- the product's comparison of two expressions reads their Pure types, and a
        // carrier that is another type would meet it as that type; the review of slice 6, findings 1, 3, 5 and 7)
        // (a side of kind Nil holds no value whatever it is carried as: the product reads only whether the OTHER
        // side holds anything, so the two kinds need not be one and the Nil side's carrier is not read)
        boolean nil = PlatformTypes.isNil(eKind) || PlatformTypes.isNil(aKind);
        if (eRows == null || aRows == null || !nil && !typedAtRunTime && !eKind.equals(aKind)
                || !PlatformTypes.isNil(eKind) && !heldAsItsKind(eKind, eRows.outputs().get(0).type())
                || !PlatformTypes.isNil(aKind) && !heldAsItsKind(aKind, aRows.outputs().get(0).type())) {
            return null;
        }
        return bound(eRows, eKind, eOne, false, aRows, aKind, aOne, false, nil, same);
    }

    /**
     * {@link #boundOnce} FOR TWO SIDES OF INSTANCES OF ONE CLASS (step 6B part 2, objects): each side's instances
     * bound once as the host's record reads them -- a keyed class restricted to its classifier and its keys
     * ({@link #restricted}, {@code HostJudgments.restrictOne}), a keyless one whole -- and {@code same}, the
     * product's own comparison of the two bound sides ({@code InstanceEquality.sameInstances}: each instance
     * reduced to its leaves), the verdict. The lists compared are the lists recorded. Null where a side holds no
     * struct or the two layouts' leaves do not pair ({@link #pairedLeaves}).
     *
     * @param whole the record's instances whole, never restricted to their keys (a membership's record: the host
     *              writes the collection's instances and the value as they are)
     */
    public static @com.legend.base.Nullable Compared boundInstances(SqlQuery eWrapped, Type eKind, boolean eOne,
            SqlQuery aWrapped, Type aKind, boolean aOne,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys, boolean whole,
            java.util.function.BiFunction<Bound, Bound, SqlExpr> same) {
        return boundInstances(eWrapped, eKind, eOne, aWrapped, aKind, aOne, keys, keys, true, whole, same);
    }

    /** {@link #boundInstances(SqlQuery, Type, boolean, SqlQuery, Type, boolean, com.legend.compiler.element.EqualityKeys,
     *  boolean, java.util.function.BiFunction)} for two sides that may be declared of TWO classes ({@code oneClass}
     *  false: a class and one of its own): bound where the two have one reading
     *  ({@link #oneReadingOfTwoClasses}), each side's instances by its own class's keys. */
    public static @com.legend.base.Nullable Compared boundInstances(SqlQuery eWrapped, Type eKind, boolean eOne,
            SqlQuery aWrapped, Type aKind, boolean aOne,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys aKeys, boolean oneClass, boolean whole,
            java.util.function.BiFunction<Bound, Bound, SqlExpr> same) {
        SqlQuery eRaw = valueRows(eWrapped);
        SqlQuery aRaw = valueRows(aWrapped);
        if (eRaw == null || aRaw == null || !(eRaw.outputs().get(0).type() instanceof SqlType.Struct es)
                || !(aRaw.outputs().get(0).type() instanceof SqlType.Struct as) || pairedLeaves(es, as, keys) == null
                || !oneClass && !oneReadingOfTwoClasses(es, keys, as, aKeys)) {
            return null;
        }
        boolean asHeld = whole || keys == null;
        return bound(asHeld ? eRaw : restrictedRows(new SqlSource.Subselect(eRaw, "i", null), eRaw, es, keys),
                eKind, eOne, tracked(eWrapped),
                asHeld ? aRaw : restrictedRows(new SqlSource.Subselect(aRaw, "i", null), aRaw, as,
                        java.util.Objects.requireNonNull(oneClass ? keys : aKeys)),
                aKind, aOne, tracked(aWrapped), false, same);
    }

    /** {@link #boundOnce}'s statement over two sides' typed rows. */
    private static Compared bound(SqlQuery eRows, Type eKind, boolean eOne, boolean eTracked, SqlQuery aRows,
            Type aKind, boolean aOne, boolean aTracked, boolean nil,
            java.util.function.BiFunction<Bound, Bound, SqlExpr> same) {
        // (a side of at most one value: its one value read beside its list, by the one read of its rows)
        // (a side of at most one value is read as that ONE value alone where the other side is too: its list would
        // be the list of it, and the record lists the value -- one aggregate for the side, where the member this
        // replaces wrote a list's; beside a many-valued side its list is read too, for the pairing below)
        boolean bothOne = eOne && aOne;
        SqlQuery eList = bothOne ? single("__e", eRows) : eOne ? listedWithItsOne("__e", eRows) : listed("__e", eRows);
        SqlQuery aList = bothOne ? single("__a", aRows) : aOne ? listedWithItsOne("__a", aRows) : listed("__a", aRows);
        // (the expected side is read before the actual, as values reads them)
        SqlSource from = cross(new SqlSource.Subselect(aList, "a", null), new SqlSource.Subselect(eList, "e", null));
        SqlExpr eValue = SqlExpr.Column.of("e", eList.outputs(), eOne ? SINGLE : LISTED);
        SqlExpr aValue = SqlExpr.Column.of("a", aList.outputs(), aOne ? SINGLE : LISTED);
        // (ONE value against a list: the product writes the one value as the list of it, the empty list where it
        // holds none -- and a side's list folded from no rows is no list at all, which is the empty list here: the
        // two must be one thing. Two folded lists need nothing: both none is both the same. Measured, the no-Bazel
        // loop: an empty optional value against an empty computed list failed its assert)
        // (never beside a Nil side: the product reads the other side's emptiness by its length, for which no list
        // is no element already; and the empty list of a type is no SQL H2 has -- measured 2026-11-28, a syntax
        // error there)
        if (eOne != aOne && !nil) {
            if (aOne) {
                eValue = orTheEmptyList(eValue, eList);
            } else {
                aValue = orTheEmptyList(aValue, aList);
            }
        }
        SqlExpr holds = same.apply(new Bound(eValue, cteRows("__e", eRows), eTracked),
                new Bound(aValue, cteRows("__a", aRows), aTracked));
        return new Compared(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)), from, holds,
                bothOne ? List.of(new Held(eValue, eList.outputs().get(0).type(), eKind, false),
                        new Held(aValue, aList.outputs().get(0).type(), aKind, false))
                        : List.of(held("e", eList, eKind), held("a", aList, aKind)));
    }

    /**
     * TWO SIDES NEVER EQUAL UNLESS BOTH ARE EMPTY, EVERY VALUE OF BOTH STILL READ: a query's answer for two sides of
     * two kinds (the edition program's step 6, its closing batch; the user, 2026-11-27: "Build 3"). Pure evaluates
     * both sides before it compares them, so a side whose computation fails raises, whatever the types say; the
     * assert's form tests only whether each side has a row ({@link #noRows}: an EXISTS reads no value), which
     * answered a quiet false for {@code [1, 2]->map(x | [1, 2]->at($x + 5))->cast(@Number) == [1.5]->cast(@Number)}
     * (measured 2026-11-27). Here each side's values are folded by one aggregate ({@link #everyValueRead}): an
     * aggregate reads every row; a side that holds no value folds to NULL, and two NULLs are the one case that is
     * the same ({@code Equalities.sameOnlyWhereBothEmpty}). Each side is written once, and no text is made of a
     * value (a side may hold an instance, a JSON value, a tagged one: the code audit of the batch, S4).
     */
    private static Compared sameOnlyWhereBothEmpty(List<SqlWith.Cte> ctes, SqlQuery eRows, SqlQuery aRows) {
        SqlSelect e = everyValueRead("__e", eRows, "l");
        SqlSelect a = everyValueRead("__a", aRows, "r");
        SqlExpr same = Equalities.sameOnlyWhereBothEmpty(
                Equalities.isNone(SqlExpr.Column.of("le", e.outputs(), READ)),
                Equalities.isNone(SqlExpr.Column.of("ra", a.outputs(), READ)));
        return new Compared(ctes, cross(new SqlSource.Subselect(a, "ra", null), new SqlSource.Subselect(e, "le", null)),
                same, List.of());
    }

    private static final String READ = "__read";

    /** Every value of the side whose typed rows are {@code rows} (the common table {@code cte}), READ: a mark for
     *  each row, asked of the row's value, folded to one; NULL where the side holds no value. */
    private static SqlSelect everyValueRead(String cte, SqlQuery rows, String mark) {
        OutputCol o = rows.outputs().get(0);
        OutputCol read = new OutputCol(READ, SqlType.Scalar.VARCHAR, true);
        SqlExpr text = new SqlExpr.Case(List.of(new SqlExpr.Case.When(Equalities.isNone(
                SqlExpr.Column.of(cte, O, o.type(), false, OutputCol.Origin.DERIVED)), new SqlExpr.StringLit("n"))),
                new SqlExpr.StringLit(mark), SqlExpr.Reason.OPERATOR);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(text), false,
                List.of()), READ, read)), false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of(read));
    }

    /** A statement whose one value is read from EVERY value of the side {@code wrapped} plans ({@link
     *  #sameOnlyWhereBothEmpty}'s fold; NULL where the side holds none): what a comparison of two types, never equal,
     *  evaluates of a side that is a computed list ({@code lowering.WholeComparisons#everyValueRead}). Null where
     *  the side's values have no typed column. */
    public static @com.legend.base.Nullable SqlQuery everyValueRead(SqlQuery wrapped) {
        SqlQuery rows = valueRows(wrapped);
        return rows == null ? null
                : SqlWith.prepend(List.of(new SqlWith.Cte("__e", rows)), everyValueRead("__e", rows, "v"));
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
                        held("xe", eHeld, eKind), held("xa", aHeld, aKind)),
                new Compared(List.of(new SqlWith.Cte("__xe", eRaw), new SqlWith.Cte("__xa", aRaw),
                        new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                        cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a", null)),
                        same, List.of()));
    }

    /** Whether {@code t} is a scalar value's type (a collection's element a typed member compares as it is). */
    static boolean scalar(SqlType t) {
        return t instanceof SqlType.Scalar sc && sc != SqlType.Scalar.JSON || t instanceof SqlType.Decimal;
    }

    /** What a map {@code m} is compared by: its entries sorted by key, each a {@code {k, v}} struct -- the order its
     *  entries were written in is no part of it. One reduction, for a side's rows ({@link #entryRows}) and for a map
     *  compared where it stands ({@code Equalities.sameMaps}). */
    static SqlExpr entriesOf(SqlExpr m, SqlType.Map t) {
        SqlExpr k = SqlExpr.Column.derived(null, "__k");
        SqlExpr entry = new SqlExpr.StructLit(List.of(new SqlExpr.StructLit.Field("k", k, t.key()),
                new SqlExpr.StructLit.Field("v", CanonicalRenderSql.valueAtKey(m), t.value())));
        return CanonicalRenderSql.sortedEntries(m, entry);
    }

    /** The type of one map both of two sides' values are held as, its keys and its values scalars: what the one
     *  comparison of two maps as values asks ({@code Equalities.sameMaps}); null for any other pair. */
    static SqlType.@com.legend.base.Nullable Map oneMapType(@com.legend.base.Nullable SqlType l,
            @com.legend.base.Nullable SqlType r) {
        return l instanceof SqlType.Map m && m.equals(r) && scalar(m.key()) && scalar(m.value()) ? m : null;
    }

    /** Whether the planner stands aside, for two sides each ONE map, for the comparison of the two as values
     *  ({@code Equalities.sameMaps}): each side's rows hold the one map type. */
    public static boolean mapsAsValues(TypedSpec left, SqlQuery leftPlan, TypedSpec right, SqlQuery rightPlan) {
        SqlQuery l = valueRows(leftPlan);
        SqlQuery r = valueRows(rightPlan);
        return PlatformTypes.isMapCarrier(left.info().type()) && PlatformTypes.isMapCarrier(right.info().type())
                && !left.info().multiplicity().isMany() && !right.info().multiplicity().isMany()
                && l != null && r != null && oneMapType(l.outputs().get(0).type(), r.outputs().get(0).type()) != null;
    }

    /** A side's maps ({@code rows}, read as {@code cte}) as their entries sorted by key, each a {@code {k, v}} struct:
     *  the value a map is compared as (the order its entries were written in no part of it). */
    static SqlQuery entryRows(String cte, SqlQuery rows, SqlType.Map t) {
        SqlExpr entries = entriesOf(SqlExpr.Column.of(cte, rows.outputs(), O), t);
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
     * two values the same only where their kinds and their values are (register S127: an exact number is never a
     * Float, and an Integer and a Decimal are one kind here, equal by their value; S139 a Decimal by its value; S399
     * a DateTime of any written precision one kind). Each side's rows ride the failure
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
                        held("e", eList, eKind), held("a", aList, aKind)),
                new Compared(List.of(new SqlWith.Cte("__ce", eCells), new SqlWith.Cte("__ca", aCells),
                        new SqlWith.Cte("__ke", structRows(cteRows("__ce", eCells))),
                        new SqlWith.Cte("__ka", structRows(cteRows("__ca", aCells)))),
                        cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        same, List.of()));
    }

    /**
     * Two instances of one declared class the same, INSIDE AN EXPRESSION (the edition program's step 6, the one
     * equality): each one's typed leaves as {@code InstanceEquality.sameInstances} reads a side's -- the class it actually is
     * ({@code __type}), then each key property (a nested keyed instance's leaves in its place), or its identity for a
     * class with no key -- every pair of leaves not distinct. So a Decimal key is compared by its number and a
     * date-time key by its instant, where a text of the keys kept how each was written, and two instances of two
     * classes are never the same whatever type they are declared under. Null where a leaf has no typed reading here
     * (a to-many key, a key slot of a polymorphic or Any class, a JSON field, a keyless class with no identity).
     */
    static @com.legend.base.Nullable SqlExpr sameInstance(SqlExpr e, SqlType.Struct es, SqlExpr a, SqlType.Struct as,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        return sameInstance(e, es, false, a, as, false, keys);
    }

    /** {@link #sameInstance(SqlExpr, SqlType.Struct, SqlExpr, SqlType.Struct, com.legend.compiler.element.EqualityKeys)};
     *  an identity read plain where its side's plan proves the product tracks it ({@link #tracked}). */
    static @com.legend.base.Nullable SqlExpr sameInstance(SqlExpr e, SqlType.Struct es, boolean eTracked, SqlExpr a,
            SqlType.Struct as, boolean aTracked,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        List<List<Leaf>> both = pairedLeaves(es, as, keys);
        if (both == null) {
            return null;
        }
        SqlExpr same = null;
        for (int i = 0; i < both.get(0).size(); i++) {
            SqlExpr one = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, leafOf(e, both.get(0).get(i), eTracked),
                    leafOf(a, both.get(1).get(i), aTracked));
            same = same == null ? one : SqlExpr.Call.of(SqlFn.AND, same, one);
        }
        return same;
    }

    /** One leaf of {@code instance}, read as {@link #leafRows} reads it: the field down its path, cast to its
     *  layout's type; an identity behind the guard that raises where the product does not track it. */
    private static SqlExpr leafOf(SqlExpr instance, Leaf leaf) {
        return leafOf(instance, leaf, false);
    }

    /** {@link #leafOf(SqlExpr, Leaf)}; an identity read plain where {@code tracked} (the side's plan proves the
     *  product tracks it, {@link #tracked}), as {@link #leafRows} reads it. */
    static SqlExpr leafOf(SqlExpr instance, Leaf leaf, boolean tracked) {
        SqlExpr v = instance;
        for (String step : leaf.path()) {
            v = new SqlExpr.StructGet(v, step);
        }
        String last = leaf.path().get(leaf.path().size() - 1);
        return last.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)
                ? new SqlExpr.Cast(tracked ? v : CanonicalRenderSql.trackedId(((SqlExpr.StructGet) v).source()),
                        SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                : new SqlExpr.Cast(v, leaf.type(), SqlExpr.Reason.JUDGE);
    }

    /** The SQL type {@link #leafOf} reads {@code leaf} as: an identity its text, any other leaf its layout's type. */
    static SqlType leafType(Leaf leaf) {
        return leaf.path().get(leaf.path().size() - 1).equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)
                ? SqlType.Scalar.VARCHAR : leaf.type();
    }

    /** Whether two sides are instances of ONE declared class, a model class's or a Pair's, that an equality
     *  compares by their keys or their identity ({@code InstanceEquality.sameInstances}; the last arm of
     *  {@link #pair}): never a table, a value declared Any, or a model element carried by its path. */
    public static boolean instancesOfOneClass(TypedSpec left, TypedSpec right) {
        Type l = left.info().type();
        Type r = right.info().type();
        String lf = EqualityKeys.fqnOf(l);
        // (which of these a database holds as a struct of its properties is its layout's to say: a list, a map and
        // an element carried by its path are no struct, and the callers read that)
        return lf != null && lf.equals(EqualityKeys.fqnOf(r)) && !Type.isRelation(l) && !Type.isRelation(r)
                && !PlatformTypes.isAny(l) && !PlatformTypes.isAny(r) && !PlatformTypes.isVariant(l)
                && !PlatformTypes.isVariant(r) && !PlatformTypes.isNil(l) && !PlatformTypes.isNil(r);
    }

    /** Whether two sides are instances an equality may compare by their keys or their identity
     *  ({@code InstanceEquality}): of one declared class ({@link #instancesOfOneClass}), or of two -- a value
     *  declared its parent's class holds the class it actually is, which the comparison reads first, so the two are
     *  compared where {@link #oneReadingOfTwoClasses} holds. Never a table, a value declared Any, a Variant. */
    public static boolean comparableInstances(TypedSpec left, TypedSpec right) {
        Type l = left.info().type();
        Type r = right.info().type();
        return EqualityKeys.fqnOf(l) != null && EqualityKeys.fqnOf(r) != null && !Type.isRelation(l)
                && !Type.isRelation(r) && !PlatformTypes.isAny(l) && !PlatformTypes.isAny(r)
                && !PlatformTypes.isVariant(l) && !PlatformTypes.isVariant(r) && !PlatformTypes.isNil(l)
                && !PlatformTypes.isNil(r);
    }

    /** Whether instances declared of TWO classes have one reading: each layout carries the class its instance
     *  actually is (compared first, so two classes are never the same), and the two classes declare the same keys
     *  (a class and one of its own that adds none; two with no key, by their identity). Where it does not hold
     *  the caller keeps its other rules. */
    static boolean oneReadingOfTwoClasses(SqlType.Struct l, com.legend.compiler.element.@com.legend.base.Nullable
            EqualityKeys lKeys, SqlType.Struct r, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys
            rKeys) {
        if (fieldType(l, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE) == null
                || fieldType(r, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE) == null) {
            return false;
        }
        if (lKeys == null || rKeys == null) {
            return lKeys == null && rKeys == null;
        }
        return lKeys.keys().stream().map(EqualityKeys.Key::name).toList()
                .equals(rKeys.keys().stream().map(EqualityKeys.Key::name).toList());
    }

    /** Whether two sides are instances of model classes declared of TWO classes, which an equality compares by the
     *  class each actually is ({@link #comparableInstances}, {@link #oneReadingOfTwoClasses}): what the test
     *  checker asks before it leaves such a pair unjudged. */
    public static boolean instancesOfTwoClasses(TypedSpec left, TypedSpec right) {
        return InstanceEquality.userClass(left.info().type()) && InstanceEquality.userClass(right.info().type())
                && comparableInstances(left, right) && !instancesOfOneClass(left, right);
    }

    /** Whether a value of type {@code t} holds a model class's instance in a SLOT of a Pair, a list or a map
     *  (a Pair of an object and a number, however deep): such a side has no rows of its own -- a statement's answer
     *  has no column type for an object inside a container -- and is compared as the value it is. */
    public static boolean holdsAnObjectInASlot(Type t) {
        return t instanceof Type.GenericType g && (PlatformTypes.isPairCarrier(t) || PlatformTypes.isListCarrier(t)
                || PlatformTypes.isMapCarrier(t)) && g.arguments().stream().anyMatch(a -> InstanceEquality.userClass(a)
                        || holdsAnObjectInASlot(a));
    }

    /** Whether the planner stands aside, for two sides of instances, for the one
     *  instruction over the two side VALUES ({@code InstanceEquality.sameInstances}): each side's rows hold a
     *  struct, and the two layouts' leaves pair -- what the value form asks of the same rows. */
    public static boolean instancesAsValues(TypedSpec left, SqlQuery leftPlan, TypedSpec right, SqlQuery rightPlan,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        SqlQuery l = valueRows(leftPlan);
        SqlQuery r = valueRows(rightPlan);
        return instancesOfOneClass(left, right) && l != null && r != null
                && l.outputs().get(0).type() instanceof SqlType.Struct ls
                && r.outputs().get(0).type() instanceof SqlType.Struct rs && pairedLeaves(ls, rs, keys) != null;
    }

    /** {@link #leafRows} over a side's typed rows that are a query of their own ({@code rows}: {@code __o},
     *  {@code __rn}), read as a sub-query: the rows that ride the "same values" instruction for a side of instances. */
    static SqlQuery leafRowsOf(SqlQuery rows, List<Leaf> leaves, boolean tracked) {
        return leafRows(new SqlSource.Subselect(rows, "i", null), rows, leaves, tracked);
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
            // (an Integer leaf against a Decimal one: two exact numbers, one family, compared by their value)
            if (!el.path().equals(al.path()) || !(java.util.Objects.equals(leafClass(el.type()), leafClass(al.type()))
                    || exactLeaf(el.type()) && exactLeaf(al.type()))) {
                return null;
            }
        }
        return List.of(eLeaves, aLeaves);
    }

    /** A side's keyed instances ({@code rows}, read as {@code cte}) each restricted to its classifier and its keys, a
     *  nested keyed instance restricted in its place -- the value the host's record writes
     *  ({@code HostJudgments.restrictOne}). */
    static SqlQuery restrictedRows(String cte, SqlQuery rows, SqlType.Struct layout,
            com.legend.compiler.element.EqualityKeys keys) {
        return restrictedRows(new SqlSource.Table(cte, "i", rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), rows, layout, keys);
    }

    private static SqlQuery restrictedRows(SqlSource from, SqlQuery rows, SqlType.Struct layout,
            com.legend.compiler.element.EqualityKeys keys) {
        SqlExpr held = restricted(SqlExpr.Column.of("i", rows.outputs(), O), layout, keys);
        OutputCol oOut = new OutputCol(O, held.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : layout,
                false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(held, O, oOut), new SqlSelect.Projection(
                SqlExpr.Column.of("i", rows.outputs(), RN), RN, rnOut)), false, from, null, List.of(), null,
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

    /** Whether a leaf's SQL type holds an exact number: a whole number's or a Decimal's. */
    private static boolean exactLeaf(SqlType t) {
        return "Integer".equals(leafClass(t)) || "Decimal".equals(leafClass(t));
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
        return leafRows(new SqlSource.Table(cte, "i", rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), rows, leaves, tracked);
    }

    private static SqlQuery leafRows(SqlSource from, SqlQuery rows, List<Leaf> leaves, boolean tracked) {
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
        return new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null, outs);
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
                stated(into, kindKey(slot.kind(), slot.type(), byType), !byType && instant(slot.kind())
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
                JSON_KINDS.forEach((k, t) -> stated(into, kindKey(k, t, false), t));
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
        stated(into, kindKey(kindText(t), held, byType), held);
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
        // compared, an Integer and a Decimal are one kind: two exact numbers are equal by their value (the user's
        // ruling of 2026-11-27), so the two share a compare column; recorded, each is its own
        return byType ? kind + "|" + type
                : MixedEncoding.exactNumber(kind) ? EXACT : kind;
    }

    /** The compare column's key that an Integer's and a Decimal's values share ({@link #kindKey}). */
    private static final String EXACT = "Exact number";

    /** A side's values as kinded rows ({@link #tagged}): {@code k}, the index of its kind's key in
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
                                        SqlExpr.Reason.JUDGE) : byType ? held : fitted(held, slot.type(),
                                        java.util.Objects.requireNonNull(columns.get(key), "a kind's SQL type"))));
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
                            SqlType column = java.util.Objects.requireNonNull(columns.get(key), "a kind's SQL type");
                            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(jsonKindIs(k, jt), fitted(jsonValue(k,
                                    v), java.util.Objects.requireNonNull(JSON_KINDS.get(k)), column))), nullOf(column),
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
            valueOf = key -> key.equals(own) ? byType ? SqlExpr.Column.of("r", r.outputs(), O) : fitted(
                    SqlExpr.Column.of("r", r.outputs(), O), r.outputs().get(0).type(), java.util.Objects.requireNonNull(
                            columns.get(key), "a kind's SQL type")) : null;
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
        SqlExpr instant = carried == SqlType.Scalar.TIMESTAMP ? instantOfHeld(v) : instantOfText(v);
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.TIMESTAMP, false);
        OutputCol rnOut = rows.outputs().get(1);
        return new SqlSelect(List.of(new SqlSelect.Projection(instant, O, oOut), new SqlSelect.Projection(
                SqlExpr.Column.of("t", rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Subselect(rows, "t",
                null), null, List.of(), null, null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /** A date-time the database holds ({@code v}), as the instant compared with one read from its text
     *  ({@link #instantOfText}): the same cast, so the two sides are one form on a database whose date-time is a text
     *  (SQLite: measured 2026-11-27, a held date-time left as it was never matched the text's reading there). */
    static SqlExpr instantOfHeld(SqlExpr v) {
        return new SqlExpr.Cast(v, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE);
    }

    /** A date-time carried as its written text ({@code v}), read as the instant it writes ({@link #instants}'s
     *  reading, one expression for a side's rows and for a value inside an expression, {@code Equalities.equal}): the
     *  database parses it, an hour's text (no colon, past its day) at its minute, which no database parses bare. */
    static SqlExpr instantOfText(SqlExpr v) {
        SqlExpr written = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, v, new SqlExpr.StringLit(":")),
                        new SqlExpr.IntLit(0)), SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.LENGTH, v),
                        new SqlExpr.IntLit(10))),
                SqlExpr.Call.of(SqlFn.CONCAT, v, new SqlExpr.StringLit(":00")))), v, SqlExpr.Reason.JUDGE);
        return new SqlExpr.Cast(written, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE);
    }

    /** {@code v}, a date carried as its written text, as the instant it writes where it is written with a time
     *  ({@link #instantOfText}); no value for a year, a year-month or a day: none is an instant, so none equals a
     *  date-time. What a list declared the abstract {@code Date} is read by beside held date-times (step 6B, slice 5:
     *  read as an instant whatever it was, a year in such a list failed the query where the answer is false). */
    static SqlExpr instantOfTextOrNone(SqlExpr v) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, v, new SqlExpr.StringLit("T")),
                        new SqlExpr.IntLit(0)),
                instantOfText(v))), new SqlExpr.NullLit(), SqlExpr.Reason.JUDGE);
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

    /** A side's typed rows ({@link #valueRows}: {@code __o} each value as its plan types it, {@code __rn} its place
     *  in the side's own order), for a reader outside this package ({@code planner.RootComparison}). */
    public static @com.legend.base.Nullable SqlQuery typedRows(SqlQuery wrapped) {
        return valueRows(wrapped);
    }

    /** Whether two sides whose values are of these types are compared by the one "same values" instruction over the
     *  two side VALUES, wherever the comparison stands (step 6B, slice 1): exact numbers on both sides (an Integer
     *  and a Decimal are one family, equal by their value), or Strings on both. The other kinds join slice by
     *  slice (docs/records/edition-homework/step-6b/PLAN-OF-RECORD-2026-11-27.md). */
    public static boolean comparedAsValues(Type left, Type right) {
        // (slice 3's first kinds, 2026-11-28: a Boolean, a Float, a day and a date-time on both sides -- a
        // database's typed list of each compares its values, a date-time by its instant; and a side the types say
        // holds nothing, the written empty list, against a list of any of these kinds)
        return exactNumber(left) && exactNumber(right)
                || left.equals(right) && plainKind(left)
                || PlatformTypes.isNil(left) && plainKind(right) || PlatformTypes.isNil(right) && plainKind(left);
    }

    /** Whether {@code left} and {@code right}, the two sides of a comparison of numbers one of which is declared
     *  {@code Number}, each hold values of ONE kind the query's own text names -- the kind of the value under each
     *  side's upcast ({@code MixedEncoding.kindThroughUpcasts}) -- and the two kinds can be equal by the product's
     *  number rule ({@code NumberFamilies.TWO_FAMILIES}): an Integer side against a Decimal side, two Float sides.
     *  One reading for a side lowered as a value ({@code Equalities.heldAsOneKind}) and for the planner
     *  ({@code planner.RootComparison}), so the two cannot disagree. False for a side whose kind the text does not
     *  name (a column, a conditional, arithmetic: what it holds is its value's to say), and for sides declared the
     *  abstract {@code Date}: a date's list may be carried as texts the value form does not read, so those stay
     *  the planner's statement (review 5 of step 6B's slice 3, blocker 3). */
    public static boolean heldInOneKind(TypedSpec left, TypedSpec right) {
        Type lt = left.info().type();
        Type rt = right.info().type();
        if (lt != Type.Primitive.NUMBER && rt != Type.Primitive.NUMBER || !number(lt) || !number(rt)) {
            return false;
        }
        String l = MixedEncoding.kindThroughUpcasts(left);
        String r = MixedEncoding.kindThroughUpcasts(right);
        return l != null && r != null && NumberFamilies.TWO_FAMILIES.canEqual(l, r);
    }

    private static boolean number(Type t) {
        return t == Type.Primitive.NUMBER || t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT
                || t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal;
    }

    /** A kind whose values a database's typed list holds as the values themselves. */
    private static boolean plainKind(Type kind) {
        // (an enumeration's values, held as their names: two lists of ONE enumeration -- comparedAsValues asks the
        // two kinds be the same -- are compared as two lists of Strings are; step 6B part 2)
        return exactNumber(kind) || kind == Type.Primitive.STRING || kind == Type.Primitive.BOOLEAN
                || kind == Type.Primitive.FLOAT || kind == Type.Primitive.STRICT_DATE
                || kind == Type.Primitive.DATE_TIME || kind instanceof Type.EnumType;
    }

    /** Whether the query's own text says {@code equal}'s two sides hold two types that are never equal, which the
     *  lowering answers where the comparison stands ({@code Equalities.heldApart}: false, or the same only where
     *  both hold nothing, each side still evaluated): the planner plans such a pair no statement of its own. */
    public static boolean apartByTheText(com.legend.compiler.spec.typed.TypedNativeCall equal) {
        return Equalities.apartByTheText(equal);
    }

    /** Whether {@code kind} is an exact number's: an Integer's or a Decimal's (the two are equal by their value). */
    public static boolean exactNumber(Type kind) {
        return kind == Type.Primitive.INTEGER || kind == Type.Primitive.DECIMAL || kind instanceof Type.PrecisionDecimal;
    }

    /** Whether values of kind {@code a} and of kind {@code b} are compared by their value: one kind, or two exact
     *  numbers' (an Integer's and a Decimal's; the user's ruling of 2026-11-27). Any other two kinds are never equal. */
    public static boolean oneValueKind(Type a, @com.legend.base.Nullable Type b) {
        return a.equals(b) || b != null && exactNumber(a) && exactNumber(b);
    }

    private static boolean wholeNumber(SqlType t) {
        return t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.BIGINT;
    }

    /** The Decimal type that holds every Integer beside {@code d}'s values: {@code d}'s scale, and digits enough
     *  before the point for the largest Integer (19), as far as a Decimal goes (38 digits: at a scale past 19 an
     *  Integer too large for it is the database's overflow error, never a wrong answer). */
    static SqlType.Decimal wideEnough(SqlType.Decimal d) {
        return new SqlType.Decimal(Math.min(38, Math.max(d.precision(), 19 + d.scale())), d.scale());
    }

    /** {@code value}, held in {@code own}, as its compare column {@code column} holds it: an Integer read as the
     *  column's Decimal where the column holds both exact kinds ({@link #stated}); any other as it is. */
    private static SqlExpr fitted(SqlExpr value, SqlType own, SqlType column) {
        return wholeNumber(own) && column instanceof SqlType.Decimal ? new SqlExpr.Cast(value, column,
                SqlExpr.Reason.JUDGE) : value;
    }

    /** A kind's compare column stated into {@code into}: the first type stated for its key, but where the key is the
     *  exact numbers' and both an Integer and a Decimal are stated, a Decimal that holds both ({@link #wideEnough}). */
    private static void stated(java.util.Map<String, SqlType> into, String key, SqlType type) {
        SqlType had = into.putIfAbsent(key, type);
        if (had != null && wholeNumber(had) && type instanceof SqlType.Decimal d) {
            into.put(key, wideEnough(d));
        } else if (had instanceof SqlType.Decimal d && wholeNumber(type)) {
            into.put(key, wideEnough(d));
        }
    }

    /** One side's typed rows of a pair of exact numbers ({@link #values}), carried as the other side carries its:
     *  where the other side's Decimals are their canonical text (a store-read Decimal's egress,
     *  {@code LiteralSpelling.decimalText}: no fractional trailing zero, no bare point, then {@code D}), this side's
     *  whole numbers or typed Decimals as that text -- two such texts are the same exactly where the two numbers are;
     *  an Integer side beside typed Decimals as Decimals at their scale ({@link #wideEnough}). Read over the side's
     *  rows once, from {@code cte}. Any other side as it is. */
    private static SqlQuery besideDecimals(String cte, SqlQuery rows, SqlQuery other) {
        SqlType own = rows.outputs().get(0).type();
        SqlType theirs = other.outputs().get(0).type();
        boolean ownNumber = wholeNumber(own) || own instanceof SqlType.Decimal;
        boolean theirText = theirs == SqlType.Scalar.DECIMAL_TEXT;
        if (!(ownNumber || own == SqlType.Scalar.DECIMAL_TEXT)
                || !(wholeNumber(theirs) || theirs instanceof SqlType.Decimal || theirText)) {
            // a carrier this reading does not know (an Integer past 64 bits, DuckDB's HUGEINT): the side as it is;
            // whether the two sides' carriers compare is the caller's question (oneCarrier). (2026-11-27: answered
            // "no typed form" here, two Integer sides one of which was a HUGEINT lost the comparison they had --
            // PCT's testLargePlus, testLargeMinus, testLargeTimes, testSum_Integers on channel B)
            return rows;
        }
        // (an Integer beside a Decimal whose scale leaves no room for every Integer: both as the canonical text,
        // exact at any size -- holdsEveryInteger)
        boolean tooWide = wholeNumber(own) && theirs instanceof SqlType.Decimal td && !holdsEveryInteger(td)
                || own instanceof SqlType.Decimal od && wholeNumber(theirs) && !holdsEveryInteger(od);
        boolean asText = ownNumber && theirText || tooWide;
        if (!asText && !(wholeNumber(own) && theirs instanceof SqlType.Decimal)) {
            return rows;
        }
        SqlQuery read = cteRows(cte, rows);
        SqlExpr value = SqlExpr.Column.of("t", read.outputs(), O);
        OutputCol oOut = new OutputCol(O, asText ? SqlType.Scalar.DECIMAL_TEXT
                : theirs instanceof SqlType.Decimal d ? wideEnough(d) : theirs, read.outputs().get(0).nullable());
        OutputCol rnOut = read.outputs().get(1);
        SqlExpr beside = asText ? LiteralSpelling.decimalText(value)
                : new SqlExpr.Cast(value, oOut.type(), SqlExpr.Reason.JUDGE);
        return new SqlSelect(List.of(new SqlSelect.Projection(beside, O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t",
                read.outputs(), RN), RN, rnOut)), false, new SqlSource.Subselect(read, "t", null), null, List.of(), null,
                null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /**
     * {@code assertContains} / {@code assertNotContains}' TYPED member over tagged values (item 3's S4, the audit of
     * 946d16f2: a mixed list, an Any): each value its kind and its value ({@link #kindedRows}, as {@link #tagged}
     * compares them); the verdict whether some element has the value's kind and a value {@code IS NOT DISTINCT FROM} it,
     * under {@code want}; both sides' tagged values ride the failure ({@code Family.CONTAINS_TAGGED}). Null where a
     * side has no kinded reading.
     */
    public static @com.legend.base.Nullable Compared containsTagged(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind) {
        // (an assert's membership is judged by the product's own rule: a compatibility door's is the host judge's)
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
        return typedCells(grid, named, kinds, tagged, null);
    }

    /** {@link #typedCells(GridSide, String, List, List)}, and where {@code carried} names a column's type, its cells
     *  carried as that type ({@link #besideExact}: an exact number's column beside the other table's, an Integer's and
     *  a Decimal's compared by their value). */
    static SqlQuery typedCells(GridSide grid, String named, List<Type> kinds,
            @com.legend.base.Nullable List<Boolean> tagged,
            @com.legend.base.Nullable List<@com.legend.base.Nullable SqlType> carried) {
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
            } else if (carried != null && i < carried.size() && carried.get(i) != null) {
                type = java.util.Objects.requireNonNull(carried.get(i));
                cell = type == SqlType.Scalar.DECIMAL_TEXT ? LiteralSpelling.decimalText(cell)
                        : new SqlExpr.Cast(cell, type, SqlExpr.Reason.JUDGE);
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
     *
     * <p>An Integer and a Decimal are equal by their value (the user's ruling of 2026-11-27): an Integer column
     * against a Decimal one is compared by value, each side's cells carried as one type ({@link #besideExact}).
     */
    public static @com.legend.base.Nullable Compared grids(GridSide e, List<Type> eKinds, GridSide a,
            List<Type> aKinds, boolean multiset, boolean schemasDiffer, Type eRows, Type aRows) {
        if (!typedGrid(e, eKinds) || !typedGrid(a, aKinds)) {
            return null;
        }
        List<Boolean> tagged = new ArrayList<>();
        List<@com.legend.base.Nullable SqlType> eCarried = new ArrayList<>();
        List<@com.legend.base.Nullable SqlType> aCarried = new ArrayList<>();
        for (int i = 0; i < eKinds.size(); i++) {
            boolean both = !schemasDiffer && i < aKinds.size();
            boolean twoKinds = both
                    && !java.util.Objects.equals(typedClass(eKinds.get(i)), typedClass(aKinds.get(i)));
            boolean bothExact = both && exactNumber(eKinds.get(i)) && exactNumber(aKinds.get(i));
            boolean exact = twoKinds && bothExact;
            tagged.add(twoKinds && !exact);
            // carried as one type: an Integer column beside a Decimal one (two exact numbers), and two Decimal
            // columns one of which a store read carries as its text beside one the plan types (a Decimal by its
            // number, S139; the outside audit of 2026-11-27, MEDIUM 6: text against a number had no comparison)
            boolean carry = exact || bothExact && !twoKinds;
            SqlType eType = carry ? cellType(e, i) : null;
            SqlType aType = carry ? cellType(a, i) : null;
            eCarried.add(eType == null || aType == null ? null : besideExact(eType, aType));
            aCarried.add(eType == null || aType == null ? null : besideExact(aType, eType));
        }
        // a column read as something other than its cells (a kind tag, an exact number carried as the other side's
        // type): the record's rows are read apart, as the cells themselves
        boolean anyTagged = tagged.contains(true) || eCarried.stream().anyMatch(java.util.Objects::nonNull)
                || aCarried.stream().anyMatch(java.util.Objects::nonNull);
        SqlQuery ce = typedCells(e, GRID_E, eKinds, tagged, eCarried);
        SqlQuery ca = typedCells(a, GRID_A, aKinds, tagged, aCarried);
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
        Compared alone = null;
        if (anyTagged) {
            // (the record's rows are read apart: the comparison alone is the six CTEs and the two lists before them)
            alone = new Compared(List.copyOf(ctes.subList(0, 6)), from, same, List.of());
            from = cross(cross(from, new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a",
                    null));
        }
        return new Compared(ctes, from, same, List.of(
                held(anyTagged ? "e" : "ke", eList, eRows),
                held(anyTagged ? "a" : "ka", aList, aRows)), alone);
    }

    /** The SQL type of a grid's {@code i}th data column. */
    private static SqlType cellType(GridSide grid, int i) {
        return java.util.Objects.requireNonNull(((SqlSelect) grid.wrapped()).projections().get(i).out(),
                "a grid's data column with no type").type();
    }

    /** The type an exact number's cells held as {@code own} are carried as beside cells held as {@code theirs}, so the
     *  two compare by their value: beside a Decimal's canonical text (a store-read Decimal's,
     *  {@code LiteralSpelling.decimalText}) that text; a whole number and a typed Decimal both as the Decimal that
     *  holds every Integer at the Decimal's scale ({@link #wideEnough}). Null where the cells compare as they are. */
    private static @com.legend.base.Nullable SqlType besideExact(SqlType own, SqlType theirs) {
        if (theirs == SqlType.Scalar.DECIMAL_TEXT) {
            return wholeNumber(own) || own instanceof SqlType.Decimal ? SqlType.Scalar.DECIMAL_TEXT : null;
        }
        if (wholeNumber(own) && theirs instanceof SqlType.Decimal d) {
            return holdsEveryInteger(d) ? wideEnough(d) : SqlType.Scalar.DECIMAL_TEXT;
        }
        return own instanceof SqlType.Decimal d && wholeNumber(theirs)
                ? holdsEveryInteger(d) ? wideEnough(d) : SqlType.Scalar.DECIMAL_TEXT : null;
    }

    /** Whether a Decimal at {@code d}'s scale has digits enough before the point for every Integer (19) within a
     *  Decimal's 38: past that scale an Integer beside it is compared as the canonical text instead ({@code
     *  LiteralSpelling.decimalText}), which is exact at any size -- a cast that cannot hold the Integer is an error
     *  on most databases but a silent clamp on some (the outside audit of 2026-11-27, MEDIUM 5: MySQL and MariaDB,
     *  from their documentation, not run). */
    private static boolean holdsEveryInteger(SqlType.Decimal d) {
        return 19 + d.scale() <= 38;
    }

    /** Whether two tables' columns differ in number or in name, in order: such two tables are never equal, a fact of
     *  their types (the host's {@code TdsCompare.grids}). */
    public static boolean columnsDiffer(Type.RelationType left, Type.RelationType right) {
        return left.columns().size() != right.columns().size() || java.util.stream.IntStream.range(0,
                left.columns().size()).anyMatch(i -> !left.columns().get(i).name().equals(right.columns().get(i)
                        .name()));
    }

    /**
     * Two TABLES compared by {@code ==} (the edition program's step 6, the one equality, 2026-11-27): the comparison an
     * {@code assertEquals} of two tables is judged by ({@link #grids}) over each side's plan as the product planned it --
     * the same columns by name, then the same rows, in order or as a multiset ({@code multiset}: the caller's, by
     * whether the query defines both tables' order), each cell by its column's type -- its rows numbered where
     * the plan defines their order ({@link SideOrdinal}). The two tables' columns have the same names ({@link
     * #columnsDiffer}, answered by the caller from the types alone), and each side's columns are given with the kinds
     * its plan's outputs decide ({@code compiler.spec.VerdictQueries.wireDecidedKinds}, the caller's too: the lowering
     * reaches no compiler class but the typed tree's). Null where a side's plan does not carry exactly its declared
     * columns, or a column has no typed comparison.
     */
    public static @com.legend.base.Nullable Compared tables(SqlQuery left, Type.RelationType l,
            SqlQuery right, Type.RelationType r, boolean multiset) {
        if (left.outputs().size() != l.columns().size() || right.outputs().size() != r.columns().size()) {
            return null;
        }
        return grids(tableSide(left, l.columns().size()), l.columns().stream().map(Type.Column::type).toList(),
                tableSide(right, r.columns().size()), r.columns().stream().map(Type.Column::type).toList(),
                multiset, false, l, r);
    }

    /** A table's plan as a grid side: its rows' ordinal as its last output, and its data columns named and typed
     *  projections of one select (a plan that is a union, or reads its source's columns by a star, is read through one
     *  more select). */
    private static GridSide tableSide(SqlQuery plan, int width) {
        SqlQuery ordered = SideOrdinal.withOrdinal(plan);
        boolean cells = ordered instanceof SqlSelect s && s.projections().size() >= width
                && s.projections().subList(0, width).stream().allMatch(p -> p.alias() != null && p.out() != null);
        if (!cells) {
            List<SqlSelect.Projection> ps = new ArrayList<>(ordered.outputs().size());
            for (OutputCol o : ordered.outputs()) {
                ps.add(new SqlSelect.Projection(SqlExpr.Column.of("__t", o), o.name(), o));
            }
            ordered = new SqlSelect(ps, false, new SqlSource.Subselect(ordered, "__t", null), null, List.of(), null,
                    null, List.of(), null, null, ordered.outputs());
        }
        return new GridSide(ordered, width, List.of());
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

    /** Two sides of instances of one class carried as JSON documents' text (item 3's S4, the last families: a
     *  metamodel's SetImplementation, a Table -- a graph's array of its root objects), each side its documents' root
     *  elements, as the host reads a graph side ({@code Verdicts.decodeSideValues}): their texts compared -- one
     *  serializer writes both, so one instance is one text -- in order or as a multiset; each side's elements ride the
     *  failure as JSON, read as instances, as the host's record writes them. Null where a side has no value column. */
    public static @com.legend.base.Nullable Compared instanceTexts(SqlQuery eWrapped, Type eKind,
            SqlQuery aWrapped, Type aKind, boolean multiset) {
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
                    SqlExpr.Column.of("a", aList.outputs(), LISTED), new SqlExpr.ScalarSubquery(cteRows("__xe", eRows)),
                    new SqlExpr.ScalarSubquery(cteRows("__xa", aRows)));
            return new Compared(List.of(new SqlWith.Cte("__xe", eRows), new SqlWith.Cte("__xa", aRows),
                            new SqlWith.Cte("__de", jsonDocuments("__xe", eRows)),
                            new SqlWith.Cte("__da", jsonDocuments("__xa", aRows))),
                    cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a",
                            null)), new SqlSource.Subselect(eDocs, "de", null)), new SqlSource.Subselect(aDocs, "da", null)),
                    same, List.of(
                            held("de", eDocs, eKind), held("da", aDocs, aKind)));
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
                new SqlExpr.ScalarSubquery(cteRows("__te", eTexts)), new SqlExpr.ScalarSubquery(cteRows("__ta", aTexts)));
        return new Compared(List.of(new SqlWith.Cte("__xe", eRows), new SqlWith.Cte("__xa", aRows),
                        new SqlWith.Cte("__ee", eEl), new SqlWith.Cte("__ea", aEl),
                        new SqlWith.Cte("__te", eTexts), new SqlWith.Cte("__ta", aTexts)),
                cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a", null)),
                        new SqlSource.Subselect(eDocs, "de", null)), new SqlSource.Subselect(aDocs, "da", null)),
                same, List.of(
                        held("de", eDocs, eKind), held("da", aDocs, aKind)));
    }

    /** A side's document texts ({@code rows}, read as {@code cte}) each as its JSON document, in order. */
    static SqlQuery jsonDocuments(String cte, SqlQuery rows) {
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.JSON, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("t", rows.outputs(), O),
                SqlType.Scalar.JSON, SqlExpr.Reason.JUDGE), O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t",
                rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, "t", rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(oOut, rnOut));
    }

    /** A side's documents ({@code rows}, read as {@code cte}) as their root arrays' elements, each JSON, in order. */
    static SqlQuery rootElements(String cte, SqlQuery rows) {
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
    static SqlQuery elementTexts(String cte, SqlQuery rows) {
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.VARCHAR, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("t", rows.outputs(), O),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t",
                rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, "t", rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(oOut, rnOut));
    }

    /** {@code wrapped}'s first column as its text, in the side's own order. */
    static @com.legend.base.Nullable SqlQuery documentTexts(SqlQuery wrapped) {
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
     * HOW TWO SIDES COMPARE ({@link #pair}): the kinds each side's values are compared as and the form of the
     * comparison -- typed values; dates, by kind and instant; the same only where both are empty (an empty side, two
     * kinds never equal, a Variant against another type); tagged values, each its kind and its value; instances of one
     * class, each a row of its typed leaves ({@code keys}, or its identity where it has none); lists and maps; instances
     * carried as their JSON documents.
     */
    public record Pair(Type e, Type a, Form form, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        /** The form of a pair's comparison. */
        public enum Form { VALUES, DATES, EMPTY_ONLY, TAGGED, INSTANCES, COLLECTIONS, INSTANCE_TEXTS }

        Pair(Type e, Type a, Form form) {
            this(e, a, form, null);
        }

        /** Whether the pair's two typed kinds differ (register S127): a reader's question about the record it
         *  writes. An Integer against a Decimal differ in kind here too, though {@link #values} compares the two by
         *  their value under the product's two families. */
        public boolean kindsDiffer() {
            return form == Form.VALUES && !e.equals(a)
                    // a Variant against values of another type (register S401): the same only where both are empty
                    || form == Form.EMPTY_ONLY && (PlatformTypes.isVariant(e) || PlatformTypes.isVariant(a));
        }

        /** The pair's comparison over its two sides' plans, in order or as a multiset; null where a side's values
         *  have no typed column. */
        public @com.legend.base.Nullable Compared compared(SqlQuery ePlan, SqlQuery aPlan, boolean multiset) {
            return switch (form) {
                case TAGGED -> tagged(ePlan, e, aPlan, a, multiset);
                // (two sides of instances are the one instruction over the two side values, where the comparison
                // is lowered: InstanceEquality.sameInstances; no statement of their own -- step 6B part 2)
                case INSTANCES -> null;
                case COLLECTIONS -> collections(ePlan, e, aPlan, a, multiset);
                case VALUES, DATES, EMPTY_ONLY -> values(ePlan, e, aPlan, a, multiset, form == Form.EMPTY_ONLY);
                case INSTANCE_TEXTS -> instanceTexts(ePlan, e, aPlan, a, multiset);
            };
        }
    }

    /** How the two sides of an equality compare: each side's typed class (two number kinds then the same only where
     *  both are empty, register S127), a date side's its kind ({@link #dateKind}) -- or null where the pair has no
     *  typed comparison (the caller refuses it by name). A Decimal literal is typed: its written scale rides its SQL
     *  type. */
    public static @com.legend.base.Nullable Pair pair(TypedSpec eSpec, Type eKind, SqlQuery ePlan, TypedSpec aSpec,
            Type aKind, SqlQuery aPlan, ModelContext ctx) {
        // a Variant among a side's values (a mixed collection's member too): register S401's refusal where only
        // identity decides, before any other form reads the pair
        refuseIdentityOnly(eSpec, aSpec);
        // a side declared Any is a Variant only where the plan sees one under its casts ({@code VariantEquality.sideType}); its JSON
        // carrier cannot tell a Variant from the value it holds (the audit of 8477c703: an Integer cast to Any had been
        // read as a Variant, and two Variants cast to Any raised at run)
        Type eSide = PlatformTypes.isAny(eSpec.info().type()) ? com.legend.compiler.spec.typed.VariantEquality.sideType(eSpec) : eKind;
        Type aSide = PlatformTypes.isAny(aSpec.info().type()) ? com.legend.compiler.spec.typed.VariantEquality.sideType(aSpec) : aKind;
        if (PlatformTypes.isVariant(eSide) || PlatformTypes.isVariant(aSide)) {
            // register S401 (ruled by the user 2026-11-23): two whole Variants, or a Variant and an Any the plan cannot
            // see, refused by name (refuseIdentityOnly); a Variant against a value of any other type is not equal, as
            // in Pure -- the two the same only where both are empty ({@link #values}), its values riding the
            // record as carried
            refuseIdentityOnly(eSpec, aSpec);
            return new Pair(eSide, aSide, Pair.Form.EMPTY_ONLY);
        }
        boolean eMixed = carriesKinds(ePlan) || writtenDate(eKind, ePlan) || jsonCarried(ePlan);
        boolean aMixed = carriesKinds(aPlan) || writtenDate(aKind, aPlan) || jsonCarried(aPlan);
        if (eMixed || aMixed) {
            // a value whose kind the database decides among candidates (a Number's max, a mixed list), or a Date
            // written as its text (a partial date, a Date the egress writes): each value its kind and its value,
            // against a side of one typed class or another such side ({@link #tagged})
            return (eMixed || sideKind(eSpec, eKind, ePlan) != null) && (aMixed || sideKind(aSpec, aKind, aPlan) != null)
                    ? new Pair(eKind, aKind, Pair.Form.TAGGED) : null;
        }
        if (PlatformTypes.isNil(eKind) || PlatformTypes.isNil(aKind)) {
            // an empty side (Nil, the empty collection's kind): the two the same only where both are empty, the other
            // side's values typed by its own kind for the record
            Type e = PlatformTypes.isNil(eKind) ? eKind : emptyKind(eKind, ePlan) != null ? eKind
                    : sideKind(eSpec, eKind, ePlan);
            Type a = PlatformTypes.isNil(aKind) ? aKind : emptyKind(aKind, aPlan) != null ? aKind
                    : sideKind(aSpec, aKind, aPlan);
            return e == null || a == null ? null : new Pair(e, a, Pair.Form.EMPTY_ONLY);
        }
        Type e = dateKind(eSpec, eKind, ePlan);
        Type a = dateKind(aSpec, aKind, aPlan);
        if (e != null && a != null) {
            // a date literal's side rides the literal channel (its written text), its value column still the
            // database's DATE or TIMESTAMP, which the pair compares (S399)
            return new Pair(e, a, Pair.Form.DATES);
        }
        e = typedClass(eKind);
        a = typedClass(aKind);
        if (e != null && a != null) {
            return new Pair(e, a, Pair.Form.VALUES);
        }
        String ef = EqualityKeys.fqnOf(eSpec.info().type());
        if (named(ePlan) && named(aPlan) && ef != null && ef.equals(EqualityKeys.fqnOf(aSpec.info().type()))
                && !PlatformTypes.isAny(eSpec.info().type()) && !ctx.isSubtype(ef, PACKAGEABLE)) {
            // instances of one class carried as their JSON documents' text (a metamodel's SetImplementation, a Table):
            // the documents compared as texts, recorded as documents (item 3's S4, the last families)
            return new Pair(eSpec.info().type(), aSpec.info().type(), Pair.Form.INSTANCE_TEXTS);
        }
        if (named(ePlan) && named(aPlan) && EqualityKeys.fqnOf(eKind) instanceof String ek
                && ek.equals(EqualityKeys.fqnOf(aKind))) {
            // a packageable element carried by its path (a Mapping, a class reference of either class): its path
            // compared, a String; one kind for both, the metaclass they share -- Class<p::Q> and Class<Any> are two
            // views of one reference, never kinds that differ (the audit of 3f2fe452)
            return new Pair(eKind, eKind, Pair.Form.VALUES);
        }
        if (collection(ePlan) && collection(aPlan)) {
            // nested lists and maps: each value as its database holds it ({@link #collections})
            return new Pair(eKind, aKind, Pair.Form.COLLECTIONS);
        }
        // instances of ONE class -- a model class's or a Pair's -- its keys read here, at planning (a subtype pair
        // restricts by the classifiers its run carries, which the host reads)
        return ef != null && ef.equals(EqualityKeys.fqnOf(aSpec.info().type()))
                ? new Pair(eKind, aKind, Pair.Form.INSTANCES, variantKeysRefused(EqualityKeys.resolve(ctx,
                        eSpec.info().type())))
                : null;
    }

    /** The kind an emptiness assert's typed member lists a side's values as (item 3's S4): its typed class, or, where
     *  its values are carried as a struct, a map, a list or a JSON document (an instance, a Pair, a Map, a nested
     *  list, an Any), its own kind -- whether a side is empty is a fact of its rows whatever their type, its values
     *  riding the failure as the host's reader reads them; null where its plan types no value column (a tagged value
     *  lists its kinds' values: its own member). */
    public static @com.legend.base.Nullable Type emptyKind(@com.legend.base.Nullable Type kind, SqlQuery plan) {
        Type typed = typedClass(kind);
        if (typed != null || kind == null || plan.outputs().isEmpty()) {
            return typed;
        }
        // any carrier the plan types: a struct, a map, a list, JSON, or a scalar of a kind of its own (an Any-typed side
        // whose values one SQL type holds); never a tagged value's tag (its kinds' values: its own member)
        SqlType t = plan.outputs().get(0).type();
        return t instanceof SqlType.Tagged ? null : kind;
    }

    /** Whether a side of a class's kind carries each value as a text (a packageable element by its path). */
    static boolean named(SqlQuery plan) {
        return !plan.outputs().isEmpty() && plan.outputs().get(0).type() == SqlType.Scalar.VARCHAR;
    }

    /** Whether a side's values are lists or maps (a nested List, a Map). */
    static boolean collection(SqlQuery plan) {
        return !plan.outputs().isEmpty() && (plan.outputs().get(0).type() instanceof SqlType.Array
                || plan.outputs().get(0).type() instanceof SqlType.Map);
    }

    /** Whether a side's values are carried as JSON (an Any): each its JSON kind's value ({@link #tagged}). */
    static boolean jsonCarried(SqlQuery plan) {
        return !plan.outputs().isEmpty() && plan.outputs().get(0).type() == SqlType.Scalar.JSON;
    }

    /** Whether a side's plan carries a tagged value (its first column a {@code SqlType.Tagged}: the kind the
     *  database decided among candidates). */
    static boolean carriesKinds(SqlQuery plan) {
        return !plan.outputs().isEmpty() && plan.outputs().get(0).type() instanceof SqlType.Tagged;
    }

    /** A side's typed kind: its date kind ({@link #dateKind}), else its typed class; null where it has neither. */
    static @com.legend.base.Nullable Type sideKind(TypedSpec spec, Type kind, SqlQuery plan) {
        Type d = dateKind(spec, kind, plan);
        return d != null ? d : typedClass(kind);
    }

    /** A date side's kind under register S399 (the user's ruling of 2026-11-22: the product compares a date by its
     *  kind and its instant, the database comparing): a literal's own -- a day a StrictDate, a date-time of any written
     *  precision a DateTime, a partial date none (its written text the tagged form's) --, a StrictDate's or DateTime's side
     *  its own, a {@code Date} side's by its column's SQL type (a DATE holds days, a TIMESTAMP date-times); null for any
     *  other side. A fact of the types, fixed at planning. */
    static @com.legend.base.Nullable Type dateKind(TypedSpec spec, Type kind, SqlQuery plan) {
        if (spec instanceof com.legend.compiler.spec.typed.TypedCDate d) {
            return d.value() instanceof com.legend.values.PureDateLiteral.StrictDate ? Type.Primitive.STRICT_DATE
                    : d.value().precision().atLeast(com.legend.values.PureDateLiteral.Precision.HOUR)
                            ? Type.Primitive.DATE_TIME : null;
        }
        if (kind == Type.Primitive.STRICT_DATE || kind == Type.Primitive.DATE_TIME) {
            return kind;
        }
        if (kind != Type.Primitive.DATE || plan.outputs().isEmpty()) {
            return null;
        }
        SqlType sql = plan.outputs().get(0).type();
        return sql == SqlType.Scalar.DATE ? Type.Primitive.STRICT_DATE
                : sql == SqlType.Scalar.TIMESTAMP || sql == SqlType.Scalar.TIMESTAMPTZ
                        ? Type.Primitive.DATE_TIME : null;
    }

    /** Register S401 (ruled by the user 2026-11-23): an equality only identity decides -- two whole Variants, a
     *  Variant and an Any the plan cannot see, or a Variant among values of other types -- refused by name at planning,
     *  under both judges alike, as the product refuses it ({@code VariantEquality.between}, which reads a side through
     *  its casts and its collection literal's members). */
    public static com.legend.compiler.spec.typed.VariantEquality.Verdict refuseIdentityOnly(TypedSpec e, TypedSpec a) {
        refuseMixedDays(e, a);
        return com.legend.compiler.spec.typed.VariantEquality.between(e, a);
    }

    /** Register S399 (a day is never a date-time): a side whose values hold both days and date-times (a list literal,
     *  read through a map, a filter or an if as {@code VariantEquality.dateKinds} reads it, never a function's arguments) reaches the
     *  database as one kind (DuckDB's and H2's arrays hold one type), so neither judge can tell its days apart --
     *  refused by name at planning, under both judges alike (the final audit of c758fe95, 2026-11-23: the host had
     *  called a day contained in such a list; the narrow review of f9e74256: a list read through a map had escaped).
     *  A list bound by a let reaches the judges as a variable, unread: a recorded gap (S399). */
    public static void refuseMixedDays(TypedSpec... sides) {
        for (TypedSpec s : sides) {
            java.util.Set<Type> kinds = com.legend.compiler.spec.typed.VariantEquality.dateKinds(s);
            if (kinds.contains(Type.Primitive.STRICT_DATE) && kinds.contains(Type.Primitive.DATE_TIME)) {
                throw new com.legend.error.NotImplementedException("values holding both days and date-times are not"
                        + " compared: the database carries them as one kind, so their days cannot be told apart (register"
                        + " S399); compare the days and the date-times apart");
            }
        }
    }

    /** {@code k}, after register S401's refusal of a key declared Variant. */
    static @com.legend.base.Nullable EqualityKeys variantKeysRefused(@com.legend.base.Nullable EqualityKeys k) {
        com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(k);
        return k;
    }

    static final String PACKAGEABLE = "meta::pure::metamodel::PackageableElement";
}
