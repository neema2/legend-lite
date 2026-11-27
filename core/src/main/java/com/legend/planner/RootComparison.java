// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.SideEqualities;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A STATEMENT WHOSE ANSWER IS A COMPARISON OF TWO WHOLE SIDES (the edition program's step 6, 2026-11-27: the one
 * equality): {@code |$a == $b} or {@code |!($a == $b)} where a side is a computed list (of values, of instances,
 * of values typed only when the query runs), a map or a table. Each side is planned once, as the statement it is, and the
 * product's equality over two sides compares them ({@link SideEqualities}): the same code that judges a test's
 * {@code assertEquals}, so what an assert judges is what a query answers, and a side's rows are a statement's on every
 * database (no list is compared where a database has no list equality).
 *
 * <p>Not planned here, each the plain comparison it was: two sides an expression compares exactly on every database
 * (values of known scalar types, instances, anything written in the query itself); two known types that differ (the
 * comparison is whatever the door that admitted the code made of it, a compile error on lite's own); {@code eq},
 * which is identity.
 *
 * <p>Two tables are compared as two tables ({@link #tables}): their columns' names, then their rows.
 *
 * <p>{@code |sameElements($a, $b)} is planned here too ({@code Pure.Lite.SAME_ELEMENTS}; the user, 2026-11-27): the
 * same two sides, their elements or rows compared in ANY order, each as often on one side as on the other. It has no
 * other form: where {@code ==} would fall back to the plain comparison, it is refused by name.
 *
 * <p>The product's own rule alone: a comparison a compatibility door admitted is the plain comparison it was.
 */
final class RootComparison {

    private RootComparison() {
    }

    /** {@code body}'s comparison planned over its two sides ({@code lowered}: a body's plan, the lets it reads and
     *  one side), or null where its last statement is no comparison of two whole sides, or the sides have no typed
     *  comparison (the statement is then lowered as any expression is). */
    static @Nullable SqlQuery plan(List<TypedSpec> body, Function<List<TypedSpec>, SqlQuery> lowered,
            Function<TypedNativeCall, com.legend.lowering.NumberFamilies> numbersOf, ModelContext ctx) {
        if (body.isEmpty()) {
            return null;
        }
        TypedSpec root = body.get(body.size() - 1);
        boolean negated = false;
        if (root instanceof TypedNativeCall not && not.args().size() == 1
                && Pure.AT_BOOLEAN_NOT.contains(not.callee().id())) {
            negated = true;
            root = not.args().get(0);
        }
        if (!(root instanceof TypedNativeCall equal) || equal.args().size() != 2) {
            return null;
        }
        // sameElements(a, b): the two sides' elements, or two tables' rows, in ANY order (Pure.Lite.SAME_ELEMENTS)
        boolean anyOrder = Pure.AT_LEGEND_LITE_SAME_ELEMENTS.contains(equal.callee().id());
        if (!anyOrder && !Pure.AT_BOOLEAN_EQUAL.contains(equal.callee().id())) {
            return null;
        }
        TypedSpec left = equal.args().get(0);
        TypedSpec right = equal.args().get(1);
        // (sameElements is the product's own function: the product's rule for numbers, whichever door admitted the
        // code that calls it)
        com.legend.lowering.NumberFamilies numbers = anyOrder ? com.legend.lowering.NumberFamilies.TWO_FAMILIES
                : numbersOf.apply(equal);
        if (numbers != com.legend.lowering.NumberFamilies.TWO_FAMILIES) {
            // a comparison a compatibility door admitted (its table names its own rule for ==): the plain comparison
            // it was before this class, so the door keeps every answer it had -- the database's for two sides typed
            // Number or Any (1->cast(@Number) == 1D->cast(@Number) is true at legend-engine's door), no refusal the
            // plain path did not make, two tables as they were (the outside audit of 2026-11-27, HIGH 1)
            return null;
        }
        if (body.stream().anyMatch(com.legend.compiler.spec.UserCallInliner::drawsAfresh)) {
            // a value drawn afresh where it is written (random(), generateGuid()): planned as two sides, a let both
            // sides read would be drawn twice (the same audit): the plain comparison, which reads it once or refuses
            return unplanned(anyOrder, "a value drawn afresh where it is written (random, generateGuid) would be"
                    + " drawn once for each side");
        }
        List<TypedSpec> lets = body.subList(0, body.size() - 1);
        if (Type.isRelation(left.info().type()) || Type.isRelation(right.info().type())) {
            return tables(left, right, lets, negated, lowered, numbers, anyOrder);
        }
        if (known(left.info().type()) && known(right.info().type())
                && !java.util.Objects.equals(SideEqualities.typedClass(left.info().type()),
                        SideEqualities.typedClass(right.info().type()))
                // (an Integer side against a Decimal side is one comparison, by value: two exact numbers)
                && !(SideEqualities.exactNumber(left.info().type())
                        && SideEqualities.exactNumber(right.info().type()))) {
            if (anyOrder) {
                // the equality rule: different types never compare, a compile error naming the two
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                        "sameElements compares elements of one type, and its sides hold "
                                + left.info().type().typeName() + " and " + right.info().type().typeName()
                                + ": two types are never equal -- convert one side");
            }
            // two known types: the comparison is whatever the door that admitted the code made of it (refused on
            // lite's own, the database's by value on legend-engine's), which stands as it is lowered
            return null;
        }
        if (!anyOrder && !whole(left) && !whole(right)) {
            // two values an expression compares exactly on every database: the plain comparison
            return null;
        }
        SqlQuery leftPlan = lowered.apply(with(lets, left));
        SqlQuery rightPlan = lowered.apply(with(lets, right));
        SideEqualities.Pair pair = SideEqualities.pair(left, kind(left, leftPlan), leftPlan, right,
                kind(right, rightPlan), rightPlan, ctx);
        SideEqualities.Compared compared = pair == null ? null
                : pair.under(numbers).compared(leftPlan, rightPlan, anyOrder);
        return compared == null ? unplanned(anyOrder, "its sides (" + left.info().type().typeName() + " and "
                + right.info().type().typeName() + ") have no typed comparison") : answer(compared, negated);
    }

    /** A statement this class does not plan: the plain comparison for {@code ==} (null: lowered as any expression
     *  is), a refusal by name for {@code sameElements}, which has no other form. */
    private static @Nullable SqlQuery unplanned(boolean anyOrder, String why) {
        if (anyOrder) {
            throw new com.legend.error.NotImplementedException("sameElements: " + why);
        }
        return null;
    }

    /** The statement's one row: whether the two sides are the same, or (under a {@code !}) are not. */
    private static SqlQuery answer(SideEqualities.Compared judged, boolean negated) {
        // (a query lists no failure's record: the comparison without the rows only a record reads)
        SideEqualities.Compared compared = judged.answerOnly();
        SqlSelect row = new SqlSelect(List.of(new SqlSelect.Projection(compared.holds(!negated), "value", VALUE)),
                false, compared.from(), null, List.of(), null, null, List.of(), null, null, List.of(VALUE));
        return SqlWith.prepend(compared.ctes(), row);
    }

    private static final OutputCol VALUE = new OutputCol("value", SqlType.Scalar.BOOLEAN, false);

    /**
     * Two TABLES: the same where their columns have the same names in the same order and their rows are the same,
     * each cell by its column's type ({@link SideEqualities#tables}, the comparison an assert of two tables is judged
     * by), in order. A side whose order the query does not define is refused by name: {@code sameElements} is the
     * comparison of rows in any order. Two tables whose columns differ in name or number are never equal, a fact of their
     * types: answered with no read. Null (the statement is lowered as any expression is) where a side is no table,
     * a table's columns exist only when the query runs, or a column has no typed comparison.
     */
    private static @Nullable SqlQuery tables(TypedSpec left, TypedSpec right, List<TypedSpec> lets, boolean negated,
            Function<List<TypedSpec>, SqlQuery> lowered, com.legend.lowering.NumberFamilies numbers,
            boolean anyOrder) {
        if (!(Type.schemaView(left.info().type()) instanceof Type.RelationType l) || l.isLateBound()
                || !(Type.schemaView(right.info().type()) instanceof Type.RelationType r) || r.isLateBound()) {
            return unplanned(anyOrder, "a table against a side that is no table, or a table whose columns exist only"
                    + " when the query runs");
        }
        if (SideEqualities.columnsDiffer(l, r)) {
            return new SqlSelect(List.of(new SqlSelect.Projection(new com.legend.sql.SqlExpr.BoolLit(negated), "value",
                    VALUE)), false, new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null,
                    null, List.of(VALUE));
        }
        SqlQuery leftPlan = lowered.apply(with(lets, left));
        SqlQuery rightPlan = lowered.apply(with(lets, right));
        if (leftPlan.outputs().size() != l.columns().size() || rightPlan.outputs().size() != r.columns().size()) {
            return unplanned(anyOrder, "a table whose plan's columns are not its declared ones");
        }
        // == compares two tables' rows IN ORDER, as assertEquals does, and a table whose order the query does not
        // define (a store's rows, a DISTINCT, a grouping) has none to compare: refused, naming the two fixes, as the
        // assert refuses it (the user, 2026-11-27: strict ==, and "sameElements" for the rows in any order; for one
        // afternoon an unsorted side was compared as a multiset here, one operator with two meanings). Never by the
        // order a database happened to return them in (the outside audit of 2026-11-27, MEDIUM 7).
        boolean leftUnordered = !anyOrder && com.legend.compiler.spec.OrderedAsserts.unorderedRows(left, lets);
        if (leftUnordered || !anyOrder && com.legend.compiler.spec.OrderedAsserts.unorderedRows(right, lets)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                    "== compares two tables in order, and its " + (leftUnordered ? "first" : "second")
                            + " side's order is not defined (a store or frame read with no sort, or past a grouping, a"
                            + " join or a union): the database's arrival order would decide the answer -- sort both"
                            + " sides, or say sameElements when the order does not matter");
        }
        // each column by the kind its plan's output decides where its declaration leaves it open (a String, a Number,
        // an Any: the reading an assert's judge gives a table's cells); sameElements: the rows as a multiset
        SideEqualities.Compared compared = SideEqualities.tables(leftPlan,
                com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(l, leftPlan.outputs()), rightPlan,
                com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(r, rightPlan.outputs()), anyOrder, numbers);
        return compared == null ? unplanned(anyOrder, "a column of its tables has no typed comparison")
                : answer(compared, negated);
    }

    /** Whether {@code t} names its values' type: a primitive that is no supertype of others, or an enumeration. */
    private static boolean known(Type t) {
        return t instanceof Type.Primitive p && p != Type.Primitive.NUMBER && p != Type.Primitive.DATE
                || t instanceof Type.PrecisionDecimal || t instanceof Type.EnumType;
    }

    /** Whether {@code side} is compared as a whole side, never as a value inside an expression: a computed list
     *  (a database may hold none as a value, or hold it as text), a map, or values whose type is known only when the
     *  query runs. A side written in the query (a literal, a collection of literals, the empty list) is a value everywhere:
     *  the plain comparison reads it exactly. */
    private static boolean whole(TypedSpec side) {
        if (written(side)) {
            return false;
        }
        Type t = side.info().type();
        // (a map too: its entries are compared whatever order they were written in, which no database's own map
        // comparison promises -- measured 2026-11-27, newMap([a, b]) == newMap([b, a]) answered false on DuckDB and
        // SQLite where an assert of the two passes)
        // (ONE value typed Number, Any or the abstract Date is no whole side: it is compared where it stands, by the
        // one rule an expression's comparison has -- the type-checker's for an upcast, lowering.HeldKinds for a type
        // only a column gives -- so the same comparison answers the same as a query's answer and inside an if; the
        // outside audit of 2026-11-27, MEDIUM 3: planned as a side, $row.n == 1.0 over an INTEGER column answered
        // false here and true inside an expression)
        return PlatformTypes.isMapCarrier(t) || side.info().multiplicity().isMany();
    }

    private static boolean written(TypedSpec side) {
        return switch (side) {
            case com.legend.compiler.spec.typed.TypedCollection c -> c.elements().stream()
                    .allMatch(RootComparison::written);
            case com.legend.compiler.spec.typed.TypedCInteger i -> true;
            case com.legend.compiler.spec.typed.TypedCFloat f -> true;
            case com.legend.compiler.spec.typed.TypedCDecimal d -> true;
            case com.legend.compiler.spec.typed.TypedCString t -> true;
            case com.legend.compiler.spec.typed.TypedCBoolean b -> true;
            case com.legend.compiler.spec.typed.TypedCDate d -> true;
            case com.legend.compiler.spec.typed.TypedEnumValue e -> true;
            default -> false;
        };
    }

    /** The kind a side's values are compared as: its type, a {@code Number} side's the number type its plan's column
     *  holds where that names one (the fact the database judge's side reads, {@code CanonicalRenderSql}). */
    private static Type kind(TypedSpec side, SqlQuery plan) {
        Type t = side.info().type();
        if (t == Type.Primitive.NUMBER && !plan.outputs().isEmpty()) {
            Type held = Type.kindOfSqlType(plan.outputs().get(0).type());
            if (held == Type.Primitive.INTEGER || held == Type.Primitive.FLOAT || held == Type.Primitive.DECIMAL) {
                return held;
            }
        }
        return PlatformTypes.isAny(t) ? com.legend.compiler.spec.typed.VariantEquality.sideType(side) : t;
    }

    private static List<TypedSpec> with(List<TypedSpec> lets, TypedSpec side) {
        List<TypedSpec> body = new ArrayList<>(lets);
        body.add(side);
        return body;
    }
}
