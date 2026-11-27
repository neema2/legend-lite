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
 * equality): {@code |$a == $b} or {@code |!($a == $b)} where a side is a list, an optional value, an instance, or a
 * value whose type is known only when the query runs. Each side is planned once, as the statement it is, and the
 * product's equality over two sides compares them ({@link SideEqualities}): the same code that judges a test's
 * {@code assertEquals}, so what an assert judges is what a query answers, and a side's rows are a statement's on every
 * database (no list is compared where a database has no list equality).
 *
 * <p>Not planned here, each the plain comparison it was: two sides an expression compares exactly on every database
 * (values of known scalar types, instances, anything written in the query itself); two known types that differ (the
 * comparison is whatever the door that admitted the code made of it, a compile error on lite's own); {@code eq},
 * which is identity.
 *
 * <p>Two tables are compared as two tables ({@link #tables}): their columns' names, then their rows in order.
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
        if (!(root instanceof TypedNativeCall equal) || equal.args().size() != 2
                || !Pure.AT_BOOLEAN_EQUAL.contains(equal.callee().id())) {
            return null;
        }
        TypedSpec left = equal.args().get(0);
        TypedSpec right = equal.args().get(1);
        List<TypedSpec> lets = body.subList(0, body.size() - 1);
        if (Type.isRelation(left.info().type()) || Type.isRelation(right.info().type())) {
            return tables(left, right, lets, negated, lowered, numbersOf.apply(equal));
        }
        if (known(left.info().type()) && known(right.info().type())
                && !java.util.Objects.equals(SideEqualities.typedClass(left.info().type()),
                        SideEqualities.typedClass(right.info().type()))
                // (an Integer side against a Decimal side is one comparison, by value: two exact numbers -- under the
                // product's rule; a door that keeps three number types made the comparison what it is already)
                && !(numbersOf.apply(equal) == com.legend.lowering.NumberFamilies.TWO_FAMILIES
                        && SideEqualities.exactNumber(left.info().type())
                        && SideEqualities.exactNumber(right.info().type()))) {
            // two known types: the comparison is whatever the door that admitted the code made of it (refused on
            // lite's own, the database's by value on legend-engine's), which stands as it is lowered
            return null;
        }
        if (!whole(left) && !whole(right)) {
            // two values an expression compares exactly on every database: the plain comparison
            return null;
        }
        SqlQuery leftPlan = lowered.apply(with(lets, left));
        SqlQuery rightPlan = lowered.apply(with(lets, right));
        SideEqualities.Pair pair = SideEqualities.pair(left, kind(left, leftPlan), leftPlan, right,
                kind(right, rightPlan), rightPlan, ctx);
        // (numbers of different types by the rule this comparison takes at the door that admitted it: the product's
        // two families, or the three types a compatibility door's table names)
        SideEqualities.Compared compared = pair == null ? null
                : pair.under(numbersOf.apply(equal)).compared(leftPlan, rightPlan, false);
        return compared == null ? null : answer(compared, negated);
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
     * Two TABLES: the same where their columns have the same names in the same order and their rows are the same in
     * order, each cell by its column's type ({@link SideEqualities#tables}, the comparison an {@code assertEquals} of
     * two tables is judged by). Two tables whose columns differ in name or number are never equal, a fact of their
     * types: answered with no read. Null (the statement is lowered as any expression is) where a side is no table,
     * a table's columns exist only when the query runs, or a column has no typed comparison.
     */
    private static @Nullable SqlQuery tables(TypedSpec left, TypedSpec right, List<TypedSpec> lets, boolean negated,
            Function<List<TypedSpec>, SqlQuery> lowered, com.legend.lowering.NumberFamilies numbers) {
        if (!(Type.schemaView(left.info().type()) instanceof Type.RelationType l) || l.isLateBound()
                || !(Type.schemaView(right.info().type()) instanceof Type.RelationType r) || r.isLateBound()) {
            return null;
        }
        if (SideEqualities.columnsDiffer(l, r)) {
            return new SqlSelect(List.of(new SqlSelect.Projection(new com.legend.sql.SqlExpr.BoolLit(negated), "value",
                    VALUE)), false, new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null,
                    null, List.of(VALUE));
        }
        SqlQuery leftPlan = lowered.apply(with(lets, left));
        SqlQuery rightPlan = lowered.apply(with(lets, right));
        if (leftPlan.outputs().size() != l.columns().size() || rightPlan.outputs().size() != r.columns().size()) {
            return null;
        }
        // each column by the kind its plan's output decides where its declaration leaves it open (a String, a Number,
        // an Any: the reading an assert's judge gives a table's cells)
        SideEqualities.Compared compared = SideEqualities.tables(leftPlan,
                com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(l, leftPlan.outputs()), rightPlan,
                com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(r, rightPlan.outputs()), numbers);
        return compared == null ? null : answer(compared, negated);
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
        return t == Type.Primitive.NUMBER || t == Type.Primitive.DATE || PlatformTypes.isAny(t)
                || PlatformTypes.isMapCarrier(t) || side.info().multiplicity().isMany();
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
