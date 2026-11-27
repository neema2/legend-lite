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
import com.legend.lowering.WholeComparisons;
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
 * <p>{@code |hasSameElements($a, $b)} is planned here too ({@code Pure.Lite.HAS_SAME_ELEMENTS}; the user, 2026-11-27): the
 * same two sides, their elements or rows compared in ANY order, each as often on one side as on the other. It has no
 * other form: where {@code ==} would fall back to the plain comparison, it is refused by name.
 *
 * <p>THE PRODUCT'S OWN RULE, AND THE LOWERING'S TO ASK FOR ({@link WholeComparisons}): this class knows nothing of
 * who is asking. The lowering asks here only for a comparison the door's table named no rule for, by the one lookup
 * it has ({@code lowering.Scalars.doorsRule}); a comparison a compatibility door named a rule for is lowered by that
 * rule and never reaches this class (the user, 2026-11-27: "We shouldn't land with the planner knowing editions";
 * until the closing batch this class read which number rule the door's table had named, and stood aside by it).
 */
final class RootComparison {

    private RootComparison() {
    }

    /** {@code equal} (a {@code ==} or a {@code hasSameElements} of two sides; under {@code negated}, its {@code !})
     *  planned over its two sides ({@code lowered}: a body's plan, the lets it reads and one side), reading
     *  {@code lets}; or null where the sides have no whole comparison (two single values, two known types): the
     *  comparison is then lowered as any expression is. */
    private static @Nullable SqlQuery plan(List<TypedSpec> lets, TypedNativeCall equal, boolean negated,
            java.util.Collection<TypedSpec> seeded, Function<List<TypedSpec>, SqlQuery> lowered, ModelContext ctx) {
        if (equal.args().size() != 2) {
            return null;
        }
        // hasSameElements(a, b): the two sides' elements, or two tables' rows, in ANY order (Pure.Lite.HAS_SAME_ELEMENTS)
        boolean anyOrder = Pure.AT_LEGEND_LITE_HAS_SAME_ELEMENTS.contains(equal.callee().id());
        if (!anyOrder && !Pure.AT_BOOLEAN_EQUAL.contains(equal.callee().id())) {
            return null;
        }
        TypedSpec left = equal.args().get(0);
        TypedSpec right = equal.args().get(1);
        List<TypedSpec> body = with(lets, equal);
        if (body.stream().anyMatch(com.legend.compiler.spec.UserCallInliner::drawsAfresh)
                || seeded.stream().anyMatch(com.legend.compiler.spec.UserCallInliner::drawsAfresh)) {
            // a value drawn afresh where it is written (random(), generateGuid()): planned as two sides, a let both
            // sides read would be drawn twice (the outside audit of 2026-11-27): the plain comparison, which reads
            // it once or refuses. The lets the environment seeds are read too (the third review, L3: a seeded let
            // is as much the sides' as one the statement binds)
            return unplanned(anyOrder, "a value drawn afresh where it is written (random, generateGuid) would be"
                    + " drawn once for each side");
        }
        if (table(left) || table(right)) {
            return tables(left, right, lets, negated, lowered, anyOrder);
        }
        if (anyOrder) {
            // the equality rule: different types never compare, a compile error naming the two -- the type-checker's
            // own question (two unrelated classes too: the third review of 2026-11-27, M6)
            java.util.Optional<Type[]> two = com.legend.compiler.spec.EqualityClasses.neverEqual(valueType(left),
                    valueType(right), ctx);
            if (two.isPresent()) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                        "hasSameElements compares elements of one type, and its sides hold "
                                + two.get()[0].typeName() + " and " + two.get()[1].typeName()
                                + ": two types are never equal -- convert one side");
            }
        }
        if (known(valueType(left)) && known(valueType(right))
                && !java.util.Objects.equals(SideEqualities.typedClass(valueType(left)),
                        SideEqualities.typedClass(valueType(right)))
                // (an Integer side against a Decimal side is one comparison, by value: two exact numbers)
                && !(SideEqualities.exactNumber(valueType(left))
                        && SideEqualities.exactNumber(valueType(right)))) {
            // two known types: the comparison is whatever the door that admitted the code made of it (refused on
            // lite's own, the database's by value on legend-engine's), which stands as it is lowered
            return null;
        }
        if (!anyOrder && !whole(left) && !whole(right)) {
            // two values an expression compares exactly on every database: the plain comparison
            return null;
        }
        if (!anyOrder) {
            refuseUnordered(left, right, lets, "lists");
        }
        SqlQuery leftPlan = lowered.apply(with(lets, left));
        SqlQuery rightPlan = lowered.apply(with(lets, right));
        SideEqualities.Pair pair = SideEqualities.pair(left, kind(left, leftPlan), leftPlan, right,
                kind(right, rightPlan), rightPlan, ctx);
        SideEqualities.Compared compared = pair == null ? null : pair.compared(leftPlan, rightPlan, anyOrder);
        return compared == null ? unplanned(anyOrder, "its sides (" + valueType(left).typeName() + " and "
                + valueType(right).typeName() + ") have no typed comparison") : answer(compared, negated);
    }

    /**
     * THE PLANNER'S ANSWERS FOR {@code body}'S COMPARISONS OF TWO WHOLE SIDES ({@link WholeComparisons}), which the
     * lowering asks for the comparisons the product's own rule lowers: the body's last statement
     * ({@code asTheStatement}: this class's statement is the statement), and every {@code ==} and
     * {@code hasSameElements} met inside an expression -- an {@code if}'s condition, an operand of {@code &&},
     * {@code ||} or {@code !}, an assert's condition, a column's value ({@code insideAnExpression}: the same
     * statement, which the lowering reads as one value). One code answers both, and judges an assert of the two
     * sides: the comparison answers the same wherever it stands (the user, 2026-11-27: "why cant we do the work we
     * promised to get to one equality?"). Until then the comparison inside an expression was the database's own
     * comparison of two list values (two tables' column names and row order unread, two maps' entries by the order
     * they were written in, a Decimal list by its text where a list is JSON, a database error where a database has
     * no lists: measured 2026-11-27).
     *
     * <p>Inside an expression a side is planned only where it reads nothing of the row or element the comparison
     * stands in: the statement's own lets ({@code body}'s statements before its last), the lets the environment
     * seeds ({@code seeded}) and the query's parameters ({@code parameters}) -- never a name some lambda of the
     * statement binds too (a name bound twice is not told apart here: left to the plain comparison). The body's last
     * statement stands in no lambda, so it has no such test.
     *
     * @param lowered a body's plan: the lets it reads and one side, lowered as the statement it is
     */
    static WholeComparisons over(List<TypedSpec> body, java.util.Map<String, TypedSpec> seeded,
            java.util.Set<String> parameters, Function<List<TypedSpec>, SqlQuery> lowered, ModelContext ctx) {
        List<TypedSpec> lets = body.isEmpty() ? List.of() : List.copyOf(body.subList(0, body.size() - 1));
        java.util.Set<String> readable = new java.util.LinkedHashSet<>(seeded.keySet());
        readable.addAll(parameters);
        for (TypedSpec let : lets) {
            if (let instanceof com.legend.compiler.spec.typed.TypedLet named) {
                readable.add(named.name());
            }
        }
        java.util.Set<String> bound = new java.util.LinkedHashSet<>();
        if (!body.isEmpty()) {
            lambdaBound(body.get(body.size() - 1), bound);
        }
        readable.removeAll(bound);
        java.util.Collection<TypedSpec> seededValues = List.copyOf(seeded.values());
        return new WholeComparisons() {
            @Override
            public @Nullable SqlQuery asTheStatement(TypedNativeCall comparison, boolean negated) {
                return plan(lets, comparison, negated, seededValues, lowered, ctx);
            }

            @Override
            public @Nullable SqlQuery insideAnExpression(TypedNativeCall comparison) {
                if (comparison.args().size() != 2) {
                    return null;
                }
                boolean anyOrder = Pure.AT_LEGEND_LITE_HAS_SAME_ELEMENTS.contains(comparison.callee().id());
                if (!anyOrder && !wholeOrTable(comparison.args().get(0)) && !wholeOrTable(comparison.args().get(1))) {
                    return null;
                }
                if (!readable.containsAll(com.legend.compiler.spec.typed.FreeVars.of(comparison))) {
                    // a side reads the row or the element the comparison stands in
                    return null;
                }
                return plan(lets, comparison, false, seededValues, lowered, ctx);
            }
        };
    }

    /** Every name a lambda or a match beneath {@code n} binds. */
    private static void lambdaBound(TypedSpec n, java.util.Set<String> out) {
        switch (n) {
            case com.legend.compiler.spec.typed.TypedLambda l -> out.addAll(l.parameters());
            case com.legend.compiler.spec.typed.TypedMatch m -> {
                out.add(m.param());
                m.extraParam().ifPresent(out::add);
            }
            case com.legend.compiler.spec.typed.TypedMatchRuntime mr -> {
                mr.arms().forEach(arm -> out.add(arm.param()));
                mr.extraParam().ifPresent(out::add);
            }
            default -> { }
        }
        for (TypedSpec c : n.children()) {
            lambdaBound(c, out);
        }
    }

    /** {@code lowerer}, a test body's ({@code BodyPlanner.lowererFor}), handed this class's answers for the
     *  comparisons of two whole sides it meets (the body's last statement, an assert's condition, an {@code if}'s):
     *  each side lowered by a lowerer made as it was. A test body declares no parameter. */
    static com.legend.lowering.Lowerer asking(com.legend.lowering.Lowerer lowerer, List<TypedSpec> body, PlanEnv env,
            ModelContext ctx, boolean identity) {
        return lowerer.withWholeComparisons(over(body, env.queryLets(), java.util.Set.of(),
                side -> BodyPlanner.lowererFor(side, env, ctx, identity).lower(
                        com.legend.lowering.SeedableLets.withSeedableLetPrefix(side, env.queryLets(), ctx)), ctx));
    }

    /**
     * The type of {@code side}'s values: its own; or, where it is ROWS READ AS VALUES, their one column's. A store's
     * rows mapped to one value each ({@code p::Party.all()->map(p | $p.name)}) reach the planner, once the store is
     * resolved, as a relation of one column and many rows: a list of that column's values, no table (measured
     * 2026-11-27: read as a table against a list it had no comparison here, and took the plain one, in the order
     * the rows arrived).
     */
    private static Type valueType(TypedSpec side) {
        Type t = side.info().type();
        return side.info().multiplicity().isMany() && Type.schemaView(t) instanceof Type.RelationType rows
                && rows.columns().size() == 1 ? rows.columns().get(0).type() : t;
    }

    /** Whether {@code side} is a table: relation-typed, and no rows read as values ({@link #valueType}). */
    private static boolean table(TypedSpec side) {
        return Type.isRelation(side.info().type()) && valueType(side) == side.info().type();
    }

    private static boolean wholeOrTable(TypedSpec side) {
        return Type.isRelation(side.info().type()) || whole(side);
    }

    /** A statement this class does not plan: the plain comparison for {@code ==} (null: lowered as any expression
     *  is), a refusal by name for {@code hasSameElements}, which has no other form. */
    private static @Nullable SqlQuery unplanned(boolean anyOrder, String why) {
        if (anyOrder) {
            throw new com.legend.error.NotImplementedException("hasSameElements: " + why);
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
     * by), in order. A side whose order the query does not define is refused by name: {@code hasSameElements} is the
     * comparison of rows in any order. Two tables whose columns differ in name or number are never equal, a fact of their
     * types: answered with no read. Null (the statement is lowered as any expression is) where a side is no table,
     * a table's columns exist only when the query runs, or a column has no typed comparison.
     */
    private static @Nullable SqlQuery tables(TypedSpec left, TypedSpec right, List<TypedSpec> lets, boolean negated,
            Function<List<TypedSpec>, SqlQuery> lowered, boolean anyOrder) {
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
        if (!anyOrder) {
            refuseUnordered(left, right, lets, "tables");
        }
        // each column by the kind its plan's output decides where its declaration leaves it open (a String, a Number,
        // an Any: the reading an assert's judge gives a table's cells); hasSameElements: the rows as a multiset
        SideEqualities.Compared compared = SideEqualities.tables(leftPlan,
                com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(l, leftPlan.outputs()), rightPlan,
                com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(r, rightPlan.outputs()), anyOrder);
        return compared == null ? unplanned(anyOrder, "a column of its tables has no typed comparison")
                : answer(compared, negated);
    }

    /**
     * {@code ==} compares two sides' rows or elements IN ORDER, as {@code assertEquals} does, and a side whose order
     * the query does not define (a store's rows, a DISTINCT, a grouping; a list mapped from them) has none to
     * compare: refused, naming the two fixes, by the rule the assert refuses it by ({@code OrderedAsserts
     * .unorderedSide}: with at most one value on either side an ordered and an order-free comparison agree). The
     * user, 2026-11-27: strict {@code ==}, and {@code hasSameElements} for any order; for one afternoon an unsorted
     * table was compared as a multiset here, one operator with two meanings. Never by the order a database happened
     * to return them in (the outside audit of 2026-11-27, MEDIUM 7; a LIST side too since the third review, M1:
     * {@code p::Party.all()->map(p | $p.name) == ['A', 'B']} had been compared as the rows arrived).
     */
    private static void refuseUnordered(TypedSpec left, TypedSpec right, List<TypedSpec> lets, String what) {
        int side = com.legend.compiler.spec.OrderedAsserts.unorderedSide(left, right, lets);
        if (side >= 0) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                    "== compares two " + what + " in order, and its " + (side == 0 ? "first" : "second")
                            + " side's order is not defined (a store or frame read with no sort, or past a grouping, a"
                            + " join or a union): the database's arrival order would decide the answer -- sort both"
                            + " sides, or say hasSameElements when the order does not matter");
        }
    }

    /** Whether {@code t} names its values' type: a primitive that is no supertype of others, or an enumeration. */
    private static boolean known(Type t) {
        return t instanceof Type.Primitive p && p != Type.Primitive.NUMBER && p != Type.Primitive.DATE
                || t instanceof Type.PrecisionDecimal || t instanceof Type.EnumType;
    }

    /** Whether {@code side} is compared as a whole side, never as a value inside an expression: a computed list
     *  (a database may hold none as a value, or hold it as text) or a map. ONE value is no whole side, whatever its
     *  type (below). A side written in the query (a literal, a collection of literals, the empty list) is a value
     *  everywhere: the plain comparison reads it exactly. */
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
        Type t = valueType(side);
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
