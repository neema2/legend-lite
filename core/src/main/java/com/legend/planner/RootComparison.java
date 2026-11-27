// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Multiplicity;
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
 * <p>Not planned here, each the plain comparison it was: two values of known scalar types (one type: the database's
 * {@code =}; two types: whatever the door that admitted the code made of it, a compile error on lite's own);
 * {@code eq}, which is identity; two tables.
 */
final class RootComparison {

    private RootComparison() {
    }

    /** {@code body}'s comparison planned over its two sides ({@code lowered}: a body's plan, the lets it reads and
     *  one side), or null where its last statement is no comparison of two whole sides, or the sides have no typed
     *  comparison (the statement is then lowered as any expression is). */
    static @Nullable SqlQuery plan(List<TypedSpec> body, Function<List<TypedSpec>, SqlQuery> lowered,
            ModelContext ctx) {
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
        if (Type.isRelation(left.info().type()) || Type.isRelation(right.info().type())) {
            return null;
        }
        if (known(left.info().type()) && known(right.info().type())) {
            // two known types: one type, two values are the database's plain comparison; two types, the comparison is
            // whatever the door that admitted the code made of it (refused on lite's own, the database's by value
            // on legend-engine's), which stands as it is lowered
            boolean oneType = java.util.Objects.equals(SideEqualities.typedClass(left.info().type()),
                    SideEqualities.typedClass(right.info().type()));
            if (!oneType || one(left) && one(right)) {
                return null;
            }
        }
        List<TypedSpec> lets = body.subList(0, body.size() - 1);
        SqlQuery leftPlan = lowered.apply(with(lets, left));
        SqlQuery rightPlan = lowered.apply(with(lets, right));
        SideEqualities.Pair pair = SideEqualities.pair(left, kind(left, leftPlan), leftPlan, right,
                kind(right, rightPlan), rightPlan, ctx);
        SideEqualities.Compared compared = pair == null ? null : pair.compared(leftPlan, rightPlan, false);
        if (compared == null) {
            return null;
        }
        OutputCol out = new OutputCol("value", SqlType.Scalar.BOOLEAN, false);
        SqlSelect row = new SqlSelect(List.of(new SqlSelect.Projection(compared.holds(!negated), "value", out)), false,
                compared.from(), null, List.of(), null, null, List.of(), null, null, List.of(out));
        return SqlWith.prepend(compared.ctes(), row);
    }

    /** Whether {@code t} names its values' type: a primitive that is no supertype of others, or an enumeration. */
    private static boolean known(Type t) {
        return t instanceof Type.Primitive p && p != Type.Primitive.NUMBER && p != Type.Primitive.DATE
                || t instanceof Type.PrecisionDecimal || t instanceof Type.EnumType;
    }

    private static boolean one(TypedSpec side) {
        return Multiplicity.Bounded.ONE.equals(side.info().multiplicity());
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
