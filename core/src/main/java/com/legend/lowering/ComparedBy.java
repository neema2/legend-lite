// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * WHAT A VALUE IS COMPARED BY, WHATEVER HOLDS IT (step 6B part 2, 2026-11-28; the user: "reduce each side to what it
 * is compared by, then one instruction", and, of the grid: "Do we have a full matrix of types and container types?").
 * One recursion over a value's Pure type and the SQL type that holds it:
 * <ul>
 *   <li>a plain value: itself;</li>
 *   <li>an object, or a Pair: the class it actually is, then its keys by value, or its identity
 *       ({@link InstanceEquality#reduced} over {@link SideEqualities#leaves});</li>
 *   <li>a List: its elements, each reduced;</li>
 *   <li>a map: its entries sorted by key, each value reduced ({@link SideEqualities#entriesOf}) -- the order its
 *       entries were written in is no part of a map.</li>
 * </ul>
 * {@code ==}, {@code contains}, {@code in} and {@code indexOf} over containers read it, so a membership answers as
 * the equality does. Until it a membership over maps compared them in the order their entries were written, and one
 * over maps or lists that hold objects compared every field of each object (measured by the grid, 2026-11-28).
 */
final class ComparedBy {

    private ComparedBy() {
    }

    /**
     * One value's reduction: {@code by}, the expression over the value; {@code shape}, what two sides must share to
     * be compared (a Decimal and a whole number are one shape: two exact numbers); {@code asIs}, the value is
     * compared as it is (a plain value, a List of plain values) and {@code by} changes nothing.
     */
    record Reduction(UnaryOperator<SqlExpr> by, String shape, boolean asIs) {
    }

    /** The reduction of a value of Pure type {@code pure} held as {@code held}; null where it has no typed reading
     *  here (a date held as its text, a map inside a map, an object as a map's key, an object whose key has none). */
    static @com.legend.base.Nullable Reduction of(Type pure, SqlType held,
            Function<Type, @com.legend.base.Nullable EqualityKeys> keysOf, Supplier<String> fresh) {
        if (held instanceof SqlType.Map map) {
            if (!(pure instanceof Type.GenericType g) || !PlatformTypes.isMapCarrier(pure) || g.arguments().size() != 2
                    || !SideEqualities.scalar(map.key()) || map.value() instanceof SqlType.Map) {
                return null;
            }
            Reduction value = of(g.arguments().get(1), map.value(), keysOf, fresh);
            return value == null ? null : new Reduction(m -> SideEqualities.entriesOf(m, map, value.asIs() ? null
                    : value.by()), "map<" + exactOr(SideEqualities.leafClass(map.key())) + ", " + value.shape() + ">",
                    false);
        }
        if (held instanceof SqlType.Array list) {
            if (!(pure instanceof Type.GenericType g) || !PlatformTypes.isListCarrier(pure)
                    || g.arguments().size() != 1) {
                return null;
            }
            Reduction element = of(g.arguments().get(0), list.element(), keysOf, fresh);
            if (element == null) {
                return null;
            }
            return element.asIs() ? new Reduction(UnaryOperator.identity(), "list<" + element.shape() + ">", true)
                    : new Reduction(xs -> each(xs, list.element(), element, fresh), "list<" + element.shape() + ">",
                            false);
        }
        if (held instanceof SqlType.Struct layout) {
            if (!(InstanceEquality.userClass(pure) || PlatformTypes.isPairCarrier(pure))) {
                return null;
            }
            EqualityKeys keys = keysOf.apply(pure);
            com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(keys);
            List<SideEqualities.Leaf> leaves = new ArrayList<>();
            if (!SideEqualities.leaves(layout, keys, List.of(), leaves) || leaves.isEmpty()) {
                return null;
            }
            StringBuilder shape = new StringBuilder("{");
            for (SideEqualities.Leaf leaf : leaves) {
                shape.append(String.join(".", leaf.path())).append(": ")
                        .append(exactOr(SideEqualities.leafClass(SideEqualities.leafType(leaf)))).append("; ");
            }
            return new Reduction(v -> InstanceEquality.reduced(v, leaves, false), shape.append("}").toString(), false);
        }
        String plain = SideEqualities.leafClass(held);
        return plain == null ? null : new Reduction(UnaryOperator.identity(), exactOr(plain), true);
    }

    /** {@code xs}, a list of values held as {@code element}, as the list of each reduced. */
    static SqlExpr each(SqlExpr xs, SqlType element, Reduction by, Supplier<String> fresh) {
        if (by.asIs()) {
            return xs;
        }
        String y = fresh.get();
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, xs, new SqlExpr.Lambda(List.of(y),
                by.by().apply(SqlExpr.Column.of(null, y, element, true, com.legend.sql.OutputCol.Origin.DERIVED))));
    }

    /** The SQL type ONE value of {@code side} is held as: the type the SQL states for {@code value} (its elements',
     *  where {@code side} holds many), else its Pure type's own. */
    private static SqlType heldAs(com.legend.compiler.spec.typed.TypedSpec side, SqlExpr value, boolean many,
            Function<Type, SqlType> sqlTypeOf) {
        if (value.type() instanceof com.legend.sql.TypeFact.Typed stated) {
            if (!many) {
                return stated.type();
            }
            if (stated.type() instanceof SqlType.Array list) {
                return list.element();
            }
        }
        return sqlTypeOf.apply(side.info().type());
    }

    private static com.legend.error.NotImplementedException noTypedComparison(String what,
            com.legend.compiler.spec.typed.TypedSpec a, com.legend.compiler.spec.typed.TypedSpec b) {
        // (never the database's own comparison: of two maps it is by the order their entries were written in, of
        // containers that hold objects by every field of each)
        return new com.legend.error.NotImplementedException(what + " over " + a.info().type().typeName() + " and "
                + b.info().type().typeName() + ": these containers have no typed comparison here (a map inside a"
                + " map, an object as a map's key, a date held as its text, two shapes); not supported yet");
    }

    /**
     * TWO SINGLE CONTAINERS THE SAME ({@code ==}; at least one a map, or a List that holds maps or objects): the two
     * reduced, not distinct. Null where neither side is such a container; refused by name where they have no typed
     * comparison.
     */
    static @com.legend.base.Nullable SqlExpr sameOnes(com.legend.compiler.spec.typed.TypedNativeCall n,
            List<SqlExpr> sides, Function<Type, SqlType> sqlTypeOf,
            Function<Type, @com.legend.base.Nullable EqualityKeys> keysOf, Supplier<String> fresh) {
        com.legend.compiler.spec.typed.TypedSpec l = n.args().get(0);
        com.legend.compiler.spec.typed.TypedSpec r = n.args().get(1);
        if (!(container(l.info().type()) && container(r.info().type())) || l.info().multiplicity().isMany()
                || r.info().multiplicity().isMany()) {
            return null;
        }
        Reduction lBy = of(l.info().type(), heldAs(l, sides.get(0), false, sqlTypeOf), keysOf, fresh);
        Reduction rBy = of(r.info().type(), heldAs(r, sides.get(1), false, sqlTypeOf), keysOf, fresh);
        if (lBy == null || rBy == null || !lBy.shape().equals(rBy.shape())) {
            throw noTypedComparison("==", l, r);
        }
        return SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, lBy.by().apply(sides.get(0)), rBy.by().apply(sides.get(1)));
    }

    /**
     * TWO LISTS OF CONTAINERS THE SAME, IN ORDER ({@code ==} where a side holds many): the one "same values"
     * instruction over the two lists, each container reduced. Null where the elements are no such containers.
     */
    static @com.legend.base.Nullable SqlExpr sameMany(com.legend.compiler.spec.typed.TypedNativeCall n,
            List<SqlExpr> sides, Function<Type, SqlType> sqlTypeOf,
            Function<Type, @com.legend.base.Nullable EqualityKeys> keysOf, Supplier<String> fresh) {
        com.legend.compiler.spec.typed.TypedSpec l = n.args().get(0);
        com.legend.compiler.spec.typed.TypedSpec r = n.args().get(1);
        boolean lMany = l.info().multiplicity().isMany();
        boolean rMany = r.info().multiplicity().isMany();
        if (!(container(l.info().type()) && container(r.info().type())) || !(lMany || rMany)) {
            return null;
        }
        SqlType lHeld = heldAs(l, sides.get(0), lMany, sqlTypeOf);
        SqlType rHeld = heldAs(r, sides.get(1), rMany, sqlTypeOf);
        Reduction lBy = of(l.info().type(), lHeld, keysOf, fresh);
        Reduction rBy = of(r.info().type(), rHeld, keysOf, fresh);
        if (lBy == null || rBy == null || !lBy.shape().equals(rBy.shape())) {
            throw noTypedComparison("==", l, r);
        }
        return SqlExpr.Call.of(SqlFn.SAME_IN_ORDER,
                each(CollectionLanes.compacted(l, PureSql.asList(sides.get(0), lMany)), lHeld, lBy, fresh),
                each(CollectionLanes.compacted(r, PureSql.asList(sides.get(1), rMany)), rHeld, rBy, fresh));
    }

    /**
     * A CONTAINER AMONG CONTAINERS ({@code contains}, {@code in}) OR ITS PLACE ({@code indexOf}): the value and each
     * of the list's reduced, the membership or the position over the reduced values. Null where the elements are
     * no such containers.
     *
     * @param list   the list side and its lowered value
     * @param needle the value sought and its lowered value
     * @param place  whether the call asks the value's 0-based place ({@code indexOf}: -1 where it is none)
     */
    static @com.legend.base.Nullable SqlExpr among(com.legend.compiler.spec.typed.TypedSpec list, SqlExpr listValue,
            com.legend.compiler.spec.typed.TypedSpec needle, SqlExpr needleValue, boolean place,
            Function<Type, SqlType> sqlTypeOf, Function<Type, @com.legend.base.Nullable EqualityKeys> keysOf,
            Supplier<String> fresh) {
        if (!(container(list.info().type()) && container(needle.info().type()))
                || needle.info().multiplicity().isMany()) {
            return null;
        }
        boolean many = list.info().multiplicity().isMany();
        SqlType held = heldAs(list, listValue, many, sqlTypeOf);
        Reduction each = of(list.info().type(), held, keysOf, fresh);
        Reduction one = of(needle.info().type(), heldAs(needle, needleValue, false, sqlTypeOf), keysOf, fresh);
        if (each == null || one == null || !each.shape().equals(one.shape())) {
            throw noTypedComparison(place ? "indexOf" : "a membership", list, needle);
        }
        SqlExpr reduced = each(CollectionLanes.compacted(list, PureSql.asList(listValue, many)), held, each, fresh);
        SqlExpr sought = one.by().apply(needleValue);
        return place ? SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.COALESCE,
                SqlExpr.Call.of(SqlFn.LIST_POSITION, reduced, sought), new SqlExpr.IntLit(0)), new SqlExpr.IntLit(1))
                : SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Membership(sought, reduced), new SqlExpr.BoolLit(false));
    }

    /** A whole number and a Decimal are one shape, two exact numbers compared by their value. */
    private static @com.legend.base.Nullable String exactOr(@com.legend.base.Nullable String leafClass) {
        return "Integer".equals(leafClass) || "Decimal".equals(leafClass) ? "exact number" : leafClass;
    }

    /** Whether a value of type {@code t} is a CONTAINER whose own comparison a database gets wrong: a map (its
     *  entries in the order they were written), or a List that holds maps or objects (every field of each). */
    static boolean container(Type t) {
        return PlatformTypes.isMapCarrier(t) || PlatformTypes.isListCarrier(t) && t instanceof Type.GenericType g
                && g.arguments().size() == 1 && (container(g.arguments().get(0))
                        || InstanceEquality.userClass(g.arguments().get(0))
                        || SideEqualities.holdsAnObjectInASlot(g.arguments().get(0))
                        || PlatformTypes.isPairCarrier(g.arguments().get(0)));
    }
}
