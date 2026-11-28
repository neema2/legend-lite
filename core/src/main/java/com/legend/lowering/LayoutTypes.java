// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlType;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * SQL type of a pure VALUE, seeing through class layouts (structs)
 * before {@link PureSql} — the {@link Lowerer}'s layout-typing arm,
 * carrying its own recursion state: a CYCLE of stored properties
 * (Schema.database &lt;-&gt; Database.schemas in the store metamodel)
 * cannot spell a fixed-shape struct, so a revisited class rides
 * variant JSON like a heterogeneous LUB.
 */
final class LayoutTypes {

    /** The CANONICAL class-value layout resolver (ClassLayouts, supplied
     * by the driver); empty when no model rides along — class values
     * then keep hitting the loud walls. */
    private final Function<Type, Optional<List<Type.Column>>> classLayout;

    /** Whether a class FQN exists in the driving model (layoutless-LUB
     * detection). */
    private final Predicate<String> classExists;

    /** In-progress layout walk — the recursion cycle guard. */
    private final Set<String> walk = new HashSet<>();

    LayoutTypes(Function<Type, Optional<List<Type.Column>>> classLayout,
            Predicate<String> classExists) {
        this.classLayout = classLayout;
        this.classExists = classExists;
    }

    SqlType sqlTypeOf(Type t) {
        // Platform CARRIER types own their SQL shape (List = bare array,
        // Pair = struct, Map = MAP) — List's declared `values` property is
        // its pure-side surface, never a struct layout at the SQL boundary.
        if (PlatformTypes.isListCarrier(t)
                || PlatformTypes.isPairCarrier(t)
                || PlatformTypes.isMapCarrier(t)) {
            if (t instanceof Type.GenericType g && SideEqualities.holdsAnObjectInASlot(t)) {
                // a carrier that holds an OBJECT in a slot (a Pair of an object and a number, a list of objects, a
                // map to objects; step 6B part 2, 2026-11-28): the carrier's own shape over each slot's type as
                // THIS resolver gives it, a class by its layout -- asked of the layoutless mapping below, the slot
                // raised "no SQL type for Pure class", so such a value could be no statement's answer, no assert's
                // side, and its comparison read no layout
                if (PlatformTypes.isListCarrier(t)) {
                    return new SqlType.Array(sqlTypeOf(g.arguments().get(0)));
                }
                if (PlatformTypes.isPairCarrier(t)) {
                    return new SqlType.Struct(List.of(
                            new SqlType.Struct.Field("first", sqlTypeOf(g.arguments().get(0))),
                            new SqlType.Struct.Field("second", sqlTypeOf(g.arguments().get(1)))));
                }
                return new SqlType.Map(sqlTypeOf(g.arguments().get(0)), sqlTypeOf(g.arguments().get(1)));
            }
            return PureSql.type(t);
        }
        String walkKey = t instanceof Type.ClassType ct9 ? ct9.fqn()
                : t instanceof Type.GenericType g9 ? g9.rawFqn() : null;
        if (walkKey != null && !walk.add(walkKey)) {
            return SqlType.Scalar.JSON;
        }
        try {
            return sqlTypeOfUnguarded(t);
        } finally {
            if (walkKey != null) {
                walk.remove(walkKey);
            }
        }
    }

    private SqlType sqlTypeOfUnguarded(Type t) {
        return classLayout.apply(t)
                .<SqlType>map(cols -> new SqlType.Struct(
                        cols.stream().map(c -> {
                            SqlType ft = sqlTypeOf(c.type());
                            boolean many = c.multiplicity() instanceof
                                    Multiplicity.Bounded b && b.isMany();
                            return new SqlType.Struct.Field(c.name(),
                                    many ? new SqlType.Array(ft) : ft);
                        }).toList()))
                .orElseGet(() -> {
                    // A MODEL class with no layoutable properties reaching a
                    // VALUE boundary is a heterogeneous LUB (mixed instance
                    // kinds meeting at an abstract ancestor) — it travels as
                    // variant JSON, like Any. Non-model classes keep
                    // PureSql's loud wall.
                    if (t instanceof Type.ClassType ct
                            && !PlatformTypes.isVariant(ct)
                            && !PlatformTypes.isNil(ct)
                            && !PlatformTypes.isAny(ct)
                            && classExists.test(ct.fqn())) {
                        return SqlType.Scalar.JSON;
                    }
                    return PureSql.type(t);
                });
    }
}
