// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;

import java.util.Set;

/**
 * A USER FUNCTION GENERIC OVER RELATIONS (the user, 2026-10-26: a library of timeseries functions written in Pure over
 * any table -- docs/SEMANTICS_REGISTER.md S112): one whose signature reads a type parameter inside a relation or a column
 * specification -- {@code Relation<T>}, {@code Relation<T+R>}, {@code ColSpec<X⊆T>}, {@code SortInfo<Z⊆T>},
 * {@code _Window<T>}, an {@code AggColSpec} over {@code T}'s rows. legend-engine refuses type parameters in a user
 * function ("Type and/or multiplicity parameters are not authorized in Legend Engine"); legend-lite admits these and
 * expands each call into the function's body before typing it, as the engine's
 * {@code <<functionType.NormalizeRequiredFunction>>} is ({@link Overloads}): the body is checked at each call, over that
 * call's tables, never once against its parameters -- the typer's schema algebra solves concrete relations, not open
 * ones -- so the eager check of every body ({@code Compiler.compileAllBodies}) leaves it to its calls.
 */
public final class RelationGenerics {

    /** The types a relation's type parameter is read through: a relation, and the column specifications, sorts and
     *  windows over its rows. */
    private static final Set<String> RELATIONAL = Set.of(
            PlatformTypes.RELATION, PlatformTypes.COL_SPEC, PlatformTypes.COL_SPEC_ARRAY, PlatformTypes.FUNC_COL_SPEC,
            PlatformTypes.FUNC_COL_SPEC_ARRAY, PlatformTypes.AGG_COL_SPEC, PlatformTypes.AGG_COL_SPEC_ARRAY,
            PlatformTypes.WINDOW, "meta::pure::functions::relation::SortInfo");

    private RelationGenerics() {
    }

    /** Whether {@code f} is a user function generic over relations: bodied, and a type parameter read inside a relation
     *  or a column specification of its signature. */
    public static boolean of(TypedFunction f) {
        if (f.isNative() || f.body().isEmpty() || f.typeParameters().isEmpty()) {
            return false;
        }
        return f.parameters().stream().anyMatch(p -> reads(p.type(), false)) || reads(f.returnType(), false);
    }

    /** Whether {@code t} reads a type variable within a relation-typed position ({@code within}: already inside one). */
    private static boolean reads(Type t, boolean within) {
        return switch (t) {
            case Type.TypeVar ignored -> within;
            // schema algebra is a relation's own: its operands are open schemas
            case Type.SchemaAlgebra ignored -> true;
            case Type.GenericType g -> {
                boolean inside = within || RELATIONAL.contains(g.rawFqn());
                yield g.arguments().stream().anyMatch(a -> reads(a, inside));
            }
            case Type.FunctionType f -> f.params().stream().anyMatch(p -> reads(p.type(), within))
                    || reads(f.result().type(), within);
            case Type.RelationType r -> r.columns().stream().anyMatch(c -> reads(c.type(), true));
            case Type.Primitive ignored -> false;
            case Type.PrecisionDecimal ignored -> false;
            case Type.ClassType ignored -> false;
            case Type.EnumType ignored -> false;
        };
    }
}
