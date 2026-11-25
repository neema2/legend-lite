// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.FunctionId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * THE ONE EQUALITY'S TYPE RULE (B3 step 5; the user's ruling of 2026-11-24, docs/records/compat-stage-b-plan-2026-11-24.md,
 * "The one equality"): two values are equal only when they have the same type and the same value. A comparison whose
 * two sides' types are known when the query is compiled and differ is a mistake -- {@code 1 == 1.0}, an enumeration
 * against a String, a day against a date-time, two unrelated classes -- and the typer marks it
 * ({@link TypedCrossTypeComparison}). Ordering is held to the same rule.
 *
 * <p>The type of a side, as the rule reads it: Integer, Float and Decimal each a type (a Decimal of any precision is a
 * Decimal); String; Boolean; Byte; a day ({@code StrictDate}); a date-time; a time; each enumeration; Variant; a class,
 * crossing another only when no value can be of both (neither is a subtype of the other and they share no subclass).
 * Known only when the query runs, and never marked: {@code Number}, the abstract {@code Date}, {@code LatestDate},
 * {@code Any}, the {@code Enum} class, a type variable, a generic, a function, a relation. Each comparison is known by its declaration's identity, never its name.
 */
public final class EqualityClasses {

    private EqualityClasses() {
    }

    /** A comparison whose two sides' types differ: its kind, its operator as a message writes it, and the two types. */
    public record Crossing(TypedCrossTypeComparison.Kind kind, String operator, Type left, Type right) {
    }

    /** A comparison the rule reads: its kind and its operator as a message writes it. */
    private record Comparison(TypedCrossTypeComparison.Kind kind, String operator) {
    }

    /** Each comparison the rule reads, by its declaration's identity. */
    private static final Map<FunctionId, Comparison> COMPARISONS = comparisons();

    private static Map<FunctionId, Comparison> comparisons() {
        java.util.Map<FunctionId, Comparison> all = new java.util.LinkedHashMap<>();
        put(all, Pure.AT_BOOLEAN_EQUAL, TypedCrossTypeComparison.Kind.EQUALITY, "==");
        put(all, Pure.AT_BOOLEAN_EQ, TypedCrossTypeComparison.Kind.EQUALITY, "eq");
        put(all, Pure.AT_COLLECTION_IN, TypedCrossTypeComparison.Kind.MEMBERSHIP, "in");
        put(all, List.of(FunctionId.of(Pure.CONTAINS__ANY_MANY__ANY_1)), TypedCrossTypeComparison.Kind.MEMBERSHIP,
                "contains");
        put(all, Pure.AT_COLLECTION_INDEX_OF, TypedCrossTypeComparison.Kind.INDEX, "indexOf");
        put(all, Pure.AT_BOOLEAN_LESS_THAN, TypedCrossTypeComparison.Kind.ORDERING, "<");
        put(all, Pure.AT_BOOLEAN_LESS_THAN_EQUAL, TypedCrossTypeComparison.Kind.ORDERING, "<=");
        put(all, Pure.AT_BOOLEAN_GREATER_THAN, TypedCrossTypeComparison.Kind.ORDERING, ">");
        put(all, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL, TypedCrossTypeComparison.Kind.ORDERING, ">=");
        put(all, Pure.AT_LANG_COMPARE, TypedCrossTypeComparison.Kind.ORDERING, "compare");
        put(all, List.of(FunctionId.of(Pure.MAX__NUMBER_1__NUMBER_1), FunctionId.of(Pure.MAX__DATE_1__DATE_1)),
                TypedCrossTypeComparison.Kind.ORDERING, "max");
        put(all, List.of(FunctionId.of(Pure.MIN__NUMBER_1__NUMBER_1), FunctionId.of(Pure.MIN__DATE_1__DATE_1)),
                TypedCrossTypeComparison.Kind.ORDERING, "min");
        return java.util.Collections.unmodifiableMap(all);
    }

    private static void put(java.util.Map<FunctionId, Comparison> all, List<FunctionId> ids,
            TypedCrossTypeComparison.Kind kind, String operator) {
        for (FunctionId id : ids) {
            all.put(id, new Comparison(kind, operator));
        }
    }

    /** The crossing {@code chosen} applied to {@code args} makes, if it is a comparison whose sides' types are known and
     *  differ. */
    public static Optional<Crossing> crossing(TypedFunction chosen, List<TypedSpec> args, ModelContext ctx) {
        if (chosen.definition() == null || args.size() != 2) {
            return Optional.empty();
        }
        Comparison comparison = COMPARISONS.get(chosen.id());
        if (comparison == null) {
            return Optional.empty();
        }
        Type l = args.get(0).info().type();
        Type r = args.get(1).info().type();
        Optional<Type> lc = of(l);
        Optional<Type> rc = of(r);
        if (lc.isEmpty() || rc.isEmpty() || lc.get().equals(rc.get())) {
            return Optional.empty();
        }
        if (l instanceof Type.ClassType lt && r instanceof Type.ClassType rt && related(lt.fqn(), rt.fqn(), ctx)) {
            return Optional.empty();
        }
        return Optional.of(new Crossing(comparison.kind(), comparison.operator(), lc.get(), rc.get()));
    }

    /** Whether one value can be of both classes: one is the other's subtype, or they share a subclass (B3 step 5a's
     *  review, M4, 2026-11-25: a class with two supertypes). */
    private static boolean related(String l, String r, ModelContext ctx) {
        if (ctx.isSubtype(l, r) || ctx.isSubtype(r, l)) {
            return true;
        }
        java.util.Set<String> below = ctx.subtree(r);
        return ctx.subtree(l).stream().anyMatch(below::contains);
    }

    /** {@code t}'s type as the rule reads it, or empty where it is known only when the query runs. */
    public static Optional<Type> of(Type t) {
        return switch (t) {
            case Type.Primitive p -> switch (p) {
                case INTEGER, FLOAT, DECIMAL, STRING, BOOLEAN, BYTE, STRICT_DATE, DATE_TIME, STRICT_TIME -> Optional.of(p);
                case NUMBER, DATE, LATEST_DATE -> Optional.empty();
            };
            case Type.PrecisionDecimal ignored -> Optional.of(Type.Primitive.DECIMAL);
            case Type.EnumType e -> Optional.of(e);
            case Type.ClassType c -> PlatformTypes.isAny(c) || PlatformTypes.isNil(c) || PlatformTypes.isTdsNull(c)
                    || PlatformTypes.isEnumClass(c) ? Optional.empty() : Optional.of(c);
            case Type.TypeVar ignored -> Optional.empty();
            case Type.GenericType ignored -> Optional.empty();
            case Type.FunctionType ignored -> Optional.empty();
            case Type.RelationType ignored -> Optional.empty();
            case Type.SchemaAlgebra ignored -> Optional.empty();
        };
    }
}
