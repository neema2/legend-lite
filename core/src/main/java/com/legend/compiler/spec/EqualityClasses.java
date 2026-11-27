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
 * {@code Any}, the {@code Enum} class, the SQL null's sentinel in both spellings ({@code TDSNull}, {@code SQLNull}), a type variable, a generic, a function, a relation. Each comparison is known by its declaration's identity, never its name.
 */
public final class EqualityClasses {

    private EqualityClasses() {
    }

    /** A comparison whose two sides' types differ: its kind, its operator as a message writes it, and the two types. */
    public record Crossing(TypedCrossTypeComparison.Kind kind, String operator, Type left, Type right) {
    }

    /**
     * {@code answer}, a comparison written in a FUNCTION's body, marked as the function's and not the query's: the
     * comparison without its source position. What the mark is for: a comparison of two values typed only by the
     * columns they read is refused where the query is planned ({@code lowering.HeldKinds}) only when the query's
     * author wrote it -- a function's author who declared a side {@code Number}, {@code Any} or the abstract
     * {@code Date} said the values are compared when the query runs, and a library may refuse such a pair itself, in
     * its own words (the time-series library's meeting of a line of days and a line of moments: found 2026-11-27, the
     * planning-time refusal pre-empted it). The lowering reads "no position" as "not the query's own" already (a
     * mapping's translation, a milestoning filter). An assert keeps its position (its failure's record names it), as
     * does anything that is no plain comparison (a refusal's record, a comparison kept for its call).
     */
    static TypedSpec notTheQuerys(TypedSpec answer) {
        if (!(answer instanceof com.legend.compiler.spec.typed.TypedNativeCall c) || c.pos() == null
                || c.callee().definition() == null) {
            return answer;
        }
        Comparison comparison = COMPARISONS.get(c.callee().id());
        return comparison == null || comparison.kind() == TypedCrossTypeComparison.Kind.ASSERT ? answer
                : new com.legend.compiler.spec.typed.TypedNativeCall(c.callee(), c.args(), c.info(), null,
                        c.outsideStore());
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
        // an assert of two sides compares them: its first two arguments (a message and its values may follow)
        put(all, Pure.AT_ASSERTS_ASSERT_EQUALS, TypedCrossTypeComparison.Kind.ASSERT, "assertEquals");
        put(all, Pure.AT_ASSERTS_ASSERT_NOT_EQUALS, TypedCrossTypeComparison.Kind.ASSERT, "assertNotEquals");
        put(all, Pure.AT_ASSERTS_ASSERT_EQ, TypedCrossTypeComparison.Kind.ASSERT, "assertEq");
        put(all, Pure.AT_ASSERTS_ASSERT_NOT_EQ, TypedCrossTypeComparison.Kind.ASSERT, "assertNotEq");
        put(all, Pure.AT_ASSERTS_ASSERT_IS, TypedCrossTypeComparison.Kind.ASSERT, "assertIs");
        put(all, Pure.AT_ASSERTS_ASSERT_IS_NOT, TypedCrossTypeComparison.Kind.ASSERT, "assertIsNot");
        put(all, Pure.AT_ASSERTS_ASSERT_SAME_ELEMENTS, TypedCrossTypeComparison.Kind.ASSERT, "assertSameElements");
        put(all, Pure.AT_ASSERTS_ASSERT_CONTAINS, TypedCrossTypeComparison.Kind.ASSERT, "assertContains");
        put(all, Pure.AT_ASSERTS_ASSERT_NOT_CONTAINS, TypedCrossTypeComparison.Kind.ASSERT, "assertNotContains");
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
        return !compares(chosen, args) ? Optional.empty()
                : crossing(chosen, args.get(0).info().type(), args.get(1).info().type(), ctx);
    }

    /** Whether {@code chosen} applied to {@code args} is a comparison the rule reads: of its two arguments, or an
     *  assert's of its first two. */
    private static boolean compares(TypedFunction chosen, List<TypedSpec> args) {
        Comparison comparison = chosen.definition() == null ? null : COMPARISONS.get(chosen.id());
        return comparison != null && (args.size() == 2
                || args.size() > 2 && comparison.kind() == TypedCrossTypeComparison.Kind.ASSERT);
    }

    /** {@link #crossing} by the types the two sides' values HOLD, where an upcast written over a side hides it:
     *  {@code 1->cast(@Number)} is the Integer it was, {@code 'a'->cast(@Any)} the String -- a widening cast is the
     *  value as it is. So a comparison one side of which is declared {@code Number}, the abstract {@code Date} or
     *  {@code Any} by such a cast is still known to be between two types (the equality rule's run-time item: equal
     *  means the same type and the same value). */
    public static Optional<Crossing> heldCrossing(TypedFunction chosen, List<TypedSpec> args, ModelContext ctx) {
        return !compares(chosen, args) ? Optional.empty()
                : crossing(chosen, held(args.get(0)), held(args.get(1)), ctx);
    }

    private static Type held(TypedSpec side) {
        Type t = side.info().type();
        if (side instanceof com.legend.compiler.spec.typed.TypedCast c && !c.wire() && c.target().equals(t)
                && declaredOpen(t)) {
            // (a cast DOWN from Any names no held type either: its source's is Any, which has no class here)
            return held(c.source());
        }
        return t;
    }

    private static Optional<Crossing> crossing(TypedFunction chosen, Type l, Type r, ModelContext ctx) {
        Comparison comparison = COMPARISONS.get(chosen.id());
        if (comparison == null) {
            return Optional.empty();
        }
        return differing(l, r, ctx).map(d -> new Crossing(comparison.kind(), comparison.operator(), d[0], d[1]));
    }

    /**
     * The two types by which {@code l} and {@code r} differ, or empty where they are one type or one of them is known
     * only when the query runs. A PAIR, a LIST and a MAP are compared by the types INSIDE them (the edition program's
     * step 6, the plan's 6.3): {@code Pair<Integer, String>} against {@code Pair<Float, String>} differs as an
     * Integer does from a Float, so the answer names those two. Where several of the types inside differ, a pair of
     * two exact numbers (an Integer against a Decimal, which a setting may compare by value) is named only if no
     * other pair differs.
     */
    private static Optional<Type[]> differing(Type l, Type r, ModelContext ctx) {
        if (l instanceof Type.GenericType gl && r instanceof Type.GenericType gr) {
            if (!(PlatformTypes.isPairCarrier(l) && PlatformTypes.isPairCarrier(r)
                    || PlatformTypes.isListCarrier(l) && PlatformTypes.isListCarrier(r)
                    || PlatformTypes.isMapCarrier(l) && PlatformTypes.isMapCarrier(r))) {
                return Optional.empty();
            }
            Optional<Type[]> exactOnly = Optional.empty();
            for (int i = 0; i < gl.arguments().size(); i++) {
                Optional<Type[]> inside = differing(gl.arguments().get(i), gr.arguments().get(i), ctx);
                if (inside.isPresent() && !(exactNumber(inside.get()[0]) && exactNumber(inside.get()[1]))) {
                    return inside;
                }
                exactOnly = inside.isPresent() ? inside : exactOnly;
            }
            return exactOnly;
        }
        Optional<Type> lc = of(l);
        Optional<Type> rc = of(r);
        if (lc.isEmpty() || rc.isEmpty() || lc.get().equals(rc.get())) {
            return Optional.empty();
        }
        if (l instanceof Type.ClassType lt && r instanceof Type.ClassType rt && related(lt.fqn(), rt.fqn(), ctx)) {
            return Optional.empty();
        }
        return Optional.of(new Type[] {lc.get(), rc.get()});
    }

    private static boolean exactNumber(Type t) {
        return t == Type.Primitive.INTEGER || t == Type.Primitive.DECIMAL;
    }

    /** {@code call}, the application of {@code chosen} to {@code args}, as a comparison whose sides' types are known
     *  only where its function is used: a comparison the rule reads with a side declared {@code Number}, the abstract
     *  {@code Date}, {@code Any} or a type variable -- a type a parameter's argument fills in. Empty for any other
     *  call. */
    static Optional<com.legend.compiler.spec.typed.TypedRunTimeComparison> atRunTime(TypedSpec call,
            TypedFunction chosen, List<TypedSpec> args) {
        Comparison comparison = chosen.definition() == null || args.size() != 2 ? null : COMPARISONS.get(chosen.id());
        // (an assert is never kept for its call: its sides are judged when the test runs, by the types they hold)
        if (comparison == null || comparison.kind() == TypedCrossTypeComparison.Kind.ASSERT
                || !filledAtTheCall(args.get(0).info().type()) && !filledAtTheCall(args.get(1).info().type())) {
            return Optional.empty();
        }
        // compared when the query runs where the author declared a side so (Number, the abstract Date, Any); else the
        // sides are a type variable's values, which each call fixes
        return Optional.of(new com.legend.compiler.spec.typed.TypedRunTimeComparison(call, comparison.kind(),
                comparison.operator(), NumberLiterals.written(args.get(0)), NumberLiterals.written(args.get(1)),
                !declaredOpen(args.get(0).info().type()) && !declaredOpen(args.get(1).info().type()), null));
    }

    /** Whether {@code t} is a type its author declared open: any number, any date, anything -- values compared by
     *  the types they hold when the query runs. */
    static boolean declaredOpen(Type t) {
        return t == Type.Primitive.NUMBER || t == Type.Primitive.DATE
                || t instanceof Type.ClassType c && PlatformTypes.isAny(c);
    }

    private static boolean filledAtTheCall(Type t) {
        return t == Type.Primitive.NUMBER || t == Type.Primitive.DATE || t instanceof Type.TypeVar
                || t instanceof Type.ClassType c && PlatformTypes.isAny(c);
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
            // the SQL null, in both its spellings (a TDSNull instance and the bare reference's sqlNull() funnel), is
            // a presence test's sentinel, never a value's class: $v != TDSNull asks whether $v is there (step 4,
            // 2026-11-26: strict bodies met the funnel in a function, TdsNullTypingPinTest)
            case Type.ClassType c -> PlatformTypes.isAny(c) || PlatformTypes.isNil(c) || PlatformTypes.isTdsNull(c)
                    || PlatformTypes.isSqlNull(c) || PlatformTypes.isEnumClass(c) ? Optional.empty() : Optional.of(c);
            case Type.TypeVar ignored -> Optional.empty();
            case Type.GenericType ignored -> Optional.empty();
            case Type.FunctionType ignored -> Optional.empty();
            case Type.RelationType ignored -> Optional.empty();
            case Type.SchemaAlgebra ignored -> Optional.empty();
        };
    }
}
