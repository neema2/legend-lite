// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.FunctionId;
import com.legend.sql.SqlExpr;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A COMPARISON OF NUMBERS WHOSE TYPES ONLY THEIR COLUMNS GIVE (the edition program's step 6; the user's ruling of
 * 2026-11-27, "great do it"): a side declared {@code Number} or {@code Any} has no type the query's author wrote, but
 * the column it reads holds one, and the lowering knows it. Where the two sides' columns are of two number families
 * (an exact number's and a Float's) the comparison can never hold, so it is refused when the query is planned, as a
 * comparison of two known types is refused when it is compiled -- never answered false for every row. A written
 * number literal still reads as the column's type where nothing is lost ({@code $p.weight == 1} over a DOUBLE
 * column), and an Integer against a Decimal is by value (two exact numbers).
 *
 * <p>The product's own rule alone: a compatibility door's table names its own rule for these calls
 * ({@link NamedRule#NUMBERS_OF_THREE_TYPES}), under which such a comparison is the database's, as it was. Dates are
 * not read here: an abstract {@code Date} over a day's column against a date-time is the dates' own item.
 */
final class HeldNumbers {

    private HeldNumbers() {
    }

    /** A comparison this rule reads: its kind and its operator as a message writes it. */
    private record Read(TypedCrossTypeComparison.Kind kind, String operator) {
    }

    /** Each comparison read, by its declaration's identity: the ones a door's table has a row for
     *  ({@code edge.UpstreamTies}), so a door never reaches this check. */
    private static final Map<FunctionId, Read> READ = read();

    private static Map<FunctionId, Read> read() {
        Map<FunctionId, Read> all = new LinkedHashMap<>();
        put(all, Pure.AT_BOOLEAN_EQUAL, TypedCrossTypeComparison.Kind.EQUALITY, "==");
        put(all, Pure.AT_BOOLEAN_EQ, TypedCrossTypeComparison.Kind.EQUALITY, "eq");
        put(all, Pure.AT_COLLECTION_IN, TypedCrossTypeComparison.Kind.MEMBERSHIP, "in");
        put(all, Pure.AT_COLLECTION_CONTAINS, TypedCrossTypeComparison.Kind.MEMBERSHIP, "contains");
        put(all, Pure.AT_BOOLEAN_LESS_THAN, TypedCrossTypeComparison.Kind.ORDERING, "<");
        put(all, Pure.AT_BOOLEAN_LESS_THAN_EQUAL, TypedCrossTypeComparison.Kind.ORDERING, "<=");
        put(all, Pure.AT_BOOLEAN_GREATER_THAN, TypedCrossTypeComparison.Kind.ORDERING, ">");
        put(all, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL, TypedCrossTypeComparison.Kind.ORDERING, ">=");
        return java.util.Collections.unmodifiableMap(all);
    }

    private static void put(Map<FunctionId, Read> all, List<FunctionId> ids, TypedCrossTypeComparison.Kind kind,
            String operator) {
        for (FunctionId id : ids) {
            all.put(id, new Read(kind, operator));
        }
    }

    /** Refuses {@code call} where it compares, through a side typed only when the query runs, numbers of two
     *  families that the SQL its sides lowered to ({@code args}) names; any other call passes. */
    static void refuseAcrossFamilies(TypedNativeCall call, List<SqlExpr> args) {
        Read read = call.callee().definition() == null || call.args().size() != 2 || args.size() != 2 ? null
                : READ.get(call.callee().id());
        if (read == null) {
            return;
        }
        TypedSpec l = call.args().get(0);
        TypedSpec r = call.args().get(1);
        if (!open(l.info().type()) && !open(r.info().type())) {
            return;   // two declared types: the typer's rule answered it
        }
        String lk = MixedEncoding.kindOfValue(l, args.get(0));
        String rk = MixedEncoding.kindOfValue(r, args.get(1));
        if (lk == null || rk == null || MixedEncoding.numberTypePlace(lk) < 0 || MixedEncoding.numberTypePlace(rk) < 0
                || NumberFamilies.TWO_FAMILIES.canEqual(lk, rk)) {
            return;
        }
        if (readsAs(l, rk) || readsAs(r, lk)) {
            return;   // a written literal against a column of the other family: the database reads it as the column's
        }
        throw new TypedCrossTypeComparison(call, read.kind(), read.operator(), MixedEncoding.kindType(lk),
                MixedEncoding.kindType(rk), call.pos()).refusalOverColumns();
    }

    private static boolean open(Type t) {
        return t == Type.Primitive.NUMBER || t instanceof Type.ClassType c && PlatformTypes.isAny(c);
    }

    /** Whether {@code side} is number literals written in the query, each exact as a value of {@code kind}'s family:
     *  a whole number within a double's 53 bits against a Float; a Float literal that is such a whole number against
     *  an exact number. A Decimal literal states its type and is never read as another. */
    private static boolean readsAs(TypedSpec side, String kind) {
        if (side instanceof TypedCollection c) {
            return !c.elements().isEmpty() && c.elements().stream().allMatch(e -> readsAs(e, kind));
        }
        TypedSpec unsigned = side instanceof TypedNativeCall minus && minus.args().size() == 1
                && Pure.AT_MATH_MINUS.contains(minus.callee().id()) ? minus.args().get(0) : side;
        boolean toFloat = MixedEncoding.numberTypePlace(kind) == 1;
        return switch (unsigned) {
            case TypedCInteger i -> toFloat && i.value() instanceof Long whole && Math.abs(whole) <= (1L << 53);
            case TypedCFloat f -> !toFloat && f.value() == Math.rint(f.value()) && Math.abs(f.value()) <= (1L << 53);
            case TypedCDecimal d -> false;
            default -> false;
        };
    }
}
