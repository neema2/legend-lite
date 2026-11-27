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
 * A COMPARISON OF VALUES WHOSE TYPES ONLY THEIR COLUMNS GIVE (the edition program's step 6; the user's rulings of
 * 2026-11-27, "great do it" for numbers and "You can do the error way if it's only for lite mode" for dates): a side
 * declared {@code Number}, {@code Any} or the abstract {@code Date} has no type the query's author wrote, but the
 * column it reads holds one, and the lowering knows it. Where the two sides can never be equal -- an exact number's
 * column against a Float's, a day's against a date-time's -- the comparison is refused when the query is planned, as
 * a comparison of two known types is refused when it is compiled: never answered false for every row, never the
 * database's own conversion. An ordering is refused as an equality is, for numbers and for dates alike (the two
 * families' ruling, "keep ordering strict"; the user, 2026-11-27, of a day against a date-time: "We don't really
 * want date vs datetime to work for greater than/less than vs not for equal"). For one afternoon that day a day
 * against a date-time was refused for "the same" alone, to keep four of the time-series documents' queries
 * planning; their cause was a wrong type fact, not the rule: a truncation to the week or the month was recorded a
 * TIMESTAMP where the database holds a DATE ({@code sql.SqlTyping}), and a day typed only {@code Date} adjusted by
 * years became a date-time ({@code DateShifts}).
 *
 * <p>A written number literal still reads as the column's type where nothing is lost ({@code $p.weight == 1} over a
 * DOUBLE column), and an Integer against a Decimal is by value (two exact numbers). A written DAY does not read as a
 * date-time: a day is no instant, and reading it as midnight would invent a time and a zone its author did not
 * write ({@code $x.at == %2020-01-01} almost always means "on that day"; the fix is the instant, or the day of the
 * date-time).
 *
 * <p>Only a comparison the query's author wrote (one with a source position): a comparison lite writes itself (a
 * mapping's translation, a milestoning filter) is no mistake of theirs, and one written in a function's body is the
 * function's ({@code compiler.spec.EqualityClasses.notTheQuerys}: its author declared the side open, the word that
 * the values are compared when the query runs, and a library may refuse such a pair itself). And the product's own rule alone: a
 * compatibility door's table names its own rule for these calls ({@link NamedRule#UPSTREAMS_COMPARISON}), under
 * which such a comparison is the database's, as it was.
 */
final class HeldKinds {

    private HeldKinds() {
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
        // (the one overload the type-checker's rule reads, compiler.spec.EqualityClasses: the two tables name the
        // same comparisons, so one written in a function's body is marked the function's for every call read here)
        put(all, List.of(FunctionId.of(Pure.CONTAINS__ANY_MANY__ANY_1)), TypedCrossTypeComparison.Kind.MEMBERSHIP,
                "contains");
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

    /** Refuses {@code call} where it compares, through a side typed only when the query runs, values of two kinds
     *  never equal that the SQL its sides lowered to ({@code args}) names; any other call passes. */
    static void refuseAcrossKinds(TypedNativeCall call, List<SqlExpr> args) {
        Read read = call.callee().definition() == null || call.pos() == null || call.args().size() != 2
                || args.size() != 2 ? null : READ.get(call.callee().id());
        if (read == null) {
            return;
        }
        TypedSpec l = call.args().get(0);
        TypedSpec r = call.args().get(1);
        if (read.kind() != TypedCrossTypeComparison.Kind.ORDERING) {
            // a list holding both days and date-times, read through a map, a filter or an if, reaches the database
            // as one kind, so its days cannot be told from its date-times there (register S399): refused by name, as
            // both judges refuse it (measured 2026-11-27 on DuckDB: [%2020-01-01T00:00, %2020-01-02]->map(x|$x)
            // ->contains(%2020-01-02T00:00) answered true). A list written where it is compared is answered by each
            // element's written kind, rightly: not refused.
            if (!(l instanceof TypedCollection)) {
                SideEqualities.refuseMixedDays(l);
            }
            if (!(r instanceof TypedCollection)) {
                SideEqualities.refuseMixedDays(r);
            }
        }
        if (upcast(l) || upcast(r)) {
            // an upcast the query's author wrote (%2024-01-02->cast(@Date), 1->cast(@Number)) is the author's word
            // that the value is compared when the query runs, as a parameter declared so is: the type-checker's rule
            // answered it by the type the value holds (never equal to another type's, ordered by value). This rule is
            // for a type only a COLUMN gives. (Found by the round of 83367543e: planner.PlanMakerTest's
            // %9999-12-31->cast(@Date) > %2024-01-02T10:30:00, which a Date parameter of that value answers.)
            return;
        }
        String lk = MixedEncoding.kindOfValue(l, args.get(0));
        String rk = MixedEncoding.kindOfValue(r, args.get(1));
        if (lk == null || rk == null || NumberFamilies.TWO_FAMILIES.canEqual(lk, rk)) {
            return;
        }
        boolean numbers = MixedEncoding.numberTypePlace(lk) >= 0 && MixedEncoding.numberTypePlace(rk) >= 0;
        boolean dates = dayOrDateTime(lk) && dayOrDateTime(rk);
        if (!numbers && !dates) {
            return;   // (kinds this rule does not read: a text against a number through an Any, a partial date)
        }
        boolean aColumn = args.get(0) instanceof SqlExpr.Column || args.get(1) instanceof SqlExpr.Column;
        if (!open(l.info().type()) && !open(r.info().type()) && !(dates && aColumn)) {
            // two declared types: the typer's rule answered it. (A day against a date-time is read here even so
            // where a side is a COLUMN: a property declared the abstract Date reaches the lowering typed as its
            // column -- the store's resolution gives it the day or the date-time its column holds. Two written
            // values never are: a day and a date-time the author wrote as such were refused when the query was
            // compiled, and the two that reach here came through an upcast the author wrote and a let carried --
            // let d = %9999-12-31->cast(@Date); ... $d > %2024-01-02T10:30:00, planner.PlanMakerTest -- which is
            // the type-checker's to answer, as above.)
            return;
        }
        if (numbers && (readsAs(l, rk) || readsAs(r, lk))) {
            return;   // a written literal against a column of the other family: the database reads it as the column's
        }
        throw new TypedCrossTypeComparison(call, read.kind(), read.operator(), MixedEncoding.kindType(lk),
                MixedEncoding.kindType(rk), call.pos()).refusalOverColumns();
    }

    /** An upcast the author wrote over a value whose own type is known ({@code 1->cast(@Number)}): the
     *  type-checker's rule answered it by the type the value holds. An upcast of a value already typed only by its
     *  column ({@code $a.n->cast(@Number)}, {@code n} declared {@code Number}) says nothing new: this rule reads it
     *  as the column it is (the third review of 2026-11-27, M2: it had escaped the refusal the same query gets
     *  without the cast). */
    private static boolean upcast(TypedSpec side) {
        return side instanceof com.legend.compiler.spec.typed.TypedCast c && !c.wire()
                && !open(c.source().info().type());
    }

    private static boolean open(Type t) {
        return t == Type.Primitive.NUMBER || t == Type.Primitive.DATE
                || t instanceof Type.ClassType c && PlatformTypes.isAny(c);
    }

    /** Whether {@code kind} is a day's or a date-time's (at any written precision). */
    private static boolean dayOrDateTime(String kind) {
        String family = MixedEncoding.family(kind);
        return family.equals("StrictDate") || family.equals("DateTime");
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
