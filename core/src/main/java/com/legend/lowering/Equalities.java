// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;
import java.util.Objects;

/**
 * THE PRODUCT'S EQUALITY, IN ONE PLACE (the edition program's step 6, slice 6.4, piece a, 2026-11-27: one owner, no
 * behaviour change). What {@code ==} and {@code eq} lower to: the empty ladder, a tagged value or a mixed list on a
 * side, a partial date's precision, the wire operands, an enumeration's stored value, and where the comparison is
 * null-safe. It was the body of the scalar rules' entry for {@code equal} and {@code eq}, their private helpers, and
 * {@code NullSemantics.equalNullArms}; each is moved here as it stood.
 *
 * <p>Still elsewhere, to arrive with the pieces that rework them: an instance's equality ({@link InstanceEquality},
 * piece c), and the equality decisions of {@code contains}, {@code in} and {@code indexOf} (the scalar rules' own
 * arms and {@link TaggedLists}).
 */
final class Equalities {

    private Equalities() {
    }

    /** The rule of {@code equal} and {@code eq}. It is PRECISION-AWARE over dates: a partial-date literal (a year, a
     *  year-month) equals only a same-precision value: another literal's by the two precisions, a computed value's
     *  (carried as the text it prints) by the two texts; a column and any day or date-time is full-precision, never
     *  equal to one; same-precision partials compare as ISO-prefix strings. {@code eq} is strict equality, the {@code =} operator
     *  here. It takes NO optional-operand guards deliberately (T1.4): equal is a total native ({@code [] == []} is
     *  true; the guard would spell it false). The residual both-NULL divergence (SQL NULL where Pure says true) is
     *  the reference engine's own behaviour (a bare {@code =}). */
    static SqlExpr equal(TypedNativeCall n, List<SqlExpr> args) {
        return equal(n, args, NumberFamilies.TWO_FAMILIES);
    }

    /**
     * AN EQUALITY'S EMPTY SIDES, AS PURE ANSWERS THEM (the edition program's step 6; the user, 2026-11-27: "Yes do the
     * empty side fix", then "why cant we do the work we promised to get to one equality?"). Pure's {@code ==} is
     * two-valued: {@code [] == 'a'} is false and {@code [] == []} is true. A database's {@code =} over a side that
     * holds no value answers "no value", which is the same as false only where the comparison stands as a positive
     * condition ({@code Lowerer.asCondition}). So {@code compared}, the plain {@code =} of {@code n}'s two sides:
     * <ul>
     *   <li>BOTH sides may be empty: the null-safe equality, in every position -- two empties are equal, in a
     *       filter too (two plain optional columns were written so already, {@link #nullArms}). An enumeration's
     *       comparison is left to its own machinery (recorded);</li>
     *   <li>exactly ONE side may be empty, and the comparison is no positive condition: false where that side is
     *       empty, the comparison written once ({@code COALESCE(l = r, FALSE)});</li>
     *   <li>otherwise as it is.</li>
     * </ul>
     * Anything that is no plain {@code =} of two operands (a null-safe form, a list's or an instance's comparison,
     * a constant) is left as it is: it answers for its empty sides itself.
     */
    static SqlExpr emptySidesAsPure(TypedNativeCall n, SqlExpr compared, boolean positiveCondition) {
        if (!(compared instanceof SqlExpr.Call c) || c.fn() != SqlFn.EQUAL || c.args().size() != 2
                || n.args().size() != 2) {
            return compared;
        }
        boolean first = mayHoldNoValue(n.args().get(0), c.args().get(0));
        boolean second = mayHoldNoValue(n.args().get(1), c.args().get(1));
        if (first && second) {
            return NullSemantics.enumInvolved(n) ? compared : new SqlExpr.Call(SqlFn.NULL_SAFE_EQUAL, c.args());
        }
        if (first == second || positiveCondition) {
            return compared;
        }
        return SqlExpr.Call.of(SqlFn.COALESCE, compared, new SqlExpr.BoolLit(false));
    }

    /** Whether {@code side}, written as {@code lowered}, is ONE SQL value that may be no value: declared to hold at
     *  most one and possibly none; or declared many and possibly none where the SQL carries one value a row (a row's
     *  column read under a {@code forAll} over the rows is typed many, measured 2026-11-27: read by its declaration
     *  alone it was never guarded, and the {@code forAll} answered true over a row whose comparison had no value).
     *  A list value is no such side: it is empty as a list, which its own comparison answers. */
    private static boolean mayHoldNoValue(TypedSpec side, SqlExpr lowered) {
        if (!(side.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b)
                || b.lower() != 0) {
            return false;
        }
        return Integer.valueOf(1).equals(b.upper()) || oneValue(lowered);
    }

    /** Whether the SQL carries {@code lowered} as ONE value, whatever its side is declared to hold: its type says a
     *  scalar that is no JSON (a JSON value may be a list). A list, a mixed list and a value that states no type
     *  are not known to be one. */
    static boolean oneValue(SqlExpr lowered) {
        return !(lowered instanceof SqlExpr.TaggedList)
                && lowered.type() instanceof com.legend.sql.TypeFact.Typed held
                && (held.type() instanceof SqlType.Decimal
                        || held.type() instanceof SqlType.Scalar one && one != SqlType.Scalar.JSON);
    }

    /**
     * THE "SAME VALUES" INSTRUCTION OVER TWO SIDE VALUES (step 6B, slice 1, 2026-11-27; the user: "isn't that just a
     * dialect rendering of single representation of rules? Isn't that what the dialect is for?"). {@code compared} is
     * {@code n}'s plain {@code =} of two values the SQL carries as lists, of exact numbers or of Strings
     * ({@link SideEqualities#comparedAsValues}): it becomes ONE instruction, {@code SAME_IN_ORDER}, over the two
     * list VALUES, wherever the comparison stands -- a query's answer, an {@code if}, a lambda, a filter. Each
     * dialect writes it: a database that compares lists writes the two values ({@code AnsiSqlRenderer.sameLists});
     * one that compares rows reads the two sides' rows, which ride as the third and fourth arguments where the
     * lowering has them ({@code sideRows}: a side that reads nothing of the row, from the planner), and without them
     * writes the comparison of the two values it wrote before this slice ({@code AnsiSqlRenderer.sameValues}).
     * Anything else is left as it is.
     */
    static SqlExpr asSameValues(TypedNativeCall n, SqlExpr compared,
            java.util.function.Function<TypedSpec, com.legend.sql.@com.legend.base.Nullable SqlQuery> sideRows) {
        if (!(compared instanceof SqlExpr.Call c) || c.fn() != SqlFn.EQUAL || c.args().size() != 2
                || n.args().size() != 2 || !plainList(c.args().get(0)) || !plainList(c.args().get(1))
                || !(SideEqualities.comparedAsValues(n.args().get(0).info().type(), n.args().get(1).info().type())
                        || heldAsOneKind(n, c.args()))) {
            return compared;
        }
        // the rows ride where a database that compares rows needs them: a computed side; or two written lists of
        // exact numbers, whose JSON texts are not their values (1.0 and 1.000 are one number; measured 2026-11-27 on
        // H2: read by their SQL types alone, which were the same, the two lists were compared as texts again). Two
        // written lists of Strings are compared as they were: a database's comparison of two String COLUMNS follows
        // its collation, its comparison of two lists does not
        boolean written = n.args().get(0) instanceof TypedCollection && n.args().get(1) instanceof TypedCollection;
        boolean rows = !written || SideEqualities.exactNumber(n.args().get(0).info().type());
        // (each side's rows read as its VALUE is compared: a day or a date-time by its instant, a text carried as
        // written parsed, a held one in the database's one form -- SideEqualities.instants, the reading the planner's
        // own statement gave these sides; review 4 of slice 3a, blocker 1: the raw rows met a text against a
        // TIMESTAMP on a database that compares rows)
        com.legend.sql.SqlQuery left = rows
                ? SideEqualities.instants(sideRows.apply(n.args().get(0)), n.args().get(0).info().type()) : null;
        com.legend.sql.SqlQuery right = left == null ? null
                : SideEqualities.instants(sideRows.apply(n.args().get(1)), n.args().get(1).info().type());
        return new SqlExpr.Call(SqlFn.SAME_IN_ORDER, left == null || right == null ? c.args()
                : List.of(c.args().get(0), c.args().get(1), new SqlExpr.ScalarSubquery(left),
                        new SqlExpr.ScalarSubquery(right)));
    }

    /** The two sides of an equality {@link #emptySidesAsPure} wrote as "false where a side is empty", or null where
     *  {@code e} is no such form: its negation is the null-safe not-equal of the two ({@link NullSemantics#negate}),
     *  as an unguarded equality's is. */
    static @com.legend.base.Nullable List<SqlExpr> guardedSides(SqlExpr e) {
        return e instanceof SqlExpr.Call guard && guard.fn() == SqlFn.COALESCE && guard.args().size() == 2
                && guard.args().get(1) instanceof SqlExpr.BoolLit otherwise && !otherwise.value()
                && guard.args().get(0) instanceof SqlExpr.Call equal && equal.fn() == SqlFn.EQUAL
                && equal.args().size() == 2 ? equal.args() : null;
    }

    /** {@link #equal(TypedNativeCall, List)} with how numbers of different types compare inside a mixed list
     *  ({@link NumberFamilies}: the product's two families, or the three types a compatibility door's table
     *  names). */
    static SqlExpr equal(TypedNativeCall n, List<SqlExpr> args, NumberFamilies numbers) {
        // ENGINE-VERBATIM empty ladder (pureToSQLQuery.pure
        // nullSafeEqualsOperation: "a literal empty ([])
        // operand degenerates to a null check on the other
        // side ([] == [] is statically true)"; frontier
        // witness testEqualEmpty — the engine's relational
        // DuckDB executor passes it). Scoped to the LITERAL
        // empty collection: the engine's [0..0] criterion is
        // honest post-routing, but OUR checker types some
        // subtype navigations [0..0] that still read real
        // columns (the gate-4 mapping-family regression
        // pinned that narrowing at burn slice 4).
        boolean le = n.args().get(0) instanceof
                com.legend.compiler.spec.typed.TypedCollection lc
                && lc.elements().isEmpty();
        boolean re = n.args().get(1) instanceof
                com.legend.compiler.spec.typed.TypedCollection rc
                && rc.elements().isEmpty();
        // A side whose TYPE is Nil and that is no written [] (the empty list through a filter or a map; step 6B,
        // slice 3, 2026-11-28: two such sides compared inside a list's function answered no value, their SQL a NULL
        // list no `=` answers for). Nil says the side holds no value IF it answers -- []->toOne() is Nil too, and
        // raises -- so nothing is dropped: the comparison is "both sides hold nothing", each side written and
        // evaluated (review 4 of slice 3a, blocker 2: a first cut took the type for the written [] and dropped the
        // side). The product's rule alone; a door's comparison is as it was
        if (numbers == NumberFamilies.TWO_FAMILIES && !(le || re)
                && (PlatformTypes.isNil(n.args().get(0).info().type())
                        || PlatformTypes.isNil(n.args().get(1).info().type()))) {
            // (never an AND of the two: a database answers false from one operand and leaves the other, and the
            // error it would raise, unread -- probed 2026-11-28, []->toOne() == 1 answered false so)
            return sameOnlyWhereBothEmpty(holdsNothing(n.args().get(0), args.get(0)),
                    holdsNothing(n.args().get(1), args.get(1)));
        }
        if (le || re) {
            if (le && re) {
                return new SqlExpr.BoolLit(true);
            }
            // equal to [] where it holds nothing. A value the SQL carries as a list (a mixed list, a computed list): its
            // size or its length -- a computed list of no element is an empty list, never NULL (until the edition
            // program's step 6 this was a NULL test for every side, which answered false for
            // [1, 2]->filter(x | $x > 5) == []). Anything else, a to-many navigation's column among them (one value a
            // row, whatever its declared multiplicity): its absence, as before
            SqlExpr other = args.get(le ? 1 : 0);
            return other instanceof SqlExpr.TaggedList || aList(other)
                    ? Scalars.emptiness(n.args().get(le ? 1 : 0), other) : SqlExpr.Call.of(SqlFn.IS_NULL, other);
        }
        // a TAGGED value or list on a side (track L, 2026-10-29): equality by kind and value -- two
        // elements the same channel's values, an element against a value of a static kind that
        // channel's value, two lists element by element; a kind no channel holds is never equal
        SqlExpr taggedEq = taggedEquality(n.args().get(0), args.get(0), n.args().get(1), args.get(1), numbers);
        if (taggedEq != null) {
            return taggedEq;
        }
        Integer p0 = Scalars.partialPrecision(n.args().get(0));
        Integer p1 = Scalars.partialPrecision(n.args().get(1));
        if (p0 != null || p1 != null) {
            if (Objects.equals(p0, p1)) {
                return new SqlExpr.Call(SqlFn.EQUAL, args);
            }
            int o = p0 != null ? 1 : 0;
            Type other = n.args().get(o).info().type();
            // a year or a year-month COMPUTED (date(1973), a partial's datePart, a partial moved by years or by
            // months) is typed the abstract Date and carried, as the literal is, as the text it prints
            // (TEMPORAL_TEXT): the two texts are compared, equal at the same precision and value alone. Until the
            // closing batch of the edition program's step 6 "a partial is a literal only" answered false here with
            // no read (measured 2026-11-27: %1973 == date(1973) false where an assert of the two passes; six of
            // legend-pure's own date tests, on every database)
            if (other == Type.Primitive.DATE && (p0 == null || p1 == null)
                    && carriedAs(args.get(o), SqlType.Scalar.TEMPORAL_TEXT)) {
                return new SqlExpr.Call(SqlFn.EQUAL, args);
            }
            // the same, where the computed side is a LIST of such texts (a year passed through a filter): the written
            // year as the list of it, the two lists compared (step 6B, 2026-11-28: read as "a full date" it answered
            // false with no read, where the comparison written with no filter is true)
            if (numbers == NumberFamilies.TWO_FAMILIES && other == Type.Primitive.DATE && (p0 == null || p1 == null)
                    && listOf(args.get(o), SqlType.Scalar.TEMPORAL_TEXT)) {
                return new SqlExpr.Call(SqlFn.EQUAL, o == 0
                        ? List.of(args.get(0), PureSql.asList(args.get(1), false))
                        : List.of(PureSql.asList(args.get(0), false), args.get(1)));
            }
            // Covers both a full-precision opposite side AND a
            // different-precision partial (whose static type is
            // also Date) — never equal.
            if (isFullPrecisionDate(other)) {
                return new SqlExpr.BoolLit(false);
            }
            // A Date is never equal to a NON-date kind — the
            // string carrier must not leak into '2014'=='2014'
            // being true (audit). Any stays dynamic (fall through).
            if (!PlatformTypes.isAny(other)) {
                return new SqlExpr.BoolLit(false);
            }
        }
        // a date-time carried as its written text (date(2015, 4, 16, 14): a date built to the hour or the minute keeps
        // how it prints) against one the database holds as a TIMESTAMP: by its instant, the text read as the assert's
        // judge reads it (register S399; SideEqualities.instantOfText). Until the edition program's step 6 the
        // database was handed the text against a TIMESTAMP and refused it ("invalid timestamp field format")
        // (a LIST of such texts too, against one held date-time or a list of them: each read as its instant -- step
        // 6B, 2026-11-28: a date built to the hour, passed through a filter, never equalled the literal it equals
        // where it stands alone)
        // (the lists for the product's rule alone: at a door the written precision is part of the value, and a list
        // of such texts was, and stays, the database's to refuse -- review 4 of slice 3a, finding 5)
        List<SqlExpr> instants = asInstants(args, numbers == NumberFamilies.TWO_FAMILIES);
        List<SqlExpr> sides = instants == null ? args : instants;
        List<SqlExpr> cargs = List.of(
                CastPolicy.comparisonWireOperand(n.args().get(0),
                        sides.get(0), n.args().get(1)),
                CastPolicy.comparisonWireOperand(n.args().get(1),
                        sides.get(1), n.args().get(0)));
        // a literal side meeting a variant-carried side joins it there (the spelling carrier's
        // equality by emission left with track L's L2, 2026-10-29: a mixed value is tagged, above)
        cargs = VariantShapes.alignLiteralToJson(n.args(), cargs);
        SqlExpr inv = EnumSourceValues.decodeInvert(
                n.args().get(0), n.args().get(1),
                cargs.get(0), cargs.get(1));
        if (inv != null) {
            return inv;
        }
        boolean list0 = aList(cargs.get(0));
        boolean list1 = aList(cargs.get(1));
        if (list0 != list1 && Scalars.isToOne(n.args().get(list0 ? 1 : 0))) {
            // one value against a value the SQL carries as a list: the value as the list of it, so the two are
            // compared as lists (equal where the list holds that one value; until the edition program's step 6 a
            // value was compared with an array, which a database refuses). By the SQL fact, never the declared
            // multiplicity: a to-many navigation's column is one value a row
            cargs = List.of(PureSql.asList(cargs.get(0), list0), PureSql.asList(cargs.get(1), list1));
        }
        // two values typed only when the query runs, which the SQL holds in two types that are never equal
        SqlExpr apart = numbers == NumberFamilies.TWO_FAMILIES ? heldApart(n, cargs) : null;
        if (apart != null) {
            return apart;
        }
        // nullable col-vs-col equality is NULL-SAFE (engine
        // isEqualsFromFilter; task #62's equal-side arm)
        return nullArms(n, cargs);
    }

    /**
     * TWO VALUES TYPED ONLY WHEN THE QUERY RUNS, HELD IN TWO TYPES THAT ARE NEVER EQUAL (step 6B, ahead of slice 3,
     * 2026-11-28): a side declared {@code Number}, {@code Any} or the abstract {@code Date} is compared by the type
     * it holds (AGENTS.md, "Types known only at run time"), and the SQL type each side is carried in says which that
     * is. An exact number against a Float, a number against a text, a day against a date-time: false, both sides
     * still evaluated ({@link #neverEqualButEvaluated}); two that may both hold no value, or two lists: the same only
     * where both are empty ({@link #sameOnlyWhereBothEmpty}). Until then the database's own {@code =} compared them
     * (measured: {@code let a = 1->cast(@Any); let b = '1'->cast(@Any); $a == $b} answered true on DuckDB and H2,
     * where the same comparison written without the lets answers false).
     *
     * <p>Null, the comparison as it was, where ONE side is a written literal (a number literal reads as the type of
     * the value it is written against: {@code compiler.spec.NumberLiterals}), where neither side is typed at run
     * time (the type-checker and {@link HeldKinds} have ruled the pair), or where a side's carrier names no one
     * type (JSON, a date as its text, a tagged value: their own comparisons).
     */
    private static @com.legend.base.Nullable SqlExpr heldApart(TypedNativeCall n, List<SqlExpr> sides) {
        if (n.pos() == null) {
            return null;   // a comparison lite writes itself (a mapping's translation, a milestoning filter): as HeldKinds
        }
        TypedSpec l = n.args().get(0);
        TypedSpec r = n.args().get(1);
        // two WRITTEN values that reach here in two types are two the type-checker met as values typed at run time:
        // a let's cast is gone once the let is read where it is used (probed 2026-11-28: `let a = 1->cast(@Number);
        // let b = 1.0->cast(@Number); $a == $b` arrives as the literals 1 and 1.0, and no record of the comparison
        // having been kept for its types). A value read through such a let against a COLUMN is not caught here: the
        // literal rule below reads it as the column's type (review 4 of slice 3a, note 9: open)
        boolean bothWritten = literal(l) && literal(r);
        if (!bothWritten && !(typedAtRunTime(l) || typedAtRunTime(r))) {
            return null;
        }
        // each side's kind by the ONE reading the product has of it (MixedEncoding.kindOfValue: its Pure type, the
        // value under an upcast, a column's own type) -- the reading HeldKinds refuses two columns by -- and, for a
        // side typed only when the query runs whose kind the text does not name (a computed list, a filter's
        // result), the kind of the SQL type it is held in; whether two kinds can be equal by the product's number
        // rule. No kind either way: the comparison as it was
        String left = kindHeld(l, sides.get(0));
        String right = kindHeld(r, sides.get(1));
        if (left == null || right == null || NumberFamilies.TWO_FAMILIES.canEqual(left, right)) {
            return null;
        }
        // a NUMBER literal the query writes against a value of the other number family reads as that family where
        // it is exact there (the literal rule, HeldKinds.readsAs); two literals never do
        if (!bothWritten && MixedEncoding.numberTypePlace(left) >= 0 && MixedEncoding.numberTypePlace(right) >= 0
                && (HeldKinds.readsAs(l, right) || HeldKinds.readsAs(r, left))) {
            return null;
        }
        if (aList(sides.get(0)) || aList(sides.get(1))) {
            return sameOnlyWhereBothEmpty(holdsNothing(l, sides.get(0)), holdsNothing(r, sides.get(1)));
        }
        return mayHoldNoValue(l, sides.get(0)) && mayHoldNoValue(r, sides.get(1))
                ? sameOnlyWhereBothEmpty(isNone(sides.get(0)), isNone(sides.get(1)))
                : neverEqualButEvaluated(sides.get(0), sides.get(1));
    }

    private static @com.legend.base.Nullable String kindHeld(TypedSpec side, SqlExpr lowered) {
        String named = MixedEncoding.kindOfValue(side, lowered);
        return named != null || !typedAtRunTime(side) || !(lowered.type() instanceof com.legend.sql.TypeFact.Typed held)
                ? named : MixedEncoding.kindOfCarrier(side.info().type(), held.type());
    }

    /** Whether {@code side}, lowered as {@code lowered}, holds nothing: a list the SQL holds as a list by its length
     *  (a NULL list none), a tagged list by its size, any other value by its absence -- chosen by what the SQL
     *  holds, never by the side's declared multiplicity (a to-many navigation's column is one value a row; review 4
     *  of slice 3a, note 6). */
    private static SqlExpr holdsNothing(TypedSpec side, SqlExpr lowered) {
        if (lowered instanceof SqlExpr.TaggedList) {
            return Scalars.emptiness(side, lowered);
        }
        return aList(lowered) ? new SqlExpr.Call(SqlFn.EQUAL, List.of(SqlExpr.Call.of(SqlFn.COALESCE,
                SqlExpr.Call.of(SqlFn.LIST_LENGTH, lowered), new SqlExpr.IntLit(0)), new SqlExpr.IntLit(0)))
                : isNone(lowered);
    }

    private static boolean typedAtRunTime(TypedSpec side) {
        Type t = side.info().type();
        return t == Type.Primitive.NUMBER || t == Type.Primitive.DATE || PlatformTypes.isAny(t);
    }

    private static boolean literal(TypedSpec side) {
        return side instanceof com.legend.compiler.spec.typed.TypedCInteger
                || side instanceof com.legend.compiler.spec.typed.TypedCFloat
                || side instanceof com.legend.compiler.spec.typed.TypedCDecimal
                || side instanceof com.legend.compiler.spec.typed.TypedCString
                || side instanceof com.legend.compiler.spec.typed.TypedCBoolean
                || side instanceof com.legend.compiler.spec.typed.TypedCDate
                || side instanceof TypedCollection;
    }

    /**
     * Whether {@code n}'s two sides, one of them declared {@code Number} or the abstract {@code Date} (a type the
     * value fills in when the query runs), are each held by the SQL as a typed list of ONE plain kind, the two of
     * one family ({@link SideEqualities#heldInOneKind}: the kind under each side's upcast, by the product's number
     * rule): such sides are compared as the two values, by the one "same values" instruction, wherever the
     * comparison stands (step 6B, slice 3: {@code [1, 2]->cast(@Number) == [1D, 2D]->cast(@Number)}). The planner
     * stands aside for the same pairs by the same reading ({@code planner.RootComparison}). Two families never
     * reach the instruction ({@link #heldApart} answered them); a list that holds several types is a tagged list,
     * no plain one (part 2).
     */
    static boolean heldAsOneKind(TypedNativeCall n, List<SqlExpr> sides) {
        return n.args().size() == 2 && sides.size() == 2 && plainList(sides.get(0)) && plainList(sides.get(1))
                && SideEqualities.heldInOneKind(n.args().get(0), n.args().get(1));
    }

    /**
     * The false of two sides that hold two types, both sides still evaluated ({@code TypedRunTimeComparison
     * .answeredNeverEqual}): each side's text behind a mark of its own, compared -- two texts that differ at their
     * first character, so the answer is false whatever the sides hold, and no database can know that without
     * computing them; a side that holds no value makes the comparison NULL, which is false too. A constant false
     * here would let the database, and lite, leave a side unevaluated, and an error it raises unraised.
     */
    static SqlExpr neverEqualButEvaluated(SqlExpr left, SqlExpr right) {
        SqlExpr l = SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("l"), new SqlExpr.Cast(left,
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR));
        SqlExpr r = SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("r"), new SqlExpr.Cast(right,
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR));
        return SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Call.of(SqlFn.EQUAL, l, r), new SqlExpr.BoolLit(false));
    }

    /**
     * Two sides of two types, which are never equal -- unless BOTH hold no value, which are ({@code [] == []} is
     * true): each side's "none, or some" as one character, the two joined and compared with "none, none".
     * {@code leftNone} and {@code rightNone} say whether each side holds no value ({@link #isNone} of one value or
     * of a list's fold, {@code Scalars.emptiness} of a list where it stands). Each is written once and read, so a
     * side that fails raises; no text is made of a value, so a side may hold anything.
     */
    static SqlExpr sameOnlyWhereBothEmpty(SqlExpr leftNone, SqlExpr rightNone) {
        return SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.CONCAT, noneOrSome(leftNone), noneOrSome(rightNone)),
                new SqlExpr.StringLit("nn"));
    }

    /** {@code none}, whether a side holds no value, as one character: {@code n}, or {@code v}. */
    static SqlExpr noneOrSome(SqlExpr none) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(none, new SqlExpr.StringLit("n"))),
                new SqlExpr.StringLit("v"), SqlExpr.Reason.OPERATOR);
    }

    /** Whether {@code value}, one SQL value, is no value. */
    static SqlExpr isNone(SqlExpr value) {
        return SqlExpr.Call.of(SqlFn.IS_NULL, value);
    }

    /** {@link #neverEqualButEvaluated} for a membership or an index of two types ({@code in}, {@code contains},
     *  {@code indexOf}): false, or -1 for {@code index}, each of {@code singles} -- the single values among the
     *  comparison's sides -- still evaluated, their texts joined behind a mark and compared with another. */
    static SqlExpr neverAmongButEvaluated(List<SqlExpr> singles, boolean index) {
        SqlExpr read = new SqlExpr.StringLit("l");
        for (SqlExpr single : singles) {
            read = SqlExpr.Call.of(SqlFn.CONCAT, read, new SqlExpr.Cast(single, SqlType.Scalar.VARCHAR,
                    SqlExpr.Reason.OPERATOR));
        }
        SqlExpr never = SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Call.of(SqlFn.EQUAL, read, new SqlExpr.StringLit("r")),
                new SqlExpr.BoolLit(false));
        return !index ? never : new SqlExpr.Case(List.of(new SqlExpr.Case.When(never, new SqlExpr.IntLit(0))),
                new SqlExpr.IntLit(-1), SqlExpr.Reason.OPERATOR);
    }

    /**
     * UPSTREAM'S equality of a date-time BUILT TO THE HOUR OR THE MINUTE ({@code date(y, m, d, h)},
     * {@code date(y, m, d, h, mi)}) against a date-time the database holds: a date's WRITTEN PRECISION is part of
     * its value there (run 2026-11-27, {@code upstream-step6/answers-request-3}, group D:
     * {@code date(2015, 4, 16, 14) == %2015-04-16T14:00:00} false, {@code == %2015-04-16T14} true). So against a
     * date WRITTEN in the query at the same precision the two are the same where their texts are (the literal's own
     * text: the database holds a literal as a full timestamp, its precision gone); against a date written at another
     * precision, or any other date-time, never -- both sides still evaluated.
     *
     * <p>Read from what is WRITTEN where the comparison stands: the constructor's four or five parts. Null for
     * everything else, which the shared equality answers by the instant: a date-time carried as text for another
     * reason (one built with a fraction of a second, a literal past the database's precision, the latest-date
     * sentinel) is a full date-time (the code audit of the closing batch, B2: read by its carrier, it had been
     * answered "never equal" to a literal of its own precision). The compatibility doors'
     * ({@code NamedRule.THREE_NUMBER_TYPES_AND_WRITTEN_DATE_PRECISION}); the product compares the two by their instant (register S399).
     */
    static @com.legend.base.Nullable SqlExpr upstreamsPartialDateTime(TypedNativeCall n, List<SqlExpr> args) {
        if (args.size() != 2 || n.args().size() != 2) {
            return null;
        }
        int built = partsWritten(n.args().get(0)) > 0 ? 0 : partsWritten(n.args().get(1)) > 0 ? 1 : -1;
        if (built < 0 || !carriedAs(args.get(built), SqlType.Scalar.TEMPORAL_TEXT)
                || !carriedAs(args.get(1 - built), SqlType.Scalar.TIMESTAMP)) {
            return null;
        }
        com.legend.values.PureDateLiteral.Precision precision = partsWritten(n.args().get(built)) == 4
                ? com.legend.values.PureDateLiteral.Precision.HOUR : com.legend.values.PureDateLiteral.Precision.MINUTE;
        if (n.args().get(1 - built) instanceof com.legend.compiler.spec.typed.TypedCDate written
                && written.value().precision() == precision) {
            return SqlExpr.Call.of(SqlFn.EQUAL, args.get(built), new SqlExpr.StringLit(written.value().canonicalText()));
        }
        return neverEqualButEvaluated(args.get(0), args.get(1));
    }

    /** 4 or 5 where {@code side} is a date built to the hour or the minute where it stands; else 0. */
    private static int partsWritten(TypedSpec side) {
        return side instanceof TypedNativeCall c && com.legend.builtin.Pure.AT_DATE_DATE.contains(c.callee().id())
                && (c.args().size() == 4 || c.args().size() == 5) ? c.args().size() : 0;
    }

    /** Whether {@code e}'s SQL type is known to be a list of {@code element}. */
    private static boolean listOf(SqlExpr e, SqlType element) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array a
                && a.element() == element;
    }

    /** A list of date-times carried as their written texts, each read as the instant it writes
     *  ({@link SideEqualities#instantOfText}). */
    private static SqlExpr instantsOfTexts(SqlExpr texts) {
        String x = "_dtx";
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, texts,
                new SqlExpr.Lambda(List.of(x), SideEqualities.instantOfText(SqlExpr.Column.derived(null, x))));
    }

    /** A list of date-times the database holds, each as the instant compared with one read from its text
     *  ({@link SideEqualities#instantOfHeld}). */
    private static SqlExpr instantsOfHeld(SqlExpr held) {
        String x = "_dth";
        // (the element stated as the TIMESTAMP it is: the dialect's cast reads a value of no stated type as a text)
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, held, new SqlExpr.Lambda(List.of(x), SideEqualities.instantOfHeld(
                SqlExpr.Column.of(null, x, SqlType.Scalar.TIMESTAMP, false, com.legend.sql.OutputCol.Origin.DERIVED))));
    }

    /** The two sides where a date-time carried as its written text (or a list of such texts) meets one the database
     *  holds as a TIMESTAMP (or a list of them), each read as its instant, in the order given; null where the sides
     *  are no such pair. One value against a list is the list of that value. The
     *  list forms only where {@code lists} (the product's own rule). Two SINGLE texts are not read here: their
     *  written precisions are compared by the rule above this call. */
    private static @com.legend.base.Nullable List<SqlExpr> asInstants(List<SqlExpr> args, boolean lists) {
        SqlExpr l = args.get(0);
        SqlExpr r = args.get(1);
        boolean lTexts = listOf(l, SqlType.Scalar.TEMPORAL_TEXT);
        boolean rTexts = listOf(r, SqlType.Scalar.TEMPORAL_TEXT);
        // (two lists of texts are compared as the texts they are: such a list may hold a year or a year-month, which
        // is no instant, and what each element is only its value says. So two lists that write one instant at two
        // precisions -- an hour's text against a minute's -- are not equal here: a named gap, review 4 of slice 3a,
        // blocker 1's second example)
        if (lists && lTexts != rTexts) {
            SqlExpr a = lTexts ? instantsOfTexts(l) : instantListOf(l);
            SqlExpr b = rTexts ? instantsOfTexts(r) : instantListOf(r);
            return a == null || b == null ? null : List.of(a, b);
        }
        for (int t = 0; t < 2; t++) {
            SqlExpr text = args.get(t);
            SqlExpr held = args.get(1 - t);
            if (!carriedAs(text, SqlType.Scalar.TEMPORAL_TEXT)) {
                continue;
            }
            SqlExpr a;
            SqlExpr b;
            if (carriedAs(held, SqlType.Scalar.TIMESTAMP)) {
                a = SideEqualities.instantOfText(text);
                b = SideEqualities.instantOfHeld(held);
            } else if (lists && listOf(held, SqlType.Scalar.TIMESTAMP)) {
                a = PureSql.asList(SideEqualities.instantOfText(text), false);
                b = instantsOfHeld(held);
            } else {
                continue;
            }
            return t == 0 ? List.of(a, b) : List.of(b, a);
        }
        return null;
    }

    /** {@code side}, met beside a list of date-times carried as their written texts, as the list of instants it is:
     *  a list of held date-times each as its instant, one held date-time as the list of its instant; null where it
     *  is neither (the comparison is then as it was). */
    private static @com.legend.base.Nullable SqlExpr instantListOf(SqlExpr side) {
        if (listOf(side, SqlType.Scalar.TIMESTAMP)) {
            return instantsOfHeld(side);
        }
        return carriedAs(side, SqlType.Scalar.TIMESTAMP)
                ? PureSql.asList(SideEqualities.instantOfHeld(side), false) : null;
    }

    /** Whether {@code e}'s SQL type is known to be {@code type}. */
    private static boolean carriedAs(SqlExpr e, SqlType type) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() == type;
    }

    /** A date type whose VALUES are always full-precision (columns, full literals). */
    private static boolean isFullPrecisionDate(Type t) {
        return t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.DATE;
    }

    /** Equality where a side is a tagged value or a tagged list (track L); null where neither is. */
    private static @com.legend.base.Nullable SqlExpr taggedEquality(TypedSpec s0, SqlExpr a, TypedSpec s1, SqlExpr b,
            NumberFamilies numbers) {
        if (a instanceof SqlExpr.Tagged x && b instanceof SqlExpr.Tagged y) {
            return TaggedLists.sameElement(x, y, numbers);
        }
        if (a instanceof SqlExpr.Tagged x) {
            return TaggedLists.sameElement(x, MixedEncoding.kindOfValue(s1, b), b, s1.info().type(), numbers);
        }
        if (b instanceof SqlExpr.Tagged y) {
            return TaggedLists.sameElement(y, MixedEncoding.kindOfValue(s0, a), a, s0.info().type(), numbers);
        }
        SqlExpr.TaggedList la = Scalars.asTaggedList(s0, a);
        SqlExpr.TaggedList lb = Scalars.asTaggedList(s1, b);
        if (la != null && lb != null) {
            return sameTaggedLists(s0, la, s1, lb, numbers);
        }
        if (la != null || lb != null) {
            // a mixed list against a list of ONE kind: equal where every element is of that kind and equals the
            // other's at its index (a kind the mixed list has no channel for is never equal)
            SqlExpr.TaggedList tl = java.util.Objects.requireNonNull(la != null ? la : lb);
            TypedSpec otherSpec = la != null ? s1 : s0;
            SqlExpr other = la != null ? b : a;
            // a list of ONE declared kind that is not a literal collection (a String[*] variable, a map's result):
            // its kind is its element type's (the L1 audit, 2026-10-29)
            String kind = otherSpec instanceof TypedCollection ? null : MixedEncoding.kindOf(otherSpec);
            if (otherSpec instanceof TypedCollection oc && !oc.elements().isEmpty()) {
                // a row's cells (the Typer's rowCells collection, on the variant carrier for its NULL cells) are a
                // literal collection of static kinds: read as a tagged list, the two compare kind by kind
                if (other instanceof SqlExpr.ArrayLit oal && oal.elements().size() == oc.elements().size()) {
                    SqlExpr.TaggedList ot = MixedEncoding.taggedList(oc, oal.elements());
                    if (ot != null) {
                        return sameTaggedLists(la != null ? s0 : s1, tl, otherSpec, ot, numbers);
                    }
                }
                // a list of ONE static kind (one family: a date-time at any written precision is a DateTime)
                for (TypedSpec el : oc.elements()) {
                    String k = MixedEncoding.kindOf(el);
                    if (k == null || (kind != null && !kind.equals(MixedEncoding.family(k)))) {
                        kind = null;
                        break;
                    }
                    kind = MixedEncoding.family(k);
                }
            }
            if (kind != null) {
                return TaggedLists.sameListOfKind(tl, kind, other, numbers);
            }
            if (variantList(other)) {
                // against a list on the VARIANT carrier (a row's cells read in a lambda, an Any value): the mixed
                // list joins it there, and the carrier's own equality decides (its kinds are JSON's: a number's
                // text, a string, a boolean; the lane the spelling carrier's list met it on)
                return SqlExpr.Call.of(SqlFn.EQUAL, la != null ? TaggedLists.asVariantList(la) : a,
                        lb != null ? TaggedLists.asVariantList(lb) : b);
            }
            throw new com.legend.error.NotImplementedException("a mixed list compared with a list whose"
                    + " elements' kind is not static and that is not on the variant carrier: not lowered");
        }
        return null;
    }

    /** Two mixed lists are the same list. Two lists WRITTEN IN THE QUERY with every element present compare position
     *  by position, no list in the SQL ({@link TaggedLists#sameWrittenLists}); two lists in their own order whose
     *  families are each one array compare array by array (no lambda: every database); any other pair element by
     *  element. */
    private static SqlExpr sameTaggedLists(TypedSpec s0, SqlExpr.TaggedList la, TypedSpec s1, SqlExpr.TaggedList lb,
            NumberFamilies numbers) {
        if (everyElementWritten(s0) && everyElementWritten(s1)) {
            SqlExpr written = TaggedLists.sameWrittenLists(la, lb, numbers);
            if (written != null) {
                return written;
            }
        }
        return la.identityPositions() && lb.identityPositions() && TaggedLists.oneChannelPerFamily(numbers, la, lb)
                ? TaggedLists.sameListByKinds(la, lb) : TaggedLists.sameList(la, lb, numbers);
    }

    /** Whether the SQL carries {@code e} as a list of plain typed values: an array whose elements are a scalar type
     *  that is the value itself (no JSON, no Decimal or date written as its text). */
    private static boolean plainList(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array a
                && (a.element() instanceof SqlType.Decimal || a.element() instanceof SqlType.Scalar s
                        && s != SqlType.Scalar.JSON && s != SqlType.Scalar.DECIMAL_TEXT
                        && s != SqlType.Scalar.TEMPORAL_TEXT);
    }

    /** Whether the SQL carries {@code e} as a list: its type says an array. */
    private static boolean aList(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array;
    }

    /** A collection written in the query whose every element is exactly one value: its length is its element count
     *  (an element that may be empty, {@code [$optional, 'a']}, leaves the length to the run). */
    private static boolean everyElementWritten(TypedSpec list) {
        return list instanceof TypedCollection c && !c.elements().isEmpty()
                && c.elements().stream().allMatch(Scalars::isToOne);
    }

    /** A list on the variant carrier: typed JSON or an array of JSON, or a literal array of variant-wrapped
     *  elements (a row's cells as the Typer synthesizes them). */
    private static boolean variantList(SqlExpr e) {
        if (e.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.JSON
                        || (t.type() instanceof SqlType.Array a && a.element() == SqlType.Scalar.JSON))) {
            return true;
        }
        return e instanceof SqlExpr.ArrayLit al && !al.elements().isEmpty()
                && al.elements().stream().allMatch(MixedEncoding::variantWrapped);
    }

    /** Engine isEqualsFromFilter (dbExtension.pure:926-930): equality
     * of two NULLABLE COLUMNS is NULL-SAFE — pure empty==empty is TRUE
     * ({@code a = b OR (a is null AND b is null)}, the
     * testConsistencyWithNulls col-to-col golden). Approximated by
     * operand shape (both plain columns, both optional operands) — the
     * engine's callingFromFilter flag has no analog at this layer; the
     * corpus goldens referee. Anything else stays the bare EQUAL. */
    static SqlExpr nullArms(
            com.legend.compiler.spec.typed.TypedNativeCall n,
            java.util.List<SqlExpr> ops) {
        // POSITION-BLIND (engine nullSafeEqualsOperation case 5: both
        // lower bounds 0 → nullSafeEqual, no position gate — witness
        // testProjectEqualityOnNullableColumns, where the equality sits
        // in a PROJECT column); the operands' own multiplicities decide.
        // A verbatim context (the mapping's own definition) rewrites the
        // result to plain '=' at the Lowerer ({@link NullSemantics#verbatim}).
        // A NULL-literal operand (an empty value, as a NULL constant's
        // column reads once inlined): pure x == [] IS the absence test, and
        // [] == [] is TRUE -- a bare '=' with a NULL is never TRUE (#SQL's
        // SELECT NULL INTERSECT SELECT NULL, two NULLs equal, answered no
        // row). Null-safe, so that a verbatim context and the legacy
        // null-unsafe mode still write the engine's '='.
        if (ops.size() == 2
                && (ops.get(0) instanceof SqlExpr.NullLit || ops.get(1) instanceof SqlExpr.NullLit)) {
            return new SqlExpr.Call(SqlFn.NULL_SAFE_EQUAL, ops);
        }
        if (n.args().size() == 2) {
            // two whole Variants, or a Variant and an Any the plan cannot see, or a Variant among other values: only
            // identity decides (register S401), refused by name (VariantEquality.between; its DISJOINT pairs the
            // Lowerer folds before this, InstanceEquality.disjointAnswer)
            com.legend.compiler.spec.typed.VariantEquality.between(n.args().get(0), n.args().get(1));
        }
        if (ops.size() == 2
                && ops.get(0) instanceof SqlExpr.Column
                && ops.get(1) instanceof SqlExpr.Column
                && NullSemantics.isOptional(n.args().get(0).info().multiplicity())
                && NullSemantics.isOptional(n.args().get(1).info().multiplicity())) {
            return new SqlExpr.Call(SqlFn.NULL_SAFE_EQUAL, ops);
        }
        // an OPTIONAL parameter's equality is null-safe: its absence equals
        // an empty side (the engine's optionalVarPlaceHolderOperationSelector,
        // `col is null` when the value is absent, `col = value` when present;
        // the legacy printer writes the same `is not distinct from`) -- unless an
        // enumeration is compared: that routes the engine's enum machinery and
        // never becomes nullSafeEqual (enumInvolved below)
        if (ops.size() == 2 && !NullSemantics.enumInvolved(n)
                && ops.stream().anyMatch(o -> o instanceof SqlExpr.PlanParam p && p.optional())) {
            return new SqlExpr.Call(SqlFn.NULL_SAFE_EQUAL, ops);
        }
        return new SqlExpr.Call(SqlFn.EQUAL, ops);
    }
}
