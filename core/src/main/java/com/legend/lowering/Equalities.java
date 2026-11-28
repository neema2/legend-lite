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
                || !SideEqualities.comparedAsValues(n.args().get(0).info().type(), n.args().get(1).info().type())) {
            return compared;
        }
        // the rows ride where a database that compares rows needs them: a computed side; or two written lists of
        // exact numbers, whose JSON texts are not their values (1.0 and 1.000 are one number; measured 2026-11-27 on
        // H2: read by their SQL types alone, which were the same, the two lists were compared as texts again). Two
        // written lists of Strings are compared as they were: a database's comparison of two String COLUMNS follows
        // its collation, its comparison of two lists does not
        boolean written = n.args().get(0) instanceof TypedCollection && n.args().get(1) instanceof TypedCollection;
        boolean rows = !written || SideEqualities.exactNumber(n.args().get(0).info().type());
        com.legend.sql.SqlQuery left = rows ? sideRows.apply(n.args().get(0)) : null;
        com.legend.sql.SqlQuery right = left == null ? null : sideRows.apply(n.args().get(1));
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
        List<SqlExpr> sides = args;
        if (carriedAs(args.get(0), SqlType.Scalar.TEMPORAL_TEXT) && carriedAs(args.get(1), SqlType.Scalar.TIMESTAMP)) {
            sides = List.of(SideEqualities.instantOfText(args.get(0)), SideEqualities.instantOfHeld(args.get(1)));
        } else if (carriedAs(args.get(1), SqlType.Scalar.TEMPORAL_TEXT)
                && carriedAs(args.get(0), SqlType.Scalar.TIMESTAMP)) {
            sides = List.of(SideEqualities.instantOfHeld(args.get(0)), SideEqualities.instantOfText(args.get(1)));
        }
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
        TypedSpec l = n.args().get(0);
        TypedSpec r = n.args().get(1);
        // two WRITTEN values that reach here in two types are two the type-checker met as values typed at run time:
        // a let's cast is gone once the let is read where it is used (probed 2026-11-28: `let a = 1->cast(@Number);
        // let b = 1.0->cast(@Number); $a == $b` arrives as the literals 1 and 1.0)
        boolean bothWritten = literal(l) && literal(r);
        if (!bothWritten && !(typedAtRunTime(l) || typedAtRunTime(r))) {
            return null;
        }
        // a literal against a COLUMN whose type only the column gives still reads as the column's type (AGENTS.md,
        // "A type only a column gives"): the database's comparison of the two, as it was. Against any other value
        // typed at run time a literal is a value of its own type
        if (!bothWritten && (literal(l) && sides.get(1) instanceof SqlExpr.Column
                || literal(r) && sides.get(0) instanceof SqlExpr.Column)) {
            return null;
        }
        String left = heldFamily(l, sides.get(0));
        String right = heldFamily(r, sides.get(1));
        if (left == null || right == null || left.equals(right)) {
            return null;
        }
        if (aList(sides.get(0)) || aList(sides.get(1))) {
            return sameOnlyWhereBothEmpty(Scalars.emptiness(l, sides.get(0)), Scalars.emptiness(r, sides.get(1)));
        }
        return mayHoldNoValue(l, sides.get(0)) && mayHoldNoValue(r, sides.get(1))
                ? sameOnlyWhereBothEmpty(isNone(sides.get(0)), isNone(sides.get(1)))
                : neverEqualButEvaluated(sides.get(0), sides.get(1));
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

    /** The family of the type the SQL holds {@code lowered} in (a list's: its elements'), two of which are never
     *  equal: an exact number, a Float, a text, a Boolean, a day, a date-time; null where the carrier names no one
     *  type (JSON, a date as its text, a Decimal as its text against which only its own reading compares, anything
     *  untyped), or is a text under a side declared {@code Date} (a date written as its text). */
    private static @com.legend.base.Nullable String heldFamily(TypedSpec side, SqlExpr lowered) {
        if (!(lowered.type() instanceof com.legend.sql.TypeFact.Typed typed)) {
            return null;
        }
        SqlType t = typed.type() instanceof SqlType.Array a ? a.element() : typed.type();
        if (t instanceof SqlType.Decimal) {
            return "exact";
        }
        if (!(t instanceof SqlType.Scalar s)) {
            return null;
        }
        return switch (s) {
            case INTEGER, BIGINT, HUGEINT -> "exact";
            case DOUBLE -> "float";
            case BOOLEAN -> "boolean";
            case DATE -> "day";
            case TIMESTAMP, TIMESTAMPTZ -> "date-time";
            case VARCHAR -> side.info().type() == Type.Primitive.DATE ? null : "text";
            default -> null;
        };
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
