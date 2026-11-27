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

    /** The rule of {@code equal} and {@code eq}. It is PRECISION-AWARE over dates: a partial-date literal equals only
     *  a same-precision value (decided statically: partial precision is literal-only; columns are full-precision);
     *  same-precision partials compare as ISO-prefix strings. {@code eq} is strict equality, the {@code =} operator
     *  here. It takes NO optional-operand guards deliberately (T1.4): equal is a total native ({@code [] == []} is
     *  true; the guard would spell it false). The residual both-NULL divergence (SQL NULL where Pure says true) is
     *  the reference engine's own behaviour (a bare {@code =}). */
    static SqlExpr equal(TypedNativeCall n, List<SqlExpr> args) {
        return emptySideIsFalse(n, equal(n, args, NumberFamilies.TWO_FAMILIES));
    }

    /**
     * The product's own rule for ONE side that may be empty (the user, 2026-11-27: "Yes do the empty side fix";
     * "okay fix for lite if small"): {@code $x.nick == 'a'} where {@code nick} holds no value is false, as Pure says
     * ({@code [] == 'a'}), wherever the comparison stands. Left a bare {@code =}, the database's NULL was right
     * inside a filter, an {@code if} and under a {@code !} (each reads NULL as "not so") and wrong as a VALUE: a
     * projected comparison showed no value where Pure says false (measured 2026-11-27 on DuckDB, H2 and SQLite).
     * Written as the guard an ordering's optional operand already takes ({@link NullSemantics#optionalOperandGuards}:
     * {@code x IS NOT NULL AND x = v}), so a filter keeps a plain comparison a database can use an index for.
     *
     * <p>Only where exactly one side may be empty and it is a plain column: two sides that may both be empty are the
     * null-safe arm's ({@link #nullArms}: empty equals empty), and a computed side is not written twice (it stays the
     * bare comparison, a recorded gap). A compatibility door keeps the comparison it had: legend-engine writes the
     * bare {@code =} there ({@code Scalars.numbersOfThreeTypes} does not pass through here).
     */
    private static SqlExpr emptySideIsFalse(TypedNativeCall n, SqlExpr compared) {
        if (!(compared instanceof SqlExpr.Call c) || c.fn() != SqlFn.EQUAL || c.args().size() != 2
                || n.args().size() != 2) {
            return compared;
        }
        boolean first = NullSemantics.isOptional(n.args().get(0).info().multiplicity());
        boolean second = NullSemantics.isOptional(n.args().get(1).info().multiplicity());
        if (first == second) {
            return compared;
        }
        SqlExpr mayBeEmpty = c.args().get(first ? 0 : 1);
        if (!(mayBeEmpty instanceof SqlExpr.Column)) {
            return compared;
        }
        return new SqlExpr.Group(new SqlExpr.Call(SqlFn.AND, List.of(SqlExpr.Call.of(SqlFn.IS_NOT_NULL, mayBeEmpty),
                compared)));
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
            Type other = (p0 != null ? n.args().get(1) : n.args().get(0))
                    .info().type();
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
        // nullable col-vs-col equality is NULL-SAFE (engine
        // isEqualsFromFilter; task #62's equal-side arm)
        return nullArms(n, cargs);
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
