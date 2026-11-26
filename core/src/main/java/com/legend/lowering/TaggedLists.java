// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;

/**
 * The operations over a mixed list carried as a {@link SqlExpr.TaggedList} (docs/RESULTS_AND_RENDERERS_2026_10_22.md
 * §5a's step B; the result boundary program's track L, 2026-10-29; the decisions log D50). Every operation reads the
 * list's channels at its positions and writes NEW POSITIONS only: a sort orders them by the comparable, a dedup or a
 * filter keeps some, a reverse turns them. The shapes are the ones the spelling carrier's path proved on every lane
 * (a subselect of an unnest with an ordered list aggregate; the kept-list reduce of a dedup; a filter's length), so a
 * list-less dialect's strategies ({@code CarrierStrategies}) see what they own; the one shape added is an element
 * read of a literal array at a computed position ({@code LIST_GET(ArrayLit, p)}). A consumer not here refuses by name
 * at the renderer ({@code AnsiSqlRenderer}: "a tagged list reached the renderer").
 */
final class TaggedLists {

    private TaggedLists() {
    }

    /** A 1-based position's 0-based index, and back. */
    private static SqlExpr plusOne(SqlExpr e) {
        return SqlExpr.Call.of(SqlFn.PLUS, e, new SqlExpr.IntLit(1));
    }

    /** The list's size: how many positions; a list over its rows counts them (lambda-free, list-free). */
    static SqlExpr size(SqlExpr.TaggedList tl) {
        if (tl.rows() != null) {
            OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
            return new SqlExpr.ScalarSubquery(new SqlSelect(
                    List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()),
                            "__n", n)),
                    false, new SqlSource.Subselect(cellsOf(tl), "_tlc", null), null, List.of(), null, null,
                    List.of(), null, null, List.of(n)));
        }
        return SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Call.of(SqlFn.LIST_LENGTH, tl.positions()),
                new SqlExpr.IntLit(0));
    }

    /** The cells relation of a list over its rows ({@link SqlExpr.TaggedList#rows}). */
    private static SqlSelect cellsOf(SqlExpr.TaggedList tl) {
        if (tl.rows() instanceof SqlExpr.ScalarSubquery sq && sq.subquery() instanceof SqlSelect cells) {
            return cells;
        }
        throw new IllegalStateException("a tagged list's rows are a subquery over its cells, not " + tl.rows());
    }

    /** Whether no two channels of the list(s) are of one family: each family's values are in one array. */
    static boolean oneChannelPerFamily(SqlExpr.TaggedList... lists) {
        java.util.Map<String, SqlType.Tagged.Slot> seen = new java.util.HashMap<>();
        for (SqlExpr.TaggedList tl : lists) {
            for (SqlType.Tagged.Slot s : tl.slots()) {
                SqlType.Tagged.Slot first = seen.putIfAbsent(MixedEncoding.family(s.kind()), s);
                if (first != null && !first.equals(s)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Two lists WRITTEN IN THE QUERY, every element present, are the same list where they are the same length and
     * each position holds, on both sides, values of one family that are equal: no list in the SQL, one comparison
     * per position, by the database's own comparison of the type -- a Decimal by its number (S139), a date-time by
     * its instant (S399). A position whose two kinds are of different families makes the lists different by the
     * types alone (the number kinds Pure's text equality can cross are refused by name first,
     * {@link #refuseCrossingNumbers}). Null where a list is not written out (a computed list, a reordered one).
     */
    static @com.legend.base.Nullable SqlExpr sameWrittenLists(SqlExpr.TaggedList a, SqlExpr.TaggedList b) {
        List<SqlExpr.Tagged.Candidate> ea = a.literalElements();
        List<SqlExpr.Tagged.Candidate> eb = b.literalElements();
        if (ea == null || eb == null) {
            return null;
        }
        // an answer by the types alone (the lengths, a position's two kinds) writes no element: taken only where no
        // element can raise, or its error would be lost -- else the lists compare as arrays, every element evaluated
        // (the re-check of this slice's audit, 2026-11-26: [$p.name->toOne(), 1] == ['x', 'y'])
        boolean quiet = raiseNothing(ea) && raiseNothing(eb);
        if (ea.size() != eb.size()) {
            return quiet ? new SqlExpr.BoolLit(false) : null;
        }
        boolean differ = false;
        List<SqlExpr> same = new ArrayList<>(ea.size());
        for (int i = 0; i < ea.size(); i++) {
            SqlExpr.Tagged.Candidate x = ea.get(i);
            SqlExpr.Tagged.Candidate y = eb.get(i);
            if (!MixedEncoding.sameFamily(x.kind(), y.kind())) {
                refuseCrossingNumbers(x.kind(), x.slot(), y.kind(), y.slot(), "two mixed lists' equality");
                differ = true;
                continue;
            }
            // a value that cannot be NULL (a literal) compares plainly; any other null-safe, as the array compare
            // this replaces did, so the answer is always a Boolean (the audit of this slice: a bare = answered NULL
            // for an element empty at run, and a NOT over it dropped the row)
            same.add(neverNull(x.value()) && neverNull(y.value())
                    ? SqlExpr.Call.of(SqlFn.EQUAL, x.value(), y.value())
                    : SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, x.value(), y.value()));
        }
        if (differ || same.isEmpty()) {
            return quiet ? new SqlExpr.BoolLit(false) : null;
        }
        SqlExpr all = same.get(0);
        for (int i = 1; i < same.size(); i++) {
            all = SqlExpr.Call.of(SqlFn.AND, all, same.get(i));
        }
        return all;
    }

    /** Whether {@code e} is a value WRITTEN in the query, which is never NULL: a literal, under the casts, the
     *  grouping and the sign the lowering wraps one in. Read off the node, never off its type's nullable label, which
     *  repeats the Pure multiplicity: a {@code [1]} property mapped to a nullable column says "never NULL" and may
     *  hold one (the re-check of this slice's audit, 2026-11-26). */
    private static boolean neverNull(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Cast c -> neverNull(c.value());
            case SqlExpr.Group g -> neverNull(g.inner());
            case SqlExpr.Call c when c.fn() == SqlFn.NEGATE && c.args().size() == 1 -> neverNull(c.args().get(0));
            case SqlExpr.StringLit s -> true;
            case SqlExpr.IntLit i -> true;
            case SqlExpr.FloatLit f -> true;
            case SqlExpr.DecimalLit d -> true;
            case SqlExpr.BoolLit b -> true;
            case SqlExpr.DateLit d -> true;
            case SqlExpr.TimestampLit t -> true;
            case SqlExpr.TimeLit t -> true;
            default -> false;
        };
    }

    /** Whether no element of a written list can raise ({@link SqlExpr#raisesNothing}): only then may an answer be
     *  given without writing the elements. */
    private static boolean raiseNothing(List<SqlExpr.Tagged.Candidate> elements) {
        return elements.stream().allMatch(c -> SqlExpr.raisesNothing(c.value()));
    }

    /**
     * Two lists in their own order ({@link SqlExpr.TaggedList#identityPositions}) are the same list: the same size,
     * and for every kind either holds, the same channel array (null-safe: a NULL where the element is of another
     * kind meets a NULL) -- a kind one side has no channel for meets an all-NULL array of the other's length. No
     * lambda, no element read: the shape every database compares (H2 compares arrays, and runs no list lambda).
     */
    static SqlExpr sameListByKinds(SqlExpr.TaggedList a, SqlExpr.TaggedList b) {
        List<SqlExpr> conditions = new ArrayList<>();
        conditions.add(SqlExpr.Call.of(SqlFn.EQUAL, size(a), size(b)));
        // every (kind, slot) either side holds: a kind one side has no channel for meets an all-NULL array. The
        // caller sends two lists here only where each FAMILY is one slot across both (oneChannelPerFamily): Decimals
        // at two scales, date-times at two written precisions, are one type's values in two arrays, compared element
        // by element instead (the equality rule: by their number, by their instant; S139, S399)
        java.util.LinkedHashSet<SqlType.Tagged.Slot> slots = new java.util.LinkedHashSet<>();
        slots.addAll(a.slots());
        slots.addAll(b.slots());
        for (SqlType.Tagged.Slot sa : a.slots()) {
            for (SqlType.Tagged.Slot sb : b.slots()) {
                if (!sa.equals(sb)) {
                    refuseCrossingNumbers(sa.kind(), sa.type(), sb.kind(), sb.type(), "two mixed lists' equality");
                }
            }
        }
        for (SqlType.Tagged.Slot slot : slots) {
            SqlExpr.TaggedList.Channel ca = channelOfSlot(a, slot);
            SqlExpr.TaggedList.Channel cb = channelOfSlot(b, slot);
            SqlExpr va = ca != null ? ca.values() : nullChannel(a, slot.type());
            SqlExpr vb = cb != null ? cb.values() : nullChannel(b, slot.type());
            conditions.add(SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.IS_DISTINCT_FROM, va, vb)));
        }
        SqlExpr all = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            all = SqlExpr.Call.of(SqlFn.AND, all, conditions.get(i));
        }
        return all;
    }

    /**
     * Pure's equality of two NUMBERS of different kinds is by their TEXTS (compiled legend-pure's
     * {@code CompiledSupport.eq(Number, Number)}: {@code equals}, then {@code toString}): an Integer and a Decimal
     * of scale zero are equal when their digits are ({@code 3 == 3D}), an Integer and a Float never are (a double's
     * text always carries a point or an exponent), a Float and a Decimal are when the shortest digits agree
     * ({@code 1.5 == 1.5D}, not {@code 1.50D}); the L2 audit, 2026-10-29. The kinds that can cross are NOT compared
     * yet: refused by name (a static false stood here), the homework README's findings; the pairs that never cross
     * keep their false.
     */
    private static void refuseCrossingNumbers(String kindA, @com.legend.base.Nullable SqlType slotA, String kindB,
            @com.legend.base.Nullable SqlType slotB, String operation) {
        if (kindA.equals(kindB)) {
            return;
        }
        boolean crosses = (kindA.equals("Integer") && kindB.equals("Decimal") && scaleZero(slotB))
                || (kindB.equals("Integer") && kindA.equals("Decimal") && scaleZero(slotA))
                || (kindA.equals("Float") && kindB.equals("Decimal"))
                || (kindB.equals("Float") && kindA.equals("Decimal"));
        if (crosses) {
            throw new com.legend.error.NotImplementedException(operation + " across the kinds " + kindA + " ("
                    + slotA + ") and " + kindB + " (" + slotB + "): Pure equates two numbers of different kinds by"
                    + " their texts (CompiledSupport.eq), which is not lowered yet (the L2 audit, 2026-10-29)");
        }
    }

    private static boolean scaleZero(@com.legend.base.Nullable SqlType slot) {
        // a Decimal slot of unknown scale may hold a scale-zero value: it can cross too
        return !(slot instanceof SqlType.Decimal d) || d.scale() == 0;
    }

    private static SqlExpr.TaggedList.@com.legend.base.Nullable Channel channelOfSlot(SqlExpr.TaggedList tl,
            SqlType.Tagged.Slot slot) {
        for (SqlExpr.TaggedList.Channel ch : tl.channels()) {
            if (ch.kind().equals(slot.kind()) && ch.slot().equals(slot.type())) {
                return ch;
            }
        }
        return null;
    }

    /** The positions whose element is PRESENT: a literal collection with an element that may be empty
     *  ({@code [$optional, 'a']}, Pure's collection holds no empties) keeps the positions whose tagged channel holds
     *  a value (the L1 audit, 2026-10-29: the empty element was a position holding NULL). */
    static SqlExpr presentPositions(SqlExpr.TaggedList tl) {
        String p = "_tlq";
        SqlExpr pos = SqlExpr.Column.derived(null, p);
        List<SqlExpr.Case.When> whens = new ArrayList<>(tl.channels().size());
        for (int k = 0; k < tl.channels().size(); k++) {
            whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(pos), new SqlExpr.IntLit(k + 1)),
                    SqlExpr.Call.of(SqlFn.IS_NOT_NULL, tl.channelAt(k, pos))));
        }
        return SqlExpr.Call.of(SqlFn.LIST_FILTER, tl.positions(),
                new SqlExpr.Lambda(List.of(p), SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Case(whens, null, SqlExpr.Reason.CARRIER),
                        new SqlExpr.BoolLit(false))));
    }

    /** An all-NULL channel of {@code slot} over the list's positions: a literal list's n NULLs; a list over its
     *  rows aggregates a NULL per cell. (A reordered list has no such array: its equality reads elements.) */
    private static SqlExpr nullChannel(SqlExpr.TaggedList tl, SqlType slot) {
        if (tl.rows() != null) {
            String v = "_tln";
            var cell = SqlExpr.Column.lookup(v, cellsOf(tl).outputs());
            return new SqlExpr.ScalarSubquery(new SqlSelect(
                    List.of(new SqlSelect.Projection(new SqlExpr.OrderedListAgg(
                            new SqlExpr.Cast(new SqlExpr.NullLit(), slot, SqlExpr.Reason.CARRIER),
                            cell.apply(SqlExpr.TaggedList.ROW_POSITION)), "s", null)),
                    false, new SqlSource.Subselect(cellsOf(tl), v, null), null, List.of(), null, null, List.of(),
                    null, null, List.of()));
        }
        if (!(tl.positions() instanceof SqlExpr.ArrayLit ps)) {
            throw new IllegalStateException("a null channel over positions that are not the list's own: " + tl);
        }
        List<SqlExpr> nulls = new ArrayList<>(ps.elements().size());
        for (int i = 0; i < ps.elements().size(); i++) {
            nulls.add(new SqlExpr.NullLit());
        }
        return new SqlExpr.Cast(new SqlExpr.ArrayLit(nulls), new SqlType.Array(slot), SqlExpr.Reason.CARRIER);
    }

    /** The list on the VARIANT carrier: a JSON array, each element the JSON value of its channel's value (an
     *  enumeration's value its name, as the variant lane carries one) -- the lane a list of no static kind is on
     *  (a row's cells read in a lambda), met there. */
    static SqlExpr asVariantList(SqlExpr.TaggedList tl) {
        String p = "_tlv";
        SqlExpr pos = SqlExpr.Column.derived(null, p);
        List<SqlExpr.Case.When> whens = new ArrayList<>(tl.channels().size());
        for (int k = 0; k < tl.channels().size(); k++) {
            whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(pos), new SqlExpr.IntLit(k + 1)),
                    SqlExpr.Call.of(SqlFn.TO_VARIANT, tl.channelAt(k, pos))));
        }
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, tl.positions(),
                new SqlExpr.Lambda(List.of(p), new SqlExpr.Case(whens, null, SqlExpr.Reason.CARRIER)));
    }

    /** The element at 0-based {@code index}, out of bounds raising Pure's own message. */
    static SqlExpr at(SqlExpr.TaggedList tl, SqlExpr index, com.legend.compiler.spec.typed.TypedSpec list,
            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) {
        SqlExpr size = size(tl);
        SqlExpr oob = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, index, size),
                SqlExpr.Call.of(SqlFn.LESS, index, new SqlExpr.IntLit(0)));
        SqlExpr position = SqlExpr.Call.of(SqlFn.LIST_GET, tl.positions(), plusOne(index));
        return Scalars.guarded(oob,
                new SqlExpr.RaiseText(com.legend.sql.RaiseKind.INDEX_OUT_OF_BOUNDS,
                        List.of(index, Scalars.statedSize(list, size))),
                tl.element(position), pos);
    }

    /** The first element (NULL over an empty list). */
    static SqlExpr first(SqlExpr.TaggedList tl) {
        return tl.element(SqlExpr.Call.of(SqlFn.LIST_GET, tl.positions(), new SqlExpr.IntLit(1)));
    }

    /** The last element (NULL over an empty list). */
    static SqlExpr last(SqlExpr.TaggedList tl) {
        return tl.element(SqlExpr.Call.of(SqlFn.LIST_GET, tl.positions(), new SqlExpr.IntLit(-1)));
    }

    /** The list reversed. */
    static SqlExpr.TaggedList reverse(SqlExpr.TaggedList tl) {
        return tl.withPositions(SqlExpr.Call.of(SqlFn.LIST_REVERSE, tl.positions())).withOrder(tl.order().reversed());
    }

    /**
     * The list sorted by its comparable (Pure's compare over mixed kinds): the positions ordered by the comparable
     * at each -- {@code (SELECT list(p ORDER BY comparable[p]) FROM (SELECT UNNEST(positions) AS p) _tl)}, the
     * spelling carrier's own shape (its ids ordered by its comparables), the identity now a position.
     */
    static SqlExpr.TaggedList sort(SqlExpr.TaggedList tl, String alias) {
        refuseInexactRanks(tl);
        if (tl.rows() != null) {
            return sortRows(tl, alias);
        }
        SqlExpr unnested = SqlExpr.Call.of(SqlFn.UNNEST, tl.positions());
        SqlSelect inner = new SqlSelect(List.of(new SqlSelect.Projection(unnested, "p", null)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
        SqlExpr p = SqlExpr.Column.of(alias, "p", SqlType.Scalar.INTEGER, false, OutputCol.Origin.DERIVED);
        SqlSelect outer = new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.OrderedListAgg(p, tl.comparableAt(p)), "s", null)),
                false, new SqlSource.Subselect(inner, alias, null), null, List.of(), null, null, List.of(),
                null, null, List.of());
        return tl.withPositions(new SqlExpr.ScalarSubquery(outer)).withOrder(SqlExpr.TaggedList.Order.COMPARABLE);
    }

    /**
     * A list over its rows sorted (a relation's cells, {@code $t.rows.values->sort()}): its CELLS renumbered by the
     * comparable, ties by position (a stable sort's), so the sorted list is again a list over its rows, its positions
     * {@code 1..n} in its own order and every array an aggregate over the renumbered cells -- no array read, no
     * lambda: its root reads the cells (every database, H2's list-less dialect included), where reordering the
     * positions read the comparable array at each one ({@code LIST_AT}), which H2 refuses, and nested the cells'
     * own numbering inside the root's order (2026-11-06, the corpus's strictdate::testProject on both lanes). The
     * comparable's channels are the cells' scalar columns ({@code __cmp}, or {@code __cmp_g/n/d/s} for a rank across
     * kinds), compared field by field as the rank's struct compares them, a missing field after a present one.
     */
    /**
     * A mixed list's numbers are ordered by their value as a DOUBLE (MixedEncoding's rank): a Decimal, or an integer
     * beyond 2^53, is not held exactly there, and two such values could be ordered wrongly
     * ({@code [9007199254740993, 9007199254740992, 'a']->min()} answered ...993). Such an order is refused by name at
     * planning until the kinds-kept carrier ranks numbers exactly (2026-11-15, PR1's review, D2).
     */
    static void refuseInexactRanks(SqlExpr.TaggedList tl) {
        List<SqlExpr> scanned = new ArrayList<>();
        scanned.add(tl.comparable());
        if (tl.rows() != null) {
            scanned.add(tl.rows());
        }
        for (SqlExpr e : scanned) {
            String why = inexact(e);
            if (why != null) {
                throw new com.legend.error.NotImplementedException("a mixed list's order over " + why
                        + ": its numbers are ranked as DOUBLE, which does not hold it exactly (refused until the"
                        + " kinds-kept carrier ranks numbers exactly)");
            }
        }
    }

    /** The literal {@code v} is, under any casts, groupings and negations; else {@code v} itself. */
    private static SqlExpr literalUnder(SqlExpr v) {
        SqlExpr at = v;
        while (true) {
            if (at instanceof SqlExpr.Cast c) {
                at = c.value();
            } else if (at instanceof SqlExpr.Group g) {
                at = g.inner();
            } else if (at instanceof SqlExpr.Call c && c.fn() == SqlFn.NEGATE && c.args().size() == 1) {
                // a negative literal ({@code -2.0}): its magnitude decides, and the double's range is symmetric
                at = c.args().get(0);
            } else {
                return at;
            }
        }
    }

    /** Whether the double nearest {@code d} is {@code d} itself (it round-trips). */
    private static boolean exactInADouble(java.math.BigDecimal d) {
        double x = d.doubleValue();
        return Double.isFinite(x) && new java.math.BigDecimal(x).compareTo(d) == 0;
    }

    private static @com.legend.base.Nullable String inexact(SqlExpr e) {
        if (e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.DOUBLE) {
            SqlExpr v = c.value();
            SqlExpr lit = literalUnder(v);
            // a literal is known at planning, whatever casts carry it (an Integer's rank may pass through a DECIMAL
            // cast): it ranks exactly where the double holds its value (round 55f, 2026-11-15: an Integer literal under
            // a DECIMAL cast read as "a Decimal" and refused testMixedSortNoComparator)
            if (lit instanceof SqlExpr.DecimalLit d) {
                if (!exactInADouble(d.value())) {
                    return "the Decimal " + d.value().toPlainString() + " (not exact as a DOUBLE)";
                }
            } else if (lit instanceof SqlExpr.IntLit i) {
                // the double holds it or not (the PR1 re-review's B5, 2026-11-16: 2^53 and 2^60 are exact, and were
                // refused by a magnitude bound)
                if (!exactInADouble(java.math.BigDecimal.valueOf(i.value()))) {
                    return "the integer " + i.value() + " (not exact as a DOUBLE)";
                }
            } else if (!(lit instanceof SqlExpr.FloatLit) && v.type() instanceof com.legend.sql.TypeFact.Typed t
                    && t.type() instanceof SqlType.Decimal) {
                // a Decimal computed at run time: not known at planning
                return "a Decimal";
            }
        }
        if (e instanceof SqlExpr.ScalarSubquery q && q.subquery() instanceof SqlSelect s) {
            for (SqlSelect.Projection pr : s.projections()) {
                String why = inexact(pr.expr());
                if (why != null) {
                    return why;
                }
            }
            if (s.from() instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect in) {
                for (SqlSelect.Projection pr : in.projections()) {
                    String why = inexact(pr.expr());
                    if (why != null) {
                        return why;
                    }
                }
            }
        }
        for (SqlExpr c : e.children()) {
            String why = inexact(c);
            if (why != null) {
                return why;
            }
        }
        return null;
    }

    private static SqlExpr.TaggedList sortRows(SqlExpr.TaggedList tl, String alias) {
        SqlSelect cells = cellsOf(tl);
        var c = SqlExpr.Column.lookup(alias, cells.outputs());
        // the comparable's columns in the cells' order: __cmp, or a rank across kinds' __cmp_<field> columns
        List<SqlSelect.SortKey> keys = new ArrayList<>(5);
        for (OutputCol o : cells.outputs()) {
            if (o.name().equals(SqlExpr.TaggedList.ROW_COMPARABLE)
                    || o.name().startsWith(SqlExpr.TaggedList.ROW_COMPARABLE + "_")) {
                keys.add(new SqlSelect.SortKey(c.apply(o.name()), true, SqlSelect.SortKey.NullOrder.NULLS_LAST,
                        null));
            }
        }
        keys.add(new SqlSelect.SortKey(c.apply(SqlExpr.TaggedList.ROW_POSITION), true, null, null));
        List<SqlSelect.Projection> renumbered = new ArrayList<>(cells.outputs().size());
        for (OutputCol o : cells.outputs()) {
            renumbered.add(o.name().equals(SqlExpr.TaggedList.ROW_POSITION)
                    ? new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER,
                            List.of()), List.of(), keys, null), o.name(), o)
                    : new SqlSelect.Projection(c.apply(o.name()), o.name(), o));
        }
        SqlSelect sorted = new SqlSelect(renumbered, false, new SqlSource.Subselect(cells, alias, null), null,
                List.of(), null, null, List.of(), null, null, cells.outputs());
        // the arrays: ordered aggregates over the renumbered cells, the one construction of a list over its rows
        return ValueCollections.overCells(sorted, tl.slots(), alias + "r");
    }

    /**
     * The list's duplicates removed, each element's FIRST occurrence kept ({@code removeDuplicates}): the kept-list
     * reduce over the positions ({@link Dedup#keptDedup}, the spelling carrier's shape), a candidate dropped when a
     * kept position holds the same element -- {@code same} over two positions.
     */
    static SqlExpr.TaggedList dedup(SqlExpr.TaggedList tl, int depth, BinaryOperator<SqlExpr> same) {
        return tl.withPositions(Dedup.keptDedup(tl.positions(), depth, same));
    }

    /** Two positions of {@code tl} hold the same element: channels of one family ({@link MixedEncoding#family}), and
     *  their values equal. Where every family is one channel (no Decimal at a second scale, no date-time at a second
     *  written precision) that is the same channel, read once. */
    static SqlExpr sameElement(SqlExpr.TaggedList tl, SqlExpr p, SqlExpr q) {
        if (!oneChannelPerFamily(tl)) {
            List<SqlExpr> arms = new ArrayList<>();
            for (int k = 0; k < tl.channels().size(); k++) {
                for (int j = 0; j < tl.channels().size(); j++) {
                    if (MixedEncoding.sameFamily(tl.channels().get(k).kind(), tl.channels().get(j).kind())) {
                        arms.add(SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND,
                                        SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(p), new SqlExpr.IntLit(k + 1)),
                                        SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(q), new SqlExpr.IntLit(j + 1))),
                                SqlExpr.Call.of(SqlFn.EQUAL, tl.channelAt(k, p), tl.channelAt(j, q))));
                    }
                }
            }
            return anyOf(arms);
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>(tl.channels().size());
        for (int k = 0; k < tl.channels().size(); k++) {
            whens.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(p), new SqlExpr.IntLit(k + 1)),
                    SqlExpr.Call.of(SqlFn.EQUAL, tl.channelAt(k, p), tl.channelAt(k, q))));
        }
        return SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(p), tl.tagAt(q)),
                SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Case(whens, null, SqlExpr.Reason.CARRIER), new SqlExpr.BoolLit(false)));
    }

    /** A tagged value equals a value of the static {@code kind}: its tag names that kind's candidate, whose value
     *  equals it; no candidate of the kind, never. A value of no static kind ({@code valueType} Number, Any, Date:
     *  a kind only its runtime value names, which Pure's {@code eq} reads and no channel does) is refused by name,
     *  never answered false (the L1 audit, 2026-10-29). */
    static SqlExpr sameElement(SqlExpr.Tagged x, @com.legend.base.Nullable String kind, SqlExpr value,
            Type valueType) {
        if (kind == null) {
            throw new com.legend.error.NotImplementedException("a tagged value compared with a value of type "
                    + valueType + ", whose kind only its runtime value names: not lowered");
        }
        List<SqlExpr> arms = new ArrayList<>();
        for (int i = 0; i < x.candidates().size(); i++) {
            SqlExpr.Tagged.Candidate a = x.candidates().get(i);
            if (!MixedEncoding.sameFamily(a.kind(), kind)) {
                continue;
            }
            arms.add(SqlExpr.Call.of(SqlFn.AND,
                    SqlExpr.Call.of(SqlFn.EQUAL, x.tag(), new SqlExpr.IntLit(i + 1)),
                    SqlExpr.Call.of(SqlFn.EQUAL, a.value(), value)));
        }
        return anyOf(arms);
    }

    /** Two tagged lists are equal: the same size, and at each index the same element. */
    static SqlExpr sameList(SqlExpr.TaggedList a, SqlExpr.TaggedList b) {
        String p = "_tlp";
        String i = "_tli";
        SqlExpr.Tagged other = b.element(SqlExpr.Call.of(SqlFn.LIST_GET, b.positions(), SqlExpr.Column.derived(null, i)));
        SqlExpr differing = SqlExpr.Call.of(SqlFn.LIST_FILTER, a.positions(),
                new SqlExpr.Lambda(List.of(p, i), SqlExpr.Call.of(SqlFn.NOT,
                        sameElement(a.element(SqlExpr.Column.derived(null, p)), other))));
        return SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, size(a), size(b)),
                SqlExpr.Call.of(SqlFn.EQUAL,
                        SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Call.of(SqlFn.LIST_LENGTH, differing),
                                new SqlExpr.IntLit(0)),
                        new SqlExpr.IntLit(0)));
    }

    /** A tagged list equals a list of one {@code kind}: the same size, and at each index the tagged element is of
     *  that kind and equals the other list's element there. */
    static SqlExpr sameListOfKind(SqlExpr.TaggedList a, String kind, SqlExpr other) {
        String p = "_tlp";
        String i = "_tli";
        SqlExpr differing = SqlExpr.Call.of(SqlFn.LIST_FILTER, a.positions(),
                new SqlExpr.Lambda(List.of(p, i), SqlExpr.Call.of(SqlFn.NOT,
                        sameElement(a.element(SqlExpr.Column.derived(null, p)), kind,
                                SqlExpr.Call.of(SqlFn.LIST_GET, other, SqlExpr.Column.derived(null, i)),
                                MixedEncoding.kindType(kind)))));
        return SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, size(a),
                        SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Call.of(SqlFn.LIST_LENGTH, other),
                                new SqlExpr.IntLit(0))),
                SqlExpr.Call.of(SqlFn.EQUAL,
                        SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Call.of(SqlFn.LIST_LENGTH, differing),
                                new SqlExpr.IntLit(0)),
                        new SqlExpr.IntLit(0)));
    }

    private static SqlExpr anyOf(List<SqlExpr> arms) {
        if (arms.isEmpty()) {
            return new SqlExpr.BoolLit(false);
        }
        SqlExpr any = arms.get(0);
        for (int i = 1; i < arms.size(); i++) {
            any = SqlExpr.Call.of(SqlFn.OR, any, arms.get(i));
        }
        return SqlExpr.Call.of(SqlFn.COALESCE, any, new SqlExpr.BoolLit(false));
    }

    /** Two tagged values are the same element: a channel of each of one family ({@link MixedEncoding#family}), the
     *  tags naming them, the values equal as the database compares the type (a Decimal by its number, a date-time
     *  by its instant). A family one side has no channel for is never equal. */
    static SqlExpr sameElement(SqlExpr.Tagged x, SqlExpr.Tagged y) {
        List<SqlExpr> arms = new ArrayList<>();
        for (int i = 0; i < x.candidates().size(); i++) {
            for (int j = 0; j < y.candidates().size(); j++) {
                SqlExpr.Tagged.Candidate a = x.candidates().get(i);
                SqlExpr.Tagged.Candidate b = y.candidates().get(j);
                if (!MixedEncoding.sameFamily(a.kind(), b.kind())) {
                    refuseCrossingNumbers(a.kind(), a.slot(), b.kind(), b.slot(), "two mixed values' equality");
                    continue;
                }
                arms.add(SqlExpr.Call.of(SqlFn.AND,
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.EQUAL, x.tag(), new SqlExpr.IntLit(i + 1)),
                                SqlExpr.Call.of(SqlFn.EQUAL, y.tag(), new SqlExpr.IntLit(j + 1))),
                        SqlExpr.Call.of(SqlFn.EQUAL, a.value(), b.value())));
            }
        }
        return anyOf(arms);
    }

    /**
     * Whether the list holds {@code needle}, a value of the static {@code kind}: a position whose channel is of the
     * needle's kind's family ({@link MixedEncoding#family}: a Decimal at any scale, a date-time at any written
     * precision) and whose value there equals the needle, by the database's own comparison of the type (a Decimal
     * by its number, a date-time by its instant); a family the list has no channel for is a member of nothing
     * ({@code '1'} is not {@code 1}; {@code 1} is not {@code 1.0}), but for the number kinds Pure's text equality
     * can cross ({@link #refuseCrossingNumbers}).
     */
    static SqlExpr contains(SqlExpr.TaggedList tl, @com.legend.base.Nullable String kind, SqlExpr needle,
            Type needleType) {
        if (kind == null) {
            throw new com.legend.error.NotImplementedException("a mixed list's membership of a value of type "
                    + needleType + ", whose kind only its runtime value names: not lowered");
        }
        List<Integer> own = MixedEncoding.channelsOf(tl, kind);
        if (own.isEmpty()) {
            SqlType needleSlot = needle.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
            for (SqlExpr.TaggedList.Channel ch : tl.channels()) {
                refuseCrossingNumbers(kind, needleSlot, ch.kind(), ch.slot(), "a mixed list's membership");
            }
            return new SqlExpr.BoolLit(false);
        }
        List<SqlExpr.Tagged.Candidate> written = tl.literalElements();
        // (an element of another type is not written at all, so the plain form is taken only where no element can
        // raise: its error would be lost; the re-check of this slice's audit, 2026-11-26)
        if (written != null && raiseNothing(written)) {
            // A LIST WRITTEN IN THE QUERY needs no list at all (the user, 2026-11-26, on the measured SQL of the fold
            // rule): each element's kind is the compiler's, and only an element of the needle's kind can equal it
            // (the equality rule), so membership is the plain IN over those elements, as a list of one type writes
            // it. The elements are chosen by their TYPE; every value is compared by the database. Every element of
            // the kind's family, whichever slot holds it: Decimals of two scales are one type, equal by their number
            // (S139); date-times at two written precisions, by their instant (S399)
            List<SqlExpr> flat = new ArrayList<>();
            flat.add(needle);
            for (SqlExpr.Tagged.Candidate c : written) {
                if (MixedEncoding.sameFamily(c.kind(), kind)) {
                    flat.add(c.value());
                }
            }
            // one element of the type: an equality (IN over one value that is a parameter is read as a LIST
            // parameter by every dialect and refused; the audit of this slice, 2026-11-26)
            SqlExpr found = flat.size() == 2 ? SqlExpr.Call.of(SqlFn.EQUAL, flat.get(0), flat.get(1))
                    : new SqlExpr.Call(SqlFn.IN, flat);
            return SqlExpr.Call.of(SqlFn.COALESCE, found, new SqlExpr.BoolLit(false));
        }
        String p = "_tlp";
        List<SqlExpr> inChannel = new ArrayList<>(own.size());
        for (int k : own) {
            inChannel.add(SqlExpr.Call.of(SqlFn.AND,
                    SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(SqlExpr.Column.derived(null, p)), new SqlExpr.IntLit(k + 1)),
                    SqlExpr.Call.of(SqlFn.EQUAL, tl.channelAt(k, SqlExpr.Column.derived(null, p)), needle)));
        }
        SqlExpr hit = inChannel.get(0);
        for (int i = 1; i < inChannel.size(); i++) {
            hit = SqlExpr.Call.of(SqlFn.OR, hit, inChannel.get(i));
        }
        return SqlExpr.Call.of(SqlFn.COALESCE,
                SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, SqlExpr.Call.of(SqlFn.LIST_FILTER, tl.positions(),
                                new SqlExpr.Lambda(List.of(p), hit))),
                        new SqlExpr.IntLit(0)),
                new SqlExpr.BoolLit(false));
    }

    /** Whether the list holds the tagged value {@code needle} (a mixed selection's winner): a position whose element
     *  is the same element ({@link #sameElement(SqlExpr.Tagged, SqlExpr.Tagged)}). */
    static SqlExpr containsElement(SqlExpr.TaggedList tl, SqlExpr.Tagged needle) {
        String p = "_tlp";
        SqlExpr pos = SqlExpr.Column.derived(null, p);
        return SqlExpr.Call.of(SqlFn.COALESCE,
                SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, SqlExpr.Call.of(SqlFn.LIST_FILTER, tl.positions(),
                                new SqlExpr.Lambda(List.of(p), sameElement(tl.element(pos), needle)))),
                        new SqlExpr.IntLit(0)),
                new SqlExpr.BoolLit(false));
    }

    /** The positions whose element satisfies {@code predicate}, a lambda over a position (the binding convention:
     *  its parameter is the element there). */
    static SqlExpr.TaggedList filter(SqlExpr.TaggedList tl, SqlExpr.Lambda predicate) {
        return tl.withPositions(SqlExpr.Call.of(SqlFn.LIST_FILTER, tl.positions(), predicate));
    }

    /** Whether some element satisfies {@code predicate} (false over the empty list). */
    static SqlExpr exists(SqlExpr.TaggedList tl, SqlExpr.Lambda predicate) {
        return SqlExpr.Call.of(SqlFn.COALESCE,
                SqlExpr.Call.of(SqlFn.GREATER, size(filter(tl, predicate)), new SqlExpr.IntLit(0)),
                new SqlExpr.BoolLit(false));
    }

    /** Whether every element satisfies {@code predicate} (true over the empty list). */
    static SqlExpr forAll(SqlExpr.TaggedList tl, SqlExpr.Lambda predicate) {
        return SqlExpr.Call.of(SqlFn.EQUAL, size(filter(tl, predicate)), size(tl));
    }

    /** Each element through {@code f}, a lambda over a position: a list of the results. */
    static SqlExpr map(SqlExpr.TaggedList tl, SqlExpr.Lambda f) {
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, tl.positions(), f);
    }

    /** The list's hash: the hash of the list of its elements' hashes, each element hashed in its own kind. */
    static SqlExpr hash(SqlExpr.TaggedList tl) {
        String p = "_tlp";
        SqlExpr hashes = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, tl.positions(), new SqlExpr.Lambda(List.of(p),
                MixedEncoding.perCandidate(tl.element(SqlExpr.Column.derived(null, p)),
                        (k, v) -> SqlExpr.Call.of(SqlFn.HASH, v))));
        return SqlExpr.Call.of(SqlFn.HASH, hashes);
    }

    /** The list's print, Pure's: {@code [e1, e2, ...]}, each element printed as its kind. */
    static SqlExpr print(Styled styled, SqlExpr.TaggedList tl) {
        // the style's two-form kind (B8, 2026-11-17): the product's JSON array (the user's ruling of 2026-11-10), the
        // edge's legend-pure [e1, e2]
        return styled.lists().mixedList(styled, tl);
    }

    /** A mixed list as a JSON array: each element as its kind writes a JSON value -- a number and a Boolean as their
     *  text, anything else a JSON string of its text. */
    static SqlExpr json(Styled styled, SqlExpr.TaggedList tl) {
        String p = "_tlj";
        // through map's one carrier emission (the carrier purity ratchet: no new direct LIST_ site)
        SqlExpr texts = map(tl, new SqlExpr.Lambda(List.of(p),
                MixedEncoding.perCandidate(tl.element(SqlExpr.Column.derived(null, p)), (k, v) -> jsonValue(styled, k, v))));
        return Scalars.cat(new SqlExpr.StringLit("["),
                SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG, texts, List.of(new SqlExpr.StringLit(","))),
                        new SqlExpr.StringLit("")),
                new SqlExpr.StringLit("]"));
    }

    /** {@code v} of kind {@code k} as a JSON value's text. */
    static SqlExpr jsonValue(Styled styled, Type k, SqlExpr v) {
        SqlExpr text = Scalars.pureToString(styled, k, v);
        if (k == Type.Primitive.INTEGER || k == Type.Primitive.FLOAT || k == Type.Primitive.DECIMAL
                || k == Type.Primitive.NUMBER || k == Type.Primitive.BOOLEAN) {
            return text;
        }
        SqlExpr escaped = SqlExpr.Call.of(SqlFn.REPLACE, SqlExpr.Call.of(SqlFn.REPLACE, text,
                new SqlExpr.StringLit("\\"), new SqlExpr.StringLit("\\\\")),
                new SqlExpr.StringLit("\""), new SqlExpr.StringLit("\\\""));
        return Scalars.cat(new SqlExpr.StringLit("\""), escaped, new SqlExpr.StringLit("\""));
    }

    /** The elements kept whose key's first position is their own ({@code removeDuplicatesBy}): {@code key} a lambda
     *  over a position, the spelling carrier's shape ({@code (x, i) -> LIST_POSITION(keys, key(x)) = i}). */
    static SqlExpr.TaggedList dedupBy(SqlExpr.TaggedList tl, SqlExpr.Lambda key) {
        String x = key.params().get(0);
        SqlExpr keys = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, tl.positions(), key);
        return tl.withPositions(SqlExpr.Call.of(SqlFn.LIST_FILTER, tl.positions(),
                new SqlExpr.Lambda(List.of(x, "_rd_i"),
                        SqlExpr.Call.of(SqlFn.EQUAL,
                                SqlExpr.Call.of(SqlFn.LIST_POSITION, keys, key.body()),
                                SqlExpr.Column.derived(null, "_rd_i")))));
    }

    /** Whether the list holds an element {@code comparator} (a lambda over the needle, then a position) accepts. */
    static SqlExpr containsBy(SqlExpr.TaggedList tl, SqlExpr needle, SqlExpr.Lambda comparator) {
        SqlExpr body = Scalars.substituteRef(comparator.body(), comparator.params().get(0), needle);
        return SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.LIST_LENGTH, SqlExpr.Call.of(SqlFn.LIST_FILTER, tl.positions(),
                        new SqlExpr.Lambda(List.of(comparator.params().get(1)), body))),
                new SqlExpr.IntLit(0));
    }

    /**
     * The list's egress at a query's root (the Lowerer's; docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): ROWS of a
     * tagged value -- the positions unnested in an inner select, the root projecting {@code value}, the slot index
     * the tag at the position names (typed {@link SqlType.Tagged}), and one column per slot
     * ({@link SqlType.Tagged#slotColumn}) holding that channel's value at the position, so the plan's one column is
     * {@code TAGGED<kind slot, ...>} with the list's multiplicity, and the decoders read each row as a tagged value.
     */
    static SqlSelect root(SqlExpr.TaggedList tl, TypedSpec spec, String alias) {
        return root(tl, PureSql.nullable(spec.info().multiplicity()), alias);
    }

    /** {@link #root}, the column's nullability given ({@code nullable}): a list the judge reads, not a query's root. */
    static SqlSelect root(SqlExpr.TaggedList tl, boolean nullable, String alias) {
        List<SqlType.Tagged.Slot> slots = tl.slots();
        SqlType.Tagged carrier = new SqlType.Tagged(slots);
        if (tl.rows() != null) {
            // a list over its rows: the cells themselves, in order (no array read: every database)
            SqlSelect cells = cellsOf(tl);
            var cell = SqlExpr.Column.lookup(alias, cells.outputs());
            SqlExpr tag = cell.apply(SqlExpr.TaggedList.ROW_TAG);
            List<SqlExpr.Case.When> index = new ArrayList<>();
            for (int k = 0; k < slots.size(); k++) {
                index.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(k + 1)),
                        new SqlExpr.IntLit(k)));
            }
            List<SqlSelect.Projection> out = new ArrayList<>();
            out.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.Case(index, null, SqlExpr.Reason.CARRIER), carrier, SqlExpr.Reason.CARRIER), "value",
                    new OutputCol("value", carrier, nullable)));
            for (int k = 0; k < slots.size(); k++) {
                String name = SqlType.Tagged.slotColumn("value", k);
                out.add(new SqlSelect.Projection(cell.apply(SqlExpr.TaggedList.ROW_CHANNEL(k)), name,
                        new OutputCol(name, slots.get(k).type(), true)));
            }
            return new SqlSelect(out, false, new SqlSource.Subselect(cells, alias, null), null, List.of(), null,
                    null, List.of(new SqlSelect.SortKey(cell.apply(SqlExpr.TaggedList.ROW_POSITION), true, null,
                            null)), null, null, List.of());
        }
        SqlSelect inner = new SqlSelect(List.of(new SqlSelect.Projection(
                SqlExpr.Call.of(SqlFn.UNNEST, tl.positions()), "__p",
                new OutputCol("__p", SqlType.Scalar.INTEGER, false))),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
        SqlExpr p = SqlExpr.Column.of(alias, "__p", SqlType.Scalar.INTEGER, false, OutputCol.Origin.DERIVED);
        // the slot index is the tag less one (the tags are 1..K), read once: a database that writes the unnested
        // position at each read of it (Snowflake, SQLite, MariaDB, MySQL) wrote the position list per channel in a CASE
        SqlExpr index = SqlExpr.Call.of(SqlFn.MINUS, tl.tagAt(p), new SqlExpr.IntLit(1));
        List<SqlSelect.Projection> out = new ArrayList<>();
        out.add(new SqlSelect.Projection(new SqlExpr.Cast(index, carrier, SqlExpr.Reason.CARRIER), "value",
                new OutputCol("value", carrier, nullable)));
        for (int k = 0; k < slots.size(); k++) {
            String name = SqlType.Tagged.slotColumn("value", k);
            out.add(new SqlSelect.Projection(tl.channelAt(k, p), name,
                    new OutputCol(name, slots.get(k).type(), true)));
        }
        // the rows in the list's own order (an unnest promises none: round 18's BigQuery and MySQL): by position, or by
        // comparable with the position after it (a stable sort's ties), either way
        boolean desc = tl.order() == SqlExpr.TaggedList.Order.POSITION_DESC
                || tl.order() == SqlExpr.TaggedList.Order.COMPARABLE_DESC;
        List<SqlSelect.SortKey> order = new ArrayList<>();
        if (tl.order() == SqlExpr.TaggedList.Order.COMPARABLE || tl.order() == SqlExpr.TaggedList.Order.COMPARABLE_DESC) {
            order.add(new SqlSelect.SortKey(tl.comparableAt(p), !desc, null, null));
        }
        order.add(new SqlSelect.SortKey(p, !desc, null, null));
        return new SqlSelect(out, false, new SqlSource.Subselect(inner, alias, null), null, List.of(), null, null,
                order, null, null, List.of());
    }
}
