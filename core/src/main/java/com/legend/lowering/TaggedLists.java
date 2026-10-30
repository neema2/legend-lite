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

    /**
     * Two lists in their own order ({@link SqlExpr.TaggedList#identityPositions}) are the same list: the same size,
     * and for every kind either holds, the same channel array (null-safe: a NULL where the element is of another
     * kind meets a NULL) -- a kind one side has no channel for meets an all-NULL array of the other's length. No
     * lambda, no element read: the shape every database compares (H2 compares arrays, and runs no list lambda).
     */
    static SqlExpr sameListByKinds(SqlExpr.TaggedList a, SqlExpr.TaggedList b) {
        List<SqlExpr> conditions = new ArrayList<>();
        conditions.add(SqlExpr.Call.of(SqlFn.EQUAL, size(a), size(b)));
        // every (kind, slot) either side holds, exactly: a Decimal at another scale, a DateTime at another written
        // precision, is another kind (Pure's BigDecimal and PureDate equality: the scale, the subsecond text), so
        // its values meet an all-NULL array (the L1 audit, 2026-10-29)
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
                new SqlExpr.Lambda(List.of(p), SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Case(whens, null),
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
                            new SqlExpr.Cast(new SqlExpr.NullLit(), slot),
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
        return new SqlExpr.Cast(new SqlExpr.ArrayLit(nulls), new SqlType.Array(slot));
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
                new SqlExpr.Lambda(List.of(p), new SqlExpr.Case(whens, null)));
    }

    /** The element at 0-based {@code index}, out of bounds raising Pure's own message. */
    static SqlExpr at(SqlExpr.TaggedList tl, SqlExpr index,
            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) {
        SqlExpr size = size(tl);
        SqlExpr oob = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, index, size),
                SqlExpr.Call.of(SqlFn.LESS, index, new SqlExpr.IntLit(0)));
        SqlExpr position = SqlExpr.Call.of(SqlFn.LIST_GET, tl.positions(), plusOne(index));
        return Scalars.guarded(oob,
                Scalars.cat(new SqlExpr.StringLit("The system is trying to get an element at offset "),
                        Scalars.str(index),
                        new SqlExpr.StringLit(" where the collection is of size "),
                        Scalars.str(size)),
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
        return tl.withPositions(SqlExpr.Call.of(SqlFn.LIST_REVERSE, tl.positions()));
    }

    /**
     * The list sorted by its comparable (Pure's compare over mixed kinds): the positions ordered by the comparable
     * at each -- {@code (SELECT list(p ORDER BY comparable[p]) FROM (SELECT UNNEST(positions) AS p) _tl)}, the
     * spelling carrier's own shape (its ids ordered by its comparables), the identity now a position.
     */
    static SqlExpr.TaggedList sort(SqlExpr.TaggedList tl, String alias) {
        SqlExpr unnested = SqlExpr.Call.of(SqlFn.UNNEST, tl.positions());
        SqlSelect inner = new SqlSelect(List.of(new SqlSelect.Projection(unnested, "p", null)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
        SqlExpr p = SqlExpr.Column.of(alias, "p", SqlType.Scalar.INTEGER, false, OutputCol.Origin.DERIVED);
        SqlSelect outer = new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.OrderedListAgg(p, tl.comparableAt(p)), "s", null)),
                false, new SqlSource.Subselect(inner, alias, null), null, List.of(), null, null, List.of(),
                null, null, List.of());
        return tl.withPositions(new SqlExpr.ScalarSubquery(outer));
    }

    /**
     * The list's duplicates removed, each element's FIRST occurrence kept ({@code removeDuplicates}): the kept-list
     * reduce over the positions ({@link Dedup#keptDedup}, the spelling carrier's shape), a candidate dropped when a
     * kept position holds the same element -- {@code same} over two positions.
     */
    static SqlExpr.TaggedList dedup(SqlExpr.TaggedList tl, int depth, BinaryOperator<SqlExpr> same) {
        return tl.withPositions(Dedup.keptDedup(tl.positions(), depth, same));
    }

    /** Two positions of {@code tl} hold the same element: the same channel, and that channel's values equal. */
    static SqlExpr sameElement(SqlExpr.TaggedList tl, SqlExpr p, SqlExpr q) {
        List<SqlExpr.Case.When> whens = new ArrayList<>(tl.channels().size());
        for (int k = 0; k < tl.channels().size(); k++) {
            whens.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(p), new SqlExpr.IntLit(k + 1)),
                    SqlExpr.Call.of(SqlFn.EQUAL, tl.channelAt(k, p), tl.channelAt(k, q))));
        }
        return SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(p), tl.tagAt(q)),
                SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Case(whens, null), new SqlExpr.BoolLit(false)));
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
            if (!a.kind().equals(kind)) {
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

    /** Two tagged values are the same element: a channel of each with the same kind, both tags naming it, the
     *  values equal. A kind one side has no channel for is never equal. */
    static SqlExpr sameElement(SqlExpr.Tagged x, SqlExpr.Tagged y) {
        List<SqlExpr> arms = new ArrayList<>();
        for (int i = 0; i < x.candidates().size(); i++) {
            for (int j = 0; j < y.candidates().size(); j++) {
                SqlExpr.Tagged.Candidate a = x.candidates().get(i);
                SqlExpr.Tagged.Candidate b = y.candidates().get(j);
                if (!a.kind().equals(b.kind())) {
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
     * Whether the list holds {@code needle}, a value of the static {@code kind}: a position whose channel is the
     * needle's kind's and whose value there equals the needle; a kind the list has no channel for is a member of
     * nothing ({@code '1'} is not {@code 1}; {@code 1} is not {@code 1.0}: a double's text differs), but for the
     * number kinds Pure's text equality can cross ({@link #refuseCrossingNumbers}).
     */
    static SqlExpr contains(SqlExpr.TaggedList tl, @com.legend.base.Nullable String kind, SqlExpr needle,
            Type needleType) {
        if (kind == null) {
            throw new com.legend.error.NotImplementedException("a mixed list's membership of a value of type "
                    + needleType + ", whose kind only its runtime value names: not lowered");
        }
        int k = MixedEncoding.channelOf(tl, kind);
        if (k < 0) {
            SqlType needleSlot = needle.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
            for (SqlExpr.TaggedList.Channel ch : tl.channels()) {
                refuseCrossingNumbers(kind, needleSlot, ch.kind(), ch.slot(), "a mixed list's membership");
            }
            return new SqlExpr.BoolLit(false);
        }
        String p = "_tlp";
        SqlExpr hit = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, tl.tagAt(SqlExpr.Column.derived(null, p)), new SqlExpr.IntLit(k + 1)),
                SqlExpr.Call.of(SqlFn.EQUAL, tl.channelAt(k, SqlExpr.Column.derived(null, p)), needle));
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
    static SqlExpr print(SqlExpr.TaggedList tl) {
        String p = "_tlp";
        SqlExpr texts = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, tl.positions(), new SqlExpr.Lambda(List.of(p),
                Scalars.pureToString(new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY),
                        tl.element(SqlExpr.Column.derived(null, p)))));
        return Scalars.cat(new SqlExpr.StringLit("["),
                SqlExpr.Call.of(SqlFn.COALESCE,
                        new SqlExpr.ReduceCollection(SqlAgg.Fn.STRING_AGG, texts,
                                List.of(new SqlExpr.StringLit(", "))),
                        new SqlExpr.StringLit("")),
                new SqlExpr.StringLit("]"));
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
            out.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.Case(index, null), carrier), "value",
                    new OutputCol("value", carrier, PureSql.nullable(spec.info().multiplicity()))));
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
        SqlExpr tag = tl.tagAt(p);
        List<SqlExpr.Case.When> index = new ArrayList<>();
        for (int k = 0; k < slots.size(); k++) {
            index.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(k + 1)),
                    new SqlExpr.IntLit(k)));
        }
        List<SqlSelect.Projection> out = new ArrayList<>();
        out.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.Case(index, null), carrier), "value",
                new OutputCol("value", carrier, PureSql.nullable(spec.info().multiplicity()))));
        for (int k = 0; k < slots.size(); k++) {
            String name = SqlType.Tagged.slotColumn("value", k);
            out.add(new SqlSelect.Projection(tl.channelAt(k, p), name,
                    new OutputCol(name, slots.get(k).type(), true)));
        }
        return new SqlSelect(out, false, new SqlSource.Subselect(inner, alias, null), null, List.of(), null, null,
                List.of(), null, null, List.of());
    }
}
