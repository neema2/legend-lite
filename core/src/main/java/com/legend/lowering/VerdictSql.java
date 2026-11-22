// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;

/**
 * DATABASE-MODE JUDGING, leg 3.1 (docs/DATABASE_MODE_HOMEWORK_2026_09_18.md
 * §4b): ONE statement decides an equality assert. Both sides are the
 * canon-wrapped side plans the verdict lane already lowers
 * ({@link CanonicalRenderSql#wrapWithCanon}); this class composes them
 * into
 *
 * <pre>
 * WITH __e AS (SELECT canon AS __c, row_number() OVER () AS __rn FROM (e) w),
 *      __a AS (...)
 * SELECT (frame(__e) IS NOT DISTINCT FROM frame(__a)) AS __verdict,
 *        frame(__e) AS __expected, frame(__a) AS __actual,
 *        CASE WHEN a null canon cell THEN 'null-canon-cell' END AS __unjudged
 * </pre>
 *
 * where {@code frame} is CANONICAL_FORM_SPEC's side framing in SQL — the
 * same rule {@code AssertVerdicts.frame} applied in Java: no element
 * {@code '[]'}, one element its bare text, many {@code '[a, b]'} in the
 * side's order (arrival order for an ordered side, canon-text order for a
 * multiset side). The verdict column can never be NULL (P-19: a NULL
 * verdict read as a pass was the highest-ranked risk); the evidence
 * columns ARE the failure message; {@code __unjudged} names a shape the
 * statement could not decide, and the caller FAILS the assert with it.
 * No Java compares a value.
 */
public final class VerdictSql {

    private VerdictSql() {
    }

    public static final String VERDICT = "__verdict";
    public static final String EXPECTED = "__expected";
    public static final String ACTUAL = "__actual";
    public static final String UNJUDGED = "__unjudged";
    /** True when the verdict held ONLY through the declared 2-ULP Float
     * leniency (the harness counts it, as host mode counts its own). */
    public static final String LENIENT = "__lenient";
    /** The verdict under the assert's polarity (the test-is-the-product plan's M8b, MV2/MV3, 2026-11-01): what the
     *  judge raises on when false. */
    public static final String HOLDS = "__holds";
    /** The failure text the judge raises with, composed in the statement (M8b): the family's name and the two
     *  canons each on its line, or the negated families' one sentence; computed on every row, read where the row does
     *  not hold. */
    public static final String MESSAGE = "__message";
    /** A planning-time hint the frame reads and drops: the sides were compared as MULTISETS, so lite's sentence is
     *  {@code AssertKind.SAME_ELEMENTS} ("in any order") whatever the assert's name. */
    private static final String MULTISET_HINT = "__multiset";
    /** A planning-time hint the frame reads and drops: the negated assert is {@code assertIsNot} (#28), its sentence
     *  {@code AssertKind.IS_NOT} (legend-pure's 'Should not be identical to: %r'), not the not-equal one. */
    private static final String IS_NOT_HINT = "__isnot";
    /** A planning-time hint the frame reads and drops: the two sides are JSON documents, the sentence
     *  {@code AssertKind.JSON_EQUALS}. */
    private static final String JSON_HINT = "__json";
    /** Each side's kind as lite's sentence names it where the two texts tie: the frame's, dropped there. */
    private static final String EXPECTED_KIND = "__ekind";
    private static final String ACTUAL_KIND = "__akind";

    /** {@code message} inside the AT_INDEX frame (item 3, 2026-11-21: a frame of the record, no longer a sentence of
     *  its own): the message, then the frame's text with the index last, as {@link com.legend.error.FrameKind#wrap}
     *  words it. Until item 3's S4, which carries the database judge's values to the record and deletes this SQL. */
    public static SqlExpr atIndexFrame(SqlExpr message, SqlExpr index) {
        return new SqlExpr.Call(SqlFn.CONCAT, List.of(message,
                new SqlExpr.StringLit(com.legend.error.FrameKind.AT_INDEX.wrap(List.of(""), "")), index));
    }

    /** {@code row}'s sentence as SQL: its text pieces as literals and each slot the expression {@code slots} names, one
     *  concatenation -- the database judge's twin of the host's {@code AssertKind.fill} (the user's judge model,
     *  2026-11-17: one table for both judges). */
    public static SqlExpr sentence(com.legend.error.AssertKind row, java.util.Map<String, SqlExpr> slots) {
        if (!slots.keySet().equals(new java.util.HashSet<>(row.slots()))) {
            throw new IllegalArgumentException(row.name() + ": slots " + row.slots() + ", given " + slots.keySet());
        }
        List<SqlExpr> parts = new ArrayList<>();
        for (com.legend.error.AssertKind.Piece p : row.pieces()) {
            parts.add(switch (p) {
                case com.legend.error.AssertKind.Text t -> new SqlExpr.StringLit(t.text());
                case com.legend.error.AssertKind.Slot slot -> slots.get(slot.name());
            });
        }
        return parts.size() == 1 ? parts.get(0) : new SqlExpr.Call(SqlFn.CONCAT, parts);
    }

    /** {@code row}'s sentence with no slot, as a literal. */
    public static String sentence(com.legend.error.AssertKind row) {
        return row.fill(java.util.Map.of());
    }

    /** One canon-wrapped side: the wrapped plan, the name of the canon
     * column that decides (one of {@code __canon<i>}), whether the side
     * is a collection, and whether its elements order by canon text (the
     * multiset forms) or by arrival (an ordered assert). */
    /** {@code decimalByValue}: a Decimal side compared by its value, the scale dropped from its canon (register S139,
     *  lite's own rules); compatibility mode compares the canon as written, scale and all. */
    public record Side(SqlQuery wrapped, String canonColumn, boolean many,
            boolean byCanonText, boolean isFloat, @com.legend.base.Nullable Message message, boolean decimalByValue) {
        public Side(SqlQuery wrapped, String canonColumn, boolean many, boolean byCanonText, boolean isFloat) {
            this(wrapped, canonColumn, many, byCanonText, isFloat, null, false);
        }
    }

    /** A Decimal canon's text with its scale dropped ({@code 0.30} and {@code 0.3} one value, {@code 1.00} and
     *  {@code 1.0} one): the trailing zeros of a fraction cut, then a bare point (S139). */
    private static SqlExpr decimalValue(SqlExpr canon) {
        SqlExpr trimmed = SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, canon,
                new SqlExpr.StringLit("0+$"), new SqlExpr.StringLit("")), new SqlExpr.StringLit("[.]$"),
                new SqlExpr.StringLit(""));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, canon, new SqlExpr.StringLit(".")), new SqlExpr.IntLit(0)), trimmed)),
                canon, SqlExpr.Reason.JUDGE);
    }

    private static SqlExpr canonOf(Side s, SqlExpr canon) {
        return s.decimalByValue() ? decimalValue(canon) : canon;
    }

    /**
     * How a side is WRITTEN in the failure message, where its elements are of one known primitive {@code kind}
     * (2026-11-04, the assert bar): each element by lite's own rules ({@link LiteText}: a String quoted JSON-style,
     * every other value its product text), never the canon the comparison reads; each side's elements in its own order
     * (the orchestrator's ruling, 2026-11-17). {@code sorted}: assertSameElements' form, read by the frame (item 3's
     * S0, 2026-11-21: legend-pure's sorted {@code toRepresentation} list went with the lite flag, and its style).
     */
    public record Message(Type kind, boolean sorted, @com.legend.base.Nullable String literal, boolean canon,
            @com.legend.base.Nullable String kindText, @com.legend.base.Nullable String canonClass) {
        // (the lite flag, the listed flag and the five-argument constructor whose default was legend-pure's words --
        // an invariant-4 default every caller overrode -- gone 2026-11-21, item 3's S0: the database judge plans for
        // lite's flow alone since A4, so a side is written by lite's own rules, LiteText)

        /** The side's own kind (its style went with the legend-pure branch, item 3's S0: lite's text asks none). */
        public Message(Type kind, boolean sorted, @com.legend.base.Nullable String literal) {
            this(kind, sorted, literal, false, null, null);
        }

        /** This message with the side's KIND as lite's sentence names it where two texts tie
         *  ({@code AssertKind.EQUALS_KINDS}). */
        public Message withKindText(String kindText) {
            return new Message(kind, sorted, literal, canon, kindText, canonClass);
        }

        /** A side of no one primitive kind (a mixed-kind collection, a literal channel), written by lite's own rules
         *  from each element's CANON ({@link #liteOfCanon}: its runtime kind read off the canon's mark), in the side's
         *  own order (the orchestrator's ruling, 2026-11-17); {@code classFqn}: the side's declared class, prefixed to
         *  an element whose canon is its JSON object (an instance as the product writes it), or null. */
        public static Message fromCanon(boolean sorted, @com.legend.base.Nullable String classFqn) {
            return new Message(Type.Primitive.STRING, sorted, null, true, null, classFqn);
        }

        public Message(Type kind, boolean sorted) {
            this(kind, sorted, null);
        }

        /** A LITERAL side (one value the planner holds): {@code text}, its lite text spelled at planning. */
        public static Message literal(Type kind, String text) {
            return new Message(kind, false, text);
        }

        /** {@code kind}'s message for a side, or null where its elements' text is no one primitive's. */
        public static @com.legend.base.Nullable Message of(@com.legend.base.Nullable Type kind, boolean sorted) {
            if (kind == null) {
                return null;
            }
            // Nil: an empty side, written [] whatever the other's kind (it has no element to write)
            boolean spelled = kind instanceof Type.PrecisionDecimal || kind == Type.Primitive.INTEGER
                    || PlatformTypes.isNil(kind)
                    || kind == Type.Primitive.FLOAT || kind == Type.Primitive.DECIMAL || kind == Type.Primitive.STRING
                    || kind == Type.Primitive.BOOLEAN || kind == Type.Primitive.DATE
                    || kind == Type.Primitive.STRICT_DATE || kind == Type.Primitive.DATE_TIME;
            return spelled ? new Message(kind, sorted) : null;
        }

        /** Instances of a GENERIC class (a {@code Pair}) carried as structs of primitive fields -- the caller has seen
         *  the side's plan answer such a struct: each written by lite's own rules, its class then its properties
         *  ({@link #instanceLite}). */
        public static Message instances(Type.GenericType kind, boolean sorted) {
            return new Message(kind, sorted);
        }
    }

    private static final String C = "__c";
    private static final String RN = "__rn";
    /** The NAMED wrapped grid(s) a grid verdict reads (see {@link #over}). */
    private static final String GRID = "__wg";
    private static final String GRID_E = "__we";
    private static final String GRID_A = "__wa";
    private static final String V = "__v";
    /** A row's element as the failure message writes it ({@link Message}); and {@code __o}, the value legend-pure's
     *  sorted form ordered by, unread since item 3's S0 (2026-11-21; its projection goes with the message SQL, S4). */
    private static final String W = "__w";
    private static final String O = "__o";

    /** Whether the database judge compares two values of {@code kind} by their value, not their text: a Decimal, a
     *  declared-precision Decimal (lite's rule, the two-form kind VerdictNumbers' product form until A4). */
    public static boolean decimalByValue(Type kind) {
        return kind == Type.Primitive.DECIMAL || kind instanceof Type.PrecisionDecimal;
    }

    /** The equality verdict statement over two framed sides; a Float compares exactly, as lite's rule has it (A4 of
     *  the compatibility program, stage A ratified by the user 2026-11-19: the database judge plans for the product
     *  alone, rule 4 of the rules of record, so compiled legend-pure's Float tolerance, S15/S132, and its two-form kind
     *  VerdictNumbers left it). */
    public static SqlQuery equality(Side e, Side a) {
        // THE SIDE'S SHAPE FOLLOWS ITS DECLARED MULTIPLICITY (lean ladder rung
        // 3): a side declared exactly one spells its facts straight over its
        // plan — no rows CTE, no aggregate (count is one, first is the value);
        // an optional or many side is a rows CTE folded once.
        Folded ef = fold("__e", e, false);
        Folded af = fold("__a", a, false);
        SqlQuery row = statementOf(ef, af, List.of(), List.of());
        if (e.message() == null || a.message() == null) {
            return row;
        }
        SqlQuery written = withWrittenSides(row, e.message().sorted(), e.byCanonText());
        return e.message().kindText() != null && a.message().kindText() != null
                ? withKinds(written, e.message().kindText(), a.message().kindText()) : written;
    }

    /** A TYPED verdict row and the schema of its columns (item 3's S4, 2026-11-21): what the database judge defers. */
    public record TypedRow(SqlQuery row, Type.RelationType schema) {
    }

    /** A typed member's expected values: a list in the side's own order, written where the verdict fails. */
    public static final String TYPED_EXPECTED = "__e";
    /** A typed member's actual values. */
    public static final String TYPED_ACTUAL = "__a";
    private static final String LISTED = "__l";

    /**
     * {@code assertEquals}'s and {@code assertSameElements}' TYPED member (item 3's S4, 2026-11-21, the user's Q-A: the
     * database decides over typed values): each side's values as its plan types them ({@link #valueRows}), folded to ONE
     * list in the side's own order; the verdict their {@code IS NOT DISTINCT FROM} -- two sorted lists where the order is
     * incidental ({@code multiset}) -- under the assert's polarity; each side's list written only where the verdict
     * fails, for the failure's record, which the run builds with the host judge's builders. The expected side is read
     * before the actual, as the host's side batch reads them. Null where a side's value has no type the row can carry.
     */
    public static @com.legend.base.Nullable TypedRow typedEquals(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean wantEqual, boolean emptyOnly) {
        SqlQuery eRaw = valueRows(eWrapped);
        SqlQuery aRaw = valueRows(aWrapped);
        // each side computed once: where its values are read as instants, the instants read its rows' table (__xe,
        // __xa), which the record lists
        SqlQuery eRows = readOnce("__xe", eRaw, eKind);
        SqlQuery aRows = readOnce("__xa", aRaw, aKind);
        boolean bothEmptyOnly = emptyOnly || !eKind.equals(aKind);
        // the carriers matter only where the values are compared: two kinds lite never calls equal compare no value
        if (eRows == null || aRows == null || !bothEmptyOnly && !oneCarrier(eRows, aRows)) {
            return null;
        }
        SqlQuery eList = listed("__e", eRows);
        SqlQuery aList = listed("__a", aRows);
        // the comparison over the typed values themselves, ONE node each dialect writes (the user, 2026-11-22: "Yes on
        // list compare"): over the two lists where its database compares lists, else the portable form over the rows
        // (H2 has no list equality, no EXCEPT ALL, no FULL OUTER JOIN; M1), the dialect's (SameValuesForm)
        SqlExpr eL = SqlExpr.Column.of("e", eList.outputs(), LISTED);
        SqlExpr aL = SqlExpr.Column.of("a", aList.outputs(), LISTED);
        // two kinds lite never calls equal (register S127: an Integer and a Decimal, the type part of the value), or an
        // empty side's (Nil, {@code emptyOnly}): the sides are the same only where both are empty, an empty side's list
        // NULL
        SqlExpr holds = bothEmptyOnly
                ? SqlExpr.Call.of(SqlFn.AND, noRows("__e", eRows), noRows("__a", aRows))
                : SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER, eL, aL,
                        new SqlExpr.ScalarSubquery(rowsOf("__e", eRows)), new SqlExpr.ScalarSubquery(rowsOf("__a", aRows)));
        // the record's values as each side carries them, not the instants compared (the precision audit's M4: a
        // record of the instant named another precision than the host's; H3: a nanosecond instant shared a fused
        // column with another assert's year past 2262)
        List<SqlWith.Cte> ctes = new ArrayList<>();
        SqlSource from = cross(new SqlSource.Subselect(aList, "a", null), new SqlSource.Subselect(eList, "e", null));
        SqlQuery eHeld = eList;
        SqlQuery aHeld = aList;
        String eAt = "e";
        String aAt = "a";
        if (eRows != eRaw && eRaw != null) {
            ctes.add(new SqlWith.Cte("__xe", eRaw));
            eHeld = listed("__xe", eRaw);
            eAt = "xe";
            from = cross(from, new SqlSource.Subselect(eHeld, eAt, null));
        }
        if (aRows != aRaw && aRaw != null) {
            ctes.add(new SqlWith.Cte("__xa", aRaw));
            aHeld = listed("__xa", aRaw);
            aAt = "xa";
            from = cross(from, new SqlSource.Subselect(aHeld, aAt, null));
        }
        ctes.add(new SqlWith.Cte("__e", eRows));
        ctes.add(new SqlWith.Cte("__a", aRows));
        return typedRow(ctes, from, wantEqual ? holds : SqlExpr.Call.of(SqlFn.NOT, holds), List.of(
                listValue(TYPED_EXPECTED, eAt, eHeld, eKind), listValue(TYPED_ACTUAL, aAt, aHeld, aKind)));
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over two sides of lists or maps (a nested
     * {@code List<T>}, a {@code Map<K, V>}; item 3's S4, 2026-11-22): each value compared as its database holds it -- a
     * list as itself, in order; a map as its entries sorted by key ({@code [{k, v}, ...]}: a map has no order, the host
     * compares its keys as a set) -- by the one list node, typed; where the database carries the collection as JSON the
     * node is refused by name ({@code SameValuesForm}: typed values only). Each side's values ride the failure as
     * carried. Null where a side carries no list or map, the two carry different types, or an element is no scalar.
     */
    public static @com.legend.base.Nullable TypedRow typedCollections(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean wantEqual) {
        SqlQuery eRaw = valueRows(eWrapped);
        SqlQuery aRaw = valueRows(aWrapped);
        if (eRaw == null || aRaw == null || !eRaw.outputs().get(0).type().equals(aRaw.outputs().get(0).type())) {
            return null;
        }
        SqlType t = eRaw.outputs().get(0).type();
        boolean list = t instanceof SqlType.Array ar && scalar(ar.element());
        boolean map = t instanceof SqlType.Map mp && scalar(mp.key()) && scalar(mp.value());
        if (!list && !map) {
            return null;
        }
        SqlQuery eRows = map ? entryRows("__xe", eRaw, (SqlType.Map) t) : cteRows("__xe", eRaw);
        SqlQuery aRows = map ? entryRows("__xa", aRaw, (SqlType.Map) t) : cteRows("__xa", aRaw);
        SqlQuery eList = listed("__e", eRows);
        SqlQuery aList = listed("__a", aRows);
        SqlQuery eHeld = listed("__xe", eRaw);
        SqlQuery aHeld = listed("__xa", aRaw);
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("e", eList.outputs(), LISTED), SqlExpr.Column.of("a", aList.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__e", eRows)), new SqlExpr.ScalarSubquery(rowsOf("__a", aRows)));
        return typedRow(List.of(new SqlWith.Cte("__xe", eRaw), new SqlWith.Cte("__xa", aRaw),
                        new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                cross(cross(cross(new SqlSource.Subselect(eList, "e", null), new SqlSource.Subselect(aList, "a",
                        null)), new SqlSource.Subselect(eHeld, "xe", null)), new SqlSource.Subselect(aHeld, "xa",
                        null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "xe", eHeld, eKind), listValue(TYPED_ACTUAL, "xa", aHeld, aKind)));
    }

    /** Whether {@code t} is a scalar value's type (a collection's element a typed member compares as it is). */
    private static boolean scalar(SqlType t) {
        return t instanceof SqlType.Scalar sc && sc != SqlType.Scalar.JSON || t instanceof SqlType.Decimal;
    }

    /** A side's maps ({@code rows}, read as {@code cte}) as their entries sorted by key, each a {@code {k, v}} struct:
     *  the value a map is compared as (the order its entries were written in no part of it). */
    private static SqlQuery entryRows(String cte, SqlQuery rows, SqlType.Map t) {
        SqlExpr m = SqlExpr.Column.of(cte, rows.outputs(), O);
        SqlExpr k = SqlExpr.Column.derived(null, "__k");
        SqlExpr entry = new SqlExpr.StructLit(List.of(new SqlExpr.StructLit.Field("k", k, t.key()),
                new SqlExpr.StructLit.Field("v", CanonicalRenderSql.valueAtKey(m), t.value())));
        SqlExpr entries = CanonicalRenderSql.sortedEntries(m, entry);
        SqlType et = new SqlType.Array(new SqlType.Struct(List.of(new SqlType.Struct.Field("k", t.key()),
                new SqlType.Struct.Field("v", t.value()))));
        OutputCol oOut = new OutputCol(O, et, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(entries, O, oOut), new SqlSelect.Projection(
                SqlExpr.Column.of(cte, rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, cte,
                rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null,
                null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /** {@code raw}, a side's typed rows, as the values compared: itself, or, where {@link #instants} reads them as
     *  the judge's instants, those instants over the rows' table {@code cte} (which the caller defines and the record
     *  lists), so the side's plan is computed once (the precision fix's round: a second copy made the database compute
     *  each side twice, its errors in another order). */
    private static @com.legend.base.Nullable SqlQuery readOnce(String cte,
            @com.legend.base.Nullable SqlQuery raw, Type kind) {
        if (raw == null || instants(raw, kind) == raw) {
            return raw == null ? null : instants(raw, kind);
        }
        return instants(cteRows(cte, raw), kind);
    }

    /** The rows of {@code cte}, a side's typed rows ({@code __o}, {@code __rn}), read back. */
    private static SqlQuery cteRows(String cte, SqlQuery rows) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : rows.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of(cte, rows.outputs(), o.name()), o.name(), o));
        }
        return new SqlSelect(ps, false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, rows.outputs());
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over a TAGGED side (item 3's S4, 2026-11-22):
     * a value whose kind the database decided among candidates (a Number's max, a mixed list's elements; the plan's
     * {@link SqlType.Tagged}: its tag and one slot per kind), against another tagged side or one of a typed class. Each
     * value is a row of its kind's index ({@code k}, into the kinds of both sides, one order) and one typed column per
     * kind ({@code v0, v1, ...}, the value in its kind's column, NULL in the others), compared by the one list node --
     * two values the same only where their kinds and their values are (register S127: an Integer is never a Float;
     * S139 a Decimal by its value; S399 a DateTime of any written precision one kind). Each side's rows ride the failure
     * as structs, the record un-tagging each to its kind's value. Null where a side carries no tagged or typed value,
     * or a kind (an enumeration's) no column holds.
     */
    public static @com.legend.base.Nullable TypedRow typedTagged(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, boolean multiset, boolean wantEqual) {
        // the values compared by KIND (a Decimal of any scale by its value, S139); the values recorded by kind AND SQL
        // type, each Decimal in a column of its own scale, so none is rescaled in the record (batch 3's audit, H1)
        java.util.TreeMap<String, SqlType> kinds = new java.util.TreeMap<>();
        java.util.TreeMap<String, SqlType> held = new java.util.TreeMap<>();
        if (!kindsOf(eWrapped, eKind, kinds, false) || !kindsOf(aWrapped, aKind, kinds, false)
                || !kindsOf(eWrapped, eKind, held, true) || !kindsOf(aWrapped, aKind, held, true)) {
            return null;
        }
        SqlQuery eCells = kindedRows(eWrapped, eKind, kinds, false);
        SqlQuery aCells = kindedRows(aWrapped, aKind, kinds, false);
        SqlQuery eHeld = kindedRows(eWrapped, eKind, held, true);
        SqlQuery aHeld = kindedRows(aWrapped, aKind, held, true);
        if (eCells == null || aCells == null || eHeld == null || aHeld == null) {
            return null;
        }
        SqlQuery eCmp = listed("__ke", structRows(rowsOf("__ce", eCells)));
        SqlQuery aCmp = listed("__ka", structRows(rowsOf("__ca", aCells)));
        SqlQuery eList = listed("__he", structRows(rowsOf("__ge", eHeld)));
        SqlQuery aList = listed("__ha", structRows(rowsOf("__ga", aHeld)));
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__ce", eCells)), new SqlExpr.ScalarSubquery(rowsOf("__ca", aCells)));
        return typedRow(List.of(new SqlWith.Cte("__ce", eCells), new SqlWith.Cte("__ca", aCells),
                        new SqlWith.Cte("__ke", structRows(rowsOf("__ce", eCells))),
                        new SqlWith.Cte("__ka", structRows(rowsOf("__ca", aCells))),
                        new SqlWith.Cte("__ge", eHeld), new SqlWith.Cte("__ga", aHeld),
                        new SqlWith.Cte("__he", structRows(rowsOf("__ge", eHeld))),
                        new SqlWith.Cte("__ha", structRows(rowsOf("__ga", aHeld)))),
                cross(cross(cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "e", eList, eKind), listValue(TYPED_ACTUAL, "a", aList, aKind)));
    }

    /**
     * {@code assertEquals}' and {@code assertSameElements}' TYPED member over two sides of instances of one class -- a
     * model class's or a Pair's, each value a struct (item 3's S4, instances, 2026-11-22; the user's Q-A): each value
     * ONE row of typed leaves, as the host restricts it ({@code HostJudgments.restrictOne}: the classifier, then the
     * keys, upstream's {@code EqualityUtilities}) -- a keyed class its {@code __type} where it carries one and each key
     * property, a nested keyed instance's leaves in its place; a keyless class its {@code __type} and its identity,
     * {@code __id} (register S111; read where the product tracks it, else raised by name: {@link
     * CanonicalRenderSql#trackedId}) -- compared by the one list node as the grids are, in order or as a multiset of
     * rows. Each leaf is read as its layout types it (a dialect carrying a struct as JSON reads the field by its type), a
     * date-time through the judge's instant. Where the verdict fails, each side's instances ride back as they are for the
     * host's record. Null where a side carries no struct, the two layouts' leaves differ in kind, or a leaf has no typed
     * reading here: a to-many key, a key slot of a polymorphic or Any class, a JSON field, a keyless class with no
     * identity.
     */
    public static @com.legend.base.Nullable TypedRow typedInstances(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys, boolean multiset,
            boolean wantEqual) {
        SqlQuery eRows = valueRows(eWrapped);
        SqlQuery aRows = valueRows(aWrapped);
        if (eRows == null || aRows == null || !(eRows.outputs().get(0).type() instanceof SqlType.Struct es)
                || !(aRows.outputs().get(0).type() instanceof SqlType.Struct as)) {
            return null;
        }
        List<Leaf> eLeaves = new ArrayList<>();
        List<Leaf> aLeaves = new ArrayList<>();
        if (!leaves(es, keys, List.of(), eLeaves) || !leaves(as, keys, List.of(), aLeaves)
                || eLeaves.size() != aLeaves.size() || eLeaves.isEmpty()) {
            return null;
        }
        for (int i = 0; i < eLeaves.size(); i++) {
            Leaf el = eLeaves.get(i);
            Leaf al = aLeaves.get(i);
            if (!el.path().equals(al.path()) || !java.util.Objects.equals(leafClass(el.type()), leafClass(al.type()))) {
                return null;
            }
        }
        SqlQuery ce = leafRows("__ie", eRows, eLeaves);
        SqlQuery ca = leafRows("__ia", aRows, aLeaves);
        SqlQuery eCmp = listed("__ke", structRows(rowsOf("__ce", ce)));
        SqlQuery aCmp = listed("__ka", structRows(rowsOf("__ca", ca)));
        // the record's instances as the host's reads them: a keyed class restricted to its classifier and its keys
        // (HostJudgments.restrictOne), a keyless one whole
        SqlQuery eHeld = keys == null ? eRows : restrictedRows("__ie", eRows, es, keys);
        SqlQuery aHeld = keys == null ? aRows : restrictedRows("__ia", aRows, as, keys);
        SqlQuery eList = listed("__re", eHeld);
        SqlQuery aList = listed("__ra", aHeld);
        SqlExpr same = SqlExpr.Call.of(multiset ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__ce", ce)), new SqlExpr.ScalarSubquery(rowsOf("__ca", ca)));
        return typedRow(List.of(new SqlWith.Cte("__ie", eRows), new SqlWith.Cte("__ia", aRows),
                        new SqlWith.Cte("__re", eHeld), new SqlWith.Cte("__ra", aHeld),
                        new SqlWith.Cte("__ce", ce), new SqlWith.Cte("__ca", ca),
                        new SqlWith.Cte("__ke", structRows(rowsOf("__ce", ce))),
                        new SqlWith.Cte("__ka", structRows(rowsOf("__ca", ca)))),
                cross(cross(cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null)),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        listValue(TYPED_EXPECTED, "e", eList, eKind), listValue(TYPED_ACTUAL, "a", aList, aKind)));
    }

    /** A side's keyed instances ({@code rows}, read as {@code cte}) each restricted to its classifier and its keys, a
     *  nested keyed instance restricted in its place -- the value the host's record writes
     *  ({@code HostJudgments.restrictOne}). */
    private static SqlQuery restrictedRows(String cte, SqlQuery rows, SqlType.Struct layout,
            com.legend.compiler.element.EqualityKeys keys) {
        SqlExpr held = restricted(SqlExpr.Column.of("i", rows.outputs(), O), layout, keys);
        OutputCol oOut = new OutputCol(O, held.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : layout,
                false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(held, O, oOut), new SqlSelect.Projection(
                SqlExpr.Column.of("i", rows.outputs(), RN), RN, rnOut)), false, new SqlSource.Table(cte, "i",
                rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null,
                null, List.of(), null, null, List.of(oOut, rnOut));
    }

    /** {@code v}, an instance of layout {@code st}, as a struct of its classifier and its key properties. */
    private static SqlExpr restricted(SqlExpr v, SqlType.Struct st, com.legend.compiler.element.EqualityKeys keys) {
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
        SqlType type = fieldType(st, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE);
        if (type != null) {
            fields.add(new SqlExpr.StructLit.Field(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE,
                    new SqlExpr.StructGet(v, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE), type));
        }
        for (com.legend.compiler.element.EqualityKeys.Key k : keys.keys()) {
            SqlType ft = java.util.Objects.requireNonNull(fieldType(st, k.name()), "a key's field (leaves read it)");
            SqlExpr field = new SqlExpr.StructGet(v, k.name());
            // a nested keyed instance restricted in its place, an absent one NULL (the instances audit's M-b)
            SqlExpr value = ft instanceof SqlType.Struct nested && k.nested() != null
                    ? new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, field),
                            nullOf(nested))), restricted(field, nested, k.nested()), SqlExpr.Reason.JUDGE) : field;
            SqlType vt = value.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : ft;
            fields.add(new SqlExpr.StructLit.Field(k.name(), value, vt));
        }
        return new SqlExpr.StructLit(fields);
    }

    /** One typed leaf of an instance: the field path from the instance down to it, and its layout's SQL type. */
    private record Leaf(List<String> path, SqlType type) {
    }

    /** The leaves of a struct of layout {@code st} as the host restricts it, into {@code out} (path-prefixed by
     *  {@code at}); false where one has no typed reading. */
    private static boolean leaves(SqlType.Struct st, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys
            keys, List<String> at, List<Leaf> out) {
        SqlType type = fieldType(st, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE);
        if (type != null) {
            out.add(new Leaf(append(at, com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE), type));
        }
        if (keys == null) {
            // a keyless class: its identity, tracked where the product tracks it (S111)
            SqlType id = fieldType(st, com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID);
            if (id == null || type == null) {
                return false;
            }
            out.add(new Leaf(append(at, com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID), id));
            return true;
        }
        for (com.legend.compiler.element.EqualityKeys.Key k : keys.keys()) {
            SqlType ft = fieldType(st, k.name());
            if (ft == null || k.many()) {
                return false;
            }
            if (ft instanceof SqlType.Struct nested) {
                // a nested keyed instance's own leaves; a slot whose class is keyless, polymorphic or Any has none here
                if (k.nested() == null || !leaves(nested, k.nested(), append(at, k.name()), out)) {
                    return false;
                }
            } else if (leafClass(ft) == null) {
                return false;
            } else {
                out.add(new Leaf(append(at, k.name()), ft));
            }
        }
        return true;
    }

    /** The kind a leaf's SQL type holds, as the host reads it, or null where it has no typed reading here (a JSON
     *  value, a list, a map, a struct). Two leaves compare only where their classes agree. */
    private static @com.legend.base.Nullable String leafClass(SqlType t) {
        return switch (t) {
            case SqlType.Decimal d -> "Decimal";
            case SqlType.Scalar sc -> switch (sc) {
                case BOOLEAN -> "Boolean";
                case INTEGER, BIGINT, HUGEINT -> "Integer";
                case DOUBLE -> "Float";
                case VARCHAR -> "String";
                case DATE -> "StrictDate";
                case TIMESTAMP -> "DateTime";
                default -> null;
            };
            default -> null;
        };
    }

    private static @com.legend.base.Nullable SqlType fieldType(SqlType.Struct st, String name) {
        for (SqlType.Struct.Field f : st.fields()) {
            if (f.name().equals(name)) {
                return f.type();
            }
        }
        return null;
    }

    private static List<String> append(List<String> at, String name) {
        List<String> out = new ArrayList<>(at);
        out.add(name);
        return out;
    }

    /** A side's instances ({@code rows}, read as {@code cte}) as rows of their typed leaves ({@code c0, c1, ...,
     *  __rn}): each leaf the field read down its path, cast to its layout's type (a dialect carrying a struct as JSON
     *  reads the field by it), a date-time as the judge's instant, an identity where the product tracks it. */
    private static SqlQuery leafRows(String cte, SqlQuery rows, List<Leaf> leaves) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        for (int i = 0; i < leaves.size(); i++) {
            Leaf leaf = leaves.get(i);
            SqlExpr v = SqlExpr.Column.of("i", rows.outputs(), O);
            for (String step : leaf.path()) {
                v = new SqlExpr.StructGet(v, step);
            }
            String last = leaf.path().get(leaf.path().size() - 1);
            SqlExpr read = last.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)
                    ? new SqlExpr.Cast(CanonicalRenderSql.trackedId(((SqlExpr.StructGet) v).source()),
                            SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                    : new SqlExpr.Cast(v, leaf.type(), SqlExpr.Reason.JUDGE);
            SqlType type = last.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID) ? SqlType.Scalar.VARCHAR
                    : leaf.type();
            OutputCol out = new OutputCol("c" + i, type, true);
            ps.add(new SqlSelect.Projection(read, out.name(), out));
            outs.add(out);
        }
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(SqlExpr.Column.of("i", rows.outputs(), RN), RN, rnOut));
        outs.add(rnOut);
        return new SqlSelect(ps, false, new SqlSource.Table(cte, "i", rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** Whether a tagged side ({@code wrapped}'s first column a {@link SqlType.Tagged}) or a side of one typed class
     *  states its kinds, each with the SQL type that holds it, into {@code into}, keyed by {@link #kindKey}. */
    private static boolean kindsOf(SqlQuery wrapped, Type kind, java.util.Map<String, SqlType> into, boolean byType) {
        if (wrapped.outputs().isEmpty()) {
            return false;
        }
        if (wrapped.outputs().get(0).type() instanceof SqlType.Tagged tg) {
            for (SqlType.Tagged.Slot slot : tg.slots()) {
                if (MixedEncoding.enumerationKind(slot.kind()) != null) {
                    return false;
                }
                if (!byType && slot.kind().equals("Date")) {
                    // a Date slot holds its value written: each one of the four date kinds, as a written side's
                    for (java.util.Map.Entry<String, SqlType> k : DATE_KINDS.entrySet()) {
                        into.putIfAbsent(kindKey(k.getKey(), k.getValue(), false), k.getValue());
                    }
                    continue;
                }
                // a DateTime compared at the judge's instant (S399): its compare column a TIMESTAMP whatever its slot
                into.putIfAbsent(kindKey(slot.kind(), slot.type(), byType), !byType && instant(slot.kind())
                        ? SqlType.Scalar.TIMESTAMP : slot.type());
            }
            return true;
        }
        if (wrapped.outputs().get(0).type() == SqlType.Scalar.JSON) {
            // a value carried as JSON (an Any): each value its JSON kind's -- a String, an Integer, a Float, a Boolean --
            // as the product reads a Variant (CastPolicy), compared as that kind's value; recorded as carried
            if (byType) {
                into.putIfAbsent(kindKey("Any", SqlType.Scalar.JSON, true), SqlType.Scalar.JSON);
            } else {
                JSON_KINDS.forEach((k, t) -> into.putIfAbsent(kindKey(k, t, false), t));
            }
            return true;
        }
        if (writtenDate(kind, wrapped)) {
            // a Date written as its text: each value one of the four date kinds, as its text says (S399), compared as
            // its kind's value and recorded as it is written
            SqlType carried = wrapped.outputs().get(0).type();
            for (java.util.Map.Entry<String, SqlType> k : DATE_KINDS.entrySet()) {
                into.putIfAbsent(writtenKey(k.getKey(), carried, byType), byType ? carried : k.getValue());
            }
            return true;
        }
        Type t = typedClass(kind);
        if (t == null || t instanceof Type.EnumType || PlatformTypes.isNil(t)) {
            return false;
        }
        SqlType held = byType ? wrapped.outputs().get(0).type() : heldType(wrapped, t);
        into.putIfAbsent(kindKey(kindText(t), held, byType), held);
        return true;
    }

    /** The kinds a Date written as its text may be, each with the SQL type its value is compared in (S399): a year and
     *  a year-month by their components, typed as integers (a year itself; a year-month its year times twelve plus its
     *  month), a day the database's DATE, a date-time the judge's instant (the precision audit's H1, 2026-11-22: a
     *  year's and a year-month's text were compared as text). */
    private static final java.util.Map<String, SqlType> DATE_KINDS = java.util.Map.of("Year", SqlType.Scalar.BIGINT,
            "YearMonth", SqlType.Scalar.BIGINT, "StrictDate", SqlType.Scalar.DATE, "DateTime", SqlType.Scalar.TIMESTAMP);

    /** The kinds a value carried as JSON may be, each with the SQL type its value is compared in. */
    private static final java.util.Map<String, SqlType> JSON_KINDS = java.util.Map.of("String", SqlType.Scalar.VARCHAR,
            "Integer", SqlType.Scalar.BIGINT, "Float", SqlType.Scalar.DOUBLE, "Boolean", SqlType.Scalar.BOOLEAN);

    /** A JSON value's kind's name ({@link #JSON_KINDS}) where {@code jsonType} (its JSON_TYPE) answers it. */
    private static SqlExpr jsonKindIs(String kind, SqlExpr jsonType) {
        return switch (kind) {
            case "String" -> SqlExpr.Call.of(SqlFn.EQUAL, jsonType, new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.STRING));
            case "Integer" -> SqlExpr.Call.of(SqlFn.OR,
                    SqlExpr.Call.of(SqlFn.EQUAL, jsonType, new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.INTEGER)),
                    SqlExpr.Call.of(SqlFn.EQUAL, jsonType,
                            new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.UNSIGNED_INTEGER)));
            case "Float" -> SqlExpr.Call.of(SqlFn.EQUAL, jsonType, new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.FLOAT));
            case "Boolean" -> SqlExpr.Call.of(SqlFn.EQUAL, jsonType,
                    new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.BOOLEAN));
            default -> throw new IllegalArgumentException("no JSON kind is " + kind);
        };
    }

    /** A JSON value of {@code kind} read as that kind's value, as the product reads a Variant ({@code CastPolicy}): a
     *  String its root's text, a number or a Boolean its cast. */
    private static SqlExpr jsonValue(String kind, SqlExpr v) {
        return kind.equals("String")
                ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.VARIANT_GET, v, new SqlExpr.StringLit("$")),
                        SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                : new SqlExpr.Cast(v, java.util.Objects.requireNonNull(JSON_KINDS.get(kind)), SqlExpr.Reason.JUDGE);
    }

    /** Whether a side is a {@code Date} carried as its written text ({@code JudgmentFacts}' the same rule). */
    private static boolean writtenDate(Type kind, SqlQuery wrapped) {
        return kind == Type.Primitive.DATE && !wrapped.outputs().isEmpty()
                && (wrapped.outputs().get(0).type() == SqlType.Scalar.TEMPORAL_TEXT
                        || wrapped.outputs().get(0).type() == SqlType.Scalar.VARCHAR);
    }

    /** The kind a Date's written text names, read from its STRUCTURE -- a time after the day (a {@code T} or a space)
     *  a date-time; else by its dashes after the year's sign: none a year, one a year-month, two a day -- as the kind
     *  key's index. Never its length (the precision audit's H1, 2026-11-22: a year of three or five digits, or a
     *  negative one, took another kind's length). */
    private static SqlExpr dateKindOfText(SqlExpr text, java.util.function.Function<String, Integer> index) {
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (String k : List.of("DateTime", "Year", "YearMonth")) {
            whens.add(new SqlExpr.Case.When(writtenKindIs(k, text), new SqlExpr.IntLit(index.apply(k))));
        }
        return new SqlExpr.Case(whens, new SqlExpr.IntLit(index.apply("StrictDate")), SqlExpr.Reason.JUDGE);
    }

    /** Whether a Date's written {@code text} is of {@code kind} ({@link #dateKindOfText}). */
    private static SqlExpr writtenKindIs(String kind, SqlExpr text) {
        SqlExpr body = unsignedYear(text);
        SqlExpr time = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, body, new SqlExpr.StringLit("T")),
                        new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, body, new SqlExpr.StringLit(" ")),
                        new SqlExpr.IntLit(0)));
        SqlExpr dashes = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, body), SqlExpr.Call.of(SqlFn.LENGTH,
                SqlExpr.Call.of(SqlFn.REPLACE, body, new SqlExpr.StringLit("-"), new SqlExpr.StringLit(""))));
        return switch (kind) {
            case "DateTime" -> time;
            case "Year" -> SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, time),
                    SqlExpr.Call.of(SqlFn.EQUAL, dashes, new SqlExpr.IntLit(0)));
            case "YearMonth" -> SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, time),
                    SqlExpr.Call.of(SqlFn.EQUAL, dashes, new SqlExpr.IntLit(1)));
            case "StrictDate" -> SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.NOT, time),
                    SqlExpr.Call.of(SqlFn.EQUAL, dashes, new SqlExpr.IntLit(2)));
            default -> throw new IllegalArgumentException("no written date's kind is " + kind);
        };
    }

    /** {@code text} without the sign before its year. */
    private static SqlExpr unsignedYear(SqlExpr text) {
        SqlExpr first = SqlExpr.Call.of(SqlFn.LEFT, text, new SqlExpr.IntLit(1));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("-")),
                SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("+"))),
                SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(2)))), text, SqlExpr.Reason.JUDGE);
    }

    /** A Date's written {@code text} of {@code kind} as the value it is compared as ({@link #DATE_KINDS}): a year its
     *  number; a year-month its year times twelve plus its month; a day the database's DATE; a date-time the judge's
     *  instant (an hour's text read at its minute). */
    private static SqlExpr writtenDateValue(String kind, SqlExpr text) {
        return switch (kind) {
            case "Year" -> new SqlExpr.Cast(text, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE);
            case "YearMonth" -> {
                SqlExpr body = unsignedYear(text);
                SqlExpr dash = SqlExpr.Call.of(SqlFn.STRPOS, body, new SqlExpr.StringLit("-"));
                // the year with its sign: the text up to the dash that ends it
                SqlExpr year = SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(1), SqlExpr.Call.of(
                        SqlFn.MINUS, SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(
                                SqlFn.LENGTH, text), SqlExpr.Call.of(SqlFn.LENGTH, body)), dash), new SqlExpr.IntLit(1)));
                SqlExpr month = SqlExpr.Call.of(SqlFn.SUBSTRING, body, SqlExpr.Call.of(SqlFn.PLUS, dash,
                        new SqlExpr.IntLit(1)));
                yield SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.TIMES,
                        new SqlExpr.Cast(year, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE), new SqlExpr.IntLit(12)),
                        new SqlExpr.Cast(month, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE));
            }
            case "StrictDate" -> new SqlExpr.Cast(text, SqlType.Scalar.DATE, SqlExpr.Reason.JUDGE);
            case "DateTime" -> new SqlExpr.Cast(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, text, new SqlExpr.StringLit(":")),
                            new SqlExpr.IntLit(0)),
                    SqlExpr.Call.of(SqlFn.CONCAT, text, new SqlExpr.StringLit(":00")))), text, SqlExpr.Reason.JUDGE),
                    SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE);
            default -> throw new IllegalArgumentException("no written date's kind is " + kind);
        };
    }

    /** The SQL type a typed side's values are compared in: a date's the database's DATE or TIMESTAMP ({@link #instants}
     *  reads a written one), any other its column's. */
    private static SqlType heldType(SqlQuery wrapped, Type t) {
        return t == Type.Primitive.DATE_TIME ? SqlType.Scalar.TIMESTAMP
                : t == Type.Primitive.STRICT_DATE ? SqlType.Scalar.DATE : wrapped.outputs().get(0).type();
    }

    /** Whether a slot's kind is a DateTime's (of any written precision), compared at the judge's instant. */
    private static boolean instant(String slotKind) {
        return MixedEncoding.writtenDigits(slotKind) != null || slotKind.equals("DateTime");
    }

    /** A written Date's kind's column key: its compared kind's ({@link #DATE_KINDS}), and where the column is the
     *  record's, the text it is written in. */
    private static String writtenKey(String kind, SqlType carried, boolean byType) {
        return kindKey(kind, byType ? carried : java.util.Objects.requireNonNull(DATE_KINDS.get(kind)), byType);
    }

    /** A kind's column key: its name (a DateTime of any written precision one kind, register S399), and where the
     *  column is the record's ({@code byType}) its SQL type too. */
    private static String kindKey(String slotKind, SqlType type, boolean byType) {
        String kind = MixedEncoding.writtenDigits(slotKind) != null ? "DateTime" : slotKind;
        return byType ? kind + "|" + type : kind;
    }

    /** A side's values as kinded rows ({@link #typedTagged}): {@code k}, the index of its kind's key in
     *  {@code columns}, then {@code v0..vn} one per key, then {@code __rn}, numbered in the side's own order. A tagged
     *  side's tag names its slot; a typed side's every value is its one kind's. */
    private static @com.legend.base.Nullable SqlQuery kindedRows(SqlQuery wrapped, Type kind,
            java.util.SortedMap<String, SqlType> columns, boolean byType) {
        List<String> order = new ArrayList<>(columns.keySet());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        OutputCol kOut = new OutputCol("k", SqlType.Scalar.BIGINT, false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlQuery rows;
        SqlExpr kOf;
        java.util.function.Function<String, @com.legend.base.Nullable SqlExpr> valueOf;
        if (wrapped.outputs().get(0).type() instanceof SqlType.Tagged tg) {
            // the tag and its slots, in the side's own order (valueRows: a wrap's ORDER BY numbered, never lost), each
            // slot the column its name says (batch 4's audit, L2: read by position, so checked by name)
            String tagName = wrapped.outputs().get(0).name();
            for (int i = 0; i < tg.slots().size(); i++) {
                if (wrapped.outputs().size() <= i + 1
                        || !wrapped.outputs().get(i + 1).name().equals(SqlType.Tagged.slotColumn(tagName, i))) {
                    return null;
                }
            }
            rows = valueRows(wrapped, 1 + tg.slots().size());
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            SqlExpr tag = SqlExpr.Column.of("r", r.outputs(), O);
            List<SqlExpr.Case.When> ks = new ArrayList<>();
            for (int i = 0; i < tg.slots().size(); i++) {
                SqlType.Tagged.Slot slot = tg.slots().get(i);
                SqlExpr at = SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(i));
                if (!byType && slot.kind().equals("Date")) {
                    // a Date slot's kind is its written value's (dateKindOfText)
                    SqlExpr text = SqlExpr.Column.of("r", r.outputs(), "__s" + (i + 1));
                    for (String k : List.of("DateTime", "Year", "YearMonth", "StrictDate")) {
                        ks.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, at, writtenKindIs(k, text)),
                                new SqlExpr.IntLit(order.indexOf(kindKey(k, java.util.Objects.requireNonNull(
                                        DATE_KINDS.get(k)), false)))));
                    }
                    continue;
                }
                ks.add(new SqlExpr.Case.When(at,
                        new SqlExpr.IntLit(order.indexOf(kindKey(slot.kind(), slot.type(), byType)))));
            }
            kOf = new SqlExpr.Case(ks, null, SqlExpr.Reason.JUDGE);
            valueOf = key -> {
                List<SqlExpr.Case.When> vs = new ArrayList<>();
                for (int i = 0; i < tg.slots().size(); i++) {
                    SqlType.Tagged.Slot slot = tg.slots().get(i);
                    if (!byType && slot.kind().equals("Date")) {
                        for (String k : DATE_KINDS.keySet()) {
                            if (kindKey(k, java.util.Objects.requireNonNull(DATE_KINDS.get(k)), false).equals(key)) {
                                SqlExpr text = SqlExpr.Column.of("r", r.outputs(), "__s" + (i + 1));
                                vs.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.EQUAL,
                                        tag, new SqlExpr.IntLit(i)), writtenKindIs(k, text)),
                                        writtenDateValue(k, text)));
                            }
                        }
                        continue;
                    }
                    if (kindKey(slot.kind(), slot.type(), byType).equals(key)) {
                        SqlExpr held = SqlExpr.Column.of("r", r.outputs(), "__s" + (i + 1));
                        vs.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(i)),
                                !byType && instant(slot.kind()) ? new SqlExpr.Cast(held, SqlType.Scalar.TIMESTAMP,
                                        SqlExpr.Reason.JUDGE) : held));
                    }
                }
                return vs.isEmpty() ? null : new SqlExpr.Case(vs, nullOf(java.util.Objects.requireNonNull(
                        columns.get(key), "a kind's SQL type")), SqlExpr.Reason.JUDGE);
            };
        } else if (wrapped.outputs().get(0).type() == SqlType.Scalar.JSON) {
            rows = valueRows(wrapped);
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            SqlExpr v = SqlExpr.Column.of("r", r.outputs(), O);
            if (byType) {
                // recorded as carried: the JSON value itself
                kOf = new SqlExpr.IntLit(order.indexOf(kindKey("Any", SqlType.Scalar.JSON, true)));
                valueOf = key -> key.equals(kindKey("Any", SqlType.Scalar.JSON, true)) ? v : null;
            } else {
                SqlExpr jt = SqlExpr.Call.of(SqlFn.JSON_TYPE, v);
                List<SqlExpr.Case.When> ks = new ArrayList<>();
                ks.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, jt,
                        new SqlExpr.JsonKindLit(com.legend.sql.JsonKind.NULL)), new SqlExpr.NullLit()));
                for (String k : JSON_KINDS.keySet()) {
                    ks.add(new SqlExpr.Case.When(jsonKindIs(k, jt), new SqlExpr.IntLit(order.indexOf(kindKey(k,
                            java.util.Objects.requireNonNull(JSON_KINDS.get(k)), false)))));
                }
                // an object or an array: no typed value here, refused by name
                kOf = new SqlExpr.Case(ks, SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(
                        com.legend.sql.RaiseKind.JUDGED_TREE_NOT_TYPED, List.of())), SqlExpr.Reason.JUDGE);
                valueOf = key -> {
                    for (String k : JSON_KINDS.keySet()) {
                        if (kindKey(k, java.util.Objects.requireNonNull(JSON_KINDS.get(k)), false).equals(key)) {
                            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(jsonKindIs(k, jt), jsonValue(k, v))),
                                    nullOf(java.util.Objects.requireNonNull(columns.get(key), "a kind's SQL type")),
                                    SqlExpr.Reason.JUDGE);
                        }
                    }
                    return null;
                };
            }
        } else if (writtenDate(kind, wrapped)) {
            rows = valueRows(wrapped);
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            SqlExpr text = SqlExpr.Column.of("r", r.outputs(), O);
            SqlType carried = r.outputs().get(0).type();
            kOf = dateKindOfText(text, name -> order.indexOf(writtenKey(name, carried, byType)));
            valueOf = key -> {
                for (String k : DATE_KINDS.keySet()) {
                    if (writtenKey(k, carried, byType).equals(key)) {
                        // compared as its kind's value; recorded as it is written
                        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(writtenKindIs(k, text),
                                byType ? text : writtenDateValue(k, text))), nullOf(java.util.Objects.requireNonNull(
                                columns.get(key), "a kind's SQL type")), SqlExpr.Reason.JUDGE);
                    }
                }
                return null;
            };
        } else {
            Type t = java.util.Objects.requireNonNull(typedClass(kind), "a typed side's class");
            // compared at the judge's instant; recorded as it is held (the precision audit's M4)
            rows = byType ? valueRows(wrapped) : instants(valueRows(wrapped), t);
            if (rows == null) {
                return null;
            }
            SqlQuery r = rows;
            String own = kindKey(kindText(t), byType ? wrapped.outputs().get(0).type() : heldType(wrapped, t),
                    byType);
            kOf = new SqlExpr.IntLit(order.indexOf(own));
            valueOf = key -> key.equals(own) ? SqlExpr.Column.of("r", r.outputs(), O) : null;
        }
        ps.add(new SqlSelect.Projection(kOf, "k", kOut));
        outs.add(kOut);
        for (int j = 0; j < order.size(); j++) {
            SqlType vt = java.util.Objects.requireNonNull(columns.get(order.get(j)), "a kind's SQL type");
            SqlExpr v = valueOf.apply(order.get(j));
            OutputCol out = new OutputCol("v" + j, vt, true);
            ps.add(new SqlSelect.Projection(v == null ? nullOf(vt) : v, out.name(), out));
            outs.add(out);
        }
        ps.add(new SqlSelect.Projection(SqlExpr.Column.of("r", rows.outputs(), RN), RN, rnOut));
        outs.add(rnOut);
        return new SqlSelect(ps, false, new SqlSource.Subselect(rows, "r", null), null, List.of(), null, null,
                List.of(), null, null, outs);
    }

    /** The kind the database judge compares {@code kind}'s values as, typed (item 3's S4), or null where it does not
     *  yet: a String, an Integer, a Float, a Boolean as itself; every Decimal, a declared precision's too, as a Decimal
     *  (compared by value, register S139); an enumeration as itself (its values' names); a StrictDate and a DateTime as
     *  themselves, compared by their instant (register S399; a {@code Date} side's kind is its column's,
     *  {@code JudgmentFacts.typedPair}); an empty side's kind (Nil, a side the types say holds nothing) as itself.
     *  An instance, a generic type and a table stay the seven-column row until S4 converts them. */
    public static @com.legend.base.Nullable Type typedClass(@com.legend.base.Nullable Type kind) {
        if (kind == Type.Primitive.STRING || kind == Type.Primitive.INTEGER || kind == Type.Primitive.FLOAT
                || kind == Type.Primitive.BOOLEAN || kind instanceof Type.EnumType || kind == Type.Primitive.STRICT_DATE
                || kind == Type.Primitive.DATE_TIME || kind != null && PlatformTypes.isNil(kind)) {
            return kind;
        }
        return kind == Type.Primitive.DECIMAL || kind instanceof Type.PrecisionDecimal ? Type.Primitive.DECIMAL : null;
    }

    /**
     * A date-time side's typed rows as the database's TIMESTAMPs (register S399, the user's ruling of 2026-11-22: the
     * product compares a date-time by its instant, the database comparing). A side carried as its written text (a
     * literal's TEMPORAL_TEXT, which keeps how it was written for its print) is parsed by the database: every written
     * precision parses as an instant but an hour's, which no database parses, so it is read at its minute (probed on
     * DuckDB 1.5.6 and H2 2.5.252, 2026-11-22). Any other side's rows as they are; null stays null.
     */
    private static @com.legend.base.Nullable SqlQuery instants(@com.legend.base.Nullable SqlQuery rows, Type kind) {
        if (rows == null || kind != Type.Primitive.DATE_TIME && kind != Type.Primitive.STRICT_DATE) {
            return rows;
        }
        SqlType carried = rows.outputs().get(0).type();
        if (carried == SqlType.Scalar.DATE || carried == SqlType.Scalar.TIMESTAMPTZ
                || carried == SqlType.Scalar.TIMESTAMP && kind != Type.Primitive.DATE_TIME) {
            return rows;
        }
        if (carried != SqlType.Scalar.TEMPORAL_TEXT && carried != SqlType.Scalar.VARCHAR
                && carried != SqlType.Scalar.TIMESTAMP) {
            // a date the side carries in no form the database reads as one (a JSON carrier: the dates audit's LOW 6,
            // 2026-11-22): no typed member
            return null;
        }
        if (kind == Type.Primitive.STRICT_DATE) {
            // a day written as its text, read as the database's DATE, the kind a stored day's column holds
            OutputCol dOut = new OutputCol(O, SqlType.Scalar.DATE, false);
            return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(SqlExpr.Column.of("t", rows.outputs(),
                    O), SqlType.Scalar.DATE, SqlExpr.Reason.JUDGE), O, dOut), new SqlSelect.Projection(
                    SqlExpr.Column.of("t", rows.outputs(), RN), RN, rows.outputs().get(1))), false,
                    new SqlSource.Subselect(rows, "t", null), null, List.of(), null, null, List.of(), null, null,
                    List.of(dOut, rows.outputs().get(1)));
        }
        SqlExpr v = SqlExpr.Column.of("t", rows.outputs(), O);
        // every date-time read as the judge's instant (the dialect's JUDGE cast to TIMESTAMP: a value the database
        // types as a date-time, as it holds it, every digit; SQLite's, a text, in one form at nine digits; a text with
        // more digits than the database's date-time reads refused by name -- S399, S181, the user's rule of
        // 2026-11-22) --
        // a stored one as it is held, a written one from its text: an hour's text read at its minute; a day's text (a
        // DateTime over a column that holds days: the egress writes its day alone, the dates audit's MEDIUM 4) read
        // at its midnight, as it is
        SqlExpr written = carried == SqlType.Scalar.TIMESTAMP ? v : new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, v, new SqlExpr.StringLit(":")),
                        new SqlExpr.IntLit(0)), SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.LENGTH, v),
                        new SqlExpr.IntLit(10))),
                SqlExpr.Call.of(SqlFn.CONCAT, v, new SqlExpr.StringLit(":00")))), v, SqlExpr.Reason.JUDGE);
        OutputCol oOut = new OutputCol(O, SqlType.Scalar.TIMESTAMP, false);
        OutputCol rnOut = rows.outputs().get(1);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(written, SqlType.Scalar.TIMESTAMP,
                SqlExpr.Reason.JUDGE), O, oOut), new SqlSelect.Projection(SqlExpr.Column.of("t", rows.outputs(), RN), RN,
                rnOut)), false, new SqlSource.Subselect(rows, "t", null), null, List.of(), null, null, List.of(), null,
                null, List.of(oOut, rnOut));
    }

    /** Whether a side's typed rows CTE ({@code cte}) holds no row: emptiness by the rows, never by the list (a database's
     *  list of no rows is NULL on one and {@code []} on another: SQLite's json_group_array; batch 2's audit, HIGH 1). */
    private static SqlExpr noRows(String cte, SqlQuery rows) {
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        return SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.IntLit(1), "__one", one)), false, new SqlSource.Table(cte, cte, rows.outputs(), false,
                java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null,
                null, List.of(one))));
    }

    /** Whether two sides' typed rows carry their values alike: a Decimal the root writes as its text (a store-read
     *  Decimal's {@code DECIMAL_TEXT}, the value lane's egress) against a decimal value does not compare -- no typed
     *  member for the pair (it stays the seven-column row). */
    private static boolean oneCarrier(SqlQuery e, SqlQuery a) {
        return (e.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT)
                == (a.outputs().get(0).type() == SqlType.Scalar.DECIMAL_TEXT);
    }

    /** A typed rows CTE read whole: its {@code __o} and {@code __rn}, the rows the portable comparison reads. */
    private static SqlQuery rowsOf(String cte, SqlQuery rows) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : rows.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of(cte, rows.outputs(), o.name()), o.name(), o));
        }
        return new SqlSelect(ps, false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, rows.outputs());
    }

    private static final String K = "__k";

    /** A typed rows CTE's count ({@link #K}): its rows. */
    private static SqlQuery counted(String cte, SqlQuery rows) {
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of(cte, rows.outputs(), RN)), false, List.of()), K, kOut)), false,
                new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of(kOut));
    }

    /** {@code assert} / {@code assertFalse}'s TYPED member (item 3's S4): the condition's one Boolean as the side's plan
     *  types it, the verdict its {@code IS NOT DISTINCT FROM} the wanted truth; no value rides the failure (its record
     *  is fixed at planning). Null where the side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedCondition(SqlQuery condWrapped, boolean wantTrue) {
        SqlQuery rows = valueRows(condWrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery one = single("__c", rows);
        return typedRow(List.of(new SqlWith.Cte("__c", rows)), new SqlSource.Subselect(one, "c", null),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("c", one.outputs(), SINGLE),
                        new SqlExpr.BoolLit(wantTrue)), List.of());
    }

    /** {@code assertSize}'s TYPED member (item 3's S4): {@code countRows}' count (its {@code __rn}s) against the size
     *  side's one Integer; {@code wantSize} false, {@code assertNotSize}. Each rides the failure: the size, then the
     *  count. Null where the size side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedSize(SqlQuery countRows, SqlQuery nWrapped,
            boolean wantSize) {
        SqlQuery nRows = valueRows(nWrapped);
        if (nRows == null) {
            return null;
        }
        SqlQuery counted = counted("__a", countRows);
        SqlQuery size = single("__n", nRows);
        SqlExpr wanted = new SqlExpr.Cast(SqlExpr.Column.of("n", size.outputs(), SINGLE), SqlType.Scalar.BIGINT,
                SqlExpr.Reason.JUDGE);
        SqlExpr count = SqlExpr.Column.of("k", counted.outputs(), K);
        SqlExpr same = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, count, wanted);
        return typedRow(List.of(new SqlWith.Cte("__a", countRows), new SqlWith.Cte("__n", nRows)),
                cross(new SqlSource.Subselect(counted, "k", null), new SqlSource.Subselect(size, "n", null)),
                wantSize ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                        new Valued(TYPED_EXPECTED, wanted, SqlType.Scalar.BIGINT, Type.Primitive.INTEGER, false),
                        new Valued(TYPED_ACTUAL, count, SqlType.Scalar.BIGINT, Type.Primitive.INTEGER, false)));
    }

    /** {@code assertEmpty} / {@code assertNotEmpty}'s TYPED member (item 3's S4): the side's values as one list (an empty
     *  side's NULL); the verdict its emptiness under {@code wantEmpty}; the list rides the failure. */
    public static @com.legend.base.Nullable TypedRow typedEmpty(SqlQuery wrapped, Type kind, boolean wantEmpty) {
        SqlQuery rows = valueRows(wrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery list = listed("__a", rows);
        SqlExpr empty = noRows("__a", rows);
        return typedRow(List.of(new SqlWith.Cte("__a", rows)), new SqlSource.Subselect(list, "a", null),
                wantEmpty ? empty : SqlExpr.Call.of(SqlFn.NOT, empty),
                List.of(listValue(TYPED_ACTUAL, "a", list, kind)));
    }

    /** {@code assertInstanceOf}'s TYPED member where the value's static type decided it (item 3's S4): the decided
     *  verdict, then the value's typed list and the type's name riding the failure for the host's record
     *  ({@code AssertKind.INSTANCE_OF}). Null where the value has no typed column. */
    public static @com.legend.base.Nullable TypedRow typedInstanceOf(SqlQuery wrapped, Type kind, boolean holds,
            String type) {
        SqlQuery rows = valueRows(wrapped);
        if (rows == null) {
            return null;
        }
        SqlQuery list = listed("__a", rows);
        return typedRow(List.of(new SqlWith.Cte("__a", rows)), new SqlSource.Subselect(list, "a", null),
                new SqlExpr.BoolLit(holds), List.of(listValue(TYPED_ACTUAL, "a", list, kind), new Valued("__t",
                        new SqlExpr.StringLit(type), SqlType.Scalar.VARCHAR, Type.Primitive.STRING, false)));
    }

    /** {@code assertContains} / {@code assertNotContains}' TYPED member (item 3's S4): the collection's values and the
     *  value's, typed; the verdict whether some element {@code IS NOT DISTINCT FROM} the value, under {@code want}; both
     *  lists ride the failure, the collection's first. */
    public static @com.legend.base.Nullable TypedRow typedContains(SqlQuery collWrapped, Type collKind,
            SqlQuery valWrapped, Type valKind, boolean want) {
        // a date-time compared by its instant (register S399), as equality's sides are (the dates audit, 2026-11-22)
        SqlQuery cRaw = valueRows(collWrapped);
        SqlQuery vRaw = valueRows(valWrapped);
        SqlQuery cRows = cRaw == null ? null : instants(cteRows("__xc", cRaw), collKind);
        SqlQuery vRows = vRaw == null ? null : instants(cteRows("__xv", vRaw), valKind);
        if (cRaw == null || vRaw == null || cRows == null || vRows == null || !oneCarrier(cRows, vRows)) {
            return null;
        }
        // the record's values as each side carries them, not the instants compared (the precision audit's M4)
        SqlQuery cList = listed("__xc", cRaw);
        SqlQuery vList = listed("__xv", vRaw);
        OutputCol mOut = new OutputCol("__m", SqlType.Scalar.BIGINT, false);
        SqlSource matched = new SqlSource.Join(
                new SqlSource.Table("__c", "__c", cRows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE),
                new SqlSource.Table("__v", "__v", vRows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.of("__c", cRows.outputs(), O),
                        SqlExpr.Column.of("__v", vRows.outputs(), O)));
        SqlSelect m = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                false, List.of()), "__m", mOut)), false, matched, null, List.of(), null, null, List.of(), null, null,
                List.of(mOut));
        SqlExpr some = SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Column.of("m", m.outputs(), "__m"),
                new SqlExpr.IntLit(0));
        return typedRow(List.of(new SqlWith.Cte("__xc", cRaw), new SqlWith.Cte("__xv", vRaw),
                        new SqlWith.Cte("__c", cRows), new SqlWith.Cte("__v", vRows)),
                cross(cross(new SqlSource.Subselect(m, "m", null), new SqlSource.Subselect(cList, "c", null)),
                        new SqlSource.Subselect(vList, "v", null)),
                want ? some : SqlExpr.Call.of(SqlFn.NOT, some), List.of(
                        listValue(TYPED_EXPECTED, "c", cList, collKind), listValue(TYPED_ACTUAL, "v", vList, valKind)));
    }

    /** {@code assertEqWithinTolerance}'s TYPED member (item 3's S4): the three numbers as their plans type them, the
     *  verdict {@code |e - a| <= t} computed in {@code arithmetic} (a NULL operand fails); each number rides the failure
     *  as its own kind. Null where a side has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedTolerance(SqlQuery eWrapped, Type eKind, SqlQuery aWrapped,
            Type aKind, SqlQuery tWrapped, Type tKind, SqlType arithmetic) {
        SqlQuery eRows = valueRows(eWrapped);
        SqlQuery aRows = valueRows(aWrapped);
        SqlQuery tRows = valueRows(tWrapped);
        if (eRows == null || aRows == null || tRows == null) {
            return null;
        }
        SqlQuery ev = single("__e", eRows);
        SqlQuery av = single("__a", aRows);
        SqlQuery tv = single("__t", tRows);
        SqlExpr e = SqlExpr.Column.of("e", ev.outputs(), SINGLE);
        SqlExpr a = SqlExpr.Column.of("a", av.outputs(), SINGLE);
        SqlExpr t = SqlExpr.Column.of("t", tv.outputs(), SINGLE);
        SqlExpr within = SqlExpr.Call.of(SqlFn.LESS_EQUAL, SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS,
                new SqlExpr.Cast(e, arithmetic, SqlExpr.Reason.JUDGE), new SqlExpr.Cast(a, arithmetic,
                        SqlExpr.Reason.JUDGE))), new SqlExpr.Cast(t, arithmetic, SqlExpr.Reason.JUDGE));
        return typedRow(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows),
                        new SqlWith.Cte("__t", tRows)),
                cross(cross(new SqlSource.Subselect(ev, "e", null), new SqlSource.Subselect(av, "a", null)),
                        new SqlSource.Subselect(tv, "t", null)),
                SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false)), List.of(
                        new Valued(TYPED_EXPECTED, e, ev.outputs().get(0).type(), eKind, false),
                        new Valued(TYPED_ACTUAL, a, av.outputs().get(0).type(), aKind, false),
                        new Valued("__t", t, tv.outputs().get(0).type(), tKind, false)));
    }

    /** A quantified {@code assert} / {@code assertFalse}'s vector form, TYPED (item 3's S4): the predicate's Booleans
     *  as the vector's plan types them; the verdict that none is otherwise than {@code wantTrue}; its record fixed at
     *  planning, as the host's vector judgment's. Null where the vector has no typed value column. */
    public static @com.legend.base.Nullable TypedRow typedAllOf(SqlQuery vectorWrapped, boolean wantTrue) {
        SqlQuery rows = valueRows(vectorWrapped);
        if (rows == null) {
            return null;
        }
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        SqlSelect otherwise = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of("__v", rows.outputs(), RN)), false, List.of()), K, kOut)), false,
                new SqlSource.Table("__v", "__v", rows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                        SqlExpr.Column.of("__v", rows.outputs(), O), new SqlExpr.BoolLit(wantTrue))),
                List.of(), null, null, List.of(), null, null, List.of(kOut));
        return typedRow(List.of(new SqlWith.Cte("__v", rows)), new SqlSource.Subselect(otherwise, "o", null),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("o", otherwise.outputs(), K), new SqlExpr.IntLit(0)),
                List.of());
    }

    /** The {@code $need->forAll(n | $have->contains($n))} subset fold of a condition, TYPED (item 3's S4): no needed
     *  value without a have that is not distinct from it, under {@code wantTrue}; its record the condition's, fixed at
     *  planning; a date-time by its instant ({@code kind}, the sides' one typed class). Null where a side has no typed
     *  value column. */
    public static @com.legend.base.Nullable TypedRow typedSubset(SqlQuery needWrapped, SqlQuery haveWrapped,
            Type kind, boolean wantTrue) {
        SqlQuery nRows = instants(valueRows(needWrapped), kind);
        SqlQuery hRows = instants(valueRows(haveWrapped), kind);
        if (nRows == null || hRows == null || !oneCarrier(nRows, hRows)) {
            return null;
        }
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlExpr present = new SqlExpr.Exists(new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1),
                "__one", one)), false, new SqlSource.Table("__h", "__h", hRows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                SqlExpr.Column.of("__h", hRows.outputs(), O), SqlExpr.Column.of("__n", nRows.outputs(), O)), List.of(),
                null, null, List.of(), null, null, List.of(one)));
        OutputCol kOut = new OutputCol(K, SqlType.Scalar.BIGINT, false);
        SqlSelect missing = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(SqlExpr.Column.of("__n", nRows.outputs(), RN)), false, List.of()), K, kOut)), false,
                new SqlSource.Table("__n", "__n", nRows.outputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), SqlExpr.Call.of(SqlFn.NOT, present), List.of(), null, null,
                List.of(), null, null, List.of(kOut));
        SqlExpr none = SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of("m", missing.outputs(), K),
                new SqlExpr.IntLit(0));
        return typedRow(List.of(new SqlWith.Cte("__n", nRows), new SqlWith.Cte("__h", hRows)),
                new SqlSource.Subselect(missing, "m", null), wantTrue ? none : SqlExpr.Call.of(SqlFn.NOT, none),
                List.of());
    }

    /**
     * A quantified equality judged element by element, TYPED (item 3's S4): {@code pairs}' rows, each element's two
     * operands one {@code {first, second}} struct in the source's order; each element's verdict its two typed operands
     * not distinct ({@code neverEqual}: kinds lite never calls equal, S127); the verdict that none fails, and where one
     * does, the FIRST failing element's zero-based index and its two operands ride the failure. Null where the pairs'
     * struct states no field types.
     */
    public static @com.legend.base.Nullable TypedRow typedPerElement(SqlQuery pairs, Type expectedKind,
            Type actualKind, boolean neverEqual) {
        OutputCol valueOut = pairs.outputs().get(0);
        if (!(valueOut.type() instanceof SqlType.Struct st) || st.fields().size() != 2) {
            return null;
        }
        SqlType ft = st.fields().stream().filter(f -> f.name().equals("first")).map(SqlType.Struct.Field::type)
                .findFirst().orElse(null);
        SqlType sdt = st.fields().stream().filter(f -> f.name().equals("second")).map(SqlType.Struct.Field::type)
                .findFirst().orElse(null);
        if (ft == null || sdt == null) {
            return null;
        }
        SqlQuery numbered = SideOrdinal.withOrdinal(pairs);
        SqlExpr value = SqlExpr.Column.of("__s", numbered.outputs(), valueOut.name());
        SqlExpr first = SqlExpr.StructGet.of(value, "first");
        SqlExpr second = SqlExpr.StructGet.of(value, "second");
        SqlExpr ok = neverEqual ? new SqlExpr.BoolLit(false) : SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, first, second);
        OutputCol okOut = new OutputCol("__ok", SqlType.Scalar.BOOLEAN, false);
        OutputCol fOut = new OutputCol("__f", ft, true);
        OutputCol sOut = new OutputCol("__x", sdt, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<OutputCol> pe = List.of(okOut, fOut, sOut, rnOut);
        SqlQuery rows = new SqlSelect(List.of(new SqlSelect.Projection(ok, "__ok", okOut),
                new SqlSelect.Projection(first, "__f", fOut), new SqlSelect.Projection(second, "__x", sOut),
                new SqlSelect.Projection(SqlExpr.Column.of("__s", numbered.outputs(), SideOrdinal.ORD), RN, rnOut)),
                false, new SqlSource.Subselect(numbered, "__s", null), null, List.of(), null, null, List.of(), null,
                null, pe);
        SqlExpr failingAt = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Column.of("__pe", pe, "__ok")),
                SqlExpr.Column.of("__pe", pe, RN))), null, SqlExpr.Reason.JUDGE);
        OutputCol fi = new OutputCol("__fi", SqlType.Scalar.BIGINT, true);
        OutputCol fe = new OutputCol("__fe", ft, true);
        OutputCol fa = new OutputCol("__fa", sdt, true);
        SqlSelect b = new SqlSelect(List.of(
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(failingAt), false, List.of()),
                        "__fi", fi),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.ARG_MIN,
                        List.of(SqlExpr.Column.of("__pe", pe, "__f"), failingAt), false, List.of()), "__fe", fe),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.ARG_MIN,
                        List.of(SqlExpr.Column.of("__pe", pe, "__x"), failingAt), false, List.of()), "__fa", fa)),
                false, new SqlSource.Table("__pe", "__pe", pe, false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(fi, fe, fa));
        SqlExpr firstFailing = SqlExpr.Column.of("b", b.outputs(), "__fi");
        return typedRow(List.of(new SqlWith.Cte("__pe", rows)), new SqlSource.Subselect(b, "b", null),
                SqlExpr.Call.of(SqlFn.IS_NULL, firstFailing), List.of(
                        new Valued(TYPED_EXPECTED, SqlExpr.Column.of("b", b.outputs(), "__fe"), ft, expectedKind, false),
                        new Valued(TYPED_ACTUAL, SqlExpr.Column.of("b", b.outputs(), "__fa"), sdt, actualKind, false),
                        new Valued("__i", SqlExpr.Call.of(SqlFn.MINUS, firstFailing, new SqlExpr.IntLit(1)),
                                SqlType.Scalar.BIGINT, Type.Primitive.INTEGER, false)));
    }

    /** A typed member's value: its column, its expression, its SQL and Pure types, a list or one. */
    private record Valued(String name, SqlExpr expr, SqlType type, Type kind, boolean many) {
    }

    /** A listed side's list ({@link #listed}) as a typed member's value. */
    private static Valued listValue(String name, String alias, SqlQuery listed, Type kind) {
        return new Valued(name, SqlExpr.Column.of(alias, listed.outputs(), LISTED), listed.outputs().get(0).type(), kind,
                true);
    }

    /**
     * A TYPED member's row (item 3's S4): its verdict {@code holds}, then each of {@code values}, written only where
     * the verdict fails -- one row over {@code from}, the members' CTEs at its head -- and the schema of its columns.
     */
    private static TypedRow typedRow(List<SqlWith.Cte> ctes, SqlSource from, SqlExpr holds, List<Valued> values) {
        if (comparesSides(holds)) {
            // the comparison node is computed ONCE, beneath the row that writes the values only where it fails (its
            // portable form is a subquery a database would run once per reference)
            List<SqlSelect.Projection> ips = new ArrayList<>();
            List<OutputCol> iOuts = new ArrayList<>();
            OutputCol hOut = new OutputCol("__h", SqlType.Scalar.BOOLEAN, false);
            ips.add(new SqlSelect.Projection(holds, "__h", hOut));
            iOuts.add(hOut);
            for (int i = 0; i < values.size(); i++) {
                OutputCol x = new OutputCol("__x" + i, values.get(i).type(), true);
                ips.add(new SqlSelect.Projection(values.get(i).expr(), x.name(), x));
                iOuts.add(x);
            }
            SqlSelect inner = new SqlSelect(ips, false, from, null, List.of(), null, null, List.of(), null, null,
                    iOuts);
            List<Valued> lifted = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                Valued v = values.get(i);
                lifted.add(new Valued(v.name(), SqlExpr.Column.of("w", iOuts, "__x" + i), v.type(), v.kind(), v.many()));
            }
            from = new SqlSource.Subselect(inner, "w", null);
            holds = SqlExpr.Column.of("w", iOuts, "__h");
            values = lifted;
        }
        OutputCol vOut = new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false);
        List<SqlSelect.Projection> ps = new ArrayList<>(List.of(new SqlSelect.Projection(holds, VERDICT, vOut)));
        List<OutputCol> outs = new ArrayList<>(List.of(vOut));
        List<Type.RelationType.Column> cols = new ArrayList<>(List.of(new Type.RelationType.Column(VERDICT,
                Type.Primitive.BOOLEAN, com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
        for (Valued v : values) {
            OutputCol o = new OutputCol(v.name(), v.type(), true);
            ps.add(new SqlSelect.Projection(whereFails(holds, v.expr()), v.name(), o));
            outs.add(o);
            cols.add(new Type.RelationType.Column(v.name(), v.kind(), v.many()
                    ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                    : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE));
        }
        SqlSelect row = new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null, outs);
        return new TypedRow(nanos(ctes.isEmpty() ? row : new SqlWith(ctes, row)), new Type.RelationType(cols));
    }

    /** {@code member} with its judge's instants read to the nanosecond where a value it compares carries a digit of a
     *  second past the microsecond -- a literal's, a fact of the plan, never a row's (register S399; S181 and the
     *  user's rule of 2026-11-22: "do nanoseconds typed for whatever range allows, refuse if out of range"): every
     *  {@code JUDGE} cast to TIMESTAMP becomes {@code JUDGE_NANOS}, which a database whose date-time holds
     *  microseconds reads in its nanosecond type where it has one, refusing by name a value outside the days that
     *  type holds (DuckDB's); an assert with no such value is read to the microsecond, every year. */
    private static SqlQuery nanos(SqlQuery member) {
        boolean[] carries = {false};
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                carries[0] |= digitsPastMicros(e);
                return e;
            }
        }.rewrite(member);
        if (!carries[0]) {
            return member;
        }
        return new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                return e instanceof SqlExpr.Cast c && c.reason() == SqlExpr.Reason.JUDGE
                        && c.target() == SqlType.Scalar.TIMESTAMP
                        ? new SqlExpr.Cast(c.value(), SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE_NANOS) : e;
            }
        }.rewrite(member);
    }

    /** Whether {@code e} is a date-time literal -- typed, or its text carrier ({@code TEMPORAL_TEXT}, a root literal's
     *  written precision: RootLiterals) -- with a nonzero digit of a second past the sixth. A String literal is none,
     *  whatever it spells (the precision audit's M2, 2026-11-22: a filter's text switched an assert to nanoseconds). */
    private static boolean digitsPastMicros(SqlExpr e) {
        if (e instanceof SqlExpr.Call ns && ns.fn() == SqlFn.FROM_EPOCH_NS) {
            // a date-time computed to the nanosecond (an adjust by nanoseconds: DateShifts' epoch count): a fact of the
            // plan too (the instances audit's M-a, 2026-11-22: its value, a TIMESTAMP_NS, was refused where DuckDB
            // compares it)
            return true;
        }
        if (e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.TEMPORAL_TEXT
                && !(c.value() instanceof SqlExpr.StringLit)) {
            // a literal collection's carrier: its elements' texts inside (LiteralSpelling.staticTemporalText; the
            // instances audit's H-A, 2026-11-22: a list of nanosecond literals was read to the microsecond)
            boolean[] found = {false};
            new com.legend.sql.SqlRewriter() {
                @Override
                protected SqlExpr expr(SqlExpr x) {
                    found[0] |= x instanceof SqlExpr.StringLit sl && fractionDigits(sl.value()) > 6;
                    return x;
                }
            }.rewrite(SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(
                    new SqlSelect.Projection(c.value(), "v", null))));
            return found[0];
        }
        String text = e instanceof SqlExpr.TimestampLit t ? t.iso()
                : e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.TEMPORAL_TEXT
                        && c.value() instanceof SqlExpr.StringLit s ? s.value() : null;
        return text != null && fractionDigits(text) > 6;
    }

    /** The significant digits of a second a date-time's text writes (its trailing zeros no digit). */
    private static int fractionDigits(String text) {
        if (text.indexOf('.') < 0) {
            return 0;
        }
        int at = text.indexOf('.') + 1;
        int end = at;
        while (end < text.length() && Character.isDigit(text.charAt(end))) {
            end++;
        }
        String digits = text.substring(at, end);
        int significant = digits.length();
        while (significant > 0 && digits.charAt(significant - 1) == '0') {
            significant--;
        }
        return significant;
    }

    /** Whether {@code holds} is the sides' comparison node, or its negation. */
    private static boolean comparesSides(SqlExpr holds) {
        SqlExpr h = holds instanceof SqlExpr.Call n && n.fn() == SqlFn.NOT ? n.args().get(0) : holds;
        return h instanceof SqlExpr.Call c && (c.fn() == SqlFn.SAME_IN_ORDER || c.fn() == SqlFn.SAME_IN_ANY_ORDER);
    }

    private static SqlSource cross(SqlSource left, SqlSource right) {
        return new SqlSource.Join(left, right, SqlSource.Join.Kind.CROSS, null);
    }

    private static final String SINGLE = "__v";

    /** A side's typed rows ({@code cte}) as its one value: {@code __v}, the least of its values (a one-valued side's
     *  value; NULL where it has none). */
    private static SqlQuery single(String cte, SqlQuery rows) {
        OutputCol o = new OutputCol(SINGLE, rows.outputs().get(0).type(), true);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN,
                List.of(SqlExpr.Column.of(cte, rows.outputs(), O)), false, List.of()), SINGLE, o)), false,
                new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                null, List.of(), null, null, List.of(), null, null, List.of(o));
    }

    /** {@code value} where the verdict {@code holds} not, else NULL: a typed member's values ride its failure alone. */
    private static SqlExpr whereFails(SqlExpr holds, SqlExpr value) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(holds, new SqlExpr.NullLit())), value,
                SqlExpr.Reason.JUDGE);
    }

    /** A side's typed rows ({@code cte}: {@code __o}, {@code __rn}) folded to one row: {@code __l}, its values in its own
     *  order -- the LIST reducer, which each database carries as it carries a list (H2: a JSON array) and exec decodes
     *  as any list result. The list rides the failure's record; no verdict reads it (emptiness is the rows', noRows). */
    private static SqlQuery listed(String cte, SqlQuery rows) {
        OutputCol o = rows.outputs().get(0);
        OutputCol l = new OutputCol(LISTED, new SqlType.Array(o.type()), true);
        // never NULL: a typed rows CTE drops a NULL value (valueRows), so the list needs no null clause (H2's)
        SqlExpr value = SqlExpr.Column.of(cte, O, o.type(), false, OutputCol.Origin.DERIVED);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.LIST,
                List.of(value), false,
                List.of(new SqlSelect.SortKey(SqlExpr.Column.of(cte, rows.outputs(), RN), true, null, null))),
                LISTED, l)), false, new SqlSource.Table(cte, cte, rows.outputs(), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, List.of(l));
    }

    /**
     * A side's TYPED rows (item 3's S4): {@code __o}, each value as the side's plan types it, and {@code __rn}, its place
     * in the side's own order -- numbered as {@link #canonRows} numbers them, with no canon; a NULL value dropped (Pure
     * has no null value). Null where the value's column states no type.
     */
    private static @com.legend.base.Nullable SqlQuery valueRows(SqlQuery wrapped) {
        return valueRows(wrapped, 1);
    }

    /** {@link #valueRows} carrying the side's first {@code width} columns: {@code __o}, then {@code __s1, __s2, ...}
     *  (a tagged value's slots beside its tag), then {@code __rn}; the rows those whose first column is not NULL. */
    private static @com.legend.base.Nullable SqlQuery valueRows(SqlQuery wrapped, int width) {
        if (!(wrapped instanceof SqlSelect ws) || ws.projections().size() < width) {
            return null;
        }
        List<SqlSelect.Projection> cols = ws.projections().subList(0, width);
        for (SqlSelect.Projection p : cols) {
            if (p.out() == null || p.alias() == null) {
                return null;
            }
        }
        List<OutputCol> outs = new ArrayList<>();
        for (int i = 0; i < width; i++) {
            outs.add(new OutputCol(i == 0 ? O : "__s" + i, java.util.Objects.requireNonNull(cols.get(i).out()).type(),
                    i > 0));
        }
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<OutputCol> all = new ArrayList<>(outs);
        all.add(rnOut);
        if (!plainProjection(ws)) {
            // a wrap that stays a layer (an aggregate value, a filtered or capped wrap): its values, its own order
            List<OutputCol> kept = cols.stream().map(p -> java.util.Objects.requireNonNull(p.out())).toList();
            SqlSelect trimmed = new SqlSelect(cols, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                    ws.having(), ws.qualify(), ws.orderBy(), ws.limit(), ws.offset(), kept);
            SqlQuery read = ws.orderBy().isEmpty() ? trimmed : SideOrdinal.withOrdinal(trimmed);
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (int i = 0; i < width; i++) {
                ps.add(new SqlSelect.Projection(SqlExpr.Column.of("w", read.outputs(),
                        java.util.Objects.requireNonNull(cols.get(i).alias())), outs.get(i).name(), outs.get(i)));
            }
            ps.add(new SqlSelect.Projection(rowNumber(read.outputs()), RN, rnOut));
            return new SqlSelect(ps, false, new SqlSource.Subselect(read, "w", null), SqlExpr.Call.of(
                    SqlFn.IS_NOT_NULL, SqlExpr.Column.of("w", read.outputs(), java.util.Objects.requireNonNull(
                            cols.get(0).alias()))), List.of(), null,
                    null, List.of(), null, null, all);
        }
        List<SqlSelect.SortKey> order = ws.orderBy();
        SqlSource from = ws.from();
        if (order.isEmpty() && from instanceof SqlSource.Subselect sub
                && SideOrdinal.defined(sub.inner()) instanceof SqlQuery ordered) {
            // the order the side's own plan defines, numbered inside it (SideOrdinal), as canonRows numbers it
            from = new SqlSource.Subselect(ordered, sub.alias(), sub.frameName());
            order = List.of(new SqlSelect.SortKey(SqlExpr.Column.of(sub.alias(), ordered.outputs(), SideOrdinal.ORD),
                    true, null, null));
        }
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), order,
                null);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (int i = 0; i < width; i++) {
            ps.add(new SqlSelect.Projection(cols.get(i).expr(), outs.get(i).name(), outs.get(i)));
        }
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        return new SqlSelect(ps, false, from, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, cols.get(0).expr()), List.of(), null,
                null, List.of(), null, null, all);
    }

    /** {@code row} carrying each side's kind for the frame (lite's sentence where the two texts tie). */
    private static SqlQuery withKinds(SqlQuery row, String expectedKind, String actualKind) {
        SqlWith w = (SqlWith) row;
        SqlSelect body = (SqlSelect) w.body();
        List<SqlSelect.Projection> ps = new ArrayList<>(body.projections());
        ps.add(new SqlSelect.Projection(new SqlExpr.StringLit(expectedKind), EXPECTED_KIND,
                new OutputCol(EXPECTED_KIND, SqlType.Scalar.VARCHAR, false)));
        ps.add(new SqlSelect.Projection(new SqlExpr.StringLit(actualKind), ACTUAL_KIND,
                new OutputCol(ACTUAL_KIND, SqlType.Scalar.VARCHAR, false)));
        return new SqlWith(w.ctes(), body.withProjections(ps));
    }

    /** {@code row} with the two sides as the failure message writes them ({@link Message}), read off the facts rows'
     *  written columns: the message's two texts, which {@link #selfContained} prefers to the canons. */
    private static SqlQuery withWrittenSides(SqlQuery row, boolean sorted) {
        return withWrittenSides(row, sorted, false);
    }

    /** {@code multiset}: the sides were compared as multisets: the frame's lite sentence says "in any order". */
    private static SqlQuery withWrittenSides(SqlQuery row, boolean sorted, boolean multiset) {
        SqlWith w = (SqlWith) row;
        SqlSelect body = (SqlSelect) w.body();
        List<OutputCol> outs = writtenFactOutputs();
        java.util.function.Function<String, SqlExpr> written = alias -> {
            SqlExpr list = SqlExpr.Column.of(alias, outs, F_LIST_W);
            if (sorted) {
                return list;
            }
            SqlExpr bothOne = SqlExpr.Call.of(SqlFn.AND,
                    SqlExpr.Call.of(SqlFn.EQUAL, factCol("e", F_N), new SqlExpr.IntLit(1)),
                    SqlExpr.Call.of(SqlFn.EQUAL, factCol("a", F_N), new SqlExpr.IntLit(1)));
            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(bothOne, SqlExpr.Column.of(alias, outs, F_ONE_W))),
                    list, SqlExpr.Reason.JUDGE);
        };
        List<SqlSelect.Projection> ps = new ArrayList<>(body.projections());
        ps.add(new SqlSelect.Projection(written.apply("e"), EXPECTED_WRITTEN,
                new OutputCol(EXPECTED_WRITTEN, SqlType.Scalar.VARCHAR, true)));
        ps.add(new SqlSelect.Projection(written.apply("a"), ACTUAL_WRITTEN,
                new OutputCol(ACTUAL_WRITTEN, SqlType.Scalar.VARCHAR, true)));
        if (multiset) {
            ps.add(new SqlSelect.Projection(new SqlExpr.BoolLit(true), MULTISET_HINT,
                    new OutputCol(MULTISET_HINT, SqlType.Scalar.BOOLEAN, false)));
        }
        return new SqlWith(w.ctes(), new SqlSelect(ps, body.distinct(), body.from(), body.where(), body.groupBy(),
                body.having(), body.qualify(), body.orderBy(), body.limit(), body.offset(), body.outputs()));
    }

    /** A side folded to its facts row: {@code rows} is the rows CTE's query
     * (null when the facts are spelled inline over the plan). */
    private record Folded(String name, @com.legend.base.Nullable SqlQuery rows, SqlQuery facts) {
    }

    private static Folded fold(String name, Side s, boolean keepRows) {
        if (!s.many() && !keepRows && s.wrapped() instanceof SqlSelect ws && plainProjection(ws)) {
            return new Folded(name, null, inlineFacts(s));
        }
        return new Folded(name, canonRows(s, keepRows), sideFacts(name, s.many(), s.byCanonText(), s.message()));
    }

    // ── the GRID forms (leg 3.1b): a TABULAR side rides the grid wrap
    // (CanonicalRenderSql.wrapTdsCanon: __rowcanon + __cell<i>); its value
    // PEER (a literal list) is framed into rows of the grid's width, or
    // compared as a loose cell pool (the sameElements form) — the same
    // rules TdsCompare.peerRowCanons / tdsCellCanons apply in Java.

    /** A grid side: its wrapped plan and its width (columns). */
    /** {@code floatColumns}: per column, whether it is DECLARED Float —
     * the 2-ULP leniency's operand columns. */
    /** {@code emptyIsNull}: per column, whether the assert's own grammar
     * conflates the empty string with NULL ({@code toCSV} prints both as
     * an empty cell) — the verdict then judges under that equivalence: a
     * String cell's empty canon reads as the TDSNull sentinel. */
    public record GridSide(SqlQuery wrapped, int width, List<Boolean> floatColumns,
            List<Boolean> emptyIsNull) {
        public GridSide(SqlQuery wrapped, int width, List<Boolean> floatColumns) {
            this(wrapped, width, floatColumns, List.of());
        }
        boolean emptyIsNullAt(int i) {
            return i < emptyIsNull.size() && emptyIsNull.get(i);
        }
    }

    /** A value peer of a grid: its wrapped plan, the literal-channel canon
     * column, and whether it is the EXPECTED side (the golden's
     * {@code 'TDSNull'} string cells spell the bare sentinel there only —
     * a real 'TDSNull' string on OUR wire stays quoted). */
    public record PeerSide(SqlQuery wrapped, String canonColumn, boolean expected,
            boolean isFloat) {
    }

    /** {@code assertEquals} over rows: the grid's row canons against the peer's cells chunked by the grid's width;
     * ordered by arrival, or as a row multiset when {@code multiset}. {@code gridIsExpected} says which side the grid
     * is; {@code words} what the frame reads (lite's: each side written, the grid's cells flattened as the peer's
     * are). */
    public static SqlQuery gridRows(GridSide grid, PeerSide peer,
            boolean gridIsExpected, boolean multiset, VerdictWords words) {
        // each side WRITTEN, as lite's words read it (legend-pure's canon reading, VerdictReads.CANONS, left with A4 of
        // the compatibility program, stage A ratified by the user 2026-11-19: the database judge plans for the product
        // alone, rule 4 of the rules of record)
        SqlQuery g = gridRowCanons(grid, GRID, RowWriting.FLAT);
        SqlQuery p = peerRowCanons(peer, grid.width(), true);
        SqlExpr divisible = SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                SqlExpr.Call.of(SqlFn.MOD,
                        scalarOver("__peer", new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                                List.of(col("__peer", RN)), false, List.of()),
                                "__n", SqlType.Scalar.BIGINT, null),
                        new SqlExpr.IntLit(grid.width())),
                new SqlExpr.IntLit(0));
        List<SqlWith.Cte> extra = List.of(new SqlWith.Cte(GRID, grid.wrapped()),
                new SqlWith.Cte("__peer", peerCells(peer)));
        List<SqlExpr.Case.When> more = List.of(new SqlExpr.Case.When(divisible,
                new SqlExpr.StringLit("tds-peer: cells not divisible by width "
                        + grid.width())));
        // a Float compares exactly, as the host's grid compare does (census C1's J11, 2026-11-17; the tolerance's
        // positional cells left with A4)
        return gridIsExpected ? writtenStatement(g, p, multiset, more, extra, multiset)
                : writtenStatement(p, g, multiset, more, extra, multiset);
    }

    /** {@code assertSameElements} over a grid: the loose CELL pool (every cell of every row) against the peer's
     * cells, as a multiset; {@code words} what the frame reads (lite's: each side's cells written, as a list). */
    public static SqlQuery gridCells(GridSide grid, PeerSide peer,
            boolean gridIsExpected, VerdictWords words) {
        // each side's cells WRITTEN, as lite's words read them (the canon reading left with A4)
        SqlQuery g = gridCellCanons(grid, GRID, true);
        SqlQuery p = peerCells(peer, true);
        List<SqlWith.Cte> named = List.of(new SqlWith.Cte(GRID, grid.wrapped()));
        return gridIsExpected ? writtenStatement(g, p, true, List.of(), named, true)
                : writtenStatement(p, g, true, List.of(), named, true);
    }

    // ── the ONE-LINE families (leg 3.1c): size / empty / contains / a boolean
    // condition / the tolerance assert / the forAll-contains subset — each a
    // predicate over the same row sources, returned in the same verdict row.

    /** {@code assertSize}: the side's row count against {@code n} (the size
     * side's one canon text as a BIGINT); {@code envelope} = the read is a
     * relation-rooted execute's {@code .values}, which holds ONE TDS. */
    /** {@code negated}: {@code assertNotSize} (#28): the same verdict row, its failure the negated sentence -- lite's
     *  {@code NOT_SIZE}, or legend-pure's 'size should not equal: %s' -- the caller planning it with the polarity
     *  flipped. */
    public static SqlQuery size(SqlQuery sideRows, SqlQuery nScalar, boolean envelope, VerdictWords words,
            boolean negated) {
        OneRow c = envelope ? constantOf("c", new SqlExpr.IntLit(1), "__n", SqlType.Scalar.BIGINT)
                : countOf("c", "__a");
        OneRow n = rowOf("n", nScalar);
        SqlExpr count = c.col("__n");
        SqlExpr want = new SqlExpr.Cast(n.col(C), SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE);
        SqlExpr wantText = new SqlExpr.Cast(want, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        SqlExpr countText = new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        // upstream's own words (assertSize.pure: 'expected size: %s, actual size: %s'), the host's (PureAsserts)
        return predicateOver(envelope ? List.of() : List.of(new SqlWith.Cte("__a", sideRows)),
                List.of(c, n), SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, count, want), wantText, countText,
                new SqlExpr.NullLit(), negated ? words.sizeEquals(wantText) : words.sizeDiffers(wantText, countText));
    }

    /** A GRAPH-shaped side (a class collection serialized as ONE JSON
     * document): its size is the number of ROOT ROWS the fold aggregates —
     * the host rule ({@code p instanceof List ? size : 1}) read off the
     * PLAN, not the document. An array-wrapped root ({@code JsonArrayAgg}
     * under the fold's VARCHAR cast / empty-array COALESCE) counts the rows
     * under the aggregate; a bare-object root is one document when a row
     * exists and NULL (0) otherwise. No JSON function on any dialect, no
     * document built to be measured. A plan that is not the fold's
     * one-projection select is a construction fault, loud. */
    /** A graph plan's ROOT ROWS (rung 12): the fold's own from / where / caps
     * projecting the root table's PHYSICAL columns ({@code root.*} — the
     * store's declared list can exceed what a seeded table carries; a reader
     * that names an absent column fails exactly as it would over the table).
     * Null when the plan is not a fold over a single root TABLE. */
    public static @com.legend.base.Nullable SqlSelect classExtentRows(SqlQuery graphPlan) {
        SqlQuery fold = graphPlan;
        for (int depth = 0; depth < 4 && fold instanceof SqlSelect w
                && !w.projections().isEmpty()
                && w.projections().get(0).expr() instanceof SqlExpr.Column
                && w.from() instanceof SqlSource.Subselect inner; depth++) {
            fold = inner.inner();
        }
        if (!(fold instanceof SqlSelect ps) || ps.projections().size() != 1
                || !ps.groupBy().isEmpty()) {
            return null;
        }
        SqlSource leftmost = ps.from();
        while (leftmost instanceof SqlSource.Join j) {
            leftmost = j.left();
        }
        if (!(leftmost instanceof SqlSource.Table root) || root.outputs().isEmpty()) {
            return null;
        }
        List<SqlSelect.Projection> ps2 = List.of(
                new SqlSelect.Projection(new SqlExpr.Star(root.alias()), null, null));
        return new SqlSelect(ps2, ps.distinct(), ps.from(), ps.where(), ps.groupBy(), ps.having(),
                ps.qualify(), ps.orderBy(), ps.limit(), ps.offset(), root.outputs());
    }

    public static SqlExpr graphCount(SqlQuery graphPlan) {
        // the canon wrap over a graph side is a pass-through select (the
        // document column beside its canon) around the fold: descend to it
        SqlQuery fold = graphPlan;
        for (int depth = 0; depth < 4 && fold instanceof SqlSelect w
                && !w.projections().isEmpty()
                && w.projections().get(0).expr() instanceof SqlExpr.Column
                && w.from() instanceof SqlSource.Subselect inner; depth++) {
            fold = inner.inner();
        }
        if (!(fold instanceof SqlSelect ps) || ps.projections().size() != 1
                || !ps.groupBy().isEmpty()) {
            throw new IllegalStateException("graph side: not the fold's one-projection select: "
                    + fold.getClass().getSimpleName()
                    + (fold instanceof SqlSelect gs ? " projections=" + gs.projections().stream()
                            .map(pr -> pr.alias() + ":" + pr.expr().getClass().getSimpleName()).toList()
                            + " groupBy=" + gs.groupBy().size() + " from=" + gs.from().getClass().getSimpleName()
                            : ""));
        }
        SqlExpr top = ps.projections().get(0).expr();
        while (true) {
            if (top instanceof SqlExpr.Cast c) {
                top = c.value();
            } else if (top instanceof SqlExpr.Call k && k.fn() == SqlFn.COALESCE
                    && !k.args().isEmpty()) {
                top = k.args().get(0);
            } else {
                break;
            }
        }
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        SqlSelect.Projection countStar = new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__n", n);
        // a value that aggregates its rows into an array (the array itself, or any form a style writes over it -- the
        // edge's one-object-bare choice among them): the same rows, counted the same way (one object bare is one, the
        // host rule's 1). Read by the value's own structure, never by the edge's shape (L-A.foldedAggregate, the
        // lens audit, 2026-11-18)
        if (aggregatesRowsIntoAnArray(top)) {
            return new SqlExpr.ScalarSubquery(ps.withProjections(List.of(countStar)));
        }
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSelect first = new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                ps.distinct(), ps.from(), ps.where(), ps.groupBy(), ps.having(), ps.qualify(),
                ps.orderBy(), 1L, ps.offset(), List.of(one));
        return new SqlExpr.ScalarSubquery(new SqlSelect(List.of(countStar), false,
                new SqlSource.Subselect(first, "w", null), null, List.of(), null, null,
                List.of(), null, null, List.of(n)));
    }

    /** Whether {@code e} holds the array aggregate of its select's rows outside any subquery. */
    private static boolean aggregatesRowsIntoAnArray(SqlExpr e) {
        return e instanceof SqlExpr.JsonArrayAgg
                || e.children().stream().anyMatch(VerdictSql::aggregatesRowsIntoAnArray);
    }

    /** {@code assertSize} over a graph side. */
    public static SqlQuery sizeOfGraph(SqlQuery graphPlan, SqlQuery nScalar) {
        OneRow c = rowOf("c", ((SqlExpr.ScalarSubquery) graphCount(graphPlan)).subquery());
        OneRow n = rowOf("n", nScalar);
        SqlExpr count = c.col("__n");
        SqlExpr want = new SqlExpr.Cast(n.col(C), SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE);
        return predicateOver(List.of(), List.of(c, n),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, count, want),
                new SqlExpr.Cast(want, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), new SqlExpr.NullLit());
    }

    /** {@code assertEmpty} / {@code assertNotEmpty} over a graph side. */
    public static SqlQuery emptyOfGraph(SqlQuery graphPlan, boolean wantEmpty) {
        OneRow c = rowOf("c", ((SqlExpr.ScalarSubquery) graphCount(graphPlan)).subquery());
        SqlExpr count = c.col("__n");
        SqlExpr isEmpty = SqlExpr.Call.of(SqlFn.EQUAL, count, new SqlExpr.IntLit(0));
        return predicateOver(List.of(), List.of(c),
                wantEmpty ? isEmpty : SqlExpr.Call.of(SqlFn.NOT, isEmpty),
                new SqlExpr.StringLit(wantEmpty ? "empty" : "not empty"),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                        new SqlExpr.StringLit(" element(s)")), new SqlExpr.NullLit());
    }

    /** {@code assertEmpty} / {@code assertNotEmpty}: the side's row count. {@code written}: the rows carry each element
     *  as the failure message writes it ({@link #writtenRows}), else the message names the count alone. */
    /** {@code words}: the failure's words (lite's sentences, or the edge's). */
    public static SqlQuery empty(SqlQuery sideRows, boolean wantEmpty, boolean written, VerdictWords words) {
        OneRow c = countOf("c", "__a");
        SqlExpr count = c.col("__n");
        SqlExpr isEmpty = SqlExpr.Call.of(SqlFn.EQUAL, count, new SqlExpr.IntLit(0));
        List<OneRow> ops = new ArrayList<>(List.of(c));
        // upstream's own words (2026-11-04, the assert bar; the host's PureAsserts): assertNotEmpty.pure's 'Expected
        // non-empty collection'; assertEmpty.pure's '%s is not empty' over the collection as a List, where its
        // elements are written
        SqlExpr message = wantEmpty ? null : words.notEmpty();
        if (wantEmpty && written) {
            OneRow l = writtenListOf("l", "__a");
            ops.add(l);
            message = words.notEmptyWritten(l.col(F_LIST_W));
        }
        return predicateOver(List.of(new SqlWith.Cte("__a", sideRows)), ops,
                wantEmpty ? isEmpty : SqlExpr.Call.of(SqlFn.NOT, isEmpty),
                new SqlExpr.StringLit(wantEmpty ? "empty" : "not empty"),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.Cast(count, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                        new SqlExpr.StringLit(" element(s)")), new SqlExpr.NullLit(), message);
    }

    /** A rows CTE's written elements ({@link #writtenRows}) as one bracketed list in the rows' order: {@code F_LIST_W}. */
    private static OneRow writtenListOf(String alias, String rowsCte) {
        OutputCol o = new OutputCol(F_LIST_W, SqlType.Scalar.VARCHAR, true);
        SqlExpr joined = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG, List.of(col(rowsCte, W), new SqlExpr.StringLit(", ")),
                false, List.of(new SqlSelect.SortKey(col(rowsCte, RN), true, null, null)));
        SqlExpr list = SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["),
                SqlExpr.Call.of(SqlFn.COALESCE, joined, new SqlExpr.StringLit("")), new SqlExpr.StringLit("]"));
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(list, F_LIST_W, o)),
                false, cte(rowsCte), null, List.of(), null, null, List.of(), null, null, List.of(o)), List.of(o));
    }

    /** {@code assertContains}: some element's canon equals the value's. {@code valueWritten}: the value's message
     *  (its kind and the planning style), where the collection's rows are {@link #writtenRows} and the message writes
     *  both; null, the canons'. */
    /** {@code negated}: {@code assertNotContains} (#28): the same verdict row over the written collection, its failure
     *  the negated sentence -- lite's {@code NOT_CONTAINS}, or legend-pure's '%s should not contain %r'. */
    public static SqlQuery contains(SqlQuery collRows, SqlQuery valScalar,
            @com.legend.base.Nullable Message valueWritten, boolean negated, VerdictWords words) {
        OutputCol vc = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        SqlSource v0 = new SqlSource.Table("__v0", "__v0", valScalar.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
        OneRow v = new OneRow("v", new SqlSelect(List.of(new SqlSelect.Projection(
                SqlExpr.Column.of("__v0", valScalar.outputs(), C), C, vc)),
                false, v0, null, List.of(), null, null, List.of(), null, null, List.of(vc)), List.of(vc));
        OutputCol mOut = new OutputCol("__m", SqlType.Scalar.BIGINT, false);
        SqlSource matched = new SqlSource.Join(cte("__a"),
                new SqlSource.Table("__v0", "__v0", valScalar.outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__a", C), SqlExpr.Column.of("__v0", valScalar.outputs(), C)));
        OneRow m = new OneRow("m", new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__m", mOut)),
                false, matched, null, List.of(), null, null, List.of(), null, null, List.of(mOut)), List.of(mOut));
        OneRow t = textAndCountOf("t", "__sa");
        // upstream's own words (assertContains.pure: '%s does not contain %r', the collection its representations
        // joined '[', ', ', ']' in its own order), the host's (PureAsserts.containsMessage): one element bracketed too
        SqlExpr list = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, t.col(F_N), new SqlExpr.IntLit(1)),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["), t.col(F_TEXT), new SqlExpr.StringLit("]")))),
                t.col(F_TEXT), SqlExpr.Reason.JUDGE);
        List<OneRow> ops = new ArrayList<>(List.of(v, m, t));
        SqlExpr message = words.notContainedCanons(list, SqlExpr.Call.of(SqlFn.COALESCE, v.col(C),
                new SqlExpr.StringLit("[]")));
        if (valueWritten != null) {
            OneRow l = writtenListOf("l", "__a");
            OutputCol wo = new OutputCol(W, SqlType.Scalar.VARCHAR, true);
            OneRow vw = new OneRow("vw", new SqlSelect(List.of(new SqlSelect.Projection(
                    written(new Message(valueWritten.kind(), false),
                            SqlExpr.Column.of("__v0", valScalar.outputs(), "value")), W, wo)),
                    false, v0, null, List.of(), null, null, List.of(), null, null, List.of(wo)), List.of(wo));
            ops.add(l);
            ops.add(vw);
            SqlExpr valueText = SqlExpr.Call.of(SqlFn.COALESCE, vw.col(W), new SqlExpr.StringLit("[]"));
            message = negated ? words.contained(l.col(F_LIST_W), valueText)
                    : words.notContained(l.col(F_LIST_W), valueText);
        }
        return predicateOver(List.of(new SqlWith.Cte("__a", collRows), new SqlWith.Cte("__v0", valScalar),
                        new SqlWith.Cte("__sa", sideFacts("__a", true, false))),
                ops, SqlExpr.Call.of(SqlFn.GREATER, m.col("__m"), new SqlExpr.IntLit(0)),
                v.col(C), t.col(F_TEXT), new SqlExpr.NullLit(), message);
    }

    /** {@code assert(cond)} / {@code assertFalse(cond)}: the condition's one
     * canon text is {@code true} / {@code false}; a failure raises {@code message}, the assert's own (upstream's
     * {@code assert(condition, message)}, "Assert failed" with none), never the two canons (2026-11-03, the core
     * parity program: the host's words); null, the canons' frame (a condition another assert is judged by). */
    public static SqlQuery condition(SqlQuery condScalar, boolean wantTrue,
            @com.legend.base.Nullable String message) {
        OneRow a = rowOf("a", condScalar);
        SqlExpr c = a.col(C);
        return predicateOver(List.of(), List.of(a),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, c, new SqlExpr.StringLit(wantTrue ? "true" : "false")),
                new SqlExpr.StringLit(wantTrue ? "true" : "false"),
                SqlExpr.Call.of(SqlFn.COALESCE, c, new SqlExpr.StringLit("[]")), new SqlExpr.NullLit(), message);
    }

    /** {@code coll->map(x | assert(pred))} — the quantified assert (task #14 leg 2,
     * 2026-09-21): every element's condition canon is {@code true} / {@code false};
     * the verdict is that NO row says otherwise (a NULL condition is otherwise); a failure raises {@code message}, the
     * assert's own at its first failing element, as upstream's map raises it. */
    public static SqlQuery allOf(SqlQuery condRows, boolean wantTrue,
            @com.legend.base.Nullable String message) {
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        SqlExpr wanted = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__a", C),
                new SqlExpr.StringLit(wantTrue ? "true" : "false"));
        OneRow otherwise = new OneRow("b", new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col("__a", RN)), false, List.of()), "__n", n)),
                false, cte("__a"), SqlExpr.Call.of(SqlFn.NOT, wanted), List.of(), null, null, List.of(),
                null, null, List.of(n)), List.of(n));
        return predicateOver(List.of(new SqlWith.Cte("__a", condRows)), List.of(otherwise),
                SqlExpr.Call.of(SqlFn.EQUAL, otherwise.col("__n"), new SqlExpr.IntLit(0)),
                new SqlExpr.StringLit("every element " + (wantTrue ? "true" : "false")),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.Cast(otherwise.col("__n"), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                        new SqlExpr.StringLit(" element(s) otherwise")),
                new SqlExpr.NullLit(), message);
    }

    /**
     * A quantified EQUALITY judged element by element (the asserts-as-plan-nodes program, step 5, 2026-11-04):
     * {@code pairs}, the plan of {@code source->map(x | pair(expected, actual))} -- each element's two operands one row's
     * {@code {first, second}}, in the source's order -- and the verdict that every element's operands are equal by the
     * database judge's own spelling of their kinds ({@link LiteralSpelling#leaf}: a Decimal's scale kept); a failure's
     * texts the FIRST failing element's, each as upstream's {@code %r} writes it ({@link Repr}), so the row's message is
     * upstream's {@code '\nexpected: %r\nactual:   %r'} for that element. {@code neverEqual}: the two kinds are never
     * equal under the run's rules (register S127): every element fails, an empty source holds. {@code floats}: both are
     * Floats, two finite doubles within 2 ULP equal too (S15, as the scalar equality's leniency). Null where a kind has no
     * spelling (the caller declines by name). One statement: the pairs read once, numbered in their own select.
     */
    /** {@code words}: each element written as they write it, and the failure theirs (lite's naming the first failing
     *  element by its zero-based index), or the frame's. */
    public static @com.legend.base.Nullable SqlQuery perElement(SqlQuery pairs, Type expectedKind, Type actualKind,
            boolean neverEqual, boolean decimalByValue, VerdictWords words, Styled styled) {
        SqlQuery numbered = SideOrdinal.withOrdinal(pairs);
        OutputCol valueOut = pairs.outputs().get(0);
        SqlExpr value = SqlExpr.Column.of("__s", numbered.outputs(), valueOut.name());
        SqlExpr first = SqlExpr.StructGet.of(value, "first");
        SqlExpr second = SqlExpr.StructGet.of(value, "second");
        SqlExpr ce = LiteralSpelling.leaf(first, expectedKind);
        SqlExpr ca = LiteralSpelling.leaf(second, actualKind);
        if (ce == null || ca == null) {
            return null;
        }
        if (decimalByValue) {
            ce = decimalValue(ce);
            ca = decimalValue(ca);
        }
        SqlExpr ok;
        if (neverEqual) {
            ok = new SqlExpr.BoolLit(false);
        } else {
            ok = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, asText(ce), asText(ca));
        }
        OutputCol okOut = new OutputCol("__ok", SqlType.Scalar.BOOLEAN, false);
        OutputCol reOut = new OutputCol("__re", SqlType.Scalar.VARCHAR, true);
        OutputCol raOut = new OutputCol("__ra", SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlQuery rows = new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.COALESCE, ok, new SqlExpr.BoolLit(false)), "__ok", okOut),
                new SqlSelect.Projection(words.elementText(expectedKind, first, styled), "__re", reOut),
                new SqlSelect.Projection(words.elementText(actualKind, second, styled), "__ra", raOut),
                new SqlSelect.Projection(SqlExpr.Column.of("__s", numbered.outputs(), SideOrdinal.ORD), RN, rnOut)),
                false, new SqlSource.Subselect(numbered, "__s", null), null, List.of(), null, null, List.of(), null,
                null, List.of(okOut, reOut, raOut, rnOut));
        List<OutputCol> pe = List.of(okOut, reOut, raOut, rnOut);
        SqlExpr failingAt = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Column.of("__pe", pe, "__ok")),
                SqlExpr.Column.of("__pe", pe, RN))), null, SqlExpr.Reason.JUDGE);
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        OutputCol fe = new OutputCol("__fe", SqlType.Scalar.VARCHAR, true);
        OutputCol fa = new OutputCol("__fa", SqlType.Scalar.VARCHAR, true);
        OutputCol fi = new OutputCol("__fi", SqlType.Scalar.BIGINT, true);
        OneRow b = new OneRow("b", new SqlSelect(List.of(
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(failingAt), false, List.of()),
                        "__fi", fi),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(failingAt), false, List.of()),
                        "__n", n),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.ARG_MIN,
                        List.of(SqlExpr.Column.of("__pe", pe, "__re"), failingAt), false, List.of()), "__fe", fe),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.ARG_MIN,
                        List.of(SqlExpr.Column.of("__pe", pe, "__ra"), failingAt), false, List.of()), "__fa", fa)),
                false, new SqlSource.Table("__pe", "__pe", pe, false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null,
                List.of(fi, n, fe, fa)),
                List.of(fi, n, fe, fa));
        // the first failing element's zero-based index: its 1-based ordinal less one
        SqlExpr index = new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.MINUS, b.col("__fi"), new SqlExpr.IntLit(1)),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        SqlExpr feText = SqlExpr.Call.of(SqlFn.COALESCE, b.col("__fe"), new SqlExpr.StringLit("[]"));
        SqlExpr faText = SqlExpr.Call.of(SqlFn.COALESCE, b.col("__fa"), new SqlExpr.StringLit("[]"));
        SqlExpr message = words.elementFailure(feText, faText, kindText(expectedKind), kindText(actualKind), index);
        return predicateOver(List.of(new SqlWith.Cte("__pe", rows)), List.of(b),
                SqlExpr.Call.of(SqlFn.EQUAL, b.col("__n"), new SqlExpr.IntLit(0)), b.col("__fe"), b.col("__fa"),
                new SqlExpr.NullLit(), message);
    }

    /** {@code assertEqWithinTolerance(e, a, tol)}: {@code |e − a| ≤ tol} over the three sides' canon texts, computed
     *  in {@code arithmetic} -- the sides' own kind (the user's ruling, 2026-11-03: never more lenient than upstream's
     *  exact {@code abs($e-$a) <= abs($delta)}): BIGINT for Integers, DECIMAL for Decimals, DOUBLE where a side is a
     *  Float (the database's Float). */
    /** {@code words}: the failure's words, each number of the arithmetic's kind; null from them, the frame's. */
    public static SqlQuery tolerance(SqlQuery eScalar, SqlQuery aScalar, SqlQuery tolScalar, SqlType arithmetic,
            VerdictWords words) {
        OneRow eo = rowOf("e", eScalar);
        OneRow ao = rowOf("a", aScalar);
        OneRow to = rowOf("t", tolScalar);
        SqlExpr e = new SqlExpr.Cast(eo.col(C), arithmetic, SqlExpr.Reason.JUDGE);
        SqlExpr a = new SqlExpr.Cast(ao.col(C), arithmetic, SqlExpr.Reason.JUDGE);
        SqlExpr t = new SqlExpr.Cast(to.col(C), arithmetic, SqlExpr.Reason.JUDGE);
        SqlExpr within = SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, e, a)), t);
        return predicateOver(List.of(), List.of(eo, ao, to),
                SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false)),
                new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), new SqlExpr.Cast(a, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                new SqlExpr.NullLit(), words.notWithin(e, t, a, numberKind(arithmetic)));
    }

    /** A number's kind by its arithmetic's SQL kind: a Float, a Decimal, else an Integer. */
    private static Type numberKind(SqlType arithmetic) {
        return arithmetic == SqlType.Scalar.DOUBLE ? Type.Primitive.FLOAT
                : arithmetic instanceof SqlType.Decimal ? Type.Primitive.DECIMAL : Type.Primitive.INTEGER;
    }

    /** The {@code $need->forAll(n | $have->contains($n))} subset idiom:
     * no needed canon is absent from the haves; a failure raises {@code message}, its condition's (null, the frame). */
    public static SqlQuery subset(SqlQuery needRows, SqlQuery haveRows, boolean wantTrue,
            @com.legend.base.Nullable String message) {
        OutputCol one = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlExpr present = new SqlExpr.Exists(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                false, cte("__h"),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__h", C), col("__n", C)),
                List.of(), null, null, List.of(), null, null, List.of(one)));
        SqlExpr missing = new SqlExpr.Exists(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", one)),
                false, cte("__n"), SqlExpr.Call.of(SqlFn.NOT, present),
                List.of(), null, null, List.of(), null, null, List.of(one)));
        OutputCol miss = new OutputCol("__missing", SqlType.Scalar.BIGINT, false);
        OneRow m = new OneRow("m", new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "__missing", miss)),
                false, cte("__n"), SqlExpr.Call.of(SqlFn.NOT, present),
                List.of(), null, null, List.of(), null, null, List.of(miss)), List.of(miss));
        OneRow t = textOf("t", "__sn");
        SqlExpr anyMissing = SqlExpr.Call.of(SqlFn.GREATER, m.col("__missing"), new SqlExpr.IntLit(0));
        return predicateOver(List.of(new SqlWith.Cte("__n", needRows), new SqlWith.Cte("__h", haveRows),
                        new SqlWith.Cte("__sn", sideFacts("__n", true, true))),
                List.of(m, t), wantTrue ? SqlExpr.Call.of(SqlFn.NOT, anyMissing) : anyMissing,
                new SqlExpr.StringLit(wantTrue ? "subset" : "not a subset"), t.col(F_TEXT), new SqlExpr.NullLit(),
                message);
    }

    /**
     * {@code assertTdsEquivalent}'s TYPED member (item 3's S4, tables): the verdict the database computes over each
     * grid's typed cells, as before -- the same count of cells, and no pair of cells joined by position that is not
     * within: two numbers within {@code |delta|}, two dates within {@code |timeDelta|} seconds, any other two the same,
     * two NULLs alike (the host's {@code TdsCompare.tdsEquivalent}) -- or false at planning where the two grids'
     * columns differ ({@code columnsDiffer}). Where it fails, each grid's rows ride back as a list of row structs in its
     * order, and the delta, for the host's record ({@code AssertKind.TDS_EQUIVALENT}); until then the row's sentence
     * written in SQL. {@code eRows}, {@code aRows}: each grid's relation type, the kind its rows decode by.
     */
    public static @com.legend.base.Nullable TypedRow typedTdsEquivalent(GridSide one, GridSide two, List<Type> kinds,
            List<Type> twoKinds, SqlQuery deltaRows, SqlQuery timeDeltaRows, boolean columnsDiffer, Type eRows,
            Type aRows) {
        if (!carriesCells(one) || !carriesCells(two)) {
            return null;
        }
        SqlQuery e = toleranceCells(one, kinds, GRID_E);
        SqlQuery a = toleranceCells(two, twoKinds, GRID_A);
        SqlExpr delta = new SqlExpr.Cast(scalarOver("__d", col("__d", C), "__one",
                SqlType.Scalar.VARCHAR, 1L), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
        SqlExpr timeDelta = new SqlExpr.Cast(scalarOver("__t", col("__t", C), "__one",
                SqlType.Scalar.VARCHAR, 1L), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
        SqlExpr en = SqlExpr.Column.of("__e", "__n", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr an = SqlExpr.Column.of("__a", "__n", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr es = SqlExpr.Column.of("__e", "__s", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr as = SqlExpr.Column.of("__a", "__s", SqlType.Scalar.DOUBLE, true, OutputCol.Origin.DERIVED);
        SqlExpr within = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, en),
                                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, an)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, en, an)),
                                        SqlExpr.Call.of(SqlFn.ABS, delta))),
                        SqlExpr.Call.of(SqlFn.AND,
                                SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.IS_NOT_NULL, es),
                                        SqlExpr.Call.of(SqlFn.IS_NOT_NULL, as)),
                                SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                                        SqlExpr.Call.of(SqlFn.ABS, SqlExpr.Call.of(SqlFn.MINUS, es, as)),
                                        SqlExpr.Call.of(SqlFn.ABS, timeDelta)))),
                SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, col("__e", C), col("__a", C)));
        // the BAD pairs: cells joined by position that are not within
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlSelect bad = new SqlSelect(List.of(new SqlSelect.Projection(col("__e", RN), RN, rnOut)), false,
                new SqlSource.Join(cte("__e"), cte("__a"), SqlSource.Join.Kind.INNER,
                        SqlExpr.Call.of(SqlFn.EQUAL, col("__e", RN), col("__a", RN))),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.COALESCE, within, new SqlExpr.BoolLit(false))),
                List.of(), null, null, List.of(), null, null, List.of(rnOut));
        SqlExpr holds = columnsDiffer ? new SqlExpr.BoolLit(false) : SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, count("__e"), count("__a")),
                SqlExpr.Call.of(SqlFn.EQUAL, count("__p"), new SqlExpr.IntLit(0)));
        OutputCol hOut = new OutputCol(SAME_H, SqlType.Scalar.BOOLEAN, false);
        SqlSelect same = new SqlSelect(List.of(new SqlSelect.Projection(holds, SAME_H, hOut)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(hOut));
        SqlQuery eList = listed("__ge", rowStructs(one, GRID_E, kinds));
        SqlQuery aList = listed("__ga", rowStructs(two, GRID_A, twoKinds));
        OutputCol dOut = new OutputCol("__dv", SqlType.Scalar.DOUBLE, true);
        SqlSelect deltaValue = new SqlSelect(List.of(new SqlSelect.Projection(delta, "__dv", dOut)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(dOut));
        List<SqlWith.Cte> ctes = List.of(new SqlWith.Cte(GRID_E, one.wrapped()), new SqlWith.Cte(GRID_A, two.wrapped()),
                new SqlWith.Cte("__e", e), new SqlWith.Cte("__a", a), new SqlWith.Cte("__d", deltaRows),
                new SqlWith.Cte("__t", timeDeltaRows), new SqlWith.Cte("__p", bad),
                new SqlWith.Cte("__ge", rowStructs(one, GRID_E, kinds)),
                new SqlWith.Cte("__ga", rowStructs(two, GRID_A, twoKinds)));
        return typedRow(ctes, cross(cross(cross(new SqlSource.Subselect(same, "q", null),
                        new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a", null)),
                        new SqlSource.Subselect(deltaValue, "d", null)),
                SqlExpr.Column.of("q", same.outputs(), SAME_H), List.of(listValue(TYPED_EXPECTED, "e", eList, eRows),
                        listValue(TYPED_ACTUAL, "a", aList, aRows), new Valued("__dv", SqlExpr.Column.of("d",
                                deltaValue.outputs(), "__dv"), SqlType.Scalar.DOUBLE, Type.Primitive.FLOAT, false)));
    }

    private static final String SAME_H = "__holds";

    /** A grid's rows as row structs ({@code __o}, its cells as fields {@code c0, c1, ...}, each typed as its column
     *  declares ({@link #typedCell}); a struct is never NULL, whatever its cells) numbered as its cells are
     *  ({@code __rn}), read through its CTE. */
    private static SqlQuery rowStructs(GridSide grid, String named, List<Type> kinds) {
        return structRows(typedCells(grid, named, kinds, null));
    }

    /** {@code cells}' rows ({@code c0, c1, ..., __rn}) as row structs: {@code __o} and {@code __rn}. */
    private static SqlQuery structRows(SqlQuery cells) {
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
        List<SqlType.Struct.Field> types = new ArrayList<>();
        for (OutputCol o : cells.outputs()) {
            if (!o.name().equals(RN)) {
                fields.add(new SqlExpr.StructLit.Field(o.name(), SqlExpr.Column.of("k", cells.outputs(), o.name()),
                        o.type()));
                types.add(new SqlType.Struct.Field(o.name(), o.type()));
            }
        }
        OutputCol oOut = new OutputCol(O, new SqlType.Struct(types), false);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.StructLit(fields), O, oOut),
                new SqlSelect.Projection(SqlExpr.Column.of("k", cells.outputs(), RN), RN, rnOut)), false,
                new SqlSource.Subselect(cells, "k", null), null, List.of(), null, null, List.of(), null, null,
                List.of(oOut, rnOut));
    }

    /**
     * A grid's rows as typed cells, one column each ({@code c0, c1, ...}), numbered ({@code __rn}) in the grid's order,
     * read through its CTE ({@code named}): each cell as its plan types it, and, where {@code tagged} names
     * a column, its KIND in place of its value ({@code CASE WHEN cell IS NULL THEN NULL ELSE 'Integer' END}): two cells
     * of kinds lite never calls equal (register S127) alike only where both are NULL, the database comparing.
     */
    private static SqlQuery typedCells(GridSide grid, String named, List<Type> kinds,
            @com.legend.base.Nullable List<Boolean> tagged) {
        List<SqlSelect.Projection> data = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size())) : List.of();
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            SqlExpr cell = typedCell(grid, data.get(i));
            SqlType type = java.util.Objects.requireNonNull(data.get(i).out(), "a grid's data column with no type").type();
            if (tagged != null && i < tagged.size() && tagged.get(i)) {
                cell = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, cell),
                        new SqlExpr.NullLit())), new SqlExpr.StringLit(kindText(kinds.get(i))), SqlExpr.Reason.JUDGE);
                type = SqlType.Scalar.VARCHAR;
            }
            OutputCol out = new OutputCol("c" + i, type, true);
            ps.add(new SqlSelect.Projection(cell, out.name(), out));
            outs.add(out);
        }
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(rowNumber(grid.wrapped().outputs()), RN, rnOut));
        outs.add(rnOut);
        return new SqlSelect(ps, false, over(grid, named), null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** A grid's data column's cell as its plan types it: the carrier the host's rows read too (the table slice's
     *  audit, M1, 2026-11-22: a Float held as a DECIMAL cast to a DOUBLE wrote 1.5 where the host writes 1.50). */
    private static SqlExpr typedCell(GridSide grid, SqlSelect.Projection p) {
        return SqlExpr.Column.of("w", grid.wrapped().outputs(),
                java.util.Objects.requireNonNull(p.alias(), "a grid's data column with no name"));
    }

    /**
     * {@code assertEquals} and {@code assertNotEquals} over two GRIDS, TYPED (item 3's S4, tables, 2026-11-22): each
     * grid's rows as typed cells ({@link #typedCells}), compared row by row as the host's {@code TdsCompare.grids}
     * compares them -- in the grid's order where the actual is sorted, else as a multiset of rows ({@code multiset}) --
     * by the one list node ({@link SqlFn#SAME_IN_ORDER}, {@link SqlFn#SAME_IN_ANY_ORDER}); false where the two grids'
     * columns differ in name or number ({@code schemasDiffer}, a fact of the types); a column whose two kinds lite never
     * calls equal compared by its kind tag. Where the verdict fails, each grid's rows ride back as typed row structs
     * for the host's record ({@code AssertKind.EQUALS} over the rows, {@code NOT_EQUALS}). Null where a column has no
     * typed class ({@link #typedClass}), or a date column's cells are no date the database holds.
     */
    public static @com.legend.base.Nullable TypedRow typedGridPair(GridSide e, List<Type> eKinds, GridSide a,
            List<Type> aKinds, boolean multiset, boolean wantEqual, boolean schemasDiffer, Type eRows, Type aRows) {
        if (!typedGrid(e, eKinds) || !typedGrid(a, aKinds)) {
            return null;
        }
        List<Boolean> tagged = new ArrayList<>();
        for (int i = 0; i < eKinds.size(); i++) {
            tagged.add(!schemasDiffer && i < aKinds.size()
                    && !java.util.Objects.equals(typedClass(eKinds.get(i)), typedClass(aKinds.get(i))));
        }
        boolean anyTagged = tagged.contains(true);
        SqlQuery ce = typedCells(e, GRID_E, eKinds, tagged);
        SqlQuery ca = typedCells(a, GRID_A, aKinds, tagged);
        List<SqlWith.Cte> ctes = new ArrayList<>(List.of(new SqlWith.Cte(GRID_E, e.wrapped()),
                new SqlWith.Cte(GRID_A, a.wrapped()), new SqlWith.Cte("__ce", ce), new SqlWith.Cte("__ca", ca),
                new SqlWith.Cte("__ke", structRows(rowsOf("__ce", ce))),
                new SqlWith.Cte("__ka", structRows(rowsOf("__ca", ca)))));
        SqlQuery eCmp = listed("__ke", structRows(rowsOf("__ce", ce)));
        SqlQuery aCmp = listed("__ka", structRows(rowsOf("__ca", ca)));
        SqlQuery eList = eCmp;
        SqlQuery aList = aCmp;
        if (anyTagged) {
            // the record's rows are the cells themselves, not their tags
            ctes.add(new SqlWith.Cte("__ge", rowStructs(e, GRID_E, eKinds)));
            ctes.add(new SqlWith.Cte("__ga", rowStructs(a, GRID_A, aKinds)));
            eList = listed("__ge", rowStructs(e, GRID_E, eKinds));
            aList = listed("__ga", rowStructs(a, GRID_A, aKinds));
        }
        SqlExpr same = schemasDiffer ? new SqlExpr.BoolLit(false) : SqlExpr.Call.of(multiset
                        ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                SqlExpr.Column.of("ke", eCmp.outputs(), LISTED), SqlExpr.Column.of("ka", aCmp.outputs(), LISTED),
                new SqlExpr.ScalarSubquery(rowsOf("__ce", ce)), new SqlExpr.ScalarSubquery(rowsOf("__ca", ca)));
        SqlSource from = cross(new SqlSource.Subselect(eCmp, "ke", null), new SqlSource.Subselect(aCmp, "ka", null));
        if (anyTagged) {
            from = cross(cross(from, new SqlSource.Subselect(eList, "e", null)), new SqlSource.Subselect(aList, "a",
                    null));
        }
        return typedRow(ctes, from, wantEqual ? same : SqlExpr.Call.of(SqlFn.NOT, same), List.of(
                listValue(TYPED_EXPECTED, anyTagged ? "e" : "ke", eList, eRows),
                listValue(TYPED_ACTUAL, anyTagged ? "a" : "ka", aList, aRows)));
    }

    /** Whether a grid's plan names and types each of its data columns, the cells a typed member reads. */
    private static boolean carriesCells(GridSide grid) {
        return grid.wrapped() instanceof SqlSelect ws && ws.projections().size() >= grid.width()
                && ws.projections().subList(0, grid.width()).stream().allMatch(p -> p.alias() != null
                        && p.out() != null);
    }

    /** Whether each of a grid's columns has a typed class the cells carry: a date column's cells a DATE or TIMESTAMP,
     *  never a text, and no column empty-is-NULL (toCSV's equivalence stays the seven-column row). */
    private static boolean typedGrid(GridSide grid, List<Type> kinds) {
        if (!carriesCells(grid) || kinds.size() != grid.width()) {
            return false;
        }
        List<SqlSelect.Projection> data = ((SqlSelect) grid.wrapped()).projections().subList(0, grid.width());
        for (int i = 0; i < data.size(); i++) {
            Type k = typedClass(kinds.get(i));
            SqlSelect.Projection p = data.get(i);
            if (k == null || grid.emptyIsNullAt(i)) {
                return false;
            }
            SqlType t = java.util.Objects.requireNonNull(p.out(), "a grid's data column with no type").type();
            if (k == Type.Primitive.STRICT_DATE && t != SqlType.Scalar.DATE
                    || k == Type.Primitive.DATE_TIME && t != SqlType.Scalar.TIMESTAMP) {
                return false;
            }
        }
        return true;
    }

    /** A grid's cells row-major with a NUMERIC value ({@code __n}, any
     * numeric kind) and a TEMPORAL value in epoch seconds ({@code __s}). */
    private static SqlQuery toleranceCells(GridSide grid, List<Type> kinds, String named) {
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol nOut = new OutputCol("__n", SqlType.Scalar.DOUBLE, true);
        OutputCol sOut = new OutputCol("__s", SqlType.Scalar.DOUBLE, true);
        List<SqlSelect.Projection> values = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size()))
                : List.of();
        List<SqlExpr> cells = new ArrayList<>();
        List<SqlExpr> ns = new ArrayList<>();
        List<SqlExpr> secs = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(gridCell(grid, i));
            Type k = i < kinds.size() ? kinds.get(i) : null;
            String alias = i < values.size() ? values.get(i).alias() : null;
            boolean numeric = k == Type.Primitive.INTEGER || k == Type.Primitive.FLOAT
                    || k == Type.Primitive.DECIMAL || k == Type.Primitive.NUMBER
                    || k instanceof Type.PrecisionDecimal;
            boolean temporal = k == Type.Primitive.DATE_TIME || k == Type.Primitive.STRICT_DATE
                    || k == Type.Primitive.DATE;
            // a cell the side carries as a temporal value is read as one (a literal grid's timestamp: a text cast of
            // it is no timestamp's on Trino), any other as the text it arrives as
            SqlExpr raw = alias == null ? null
                    : temporal && carriedTemporal(grid.wrapped().outputs(), alias)
                            ? SqlExpr.Column.of("w", grid.wrapped().outputs(), alias)
                            : SqlExpr.Column.of("w", alias, SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED);
            ns.add(raw != null && numeric ? new SqlExpr.Cast(raw, SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                    : new SqlExpr.NullLit());
            // a temporal cell casts to a timestamp for its epoch: a computed
            // one arrives DECODED as text (the fetch conformance: nine-digit
            // temporals)
            secs.add(raw != null && temporal
                    ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.EPOCH_SECONDS,
                            new SqlExpr.Cast(raw, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE)), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                    : new SqlExpr.NullLit());
        }
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(pick(cells), C, cOut),
                        new SqlSelect.Projection(position(grid.width()), RN, rnOut),
                        new SqlSelect.Projection(pick(ns), "__n", nOut),
                        new SqlSelect.Projection(pick(secs), "__s", sOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut, rnOut, nOut, sOut));
    }

    /** Whether {@code name} among {@code outs} is a date or a timestamp value (not its text). */
    private static boolean carriedTemporal(List<OutputCol> outs, String name) {
        return outs.stream().anyMatch(o -> o.name().equals(name)
                && (o.type() == SqlType.Scalar.TIMESTAMP || o.type() == SqlType.Scalar.TIMESTAMPTZ
                        || o.type() == SqlType.Scalar.DATE));
    }

    /** The JSON verdict (bucket 3): the document the database built (its
     * objects' keys sorted by {@link com.legend.sql.JsonKeyOrder}) against
     * the golden's canonical text (compact, keys sorted, the engine's
     * root {@code [x] ≡ x} applied at compile time) — byte-equal is the
     * verdict; a differing pair is unjudged with its evidence. */
    public static SqlQuery jsonText(SqlQuery eRows, SqlQuery aRows) {
        OneRow eo = firstOf("e", "__e");
        OneRow ao = firstOf("a", "__a");
        SqlExpr equal = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, eo.col(C), ao.col(C));
        SqlExpr unjudged = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.NOT, equal),
                new SqlExpr.StringLit("json: not byte-equal (keys sorted, compact)"))),
                null, SqlExpr.Reason.JUDGE);
        return withHint(predicateOver(List.of(new SqlWith.Cte("__e", eRows), new SqlWith.Cte("__a", aRows)),
                List.of(eo, ao), equal, eo.col(C), ao.col(C), unjudged), JSON_HINT);
    }

    /** {@code assertJsonStringsEqual} over a document whose root array has
     * no defined order: the golden's element texts against the document's
     * root elements, as a multiset. */
    public static SqlQuery jsonRootMultiset(SqlQuery eElementRows, SqlQuery aDocRows) {
        OutputCol vOut = new OutputCol("value", SqlType.Scalar.JSON, true);
        SqlExpr doc = new SqlExpr.Cast(SqlExpr.Column.of("d", aDocRows.outputs(), C), SqlType.Scalar.JSON, SqlExpr.Reason.JUDGE);
        SqlSelect elements = CollectionRelations.rows(SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, doc),
                "value", vOut, List.of(vOut), new SqlSource.Subselect(aDocRows, "d", null));
        SqlQuery aRows = rowsOf(new SqlExpr.Cast(SqlExpr.Column.of("w", List.of(vOut), "value"),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), elements);
        return withHint(statement(eElementRows, aRows, true, true, true, List.of()), JSON_HINT);
    }

    /** One verdict row from a predicate: {@code __verdict} never NULL, the
     * two evidence texts, no unjudged, no leniency. */
    // ── the ONE-LINE families: operands as ONE-ROW relations, cross-joined
    // ONCE into the __p facts row; the verdict row reads its columns — every
    // operand computed once, no scalar subquery (lean ladder rung 4) ───────
    private record OneRow(String alias, SqlQuery query, List<OutputCol> outs) {
        SqlExpr col(String name) {
            return SqlExpr.Column.of("p", pOuts(), pName(alias, name));
        }
        private List<OutputCol> pOuts() {
            List<OutputCol> out = new ArrayList<>();
            for (OutputCol o : outs) {
                out.add(new OutputCol(pName(alias, o.name()), o.type(), o.nullable()));
            }
            return out;
        }
        /** {@code __<alias>_<col>}: the facts row's column for an operand's column. */
        static String pName(String alias, String col) {
            return "__" + alias + "_" + col.replaceFirst("^_+", "");
        }
    }

    private static final String P = "__p";

    /** A TEXT assert's verdict row whose OUR text is a side the database computes (2026-11-04, the core program): the
     *  golden a constant, ours the side's one-row relation ({@code oursRow}: {@link #scalarRow}'s {@code value}), string
     *  equality; the ACTUAL column carries our text back to the appeal of a failed row. */
    /** {@code plan}'s first output as a one-row relation {@code (value)} (a one-row seed LEFT JOINed to the plan, so an
     *  empty plan is one NULL row, as {@link #scalarRow}'s): a rendered text's value, no canon (2026-11-04). */
    public static SqlQuery firstValueRow(SqlQuery plan) {
        OutputCol first = plan.outputs().get(0);
        OutputCol vOut = new OutputCol("value", first.type(), true);
        OutputCol seedOut = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSource seed = new SqlSource.Subselect(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", seedOut)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                List.of(seedOut)), "__one", null);
        SqlSource from = new SqlSource.Join(seed, new SqlSource.Subselect(plan, "w", null), SqlSource.Join.Kind.LEFT,
                new SqlExpr.BoolLit(true));
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("w", plan.outputs(), first.name()),
                "value", vOut)), false, from, null, List.of(), null, null, List.of(), null, null, List.of(vOut));
    }

    private static SqlQuery predicateOver(List<SqlWith.Cte> ctes, List<OneRow> ops,
            SqlExpr verdict, SqlExpr expected, SqlExpr actual, SqlExpr unjudged) {
        return predicateOver(ctes, ops, verdict, expected, actual, unjudged, (SqlExpr) null);
    }

    /** {@code message}: the failure's fixed text (an assert's own message), carried as the row's message column, which
     *  {@link #selfContained} keeps in place of the two canons' frame; null, the frame. */
    private static SqlQuery predicateOver(List<SqlWith.Cte> ctes, List<OneRow> ops,
            SqlExpr verdict, SqlExpr expected, SqlExpr actual, SqlExpr unjudged,
            @com.legend.base.Nullable String message) {
        return predicateOver(ctes, ops, verdict, expected, actual, unjudged,
                message == null ? null : new SqlExpr.StringLit(message));
    }

    private static SqlQuery predicateOver(List<SqlWith.Cte> ctes, List<OneRow> ops,
            SqlExpr verdict, SqlExpr expected, SqlExpr actual, SqlExpr unjudged,
            @com.legend.base.Nullable SqlExpr message) {
        List<SqlSelect.Projection> pps = new ArrayList<>();
        List<OutputCol> pOuts = new ArrayList<>();
        SqlSource from = null;
        for (OneRow op : ops) {
            for (OutputCol o : op.outs()) {
                OutputCol po = new OutputCol(OneRow.pName(op.alias(), o.name()), o.type(), o.nullable());
                pps.add(new SqlSelect.Projection(SqlExpr.Column.of(op.alias(), op.outs(), o.name()),
                        po.name(), po));
                pOuts.add(po);
            }
            SqlSource src = new SqlSource.Subselect(op.query(), op.alias(), null);
            from = from == null ? src : new SqlSource.Join(from, src, SqlSource.Join.Kind.CROSS, null);
        }
        List<SqlWith.Cte> all = new ArrayList<>(ctes);
        all.add(new SqlWith.Cte(P, new SqlSelect(pps, false, java.util.Objects.requireNonNull(from),
                null, List.of(), null, null, List.of(), null, null, pOuts)));
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.COALESCE, verdict, new SqlExpr.BoolLit(false)), VERDICT,
                        new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(expected, EXPECTED,
                        new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(actual, ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(unjudged, UNJUDGED,
                        new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(new SqlExpr.BoolLit(false), LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)));
        if (message != null) {
            ps = new ArrayList<>(ps);
            ps.add(new SqlSelect.Projection(message, MESSAGE,
                    new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, true)));
        }
        SqlSelect body = new SqlSelect(ps, false, new SqlSource.Table(P, "p", pOuts, false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null,
                List.of(), null, null, List.of(), null, null, List.of());
        return new SqlWith(all, body);
    }

    private static OneRow countOf(String alias, String rowsCte) {
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col(rowsCte, RN)), false, List.of()), "__n", n)),
                false, cte(rowsCte), null, List.of(), null, null, List.of(), null, null, List.of(n)), List.of(n));
    }

    private static OneRow constantOf(String alias, SqlExpr value, String name, SqlType type) {
        OutputCol o = new OutputCol(name, type, false);
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(value, name, o)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(o)),
                List.of(o));
    }

    private static OneRow textOf(String alias, String factsCte) {
        return new OneRow(alias, new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of(factsCte, factOutputs(), F_TEXT), F_TEXT, factOutputs().get(0))),
                false, new SqlSource.Table(factsCte, factsCte, factOutputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null,
                List.of(), null, null, List.of(factOutputs().get(0))), List.of(factOutputs().get(0)));
    }

    /** {@link #textOf} with the side's count beside its text ({@code F_N}): a message that brackets one element. */
    private static OneRow textAndCountOf(String alias, String factsCte) {
        List<OutputCol> outs = List.of(factOutputs().get(0), factOutputs().get(3));
        return new OneRow(alias, new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of(factsCte, factOutputs(), F_TEXT), F_TEXT, outs.get(0)),
                new SqlSelect.Projection(SqlExpr.Column.of(factsCte, factOutputs(), F_N), F_N, outs.get(1))),
                false, new SqlSource.Table(factsCte, factsCte, factOutputs(), false, java.util.Map.of(),
                        com.legend.sql.NameQuoting.BARE), null, List.of(), null, null, List.of(), null, null, outs),
                outs);
    }

    /** The first row of a rows CTE as a one-row relation ({@code __c}). */
    private static OneRow firstOf(String alias, String rowsCte) {
        OutputCol c = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        return new OneRow(alias, new SqlSelect(List.of(new SqlSelect.Projection(col(rowsCte, C), C, c)),
                false, cte(rowsCte), null, List.of(), null, null,
                List.of(new SqlSelect.SortKey(col(rowsCte, RN), true, null, null)), 1L, null, List.of(c)),
                List.of(c));
    }

    private static OneRow rowOf(String alias, SqlQuery oneRow) {
        return new OneRow(alias, oneRow, oneRow.outputs());
    }

    /** A side DECLARED exactly one as a one-row relation {@code (__c, value)}:
     * its canon text and value straight over its plan (a one-row seed LEFT
     * JOINed to the plan, so an empty plan is one NULL row), or over the
     * trimmed wrap when the wrap is not a plain projection. */
    public static SqlQuery scalarRow(SqlQuery wrapped, String canonColumn) {
        SqlSelect ws = (SqlSelect) wrapped;
        SqlSelect.Projection value = ws.projections().get(0);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol vOut = new OutputCol("value", value.out() != null ? value.out().type() : SqlType.Scalar.VARCHAR, true);
        if (plainProjection(ws)) {
            SqlExpr canon = asText(java.util.Objects.requireNonNull(projectionOf(ws, canonColumn), "canon column").expr());
            OutputCol seedOut = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
            SqlSource seed = new SqlSource.Subselect(new SqlSelect(
                    List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", seedOut)),
                    false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                    List.of(seedOut)), "__one", null);
            SqlSource from = new SqlSource.Join(seed, ws.from(), SqlSource.Join.Kind.LEFT, new SqlExpr.BoolLit(true));
            return new SqlSelect(List.of(new SqlSelect.Projection(canon, C, cOut),
                    new SqlSelect.Projection(value.expr(), "value", vOut)),
                    false, from, null, List.of(), null, null, List.of(), null, null, List.of(cOut, vOut));
        }
        SqlSelect.Projection chosen = java.util.Objects.requireNonNull(projectionOf(ws, canonColumn), "canon column");
        List<SqlSelect.Projection> kept = chosen == value ? List.of(value) : List.of(value, chosen);
        List<OutputCol> keptOuts = kept.stream().map(SqlSelect.Projection::out).filter(java.util.Objects::nonNull).toList();
        SqlSelect trimmed = new SqlSelect(kept, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                ws.having(), ws.qualify(), List.of(), ws.limit(), ws.offset(), keptOuts);
        return new SqlSelect(List.of(
                new SqlSelect.Projection(asText(SqlExpr.Column.of("w", keptOuts, canonColumn)), C, cOut),
                new SqlSelect.Projection(SqlExpr.Column.of("w", keptOuts, java.util.Objects.requireNonNull(value.alias())), "value", vOut)),
                false, new SqlSource.Subselect(trimmed, "w", null), null, List.of(), null, null, List.of(), null, null,
                List.of(cOut, vOut));
    }

    /** A side's rows for the predicate forms: a grid's row canons, or a
     * scalar / collection side's canons (NULL values dropped). */
    public static SqlQuery sideRows(SqlQuery wrapped, boolean grid, String canonColumn, boolean many) {
        return grid ? gridRowCanons(wrapped)
                : canonRows(new Side(wrapped, canonColumn, many, false, false));
    }

    /** A scalar / collection side's canon rows, each element also written as {@code message} writes it ({@link
     *  Message}): the rows a membership or emptiness verdict's failure message reads. */
    public static SqlQuery writtenRows(SqlQuery wrapped, String canonColumn, boolean many, Message message) {
        return canonRows(new Side(wrapped, canonColumn, many, false, false, message, false));
    }

    /** A side's rows for COUNTING only (size / emptiness): no canon needed —
     * a collection of instances the canon declines still has a row count;
     * NULL values are dropped as everywhere (pure has no null value). */
    public static SqlQuery countRows(SqlQuery plan) {
        SqlExpr where = null;
        if (plan instanceof SqlSelect vs && !vs.projections().isEmpty()
                && vs.projections().get(0).alias() != null) {
            where = SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                    SqlExpr.Column.of("w", vs.projections().get(0).alias(),
                            SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED));
        }
        return rowsOf(new SqlExpr.NullLit(), new SqlExpr.NullLit(), plan, where);
    }

    /** A GRID's rows for counting: every row counts (a grid row is never
     * dropped), over the plan BEFORE the canon wrap — no cell is spelled. */
    public static SqlQuery gridCountRows(SqlQuery wrapped) {
        return rowsOf(new SqlExpr.NullLit(), new SqlExpr.NullLit(),
                CanonicalRenderSql.unwrapTdsCanon(wrapped), null);
    }

    /** Two grids: row canons against row canons. */
    public static SqlQuery gridPair(SqlQuery e, SqlQuery a, boolean multiset) {
        return statement(gridRowCanons(e), gridRowCanons(a), true, true, multiset, List.of());
    }

    /** Two grids of ONE schema: row canons against row canons, a Float compared exactly (A4 of the compatibility
     * program: the database judge plans for the product alone), each side's empty-is-NULL columns read as the
     * sentinel, each side written as lite's words read it, its rows as bracketed cells, {@code [[30], [41]]}. */
    public static SqlQuery gridPair(GridSide e, GridSide a, boolean multiset, VerdictWords words) {
        return writtenStatement(gridRowCanons(e, GRID_E, RowWriting.BRACKETED),
                gridRowCanons(a, GRID_A, RowWriting.BRACKETED), multiset, List.of(),
                List.of(new SqlWith.Cte(GRID_E, e.wrapped()), new SqlWith.Cte(GRID_A, a.wrapped())), true);
    }

    private static SqlQuery statement(SqlQuery eRows, SqlQuery aRows,
            boolean eMany, boolean aMany, boolean byCanonText,
            List<SqlExpr.Case.When> moreUnjudged) {
        return statement(eRows, aRows, eMany, aMany, byCanonText, moreUnjudged, List.of());
    }

    /** The statement over two ROW SOURCES (each {@code (__c, __rn[, __v])}). */
    private static SqlQuery statement(SqlQuery eRows, SqlQuery aRows,
            boolean eMany, boolean aMany, boolean byCanonText,
            List<SqlExpr.Case.When> moreUnjudged, List<SqlWith.Cte> extraCtes) {
        return statementOf(new Folded("__e", eRows, sideFacts("__e", eMany, byCanonText)),
                new Folded("__a", aRows, sideFacts("__a", aMany, byCanonText)),
                moreUnjudged, extraCtes);
    }

    /** {@link #statement} over two rows relations that carry their WRITTEN column (lite's own rules): the facts read
     *  it, and the frame's sentence writes each side from it -- always as a list ({@code alwaysList}: rows of rows,
     *  a multiset's elements), else bare where both sides hold one. */
    private static SqlQuery writtenStatement(SqlQuery eRows, SqlQuery aRows, boolean byCanonText,
            List<SqlExpr.Case.When> moreUnjudged, List<SqlWith.Cte> extraCtes, boolean alwaysList) {
        // lite's own text, written from each canon's mark (liteOfCanon): the product's, so the product's style
        Message written = Message.fromCanon(false, null);
        return withWrittenSides(statementOf(new Folded("__e", eRows, sideFacts("__e", true, byCanonText, written)),
                new Folded("__a", aRows, sideFacts("__a", true, byCanonText, written)),
                moreUnjudged, extraCtes), alwaysList, byCanonText);
    }

    private static SqlQuery statementOf(Folded e, Folded a,
            List<SqlExpr.Case.When> moreUnjudged, List<SqlWith.Cte> extraCtes) {
        SqlQuery eRows = e.rows();
        SqlQuery aRows = a.rows();
        // THE GENERAL SHAPE (lean ladder, 2026-09-20): each side is a rows
        // relation (__c, __rn[, __v]) folded to ONE aggregate row (count, first,
        // joined, null count, tree count); the verdict row reads the two one-row
        // CTEs. No scalar subqueries, every side spelled once. The 2-ULP
        // leniency block exists only when a cell side carries a Float value —
        // a compile-time fact of the sides, not a special case — and folds to
        // one aggregate over the positional join.
        List<SqlWith.Cte> ctes = new ArrayList<>(extraCtes);
        if (eRows != null) {
            ctes.add(new SqlWith.Cte("__e", eRows));
        }
        if (aRows != null) {
            ctes.add(new SqlWith.Cte("__a", aRows));
        }
        ctes.add(new SqlWith.Cte("__se", e.facts()));
        ctes.add(new SqlWith.Cte("__sa", a.facts()));
        // (the 2-ULP Float leniency's positional pair facts left with A4: a Float compares exactly)
        // the actual side's facts joined to the expected's, which a database builds first: the expected side read before
        // the actual, as the host's side batch reads them, so where both raise the two judges raise one error (G2, G10:
        // PCT's testAdjustBy*BigNumber on DuckDB, whose test sessions run single-threaded, 2026-11-19; the compatibility
        // owner's option 2 -- the literal stays in the database). The verdict row reads each fact by its alias.
        SqlSource from = new SqlSource.Join(facts("__sa", "a"), facts("__se", "e"),
                SqlSource.Join.Kind.CROSS, null);
        SqlExpr ex = factCol("e", F_TEXT);
        SqlExpr ax = factCol("a", F_TEXT);
        SqlExpr exact = SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, ex, ax);
        SqlExpr verdict = exact;
        // the verdict row keeps its lenient column, false: no tolerance decides a database verdict (A4)
        SqlExpr lenientOnly = new SqlExpr.BoolLit(false);
        List<SqlExpr.Case.When> whens = new ArrayList<>(moreUnjudged);
        whens.add(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("e", F_NULLS), new SqlExpr.IntLit(0)),
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("a", F_NULLS), new SqlExpr.IntLit(0))),
                new SqlExpr.StringLit("null-canon-cell")));
        whens.add(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("e", F_TREES), new SqlExpr.IntLit(0)),
                        SqlExpr.Call.of(SqlFn.GREATER, factCol("a", F_TREES), new SqlExpr.IntLit(0))),
                new SqlExpr.StringLit("unclaimable tree cell")));
        SqlExpr unjudged = new SqlExpr.Case(whens, null, SqlExpr.Reason.JUDGE);
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(verdict, VERDICT,
                        new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(ex, EXPECTED,
                        new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(ax, ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(unjudged, UNJUDGED,
                        new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(lenientOnly, LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)));
        SqlSelect body = new SqlSelect(ps, false, from, null,
                List.of(), null, null, List.of(), null, null, List.of());
        return new SqlWith(ctes, body);
    }

    // ── the side FACTS row: one aggregate over a rows CTE ──────────────────
    private static final String F_TEXT = "__text";
    private static final String F_NULLS = "__nulls";
    private static final String F_TREES = "__trees";
    private static final String F_N = "__n";

    /** A side's one element as the failure message writes it, and its elements' bracketed list ({@link Message}). */
    private static final String F_ONE_W = "__onew";
    private static final String F_LIST_W = "__listw";
    /** The verdict row's two sides as the message writes them, read by {@link #selfContained} and dropped there. */
    private static final String EXPECTED_WRITTEN = "__expectedw";
    private static final String ACTUAL_WRITTEN = "__actualw";

    private static List<OutputCol> factOutputs() {
        return List.of(new OutputCol(F_TEXT, SqlType.Scalar.VARCHAR, true),
                new OutputCol(F_NULLS, SqlType.Scalar.BIGINT, false),
                new OutputCol(F_TREES, SqlType.Scalar.BIGINT, false),
                new OutputCol(F_N, SqlType.Scalar.BIGINT, false));
    }

    /** {@link #factOutputs} with the side's written columns ({@link Message}). */
    private static List<OutputCol> writtenFactOutputs() {
        List<OutputCol> outs = new ArrayList<>(factOutputs());
        outs.add(new OutputCol(F_ONE_W, SqlType.Scalar.VARCHAR, true));
        outs.add(new OutputCol(F_LIST_W, SqlType.Scalar.VARCHAR, true));
        return List.copyOf(outs);
    }

    private static SqlSource facts(String cteName, String alias) {
        return new SqlSource.Table(cteName, alias, factOutputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    private static SqlExpr factCol(String alias, String col) {
        return SqlExpr.Column.of(alias, factOutputs(), col);
    }

    /** {@code SELECT <framed text>, nulls, trees FROM (SELECT COUNT, first,
     * STRING_AGG, null count, tree count FROM <rows>) AS s} — the side's
     * verdict facts, one row, the rows scanned once. */
    private static SqlQuery sideFacts(String rowsCte, boolean many, boolean byCanonText) {
        return sideFacts(rowsCte, many, byCanonText, null);
    }

    /** {@code message}: the side's elements also WRITTEN as the failure message writes them ({@link Message}), two
     *  more columns of the facts row -- the one element's text, and the elements' bracketed list in the side's own
     *  order. */
    private static SqlQuery sideFacts(String rowsCte, boolean many, boolean byCanonText,
            @com.legend.base.Nullable Message message) {
        OutputCol nOut = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        OutputCol oneOut = new OutputCol("__one", SqlType.Scalar.VARCHAR, true);
        OutputCol joinedOut = new OutputCol("__joined", SqlType.Scalar.VARCHAR, true);
        OutputCol nullsOut = new OutputCol(F_NULLS, SqlType.Scalar.BIGINT, false);
        OutputCol treesOut = new OutputCol(F_TREES, SqlType.Scalar.BIGINT, false);
        OutputCol oneWOut = new OutputCol(F_ONE_W, SqlType.Scalar.VARCHAR, true);
        OutputCol joinedWOut = new OutputCol("__joinedw", SqlType.Scalar.VARCHAR, true);
        List<OutputCol> aggOuts = message == null ? List.of(nOut, oneOut, joinedOut, nullsOut, treesOut)
                : List.of(nOut, oneOut, joinedOut, nullsOut, treesOut, oneWOut, joinedWOut);
        SqlExpr c = col(rowsCte, C);
        SqlExpr rn = col(rowsCte, RN);
        SqlExpr key = byCanonText ? c : rn;
        SqlExpr one = new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.EQUAL, rn, new SqlExpr.IntLit(1)), c)), null, SqlExpr.Reason.JUDGE)),
                false, List.of());
        List<SqlSelect.Projection> aggs = new ArrayList<>(List.of(
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(rn), false, List.of()), "__n", nOut),
                new SqlSelect.Projection(one, "__one", oneOut),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(c, new SqlExpr.StringLit(", ")), false,
                        List.of(new SqlSelect.SortKey(key, true, null, null))), "__joined", joinedOut),
                new SqlSelect.Projection(countWhere(SqlExpr.Call.of(SqlFn.IS_NULL, c)), F_NULLS, nullsOut),
                new SqlSelect.Projection(countWhere(SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.STRPOS, c, new SqlExpr.StringLit(CanonicalRenderSql.TREE_MARKER)),
                        new SqlExpr.IntLit(0))), F_TREES, treesOut)));
        if (message != null) {
            SqlExpr w = col(rowsCte, W);
            aggs.add(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(new SqlExpr.Case(
                    List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, rn, new SqlExpr.IntLit(1)), w)), null, SqlExpr.Reason.JUDGE)),
                    false, List.of()), F_ONE_W, oneWOut));
            aggs.add(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                    List.of(w, new SqlExpr.StringLit(", ")), false,
                    // lite's sentence lists a side in its own order (the orchestrator's ruling, 2026-11-17)
                    List.of(new SqlSelect.SortKey(rn, true,
                            null, null))),
                    "__joinedw", joinedWOut));
        }
        SqlSelect agg = new SqlSelect(aggs,
                false, cte(rowsCte), null, List.of(), null, null, List.of(), null, null, aggOuts);
        SqlExpr n = SqlExpr.Column.of("s", aggOuts, "__n");
        SqlExpr first = SqlExpr.Column.of("s", aggOuts, "__one");
        SqlExpr joined = SqlExpr.Column.of("s", aggOuts, "__joined");
        SqlExpr empty = new SqlExpr.StringLit("[]");
        SqlExpr text = many
                ? new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, n, new SqlExpr.IntLit(0)), empty),
                        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, n, new SqlExpr.IntLit(1)), first)),
                        SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["), joined, new SqlExpr.StringLit("]")), SqlExpr.Reason.JUDGE)
                : SqlExpr.Call.of(SqlFn.COALESCE, first, empty);
        List<OutputCol> outs = message == null ? factOutputs() : writtenFactOutputs();
        List<SqlSelect.Projection> ps = new ArrayList<>(List.of(
                new SqlSelect.Projection(text, F_TEXT, outs.get(0)),
                new SqlSelect.Projection(SqlExpr.Column.of("s", aggOuts, F_NULLS), F_NULLS, outs.get(1)),
                new SqlSelect.Projection(SqlExpr.Column.of("s", aggOuts, F_TREES), F_TREES, outs.get(2)),
                new SqlSelect.Projection(n, F_N, outs.get(3))));
        if (message != null) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of("s", aggOuts, F_ONE_W), F_ONE_W, outs.get(4)));
            ps.add(new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["),
                    SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.of("s", aggOuts, "__joinedw"),
                            new SqlExpr.StringLit("")), new SqlExpr.StringLit("]")), F_LIST_W, outs.get(5)));
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(agg, "s", null), null, List.of(), null, null,
                List.of(), null, null, outs);
    }

    /** {@code COUNT(CASE WHEN <cond> THEN 1 END)} — portable filtered count. */
    private static SqlExpr countWhere(SqlExpr cond) {
        return new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(cond, new SqlExpr.IntLit(1))), null, SqlExpr.Reason.JUDGE)), false, List.of());
    }

    /** A HOST-CONSTANT side (a compile-time fact the pipeline answered
     * without SQL — a generated seed-data string, a rendered DDL text, a
     * folded literal) bound as a VALUES relation, so the comparison still
     * happens in the database (TWO_DESIGN_LEGS §2.6 blocker 1:
     * database-ADJUDICATED, not database-computed — the ledger says
     * which). Null when a value has no literal spelling here (the caller
     * reports it unjudged by kind). */
    public static @com.legend.base.Nullable SqlQuery constantPlan(List<Object> values) {
        List<List<SqlExpr>> rows = new ArrayList<>();
        SqlType type = SqlType.Scalar.VARCHAR;   // the EMPTY constant's column kind
        for (Object v : values) {
            SqlExpr lit;
            SqlType t;
            switch (v) {
                case String str -> { lit = new SqlExpr.StringLit(str); t = SqlType.Scalar.VARCHAR; }
                case Long l -> { lit = new SqlExpr.IntLit(l); t = SqlType.Scalar.BIGINT; }
                case Integer i -> { lit = new SqlExpr.IntLit(i); t = SqlType.Scalar.BIGINT; }
                case Boolean b -> { lit = new SqlExpr.BoolLit(b); t = SqlType.Scalar.BOOLEAN; }
                case null, default -> { return null; }
            }
            if (!rows.isEmpty() && type != t) {
                return null;   // a mixed constant collection has no one column type
            }
            type = t;
            rows.add(List.of(lit));
        }
        OutputCol out = new OutputCol("value", type, rows.isEmpty());
        SqlSource src = rows.isEmpty()
                ? new SqlSource.Subselect(new SqlSelect(
                        List.of(new SqlSelect.Projection(new SqlExpr.NullLit(), "value", out)),
                        false, new SqlSource.Dual(),
                        new SqlExpr.BoolLit(false), List.of(), null, null, List.of(),
                        null, null, List.of(out)), "k", null)
                : new SqlSource.Values(rows, List.of("value"), "k", List.of(out));
        return new SqlSelect(
                List.of(new SqlSelect.Projection(SqlExpr.Column.of("k", out), "value", out)),
                false, src, null, List.of(), null, null, List.of(), null, null, List.of(out));
    }

    /** A fused statement's column naming the member a row is ({@link #batch}). */
    public static final String MEMBER = "__m";

    /** A fused verdict statement ({@link #batch}): the statement, its columns' schema, and each member's row as the
     *  fused columns that hold it, in the member's own order. */
    public record Batch(SqlQuery query, Type.RelationType schema, List<List<Integer>> layout) {
        public Batch {
            layout = layout.stream().map(List::copyOf).toList();
        }
    }

    /** A fused statement's shared column: the members' values at {@code position} of one SQL type and one Pure type
     *  share it -- a member read to the nanosecond ({@code nanos}: {@link #nanos}) only with another such, so a
     *  nanosecond date-time never shares a column with another assert's year past what a nanosecond timestamp holds
     *  (the precision audit's H3, 2026-11-22: DuckDB unified the column to TIMESTAMP_NS and refused the year). */
    private record Slot(int position, SqlType sql, Type kind, com.legend.compiler.element.type.Multiplicity many,
            boolean nanos) {
    }

    /** Whether {@code member} reads a date-time to the nanosecond ({@code JUDGE_NANOS}, {@link #nanos}). */
    private static boolean readsNanos(SqlQuery member) {
        boolean[] found = {false};
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                found[0] |= e instanceof SqlExpr.Cast c && c.reason() == SqlExpr.Reason.JUDGE_NANOS;
                return e;
            }
        }.rewrite(member);
        return found[0];
    }

    /** LEG 3.4: several verdict statements as ONE. Every statement's CTEs are hoisted to ONE top-level {@code WITH}
     * under per-statement names ({@code __e} of statement 3 is {@code __e_3}; the CTE's own alias is unchanged, so its
     * column references stand). The statement is TALL (item 3's S4, 2026-11-22, the user: "Don't we just need 1 column
     * per type that each assert reuses?"): a row per member, a {@code UNION ALL} branch, its {@link #MEMBER} number
     * first; then ONE column per value position and type that some member writes -- each member's value at position
     * {@code j} of SQL type {@code T} and Pure type {@code P} in the column all such values share, NULL in the columns
     * it does not write. So the row's width is the types the statement uses, never its asserts' count, and no
     * database's join limit applies (until then one wide row, a derived table per member CROSS JOINed: SQLite joins at
     * most 64 tables, MariaDB and MySQL 61 -- the S4 audit's MEDIUM 2). Each member's columns are the {@code layout}
     * the plan carries. (A {@code WITH} inside a derived table is NOT this shape: H2 blew its heap on one such branch,
     * a metamodel read with JSON aggregation -- the Linux-independent catch of 2026-09-20; the members' CTEs are
     * hoisted.) */
    public static Batch batch(List<SqlQuery> statements, List<Type.RelationType> schemas,
            java.util.Map<String, SqlQuery> frames) {
        // leg 3.4 step 2: the frames the sides reference, DEFINED once at the
        // head (MATERIALIZED: every side sees the same rows), ahead of all
        List<SqlWith.Cte> ctes = new ArrayList<>();
        java.util.Set<String> used = new java.util.LinkedHashSet<>();
        for (SqlQuery st : statements) {
            used.addAll(com.legend.sql.FrameCtes.referenced(st));
        }
        for (var f : frames.entrySet()) {
            if (used.contains(f.getKey())) {
                ctes.add(new SqlWith.Cte(f.getKey(), f.getValue(), true));
            }
        }
        // A CTE whose body is IDENTICAL to one already hoisted (after the
        // renames) is not hoisted again: its name maps to the first. The asserts
        // of one body often read the same relation — each spelled its own copy
        // of the formatted grid (a 30 KB canon each, 2026-09-23). Walking in
        // order makes it compound: once two grids are one, the cell stacks over
        // them are identical too. Frames stay as they are (named, materialized).
        java.util.Map<SqlQuery, String> hoisted = new java.util.HashMap<>();
        List<SqlQuery> bodies = new ArrayList<>(statements.size());
        List<Slot> slots = new ArrayList<>();
        List<List<OutputCol>> owned = new ArrayList<>(statements.size());
        List<List<Integer>> layout = new ArrayList<>(statements.size());
        for (int i = 0; i < statements.size(); i++) {
            SqlQuery st = statements.get(i);
            SqlQuery body = st;
            if (st instanceof SqlWith w) {
                java.util.Map<String, String> names = new java.util.LinkedHashMap<>();
                RenameCtes rename = new RenameCtes(names);
                for (SqlWith.Cte c : w.ctes()) {
                    SqlQuery q = rename.rewriteRoot(c.query());
                    String same = hoisted.get(q);
                    if (same != null && !c.materialized()) {
                        names.put(c.name(), same);
                        continue;
                    }
                    String name = c.name() + "_" + i;
                    names.put(c.name(), name);
                    hoisted.putIfAbsent(q, name);
                    ctes.add(new SqlWith.Cte(name, q, c.materialized()));
                }
                body = rename.rewriteRoot(w.body());
            }
            bodies.add(body);
            // the member's own columns, in its schema's order, by name (a seven-column row may carry more: its hints),
            // each in the shared column of its position and types
            List<OutputCol> mine = new ArrayList<>();
            List<Integer> at = new ArrayList<>();
            boolean nanos = readsNanos(st);
            List<Type.RelationType.Column> cols = schemas.get(i).columns();
            for (int j = 0; j < cols.size(); j++) {
                Type.RelationType.Column c = cols.get(j);
                OutputCol o = body.outputs().stream().filter(x -> x.name().equals(c.name())).findFirst()
                        .orElseThrow(() -> new IllegalStateException("a verdict row without its " + c.name()));
                Slot slot = new Slot(j, o.type(), c.type(), c.multiplicity(), nanos);
                int k = slots.indexOf(slot);
                if (k < 0) {
                    slots.add(slot);
                    k = slots.size() - 1;
                }
                mine.add(o);
                at.add(k + 1);   // the fused columns: the member's number first
            }
            owned.add(mine);
            layout.add(at);
        }
        List<OutputCol> outs = new ArrayList<>(List.of(new OutputCol(MEMBER, SqlType.Scalar.BIGINT, false)));
        List<Type.RelationType.Column> schema = new ArrayList<>(List.of(new Type.RelationType.Column(MEMBER,
                Type.Primitive.INTEGER, com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
        for (int k = 0; k < slots.size(); k++) {
            Slot slot = slots.get(k);
            outs.add(new OutputCol("__s" + k, slot.sql(), true));
            // a slot holds NULL in every branch that writes none: at most one value, or a list (the tall-row audit's
            // LOW 3: until then the member's own multiplicity, [1] for a column another member leaves NULL)
            schema.add(new Type.RelationType.Column("__s" + k, slot.kind(), slot.many().isMany()
                    ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                    : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE));
        }
        List<SqlQuery> branches = new ArrayList<>(bodies.size());
        for (int i = 0; i < bodies.size(); i++) {
            SqlQuery body = bodies.get(i);
            java.util.Map<Integer, OutputCol> byColumn = new java.util.HashMap<>();
            for (int j = 0; j < owned.get(i).size(); j++) {
                byColumn.put(layout.get(i).get(j), owned.get(i).get(j));
            }
            List<SqlSelect.Projection> ps = new ArrayList<>(outs.size());
            ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(i), MEMBER, outs.get(0)));
            for (int c = 1; c < outs.size(); c++) {
                OutputCol own = byColumn.get(c);
                ps.add(new SqlSelect.Projection(own == null ? nullOf(outs.get(c).type())
                        : SqlExpr.Column.of("v", body.outputs(), own.name()), outs.get(c).name(), outs.get(c)));
            }
            branches.add(new SqlSelect(ps, false, new SqlSource.Subselect(body, "v", null), null, List.of(), null,
                    null, List.of(), null, null, outs));
        }
        SqlQuery union = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        return new Batch(ctes.isEmpty() ? union : new SqlWith(ctes, union), new Type.RelationType(schema), layout);
    }

    /** A slot's NULL where a member writes none: typed where the renderer leaves a typed slot's NULL bare -- a Decimal,
     *  a TIME -- since Postgres types a UNION's column pairwise from its left, and two bare NULL branches first make it
     *  text, which a numeric branch then fails to match (the S4 tall-row audit's MEDIUM, 2026-11-22). The renderer
     *  types every other scalar slot's NULL itself; a list's, a struct's and a label carrier's stay bare, as it writes
     *  them. */
    private static SqlExpr nullOf(SqlType slot) {
        return slot instanceof SqlType.Decimal || slot == SqlType.Scalar.TIME
                ? new SqlExpr.Cast(new SqlExpr.NullLit(), slot, SqlExpr.Reason.JUDGE) : new SqlExpr.NullLit();
    }

    /** CTE references renamed (a table source named after a CTE; the
     * alias — what column references spell — is kept). */
    private static final class RenameCtes extends com.legend.sql.SqlRewriter {
        private final java.util.Map<String, String> names;

        RenameCtes(java.util.Map<String, String> names) {
            this.names = names;
        }

        @Override
        protected SqlSource source(SqlSource s) {
            if (s instanceof SqlSource.Table t) {
                String renamed = names.get(t.name());
                if (renamed != null) {
                    return new SqlSource.Table(renamed, t.alias(), t.outputs(), t.call(), t.storedTypes(), t.quoting());
                }
            }
            return s;
        }
    }

    /** {@code side}'s rows in their branches' order where it is a UNION ALL (directly or under pass-through selects),
     *  for the HOST judge's read of a side: the order the database judge numbers the same side by
     *  ({@code SideOrdinal.inBranchOrder}, 2026-11-06); {@code side} itself otherwise. */
    public static SqlQuery inBranchOrder(SqlQuery side) {
        return SideOrdinal.inBranchOrder(side);
    }

    /**
     * THE VERDICT ROW SELF-CONTAINED (the test-is-the-product plan's M8b, MV2/MV3, 2026-11-01; the user's ruling of
     * 2026-11-01, §6 "B3 as settled": an assert is one lowered statement answering holds, message, unjudged, and the
     * plan carries nothing about the assert): {@code row}, a verdict row of the five columns, gains {@link #HOLDS} --
     * the verdict under the assert's polarity -- and {@link #MESSAGE} -- the failure text the judge raises with, spelled
     * exactly as the host judge spelled it from the row: the family's {@code name}, {@code \nexpected: } the expected
     * canon, {@code \nactual:   } the actual canon (every family's two canon columns were text already -- the
     * tolerance's numbers and the counts cast by the database before M8b too -- so the message pastes the text Java
     * pasted; a null canon the word {@code null}, as Java spelled it), or the negated families' one sentence. The judge
     * reads holds and message and knows no polarity. A row wrapped once: a second wrap is a loud error.
     */
    /** {@code row} with its expected and actual texts framed as the Strings they print (Pure's {@code %r}): the
     *  rendered-text family, whose sides upstream compares as Strings -- a golden's text against a relation's
     *  {@code toString} or {@code toCSV} -- while the database judge compares their rows, whose framed text is that
     *  same text (2026-11-03: Postgres's Variant key extractions read unquoted under the database judge alone, quoted
     *  under the host). */
    /** {@code row} with its verdict decided false at planning (a grid pair whose schemas differ): its canons kept for
     *  the message. */
    /** What running an assertError's function raised, as its verdict statement reads it: its message, line and column,
     *  each a value or absent (SQL NULL) -- the runner's bound parameters ({@link #bound}), or constants where the
     *  function raised while it was planned ({@link #raised}). */
    public record Raised(SqlExpr message, SqlExpr line, SqlExpr column) {

        /** The outcome the runner binds once it ran the function: the three parameters the plan names. */
        public static Raised bound(String messageParameter, String lineParameter, String columnParameter) {
            return new Raised(param(messageParameter, SqlType.Scalar.VARCHAR),
                    param(lineParameter, SqlType.Scalar.BIGINT), param(columnParameter, SqlType.Scalar.BIGINT));
        }

        /** An error raised while the function was planned: its message, and its position where it carried one. */
        public static Raised raised(String message, @com.legend.base.Nullable Long line,
                @com.legend.base.Nullable Long column) {
            return new Raised(new SqlExpr.StringLit(message), lineOrNull(line), lineOrNull(column));
        }

        private static SqlExpr lineOrNull(@com.legend.base.Nullable Long n) {
            return n == null ? new SqlExpr.NullLit(com.legend.sql.SqlTyping.typed(SqlType.Scalar.BIGINT))
                    : new SqlExpr.IntLit(n);
        }

        private static SqlExpr param(String name, SqlType type) {
            return new SqlExpr.PlanParam(name, type == SqlType.Scalar.VARCHAR ? SqlExpr.PlanParam.ValueType.STRING
                    : SqlExpr.PlanParam.ValueType.OTHER, true, com.legend.sql.SqlTyping.typed(type), null);
        }
    }

    /**
     * ASSERTERROR'S VERDICT ROW (the user's ruling S135, 2026-11-05: the host catches, the database judges): what the
     * function raised ({@code raised}, read once through a one-row derived table) compared with the expected message,
     * line and column BY THE DATABASE, and upstream's own failure texts written here -- interpreted AssertError.java's
     * "No error was thrown", then the assertError/4 body's (assertError.pure:24-26), message first, then line, then
     * column, each only where it is expected; an expected position over a raise that carried none fails by name, as
     * upstream's {@code si.line->toOne()} raises on an empty source info. The row is the verdict schema's
     * ({@link #schema}): its polarity and its message the statement's own.
     */
    public static SqlQuery assertError(String expected, @com.legend.base.Nullable Long line,
            @com.legend.base.Nullable Long column, Raised raised, VerdictWords words, boolean passesUnraised) {
        OutputCol mOut = new OutputCol("m", SqlType.Scalar.VARCHAR, true);
        OutputCol lOut = new OutputCol("l", SqlType.Scalar.BIGINT, true);
        OutputCol cOut = new OutputCol("c", SqlType.Scalar.BIGINT, true);
        SqlSelect outcome = new SqlSelect(List.of(new SqlSelect.Projection(raised.message(), "m", mOut),
                new SqlSelect.Projection(raised.line(), "l", lOut), new SqlSelect.Projection(raised.column(), "c", cOut)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                List.of(mOut, lOut, cOut));
        SqlExpr m = SqlExpr.Column.of("o", mOut);
        SqlExpr l = SqlExpr.Column.of("o", lOut);
        SqlExpr c = SqlExpr.Column.of("o", cOut);
        SqlExpr exp = new SqlExpr.StringLit(expected);
        List<SqlExpr.Case.When> failures = new ArrayList<>();
        if (!passesUnraised) {
            // a function that raised nothing fails, but where the assert expects legend-pure's own "nothing was raised"
            // text (passesUnraised: decided at planning by comparing the two constants; item 3's S8, Q-B)
            failures.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, m), words.noError(expected)));
        }
        failures.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.NOT_EQUAL, m, exp), words.errorDiffers(expected, m)));
        if (line != null || column != null) {
            failures.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, l),
                    sentence(com.legend.error.AssertKind.ERROR_NO_POSITION, java.util.Map.of())));
        }
        if (line != null) {
            failures.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.NOT_EQUAL, l, new SqlExpr.IntLit(line)),
                    words.lineDiffers(line, new SqlExpr.Cast(l, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE))));
        }
        if (column != null) {
            failures.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.NOT_EQUAL, c, new SqlExpr.IntLit(column)),
                    words.columnDiffers(column, new SqlExpr.Cast(c, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE))));
        }
        SqlExpr message = new SqlExpr.Case(failures, new SqlExpr.NullLit(com.legend.sql.SqlTyping.typed(
                SqlType.Scalar.VARCHAR)), SqlExpr.Reason.JUDGE);
        SqlExpr holds = SqlExpr.Call.of(SqlFn.IS_NULL, message);
        List<SqlSelect.Projection> ps = List.of(
                new SqlSelect.Projection(holds, VERDICT, new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(exp, EXPECTED, new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.COALESCE, m, new SqlExpr.StringLit("")), ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(new SqlExpr.NullLit(com.legend.sql.SqlTyping.typed(SqlType.Scalar.VARCHAR)),
                        UNJUDGED, new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(new SqlExpr.BoolLit(false), LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(holds, HOLDS, new OutputCol(HOLDS, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(message, MESSAGE, new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, true)));
        return new SqlSelect(ps, false, new SqlSource.Subselect(outcome, "o", null), null, List.of(), null, null,
                List.of(), null, null, ps.stream().map(SqlSelect.Projection::out).toList());
    }

    /** {@code row}, a verdict row ({@link #schema}'s columns), as a statement that RAISES its failure (the user's
     *  ruling S135, 2026-11-05: an assert inside assertError fails as upstream's does, by raising): one Boolean value,
     *  true where the row holds, the row's message raised where it does not. */
    public static SqlQuery raisingRow(SqlQuery row) {
        if (row instanceof SqlWith w) {
            return w.with(w.ctes(), raisingRow(w.body()));
        }
        OutputCol holdsOut = new OutputCol(HOLDS, SqlType.Scalar.BOOLEAN, false);
        OutputCol messageOut = new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, true);
        SqlExpr raise = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Column.of("i", holdsOut),
                new SqlExpr.BoolLit(true))), SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.Column.of("i", messageOut)), SqlExpr.Reason.JUDGE);
        OutputCol value = new OutputCol("value", SqlType.Scalar.BOOLEAN, false);
        return new SqlSelect(List.of(new SqlSelect.Projection(raise, "value", value)), false,
                new SqlSource.Subselect(row, "i", null), null, List.of(), null, null, List.of(), null, null,
                List.of(value));
    }

    public static SqlQuery decidedFalse(SqlQuery row) {
        java.util.function.UnaryOperator<SqlSelect> f = sel -> sel.withProjections(sel.projections().stream()
                .map(p -> VERDICT.equals(p.alias()) ? new SqlSelect.Projection(new SqlExpr.BoolLit(false), p.alias(),
                        p.out()) : p).toList());
        if (row instanceof SqlWith w && w.body() instanceof SqlSelect body) {
            return new SqlWith(w.ctes(), f.apply(body));
        }
        if (row instanceof SqlSelect sel) {
            return f.apply(sel);
        }
        throw new IllegalStateException("a verdict row is a select, not " + row.getClass().getSimpleName());
    }

    /** The struct of primitive fields {@code plan}'s value is (a generic class's instance), or null. */
    public static SqlType.@com.legend.base.Nullable Struct primitiveStruct(SqlSelect plan) {
        if (plan.projections().isEmpty()
                || !(plan.projections().get(0).expr().type() instanceof com.legend.sql.TypeFact.Typed ty)
                || !(ty.type() instanceof SqlType.Struct st) || st.fields().isEmpty()) {
            return null;
        }
        for (SqlType.Struct.Field f : st.fields()) {
            if (f.type() instanceof SqlType.Struct || f.type() instanceof SqlType.Array
                    || f.type() == SqlType.Scalar.JSON) {
                return null;
            }
        }
        return st;
    }

    /** {@code value}, a struct of primitive fields, written by lite's own rules (the orchestrator's ruling, 2026-11-17):
     *  its class, then its properties as the product's compact JSON object, each value its lite text ({@link LiteText#of};
     *  an empty one {@code null}) -- {@code meta::pure::functions::collection::Pair {"first":1,"second":"a"}}. */
    private static SqlExpr instanceLite(SqlExpr value, String typeFqn) {
        SqlType.Struct st = (SqlType.Struct) ((com.legend.sql.TypeFact.Typed) value.type()).type();
        List<SqlExpr> parts = new ArrayList<>();
        parts.add(new SqlExpr.StringLit(typeFqn + " {"));
        boolean first = true;
        for (SqlType.Struct.Field f : st.fields()) {
            parts.add(new SqlExpr.StringLit((first ? "" : ",") + "\"" + f.name() + "\":"));
            first = false;
            SqlExpr v = new SqlExpr.StructGet(value, f.name());
            parts.add(SqlExpr.Call.of(SqlFn.COALESCE, LiteText.of(pureKind(f.type()), v), new SqlExpr.StringLit("null")));
        }
        parts.add(new SqlExpr.StringLit("}"));
        return new SqlExpr.Call(SqlFn.CONCAT, parts);
    }

    /** The Pure kind a struct field of SQL type {@code t} carries, for its lite text. */
    private static Type pureKind(SqlType t) {
        if (t == SqlType.Scalar.VARCHAR) {
            return Type.Primitive.STRING;
        }
        if (t == SqlType.Scalar.BOOLEAN) {
            return Type.Primitive.BOOLEAN;
        }
        if (t == SqlType.Scalar.DOUBLE) {
            return Type.Primitive.FLOAT;
        }
        if (t instanceof SqlType.Decimal) {
            return Type.Primitive.DECIMAL;
        }
        if (t == SqlType.Scalar.DATE) {
            return Type.Primitive.STRICT_DATE;
        }
        if (t == SqlType.Scalar.TIMESTAMP || t == SqlType.Scalar.TIMESTAMPTZ) {
            return Type.Primitive.DATE_TIME;
        }
        return Type.Primitive.INTEGER;
    }

    /** A caller's failure message for an assert (the asserts-as-plan-nodes program, step 2, 2026-11-04): a constant the
     *  plan spells ({@code literal}), or the one-row relation of its {@code value} the database computes
     *  ({@link #firstValueRow}). */
    public record FailureMessage(@com.legend.base.Nullable String literal, @com.legend.base.Nullable SqlQuery row) {
        public FailureMessage {
            if ((literal == null) == (row == null)) {
                throw new IllegalArgumentException("a failure message is a literal or a row, one of the two");
            }
        }
    }

    /** A caller's message READ where its assert fails (the assert bar, 2026-11-04): {@code textRow}, the message's one
     *  value, as a self-contained verdict row that holds nothing and whose message is that value -- the statement the
     *  database judge's batch sends at the failing assert's place alone, so a message that would raise where the assert
     *  holds is never sent (upstream evaluates it only where the assert fails). */
    public static SqlQuery messageRow(SqlQuery textRow, String name, VerdictWords words) {
        SqlExpr text = SqlExpr.Call.of(SqlFn.COALESCE, asText(SqlExpr.Column.of("rm", textRow.outputs(), "value")),
                new SqlExpr.StringLit("null"));
        SqlSelect row = new SqlSelect(List.of(
                new SqlSelect.Projection(new SqlExpr.BoolLit(false), VERDICT,
                        new OutputCol(VERDICT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(new SqlExpr.StringLit(""), EXPECTED,
                        new OutputCol(EXPECTED, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(new SqlExpr.StringLit(""), ACTUAL,
                        new OutputCol(ACTUAL, SqlType.Scalar.VARCHAR, false)),
                new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), UNJUDGED,
                        new OutputCol(UNJUDGED, SqlType.Scalar.VARCHAR, true)),
                new SqlSelect.Projection(new SqlExpr.BoolLit(false), LENIENT,
                        new OutputCol(LENIENT, SqlType.Scalar.BOOLEAN, false)),
                new SqlSelect.Projection(text, MESSAGE, new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, true))),
                false, new SqlSource.Subselect(textRow, "rm", null), null, List.of(), null, null, List.of(), null, null,
                List.of());
        return selfContained(row, name, true, words);
    }

    /** {@code row}, a verdict row, failing with {@code message}: its MESSAGE column the caller's text, replacing any the
     *  family composed -- upstream's {@code assert(condition, | message)}; the comparison the row's own. */
    public static SqlQuery withFailureMessage(SqlQuery row, FailureMessage message) {
        java.util.function.UnaryOperator<SqlSelect> f = sel -> {
            SqlSource from = sel.from();
            SqlExpr text;
            if (message.row() != null) {
                SqlQuery m = message.row();
                from = new SqlSource.Join(from, new SqlSource.Subselect(m, "rm", null), SqlSource.Join.Kind.CROSS,
                        null);
                text = SqlExpr.Call.of(SqlFn.COALESCE, asText(SqlExpr.Column.of("rm", m.outputs(), "value")),
                        new SqlExpr.StringLit("null"));
            } else {
                text = new SqlExpr.StringLit(java.util.Objects.requireNonNull(message.literal()));
            }
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (SqlSelect.Projection p : sel.projections()) {
                if (!MESSAGE.equals(p.alias())) {
                    ps.add(p);
                }
            }
            ps.add(new SqlSelect.Projection(text, MESSAGE, new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, true)));
            return new SqlSelect(ps, sel.distinct(), from, sel.where(), sel.groupBy(), sel.having(), sel.qualify(),
                    sel.orderBy(), sel.limit(), sel.offset(), sel.outputs());
        };
        if (row instanceof SqlWith w && w.body() instanceof SqlSelect body) {
            return new SqlWith(w.ctes(), f.apply(body));
        }
        if (row instanceof SqlSelect sel) {
            return f.apply(sel);
        }
        throw new IllegalStateException("a verdict row is a select, not " + row.getClass().getSimpleName());
    }

    /** {@code words}: the frame's sentence, lite's (the database judge plans for lite's flow alone). */
    public static SqlQuery selfContained(SqlQuery row, String name, boolean wantEqual, VerdictWords words) {
        boolean cells = joinsCells(row);
        if (row instanceof SqlWith w && w.body() instanceof SqlSelect body) {
            return new SqlWith(w.ctes(), withHoldsAndMessage(body, name, wantEqual, cells, words));
        }
        if (row instanceof SqlSelect sel) {
            return withHoldsAndMessage(sel, name, wantEqual, cells, words);
        }
        throw new IllegalStateException("a verdict row is a select, not " + row.getClass().getSimpleName());
    }

    /** Whether {@code row} joins a row's cells by the canon's separator anywhere (a grid's row canons): the one case
     *  its message must write the cells as a list ({@link #cellsAsList}); every other verdict's message carries no
     *  such step (core's statements small: the user's order, 2026-11-03). */
    private static boolean joinsCells(SqlQuery row) {
        boolean[] found = new boolean[1];
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlExpr.StringLit lit && CanonicalRenderSql.TDS_CELL_SEP.equals(lit.value())) {
                    found[0] = true;
                }
                return e;
            }
        }.rewrite(row);
        return found[0];
    }

    private static SqlSelect withHoldsAndMessage(SqlSelect sel, String name, boolean wantEqual, boolean cells,
            VerdictWords words) {
        if (projectionOf(sel, HOLDS) != null) {
            throw new IllegalStateException("a verdict row is made self-contained once: " + name);
        }
        SqlExpr verdict = java.util.Objects.requireNonNull(projectionOf(sel, VERDICT), "the verdict column").expr();
        SqlExpr expected = java.util.Objects.requireNonNull(projectionOf(sel, EXPECTED), "the expected column").expr();
        SqlExpr actual = java.util.Objects.requireNonNull(projectionOf(sel, ACTUAL), "the actual column").expr();
        SqlExpr holds = wantEqual ? verdict : SqlExpr.Call.of(SqlFn.NOT, verdict);
        // upstream's own text (essential/tests/assertEquals.pure: '\nexpected: %r\nactual:   %r'; assertNotEquals.pure:
        // '%r should not equal %r'), the host judge's (PureAsserts): no family name before it, and a newline inside a
        // spelled value written as %r writes it, \n (2026-11-03: the database judge's failures read 'assertEquals
        // \nexpected: ...' on every lane, so a lane's ledger, built from the host's text, did not match them)
        java.util.function.UnaryOperator<SqlExpr> list = t -> cells ? cellsAsList(t) : t;
        SqlExpr e = words.frameSide(asText(expected), list);
        SqlExpr a = words.frameSide(asText(actual), list);
        SqlSelect.Projection ew = projectionOf(sel, EXPECTED_WRITTEN);
        SqlSelect.Projection aw = projectionOf(sel, ACTUAL_WRITTEN);
        if (ew != null && aw != null) {
            // the two sides as upstream's message writes them (Message): the canons decide, these are read
            e = ew.expr();
            a = aw.expr();
        }
        SqlSelect.Projection fixed = projectionOf(sel, MESSAGE);
        SqlSelect.Projection multiset = projectionOf(sel, MULTISET_HINT);
        SqlSelect.Projection isNot = projectionOf(sel, IS_NOT_HINT);
        SqlSelect.Projection json = projectionOf(sel, JSON_HINT);
        SqlSelect.Projection ek = projectionOf(sel, EXPECTED_KIND);
        SqlSelect.Projection ak = projectionOf(sel, ACTUAL_KIND);
        SqlExpr message = fixed != null ? fixed.expr()
                : isNot != null && !wantEqual ? words.identical(a)
                : words.frame(json != null, multiset != null, wantEqual, e, a, ek == null ? null : ek.expr(),
                        ak == null ? null : ak.expr());
        List<SqlSelect.Projection> ps = new ArrayList<>(sel.projections());
        ps.remove(fixed);   // a fixed message (an assert's own) moves after the polarity, the schema's order
        ps.remove(ew);      // the written sides are the message's, no column of the verdict's schema
        ps.remove(aw);
        ps.remove(multiset);   // the hint is the frame's, no column of the verdict's schema
        ps.remove(ek);
        ps.remove(ak);
        ps.remove(isNot);
        ps.remove(json);
        ps.add(new SqlSelect.Projection(holds, HOLDS, new OutputCol(HOLDS, SqlType.Scalar.BOOLEAN, false)));
        ps.add(new SqlSelect.Projection(message, MESSAGE, new OutputCol(MESSAGE, SqlType.Scalar.VARCHAR, true)));
        // the select's outputs are rebuilt from its projections by its constructor
        return new SqlSelect(ps, sel.distinct(), sel.from(), sel.where(), sel.groupBy(), sel.having(), sel.qualify(),
                sel.orderBy(), sel.limit(), sel.offset(), sel.outputs());
    }

    /** {@code row}, a verdict row before it is made self-contained, marked {@code assertIsNot}'s (#28): the frame writes
     *  that sentence where it fails. */
    public static SqlQuery withIsNotHint(SqlQuery row) {
        return withHint(row, IS_NOT_HINT);
    }

    /** {@code row} carrying the frame's hint {@code alias}, a Boolean column the frame reads and drops. */
    private static SqlQuery withHint(SqlQuery row, String alias) {
        java.util.function.UnaryOperator<SqlSelect> mark = sel -> {
            List<SqlSelect.Projection> ps = new ArrayList<>(sel.projections());
            ps.add(new SqlSelect.Projection(new SqlExpr.BoolLit(true), alias,
                    new OutputCol(alias, SqlType.Scalar.BOOLEAN, false)));
            return sel.withProjections(ps);
        };
        if (row instanceof SqlWith w && w.body() instanceof SqlSelect body) {
            return new SqlWith(w.ctes(), mark.apply(body));
        }
        if (row instanceof SqlSelect sel) {
            return mark.apply(sel);
        }
        throw new IllegalStateException("a verdict row is a select, not " + row.getClass().getSimpleName());
    }

    /** A kind's name as lite's sentence writes it where two texts tie: a primitive's short name (a Decimal of a declared
     *  precision a Decimal), any other type its own name. */
    public static String kindText(Type t) {
        return t instanceof Type.PrecisionDecimal ? "Decimal" : t.typeName();
    }

    /** {@code row}, a self-contained verdict row, its message as {@code words} name an unrolled quantifier's element
     *  {@code index} (the {@code FrameKind.AT_INDEX} frame: the row's message, then the zero-based index). */
    public static SqlQuery atIndex(SqlQuery row, int index, VerdictWords words) {
        java.util.function.UnaryOperator<SqlSelect> at = sel -> {
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (SqlSelect.Projection p : sel.projections()) {
                ps.add(MESSAGE.equals(p.alias()) ? new SqlSelect.Projection(words.atIndex(p.expr(), index), p.alias(),
                        p.out()) : p);
            }
            return sel.withProjections(ps);
        };
        if (row instanceof SqlWith w && w.body() instanceof SqlSelect body) {
            return new SqlWith(w.ctes(), at.apply(body));
        }
        if (row instanceof SqlSelect sel) {
            return at.apply(sel);
        }
        throw new IllegalStateException("a verdict row is a select, not " + row.getClass().getSimpleName());
    }

    /** {@code plain} where the two written texts differ, else {@code EQUALS_KINDS}. */
    static SqlExpr equalsOrKinds(SqlExpr e, SqlExpr a, SqlExpr expectedKind, SqlExpr actualKind,
            SqlExpr plain) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, e, a),
                sentence(com.legend.error.AssertKind.EQUALS_KINDS, java.util.Map.of("expected", e,
                        "expectedKind", expectedKind, "actual", a, "actualKind", actualKind)))),
                plain, SqlExpr.Reason.JUDGE);
    }

    /** The relation the verdict statement returns (the executor's
     * TABULAR decode needs the declared schema). */
    public static com.legend.compiler.element.type.Type.RelationType schema() {
        var one = new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1);
        var opt = new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1);
        var t = com.legend.compiler.element.type.Type.Primitive.STRING;
        return new com.legend.compiler.element.type.Type.RelationType(List.of(
                new com.legend.compiler.element.type.Type.RelationType.Column(VERDICT,
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(EXPECTED, t, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(ACTUAL, t, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(UNJUDGED, t, opt),
                new com.legend.compiler.element.type.Type.RelationType.Column(LENIENT,
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(HOLDS,
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one),
                new com.legend.compiler.element.type.Type.RelationType.Column(MESSAGE, t, opt)));
    }

    /** {@code SELECT canon AS __c, row_number() OVER () AS __rn FROM (wrapped) w}
     * — the side reduced to its deciding canon texts in arrival order. */
    /** The wrap's projection named {@code alias}, or null. */
    private static SqlSelect.@com.legend.base.Nullable Projection projectionOf(SqlSelect ws, String alias) {
        for (SqlSelect.Projection p : ws.projections()) {
            if (alias.equals(p.alias())) {
                return p;
            }
        }
        return null;
    }

    /** TEXT by contract: a cast to VARCHAR unless the expression is one already. */
    /** A row canon's cells written as the list %r writes them: the canon joins a row's cells by its own separator
     *  ({@link CanonicalRenderSql#TDS_CELL_SEP}, row cohesion), which a message never shows (2026-11-03, the core parity
     *  program: {@code ['p1'<GS>40, ...]} under the database judge, upstream's {@code ['p1', 40, ...]}). */
    private static SqlExpr cellsAsList(SqlExpr text) {
        return SqlExpr.Call.of(SqlFn.REPLACE, text, new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP),
                new SqlExpr.StringLit(", "));
    }

    private static SqlExpr asText(SqlExpr e) {
        return e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.VARCHAR ? e
                : new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
    }

    /** A side declared exactly one: its facts row spelled straight over the
     * plan (ONE row by construction — a one-row seed LEFT JOINed to the plan,
     * so an empty plan still frames {@code []} exactly as the rows form does). */
    /** E-5b's one-row layer under a single-value side's facts: its canon and its value, each computed once. */
    private static final String ONCE_ALIAS = "__once";
    private static final String CANON_ONCE = "__oc";
    private static final String VALUE_ONCE = "__ov";

    private static SqlQuery inlineFacts(Side s) {
        SqlSelect ws = (SqlSelect) s.wrapped();
        SqlExpr valueRef = ws.projections().get(0).expr();
        SqlExpr canon = canonOf(s, asText(java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column").expr()));
        List<OutputCol> outs = factOutputs();
        OutputCol seedOut = new OutputCol("__one", SqlType.Scalar.BIGINT, false);
        SqlSource seed = new SqlSource.Subselect(new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", seedOut)),
                false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                List.of(seedOut)), "__one", null);
        SqlSource joined = new SqlSource.Join(seed, ws.from(), SqlSource.Join.Kind.LEFT,
                new SqlExpr.BoolLit(true));
        // E-5b (the user's ruling, 2026-11-13; docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §5.9): the side's canon and
        // its value computed ONCE, in a one-row layer over the side's own source, and read by column -- the canon was
        // written three times here (the text, the null flag, the tree test), each a copy of its whole spelling
        OutputCol canonOut = new OutputCol(CANON_ONCE, SqlType.Scalar.VARCHAR, true);
        OutputCol valueOut = new OutputCol(VALUE_ONCE, java.util.Objects.requireNonNull(ws.projections().get(0).out(),
                "a single-value side's value projection declares its column").type(), true);
        SqlSelect once = new SqlSelect(List.of(new SqlSelect.Projection(canon, CANON_ONCE, canonOut),
                new SqlSelect.Projection(valueRef, VALUE_ONCE, valueOut)), false, joined, null, List.of(), null, null,
                List.of(), null, null, List.of(canonOut, valueOut));
        SqlSource from = new SqlSource.Subselect(once, ONCE_ALIAS, null);
        canon = SqlExpr.Column.of(ONCE_ALIAS, CANON_ONCE, SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED);
        valueRef = SqlExpr.Column.of(ONCE_ALIAS, VALUE_ONCE, valueOut.type(), true, OutputCol.Origin.DERIVED);
        SqlExpr flag = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef), SqlExpr.Call.of(SqlFn.IS_NULL, canon));
        List<SqlSelect.Projection> ps = new ArrayList<>(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.COALESCE, canon,
                        new SqlExpr.StringLit("[]")), F_TEXT, outs.get(0)),
                new SqlSelect.Projection(flagCount(flag), F_NULLS, outs.get(1)),
                new SqlSelect.Projection(flagCount(SqlExpr.Call.of(SqlFn.GREATER,
                        SqlExpr.Call.of(SqlFn.STRPOS, canon, new SqlExpr.StringLit(CanonicalRenderSql.TREE_MARKER)),
                        new SqlExpr.IntLit(0))), F_TREES, outs.get(2)),
                new SqlSelect.Projection(flagCount(SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef)), F_N, outs.get(3))));
        if (s.message() != null) {
            // the one element as the message writes it, and as a list of one (or none: [])
            List<OutputCol> wouts = writtenFactOutputs();
            SqlExpr w = written(s.message(), valueRef, canon);
            ps.add(new SqlSelect.Projection(w, F_ONE_W, wouts.get(4)));
            ps.add(new SqlSelect.Projection(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.IS_NULL, valueRef), new SqlExpr.StringLit("[]"))),
                    SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("["), w, new SqlExpr.StringLit("]")), SqlExpr.Reason.JUDGE),
                    F_LIST_W, wouts.get(5)));
            outs = wouts;
        }
        return new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null, outs);
    }

    /** {@code CASE WHEN <cond> THEN 1 ELSE 0 END}. */
    private static SqlExpr flagCount(SqlExpr cond) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(cond, new SqlExpr.IntLit(1))),
                new SqlExpr.IntLit(0), SqlExpr.Reason.JUDGE);
    }

    /** A many or optional side as rows {@code (__c, __rn[, __v])} at ONE
     * level: the chosen canon expression spliced over the plan; the wrap's
     * canonical order rides the row number's own window. */
    /** A wrap that only projects row expressions over its source: no
     * aggregate or window in a projection, no where / group / having /
     * qualify / distinct / limit / offset — its projections can be spliced
     * into a reader without changing what they compute. A JSON-document
     * side ({@code to_json(list(…))}) is NOT one: its value is an aggregate
     * over the plan's rows and must stay a layer. */
    private static boolean plainProjection(SqlSelect ws) {
        if (ws.where() != null || !ws.groupBy().isEmpty() || ws.having() != null
                || ws.qualify() != null || ws.distinct() || ws.limit() != null || ws.offset() != null) {
            return false;
        }
        boolean[] aggregate = {false};
        SqlSelect projectionsOnly = new SqlSelect(ws.projections(), false, new SqlSource.Dual(),
                null, List.of(), null, null, List.of(), null, null, List.of());
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlAgg || e instanceof SqlExpr.WindowCall
                        || e instanceof SqlExpr.JsonArrayAgg || e instanceof SqlExpr.ScalarSubquery
                        || e instanceof SqlExpr.Exists) {
                    aggregate[0] = true;
                }
                return e;
            }
        }.rewriteRoot(projectionsOnly);
        return !aggregate[0];
    }

    private static SqlQuery canonRows(Side s) {
        return canonRows(s, false);
    }

    /** {@code withValues}: the PAIR may be judged with the 2-ULP leniency,
     * so this side carries the value column — its Float value, or a typed
     * NULL when it has none (the column belongs to the pair, not the side). */
    private static SqlQuery canonRows(Side s, boolean withValues) {
        SqlSelect ws = (SqlSelect) s.wrapped();
        if (!plainProjection(ws)) {
            return canonRowsLayered(s, ws, withValues);
        }
        SqlExpr valueRef = ws.projections().get(0).expr();
        SqlExpr canon = canonOf(s, asText(java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column").expr()));
        // pure has no null VALUE: a NULL row of a value collection is dropped
        // and a NULL scalar reads as the EMPTY collection — the side drops the
        // row, so an empty [] frames '[]' and never counts as a null canon
        // cell (a NULL canon over a non-null value stays unjudged)
        SqlExpr where = SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef);
        // a canon-ordered side re-orders by __c in the aggregate; the wrap's
        // own ORDER BY is the row number's window otherwise
        List<SqlSelect.SortKey> order = s.byCanonText() ? List.of() : ws.orderBy();
        SqlSource from = ws.from();
        if (!s.byCanonText() && order.isEmpty() && from instanceof SqlSource.Subselect sub
                && SideOrdinal.defined(sub.inner()) instanceof SqlQuery ordered) {
            // the order the side's own plan defines (its sort, a literal's rows), numbered inside it (SideOrdinal),
            // never its rows' arrival through the derived table, which a database need not keep: Snowflake numbered
            // sort's testSimpleSortShared [2, 3, 1, 1, 5, 4, 2] by ORDER BY NULL at 81960bf7 (and Trino, the residue
            // GATES records), where 9dfb0ae1 had fixed a layered side only (2026-10-22)
            from = new SqlSource.Subselect(ordered, sub.alias(), sub.frameName());
            order = List.of(new SqlSelect.SortKey(SqlExpr.Column.of(sub.alias(), ordered.outputs(), SideOrdinal.ORD),
                    true, null, null));
        }
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), order, null);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(canon, C, cOut));
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (withValues) {
            OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
            ps.add(new SqlSelect.Projection(valueColumn(valueRef, s.isFloat()), V, vOut));
            outs.add(vOut);
        }
        written(s, valueRef, canon, ws.projections().get(0).out(), ps, outs);
        return new SqlSelect(ps, false, from, where, List.of(), null, null, List.of(),
                null, null, List.copyOf(outs));
    }

    /** {@code value} as the failure message writes it: its kind's representation; an empty side's (Nil) none, as it
     *  holds no element. */
    private static SqlExpr written(Message m, SqlExpr value, SqlExpr canon) {
        return m.canon() ? liteOfCanon(canon, m.canonClass()) : written(m, value);
    }

    /** An element's lite text from its CANON (legend-pure's print, the mixed-kind canon's spelling): a String
     *  ({@code 'a'}, backslash-escaped) quoted JSON-style, a date's {@code %} dropped, a Decimal's {@code D} dropped,
     *  every other element as written -- each element's runtime kind read off its canon's mark, per row, in a CASE
     *  (the orchestrator's ruling, 2026-11-17). */
    static SqlExpr liteOfCanon(SqlExpr canon) {
        return liteOfCanon(canon, null);
    }

    /** {@code classFqn}: an element whose canon is a JSON object (an instance) is written class-prefixed. */
    static SqlExpr liteOfCanon(SqlExpr canon, @com.legend.base.Nullable String classFqn) {
        SqlExpr c = asText(canon);
        SqlExpr len = SqlExpr.Call.of(SqlFn.LENGTH, c);
        SqlExpr inner = SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(2),
                SqlExpr.Call.of(SqlFn.MINUS, len, new SqlExpr.IntLit(2)));
        // inside legend-pure's quotes: \' a quote, \\ a backslash, \n a newline; JSON keeps \\ and \n, escapes a
        // double quote, a tab and a carriage return
        SqlExpr json = inner;
        String[][] pairs = {{"\"", "\\\""}, {"\\'", "'"}, {"\t", "\\t"}, {"\r", "\\r"}};
        for (String[] p : pairs) {
            json = SqlExpr.Call.of(SqlFn.REPLACE, json, new SqlExpr.StringLit(p[0]), new SqlExpr.StringLit(p[1]));
        }
        SqlExpr first = SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(1), new SqlExpr.IntLit(1));
        SqlExpr last = SqlExpr.Call.of(SqlFn.SUBSTRING, c, len, new SqlExpr.IntLit(1));
        SqlExpr rest = SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(2),
                SqlExpr.Call.of(SqlFn.MINUS, len, new SqlExpr.IntLit(1)));
        SqlExpr butLast = SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(1),
                SqlExpr.Call.of(SqlFn.MINUS, len, new SqlExpr.IntLit(1)));
        // an instance's identity canon, <id instanceOf T>: T (instance id)
        SqlExpr marker = SqlExpr.Call.of(SqlFn.STRPOS, c, new SqlExpr.StringLit(" instanceOf "));
        SqlExpr identity = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.SUBSTRING, c, SqlExpr.Call.of(SqlFn.PLUS, marker, new SqlExpr.IntLit(12)),
                        SqlExpr.Call.of(SqlFn.MINUS, len, SqlExpr.Call.of(SqlFn.PLUS, marker, new SqlExpr.IntLit(12)))),
                new SqlExpr.StringLit(" (instance "),
                SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(2),
                        SqlExpr.Call.of(SqlFn.MINUS, marker, new SqlExpr.IntLit(2))),
                new SqlExpr.StringLit(")"));
        List<SqlExpr.Case.When> instance = new ArrayList<>();
        if (classFqn != null) {
            // an instance's JSON object, class-prefixed
            instance.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("{")),
                    SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit(classFqn + " "), c)));
            // a side carried as one JSON array of instances: one written bare, several each prefixed, in brackets
            SqlExpr second = SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(2), new SqlExpr.IntLit(1));
            SqlExpr objects = SqlExpr.Call.of(SqlFn.SUBSTRING, c, new SqlExpr.IntLit(2),
                    SqlExpr.Call.of(SqlFn.MINUS, len, new SqlExpr.IntLit(2)));
            SqlExpr several = SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("[" + classFqn + " "),
                    SqlExpr.Call.of(SqlFn.REPLACE, objects, new SqlExpr.StringLit("},{"),
                            new SqlExpr.StringLit("}, " + classFqn + " {")), new SqlExpr.StringLit("]"));
            SqlExpr one = SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit(classFqn + " "), objects);
            instance.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.AND,
                    SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("[")),
                    SqlExpr.Call.of(SqlFn.EQUAL, second, new SqlExpr.StringLit("{"))),
                    SqlExpr.Call.of(SqlFn.EQUAL, last, new SqlExpr.StringLit("]"))),
                    new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL,
                            SqlExpr.Call.of(SqlFn.STRPOS, objects, new SqlExpr.StringLit("},{")), new SqlExpr.IntLit(0)),
                            one)), several, SqlExpr.Reason.JUDGE)));
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>(instance);
        whens.addAll(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND, SqlExpr.Call.of(SqlFn.AND,
                        SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("<")),
                        SqlExpr.Call.of(SqlFn.EQUAL, last, new SqlExpr.StringLit(">"))),
                        SqlExpr.Call.of(SqlFn.GREATER, marker, new SqlExpr.IntLit(0))), identity),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND,
                        SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("'")),
                        SqlExpr.Call.of(SqlFn.GREATER_EQUAL, len, new SqlExpr.IntLit(2))),
                        SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("\""), json, new SqlExpr.StringLit("\""))),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, first, new SqlExpr.StringLit("%")), rest),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.AND,
                        SqlExpr.Call.of(SqlFn.EQUAL, last, new SqlExpr.StringLit("D")),
                        SqlExpr.Call.of(SqlFn.GREATER, len, new SqlExpr.IntLit(1))), butLast)));
        return new SqlExpr.Case(whens, c, SqlExpr.Reason.JUDGE);
    }

    private static SqlExpr written(Message m, SqlExpr value) {
        if (m.literal() != null) {
            return new SqlExpr.StringLit(m.literal());
        }
        if (m.kind() instanceof Type.GenericType g) {
            return instanceLite(value, g.rawFqn());
        }
        return PlatformTypes.isNil(m.kind())
                ? new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                : LiteText.of(m.kind(), value);
    }

    /** A row's element as the failure message writes it ({@link Message}), and {@code __o} (unread since item 3's S0;
     *  goes at S4): two more columns of a side's rows, where the side is written. */
    private static void written(Side s, SqlExpr valueRef, SqlExpr canon, @com.legend.base.Nullable OutputCol valueOut,
            List<SqlSelect.Projection> ps, List<OutputCol> outs) {
        if (s.message() == null) {
            return;
        }
        OutputCol wOut = new OutputCol(W, SqlType.Scalar.VARCHAR, true);
        ps.add(new SqlSelect.Projection(written(s.message(), valueRef, canon), W, wOut));
        outs.add(wOut);
        if (s.message().sorted()) {
            OutputCol oOut = new OutputCol(O, valueOut != null ? valueOut.type() : SqlType.Scalar.VARCHAR, true);
            ps.add(new SqlSelect.Projection(valueRef, O, oOut));
            outs.add(oOut);
        }
    }

    /** A wrap that must stay a layer (an aggregate value, a filtered or
     * capped wrap): trimmed to its value and the ONE canon the side reads,
     * the rows read over it under {@code w}. */
    /** The pair's value column for one side: the Float value as DOUBLE, or a
     * typed NULL for a side that has none. */
    private static SqlExpr valueColumn(SqlExpr valueRef, boolean isFloat) {
        return isFloat ? new SqlExpr.Cast(valueRef, SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                : new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE);
    }

    private static SqlQuery canonRowsLayered(Side s, SqlSelect ws, boolean withValues) {
        SqlSelect.Projection value = ws.projections().get(0);
        SqlSelect.Projection chosen = java.util.Objects.requireNonNull(
                projectionOf(ws, s.canonColumn()), "canon column");
        List<SqlSelect.Projection> kept = chosen == value ? List.of(value) : List.of(value, chosen);
        List<OutputCol> keptOuts = kept.stream().map(SqlSelect.Projection::out)
                .filter(java.util.Objects::nonNull).toList();
        // a canon-ordered side re-orders by __c in the aggregate; the wrap's
        // own ORDER BY stays for an ordered compare (the row number follows it)
        List<SqlSelect.SortKey> order = s.byCanonText() ? List.of() : ws.orderBy();
        SqlSelect trimmed = new SqlSelect(kept, ws.distinct(), ws.from(), ws.where(), ws.groupBy(),
                ws.having(), ws.qualify(), order, ws.limit(), ws.offset(), keptOuts);
        // an ordered side's rows numbered by its own order, inside it (SideOrdinal), never by their arrival through
        // the derived table: a database need not keep a derived table's order (Snowflake's numbering of the arrival,
        // ORDER BY NULL, and Trino's lost sort's testSimpleSortShared and testSimpleSort_MultipleExpressions, measured
        // 2026-10-21; the grid sides' own fix of 2026-10-20)
        SqlQuery read = order.isEmpty() ? trimmed : SideOrdinal.withOrdinal(trimmed);
        List<OutputCol> readOuts = read.outputs();
        SqlExpr canon = canonOf(s, asText(SqlExpr.Column.of("w", readOuts, s.canonColumn())));
        SqlExpr valueRef = SqlExpr.Column.of("w", readOuts, java.util.Objects.requireNonNull(value.alias()));
        SqlExpr rn = rowNumber(readOuts);
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(canon, C, cOut));
        ps.add(new SqlSelect.Projection(rn, RN, rnOut));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (withValues) {
            OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
            ps.add(new SqlSelect.Projection(valueColumn(valueRef, s.isFloat()), V, vOut));
            outs.add(vOut);
        }
        written(s, valueRef, canon, value.out(), ps, outs);
        return new SqlSelect(ps, false, new SqlSource.Subselect(read, "w", null),
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, valueRef), List.of(), null, null, List.of(),
                null, null, List.copyOf(outs));
    }

    /** A grid side's row canons in arrival order: {@code SELECT
     * CAST(w.__rowcanon AS VARCHAR) AS __c, row_number() OVER () AS __rn}. */
    private static SqlQuery gridRowCanons(SqlQuery wrapped) {
        return rowsOf(new SqlExpr.Cast(
                SqlExpr.Column.of("w", wrapped.outputs(), CanonicalRenderSql.ROW_CANON),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), wrapped);
    }

    /** A grid's row canons; under a column's empty-is-NULL equivalence the
     * row canon is rebuilt from the cell canons ({@code __cell<i>} joined by
     * the cell separator, as the wrap builds {@code __rowcanon}). */
    private static SqlQuery gridRowCanons(GridSide grid, String named) {
        return gridRowCanons(grid, named, null);
    }

    /** How a grid's row is WRITTEN in lite's sentence: its cells in brackets (a row of rows), or its cells alone (a
     *  grid's cells flattened against a peer's); null, not written. */
    private enum RowWriting { BRACKETED, FLAT }

    /** A grid row's lite text: each cell from its canon ({@link #liteOfCanon}), comma-joined, bracketed or not. */
    private static SqlExpr writtenRow(GridSide grid, RowWriting writing) {
        List<SqlExpr> parts = new ArrayList<>();
        if (writing == RowWriting.BRACKETED) {
            parts.add(new SqlExpr.StringLit("["));
        }
        for (int i = 0; i < grid.width(); i++) {
            if (i > 0) {
                parts.add(new SqlExpr.StringLit(", "));
            }
            parts.add(liteOfCanon(gridCell(grid, i)));
        }
        if (writing == RowWriting.BRACKETED) {
            parts.add(new SqlExpr.StringLit("]"));
        }
        return parts.size() == 1 ? parts.get(0) : new SqlExpr.Call(SqlFn.CONCAT, parts);
    }

    private static SqlQuery gridRowCanons(GridSide grid, String named, @com.legend.base.Nullable RowWriting writing) {
        SqlExpr w = writing == null ? null : writtenRow(grid, writing);
        if (grid.emptyIsNull().stream().noneMatch(b -> b)) {
            return rowsOf(new SqlExpr.Cast(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), CanonicalRenderSql.ROW_CANON),
                    SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE), new SqlExpr.NullLit(), w, over(grid, named), null);
        }
        SqlExpr row = null;
        for (int i = 0; i < grid.width(); i++) {
            SqlExpr cell = gridCell(grid, i);
            row = row == null ? cell : SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, row,
                            new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP)), cell);
        }
        return rowsOf(java.util.Objects.requireNonNull(row), new SqlExpr.NullLit(), w, over(grid, named), null);
    }

    /** One cell canon of a grid, the empty String canon ({@code ''}) read as
     * the sentinel where the column's grammar conflates it with NULL. */
    private static SqlExpr gridCell(GridSide grid, int i) {
        SqlExpr cell = new SqlExpr.Cast(SqlExpr.Column.of("w", grid.wrapped().outputs(),
                CanonicalRenderSql.CELL_CANON + i), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        if (!grid.emptyIsNullAt(i)) {
            return cell;
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, cell, new SqlExpr.StringLit("''")),
                new SqlExpr.StringLit(CanonicalRenderSql.NULL_CELL))), cell, SqlExpr.Reason.JUDGE);
    }

    /** A grid side's loose CELL pool: one row per cell of every row, the
     * per-cell canons the wrap projected ({@code __cell<i>}), stacked. */
    private static SqlQuery gridCellCanons(GridSide grid, String named) {
        return gridCellCanons(grid, named, false);
    }

    /** {@code lite}: each cell also written, its lite text from its canon. */
    private static SqlQuery gridCellCanons(GridSide grid, String named, boolean lite) {
        if (lite) {
            // lite's sentence lists the pool's cells in the grid's own order, row by row (as the host reads them):
            // the row-major cells, each also written from its canon
            SqlSelect rowMajor = (SqlSelect) gridCellsRowMajor(grid, named);
            OutputCol wOut = new OutputCol(W, SqlType.Scalar.VARCHAR, true);
            List<SqlSelect.Projection> ps = new ArrayList<>(rowMajor.projections());
            ps.add(new SqlSelect.Projection(liteOfCanon(rowMajor.projections().get(0).expr()), W, wOut));
            return rowMajor.withProjections(ps);
        }
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        List<SqlExpr> cells = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(new SqlExpr.Cast(SqlExpr.Column.of("w", grid.wrapped().outputs(),
                    CanonicalRenderSql.CELL_CANON + i), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE));
        }
        SqlQuery stacked = new SqlSelect(List.of(new SqlSelect.Projection(pick(cells), C, cOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut));
        // the pool's arrival order is meaningless (a multiset by definition);
        // __rn exists for the frame's count and the LIMIT 1 read only
        SqlExpr cell = SqlExpr.Column.of("w", List.of(cOut), C);
        return rowsOf(cell, new SqlExpr.NullLit(), lite ? liteOfCanon(cell) : null,
                new SqlSource.Subselect(stacked, "w", null), null);
    }

    /** The peer's element canons as cells: the literal-channel canon,
     * the golden's quoted {@code 'TDSNull'} cell spelled as the bare
     * sentinel on the expected side. */
    private static SqlQuery peerCells(PeerSide peer) {
        return peerCells(peer, false);
    }

    /** {@code lite}: each cell also written, its lite text from its canon. */
    private static SqlQuery peerCells(PeerSide peer, boolean lite) {
        SqlExpr c = new SqlExpr.Cast(
                SqlExpr.Column.of("w", peer.wrapped().outputs(), peer.canonColumn()),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        if (peer.expected()) {
            c = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, c,
                            new SqlExpr.StringLit("'" + CanonicalRenderSql.NULL_CELL_WIRE + "'")),
                    new SqlExpr.StringLit(CanonicalRenderSql.NULL_CELL))), c, SqlExpr.Reason.JUDGE);
        }
        SqlExpr value = peer.wrapped() instanceof SqlSelect ps && !ps.projections().isEmpty()
                ? doubleValue(ps.projections().get(0), "w", peer.isFloat()) : new SqlExpr.NullLit();
        // pure has no null VALUE: the peer drops a NULL-value row like every
        // other side (an empty [] peer is one NULL row → zero cells)
        SqlExpr where = peer.wrapped() instanceof SqlSelect vs && !vs.projections().isEmpty()
                && vs.projections().get(0).alias() != null
                ? SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                        SqlExpr.Column.of("w", vs.projections().get(0).alias(),
                                SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED))
                : null;
        return rowsOf(c, value, lite ? liteOfCanon(c) : null, new SqlSource.Subselect(peer.wrapped(), "w", null),
                where);
    }

    /** The peer's cells chunked into rows of {@code width}: cells in
     * arrival order, grouped by {@code (rn - 1) - ((rn - 1) MOD width)}
     * (integer arithmetic on every dialect), each group joined by the
     * cell separator in cell order — the same framing
     * {@code TdsCompare.peerRowCanons} writes in Java. Reads the
     * {@code __peer} CTE ({@link #peerCells}). */
    private static SqlQuery peerRowCanons(PeerSide peer, int width) {
        return peerRowCanons(peer, width, false);
    }

    /** {@code lite}: each row also written, its cells' lite texts comma-joined (a grid's cells flattened). */
    private static SqlQuery peerRowCanons(PeerSide peer, int width, boolean lite) {
        SqlExpr rn = col("__peer", RN);
        SqlExpr rn0 = SqlExpr.Call.of(SqlFn.MINUS, rn, new SqlExpr.IntLit(1));
        SqlExpr group = SqlExpr.Call.of(SqlFn.MINUS, rn0,
                SqlExpr.Call.of(SqlFn.MOD, rn0, new SqlExpr.IntLit(width)));
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlExpr joined = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(col("__peer", C), new SqlExpr.StringLit(CanonicalRenderSql.TDS_CELL_SEP)),
                false, List.of(new SqlSelect.SortKey(rn, true, null, null)));
        SqlExpr first = new SqlAgg.Reducer(SqlAgg.Fn.MIN, List.of(rn), false, List.of());
        List<SqlSelect.Projection> ps = new ArrayList<>(List.of(
                new SqlSelect.Projection(joined, C, cOut),
                new SqlSelect.Projection(first, RN, rnOut)));
        List<OutputCol> outs = new ArrayList<>(List.of(cOut, rnOut));
        if (lite) {
            OutputCol wOut = new OutputCol(W, SqlType.Scalar.VARCHAR, true);
            ps.add(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                    List.of(liteOfCanon(col("__peer", C)), new SqlExpr.StringLit(", ")), false,
                    List.of(new SqlSelect.SortKey(rn, true, null, null))), W, wOut));
            outs.add(wOut);
        }
        return new SqlSelect(ps, false, cte("__peer"), null, List.of(group), null, null,
                List.of(), null, null, outs);
    }

    /** {@code SELECT <canon> AS __c, row_number() OVER () AS __rn, <value> AS __v
     * FROM (source) w} — {@code value} the cell's DOUBLE value when the cell
     * IS a Float (the leniency's operand), NULL otherwise. */
    private static SqlQuery rowsOf(SqlExpr canon, SqlQuery source) {
        return rowsOf(canon, new SqlExpr.NullLit(), source, null);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlQuery source,
            @com.legend.base.Nullable SqlExpr where) {
        return rowsOf(canon, value, new SqlSource.Subselect(source, "w", null), where);
    }

    /** A grid side read through its NAMED CTE, aliased {@code w} as every
     * cell and row reference spells it. A grid's rows and each of its
     * columns' cell branches read the one wrapped plan; inlined, the plan —
     * every formatted cell and the row canon — was spelled once per reader:
     * width + 1 copies per side (columnValueDifferenceTest, 8 columns: a
     * 1.2 MB statement; H2's parser, which copies the token list per
     * subquery, needed over 4 GB to read it — 2026-09-23). */
    private static SqlSource over(GridSide grid, String named) {
        return new SqlSource.Table(named, "w", grid.wrapped().outputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    /** {@link #rowsOf} with each row also WRITTEN ({@code w}, the message's text of it), or none. */
    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, @com.legend.base.Nullable SqlExpr w, SqlSource source,
            @com.legend.base.Nullable SqlExpr where) {
        SqlSelect rows = (SqlSelect) rowsOf(canon, value, source, where);
        if (w == null) {
            return rows;
        }
        OutputCol wOut = new OutputCol(W, SqlType.Scalar.VARCHAR, true);
        List<SqlSelect.Projection> ps = new ArrayList<>(rows.projections());
        ps.add(new SqlSelect.Projection(w, W, wOut));
        List<OutputCol> outs = new ArrayList<>(rows.outputs());
        outs.add(wOut);
        return new SqlSelect(ps, false, source, where, List.of(), null, null, List.of(), null, null, outs);
    }

    private static SqlQuery rowsOf(SqlExpr canon, SqlExpr value, SqlSource source,
            @com.legend.base.Nullable SqlExpr where) {
        SqlExpr rn = rowNumber(source.outputs());
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(canon, C, cOut),
                        new SqlSelect.Projection(rn, RN, rnOut),
                        new SqlSelect.Projection(value, V, vOut)),
                false, source, where,
                List.of(), null, null, List.of(), null, null,
                List.of(cOut, rnOut, vOut));
    }

    /** The rows of a source read as {@code w}, numbered: a grid side's by its ordinal (SideOrdinal: the order its plan
     *  defines, never its rows' arrival through the verdict's selects, which a database need not keep: MariaDB and
     *  Trino did not, 2026-10-20); any other source's by arrival. */
    private static SqlExpr rowNumber(List<OutputCol> outs) {
        List<SqlSelect.SortKey> by = outs.stream().anyMatch(o -> o.name().equals(SideOrdinal.ORD))
                ? List.of(new SqlSelect.SortKey(SqlExpr.Column.of("w", outs, SideOrdinal.ORD), true, null, null))
                : List.of();
        return new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), by, null);
    }

    /** The DOUBLE value of a projection when its type fact says DOUBLE,
     * else NULL (only Float pairs take the leniency). */
    private static SqlExpr doubleValue(SqlSelect.Projection p, String table, boolean isFloat) {
        return isFloat && p.alias() != null
                ? new SqlExpr.Cast(SqlExpr.Column.of(table, p.alias(), SqlType.Scalar.DOUBLE,
                        true, OutputCol.Origin.DERIVED), SqlType.Scalar.DOUBLE, SqlExpr.Reason.JUDGE)
                : new SqlExpr.NullLit();
    }

    /** A grid's cells ROW-MAJOR with their Float values: one row per cell,
     * {@code __rn = (row - 1) * width + i + 1} — the positional sequence
     * the leniency walks against the peer's cells. */
    private static SqlQuery gridCellsRowMajor(GridSide grid, String named) {
        OutputCol cOut = new OutputCol(C, SqlType.Scalar.VARCHAR, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        OutputCol vOut = new OutputCol(V, SqlType.Scalar.DOUBLE, true);
        List<SqlSelect.Projection> values = grid.wrapped() instanceof SqlSelect ws
                ? ws.projections().subList(0, Math.min(grid.width(), ws.projections().size()))
                : List.of();
        List<SqlExpr> cells = new ArrayList<>();
        List<SqlExpr> vs = new ArrayList<>();
        for (int i = 0; i < grid.width(); i++) {
            cells.add(gridCell(grid, i));
            boolean isFloat = i < grid.floatColumns().size() && grid.floatColumns().get(i);
            vs.add(i < values.size() ? doubleValue(values.get(i), "w", isFloat)
                    : new SqlExpr.NullLit());
        }
        return new SqlSelect(List.of(
                        new SqlSelect.Projection(pick(cells), C, cOut),
                        new SqlSelect.Projection(position(grid.width()), RN, rnOut),
                        new SqlSelect.Projection(pick(vs), V, vOut)),
                false, unpivot(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.of(cOut, rnOut, vOut));
    }

    // ── THE UNPIVOT (2026-09-23): a grid as one row per cell, reading the grid
    // ONCE. It was one UNION ALL branch per column, each re-reading the grid
    // and numbering its rows on its own: width references to the grid, which
    // H2 re-expands per reference while planning (an 8-column grid needed
    // gigabytes to plan), and width row numberings that had to agree on
    // arrival order. Now: the grid's rows numbered once (__r), cross-joined
    // with the column positions 1..width (k.__i); each output row picks its
    // cell by position.

    private static final String ROW = "__r";
    private static final String POS = "__i";

    /** {@code (SELECT w.*, ROW_NUMBER() OVER () AS __r FROM <grid> w) w
     * CROSS JOIN (VALUES 1, …, width) k(__i)}. */
    private static SqlSource unpivot(GridSide grid, String named) {
        List<OutputCol> outs = new ArrayList<>(grid.wrapped().outputs());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : grid.wrapped().outputs()) {
            ps.add(new SqlSelect.Projection(
                    SqlExpr.Column.of("w", grid.wrapped().outputs(), o.name()), o.name(), o));
        }
        OutputCol r = new OutputCol(ROW, SqlType.Scalar.BIGINT, false);
        ps.add(new SqlSelect.Projection(rowNumber(grid.wrapped().outputs()), ROW, r));
        outs.add(r);
        SqlSource rows = new SqlSource.Subselect(new SqlSelect(ps, false, over(grid, named), null,
                List.of(), null, null, List.of(), null, null, List.copyOf(outs)), "w", null);
        List<List<SqlExpr>> positions = new ArrayList<>();
        for (int p = 1; p <= grid.width(); p++) {
            positions.add(List.of(new SqlExpr.IntLit(p)));
        }
        SqlSource k = new SqlSource.Values(positions, List.of(POS), "k",
                List.of(new OutputCol(POS, SqlType.Scalar.INTEGER, false)));
        return new SqlSource.Join(rows, k, SqlSource.Join.Kind.CROSS, null);
    }

    private static SqlExpr pos() {
        return SqlExpr.Column.of("k", List.of(new OutputCol(POS, SqlType.Scalar.INTEGER, false)), POS);
    }

    /** {@code CASE WHEN k.__i = 1 THEN e1 … END}: the cell at this row's position
     * (NULL when every candidate is NULL). */
    private static SqlExpr pick(List<SqlExpr> perColumn) {
        if (perColumn.stream().allMatch(e -> e instanceof SqlExpr.NullLit)) {
            return new SqlExpr.NullLit();
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (int i = 0; i < perColumn.size(); i++) {
            whens.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, pos(), new SqlExpr.IntLit(i + 1)), perColumn.get(i)));
        }
        return new SqlExpr.Case(whens, null, SqlExpr.Reason.JUDGE);
    }

    /** {@code (w.__r - 1) * width + k.__i}: the cell's row-major position. */
    private static SqlExpr position(int width) {
        SqlExpr r = SqlExpr.Column.of("w", ROW, SqlType.Scalar.BIGINT, false, OutputCol.Origin.DERIVED);
        return SqlExpr.Call.of(SqlFn.PLUS,
                SqlExpr.Call.of(SqlFn.TIMES,
                        SqlExpr.Call.of(SqlFn.MINUS, r, new SqlExpr.IntLit(1)),
                        new SqlExpr.IntLit(width)),
                pos());
    }

    private static List<OutputCol> cteOutputs() {
        return List.of(new OutputCol(C, SqlType.Scalar.VARCHAR, true),
                new OutputCol(RN, SqlType.Scalar.BIGINT, false),
                new OutputCol(V, SqlType.Scalar.DOUBLE, true),
                new OutputCol(W, SqlType.Scalar.VARCHAR, true),
                new OutputCol(O, SqlType.Scalar.VARCHAR, true));
    }

    private static SqlSource cte(String name) {
        return new SqlSource.Table(name, name, cteOutputs(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
    }

    private static SqlExpr col(String cteName, String col) {
        return SqlExpr.Column.of(cteName, cteOutputs(), col);
    }

    /** {@code (SELECT <agg> FROM cte)} as a scalar. */
    private static SqlExpr scalarOver(String cteName, SqlExpr projected,
            String alias, SqlType type, @com.legend.base.Nullable Long limit) {
        OutputCol out = new OutputCol(alias, type, true);
        return new SqlExpr.ScalarSubquery(new SqlSelect(
                List.of(new SqlSelect.Projection(projected, alias, out)),
                false, cte(cteName), null, List.of(), null, null,
                limit == null ? List.of()
                        : List.of(new SqlSelect.SortKey(col(cteName, RN), true, null, null)),
                limit, null, List.of(out)));
    }

    private static SqlExpr count(String cteName) {
        return scalarOver(cteName,
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(col(cteName, RN)),
                        false, List.of()),
                "__n", SqlType.Scalar.BIGINT, null);
    }

    /** {@code (SELECT count(*) FROM cte WHERE strpos(__c, marker) > 0) > 0}. */
    /** {@code (SELECT count(*) FROM cte WHERE __c IS NULL) > 0}. */
    /** The spec's side framing in SQL. */
}
