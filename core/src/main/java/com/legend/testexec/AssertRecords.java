// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.TypedSide;
import com.legend.error.AssertKind;
import com.legend.error.ErrorRecord;
import com.legend.error.FrameKind;

import java.util.List;

import static com.legend.testexec.HostJudgments.record;
import static com.legend.testexec.HostJudgments.typeName;

/**
 * THE HOST JUDGE'S FAILED ASSERTS, AS RECORDS (item 3 of the compatibility program, 2026-11-21: assert verdicts as
 * records; the user's Q15 = A, "lite's own assert-failure words on every route"): one method per failure the host judge
 * decides, each the {@link AssertKind} record of the values it holds -- the catalog words it at the last step, the same
 * on every route. Each value is written by lite's value printer ({@link LiteWords}); a method states a failure, it never
 * decides one. Until item 3 the host judge's words had two forms behind {@code HostWords}: this one
 * ({@code LiteHostWords}) and legend-pure's at the compatibility edge ({@code EdgeHostWords}), both deleted.
 */
final class AssertRecords {

    private AssertRecords() {
    }

    static ErrorRecord rowValuesDiffer(List<Object> e, List<Object> a, boolean incidental) {
        // an incidental order compares the rows as a multiset: "in any order"
        return incidental ? sameElements(e, a, null, null)
                : record(AssertKind.EQUALS, "expected", LiteWords.pairSide(e, a, null), "actual",
                        LiteWords.pairSide(a, e, null));
    }

    /** THE ONE CHOICE of an equality's failure record (item 3's S4, 2026-11-22, the user: "exactly one translation
     *  from error to English"): both judges call it, the host over the values it read and the database judge's run over
     *  the values the database returned. {@code incidental}: the order did not count ("in any order"); the carried
     *  sides are the values as they arrived (a class side's JSON carrier); {@code ef}, {@code af}: each side's facts,
     *  its class, and its kind -- a primitive side's values are never a JSON carrier, so a String whose text starts
     *  with a bracket is written as the String it is (the S4 audit's HIGH 1, 2026-11-22: until then any String text
     *  starting '[' or '{' was written as minified JSON, on both judges). */
    static ErrorRecord equality(boolean incidental, boolean wantEqual, List<Object> e, List<Object> a,
            List<Object> eCarried, List<Object> aCarried, com.legend.testexecutionplan.HostJudgment.@Nullable SideFacts ef,
            com.legend.testexecutionplan.HostJudgment.@Nullable SideFacts af) {
        @Nullable String eClass = ef == null ? null : ef.classFqn();
        @Nullable String aClass = af == null ? null : af.classFqn();
        if (!wantEqual) {
            return notEqual(e, a, aClass);
        }
        return incidental ? sameElements(e, a, eClass, aClass)
                : sidesNotEqual(e, a, carrier(ef, eCarried), carrier(af, aCarried), eClass, aClass,
                        ef == null ? null : ef.declared(), af == null ? null : af.declared());
    }

    /** A side's values as a JSON carrier may have brought them: none for a side of a primitive kind. */
    private static List<Object> carrier(com.legend.testexecutionplan.HostJudgment.@Nullable SideFacts f,
            List<Object> carried) {
        return f != null && f.kind() instanceof com.legend.testexecutionplan.KindClass.Primitive ? List.of() : carried;
    }

    /**
     * A failed TYPED verdict row's record (item 3's S4, 2026-11-21, the user's Q-A): {@code row} as the database
     * returned it, {@code [verdict, expected, actual]}, each side a list in its own order (an empty side's NULL); its
     * {@code report} names the family, whose record is chosen here as the host judge chooses it -- a caller's literal
     * message in its place, an unrolled element framed at its index.
     */
    static ErrorRecord ofTypedRow(com.legend.testexecutionplan.RootLoop.Report report, List<Object> row) {
        if (!Boolean.FALSE.equals(row.get(0))) {
            throw new IllegalStateException("a failed typed row's verdict is false, not " + row.get(0));
        }
        List<Object> e = row.size() > 1 ? typedSide(row.get(1)) : List.of();
        List<Object> a = row.size() > 2 ? typedSide(row.get(2)) : List.of();
        @Nullable String eClass = report.e() == null ? null : report.e().classFqn();
        @Nullable String aClass = report.a() == null ? null : report.a().classFqn();
        ErrorRecord failure = report.fixed() != null ? report.fixed() : switch (report.family()) {
            // a typed row's values are typed, never a JSON carrier's
            case EQUALS -> equality(false, report.want(), e, a, List.of(), List.of(), report.e(), report.a());
            case SAME_ELEMENTS -> equality(true, report.want(), e, a, List.of(), List.of(), report.e(), report.a());
            case IS_NOT -> negated("IS_NOT", e, a);
            case CONDITION -> condition(report.want());
            case SIZE -> report.want() ? size(row.get(1), ((Number) row.get(2)).longValue())
                    : negated("NOT_SIZE", e, a);
            case EMPTY -> emptiness(report.want(), null, e);
            case CONTAINS -> report.want() ? notContained(e, a.isEmpty() ? null : a.get(0), eClass, aClass)
                    : negated("NOT_CONTAINS", e, a);
            case TOLERANCE -> notWithin((Number) row.get(1), (Number) row.get(2), (Number) row.get(3));
            case PER_ELEMENT -> atIndex(rowNotEqual(row.get(1), row.get(2), eClass, aClass),
                    ((Number) row.get(3)).intValue());
            case TDS_EQUIVALENT -> notEquivalent(gridRows(row.get(1)), gridRows(row.get(2)), row.get(3));
            case GRIDS -> grids(gridRows(row.get(1)), gridRows(row.get(2)), report.want());
        };
        return report.index() >= 0 ? atIndex(failure, report.index()) : failure;
    }

    /** A typed row's side: the list the database wrote (an empty side's none, written NULL); one value its own. */
    private static List<Object> typedSide(@Nullable Object v) {
        if (v == null) {
            return List.of();
        }
        if (!(v instanceof List<?> l)) {
            return List.of(v);
        }
        return java.util.Collections.unmodifiableList(new java.util.ArrayList<>(l));
    }

    static ErrorRecord notEqual(List<Object> e, List<Object> a, @Nullable String aClass) {
        return record(AssertKind.NOT_EQUALS, "actual", LiteWords.pairSide(a, e, aClass));
    }

    static ErrorRecord sameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass) {
        // each side's values in its own order: "in any order" is the sentence's (the orchestrator's ruling, 2026-11-17)
        return record(AssertKind.SAME_ELEMENTS, "expected", LiteWords.values(e, eClass), "actual",
                LiteWords.values(a, aClass));
    }

    static ErrorRecord notEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta) {
        return notEquivalent(one.rows().stream().map(com.legend.testexecutionplan.Row::values).toList(),
                two.rows().stream().map(com.legend.testexecutionplan.Row::values).toList(), delta);
    }

    /** {@code assertTdsEquivalent}'s failure over each grid's rows, each row its cells in order (both judges: the
     *  host's grids, the database judge's typed row structs, item 3's S4). */
    static ErrorRecord notEquivalent(List<? extends List<?>> one, List<? extends List<?>> two, @Nullable Object delta) {
        return record(AssertKind.TDS_EQUIVALENT, "expected", rowsText(one), "delta", LiteWords.value(delta),
                "actual", rowsText(two));
    }

    /** A grid's rows written as the host writes them: each row its cells, bracketed, comma-joined. */
    private static String rowsText(List<? extends List<?>> rows) {
        List<String> out = new java.util.ArrayList<>(rows.size());
        for (List<?> r : rows) {
            out.add(LiteWords.values(r, null));
        }
        return "[" + String.join(", ", out) + "]";
    }

    /** A typed row's grid: its list of row structs (each a map of its cells in order), as rows of cells; none where
     *  the grid has no row. */
    private static List<List<Object>> gridRows(@Nullable Object v) {
        if (v == null) {
            return List.of();
        }
        List<List<Object>> out = new java.util.ArrayList<>();
        for (Object row : (List<?>) v) {
            out.add(new java.util.ArrayList<>(((java.util.Map<?, ?>) row).values()));
        }
        return out;
    }

    static ErrorRecord grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual) {
        return grids(te.rows().stream().map(com.legend.testexecutionplan.Row::values).toList(),
                ta.rows().stream().map(com.legend.testexecutionplan.Row::values).toList(), wantEqual);
    }

    /** Two grids' failure over each grid's rows, each row its cells in order (both judges: the host's grids, the
     *  database judge's typed row structs, item 3's S4). */
    static ErrorRecord grids(List<? extends List<?>> e, List<? extends List<?>> a, boolean wantEqual) {
        return wantEqual ? record(AssertKind.EQUALS, "expected", rowsText(e), "actual", rowsText(a))
                : record(AssertKind.NOT_EQUALS, "actual", rowsText(a));
    }

    static ErrorRecord size(@Nullable Object expected, long actual) {
        return record(AssertKind.SIZE, "expected", LiteWords.value(expected), "actual", Long.toString(actual));
    }

    static ErrorRecord notContained(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass) {
        return record(AssertKind.CONTAINS, "collection", LiteWords.values(collection, collectionClass), "value",
                LiteWords.value(value, valueClass));
    }

    static ErrorRecord notWithin(Number expected, Number actual, Number delta) {
        return record(AssertKind.WITHIN_TOLERANCE, "expected", LiteWords.value(expected), "delta",
                LiteWords.value(delta), "actual", LiteWords.value(actual));
    }

    static ErrorRecord negated(String kind, List<Object> first, List<Object> second) {
        return switch (kind) {
            case "NOT_EQ" -> record(AssertKind.NOT_EQUALS, "actual", LiteWords.side(second, null));
            case "IS_NOT" -> record(AssertKind.IS_NOT, "actual", LiteWords.side(second, null));
            // the sides are the collection's, then the value's (planContains)
            case "NOT_CONTAINS" -> record(AssertKind.NOT_CONTAINS, "collection", LiteWords.values(first, null),
                    "value", LiteWords.side(second, null));
            // the sides are the size's, then the collection's (planSize)
            case "NOT_SIZE" -> record(AssertKind.NOT_SIZE, "size", LiteWords.side(first, null));
            default -> throw new IllegalStateException("a negated assert of kind '" + kind + "'");
        };
    }

    static ErrorRecord condition(boolean wantTrue) {
        return (wantTrue ? AssertKind.TRUE : AssertKind.FALSE).record();
    }

    static ErrorRecord emptiness(boolean wantEmpty, @Nullable TypedSide side, List<Object> values) {
        return wantEmpty ? record(AssertKind.EMPTY, "collection", side instanceof TypedSide.Tabular t
                ? rowsText(t.rows().stream().map(com.legend.testexecutionplan.Row::values).toList()) : LiteWords.values(values, null))
                : AssertKind.NOT_EMPTY.record();
    }

    static ErrorRecord notEq(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass,
            @Nullable String eDeclared, @Nullable String aDeclared) {
        // each side's kind the one it was compared under: its declared numeric kind, else its carrier's (PR #118 fix
        // round, 2026-11-18: a Decimal property over an INTEGER column is a Decimal, as the database judge names it)
        return LiteWords.equalsRecord(LiteWords.identity(e, eClass), LiteWords.identity(a, aClass),
                () -> comparedKind(e, eDeclared, eClass), () -> comparedKind(a, aDeclared, aClass), e, a);
    }

    /** {@code v}'s kind as the host judge compared it ({@link Equality#declaredNumeric}): the numeric kind its
     *  declaration names where {@code v} is a number, else its carrier's ({@link LiteWords#kind}). */
    private static String comparedKind(@Nullable Object v, @Nullable String declared, @Nullable String cls) {
        // compared, not switched: an enum switch would be a class of its own reaching the judge's kinds (V3)
        Equality.Kind k = v instanceof Number ? Equality.declaredNumeric(declared) : null;
        if (k == Equality.Kind.INTEGER) {
            return "Integer";
        }
        if (k == Equality.Kind.FLOAT) {
            return "Float";
        }
        if (k == Equality.Kind.DECIMAL) {
            return "Decimal";
        }
        return LiteWords.kind(v, cls);   // no numeric declaration (declaredNumeric names those three, else null)
    }

    static ErrorRecord notInstanceOfClass(@Nullable Object v, String wireType, String type) {
        return record(AssertKind.INSTANCE_OF, "type", typeName(type), "actual", LiteWords.value(v, wireType));
    }

    static ErrorRecord notInstanceOf(@Nullable Object v, String type) {
        return record(AssertKind.INSTANCE_OF, "type", typeName(type), "actual", LiteWords.value(v));
    }

    static ErrorRecord notSameInstance() {
        return AssertKind.SAME_INSTANCE.record();
    }

    /** {@code failed}, an unrolled element's failure, framed at the element {@code index} names (both judges: the
     *  host's {@code HostJudgments.unrolledOf}, the database judge's {@code RootLoops}): its record in the frame; a
     *  decline, a failure with no record (the edge's appeal words) or another error, as it is. */
    static RuntimeException atIndex(RuntimeException failed, int index) {
        if (failed instanceof com.legend.error.AssertFailed af && af.unjudgedReason() == null && af.record() != null
                && index >= 0) {
            return new com.legend.error.AssertFailed(atIndex(af.record(), index));
        }
        return failed;
    }

    /** An element's failure inside the frame naming it by Pure's zero-based index (the user's direction,
     *  2026-11-17): a record of its own, framed, never a sentence inside a sentence. A caller's own message is the
     *  failure whole, unframed, as upstream raises it (the element's {@code assert} raises the message it was given;
     *  item 3's S4 step 3, decision 4, 2026-11-22). */
    static ErrorRecord atIndex(ErrorRecord element, int index) {
        if (element.code() == AssertKind.MESSAGE) {
            return element;
        }
        return element.framed(new ErrorRecord.Frame(FrameKind.AT_INDEX, List.of(Integer.toString(index))));
    }

    static ErrorRecord rowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass,
            @Nullable String aClass) {
        return LiteWords.equalsRecord(LiteWords.value(e, eClass), LiteWords.value(a, aClass),
                () -> LiteWords.kind(e, eClass), () -> LiteWords.kind(a, aClass), e, a);
    }

    /** {@code eDeclared}, {@code aDeclared}: each side's declared type, where known; a single number's kind is named
     *  as it was compared, by its declaration (a Decimal property read off an INTEGER column is a Decimal), as
     *  {@link #notEq} names it (the S4 kinds round, 2026-11-22: the database judge's typed row named it by the value's
     *  carrier where the host's assertEq named the declaration). */
    static ErrorRecord sidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass, @Nullable String eDeclared, @Nullable String aDeclared) {
        // a class side carried as JSON text is written as that JSON (the shared canon's, ruling 5)
        if (LiteWords.jsonCarried(eCarried) || LiteWords.jsonCarried(aCarried)) {
            return LiteWords.equalsRecord(LiteWords.carriedSide(eCarried, aCarried, eClass),
                    LiteWords.carriedSide(aCarried, eCarried, aClass), () -> "", () -> "", null, null);
        }
        return LiteWords.equalsRecord(LiteWords.pairSide(e, a, eClass), LiteWords.pairSide(a, e, aClass),
                () -> e.size() == 1 ? comparedKind(e.get(0), eDeclared, eClass) : LiteWords.sideKind(e, eClass),
                () -> a.size() == 1 ? comparedKind(a.get(0), aDeclared, aClass) : LiteWords.sideKind(a, aClass),
                e.size() == 1 ? e.get(0) : null, a.size() == 1 ? a.get(0) : null);
    }

    static ErrorRecord jsonDiffers(String expected, String actual, boolean incidentalRoot) {
        return record(AssertKind.JSON_EQUALS, "expected", LiteWords.canonJson(expected, incidentalRoot),
                "actual", LiteWords.canonJson(actual, incidentalRoot));
    }

    static ErrorRecord noError(String expected) {
        return record(AssertKind.NO_ERROR, "expected", LiteWords.quoted(expected));
    }

    static ErrorRecord errorDiffers(String expected, String actual) {
        return record(AssertKind.ERROR, "expected", LiteWords.quoted(expected), "actual",
                LiteWords.quoted(actual));
    }

    static ErrorRecord lineDiffers(long expected, long actual) {
        return record(AssertKind.ERROR_LINE, "expected", Long.toString(expected), "actual",
                Long.toString(actual));
    }

    static ErrorRecord columnDiffers(long expected, long actual) {
        return record(AssertKind.ERROR_COLUMN, "expected", Long.toString(expected), "actual",
                Long.toString(actual));
    }
}
