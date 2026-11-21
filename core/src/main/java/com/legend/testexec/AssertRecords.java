// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.TypedSide;
import com.legend.error.AssertKind;
import com.legend.error.ErrorRecord;
import com.legend.error.FrameKind;

import java.util.List;

import static com.legend.testexec.HostJudgments.liteRows;
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

    static ErrorRecord notEqual(List<Object> e, List<Object> a, @Nullable String aClass) {
        return record(AssertKind.NOT_EQUALS, "actual", LiteWords.pairSide(a, e, aClass));
    }

    static ErrorRecord sameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass) {
        // each side's values in its own order: "in any order" is the sentence's (the orchestrator's ruling, 2026-11-17)
        return record(AssertKind.SAME_ELEMENTS, "expected", LiteWords.values(e, eClass), "actual",
                LiteWords.values(a, aClass));
    }

    static ErrorRecord notEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta) {
        return record(AssertKind.TDS_EQUIVALENT, "expected", liteRows(one), "delta", LiteWords.value(delta),
                "actual", liteRows(two));
    }

    static ErrorRecord grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual) {
        return wantEqual ? record(AssertKind.EQUALS, "expected", liteRows(te), "actual", liteRows(ta))
                : record(AssertKind.NOT_EQUALS, "actual", liteRows(ta));
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

    static ErrorRecord emptiness(boolean wantEmpty, TypedSide side, List<Object> values) {
        return wantEmpty ? record(AssertKind.EMPTY, "collection", side instanceof TypedSide.Tabular t
                ? liteRows(t) : LiteWords.values(values, null))
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

    /** An element's failure inside the frame naming it by Pure's zero-based index (the user's direction,
     *  2026-11-17): a record of its own, framed, never a sentence inside a sentence. */
    static ErrorRecord atIndex(ErrorRecord element, int index) {
        return element.framed(new ErrorRecord.Frame(FrameKind.AT_INDEX, List.of(Integer.toString(index))));
    }

    static ErrorRecord rowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass,
            @Nullable String aClass) {
        return LiteWords.equalsRecord(LiteWords.value(e, eClass), LiteWords.value(a, aClass),
                () -> LiteWords.kind(e, eClass), () -> LiteWords.kind(a, aClass), e, a);
    }

    static ErrorRecord sidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass) {
        // a class side carried as JSON text is written as that JSON (the shared canon's, ruling 5)
        if (LiteWords.jsonCarried(eCarried) || LiteWords.jsonCarried(aCarried)) {
            return LiteWords.equalsRecord(LiteWords.carriedSide(eCarried, aCarried, eClass),
                    LiteWords.carriedSide(aCarried, eCarried, aClass), () -> "", () -> "", null, null);
        }
        return LiteWords.equalsRecord(LiteWords.pairSide(e, a, eClass), LiteWords.pairSide(a, e, aClass),
                () -> LiteWords.sideKind(e, eClass), () -> LiteWords.sideKind(a, aClass),
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
