// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.TypedSide;
import com.legend.error.AssertSentence;

import java.util.List;

import static com.legend.testexec.HostJudgments.liteRows;
import static com.legend.testexec.HostJudgments.sentence;
import static com.legend.testexec.HostJudgments.typeName;

/**
 * THE PRODUCT'S FORM OF {@link HostWords} (census J29, 2026-11-18): lite's own sentences, the {@link AssertSentence}
 * table both judges share, each slot written as the product writes a value ({@link LiteWords}).
 */
public final class LiteHostWords implements HostWords {

    public static final LiteHostWords INSTANCE = new LiteHostWords();

    private LiteHostWords() {
    }

    /** The name a plan worded in lite's sentences carries. */
    public static final String NAME = com.legend.testexecutionplan.TestPlan.LITE_WORDS;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String rowValuesDiffer(List<Object> e, List<Object> a, List<Equality.Typed> te, List<Equality.Typed> ta,
            boolean incidental) {
        // an incidental order compares the rows as a multiset: "in any order"
        return incidental ? sameElements(e, a, null, null)
                : sentence(AssertSentence.EQUALS, "expected", LiteWords.pairSide(e, a, null), "actual",
                        LiteWords.pairSide(a, e, null));
    }

    @Override
    public String notEqual(List<Object> e, List<Object> a, @Nullable String aClass) {
        return sentence(AssertSentence.NOT_EQUALS, "actual", LiteWords.pairSide(a, e, aClass));
    }

    @Override
    public String sameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass) {
        // each side's values in its own order: "in any order" is the sentence's (the orchestrator's ruling, 2026-11-17)
        return sentence(AssertSentence.SAME_ELEMENTS, "expected", LiteWords.values(e, eClass), "actual",
                LiteWords.values(a, aClass));
    }

    @Override
    public String cellMultisetDiffers(String family, List<Object> e, List<Object> a, HostRules rules) {
        return sameElements(e, a, null, null);
    }

    @Override
    public String notEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta,
            @Nullable String cellDiff) {
        return sentence(AssertSentence.TDS_EQUIVALENT, "expected", liteRows(one), "delta", LiteWords.value(delta),
                "actual", liteRows(two));
    }

    @Override
    public String grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual) {
        return wantEqual ? sentence(AssertSentence.EQUALS, "expected", liteRows(te), "actual", liteRows(ta))
                : sentence(AssertSentence.NOT_EQUALS, "actual", liteRows(ta));
    }

    @Override
    public String size(@Nullable Object expected, long actual) {
        return sentence(AssertSentence.SIZE, "expected", LiteWords.value(expected), "actual", Long.toString(actual));
    }

    @Override
    public String notContained(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass) {
        return sentence(AssertSentence.CONTAINS, "collection", LiteWords.values(collection, collectionClass), "value",
                LiteWords.value(value, valueClass));
    }

    @Override
    public String notWithin(Number expected, Number actual, Number delta) {
        return sentence(AssertSentence.WITHIN_TOLERANCE, "expected", LiteWords.value(expected), "delta",
                LiteWords.value(delta), "actual", LiteWords.value(actual));
    }

    @Override
    public String negated(String kind, List<Object> first, List<Object> second) {
        return switch (kind) {
            case "NOT_EQ" -> sentence(AssertSentence.NOT_EQUALS, "actual", LiteWords.side(second, null));
            case "IS_NOT" -> sentence(AssertSentence.IS_NOT, "actual", LiteWords.side(second, null));
            // the sides are the collection's, then the value's (planContains)
            case "NOT_CONTAINS" -> sentence(AssertSentence.NOT_CONTAINS, "collection", LiteWords.values(first, null),
                    "value", LiteWords.side(second, null));
            // the sides are the size's, then the collection's (planSize)
            case "NOT_SIZE" -> sentence(AssertSentence.NOT_SIZE, "size", LiteWords.side(first, null));
            default -> throw new IllegalStateException("a negated assert of kind '" + kind + "'");
        };
    }

    @Override
    public String condition(boolean wantTrue) {
        return (wantTrue ? AssertSentence.TRUE : AssertSentence.FALSE).fill(java.util.Map.of());
    }

    @Override
    public String emptiness(boolean wantEmpty, TypedSide side, List<Object> values) {
        return wantEmpty ? sentence(AssertSentence.EMPTY, "collection", side instanceof TypedSide.Tabular t
                ? liteRows(t) : LiteWords.values(values, null))
                : AssertSentence.NOT_EMPTY.fill(java.util.Map.of());
    }

    @Override
    public String notEq(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass,
            @Nullable String eDeclared, @Nullable String aDeclared) {
        // each side's kind the one it was compared under: its declared numeric kind, else its carrier's (PR #118 fix
        // round, 2026-11-18: a Decimal property over an INTEGER column is a Decimal, as the database judge names it)
        return LiteWords.equalsSentence(LiteWords.identity(e, eClass), LiteWords.identity(a, aClass),
                () -> LiteWords.kind(e, eDeclared, eClass), () -> LiteWords.kind(a, aDeclared, aClass), e, a);
    }

    @Override
    public String notInstanceOfClass(@Nullable Object v, String wireType, String type) {
        return sentence(AssertSentence.INSTANCE_OF, "type", typeName(type), "actual", LiteWords.value(v, wireType));
    }

    @Override
    public String notInstanceOf(@Nullable Object v, String type, String carrier) {
        return sentence(AssertSentence.INSTANCE_OF, "type", typeName(type), "actual", LiteWords.value(v));
    }

    @Override
    public String notSameInstance(String name) {
        return AssertSentence.SAME_INSTANCE.fill(java.util.Map.of());
    }

    @Override
    public String atIndex(String sentence, int index) {
        // lite's sentence names the element by Pure's zero-based index (the user's direction, 2026-11-17)
        return sentence(AssertSentence.AT_INDEX, "sentence", sentence, "index", Integer.toString(index));
    }

    @Override
    public String rowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass,
            @Nullable String aClass) {
        return LiteWords.equalsSentence(LiteWords.value(e, eClass), LiteWords.value(a, aClass),
                () -> LiteWords.kind(e, eClass), () -> LiteWords.kind(a, aClass), e, a);
    }

    @Override
    public String sidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass) {
        // a class side carried as JSON text is written as that JSON (the shared canon's, ruling 5)
        if (LiteWords.jsonCarried(eCarried) || LiteWords.jsonCarried(aCarried)) {
            return LiteWords.equalsSentence(LiteWords.carriedSide(eCarried, aCarried, eClass),
                    LiteWords.carriedSide(aCarried, eCarried, aClass), () -> "", () -> "", null, null);
        }
        return LiteWords.equalsSentence(LiteWords.pairSide(e, a, eClass), LiteWords.pairSide(a, e, aClass),
                () -> LiteWords.sideKind(e, eClass), () -> LiteWords.sideKind(a, aClass),
                e.size() == 1 ? e.get(0) : null, a.size() == 1 ? a.get(0) : null);
    }

    @Override
    public String jsonDiffers(String expected, String actual, boolean incidentalRoot, String diff) {
        return sentence(AssertSentence.JSON_EQUALS, "expected", LiteWords.canonJson(expected, incidentalRoot),
                "actual", LiteWords.canonJson(actual, incidentalRoot));
    }

    @Override
    public String noError(String expected) {
        return sentence(AssertSentence.NO_ERROR, "expected", LiteWords.quoted(expected));
    }

    @Override
    public String errorDiffers(String expected, String actual) {
        return sentence(AssertSentence.ERROR, "expected", LiteWords.quoted(expected), "actual",
                LiteWords.quoted(actual));
    }

    @Override
    public String lineDiffers(long expected, long actual) {
        return sentence(AssertSentence.ERROR_LINE, "expected", Long.toString(expected), "actual",
                Long.toString(actual));
    }

    @Override
    public String columnDiffers(long expected, long actual) {
        return sentence(AssertSentence.ERROR_COLUMN, "expected", Long.toString(expected), "actual",
                Long.toString(actual));
    }

    @Override
    public String unjudged(String expected, String actual) {
        return " (expected " + expected + ", got " + actual + ")";
    }

    /** Lite's words of a caught error: its own message (K10). */
    @Override
    public String caught(RuntimeException error, EdgeHearing hearing) {
        return String.valueOf(error.getMessage());
    }
}
