// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.testexec.Equality;
import com.legend.testexec.HostRules;
import com.legend.testexec.PureAsserts;
import com.legend.testexecutionplan.TypedSide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LEGEND-PURE'S WORDS OF A FAILED ASSERT, THE HOST JUDGE'S (census J29, 2026-11-18): the compatibility edge's form of
 * {@link com.legend.testexec.HostWords} -- the assert files' own messages (assertEquals.pure, assertSameElements.pure,
 * assertError.pure, ...) and the platform's texts around them, each value by {@code toRepresentation}
 * ({@link EdgePureWords}). The decisions stay the product's; these only word them.
 */
public final class EdgeHostWords implements com.legend.testexec.HostWords {

    public static final EdgeHostWords INSTANCE = new EdgeHostWords();

    /** The name a plan worded in legend-pure's words carries. */
    public static final String NAME = "legend-pure";

    private static final EdgePureWords W = EdgePureWords.INSTANCE;

    private EdgeHostWords() {
    }

    @Override
    public String name() {
        return NAME;
    }

    private static List<Object> values(List<Equality.Typed> typed) {
        List<Object> out = new ArrayList<>(typed.size());
        typed.forEach(t -> out.add(t.value()));
        return out;
    }

    private static @Nullable Object one(List<Object> side, String what) {
        if (side.size() != 1) {
            throw new IllegalStateException(what + " must be one value, got " + side.size());
        }
        return side.get(0);
    }

    /** The synthetic class a keyless instance's wire map carries, or null. */
    private static @Nullable String wireType(@Nullable Object v) {
        return v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE) instanceof String t ? t : null;
    }

    /** A frame's columns and row count (the spec's toString(true) is message position only). */
    private static String summarize(TypedSide.Tabular t) {
        return t.columns().stream().map(com.legend.testexecutionplan.Column::name).toList() + " ("
                + t.rows().size() + " rows)";
    }

    private static List<List<Object>> gridRows(TypedSide.Tabular t) {
        List<List<Object>> rows = new ArrayList<>(t.rows().size());
        for (com.legend.testexecutionplan.Row r : t.rows()) {
            rows.add(r.values());
        }
        return rows;
    }

    @Override
    public String rowValuesDiffer(List<Object> e, List<Object> a, List<Equality.Typed> te, List<Equality.Typed> ta,
            boolean incidental) {
        // upstream's own '\nexpected: %r\nactual:   %r' over the typed grids (the database judge's words too)
        return W.equals(values(te), values(ta), null, null);
    }

    @Override
    public String notEqual(List<Object> e, List<Object> a, @Nullable String aClass) {
        return W.notEquals(e, a);
    }

    @Override
    public String sameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass) {
        // assertSameElements.pure: assertEquals(e->sort(), a->sort()), both sides sorted
        return W.sameElements(PureAsserts.sorted(e), PureAsserts.sorted(a), eClass, aClass);
    }

    @Override
    public String cellMultisetDiffers(String family, List<Object> e, List<Object> a, HostRules rules) {
        return PureAsserts.holds(PureAsserts.sorted(e), PureAsserts.sorted(a), rules)
                ? family + " (TDSRow.values): cell multiset differs" : sameElements(e, a, null, null);
    }

    @Override
    public String notEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta,
            @Nullable String cellDiff) {
        return cellDiff != null ? cellDiff : "\n" + summarize(one) + "\n is not equivalent to:\n" + summarize(two);
    }

    @Override
    public String grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual) {
        return W.grids(gridRows(te), gridRows(ta), wantEqual);
    }

    @Override
    public String size(@Nullable Object expected, long actual) {
        return W.size(expected, actual);
    }

    @Override
    public String notContained(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass) {
        return W.notContains(collection, value, collectionClass, valueClass);
    }

    @Override
    public String notWithin(Number expected, Number actual, Number delta) {
        return W.pair(expected, null, actual, null);
    }

    @Override
    public String negated(String kind, List<Object> first, List<Object> second) {
        // the prelude's bodies these natives replace: assertNotEq '%r should not equal %r', assertIsNot 'Should not be
        // identical to: %r', assertNotContains '%s should not contain %r', assertNotSize 'size should not equal: %s'
        return switch (kind) {
            case "NOT_EQ" -> W.notEquals(first, second);
            case "IS_NOT" -> W.identical(one(second, "assertIsNot actual"));
            case "NOT_CONTAINS" -> W.contains(first, one(second, "assertNotContains value"));
            case "NOT_SIZE" -> "size should not equal: " + one(first, "assertNotSize size");
            default -> throw new IllegalStateException("a negated assert of kind '" + kind + "'");
        };
    }

    @Override
    public String condition(boolean wantTrue) {
        return "Assert failed";
    }

    @Override
    public String emptiness(boolean wantEmpty, TypedSide side, List<Object> values) {
        return wantEmpty ? W.notEmpty(values) : "Expected non-empty collection";
    }

    @Override
    public String notEq(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass) {
        // assertEq.pure: '\nexpected: %r\nactual:   %r', an instance as %r writes it (<id instanceOf Class>)
        return W.pair(e, wireType(e), a, wireType(a));
    }

    @Override
    public String notInstanceOfClass(@Nullable Object v, String wireType, String type) {
        return W.notInstanceOf(v, wireType, type, wireType);
    }

    @Override
    public String notInstanceOf(@Nullable Object v, String type, String carrier) {
        return W.notInstanceOf(v, null, PureAsserts.bare(type), carrier);
    }

    @Override
    public String notSameInstance(String name) {
        return name + ": the element references differ";
    }

    @Override
    public String atIndex(String sentence, int index) {
        // legend-pure names no element: the element's own failure
        return sentence;
    }

    @Override
    public String rowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass,
            @Nullable String aClass) {
        return W.equals(Collections.singletonList(e), Collections.singletonList(a), eClass, aClass);
    }

    @Override
    public String sidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass) {
        return W.equals(e, a, eClass, aClass);
    }

    @Override
    public String jsonDiffers(String expected, String actual, boolean incidentalRoot, String diff) {
        return "assertJsonStringsEqual: FIRST DIFF at " + diff;
    }

    @Override
    public String noError(String expected) {
        // interpreted AssertError.java: PureAssertFail("No error was thrown")
        return "No error was thrown";
    }

    @Override
    public String errorDiffers(String expected, String actual) {
        // assertError.pure:24, the /4 body's assertEquals format, verbatim
        return "Execution error message mismatch.\nThe actual message was \"" + actual
                + "\"\nwhere the expected message was:\"" + expected + "\"";
    }

    @Override
    public String lineDiffers(long expected, long actual) {
        // assertError.pure:25
        return "Execution error line mismatch. Actual: " + actual + " where expected: " + expected;
    }

    @Override
    public String columnDiffers(long expected, long actual) {
        // assertError.pure:26
        return "Execution error column mismatch. Actual: " + actual + " where expected: " + expected;
    }
}
