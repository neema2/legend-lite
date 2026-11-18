// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;

import java.util.List;

/**
 * LEGEND-PURE'S WORDS OF AN ASSERT'S FAILURE, AT RUN: the compatibility edge's, which its {@link EdgeHostWords} composes
 * (census J29, 2026-11-18; C5's step B, the orchestrator's ruling (1) of 2026-11-17), moved unchanged from
 * the product's {@code testexec.PureAsserts} (the assert family's sentences: assertEquals.pure, assertSameElements.pure,
 * assertNotEquals.pure, assertContains.pure, assertEmpty.pure, assertSize.pure, assertInstanceOf.pure) and
 * {@code testexecutionplan.Verdicts} (legend-pure's {@code toRepresentation}, a date's {@code +0000}). Words only: every
 * decision stays the product's.
 */
public final class EdgePureWords {

    public static final EdgePureWords INSTANCE = new EdgePureWords();

    private EdgePureWords() {
    }

    public String repr(@Nullable Object v, @Nullable String instanceClass) {
        if (v instanceof java.util.Map<?, ?> m) {
            String id = com.legend.testexecutionplan.Verdicts.wireId(v);
            if (id == null) {
                StringBuilder sb = new StringBuilder();
                for (Object val : m.values()) {
                    if (sb.length() > 0) {
                        sb.append('|');
                    }
                    sb.append(val);
                }
                id = sb.toString();
            }
            return "<" + id + " instanceOf " + (instanceClass == null ? "?" : instanceClass) + ">";
        }
        return switch (v) {
            case null -> "[]";   // an empty [0..1] renders as empty
            case String s -> "'" + s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n") + "'";
            case java.math.BigDecimal d -> d.toPlainString() + "D";
            case Number n -> n.toString();
            case Boolean b -> b.toString();
            // upstream's toRepresentation, '%' + its toString: a value with a minute carries +0000 (the engine runner,
            // 2026-11-04, the assert bar: %2020-01-01T10:00:00 writes '%2020-01-01T10:00:00+0000')
            case com.legend.values.PureDateLiteral d -> "%" + pureText(d);
            default -> throw new com.legend.error.NotImplementedException("toRepresentation for "
                    + v.getClass().getName() + " is not modeled (spec: '<id instanceOf Type>' -- loud until a consumer"
                    + " pins the id form)");
        };
    }

    /** legend-pure's text of a date, as its {@code toString} writes one: the literal body at its written precision,
     *  with {@code +0000} from a minute on ({@code DateFormat.append} writes it inside its minute branch; an hour-only
     *  date has none: {@code 2014-03-10T16} in its {@code TestDateFormat}). */
    static String pureText(com.legend.values.PureDateLiteral d) {
        return d.precision().atLeast(com.legend.values.PureDateLiteral.Precision.MINUTE)
                ? d.toEngineString() + "+0000" : d.toEngineString();
    }

    public String reprList(List<Object> side, @Nullable String instanceClass) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < side.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(repr(side.get(i), instanceClass));
        }
        return sb.append(']').toString();
    }

    /** Upstream's assertEquals text (assertEquals.pure:19-21): each side's {@code %r} where BOTH hold one value, else
     *  both as lists of representations -- the one-value form is the pair's, never a side's alone. */
    public String equals(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass) {
        boolean ones = e.size() == 1 && a.size() == 1;
        return "\nexpected: " + (ones ? repr(e.get(0), eClass) : reprList(e, eClass))
                + "\nactual:   " + (ones ? repr(a.get(0), aClass) : reprList(a, aClass));
    }

    /** assertSameElements.pure:17: both sides sorted, as lists of representations. */
    public String sameElements(List<Object> sortedExpected, List<Object> sortedActual, @Nullable String expectedClass,
            @Nullable String actualClass) {
        return "\nexpected: " + reprList(sortedExpected, expectedClass) + "\nactual:   "
                + reprList(sortedActual, actualClass);
    }

    public String pair(@Nullable Object expected, @Nullable String expectedClass, @Nullable Object actual,
            @Nullable String actualClass) {
        return "\nexpected: " + repr(expected, expectedClass) + "\nactual:   " + repr(actual, actualClass);
    }

    /** assertNotEquals.pure:17-21: each side's representation where both are one value, else both as lists. */
    public String notEquals(List<Object> notExpected, List<Object> actual) {
        return notExpected.size() == 1 && actual.size() == 1
                ? repr(notExpected.get(0), null) + " should not equal " + repr(actual.get(0), null)
                : reprList(notExpected, null) + " should not equal " + reprList(actual, null);
    }

    /** assertContains.pure:17-20: the collection as a list of representations, the value's representation. */
    public String notContains(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass) {
        return reprList(collection, collectionClass) + " does not contain " + repr(value, valueClass);
    }

    /** assertEmpty.pure:17-20 (its own test: {@code '[1, 2] is not empty'}): the collection as a List's {@code %s}. */
    public String notEmpty(List<Object> collection) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < collection.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(collection.get(i) instanceof String s ? s : repr(collection.get(i), null));
        }
        return sb.append("] is not empty").toString();
    }

    /** assertSize.pure:17. */
    public String size(@Nullable Object expected, long actual) {
        return "expected size: " + expected + ", actual size: " + actual;
    }

    /** assertIsNot's native message. */
    public String identical(@Nullable Object actual) {
        return "Should not be identical to: " + repr(actual, null);
    }

    /** assertNotContains' native message. */
    public String contains(List<Object> collection, @Nullable Object value) {
        return reprList(collection, null) + " should not contain " + repr(value, null);
    }

    /** upstream's words ('\nexpected: %r\nactual:   %r'; assertNotEquals.pure's '%r should not equal %r'), each grid's
     *  rows as the database judge frames them (no row '[]', one row its cells, several in brackets). */
    public String grids(List<List<Object>> expected, List<List<Object>> actual, boolean wantEqual) {
        String e = gridText(expected);
        String a = gridText(actual);
        return wantEqual ? "\nexpected: " + e + "\nactual:   " + a : e + " should not equal " + a;
    }

    private String gridText(List<List<Object>> rows) {
        List<String> out = new java.util.ArrayList<>(rows.size());
        for (List<Object> r : rows) {
            out.add(r.stream().map(v -> repr(v, null)).collect(java.util.stream.Collectors.joining(", ")));
        }
        return out.isEmpty() ? "[]" : out.size() == 1 ? out.get(0) : "[" + String.join(", ", out) + "]";
    }

    /** assertInstanceOf.pure's format (elementToPath of a top-level primitive is its name). */
    public String notInstanceOf(@Nullable Object v, @Nullable String valueClass, String type, String actual) {
        return "expected " + repr(v, valueClass) + " to be an instance of " + type + ", actual: " + actual;
    }
}
