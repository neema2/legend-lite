// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Rows from a query with no ORDER BY come back in whatever order the database likes (Bazel workplan P3-11): a test
 * reads them through {@link #sortedBy} before taking them by position, or compares them whole with
 * {@link #assertSameRows}, never by the order they arrived in.
 */
public final class Rows {

    private Rows() {}

    /** {@code rows} sorted by the cell at {@code column}: numbers by value, anything else by its text, nulls first. */
    public static <R extends List<?>> List<R> sortedBy(List<R> rows, int column) {
        return sortedBy(rows, (R r) -> r.get(column));
    }

    /** {@code rows} (of any row type) sorted by the cell {@code cell} reads from each. */
    public static <R> List<R> sortedBy(List<R> rows, Function<R, ?> cell) {
        List<R> out = new ArrayList<>(rows);
        out.sort(Comparator.comparing(cell::apply, Rows::compareCells));
        return out;
    }

    /** That {@code actual} holds exactly {@code expected}'s rows, in any order (a multiset); cells compare by value
     *  (1, 1L and 1.0 are one number). */
    public static void assertSameRows(List<? extends List<?>> expected, List<? extends List<?>> actual) {
        assertSameRows(expected, actual, r -> r);
    }

    /** {@link #assertSameRows(List, List)} for rows of any type, each read as its cells by {@code cells}. */
    public static <R> void assertSameRows(List<? extends List<?>> expected, List<R> actual,
            Function<R, ? extends List<?>> cells) {
        List<String> e = keys(expected);
        List<List<?>> read = new ArrayList<>();
        for (R r : actual) {
            read.add(cells.apply(r));
        }
        List<String> a = keys(read);
        if (!e.equals(a)) {
            throw new AssertionError("rows differ (order ignored):\n  expected " + e + "\n  actual   " + a);
        }
    }

    private static List<String> keys(List<? extends List<?>> rows) {
        List<String> out = new ArrayList<>();
        for (List<?> r : rows) {
            List<String> cells = new ArrayList<>();
            for (Object c : r) {
                cells.add(key(c));
            }
            out.add(String.join("\u0001", cells));
        }
        out.sort(null);
        return out;
    }

    /** A cell's comparison key, tagged by kind so the number 1, the text "1" and a null never meet. */
    private static String key(Object c) {
        if (c == null) {
            return "\u0000null";
        }
        if (c instanceof Number n && finite(n)) {
            return "n:" + number(n).stripTrailingZeros().toPlainString();
        }
        if (c instanceof CharSequence) {
            return "s:" + c;
        }
        return c.getClass().getSimpleName() + ":" + c;
    }

    private static boolean finite(Number n) {
        return !(n instanceof Double d && !Double.isFinite(d)) && !(n instanceof Float f && !Float.isFinite(f));
    }

    private static int compareCells(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : -1) : 1;
        }
        if (a instanceof Number x && b instanceof Number y && finite(x) && finite(y)) {
            return number(x).compareTo(number(y));
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    private static BigDecimal number(Number n) {
        return n instanceof BigDecimal d ? d : new BigDecimal(n.toString());
    }
}
