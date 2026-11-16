// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * THE TESTABLE-FRAMEWORK ASSERTION RULES — the engine's, implemented from
 * its sources (legend-engine {@code TestAssertionHelper} +
 * {@code JsonNodeComparator.NULL_MISSING_EQUIVALENT_AND_UNORDERED_ARRAYS},
 * 4.145.0), for the user test harnesses (service / mapping / function
 * suites). Distinct from {@link com.legend.exec.JsonCompare}: that is the
 * platform's own document compare (ordered arrays, the Pure
 * {@code assertEquals} semantics); THIS is what an {@code EqualToJson}
 * assertion means, and the two deliberately differ:
 *
 * <ul>
 *   <li>{@code null} and a MISSING object field are the same thing;</li>
 *   <li>arrays are UNORDERED at every level — two arrays are equal when
 *       they have the same length and every element of one pairs with an
 *       equal, not-yet-paired element of the other;</li>
 *   <li>numbers compare as exact decimals whatever their JSON spelling
 *       ({@code 1} equals {@code 1.0}; the engine parses both sides with
 *       {@code USE_BIG_DECIMAL_FOR_FLOATS} and compares
 *       {@code decimalValue()});</li>
 *   <li>strings and booleans compare strictly; a type mismatch is a
 *       difference.</li>
 * </ul>
 *
 * <p>Trees are the {@link com.legend.sql.Json#parse} shape: {@code Map}
 * (object), {@code List} (array), {@code Number}, {@code String},
 * {@code Boolean}, {@code null}.
 */
public final class TestAssertions {

    private TestAssertions() {
    }

    /** {@code EqualToJson}: null when the trees are equal under the rules
     *  above, else the path and values of the FIRST difference found. */
    public static @com.legend.base.Nullable String equalToJson(
            @com.legend.base.Nullable Object expected, @com.legend.base.Nullable Object actual) {
        return serviceJson(expected, actual);
    }

    // the engine's JsonNodeComparator, implemented here since 2026-11-16 (E-Java's ledger audit, defect D1): it was
    // testexec.Equality's, where a number within 2 ULP passed unconditionally -- the engine compares decimalValue()
    // exactly (above), and S15 keeps any float tolerance to backwards-compatibility mode alone; the tolerance is gone

    private static @com.legend.base.Nullable String serviceJson(@com.legend.base.Nullable Object expected,
            @com.legend.base.Nullable Object actual) {
        return serviceDiff(expected, actual, "$");
    }

    private static @com.legend.base.Nullable String serviceDiff(@com.legend.base.Nullable Object e,
            @com.legend.base.Nullable Object a, String path) {
        if (e == null && a == null) {
            return null;
        }
        if (e == null || a == null) {
            return path + " expected " + show(e) + ", got " + show(a);
        }
        if (e instanceof Map<?, ?> em && a instanceof Map<?, ?> am) {
            TreeSet<String> keys = new TreeSet<>();
            em.keySet().forEach(k -> keys.add(String.valueOf(k)));
            am.keySet().forEach(k -> keys.add(String.valueOf(k)));
            for (String k : keys) {
                String d = serviceDiff(em.get(k), am.get(k), path + "." + k);
                if (d != null) {
                    return d;
                }
            }
            return null;
        }
        if (e instanceof List<?> el && a instanceof List<?> al) {
            if (el.size() != al.size()) {
                return path + " expected " + el.size() + " elements, got " + al.size();
            }
            List<Object> remaining = new ArrayList<>(al);
            for (int i = 0; i < el.size(); i++) {
                Object x = el.get(i);
                int at = -1;
                for (int j = 0; j < remaining.size() && at < 0; j++) {
                    if (serviceDiff(x, remaining.get(j), path) == null) {
                        at = j;
                    }
                }
                if (at < 0) {
                    Object nearest = nearest(x, remaining);
                    String why = nearest == null ? "no actual element shares its first field"
                            : "nearest actual " + show(nearest) + " differs: "
                                    + serviceDiff(x, nearest, path + "[" + i + "]");
                    return path + "[" + i + "] expected " + show(x)
                            + " has no equal element in the actual array (" + why + ")";
                }
                remaining.remove(at);
            }
            return null;
        }
        if (e instanceof Number en && a instanceof Number an) {
            if (decimal(en).compareTo(decimal(an)) == 0) {
                return null;
            }
            return path + " expected " + en + ", got " + an;
        }
        if (e instanceof String es && a instanceof String as) {
            return es.equals(as) ? null : path + " expected " + show(es) + ", got " + show(as);
        }
        if (e instanceof Boolean eb && a instanceof Boolean ab) {
            return eb.equals(ab) ? null : path + " expected " + eb + ", got " + ab;
        }
        return path + " expected " + jsonKind(e) + " " + show(e) + ", got " + jsonKind(a) + " " + show(a);
    }

    private static @com.legend.base.Nullable Object nearest(Object expected, List<Object> candidates) {
        if (!(expected instanceof Map<?, ?> em) || em.isEmpty()) {
            return null;
        }
        var first = em.entrySet().iterator().next();
        for (Object c : candidates) {
            if (c instanceof Map<?, ?> cm
                    && serviceDiff(first.getValue(), cm.get(first.getKey()), "$") == null) {
                return c;
            }
        }
        return null;
    }

    private static String jsonKind(Object o) {
        return o instanceof Map ? "object" : o instanceof List ? "array"
                : o instanceof Number ? "number" : o instanceof String ? "string"
                : o instanceof Boolean ? "boolean" : "null";
    }

    private static String show(@com.legend.base.Nullable Object o) {
        if (o == null) {
            return "null";
        }
        String s = o instanceof String str ? "'" + str + "'" : String.valueOf(o);
        return s.length() > 160 ? s.substring(0, 157) + "..." : s;
    }

    private static BigDecimal decimal(Number n) {
        return n instanceof BigDecimal bd ? bd : new BigDecimal(n.toString());
    }
}
