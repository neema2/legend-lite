// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * LEGEND-ENGINE'S SERVICE-TEST {@code EqualToJson} (its {@code TestAssertionHelper} and
 * {@code JsonNodeComparator.NULL_MISSING_EQUIVALENT_AND_UNORDERED_ARRAYS}, 4.145.0), the edge's (the orchestrator's
 * ruling of 2026-11-17, J11's residue: moved from {@code testexec.Equality} and {@code test.TestAssertions} -- upstream's
 * comparator and its 2-ULP bound belong beside {@link CompiledPureHostForms}, not in the product's judge):
 * <ul>
 *   <li>{@code null} and a MISSING object field are the same thing;</li>
 *   <li>arrays are UNORDERED at every level -- equal when they have the same length and every element of one pairs
 *       with an equal, not-yet-paired element of the other;</li>
 *   <li>numbers compare as exact decimals whatever their JSON spelling ({@code 1} equals {@code 1.0}), with the
 *       referee's 2-ULP policy for the database's C-library doubles;</li>
 *   <li>strings and booleans compare strictly; a type mismatch is a difference.</li>
 * </ul>
 * Trees are the {@code com.legend.sql.Json#parse} shape: {@code Map}, {@code List}, {@code Number}, {@code String},
 * {@code Boolean}, {@code null}.
 */
public final class EqualToJson {

    private EqualToJson() {
    }

    /** Null when the trees are equal under the rules above, else the path and values of the FIRST difference. */
    public static @com.legend.base.Nullable String diff(@com.legend.base.Nullable Object expected,
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
            // (a 2-ULP tolerance here went 2026-11-24, B3 step 1b's review: legend-engine's JsonNodeComparator compares
            // numbers exactly, compareNumbers 4.145.0 :153-177)
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
