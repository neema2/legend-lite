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
        return com.legend.exec.Equality.serviceJson(expected, actual);
    }
}
