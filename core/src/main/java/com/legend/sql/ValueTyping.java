// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;
import java.util.Objects;

/**
 * How a parameter whose SQL type its value decides is typed (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): a
 * Float's literal is a decimal of its own digits, or a floating number at an extreme magnitude; a Number's an integer or
 * either of a Float's; a DateTime's a date-time to the microsecond or finer; a Date's a date or either of a DateTime's.
 * {@code kinds}: the kinds its values take; {@code absent}: the kind an absent value is a null of.
 */
public record ValueTyping(List<ValueKind> kinds, ValueKind absent) {
    public ValueTyping {
        kinds = List.copyOf(kinds);
        Objects.requireNonNull(absent, "absent");
        if (!kinds.contains(absent)) {
            throw new IllegalArgumentException("an absent value's kind " + absent + " is not among " + kinds);
        }
    }
}
