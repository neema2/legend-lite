// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.plan;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A scope's rows (store table -> rows) that are computed when first READ,
 * not when registered — an execute() call's activity rows carry a second
 * render of its chain (the engine-style SQL a RelationalActivity reports),
 * which only a program reading the result's activities observes. A
 * computation answering null means "no rows" (the scope is then not
 * registered at all, as before).
 */
public final class LazyRows extends java.util.AbstractMap<String, List<List<String>>> {

    private final Supplier<Map<String, List<List<String>>>> compute;
    /** Null until computed; then the rows, or empty for none. */
    private java.util.@com.legend.base.Nullable Optional<Map<String, List<List<String>>>> computed;

    public LazyRows(Supplier<Map<String, List<List<String>>>> compute) {
        this.compute = compute;
    }

    /** The rows, computed on the first call; null when there are none. */
    public @com.legend.base.Nullable Map<String, List<List<String>>> rows() {
        java.util.Optional<Map<String, List<List<String>>>> c = computed;
        if (c == null) {
            c = java.util.Optional.ofNullable(compute.get());
            computed = c;
        }
        return c.orElse(null);
    }

    /** Read as a map, the rows are computed (none reads as empty). */
    @Override
    public java.util.Set<Entry<String, List<List<String>>>> entrySet() {
        Map<String, List<List<String>>> r = rows();
        return r == null ? java.util.Set.of() : r.entrySet();
    }
}
