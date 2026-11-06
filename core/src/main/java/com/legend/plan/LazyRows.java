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

    /** A cell of {@code table}'s rows whose value is the plan parameter {@code name}, bound when the plan runs (the
     *  test-is-the-product plan's M3f, R17 C4: an activity's trace stamp); its text in the rows is not read. */
    public record Parameter(String table, int row, int column, String name) {
        public Parameter {
            java.util.Objects.requireNonNull(table, "table");
            java.util.Objects.requireNonNull(name, "name");
        }
    }

    /** The computed rows and their parameter cells. */
    public record Computed(Map<String, List<List<String>>> rows, List<Parameter> parameters) {
        public Computed {
            parameters = List.copyOf(parameters);
        }
    }

    private final Supplier<@com.legend.base.Nullable Computed> compute;
    /** Null until computed; then the rows, or empty for none. */
    private java.util.@com.legend.base.Nullable Optional<Computed> computed;

    public LazyRows(Supplier<@com.legend.base.Nullable Computed> compute) {
        this.compute = compute;
    }

    private java.util.Optional<Computed> computed() {
        java.util.Optional<Computed> c = computed;
        if (c == null) {
            c = java.util.Optional.ofNullable(compute.get());
            computed = c;
        }
        return c;
    }

    /** The rows, computed on the first call; null when there are none. */
    public @com.legend.base.Nullable Map<String, List<List<String>>> rows() {
        return computed().map(Computed::rows).orElse(null);
    }

    /** The rows' parameter cells (none when there are no rows). */
    public List<Parameter> parameters() {
        return computed().map(Computed::parameters).orElse(List.of());
    }

    /** Read as a map, the rows are computed (none reads as empty). */
    @Override
    public java.util.Set<Entry<String, List<List<String>>>> entrySet() {
        Map<String, List<List<String>>> r = rows();
        return r == null ? java.util.Set.of() : r.entrySet();
    }
}
