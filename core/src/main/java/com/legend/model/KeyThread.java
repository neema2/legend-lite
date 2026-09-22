// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import java.util.Objects;

/**
 * One primary-key THREAD of an Operation union's row: a member set's key
 * column projected as {@code <column>_<memberOrdinal>} — the union's row
 * identity across its members (a set-1 row carries NULL in set-2's key
 * thread). The engine's importDataFlow columns
 * ({@code pureToSQLQuery_union.pure:140–150}); {@code pureKind} is the
 * column's Pure primitive kind ({@code Integer}, {@code String}, …) read
 * off the store at synthesis, null when the store does not declare it
 * (a consumer that needs the kind is loud).
 *
 * <p>{@code column} and {@code ordinal} say which member's physical column
 * the thread reads (legacy routes as composition: the stack builder projects
 * the thread from the arm's own row). A SHARED table key — members over one
 * table whose sole primary key every such member projects once, ungated,
 * as {@code <col>__pk_<table>} — carries {@code ordinal = -1} and names its
 * {@code store} and {@code table}; it is the row identity a cast re-root
 * joins on, never an importDataFlow column.
 */
public record KeyThread(String name, @com.legend.base.Nullable String pureKind,
        String column, int ordinal,
        @com.legend.base.Nullable String store, @com.legend.base.Nullable String table) {
    public KeyThread {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(column, "column");
    }

    /** A per-member thread. */
    public KeyThread(String name, @com.legend.base.Nullable String pureKind, String column, int ordinal) {
        this(name, pureKind, column, ordinal, null, null);
    }

    /** Whether this is the shared table key (not a per-member thread). */
    public boolean shared() {
        return ordinal < 0;
    }
}
