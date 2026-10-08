// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.List;
import java.util.Objects;

/**
 * A statement as a dialect renders it for execution with bound values: the SQL text with a {@code ?} placeholder for each
 * bound parameter, and the parameters in placeholder order ({@link SqlDialect#renderStatement}).
 *
 * @param sql   the statement text
 * @param binds one per {@code ?}, in order
 */
public record RenderedStatement(String sql, List<Bind> binds) {

    public RenderedStatement {
        Objects.requireNonNull(sql, "sql");
        binds = List.copyOf(binds);
    }

    /**
     * What one placeholder binds: a declared parameter's value, or — {@code arrayElementSqlType} set — its whole
     * collection as one array of that element type (the target's form of a list).
     */
    public record Bind(String parameter, @com.legend.base.Nullable String arrayElementSqlType) {
        public Bind {
            Objects.requireNonNull(parameter, "parameter");
        }
    }
}
