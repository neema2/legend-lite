// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * DuckDB as the compatibility edge writes it (the D1.0 pattern: the product's dialect, one arm overridden): a raw
 * statement in legend-engine's H2 text -- its connection declares {@link RawSqlBoundary#ENGINE_TEXT}, as legend-engine's
 * test corpus's {@code executeInDb} calls do -- translated for DuckDB ({@link RawSqlBoundary#translated}: keyword
 * column names quoted, H2's type kinds mapped, {@code CURRENT_TIMESTAMP()} without parens). The product runs a raw
 * statement as written (Phase 6's B4 (ii), 2026-11-17).
 */
public class EdgeDuckDb extends com.legend.sql.dialect.DuckDb {

    @Override
    public String raw(String statement, @com.legend.base.Nullable String writtenFor) {
        return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor)
                ? RawSqlBoundary.translated(statement, types -> types, bare -> bare) : statement;
    }
}
