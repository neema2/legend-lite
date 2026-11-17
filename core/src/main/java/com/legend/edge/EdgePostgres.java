// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * Postgres as the compatibility edge writes it (the D1.0 pattern): a raw statement in legend-engine's H2 text
 * translated in the flavor DuckDB shares, its column types Postgres's (2026-10-12, docs/CLOUD_BACKENDS_PCT_2026_10_11.md
 * §20: the 8-byte double is DOUBLE PRECISION, and SMALLINT is the smallest integer, there is no TINYINT) and a bare
 * column name in lower case, as Postgres folds a bare name and spells a store's (decision D8). The product runs a raw
 * statement as written (Phase 6's B4 (ii), 2026-11-17).
 */
public class EdgePostgres extends com.legend.sql.dialect.Postgres {

    private static final java.util.regex.Pattern ENGINE_DOUBLE =
            java.util.regex.Pattern.compile("(?i)\\bDOUBLE\\b(?!\\s+PRECISION)");

    private static final java.util.regex.Pattern ENGINE_TINYINT = java.util.regex.Pattern.compile("(?i)\\bTINYINT\\b");

    @Override
    public String raw(String statement, @com.legend.base.Nullable String writtenFor) {
        return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor)
                ? RawSqlBoundary.translated(statement,
                        types -> ENGINE_TINYINT.matcher(ENGINE_DOUBLE.matcher(types).replaceAll("DOUBLE PRECISION"))
                                .replaceAll("SMALLINT"),
                        bare -> bare.toLowerCase(java.util.Locale.ROOT))
                : statement;
    }
}
