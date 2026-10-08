// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A seed spells its table and column names as the SESSION's dialect references them
 * ({@code SqlDialect.physicalName}, C3c): the seeded table and the query read one name. It used the union of
 * DuckDB's and H2's reserved words, whatever the session — Postgres, which folds a bare name to lowercase,
 * was not in it.
 */
class SeedSpellingTest {

    private static final String[] COLS = {"ID", "order"};
    private static final List<String[]> ROWS = List.<String[]>of(new String[] {"1", "a"});

    @Test
    void postgresQuotesEveryStoredName() {
        RowLoad load = CsvSeed.rowLoad(new com.legend.sql.dialect.Postgres(), "S", "T", COLS, ROWS);
        assertEquals("\"S\"", load.schema());
        assertEquals("\"T\"", load.table());
        assertEquals(List.of("\"ID\"", "\"order\""), load.columns());
    }

    @Test
    void h2AndDuckDbQuoteOnlyWhatTheyReserve() {
        for (com.legend.sql.dialect.SqlDialect d : List.of(new com.legend.sql.dialect.H2(),
                new com.legend.sql.dialect.DuckDb())) {
            RowLoad load = CsvSeed.rowLoad(d, null, "T", COLS, ROWS);
            assertEquals("T", load.table(), d.getClass().getSimpleName());
            assertEquals(List.of("ID", "\"order\""), load.columns(), d.getClass().getSimpleName());
        }
    }
}
