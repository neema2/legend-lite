// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE DESK'S TABLES, AS THE TRADING STORE OFFERS THEM (docs/DESK_DATA_ROADMAP_2026_10_29.md A2): the twenty tables the
 * trading desk reads, each column's name, type, key and NOT NULL as {@code store::DB} reaches it, held to the snapshot
 * taken before their declarations moved from hand-written tables into ingests ({@code desk-tables.tsv}). A store that
 * declares them by ingests offers the same tables, or this fails naming the one that moved.
 */
class DeskTablesTest {

    /** The tables the desk reads (datacube/demo/market-sim/tables.ts MARKET_TABLES, the world's own aside). */
    static final List<String> TABLES = List.of("EXCHANGE", "SECTOR", "INSTRUMENT", "EQUITY", "MARKET_QUOTE",
            "MARKET_TRADE", "NEWS_STORY", "BUSINESS_UNIT", "TRADER", "BOOK", "DESK", "OPTION", "POSITION", "RISK_LIMIT",
            "COUNTERPARTY", "TRADE_ORDER", "EXECUTION", "ORDER_ROUTE", "FX_RATE", "RFQ");

    /** Each table's columns as the store offers them: one line a column, {@code TABLE COLUMN TYPE key notNull}. */
    static String offered(ModelContext ctx) {
        StringBuilder b = new StringBuilder();
        for (String table : TABLES) {
            DatabaseDefinition.TableDefinition t = ctx.findTableDefinition("store::DB", table).orElseThrow(
                    () -> new AssertionError("store::DB offers no table " + table));
            for (DatabaseDefinition.ColumnDefinition c : t.columns()) {
                b.append(table).append('\t').append(c.name()).append('\t').append(c.dataType()).append('\t')
                        .append(c.primaryKey() ? "key" : "-").append('\t').append(c.notNull() ? "not-null" : "-")
                        .append('\n');
            }
        }
        return b.toString();
    }

    @Test
    void theStoreOffersTheDesksTablesAsBefore() throws Exception {
        String snapshot;
        try (InputStream in = DeskTablesTest.class.getResourceAsStream("/desk-tables.tsv")) {
            snapshot = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        String offered = offered(Compiler.compileModel(StressCorpus.model()));
        // what it offers, beside the test's outputs: the snapshot to take when a change of the tables is meant
        java.nio.file.Files.createDirectories(com.legend.testing.TestOutputs.dir());
        java.nio.file.Files.writeString(com.legend.testing.TestOutputs.file("desk-tables.tsv"), offered);
        assertEquals(snapshot, offered);
    }
}
