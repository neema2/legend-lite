// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * LITE'S TEST-DATA READER (B2d.2's audit, H2, 2026-11-11): the file loader's quote rules, ported from the reader
 * {@code base.CsvCells} held until 00e67e38 -- a cell is quoted only where it STARTS with a double quote; a quote
 * inside any other cell is a character; a text that ends inside a quoted cell is refused by name, never repaired.
 */
class CsvSeedRecordsTest {

    /** The user's G7 ruling (2026-11-12, "Def want windows files accepted"; register S302): a text's values exactly as
     *  written -- the schema and table lines verbatim, a padded cell's spaces kept -- and a CRLF line ending a line
     *  break, the '\r' never part of a value or a name, in a quoted cell too; a lone '\r' a character. */
    @Test
    void aBlockIsReadAsWrittenACrlfALineBreak() {
        String lf = "default\nK\nID,NAME\n1, ann \n2,\"two\nlines\"\n-----\nS\nT\nID\n7\n";
        assertEquals(CsvSeed.blocks(lf), CsvSeed.blocks(lf.replace("\n", "\r\n")));
        List<CsvSeed.Block> blocks = CsvSeed.blocks(lf);
        assertEquals(new CsvSeed.Block("default", "K", List.of("ID", "NAME"),
                List.of(List.of("1", " ann "), List.of("2", "two\nlines"))), blocks.get(0));
        assertEquals(" default", CsvSeed.blocks(" default\nK\nID\n1\n").get(0).schema());
        assertEquals("K ", CsvSeed.blocks("default\nK \nID\n1\n").get(0).table());
        assertEquals(List.of(List.of("1", "a\rb")), CsvSeed.blocks("default\nK\nID,NAME\n1,a\rb\n").get(0).rows());
    }

    @Test
    void aQuoteInsideACellIsACharacter() {
        // 5'11": the quote does not start the cell, so it opens nothing (the old reader toggled on it and joined the
        // rest of the text into one record)
        String text = "default\nPERSON\nID,HEIGHT\n1,5'11\"\n2,6'0\"\n";
        assertEquals(List.of("default", "PERSON", "ID,HEIGHT", "1,5'11\"", "2,6'0\"", ""),
                CsvSeed.records(text, true));
        List<CsvSeed.Block> blocks = CsvSeed.blocks(text);
        assertEquals(1, blocks.size());
        assertEquals(List.of(List.of("1", "5'11\""), List.of("2", "6'0\"")), blocks.get(0).rows());
    }

    @Test
    void anUnterminatedQuoteIsRefusedByName() {
        CsvSeed.Malformed e = assertThrows(CsvSeed.Malformed.class,
                () -> CsvSeed.records("default\nT\nID,NAME\n1,\"open\n2,b\n", true));
        assertEquals("the test data ends inside the quoted cell that starts on line 4", e.getMessage());
        assertThrows(CsvSeed.Malformed.class, () -> CsvSeed.blocks("default\nT\nID,NAME\n1,\"open\n"));
    }

    @Test
    void aQuotedCellKeepsItsCommasLineBreaksAndDoubledQuotes() {
        String text = "default\nT\nID,NAME\n1,\"a, b\nc\"\n2,\"say \"\"hi\"\"\"\n3,\"x\"  \n4,plain\n";
        assertEquals(List.of("default", "T", "ID,NAME", "1,\"a, b\nc\"", "2,\"say \"\"hi\"\"\"", "3,\"x\"  ",
                "4,plain"), CsvSeed.records(text, false));
        List<CsvSeed.Block> blocks = CsvSeed.blocks(text);
        assertEquals(List.of(List.of("1", "a, b\nc"), List.of("2", "say \"hi\""), List.of("3", "x"),
                List.of("4", "plain")), blocks.get(0).rows());
    }

    @Test
    void textAfterAClosingQuoteIsRefusedByName() {
        CsvSeed.Malformed e = assertThrows(CsvSeed.Malformed.class, () -> CsvSeed.records("ID\n\"a\"x,b\n", true));
        assertEquals("the test data's line 2 has 'x' between a quoted cell's closing quote and the next comma",
                e.getMessage());
    }

    @Test
    void cellsAreAsWrittenNothingNormalized() {
        // the reader keeps each cell as written: an empty cell, ---null---, spaces; the header's cells too
        List<CsvSeed.Block> blocks = CsvSeed.blocks("default\nT\n ID , NAME\n1,\n2,---null---\n3, x \n");
        assertEquals(List.of(" ID ", " NAME"), blocks.get(0).header());
        assertEquals(List.of(List.of("1", ""), List.of("2", "---null---"), List.of("3", " x ")), blocks.get(0).rows());
    }

    @Test
    void aTextWithNoQuoteSplitsOnEveryLineBreak() {
        assertEquals(List.of("a", "", "b", ""), CsvSeed.records("a\n\nb\n", true));
        assertEquals(List.of("a", "", "b"), CsvSeed.records("a\n\nb\n", false));
    }
}
