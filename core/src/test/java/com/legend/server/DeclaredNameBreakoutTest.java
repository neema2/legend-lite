// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A store's declared-quoted column name is a NAME on every road to the database, whatever its characters (stream S,
 * injection safety, 2026-11-14). The audit of 2026-11-14 reproduced a breakout on DuckDB: a model declares a quoted
 * column whose name closes the quote and appends statements; its {@code ###Data} load wrote the table's DDL with the
 * name undoubled ({@code DuckDb.ddlIdentifier}), and DuckDB's driver runs {@code ;}-separated statements, even
 * prepared -- so the project's query answered its rows AND the appended {@code COPY} wrote a file. The name must be
 * carried as data: the table made with a column of exactly that name, and nothing else run.
 */
class DeclaredNameBreakoutTest {

    /** A model whose table declares {@code hostile} as a quoted column's name (its Pure spelling: each {@code "} and
     *  {@code \} escaped by a backslash). */
    private static String model(String hostile) {
        String pure = hostile.replace("\\", "\\\\").replace("\"", "\\\"");
        return """
                Class proj::Party { id: Integer[1]; name: String[1]; }
                ###Relational
                Database proj::PartyDB ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), "%s" INTEGER) )
                ###Mapping
                Mapping proj::PartyMapping
                (
                  *proj::Party: Relational
                  {
                    ~primaryKey ([proj::PartyDB] PARTY.ID)
                    ~mainTable [proj::PartyDB] PARTY
                    id: [proj::PartyDB] PARTY.ID,
                    name: [proj::PartyDB] PARTY.NAME
                  }
                )
                ###Connection
                RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime proj::RT { mappings: [ proj::PartyMapping ]; connections: [ proj::PartyDB: [ db: proj::Conn ] ]; }
                ###Data
                Data proj::PartyData
                {
                  Relational
                  #{
                    default.PARTY:
                      'ID,NAME\\n1,Meridian Capital\\n2,Halberd Securities\\n';
                  }#
                }
                """.formatted(pure);
    }

    @Test
    void aQuotedColumnThatClosesItsQuoteIsANameNotAStatement(@TempDir Path dir) throws Exception {
        Path pwned = dir.resolve("pwned.csv");
        String hostile = "x\" INTEGER); COPY (SELECT 42 AS pwned) TO '" + pwned.toString().replace("'", "''")
                + "'; CREATE TABLE z(\"ID";
        List<Object> names = Lean.service(model(hostile),
                "|proj::Party.all()->project(~[name: p|$p.name])->sort(~name->ascending())", "proj::RT").rows()
                .stream().map(r -> r.get(0)).toList();
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names);
        assertFalse(Files.exists(pwned), "the declared name ran as a statement: " + pwned + " was written");
    }

    @Test
    void theColumnIsMadeWithExactlyItsDeclaredName() throws Exception {
        String hostile = "a\"b\\c'd";
        List<Object> got = Lean.service(model(hostile),
                "|#>{proj::PartyDB.PARTY}#->select(~[ID, '" + hostile.replace("\\", "\\\\").replace("'", "\\'")
                        + "'])->sort(~ID->ascending())", "proj::RT").rows()
                .stream().map(r -> r.get(0)).toList();
        assertEquals(List.of(1L, 2L), got);
    }
}
