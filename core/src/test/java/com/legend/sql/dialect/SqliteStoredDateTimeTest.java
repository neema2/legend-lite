// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.ExecuteOptions;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.testplan.PlanningOptions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DATE-TIME COLUMN'S CELLS ON SQLITE, BY WHO WROTE THEM (the independent audit of 2026-11-26, findings 1 and 5).
 * SQLite gives a column no type, so a date-time column holds whatever text its writer wrote.
 * <ul>
 *   <li>A table THIS DIALECT LOADS (test data): every cell is the one text of its instant
 *       ({@code SQLite.timestampLit}), read into it by the statement where the cell is not ISO; a cell that is no
 *       date-time is held as it was written (a load's rows can declare no raise), equal to no instant.</li>
 *   <li>A STORE'S OWN table (written here by the connection's own setup SQL, which no load of this dialect touches):
 *       <b>the product compares its cells AS HELD</b> -- the gap pinned below as it stands, the owner's to rule
 *       (the database follow-up): a cell with trailing zeros, which SQLite's own {@code strftime('%f')} writes, or
 *       with a 'T', is not equal to the literal of its instant.</li>
 *   <li>THE JUDGES read such a cell in full, both of them: the host judge decodes its text as a date-time, and the
 *       database judge reads it into the one text ({@code SQLite.variantAwareCast}), so the two reach one verdict
 *       (G2), under each judge alone and under both.</li>
 * </ul>
 */
class SqliteStoredDateTimeTest {

    /** S is the store's own: rows 1 to 3 are one instant written three ways (the one text; trailing zeros; a 'T'),
     *  rows 4 and 5 a whole second written two ways (SQLite's {@code strftime('%f')} form; its {@code datetime()}
     *  form). L is loaded by this dialect from cells that are not ISO, and one that is no date-time. */
    private static final String MODEL = """
            ###Relational
            Database s::DB
            (
                Table S ( ID INTEGER PRIMARY KEY, TS TIMESTAMP )
                Table L ( ID INTEGER PRIMARY KEY, TS TIMESTAMP )
            )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: SQLite;
                specification: LocalH2
                {
                    testDataSetupCSV: 'default\\nL\\nID,TS\\n1,2014-1-1 10:20:30.50\\n2,2014-01-01T11:20:30.500+01:00\\n3,2014-01-01 10:20:30.500000000000\\n4,2014-01-01 10\\n5,not a date\\n';
                    testDataSetupSqls: [
                        'CREATE TABLE S (ID INTEGER PRIMARY KEY, TS TIMESTAMP)',
                        'INSERT INTO S VALUES (1, \\'2014-01-01 10:20:30.5\\'), (2, \\'2014-01-01 10:20:30.500\\'), (3, \\'2014-01-01T10:20:30.5\\'), (4, \\'2014-01-01 10:20:31.000\\'), (5, \\'2014-01-01 10:20:31\\')'
                    ];
                };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    private static List<String> ids(String query) throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.SQLite).open()) {
            List<String> out = new ArrayList<>();
            for (List<Object> row : Lean.run(MODEL, query, "s::RT", c).rows()) {
                out.add(String.valueOf(row.get(0)));
            }
            return out;
        }
    }

    private static String where(String table, String condition) {
        return "|#>{s::DB." + table + "}#->filter(r|" + condition + ")->select(~[ID])->sort(~ID->ascending())";
    }

    @Test
    void aLoadedCellIsTheOneTextOfItsInstant() throws Exception {
        // unpadded with a trailing zero, an offset applied, twelve digits ending in zeros: one instant, one text
        assertEquals(List.of("1", "2", "3"), ids(where("L", "$r.TS == %2014-01-01T10:20:30.5")));
        assertEquals(List.of("1", "2", "3"), ids(where("L", "$r.TS == %2014-01-01T10:20:30.500")));
        // an hour alone is its first second
        assertEquals(List.of("4"), ids(where("L", "$r.TS == %2014-01-01T10:00:00")));
        // the cell that is no date-time is equal to no instant, and sorts as its text does
        assertEquals(List.of("1", "2", "3", "4"), ids(where("L", "$r.TS < %9999-12-31T23:59:59")));
    }

    /** THE GAP, AS IT STANDS (the owner's to rule; until then this pin says what a reader gets): a store's own cell
     *  is compared as its text is held. Right: the one text of an instant, and SQLite's own {@code datetime()} text of
     *  a whole second. Wrong: trailing zeros ({@code strftime('%f')}'s three digits) and a 'T'. Until 2026-11-26 a
     *  literal kept its written digits, so {@code == %...:30.500} found row 2 and {@code == %...:31.000} row 4. */
    @Test
    void aStoresOwnCellIsComparedAsItIsHeld_theRecordedGap() throws Exception {
        for (String literal : List.of("%2014-01-01T10:20:30.5", "%2014-01-01T10:20:30.500")) {
            assertEquals(List.of("1"), ids(where("S", "$r.TS == " + literal)), literal + " (rows 2 and 3 are missed)");
        }
        for (String literal : List.of("%2014-01-01T10:20:31", "%2014-01-01T10:20:31.000")) {
            assertEquals(List.of("5"), ids(where("S", "$r.TS == " + literal)), literal + " (row 4 is missed)");
        }
        // an order is the text's: a longer text sorts after its own instant's one text, and a 'T' after every space
        assertEquals(List.of("1", "2"), ids(where("S", "$r.TS < %2014-01-01T10:20:31")));
        assertEquals(List.of("2", "3", "4", "5"), ids(where("S", "$r.TS > %2014-01-01T10:20:30.5")));
    }

    private static boolean passes(String body, PlanningOptions.JudgeMode mode) throws Exception {
        // a session of its own per use: under both judges each judge's plan loads the store on its own session
        return Program.runFresh(MODEL, body, "s::RT", DatabaseType.SQLite, ExecuteOptions.NONE.withJudgeMode(mode),
                com.legend.testexec.NoEdgeRun.NONE).passed();
    }

    /** Each form of a store's own cell is its instant to BOTH judges: equal to the instant's literal, written either
     *  way, and unequal to another instant's -- under the host judge, the database judge and the two together. */
    @Test
    void theJudgesReadAStoresOwnCellInFull_oneVerdict() throws Exception {
        for (PlanningOptions.JudgeMode mode : List.of(PlanningOptions.JudgeMode.HOST,
                PlanningOptions.JudgeMode.DATABASE, PlanningOptions.JudgeMode.BOTH)) {
            for (int id = 1; id <= 3; id++) {
                String cell = "#>{s::DB.S}#->filter(r|$r.ID == " + id + ")->map(r|$r.TS->toOne())";
                for (String same : List.of("%2014-01-01T10:20:30.5", "%2014-01-01T10:20:30.500")) {
                    assertTrue(passes("{|assertEquals(" + same + ", " + cell + ");}", mode),
                            mode + ": row " + id + " is " + same);
                }
                assertTrue(passes("{|assertNotEquals(%2014-01-01T10:20:30.05, " + cell + ");}", mode),
                        mode + ": row " + id + " is not another instant");
            }
            for (int id = 4; id <= 5; id++) {
                String cell = "#>{s::DB.S}#->filter(r|$r.ID == " + id + ")->map(r|$r.TS->toOne())";
                assertTrue(passes("{|assertEquals(%2014-01-01T10:20:31, " + cell + ");}", mode),
                        mode + ": row " + id + " is the whole second");
            }
        }
    }
}
