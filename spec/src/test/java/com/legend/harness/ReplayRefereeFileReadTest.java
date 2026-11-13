// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;
import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE REFEREE REPLAYS A FILE'S LOAD ITSELF (B2b, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6): a ledger
 * entry holding a file's verified bytes and the referee's own statements is replayed on the referee's H2 over a copy of
 * the bytes, its {@code CSVREAD} reading the cells -- a quoted cell holding the delimiter, a doubled quote and an empty
 * cell among them -- and its undo run, so the session keeps no path; no Java reads the cells (until B2b the edge split
 * them, {@code base.CsvCells}).
 */
class ReplayRefereeFileReadTest {

    private static final List<SqlDdl.ColumnType> TYPES = List.of(
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER),
            new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.VARCHAR));

    @Test
    void aFilesLoadIsReadByTheRefereesOwnReader() {
        SqlDialect referee = com.legend.database.Databases.dialect(com.legend.database.Databases.REPLAY_REFEREE);
        SqlDialect.CsvLoad load = referee.csvLoad(new SqlDml.LoadCsv(null, "PEOPLE",
                com.legend.sql.NameQuoting.BARE, List.of("ID", "NAME"), Set.of(), TYPES, ',', '"', 500));
        byte[] csv = "ID,NAME\n1,\"Smith, \"\"Jo\"\"\"\n2,\n".getBytes(StandardCharsets.UTF_8);
        RawSqlBoundary.Recorder recorder = new RawSqlBoundary.Recorder();
        recorder.recordExecuted("CREATE TABLE PEOPLE (ID INTEGER, NAME VARCHAR(40))", false);
        recorder.recordRead(new RawSqlBoundary.FileRead(csv, load.statements(), load.after()));
        ReplayReferee r = new ReplayReferee(recorder);
        com.legend.testexec.SqlReplayReferee.RefereeRows rows = r.rows("SELECT ID, NAME FROM PEOPLE ORDER BY ID");
        assertEquals(List.of(List.of(1, "Smith, \"Jo\""), java.util.Arrays.asList(2, null)), rows.rows());
        // the undo ran: the session holds no path of the copy
        com.legend.testexec.SqlReplayReferee.RefereeRows path = r.rows("SELECT @legend_csv");
        assertTrue(path.rows().size() == 1 && path.rows().get(0).get(0) == null, String.valueOf(path.rows()));
    }

    @Test
    void theLedgerHoldsTheFilesLoadAsOneEntryOfItsStatements() {
        RawSqlBoundary.Recorder recorder = new RawSqlBoundary.Recorder();
        recorder.recordRead(new RawSqlBoundary.FileRead("ID\n1\n".getBytes(StandardCharsets.UTF_8),
                List.of("SET @legend_csv = ?", "INSERT INTO T SELECT 1"), List.of("SET @legend_csv = NULL")));
        assertEquals(1, recorder.seeds().size());
        RawSqlBoundary.Raw entry = recorder.seeds().get(0);
        assertEquals("SET @legend_csv = ?;\nINSERT INTO T SELECT 1;\nSET @legend_csv = NULL", entry.sql());
        assertEquals(new RawSqlBoundary.FileRead("ID\n1\n".getBytes(StandardCharsets.UTF_8),
                List.of("SET @legend_csv = ?", "INSERT INTO T SELECT 1"), List.of("SET @legend_csv = NULL")),
                entry.read(), "a file's load equal by its bytes and statements");
    }
}
