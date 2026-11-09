// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.DialectCapability;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDml;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A FILE'S LOAD BY REFERENCE (docs/DATA_IN_THE_PLAN_2026_11_09.md L1, 2026-11-09), on every in-process database whose
 * dialect spells one: the plan names the file by its namespace, name and digest, never a machine's path (P5); the
 * runner resolves it through the stores it was given, refuses bytes that are not the plan's (P6), and streams the cells
 * the file's declared format reads (decision 6, L2: upstream's rule on every database).
 */
class DataLoadsTest {

    // the corpus's file as it is: a header, two rows, no newline after the last
    private static final String CSV = "Firstname,Lastname,Age\nJoe,Bloggs,21\nJill,Smith,31";

    private static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "data_loads_people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of());

    /** Every in-process database whose dialect spells a CSV file's load (one that spells none refuses by name). */
    private static List<TestBackend> loading() {
        List<TestBackend> out = new ArrayList<>();
        for (TestBackend b : TestBackends.inProcess()) {
            try {
                b.dialect().csvLoad(LOAD);
                out.add(b);
            } catch (DialectCapability refused) {
                // this database takes no file yet: the dialect says so by name (CsvLoadRenderTest)
            }
        }
        assertTrue(out.size() >= 2, "DuckDB and H2 at least: " + out);
        return out;
    }

    private static ExecutionPlan.LoadStep.File file(SqlDialect dialect, String name, byte[] bytes) {
        SqlDialect.CsvLoad spelled = dialect.csvLoad(LOAD);
        ExecutionPlan.LoadStep.Delivery delivery = new ExecutionPlan.LoadStep.Delivery.Streamed(spelled.sql(),
                LOAD.columns().size());
        return new ExecutionPlan.LoadStep.File(new DataRef(DataRef.Namespace.RESOURCE, name,
                DataStores.sha256(bytes), bytes.length, DataRef.Csv.DEFAULT), delivery);
    }

    private static ExecutionPlan.Load load(TestBackend b, ExecutionPlan.LoadStep.File file) {
        return new ExecutionPlan.Load("a load", ExecutionPlan.Load.Cadence.AT_STEP, List.of(file),
                new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(b.planType(), null), List.of(),
                        List.of()));
    }

    /** {@code load} run on a fresh table of {@code b}'s, its rows read back. */
    private static List<List<Object>> loaded(TestBackend b, DataStores stores, ExecutionPlan.Load load)
            throws Exception {
        try (Connection c = b.open(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE data_loads_people (FIRSTNAME VARCHAR(200), LASTNAME VARCHAR(200), AGE INT)");
            try {
                Sessions.Session session = TestSessions.over(c);
                List<List<Boolean>> ran = new ArrayList<>();
                try (var run = com.legend.base.RunScope.enter()) {
                    PlanRunner.seed(load, session, PlanRunner.opened(session, load.target()), stores, ran::add);
                }
                assertEquals(List.of(List.of(false)), ran, "the load ran its one step");
                List<List<Object>> rows = new ArrayList<>();
                try (ResultSet rs = st.executeQuery(
                        "SELECT FIRSTNAME, LASTNAME, AGE FROM data_loads_people ORDER BY AGE")) {
                    while (rs.next()) {
                        rows.add(List.of(rs.getString(1), rs.getString(2), rs.getInt(3)));
                    }
                }
                return rows;
            } finally {
                st.execute("DROP TABLE data_loads_people");
            }
        }
    }

    @Test
    void eachDatabaseLoadsTheFileByItsReference(@TempDir Path dir) throws Exception {
        // a store whose path is long, as a Windows runner's checkout is
        Path deep = dir;
        for (int i = 0; i < 6; i++) {
            deep = deep.resolve("a-directory-name-of-some-length-" + i);
        }
        Path csv = deep.resolve("core_relational/tests/employees.csv");
        Files.createDirectories(csv.getParent());
        byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        Files.write(csv, bytes);
        DataStores stores = DataStores.resources(deep);
        for (TestBackend b : loading()) {
            ExecutionPlan.Load load = load(b, file(b.dialect(), "core_relational/tests/employees.csv", bytes));
            assertEquals(List.of(List.of("Joe", "Bloggs", 21), List.of("Jill", "Smith", 31)), loaded(b, stores, load),
                    b.name());
            // the plan holds no path: its JSON names the file by reference alone, and reads back as written
            String json = PlanJson.write(ExecutionPlan.reading(List.of(), load));
            assertFalse(json.contains(dir.toString()), json);
            assertEquals(json, PlanJson.write(PlanJson.read(json)), b.name());
        }
    }

    @Test
    void bytesThatAreNotThePlansAreRefused(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("employees.csv"), CSV);
        for (TestBackend b : loading()) {
            ExecutionPlan.Load planned = load(b, file(b.dialect(), "employees.csv",
                    (CSV + "\nJack,Jones,41").getBytes(StandardCharsets.UTF_8)));
            DataError e = assertThrows(DataError.class, () -> loaded(b, DataStores.resources(dir), planned),
                    b.name());
            assertTrue(e.getMessage().contains("not the data it was planned against"), e.getMessage());
        }
    }

    @Test
    void aRunGivenNoStoreRefusesByName() {
        byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        for (TestBackend b : loading()) {
            DataError e = assertThrows(DataError.class, () -> loaded(b, DataStores.none(),
                    load(b, file(b.dialect(), "employees.csv", bytes))), b.name());
            assertTrue(e.getMessage().contains("no store"), e.getMessage());
        }
    }

    @Test
    void aReferenceIsNeverAMachinesPath() {
        for (String name : List.of("/tmp/employees.csv", "C:\\data\\employees.csv", "c:/data.csv", "a/../b.csv",
                "a\\b.csv", "", "a//b.csv", "./a.csv")) {
            assertThrows(IllegalArgumentException.class, () -> new DataRef(DataRef.Namespace.RESOURCE, name,
                    DataStores.sha256(new byte[0]), 0, DataRef.Csv.DEFAULT), name);
        }
    }

    /** A file whose cells are malformed by its declared format, or a record of another width than its header's, is
     *  refused by name at the run as at planning -- never padded, cut, skipped or repaired (invariant 4). */
    @Test
    void aMalformedFileIsRefusedNeverRepaired(@TempDir Path dir) throws Exception {
        for (String text : List.of(CSV + "\nJack,Jones", CSV + "\nJack,Jones,41,extra", CSV + "\n\nJack,Jones,41",
                CSV + "\n\"Jack\"x,Jones,41", CSV + "\n\"Jack,Jones,41")) {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            Files.write(dir.resolve("bad.csv"), bytes);
            for (TestBackend b : loading()) {
                DataError e = assertThrows(DataError.class, () -> loaded(b, DataStores.resources(dir),
                        load(b, file(b.dialect(), "bad.csv", bytes))), b.name() + ": " + text);
                assertTrue(e.getMessage().contains("'bad.csv'"), e.getMessage());
            }
        }
    }

    /** The format's cells are upstream's: a padded cell kept, an empty or quoted empty cell NULL, a cell whose quote
     *  follows a space its text, a doubled quote one quote inside a quoted cell, CRLF a record's end -- loaded so on
     *  every database (DuckDB's own reader would read the third cell quoted: CsvLoadRenderTest). */
    @Test
    void eachDatabaseLoadsUpstreamsCells(@TempDir Path dir) throws Exception {
        String text = "Firstname,Lastname,Age\r\n  Joe  ,\"\",1\r\n \"x\",\"a \"\"q\"\" b\",2\r\n";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("cells.csv"), bytes);
        List<Object> nullLast = new ArrayList<>();
        nullLast.add("  Joe  ");
        nullLast.add(null);
        nullLast.add(1);
        for (TestBackend b : loading()) {
            assertEquals(List.of(nullLast, List.of(" \"x\"", "a \"q\" b", 2)),
                    loadedNullable(b, DataStores.resources(dir), load(b, file(b.dialect(), "cells.csv", bytes))),
                    b.name());
        }
    }

    /** {@code load} run on a fresh table of {@code b}'s, its rows read back with their nulls. */
    private static List<List<Object>> loadedNullable(TestBackend b, DataStores stores, ExecutionPlan.Load load)
            throws Exception {
        try (Connection c = b.open(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE data_loads_people (FIRSTNAME VARCHAR(200), LASTNAME VARCHAR(200), AGE INT)");
            try {
                Sessions.Session session = TestSessions.over(c);
                try (var run = com.legend.base.RunScope.enter()) {
                    PlanRunner.seed(load, session, PlanRunner.opened(session, load.target()), stores, ran -> { });
                }
                List<List<Object>> rows = new ArrayList<>();
                try (ResultSet rs = st.executeQuery(
                        "SELECT FIRSTNAME, LASTNAME, AGE FROM data_loads_people ORDER BY AGE")) {
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        row.add(rs.getString(1));
                        row.add(rs.getString(2));
                        row.add(rs.getInt(3));
                        rows.add(row);
                    }
                }
                return rows;
            } finally {
                st.execute("DROP TABLE data_loads_people");
            }
        }
    }
}
