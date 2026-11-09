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
 * runner resolves it through the stores it was given, refuses bytes that are not the plan's (P6), and delivers them as
 * the plan says -- the database reading the runner's own copy through a bound path, or taking the cells the runner
 * streams -- the rows the database's own CSV reader gave.
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
        assertTrue(out.size() >= 2, "a database that reads the file and one the runner streams to: " + out);
        return out;
    }

    private static ExecutionPlan.LoadStep.File file(SqlDialect dialect, String name, byte[] bytes) {
        SqlDialect.CsvLoad spelled = dialect.csvLoad(LOAD);
        ExecutionPlan.LoadStep.Delivery delivery = spelled.readsFile()
                ? new ExecutionPlan.LoadStep.Delivery.DatabaseReads(spelled.sql())
                : new ExecutionPlan.LoadStep.Delivery.Streamed(spelled.sql(), LOAD.columns().size());
        return new ExecutionPlan.LoadStep.File(new DataRef(DataRef.Namespace.RESOURCE, name,
                DataStores.sha256(bytes), bytes.length, DataRef.Format.CSV), delivery);
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
        // a store whose path is long, as a Windows runner's checkout is: the database reads the runner's own copy
        Path deep = dir;
        for (int i = 0; i < 6; i++) {
            deep = deep.resolve("a-directory-name-of-some-length-" + i);
        }
        Path csv = deep.resolve("core_relational/tests/employees.csv");
        Files.createDirectories(csv.getParent());
        byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        Files.write(csv, bytes);
        DataStores stores = DataStores.resources(deep);
        List<Boolean> deliveries = new ArrayList<>();
        for (TestBackend b : loading()) {
            ExecutionPlan.Load load = load(b, file(b.dialect(), "core_relational/tests/employees.csv", bytes));
            deliveries.add(b.dialect().csvLoad(LOAD).readsFile());
            assertEquals(List.of(List.of("Joe", "Bloggs", 21), List.of("Jill", "Smith", 31)), loaded(b, stores, load),
                    b.name());
            // the plan holds no path: its JSON names the file by reference alone, and reads back as written
            String json = PlanJson.write(ExecutionPlan.reading(List.of(), load));
            assertFalse(json.contains(dir.toString()), json);
            assertEquals(json, PlanJson.write(PlanJson.read(json)), b.name());
        }
        assertTrue(deliveries.contains(true) && deliveries.contains(false), "both deliveries ran: " + deliveries);
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
                    DataStores.sha256(new byte[0]), 0, DataRef.Format.CSV), name);
        }
    }

    @Test
    void theCsvRuleIsTheDatabaseReadersRule() {
        // a quoted cell exactly its text, "" inside it a quote; an unquoted cell trimmed, an empty one null; a short row
        // padded with nulls; an empty line no row; CRLF a line's end
        List<List<String>> rows = com.legend.base.CsvCells.rows(
                "A,B,C\r\n \"x, \"\"y\"\"\" , z ,\n\nlast\r\n\"\",,  \n");
        assertEquals(List.of("A", "B", "C"), rows.get(0));
        List<List<String>> body = com.legend.base.CsvCells.body(rows, 3);
        List<String> nulls = new ArrayList<>();
        nulls.add("last");
        nulls.add(null);
        nulls.add(null);
        List<String> quotedEmpty = new ArrayList<>();
        quotedEmpty.add("");
        quotedEmpty.add(null);
        quotedEmpty.add(null);
        List<String> first = new ArrayList<>();
        first.add("x, \"y\"");
        first.add("z");
        first.add(null);
        assertEquals(List.of(first, nulls, quotedEmpty), body);
    }
}
