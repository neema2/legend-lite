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

    static final SqlDml.LoadCsv LOAD = new SqlDml.LoadCsv(null, "data_loads_people", NameQuoting.BARE,
            List.of("FIRSTNAME", "LASTNAME", "AGE"), Set.of(),
            List.of(new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 200),
            new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 200),
            new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER)), ',', '"', 500);


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
        // DuckDB and H2 read a file themselves; every other database refuses one by name (A3, option 1)
        assertEquals(Set.of("duckdb", "h2"), out.stream().map(TestBackend::name).collect(java.util.stream.Collectors.toSet()));
        return out;
    }

    private static ExecutionPlan.LoadStep.File file(SqlDialect dialect, String name, byte[] bytes) {
        return file(dialect, name, bytes, 500);
    }

    /** {@link #file(SqlDialect, String, byte[])} admitting {@code limitMegabytes} (upstream's 500 by default). */
    private static ExecutionPlan.LoadStep.File file(SqlDialect dialect, String name, byte[] bytes, int limitMegabytes) {
        SqlDialect.CsvLoad reads = dialect.csvLoad(LOAD);
        ExecutionPlan.LoadStep.Delivery delivery = new ExecutionPlan.LoadStep.Delivery.Reads(reads.statements(),
                reads.after());
        return new ExecutionPlan.LoadStep.File(new DataRef(DataRef.Namespace.RESOURCE, name,
                DataStores.sha256(bytes), bytes.length, DataRef.Csv.DEFAULT), delivery, limitMegabytes);
    }

    private static ExecutionPlan.Load load(TestBackend b, ExecutionPlan.LoadStep.File file) {
        return com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_STEP, List.of(file),
                new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(b.planType(), null), List.of(),
                        List.of()));
    }

    /** {@code load} run on a fresh table of {@code b}'s, its rows read back; {@code setup} the test's own settings on the
     *  connection first. */
    private static List<List<Object>> loaded(TestBackend b, DataStores stores, ExecutionPlan.Load load, String... setup)
            throws Exception {
        try (Connection c = b.open(); Statement st = c.createStatement()) {
            for (String s : setup) {
                st.execute(s);
            }
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
                        "SELECT FIRSTNAME, LASTNAME, AGE FROM data_loads_people ORDER BY CASE WHEN AGE IS NULL THEN 1 ELSE 0 END, AGE")) {
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

    /** A load fits a database of small memory (PR #95's macOS CI, 2026-11-12): DuckDB sized its read buffer at sixteen
     *  times the line limit unless told, 7.8 GiB for 500 MB, and a 7 GB runner refused every load before reading a byte.
     *  The statement now says its buffer, the line limit's size; a 1 GB memory limit stands for the small machine here. */
    @Test
    void aLoadFitsADatabaseOfSmallMemory(@TempDir Path dir) throws Exception {
        byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("small.csv"), bytes);
        TestBackend duckdb = loading().stream().filter(b -> b.name().equals("duckdb")).findFirst().orElseThrow();
        ExecutionPlan.Load load = load(duckdb, file(duckdb.dialect(), "small.csv", bytes));
        assertEquals(List.of(List.of("Joe", "Bloggs", 21), List.of("Jill", "Smith", 31)),
                loaded(duckdb, DataStores.resources(dir), load, "SET memory_limit = '1GB'"));
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

    /** A file of another delimiter loads on each database that reads one (the correctness audit's note, 2026-11-10:
     *  H2's CSVREAD splits its column names by the field separator, so names joined by a comma read as one column under
     *  {@code ;}); and where the reader sets something on the session (H2's path variable), a load that fails leaves
     *  none of it there (the A checkpoint's tenets audit, 14: the undo is run on every exit). */
    @Test
    void aSemicolonFileLoads_andAFailedLoadLeavesNothingOfItsReaderOnTheSession(@TempDir Path dir) throws Exception {
        SqlDml.LoadCsv semi = new SqlDml.LoadCsv(null, "data_loads_people", NameQuoting.BARE, LOAD.columns(),
                Set.of(), LOAD.types(), ';', '"', 500);
        byte[] good = "Firstname;Lastname;Age\nJoe;Bloggs;21\n".getBytes(StandardCharsets.UTF_8);
        byte[] wide = "Firstname;Lastname;Age\nJoe;Bloggs;21;extra\n".getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("good.csv"), good);
        Files.write(dir.resolve("wide.csv"), wide);
        DataStores stores = DataStores.resources(dir);
        for (TestBackend b : loading()) {
            SqlDialect.CsvLoad reads = b.dialect().csvLoad(semi);
            java.util.function.Function<byte[], ExecutionPlan.LoadStep.File> file = bytes -> new ExecutionPlan.LoadStep
                    .File(new DataRef(DataRef.Namespace.RESOURCE, bytes == good ? "good.csv" : "wide.csv",
                            DataStores.sha256(bytes), bytes.length, new DataRef.Csv(';', '"')),
                            new ExecutionPlan.LoadStep.Delivery.Reads(reads.statements(), reads.after()), 500);
            assertEquals(List.of(List.of("Joe", "Bloggs", 21)), loaded(b, stores, load(b, file.apply(good))), b.name());
            if (reads.after().isEmpty()) {
                continue;   // a reader that sets nothing on the session has nothing to undo (H2's sets its path)
            }
            try (Connection c = b.open(); Statement st = c.createStatement()) {
                st.execute("CREATE TABLE data_loads_people (FIRSTNAME VARCHAR(200), LASTNAME VARCHAR(200), AGE INT)");
                try {
                    Sessions.Session session = TestSessions.over(c);
                    ExecutionPlan.Load failing = load(b, file.apply(wide));
                    assertThrows(RuntimeException.class, () -> {
                        try (var run = com.legend.base.RunScope.enter()) {
                            PlanRunner.seed(failing, session, PlanRunner.opened(session, failing.target()), stores,
                                    answered -> { });
                        }
                    });
                    try (ResultSet rs = st.executeQuery("SELECT @legend_csv")) {
                        assertTrue(rs.next());
                        assertEquals(null, rs.getString(1), "the failed load left the copy's path on the session");
                    }
                } finally {
                    st.execute("DROP TABLE data_loads_people");
                }
            }
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

    /** Each database's load at the format's edges, PINNED: upstream (Commons CSV EXCEL, an empty cell NULL) refuses
     *  each of these texts or reads it as its "upstream" entry says. DuckDB's own reader (the user's ruling of
     *  2026-11-09) and H2's {@code CSVREAD} (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.3, A3) differ where their
     *  entries say, each a row of docs/SEMANTICS_REGISTER.md (S184, S186); every other database refuses a file by name.
     *  This pin fails the day one moves. */
    @Test
    void eachReadersEdgesArePinned(@TempDir Path dir) throws Exception {
        pinsHold(loading(), dir);
    }

    /** {@link #eachReadersEdgesArePinned} on {@code backends} (DataLoadsPostgresTest's too). */
    static void pinsHold(List<TestBackend> backends, Path dir) throws Exception {
        java.util.Map<String, java.util.Map<String, String>> pins = new java.util.LinkedHashMap<>();
        String two = "[[Joe, Bloggs, 21], [Jill, Smith, 31]]";
        String jack = "[[Joe, Bloggs, 21], [Jill, Smith, 31], [Jack, Jones, 41]]";
        // a short record: refused (DuckDB's reader); H2's CSVREAD reads a missing cell as it reads an empty one, NULL,
        // so the record loads (S186)
        pins.put(CSV + "\nJack,Jones", java.util.Map.of("upstream", "refused",
                "h2", "[[Joe, Bloggs, 21], [Jill, Smith, 31], [Jack, Jones, 0]]"));
        pins.put(CSV + "\nJack", java.util.Map.of("upstream", "refused",
                "h2", "[[Joe, Bloggs, 21], [Jill, Smith, 31], [Jack, null, 0]]"));
        // a wide record: refused (DuckDB's reader; H2's record check)
        pins.put(CSV + "\nJack,Jones,41,extra", java.util.Map.of("upstream", "refused"));
        // a wide record whose extra cell is empty: DuckDB loads it, its last delimiter read as the record's end (S184,
        // pending); H2 too, its extra cell NULL (S186)
        pins.put(CSV + "\nJack,Jones,41,", java.util.Map.of("upstream", "refused", "duckdb", jack, "h2", jack));
        // an empty line: DuckDB and H2 skip it (S184, ruled; S186); upstream reads a record of one cell, refused in a
        // wider table
        pins.put(CSV + "\n\nJack,Jones,41", java.util.Map.of("upstream", "refused", "duckdb", jack, "h2", jack));
        pins.put(CSV + "\n\"Jack\"x,Jones,41", java.util.Map.of("upstream", "refused"));
        // an unclosed quote: refused (DuckDB); H2 reads the rest of the file into the cell it opens (S186)
        pins.put(CSV + "\n\"Jack,Jones,41", java.util.Map.of("upstream", "refused",
                "h2", "[[Joe, Bloggs, 21], [Jill, Smith, 31], [Jack,Jones,41, null, 0]]"));
        pins.put(CSV + "\nJack,Jones,\"41", java.util.Map.of("upstream", "refused", "h2", jack));
        // an empty last cell is a cell: NULL
        pins.put(CSV + "\nJack,Jones,", java.util.Map.of("upstream",
                "[[Joe, Bloggs, 21], [Jill, Smith, 31], [Jack, Jones, 0]]"));
        // empty and quoted empty cells are NULL
        pins.put(CSV + "\n,\"\",41", java.util.Map.of("upstream",
                "[[Joe, Bloggs, 21], [Jill, Smith, 31], [null, null, 41]]"));
        // a quote after a space: DuckDB and H2 read the cell quoted (S184, ruled; S186); upstream reads it as its text
        pins.put(CSV + "\n \"Jack\",Jones,41", java.util.Map.of("upstream",
                "[[Joe, Bloggs, 21], [Jill, Smith, 31], [ \"Jack\", Jones, 41]]", "duckdb", jack, "h2", jack));
        // mixed line breaks: DuckDB refuses them (S184, ruled)
        pins.put("Firstname,Lastname,Age\r\nJoe,Bloggs,21\nJill,Smith,31", java.util.Map.of("upstream", two,
                "duckdb", "refused"));
        pins.put("Firstname,Lastname,Age", java.util.Map.of("upstream", "[]"));
        pins.put("Firstname,Lastname,Age\n", java.util.Map.of("upstream", "[]"));
        pins.put("", java.util.Map.of("upstream", "[]"));
        List<String> moved = new ArrayList<>();
        for (TestBackend b : backends) {
            for (var pin : pins.entrySet()) {
                String text = pin.getKey();
                byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                Files.write(dir.resolve("edge.csv"), bytes);
                String got;
                try {
                    got = loadedNullable(b, DataStores.resources(dir), load(b, file(b.dialect(), "edge.csv", bytes)))
                            .toString();
                } catch (DataError e) {
                    assertTrue(e.getMessage().contains("'edge.csv'"), e.getMessage());
                    got = "refused";
                }
                System.out.println("@@PIN " + b.name() + " " + text.replace("\n", "\\n").replace("\r", "\\r")
                        + " => " + got);
                String want = pin.getValue().get(b.name());
                if (!(want == null ? pin.getValue().get("upstream") : want).equals(got)) {
                    moved.add(b.name() + ": " + text + " => " + got);
                }
            }
        }
        assertEquals(List.of(), moved, "a reader's pin moved");
    }

    /** A file larger than its load admits is refused at the run, in upstream's words, as at planning (D-b: the user's
     *  ruling, 2026-11-09; upstream admits 500 MB, here a limit of one megabyte keeps the test small). */
    @Test
    void aFileLargerThanTheLoadAdmitsIsRefused(@TempDir Path dir) throws Exception {
        StringBuilder text = new StringBuilder(CSV);
        while (text.length() <= 1024 * 1024) {
            text.append("\nJack,Jones,41");
        }
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("big.csv"), bytes);
        for (TestBackend b : loading()) {
            DataError e = assertThrows(DataError.class, () -> loaded(b, DataStores.resources(dir),
                    load(b, file(b.dialect(), "big.csv", bytes, 1))), b.name());
            assertEquals("Unable to read the CSV file '/big.csv' Execution error at ??, \"File is too large, file was"
                    + " 1.00 Mb, limit is 1 Mb\"", e.getMessage());
        }
    }

    /** The header is dropped unread, as upstream drops it (legend-pure's {@code LazyIterate.drop(..., 1)}): a header of
     *  another width than the table loads its data records (the M1 tenets audit's finding 4, 2026-11-09). */
    @Test
    void theHeaderIsDroppedUnread(@TempDir Path dir) throws Exception {
        byte[] bytes = "Name\nJoe,Bloggs,21".getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("narrow.csv"), bytes);
        for (TestBackend b : loading()) {
            assertEquals(List.of(List.of("Joe", "Bloggs", 21)), loaded(b, DataStores.resources(dir),
                    load(b, file(b.dialect(), "narrow.csv", bytes))), b.name());
        }
    }

    /** The format's cells are upstream's: a padded cell kept, an empty or quoted empty cell NULL, a cell whose quote
     *  follows a space its text, a doubled quote one quote inside a quoted cell, CRLF a record's end -- loaded so on
     *  every database. */
    @Test
    void eachDatabaseLoadsUpstreamsCells(@TempDir Path dir) throws Exception {
        String text = "Firstname,Lastname,Age\r\n  Joe  ,\"\",1\r\nx,\"a \"\"q\"\" b\",2\r\n";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("cells.csv"), bytes);
        List<Object> nullLast = new ArrayList<>();
        nullLast.add("  Joe  ");
        nullLast.add(null);
        nullLast.add(1);
        for (TestBackend b : loading()) {
            assertEquals(List.of(nullLast, List.of("x", "a \"q\" b", 2)),
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
                        "SELECT FIRSTNAME, LASTNAME, AGE FROM data_loads_people ORDER BY CASE WHEN AGE IS NULL THEN 1 ELSE 0 END, AGE")) {
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

    /** A load whose identity is not its content's digest is refused before anything runs: the seeds' memo and a shared
     *  database's key trust the identity (the M1 tenets audit's note, 2026-11-09). */
    @Test
    void aLoadWhoseIdentityIsNotItsContentsIsRefused(@TempDir Path dir) throws Exception {
        byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("employees.csv"), bytes);
        for (TestBackend b : loading()) {
            ExecutionPlan.Load honest = load(b, file(b.dialect(), "employees.csv", bytes));
            ExecutionPlan.Load named = new ExecutionPlan.Load("load:another", honest.cadence(), honest.steps(),
                    honest.target(), java.util.Map.of());
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> loaded(b, DataStores.resources(dir), named), b.name());
            assertTrue(e.getMessage().contains("is not its content's"), e.getMessage());
        }
    }
}
