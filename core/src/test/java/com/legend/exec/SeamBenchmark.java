// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE SEAM MEASUREMENT (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §8): one generated relation (an integer, a text, a
 * decimal, a date-time, a nullable integer) drained through a counting sink by each way a result can cross the
 * runner's session seam, on each database its test names (SeamBenchmarkDuckDbTest, SeamBenchmarkH2Test; split by
 * database at main's merge, 2026-10-22, when a test naming a database must be named for it). Measured per path: wall time, the heap's peak, the characters that
 * crossed. A manual target, never a gate: {@code bazel test //core:seam_benchmark --test_output=all}; the
 * small-heap variant ({@code //core:seam_benchmark_small_heap}, 256 MB) runs the streaming paths and the whole text
 * at the same row count to show which of them drains.
 *
 * <p>The paths: (1) typed columns read from the driver by the plan's slot (the typed method of the design's §3,
 * prototyped here); (2) the JSON rows wire (the database writes one object per row, Java decodes it by the plan's
 * columns: step 6's seam, retired for typed rows by step 7's typed method); (3) the same statement counted as text only, so the database's formatting and Java's
 * parsing are told apart; (4) a CSV line per row written by the database (§5's per-row rule); (5) the whole CSV
 * text in one value, as the CSV wire builds it today. Arrow is not measured: the build has no Java Arrow dependency,
 * and adding one is a decision of its own.
 *
 * <p>The product's paths ({@link #product}, the payoff measurement, 2026-11-01): the same relation as a plan the
 * plan runner runs, read raw by the driver (1'), into no writer (6), into each writer at the boundary (7 JSON rows, 8
 * CSV, 9 Arrow, 10 PureV1), and written by the database in the product's exact bytes where its test gives the
 * statements (4'', 3''). Switches: {@code -Dseam.crc} prints each output's CRC32 (a change that must not move a byte
 * is measured before and after), {@code -Dseam.sample=N} its first N characters, {@code -Dseam.jfr} dumps a flight
 * recording per path into the test's outputs, {@code -Dseam.rows=N} the DuckDB row count; one database alone by
 * {@code --test_filter=com.legend.exec.SeamBenchmarkDuckDbTest}.
 */
final class SeamBenchmark {

    private SeamBenchmark() {
    }


    static final boolean SMALL_HEAP = "small".equals(System.getProperty("seam.heap", ""));

    private static String n(String name, boolean upper) {
        return upper ? name.toUpperCase(java.util.Locale.ROOT) : name;
    }
    private static final List<ExecutionPlan.Column> PLAN_COLUMNS = List.of(
            new ExecutionPlan.Column("id", "Integer", new ExecutionPlan.Multiplicity(1, 1), "BIGINT", null, null),
            new ExecutionPlan.Column("name", "String", new ExecutionPlan.Multiplicity(1, 1), "VARCHAR", null, null),
            new ExecutionPlan.Column("amount", "Decimal", new ExecutionPlan.Multiplicity(1, 1), "DECIMAL(18,2)", null,
                    null),
            new ExecutionPlan.Column("ts", "DateTime", new ExecutionPlan.Multiplicity(1, 1), "TIMESTAMP", null,
                    ExecutionPlan.Precision.INSTANT),
            new ExecutionPlan.Column("bucket", "Integer", new ExecutionPlan.Multiplicity(0, 1), "INTEGER", null, null));

    /** Every path over {@code c}'s view {@code bench} of {@code rows} rows, written in {@code dialect} ({@code upper}:
     *  whether its unquoted names fold up), {@code csvLine} and {@code agg} its CSV line and its text aggregate. */
    static void run(String db, Connection c, com.legend.sql.dialect.SqlDialect dialect, boolean upper, long rows,
            boolean mustDrain, String csvLine, String agg) throws Exception {
        System.out.println("[seam] " + db + " rows=" + rows + " heap=" + (SMALL_HEAP ? "small" : "default")
                + " maxHeapMB=" + Runtime.getRuntime().maxMemory() / (1024 * 1024));
        // (the JSON rows wire the product wrote, its paths 2 and 3, went with the result boundary program's phase 3,
        // 2026-10-27: the typed columns are the product's path now)
        String plain = "SELECT id, name, amount, ts, bucket FROM bench";
        String csvRows = "SELECT " + csvLine + " AS line FROM bench";
        String csvWhole = "SELECT " + agg + " FROM (SELECT " + csvLine + " AS line FROM bench) AS r";

        if (!SMALL_HEAP) {
            measure(db, "1 typed columns (driver values by slot)", rows, mustDrain, () -> typedColumns(c, plain));
        }
        measure(db, "4 csv line per row by the database", rows, mustDrain, () -> textRows(c, csvRows, false));
        // the whole text is the path the design retires: whether it drains is the measurement, never the test
        measure(db, "5 csv whole text in one value", rows, false, () -> wholeText(c, csvWhole, rows));
    }

    /**
     * THE PRODUCT'S PATHS (the payoff measurement, 2026-11-01; GATES "The product path's payoff measured"): the same
     * relation as a plan the product's runner runs ({@link PlanRunner#stream}: the typed seam, batches) into no
     * writer, then into each of the boundary's writers (JSON rows, CSV, PureV1, Arrow) onto a counting sink; the
     * database writing the product's exact bytes where its test gives the statements; and, as the same-machine
     * baseline for the wire the redesign retired, one JSON object per row written by the database and decoded by the
     * plan's columns.
     */
    static void product(String db, Connection c, com.legend.model.ConnectionDefinition.DatabaseType type, long rows,
            boolean mustDrain, String jsonLine, List<ExactText> exact) throws Exception {
        // the amount cast to the plan's declared DECIMAL(18,2) (the view's division answers a DOUBLE on DuckDB, a wider
        // decimal on H2: a real plan states the column the statement answers); the target's session statements the
        // dialect's own, as the planner writes them (H2's lazy execution, D2)
        String plain = "SELECT id, name, CAST(amount AS DECIMAL(18,2)) AS amount, ts, bucket FROM bench";
        // the honest floor: the same statement the product's plan sends, read raw by the same getters' kinds
        measure(db, "1' raw typed read of the product's own statement", rows, mustDrain, () -> typedColumns(c, plain));
        measure(db, "6 product runner, typed batches, no writer (again, beside 1')", rows, mustDrain,
                () -> productDrain(c, new ExecutionPlan(List.of(), new ExecutionPlan.TdsResult(
                        ExecutionPlan.TdsResult.Shape.RELATION, PLAN_COLUMNS, new ExecutionPlan.Sql(plain, List.of(),
                                new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(type),
                                        new ExecutionPlan.Servers.Every(),
                                        com.legend.database.Databases.dialect(type).sessionSetup(), List.of()),
                                null))), null, null));
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(type),
                new ExecutionPlan.Servers.Every(), com.legend.database.Databases.dialect(type).sessionSetup(),
                List.of());
        ExecutionPlan plan = new ExecutionPlan(List.of(), new ExecutionPlan.TdsResult(
                ExecutionPlan.TdsResult.Shape.RELATION, PLAN_COLUMNS, new ExecutionPlan.Sql(plain, List.of(), target,
                        null)));
        measure(db, "6 product runner, typed batches, no writer", rows, mustDrain,
                () -> productDrain(c, plan, null, null));
        measure(db, "7 product runner + JsonRowsWriter", rows, mustDrain, () -> {
            CountingWriter w = new CountingWriter();
            return productDrain(c, plan, new com.legend.result.JsonRowsWriter(w,
                    ExecutionPlan.TdsResult.Shape.RELATION), w::count);
        });
        measure(db, "8 product runner + CsvWriter", rows, mustDrain, () -> {
            CountingWriter w = new CountingWriter();
            return productDrain(c, plan, new com.legend.result.CsvWriter(w, ExecutionPlan.TdsResult.Shape.RELATION),
                    w::count);
        });
        measure(db, "10 product runner + PureV1Writer (the server's engine-format answer)", rows, mustDrain, () -> {
            CountingWriter w = new CountingWriter();
            return productDrain(c, plan, com.legend.result.PureV1Writer.relation(w, columns ->
                    new com.legend.result.PureV1Writer.Envelope("{\"_type\":\"tdsBuilder\"}", "[]",
                            columns.stream().map(ExecutionPlan.Column::name).toList())), w::count);
        });
        measure(db, "9 product runner + ArrowWriter (bytes)", rows, mustDrain, () -> {
            CountingStream o = new CountingStream();
            return productDrain(c, plan, new com.legend.result.ArrowWriter(o, Map.of()), o::count);
        });
        if (!SMALL_HEAP && mustDrain) {
            // THE DATABASE WRITING THE PRODUCT'S OWN BYTES (2026-11-01): each row's CSV line and JSON object spelled
            // in SQL exactly as CsvWriter and JsonRowsWriter spell them (the checksums prove it; the statements the
            // database's own test's), sent to the same kind of output as the writers
            for (ExactText e : exact) {
                measure(db, e.label(), rows, false, () -> {
                    CountingWriter w = new CountingWriter();
                    return databaseText(c, e.sql(), w, e.head(), e.between(), e.after(), e.tail());
                });
            }
        }
        if (!SMALL_HEAP && mustDrain) {
            // the database's own JSON text, passed through untouched (what a database-written JSON answer costs)
            measure(db, "3' json object per row by the database, text passed through", rows, false,
                    () -> textRows(c, "SELECT " + jsonLine + " AS line FROM bench", false));
        }
        if (!SMALL_HEAP && mustDrain) {
            // after the product's paths: on H2 as our sessions open it the retired wire's shape runs out of memory and
            // closes the database (measured 2026-11-01), so it runs only where a session streams
            measure(db, "2' json object per row by the database, decoded (the retired wire's shape)", rows, false,
                    () -> textRows(c, "SELECT " + jsonLine + " AS line FROM bench", true));
        }
    }

    /** A statement whose one column is each row's text in the product's exact spelling, written by the database, and
     *  the text around the rows the product's writer writes ({@code head}, {@code between} rows, {@code after} each
     *  row, {@code tail}). */
    record ExactText(String label, String sql, String head, String between, String after, String tail) {
    }

    /** Each row's text the database wrote, onto {@code w}: {@code head}, the rows with {@code between} before every
     *  row but the first and {@code after} each, then {@code tail}. */
    private static Drained databaseText(Connection c, String sql, CountingWriter w, String head, String between,
            String after, String tail) throws Exception {
        long[] n = {0};
        java.io.Writer out = new java.io.BufferedWriter(w, 1 << 16);
        out.write(head);
        new JdbcStatements(c, new SessionState()).query(sql, List.of(), List.of(), row -> {
            try {
                if (n[0] > 0) {
                    out.write(between);
                }
                out.write(row == null ? "" : row);
                out.write(after);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            n[0]++;
        });
        out.write(tail);
        out.flush();
        return new Drained(n[0], w.count());
    }

    /** {@code plan} run by the product's runner on {@code c}, its rows into {@code writer} (none: counted only). */
    private static Drained productDrain(Connection c, ExecutionPlan plan,
            com.legend.result.@com.legend.base.Nullable ResultSink writer,
            java.util.function.@com.legend.base.Nullable LongSupplier written) throws Exception {
        long[] n = {0};
        com.legend.result.ResultSink counting = new com.legend.result.ResultSink() {
            @Override
            public void columns(List<ExecutionPlan.Column> columns) throws java.io.IOException {
                if (writer != null) {
                    writer.columns(columns);
                }
            }

            @Override
            public void batch(com.legend.result.Batch batch) throws java.io.IOException {
                n[0] += batch.size();
                if (writer != null) {
                    writer.batch(batch);
                }
            }

            @Override
            public void end() throws java.io.IOException {
                if (writer != null) {
                    writer.end();
                }
            }
        };
        PlanRunner.stream(plan, Map.of(), PlanSessions.of(Sessions.borrowed(c)), counting);
        return new Drained(n[0], written == null ? 0 : written.getAsLong());
    }

    static final boolean CRC = System.getProperty("seam.crc") != null;
    static final int SAMPLE = Integer.getInteger("seam.sample", 0);

    private static final class CountingWriter extends java.io.Writer {
        private long n;
        final java.util.zip.CRC32 crc = new java.util.zip.CRC32();

        long count() {
            if (CRC) {
                System.out.println("[seam-crc] chars=" + n + " crc=" + Long.toHexString(crc.getValue()));
            }
            if (SAMPLE > 0) {
                System.out.println("[seam-sample]\n" + sample + "\n[/seam-sample]");
            }
            return n;
        }

        final StringBuilder sample = new StringBuilder();

        private void sum(char ch) {
            if (SAMPLE > 0 && sample.length() < SAMPLE) {
                sample.append(ch);
            }
            crc.update(ch & 0xff);
            crc.update(ch >>> 8);
        }

        @Override
        public void write(char[] cbuf, int off, int len) {
            n += len;
            if (CRC) {
                for (int i = 0; i < len; i++) {
                    sum(cbuf[off + i]);
                }
            }
        }

        @Override
        public void write(int ch) {
            n++;
            if (CRC) {
                sum((char) ch);
            }
        }

        @Override
        public void write(String str, int off, int len) {
            n += len;
            if (CRC) {
                for (int i = 0; i < len; i++) {
                    sum(str.charAt(off + i));
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    private static final class CountingStream extends java.io.OutputStream {
        private long n;
        final java.util.zip.CRC32 crc = new java.util.zip.CRC32();

        long count() {
            if (CRC) {
                System.out.println("[seam-crc] bytes=" + n + " crc=" + Long.toHexString(crc.getValue()));
            }
            return n;
        }

        @Override
        public void write(int b) {
            n++;
            if (CRC) {
                crc.update(b);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            n += len;
            if (CRC) {
                crc.update(b, off, len);
            }
        }
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /** A path's result: the rows it drained and the characters that crossed the seam. */
    private record Drained(long rows, long chars) {
    }

    private interface Path {
        Drained drain() throws Exception;
    }

    private static void measure(String db, String name, long expected, boolean mustDrain, Path path)
            throws Exception {
        System.gc();
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                pool.resetPeakUsage();
            }
        }
        // the profile (2026-11-01): one flight recording per path, dumped where the test's outputs go
        jdk.jfr.Recording recording = null;
        if (System.getProperty("seam.jfr") != null) {
            recording = new jdk.jfr.Recording(jdk.jfr.Configuration.getConfiguration("profile"));
            recording.start();
        }
        long t0 = System.nanoTime();
        String outcome;
        Drained d = null;
        try {
            d = path.drain();
            outcome = d.rows() == expected ? "drained" : "ROWS MISMATCH " + d.rows();
        } catch (OutOfMemoryError e) {
            outcome = "OutOfMemoryError";
        } catch (Exception e) {
            outcome = e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).lines().findFirst().orElse("");
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        if (recording != null) {
            recording.stop();
            String file = (db + "-" + name).replaceAll("[^A-Za-z0-9]+", "_") + ".jfr";
            String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
            recording.dump(java.nio.file.Path.of(outputs == null ? "." : outputs, file));
            recording.close();
        }
        long peak = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                peak += pool.getPeakUsage().getUsed();
            }
        }
        System.out.println("[seam] " + db + " | " + name + " | " + outcome + " | wall=" + ms + "ms | peakHeap="
                + peak / (1024 * 1024) + "MB | chars=" + (d == null ? "-" : d.chars()));
        if (mustDrain) {
            assertEquals("drained", outcome, db + " " + name);
        }
    }

    /** Path 1: the driver's values read by the plan's slot, one carrier conversion each, nothing kept. */
    private static Drained typedColumns(Connection c, String sql) throws Exception {
        long n = 0;
        long sink = 0;
        try (PreparedStatement st = c.prepareStatement(sql); ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
                long id = rs.getLong(1);
                String name = rs.getString(2);
                java.math.BigDecimal amount = rs.getBigDecimal(3);
                java.time.LocalDateTime at = rs.getObject(4, java.time.LocalDateTime.class);
                Object bucket = rs.getObject(5);
                sink ^= id ^ name.length() ^ amount.scale() ^ at.getSecond() ^ (bucket == null ? 0 : 1);
                n++;
            }
        }
        if (sink == Long.MIN_VALUE) {
            throw new IllegalStateException("sink");
        }
        return new Drained(n, 0);
    }

    /** Paths 2 to 4: each row's one text from the seam, decoded by the plan's columns or counted. */
    private static Drained textRows(Connection c, String sql, boolean decode) throws Exception {
        long[] n = {0};
        long[] chars = {0};
        StringBuilder sample = new StringBuilder();
        new JdbcStatements(c, new SessionState()).query(sql, List.of(), List.of(), row -> {
            if (SAMPLE > 0 && sample.length() < SAMPLE && row != null) {
                sample.append(row).append('\n');
            }
            n[0]++;
            chars[0] += row == null ? 0 : row.length();
            if (decode) {
                List<Object> cells = com.legend.result.RowDecode.cells(row, PLAN_COLUMNS);
                if (cells.size() != PLAN_COLUMNS.size()) {
                    throw new IllegalStateException("decode");
                }
            }
        });
        if (SAMPLE > 0) {
            System.out.println("[seam-sample]\n" + sample + "[/seam-sample]");
        }
        return new Drained(n[0], chars[0]);
    }

    /** Path 5: the whole text as one value; its lines are the rows. */
    private static Drained wholeText(Connection c, String sql, long rows) throws Exception {
        try (PreparedStatement st = c.prepareStatement(sql); ResultSet rs = st.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("no text");
            }
            String text = rs.getString(1);
            if (text == null) {
                // DuckDB's driver hands back no value where the text does not fit (measured: 615 MB under 256 MB)
                throw new IllegalStateException("the driver returned no value for the whole text");
            }
            long lines = text.isEmpty() ? 0 : text.chars().filter(ch -> ch == '\n').count() + 1;
            return new Drained(lines, text.length());
        }
    }
}
