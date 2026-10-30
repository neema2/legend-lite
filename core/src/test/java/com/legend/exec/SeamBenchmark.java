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
        new JdbcStatements(c, new SessionState()).query(sql, List.of(), List.of(), row -> {
            n[0]++;
            chars[0] += row == null ? 0 : row.length();
            if (decode) {
                List<Object> cells = com.legend.result.RowDecode.cells(row, PLAN_COLUMNS);
                if (cells.size() != PLAN_COLUMNS.size()) {
                    throw new IllegalStateException("decode");
                }
            }
        });
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
