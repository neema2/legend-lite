// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Execution;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pure's {@code maxBy}/{@code minBy} over a group (SQL's ARG_MAX/ARG_MIN) and {@code timeBucket} on the product's H2
 * (2.1.214), which has none of the three: a candle's open and close and its bucket failed on H2 with "Function ARG_MIN
 * not found" and a refusal (the stress corpus's TF_InstrumentVwap and TF_TapeCandles, 2026-10-25). H2 spells the
 * first as the group's values ordered by the key, NULL keys left out, at the first place; the second as the
 * microseconds since the origin floored to the bucket's multiple.
 */
class H2ArgExtremeAndTimeBucketTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table t ( sym VARCHAR(8), seq INTEGER, px DOUBLE, tm TIMESTAMP NOT NULL ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static List<String> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:argext" + System.nanoTime())) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (sym VARCHAR(8), seq INTEGER, px DOUBLE, tm TIMESTAMP NOT NULL)");
                st.execute("INSERT INTO t VALUES ('A', 2, 10.5, TIMESTAMP '2024-06-03 09:04:59'),"
                        + " ('A', 1, 10.0, TIMESTAMP '2024-06-03 09:00:01'), ('A', 3, 11.0, TIMESTAMP '2024-06-03 09:05:00'),"
                        + " ('A', NULL, 99.0, TIMESTAMP '1969-12-31 23:58:30'), ('B', 7, 20.0, TIMESTAMP '2024-06-04 00:00:00'),"
                        + " ('B', 6, NULL, TIMESTAMP '2024-06-04 00:00:00')");
            }
            var r = Execution.execute(MODEL, query, "local::RT", c);
            return r.rows().stream().map(row -> String.join("|", row.values().stream().map(String::valueOf).toList())).toList();
        }
    }

    @Test
    void openAndCloseOfAGroupByTheKey() throws Exception {
        // the NULL-keyed row (px 99) decides nothing: A's open is seq 1's, its close seq 3's; nor does B's NULL value
        // at seq 6, as DuckDB's arg_min skips it: B's open is seq 7's
        assertEquals(List.of("A|10.0|11.0", "B|20.0|20.0"), run("|#>{local::DB.t}#->groupBy(~[sym], ~["
                + "o: x|meta::pure::functions::math::mathUtility::rowMapper($x.px, $x.seq) : y|$y->minBy(),"
                + " c: x|meta::pure::functions::math::mathUtility::rowMapper($x.px, $x.seq) : y|$y->maxBy()])"
                + "->sort([~sym->ascending()])"));
    }

    @Test
    void fiveMinuteBucketsFloorFromTheEpoch() throws Exception {
        // 09:04:59 and 09:00:01 share 09:00, 09:05:00 opens its own, and an instant before 1970 floors DOWN
        assertEquals(List.of("1969-12-31T23:55:00+0000", "2024-06-03T09:00:00+0000", "2024-06-03T09:00:00+0000",
                        "2024-06-03T09:05:00+0000", "2024-06-04T00:00:00+0000", "2024-06-04T00:00:00+0000"),
                run("|#>{local::DB.t}#->extend(~b: x|$x.tm->timeBucket(5, DurationUnit.MINUTES))->select(~[b])"
                        + "->sort([~b->ascending()])"));
    }
}
