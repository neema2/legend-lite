// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Execution;
import com.legend.error.ModelException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lite's {@code ema(values, alpha)} (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trader screens' EMA, 2026-10-25):
 * an ordered window's exponential moving average, the first value and then {@code alpha * value + (1 - alpha) * so
 * far}, a null skipped, executed on DuckDB, whose spelling folds the window's values as a list; and refused by name
 * where its values have no order, a group's or an unordered window's.
 */
class EmaTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table t ( sym VARCHAR(8), seq INTEGER PRIMARY KEY, px DOUBLE, qty INTEGER ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static List<String> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (sym VARCHAR(8), seq INTEGER PRIMARY KEY, px DOUBLE, qty INTEGER)");
                st.execute("INSERT INTO t VALUES ('A', 1, 10.0, 4), ('A', 2, 12.0, 8), ('A', 3, NULL, 2), ('A', 4, 8.0, 6),"
                        + " ('B', 5, NULL, 1), ('B', 6, 20.0, 3)");
            }
            var r = Execution.execute(MODEL, query, "local::RT", c);
            return r.rows().stream().map(row -> String.join("|", row.values().stream().map(String::valueOf).toList())).toList();
        }
    }

    @Test
    void aRunningEmaFoldsEachPartitionsValuesInItsOrderSkippingNulls() throws Exception {
        // A: 10, then 11 (half of 12 and half of 10), held at 11 over the null, then 9.5; B: null before its first
        // value, then 20. A whole-number column's EMA is a Float.
        assertEquals(List.of("A|1|10.0|4.0", "A|2|11.0|6.0", "A|3|11.0|4.0", "A|4|9.5|5.0", "B|5|null|1.0", "B|6|20.0|2.0"),
                run("|#>{local::DB.t}#->extend(over(~sym, ~seq->ascending(), rows(unbounded(), 0)),"
                        + " ~[e:{p,w,r|$r.px}:y|$y->ema(0.5), q:{p,w,r|$r.qty}:y|$y->ema(0.5)])"
                        + "->select(~[sym, seq, e, q])->sort([~seq->ascending()])"));
    }

    private static final String SERIES_MODEL = """
            ###Relational
            Database local::DB ( Table s ( sym VARCHAR(8) NOT NULL, seq INTEGER PRIMARY KEY, px DOUBLE NOT NULL,
              npx DOUBLE ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    @Test
    void aRunningEmaOfValuesNeverNullReadsOnlyTheRowsADoubleCanWeighAndAnswersAsTheWholeHistory() throws Exception {
        // 2026-10-27: the fold of each row's whole history is quadratic (24 s for 100,000 prints); where the values are
        // never null it reads the rows whose weight a double holds -- 106 for an alpha of 0.34 -- and gives the whole
        // history's answer: here px (NOT NULL) against npx, the same values in a column that may be null, which keeps
        // the whole history. 3,000 rows in two names, so the cut cuts.
        String query = "|#>{local::DB.s}#->extend(over(~sym, ~seq->ascending(), rows(unbounded(), 0)),"
                + " ~[e:{p,w,r|$r.px}:y|$y->ema(0.34), whole:{p,w,r|$r.npx}:y|$y->ema(0.34)])"
                + "->select(~[seq, e, whole])->sort([~seq->ascending()])";
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(SERIES_MODEL), query.substring(1))
                .plan("local::RT").sql();
        assertTrue(sql.contains("ROWS BETWEEN CAST(") && sql.contains("UNBOUNDED PRECEDING"),
                "the never-null column's fold reads a computed number of rows, the nullable one's the whole: " + sql);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE s (sym VARCHAR(8) NOT NULL, seq INTEGER PRIMARY KEY, px DOUBLE NOT NULL, npx DOUBLE)");
                st.execute("INSERT INTO s SELECT CASE WHEN i % 2 = 0 THEN 'A' ELSE 'B' END, i, p, p FROM ("
                        + "SELECT i, 100 + sum(sin(i * 0.7) + cos(i * 1.3)) OVER (ORDER BY i) AS p FROM range(3000) r(i))");
            }
            var rows = Execution.execute(SERIES_MODEL, query, "local::RT", c).rows();
            assertEquals(3000, rows.size());
            for (var row : rows) {
                double cut = ((Number) row.values().get(1)).doubleValue();
                double whole = ((Number) row.values().get(2)).doubleValue();
                assertEquals(whole, cut, Math.ulp(whole) * 4, "row " + row.values().get(0));
            }
        }
    }

    @Test
    void anEmaWhoseValuesHaveNoOrderIsRefusedByName() {
        ModelException grouped = assertThrows(ModelException.class, () -> run("|#>{local::DB.t}#->groupBy(~[sym],"
                + " ~[e:x|$x.px:y|$y->ema(0.5)])"));
        assertTrue(grouped.getMessage().contains("ema folds its values in their order, and these have none"),
                grouped.getMessage());
        ModelException unordered = assertThrows(ModelException.class, () -> run("|#>{local::DB.t}#->extend(over(~sym),"
                + " ~[e:{p,w,r|$r.px}:y|$y->ema(0.5)])"));
        assertTrue(unordered.getMessage().contains("these have none"), unordered.getMessage());
    }
}
