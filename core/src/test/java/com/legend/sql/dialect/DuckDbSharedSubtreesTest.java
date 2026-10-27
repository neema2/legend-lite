// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.Execution;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A select's large shared subtrees on DuckDB, each computed once (DuckDb#select, SharedSubtrees; 2026-10-27: the
 * trading desk's option chain, Black and Scholes written as a textbook writes it, 364,053 characters of SQL without it):
 * a helper column on a select of its own beneath, read by the select's columns by its name; a helper read by a helper a
 * level beneath it; a subtree only a branch reads stays in its branch; a grouped select has none; a small one is written
 * twice, as before.
 */
class DuckDbSharedSubtreesTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table t ( sym VARCHAR(8), seq INTEGER PRIMARY KEY, px DOUBLE ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    /** A subtree of more than 50 nodes: the price times 1 to 13, summed, and 16 -- 91 times the price, and 16. */
    private static final String D = "(" + IntStream.rangeClosed(1, 13).mapToObj(k -> "($x.px->toOne() * " + k + ".0)")
            .collect(Collectors.joining(" + ")) + " + 16.0)";

    private static double d(double px) {
        return 91.0 * px + 16.0;
    }

    private static String sql(String query) {
        return Compiler.query(Compiler.compileModel(MODEL), query).plan("local::RT").sql();
    }

    private static List<String> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (sym VARCHAR(8), seq INTEGER PRIMARY KEY, px DOUBLE)");
                st.execute("INSERT INTO t VALUES ('A', 1, 3.0), ('A', 2, 4.0), ('B', 3, -2.0), ('B', 4, NULL)");
            }
            var r = Execution.execute(MODEL, "|" + query, "local::RT", c);
            return r.rows().stream().map(row -> String.join("|", row.values().stream().map(String::valueOf).toList())).toList();
        }
    }

    @Test
    void aSubtreeTwoColumnsComputeIsAHelperBeneathTheirSelect() throws Exception {
        // the helper a select of its own beneath this one, so this one's filter and order are its own still
        String query = "#>{local::DB.t}#->filter(x|$x.seq < 4)->extend(~[a:x|" + D + " * 2.0, b:x|" + D + " / 4.0])"
                + "->select(~[seq, a, b])->sort([~seq->descending()])";
        String sql = sql(query);
        assertTrue(sql.contains("__e0"), sql);
        assertEquals(1, sql.split("16\\.0", -1).length - 1, "the shared subtree written once: " + sql);
        assertEquals(List.of("3|" + d(-2.0) * 2.0 + "|" + d(-2.0) / 4.0, "2|" + d(4.0) * 2.0 + "|" + d(4.0) / 4.0,
                "1|" + d(3.0) * 2.0 + "|" + d(3.0) / 4.0), run(query));
    }

    @Test
    void aHelperReadByAHelperIsALevelBeneathIt() throws Exception {
        // d read twice by e = d * d + d, and e twice by the columns: two levels of helpers
        String e = "((" + D + " * " + D + ") + " + D + ")";
        String query = "#>{local::DB.t}#->filter(x|$x.seq < 3)->extend(~[a:x|" + e + " * 2.0, b:x|" + e + " / 4.0])"
                + "->select(~[seq, a, b])->sort([~seq->ascending()])";
        String sql = sql(query);
        assertTrue(sql.contains("__e1"), sql);
        assertEquals(1, sql.split("16\\.0", -1).length - 1, "the innermost subtree written once: " + sql);
        double e1 = d(3.0) * d(3.0) + d(3.0);
        double e2 = d(4.0) * d(4.0) + d(4.0);
        assertEquals(List.of("1|" + e1 * 2.0 + "|" + e1 / 4.0, "2|" + e2 * 2.0 + "|" + e2 / 4.0), run(query));
    }

    @Test
    void aSubtreeOnlyABranchComputesStaysInItsBranch() {
        // the subtree a CASE's branch takes only for a positive price: a helper would compute it on every row
        String query = "#>{local::DB.t}#->extend(~[a:x|if($x.px->toOne() > 0.0, |" + D + " * 2.0, |0.0),"
                + " b:x|if($x.px->toOne() > 0.0, |" + D + " * 3.0, |0.0)])->select(~[seq, a, b])";
        assertFalse(sql(query).contains("__e"), sql(query));
    }

    @Test
    void aGroupedSelectHasNoHelpers() {
        String query = "#>{local::DB.t}#->groupBy(~[sym], ~[a:x|" + D + ":y|$y->sum(), b:x|" + D + ":y|$y->max()])";
        assertFalse(sql(query).contains("__e"), sql(query));
    }

    @Test
    void aSmallSubtreeIsWrittenTwiceAsBefore() {
        // under 50 nodes: computed twice costs DuckDB less than a select of its own
        String small = "(($x.px->toOne() * $x.px->toOne()) + 16.0)";
        String sql = sql("#>{local::DB.t}#->extend(~[a:x|" + small + " * 2.0, b:x|" + small + " / 4.0])->select(~[seq, a, b])");
        assertFalse(sql.contains("__e"), sql);
    }
}
