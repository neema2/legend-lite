// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A FOLD OVER A FULL OUTER JOIN, ON A DATABASE WITHOUT ONE (C5-J's batch, 2, 2026-11-16): where a database has no FULL
 * OUTER JOIN, the dialect writes it as the LEFT join's rows UNION ALL the RIGHT join's unmatched ones
 * ({@code CarrierStrategies}). Until then a select that folds the join's rows -- its groups, aggregates, windows or
 * DISTINCT -- was copied into each branch whole, so each half was folded apart: a group with rows in both halves came out
 * twice, its total split between them, and a count of the join was two rows. The data makes such a group: Pure's
 * {@code ==} matches a NULL key to a NULL key, so L's third row meets R's third in the LEFT branch, both with K NULL;
 * R's second row (K2 3) matches nothing, so it is in the RIGHT branch, its K NULL too. Grouped by K, the two are one
 * group. On each in-process database (DuckDB and SQLite write the join itself; H2 emulates it), the answer is the
 * join's own. Red on revert: the emulation without its fold check answers K NULL twice on H2, and its count fails.
 */
class FullOuterFoldTest {

    private static final String MODEL = """
            ###Relational
            Database store::DB
            (
                Table L (ID INTEGER PRIMARY KEY, K INTEGER, V INTEGER)
                Table R (ID2 INTEGER PRIMARY KEY, K2 INTEGER, W INTEGER)
            )
            ###Connection
            RelationalDatabaseConnection store::Conn { store: store::DB; CONNECTION }
            ###Runtime
            Runtime test::RT { mappings: [ ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            ###Data
            Data test::D
            {
              Relational
              #{
                default.L:
                  'ID,K,V\\n1,1,10\\n2,2,20\\n3,,30\\n';
                default.R:
                  'ID2,K2,W\\n1,2,200\\n2,3,300\\n3,,400\\n';
              }#
            }
            """;

    /** The join: (1, -) from L alone, (2, 2) and (-, -) matched, (-, 3) from R alone -- four rows. */
    private static final String JOIN = "|#>{store::DB.L}#->join(#>{store::DB.R}#,"
            + " meta::pure::functions::relation::JoinKind.FULL, {a, b | $a.K == $b.K2})";

    /** Grouped by K: the NULL group holds the matched (-, -) row, from the LEFT branch, and R's unmatched (-, 3) row,
     *  from the RIGHT: one group of two, its V the matched row's 30, its W 400 + 300. */
    @Test
    void aGroupWithRowsInBothHalvesIsOneGroup() {
        List<String> expected = List.of("-|2|30|700", "1|1|10|-", "2|1|20|200");
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals(expected, rows(backend, JOIN + "->groupBy(~[K], ~[n:x|1:y|$y->count(),"
                    + " v:x|$x.V:y|$y->sum(), w:x|$x.W:y|$y->sum()])"), backend.name());
        }
    }

    /** A total over the whole join: one count, never one per half. */
    @Test
    void aTotalIsOneRow() {
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals(List.of("4"), rows(backend, JOIN + "->size()"), backend.name());
            assertEquals(List.of("1|4"), rows(backend, JOIN + "->extend(~one:x|1)->groupBy(~[one],"
                    + " ~[n:x|1:y|$y->count()])"), backend.name());
        }
    }

    /** DISTINCT over the join: the NULL key once. */
    @Test
    void distinctKeysAreDistinctOverTheWholeJoin() {
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals(List.of("-", "1", "2"), rows(backend, JOIN + "->select(~[K])->distinct()"), backend.name());
        }
    }

    /** The answer's rows, each its cells joined by {@code |} (a NULL as {@code -}), sorted. */
    private static List<String> rows(TestBackend backend, String query) {
        List<List<Object>> rows = Lean.service(MODEL.replace("CONNECTION", backend.connectionBody()), query, "test::RT")
                .rows();
        List<String> out = new ArrayList<>();
        for (List<Object> row : rows) {
            List<String> cells = new ArrayList<>();
            for (Object v : row) {
                cells.add(v == null ? "-" : String.valueOf(v));
            }
            out.add(String.join("|", cells));
        }
        out.sort(null);
        return out;
    }
}
