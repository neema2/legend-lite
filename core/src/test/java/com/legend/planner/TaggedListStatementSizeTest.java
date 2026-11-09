// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

/**
 * A mixed list's statements stay LINEAR in the list (Track L's size, 2026-11-03): a tagged list reads its arrays at
 * positions the list holds ({@code SqlFn.LIST_AT}), each array and each position written once. Until then every
 * element read was the general {@code LIST_GET}, whose negative-index arms write the index two to five times and the
 * list up to three on every dialect but DuckDB, so nested positions multiplied: PCT's mixed-kind
 * {@code removeDuplicates()->sort()} was 6.9 MB on MySQL, 3.8 MB on MariaDB, 1.9 MB on SQLite (past its 1 MB limit)
 * and 715K chars on Snowflake (its channel B essential 595 s -> 1,059 s, past its 900 s timeout); the first bad commit
 * 63ddd3f4 by the Snowflake lane's bisect. Measured at the fix: at most 84K chars on every dialect (MySQL's), the
 * ceiling below with room for a channel's growth, never for a multiplication. A dialect that refuses the shape (H2's
 * and BigQuery's list encodings) refuses it by name.
 */
class TaggedListStatementSizeTest {

    /** The ceiling, in chars: 84K measured at the fix (MySQL), with room for a channel, not for a multiplication.
     *  150K -> 250K (2026-11-03, round 18: the root's rows in the list's own order -- by comparable, then position --
     *  read the unnested position twice more, and a database that writes the position list at each read of it
     *  (Snowflake's GET(list, INDEX), SQLite's, MariaDB's and MySQL's JSON paths) writes it twice more: MySQL 84K ->
     *  170K, linear; the order is correctness, BigQuery's and MySQL's roots returned the elements out of order). */
    private static final int CEILING = 250_000;

    private static final java.util.List<String> QUERIES = java.util.List.of(
            "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()",
            "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()->sort()",
            "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()->sort() == [1, 2, 3, '1', '3']",
            "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates(eq_Any_1__Any_1__Boolean_1_)");

    @Test
    void aMixedListsStatementsAreLinearOnEveryDialect() {
        StringBuilder over = new StringBuilder();
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(PlanMakerTest.model(type.name()));
            for (String q : QUERIES) {
                int size;
                try {
                    size = Compiler.query(ctx, q).executionPlan("s::RT").answerSql().statement().length();
                } catch (com.legend.sql.DialectCapability refused) {
                    continue;   // the dialect refuses the shape by name: no statement to measure
                }
                if (size > CEILING) {
                    over.append("\n  ").append(type).append(' ').append(size).append(" chars: ").append(q);
                }
            }
        }
        assertTrue(over.isEmpty(), "a mixed list's statement past " + CEILING + " chars (a multiplied element read?):"
                + over);
    }
}
