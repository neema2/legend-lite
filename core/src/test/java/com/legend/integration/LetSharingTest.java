// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A RELATION READ MORE THAN ONCE IS WRITTEN ONCE (2026-11-04, {@code lowering.SharedRelations}): a let's value is
 * spliced at every read of its name, so a relation bound once and read twice is one node of the statement, and it
 * lowers to one common table expression the reads name. Before, its SQL was pasted at each read, and a let read twice
 * in a function called three deep was pasted eight times.
 */
class LetSharingTest extends AbstractDatabaseTest {

    /** A let read twice: the relation joined to itself (a 1:1 join on the key, so its rows are its own). */
    private static final String FUNCTIONS = """
            ###Pure
            function test::selfJoined(t: Relation<(ID:Integer, AGE_VAL:Integer)>[1]): Relation<(ID:Integer, AGE_VAL:Integer)>[1]
            {
              let base = $t->filter(r|$r.AGE_VAL > 0);
              $base->join($base->rename(~ID, ~ID2)->rename(~AGE_VAL, ~AGE2), meta::pure::functions::relation::JoinKind.INNER,
                          {a, b|$a.ID == $b.ID2})->select(~[ID, AGE_VAL]);
            }
            """;

    private static final String PEOPLE =
            "#>{store::PersonDatabase.T_PERSON}#->select(~[ID, AGE_VAL])";

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected String getCompletePureModelWithRuntime() {
        return super.getCompletePureModelWithRuntime() + "\n" + FUNCTIONS;
    }

    private static int count(String sql, String pattern) {
        Matcher m = Pattern.compile(pattern).matcher(sql);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    private static int tableReads(String sql) {
        return count(sql, "(?i)\"?T_PERSON\"?\\s");
    }

    @Test
    @DisplayName("a let read twice is one common table expression: the table read once, the rows the same")
    void aLetReadTwiceIsWrittenOnce() throws SQLException {
        String q = "test::selfJoined(" + PEOPLE + ")->sort(ascending(~ID))->from(test::TestRuntime)";
        String sql = generateSql(q);
        assertEquals(1, tableReads(sql), sql);
        assertTrue(sql.contains("__let0"), sql);
        var result = executeRelation(q);
        assertEquals(List.of(List.of(1, 30), List.of(2, 28), List.of(3, 45)),
                result.rows().stream().map(r -> List.of(((Number) r.get(0)).intValue(),
                        ((Number) r.get(1)).intValue())).toList());
    }

    @Test
    @DisplayName("a let read twice inside a function called three deep: one table read, not eight")
    void nestedCallsStayLinear() throws SQLException {
        String q = "test::selfJoined(test::selfJoined(test::selfJoined(" + PEOPLE + ")))"
                + "->sort(ascending(~ID))->from(test::TestRuntime)";
        String sql = generateSql(q);
        assertEquals(1, tableReads(sql), sql);
        assertEquals(3, count(sql, "__let\\d+\\s+AS\\s*\\("), sql);
        var result = executeRelation(q);
        assertEquals(List.of(List.of(1, 30), List.of(2, 28), List.of(3, 45)),
                result.rows().stream().map(r -> List.of(((Number) r.get(0)).intValue(),
                        ((Number) r.get(1)).intValue())).toList());
    }

    @Test
    @DisplayName("a shared relation that binds a parameter: written once, but in place on H2 (which loses the value)")
    void aSharedRelationThatBindsAParameter() {
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(getCompletePureModelWithRuntime()),
                        "{min: Integer[1] | test::selfJoined(" + PEOPLE + "->filter(r|$r.AGE_VAL > $min))}")
                .executionPlan("test::TestRuntime").answerSql().statement();
        boolean h2 = backend().type().toString().startsWith("H2");
        assertEquals(h2 ? 0 : 1, count(sql, "__let\\d+\\s+AS\\s*\\("), sql);
        assertEquals(h2 ? 2 : 1, tableReads(sql), sql);
    }

    @Test
    @DisplayName("a relation read once is written where it stands: no common table expression")
    void aRelationReadOnceIsNotShared() {
        String sql = generateSql(PEOPLE + "->filter(r|$r.AGE_VAL > 0)->from(test::TestRuntime)");
        assertFalse(sql.contains("__let"), sql);
    }
}
