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
 * THE DUCKDB PLAN'S STEPS 1-3, HELD (the lean program, 2026-11-16): each shape's SQL as the lean form writes it, and
 * its rows, on every in-process lane -- a relation's size counts its rows with no wrapper (step 1), a sort key that is
 * no output is ordered by where it stands (step 2), and a filter over a grouped relation is the groups' HAVING where
 * the dialect merges selects (step 3).
 */
class LeanShapesTest extends AbstractDatabaseTest {

    private static final String PEOPLE =
            "#>{store::PersonDatabase.T_PERSON}#->select(~[ID, LAST_NAME, AGE_VAL])";

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

    private static int count(String sql, String pattern) {
        Matcher m = Pattern.compile(pattern).matcher(sql);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    @Test
    @DisplayName("a filtered relation's size counts the table's rows under the filter: one select, no wrapper")
    void aSizeCountsItsRows() throws SQLException {
        String q = PEOPLE + "->filter(r|$r.AGE_VAL > 29)->size()->from(test::TestRuntime)";
        String sql = generateSql(q);
        System.out.println("[lean-shapes] size: " + sql);
        assertEquals(2, count(sql, "(?i)\\bSELECT\\b"), sql);
        assertEquals(2L, ((Number) executeRelation(q).rows().get(0).get(0)).longValue(), sql);
    }

    @Test
    @DisplayName("a sort by an expression is ordered by where it stands: no hidden column, no wrapper")
    void aSortKeyThatIsNoOutputIsOrderedWhereItStands() throws SQLException {
        String q = PEOPLE + "->sort(descending(~AGE_VAL))->select(~[ID])->from(test::TestRuntime)";
        String sql = generateSql(q);
        System.out.println("[lean-shapes] sort: " + sql);
        assertFalse(sql.contains("_rows1"), sql);
        assertEquals(List.of(3, 1, 2), executeRelation(q).rows().stream()
                .map(r -> ((Number) r.get(0)).intValue()).toList(), sql);
    }

    @Test
    @DisplayName("a filter over groups is the groups' HAVING where the dialect merges selects; the rows the same")
    void aFilterOverGroupsIsHaving() throws SQLException {
        String q = PEOPLE + "->groupBy(~LAST_NAME, ~total: r|$r.AGE_VAL : y|$y->plus())"
                + "->filter(g|$g.total > 50)->from(test::TestRuntime)";
        String sql = generateSql(q);
        System.out.println("[lean-shapes] having: " + sql);
        var rows = executeRelation(q).rows();
        assertEquals(1, rows.size(), sql);
        assertEquals("Smith", rows.get(0).get(0), sql);
        assertEquals(58L, ((Number) rows.get(0).get(1)).longValue(), sql);
        assertTrue(count(sql, "(?i)\\bHAVING\\b") == 1 || count(sql, "(?i)\\bSELECT\\b") == 2, sql);
    }
}
