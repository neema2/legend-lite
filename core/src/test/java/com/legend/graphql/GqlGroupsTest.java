// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

/**
 * Groups (docs/GRAPHQL_STUDY_2026_10_18.md G7, §5.6; PR5) over {@link GqlExecutionTest}'s rows, through the island
 * (a relation of one row per group, its columns the response's paths). Acme: Alice 45/120k (UK), Bob 32/90k (US),
 * Carol 51/150k (UK), Dan 28/60k (no address); Globex: Eve 39/110k (JP), Frank 61/null (US); Initech: nobody;
 * Grace 24/70k (JP) has no firm. Firms founded 1990, 2005, 2010.
 */
class GqlGroupsTest {

    static final QueryService qs = new QueryService();

    static ExecutionResult rows(String document) throws SQLException {
        GqlExecutionTest.open();
        try {
            ExecutionResult r = qs.execute(GqlExecutionTest.MODEL, "#GQL{ " + document + " }#", "demo::Runtime",
                    GqlExecutionTest.conn);
            assertInstanceOf(ExecutionResult.Tabular.class, r, r.toString());
            return r;
        } finally {
            GqlExecutionTest.close();
        }
    }

    static String cell(ExecutionResult r, int row, String column) {
        int i = -1;
        for (int c = 0; c < r.columns().size(); c++) {
            if (r.columns().get(c).name().equals(column)) {
                i = c;
            }
        }
        assertTrue(i >= 0, column + " in " + r.columns());
        Object v = r.rows().get(row).get(i);
        return v == null ? "null" : v instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue())
                ? Long.toString(n.longValue()) : String.valueOf(v);
    }

    @Test
    void keyAsTheGroupingCountAndAFoldSorted() throws SQLException {
        ExecutionResult r = rows("{ PersonGroups(orderBy: [{ count: DESC }, { key: { firm: { legalName: ASC } } }])"
                + " { key { firm { legalName } } count avg { age } } }");
        assertEquals(3, r.rowCount(), r.toString());
        assertEquals("Acme", cell(r, 0, "key.firm.legalName"), r.toString());
        assertEquals("4", cell(r, 0, "count"), r.toString());
        assertEquals("39", cell(r, 0, "avg.age"), r.toString());
        assertEquals("Globex", cell(r, 1, "key.firm.legalName"), r.toString());
        assertEquals("2", cell(r, 1, "count"), r.toString());
        assertEquals("null", cell(r, 2, "key.firm.legalName"), "Grace's group, no firm: " + r);
        assertEquals("1", cell(r, 2, "count"), r.toString());
    }

    @Test
    void whereBeforeTheGroupingAndHavingAfterIt() throws SQLException {
        ExecutionResult r = rows("{ PersonGroups(where: { age: { gt: 25 } }, having: { count: { gte: 2 } },"
                + " orderBy: [{ key: { firm: { legalName: ASC } } }]) { key { firm { legalName } } count sum { salary } } }");
        assertEquals(2, r.rowCount(), "Grace (24) is filtered before the grouping; her firmless group is empty: " + r);
        assertEquals("Acme", cell(r, 0, "key.firm.legalName"), r.toString());
        assertEquals("420000", cell(r, 0, "sum.salary"), r.toString());
        assertEquals("Globex", cell(r, 1, "key.firm.legalName"), r.toString());
        assertEquals("110000", cell(r, 1, "sum.salary"), r.toString());
    }

    @Test
    void havingOnAFoldTheSelectionDoesNotCarry() throws SQLException {
        ExecutionResult r = rows("{ PersonGroups(having: { sum: { salary: { gt: 200000 } } },"
                + " orderBy: [{ count: DESC }]) { key { firm { legalName } } count } }");
        assertEquals(1, r.rowCount(), "Acme alone sums over 200k: " + r);
        assertEquals("Acme", cell(r, 0, "key.firm.legalName"), r.toString());
        assertEquals(2, r.columns().size(), "the hidden sum is dropped: " + r.columns());
    }

    @Test
    void anEnumKeyThroughAToOnePath() throws SQLException {
        ExecutionResult r = rows("{ PersonGroups(orderBy: [{ key: { address: { country: ASC } } }])"
                + " { key { address { country } } count } }");
        assertEquals(4, r.rowCount(), r.toString());
        String all = r.toString();
        assertTrue(all.contains("JP") && all.contains("UK") && all.contains("US"), all);
        int jp = -1;
        int uk = -1;
        int us = -1;
        for (int i = 0; i < r.rowCount(); i++) {
            String c = cell(r, i, "key.address.country");
            if (c.equals("JP")) {
                jp = i;
                assertEquals("2", cell(r, i, "count"));
            } else if (c.equals("UK")) {
                uk = i;
                assertEquals("2", cell(r, i, "count"));
            } else if (c.equals("US")) {
                us = i;
                assertEquals("2", cell(r, i, "count"));
            } else {
                assertEquals("1", cell(r, i, "count"), "Dan, no address: " + r);
            }
        }
        assertTrue(jp < uk && uk < us, "ascending by country: " + r);
    }

    @Test
    void aDateKeyTruncatedAndTheFoldsSortedAndPaged() throws SQLException {
        ExecutionResult r = rows("{ FirmGroups(orderBy: [{ key: { founded: DESC } }], limit: 2, offset: 1)"
                + " { key { founded(truncate: YEAR) } count } }");
        assertEquals(2, r.rowCount(), r.toString());
        assertTrue(cell(r, 0, "key.founded").startsWith("2005-01-01"), "Globex's year: " + r);
        assertTrue(cell(r, 1, "key.founded").startsWith("1990-01-01"), "Acme's year: " + r);
    }

    @Test
    void orderByAFoldAndAnAliasedKey() throws SQLException {
        ExecutionResult r = rows("{ PersonGroups(orderBy: [{ max: { salary: DESC } }])"
                + " { k: key { firm { legalName } } count top: max { salary } } }");
        assertEquals(3, r.rowCount(), r.toString());
        assertEquals("Acme", cell(r, 0, "k.firm.legalName"), r.toString());
        assertEquals("150000", cell(r, 0, "top.salary"), r.toString());
        assertEquals("Globex", cell(r, 1, "k.firm.legalName"), r.toString());
    }

    @Test
    void aKeyNotSelectedCannotOrder() {
        try {
            rows("{ PersonGroups(orderBy: [{ key: { age: ASC } }]) { key { firm { legalName } } count } }");
            throw new AssertionError("refused");
        } catch (SQLException | RuntimeException e) {
            assertTrue(String.valueOf(e.getMessage()).contains("not selected under key"), e.getMessage());
        }
    }
}
