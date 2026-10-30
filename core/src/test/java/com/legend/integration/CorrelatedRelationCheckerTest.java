// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A relation read inside another's row lambda (a correlated subquery in SQL): it reads the enclosing row by that
 * row's own variable, in an {@code exists} predicate, in a join condition, and as a {@code map->toOne} value; and a
 * sliced relation's {@code exists} tests the slice's rows only.
 */
class CorrelatedRelationCheckerTest extends AbstractDatabaseTest {

    private static final String PEOPLE = """
            #TDS
            id, dept
            1, 10
            2, 20
            3, 30
            #""";

    private static final String ORDERS = """
            #TDS
            person, amount
            1, 5
            1, 7
            2, 9
            #""";

    /** Two rows of one group, numbered by v: rn is a window column, which a filter cannot fold in place. */
    private static final String WINDOWED = """
            #TDS
            g, v
            1, 10
            1, 20
            #->extend(over(~g, ~v->ascending()), ~rn:{p,w,r|$p->rowNumber($r)})""";


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

    private List<Object> ids(String query) throws SQLException {
        return executeRelation(query).rows().stream().map(r -> r.get(0)).toList();
    }

    @Test
    @DisplayName("exists over a limited relation tests the limited rows only")
    void existsOverALimitedRelationTestsItsSlice() throws SQLException {
        // the largest amount is person 2's; the predicate must not run before the limit (person 1 has the others)
        assertEquals(List.of(2L), ids("|" + PEOPLE + "->filter(p|" + ORDERS
                + "->sort(descending(~amount))->limit(1)->exists(o|$o.person == $p.id))->select(~[id])"
                + "->sort(ascending(~id))"));
    }

    @Test
    @DisplayName("a join condition reads an enclosing row by its own variable, not a side's same-named column")
    void aJoinConditionReadsTheEnclosingRow() throws SQLException {
        // the right side has its own 'id': $p.id is the enclosing person's, never the department's
        String departments = """
                #TDS
                id, label
                10, a
                20, b
                #""";
        assertEquals(List.of(1L, 2L), ids("|" + PEOPLE + "->filter(p|" + ORDERS + "->join(" + departments
                + ", meta::pure::functions::relation::JoinKind.INNER, {o, d|($o.person == $p.id) && ($d.id == $p.dept)})"
                + "->exists(x|true))->select(~[id])->sort(ascending(~id))"));
    }

    @Test
    @DisplayName("a map->toOne over a filter on a window column, at the query's root: no enclosing row to consult")
    void aRootMapToOneOverAWindowFilter() throws SQLException {
        var result = executeRelation("|" + WINDOWED + "->filter(r|$r.rn == 1)->map(r|$r.v)->toOne()");
        assertEquals(10L, result.rows().get(0).get(0));
    }

    @Test
    @DisplayName("a row's own column is its own, never the enclosing row's column of the same name")
    void aRowsOwnColumnIsNeverTheEnclosingRows() throws SQLException {
        // the outer rows have an rn too (5 and 7): the inner filter's $r.rn is the window column
        var result = executeRelation("""
                |#TDS
                id, rn
                1, 5
                2, 7
                #->extend(~[first:o|""" + WINDOWED + "->filter(r|$r.rn == 1)->map(r|$r.v)->toOne()])"
                + "->select(~[id, first])->sort(ascending(~id))");
        assertEquals(List.of(10L, 10L), result.rows().stream().map(r -> r.get(1)).toList());
    }

    @Test
    @DisplayName("a correlated map->toOne is the enclosing row's value")
    void aCorrelatedMapToOneIsAScalarSubquery() throws SQLException {
        var result = executeRelation("|" + PEOPLE + "->extend(~[top:p|" + ORDERS
                + "->filter(o|$o.person == $p.id)->groupBy(~[person], ~[m:o|$o.amount:y|$y->max()])"
                + "->map(x|$x.m)->toOne()])->select(~[id, top])->sort(ascending(~id))");
        assertEquals(List.of(7L, 9L), result.rows().subList(0, 2).stream().map(r -> r.get(1)).toList());
        // no order: SQL's NULL
        assertEquals(null, result.rows().get(2).get(1));
    }
}
