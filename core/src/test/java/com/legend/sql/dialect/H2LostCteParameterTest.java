// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The H2 defect {@link GridReportCtesInline} exists for, pinned (the planner-driver seam program's S3c, 2026-10-30): a
 * bound parameter inside a common table expression that another one reads loses its value; the same query with the
 * parameter in a derived table keeps it. The day the H2 the build pins answers 1 to the first, this test fails, and the
 * pass is deleted with it.
 */
class H2LostCteParameterTest {

    private static long count(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:ctepar" + System.nanoTime());
                PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, "x");
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1);
            }
        }
    }

    @Test
    void aParameterInACteAnotherCteReadsIsLost() throws Exception {
        assertEquals(0L, count("WITH a AS (SELECT CAST(? AS VARCHAR) AS c), s AS (SELECT COUNT(a.c) AS n FROM a)"
                + " SELECT n FROM s"));
    }

    @Test
    void theSameParameterInADerivedTableIsKept() throws Exception {
        assertEquals(1L, count("WITH s AS (SELECT COUNT(a.c) AS n FROM (SELECT CAST(? AS VARCHAR) AS c) AS a)"
                + " SELECT n FROM s"));
    }
}
