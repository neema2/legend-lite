// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The legacy engine-text printer writes legend-engine's own spelling of windows and aggregates (E-4b): lowercase
 * keywords and names, {@code listagg} for the string aggregate, an ordered one {@code within group}. The queries and the
 * engine's answers are docs/execution-plan-boundary-2026-10-05/legacy-text/ (lN.pure, engine-lN.sql, 4.145.0); each
 * expected fragment below is the engine's text, its alias {@code "t_0"} read as the printer's {@code "root"}. The text
 * is {@code toSQLString}'s, through the platform's own path.
 */
class LegacyTextTest {

    private static final String MODEL = """
            ###Relational
            Database test::DB ( Table T (id INTEGER PRIMARY KEY, grp INTEGER, name VARCHAR(200), flag VARCHAR(10)) )
            ###Mapping
            Mapping test::M ( )
            """;

    /** {@code toSQLString(query, test::M, DatabaseType.H2, [])}: the legacy printer's text for {@code query}. */
    private static String legacyText(String query) throws Exception {
        String body = "|meta::relational::functions::sqlstring::toSQLString(" + query
                + ", test::M, meta::relational::runtime::DatabaseType.H2, [])";
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            var r = com.legend.Execution.execute(com.legend.test.StorelessRuntime.with(MODEL,
                    com.legend.model.ConnectionDefinition.DatabaseType.DuckDB), body,
                    com.legend.test.StorelessRuntime.RUNTIME, c);
            var text = (com.legend.exec.ExecutionResult.Scalar) java.util.Objects.requireNonNull(r);
            return String.valueOf(text.value());
        }
    }

    private static void writes(String query, String engineFragment) throws Exception {
        String text = legacyText(query);
        assertTrue(text.contains(engineFragment), "expected legend-engine's " + engineFragment + "\nin " + text);
    }

    @Test
    void anOrderedStringAggregateIsListaggWithinGroup() throws Exception {
        // l1
        writes("|#>{test::DB.T}#->groupBy(~grp, ~names : g | $g->joinStrings(~name, ',', ~id->ascending()))",
                "listagg(\"root\".name, ',') within group (order by \"root\".id asc) as \"names\"");
    }

    @Test
    void anUnorderedStringAggregateIsListagg() throws Exception {
        // l9
        writes("|#>{test::DB.T}#->groupBy(~grp, ~names : x|$x.name : y|$y->joinStrings(','))",
                "listagg(\"root\".name, ',') as \"names\"");
    }

    @Test
    void anOrderedStringAggregateInAHavingIsListaggWithinGroupToo() throws Exception {
        // l12
        writes("|#>{test::DB.T}#->groupBy(~grp, ~names : g | $g->joinStrings(~name, ',', ~id->ascending()))"
                        + "->filter(r|$r.names->isNotEmpty())",
                "having listagg(\"root\".name, ',') within group (order by \"root\".id asc) is not null");
    }

    @Test
    void aWindowsKeywordsAreLowercase() throws Exception {
        // l3
        writes("|#>{test::DB.T}#->extend(over(~grp, ~id->descending()), ~s:{p,w,r|$r.id}:y|$y->plus())",
                "sum(\"root\".id) over (partition by \"root\".grp order by \"root\".id desc) as \"s\"");
    }

    @Test
    void aStringAggregateOverAWindowIsListagg() throws Exception {
        // l5
        writes("|#>{test::DB.T}#->extend(over(~grp, ~id->ascending()), ~names:{p,w,r|$r.name}:y|$y->joinStrings(','))",
                "listagg(\"root\".name, ',') over (partition by \"root\".grp order by \"root\".id asc) as \"names\"");
    }

    @Test
    void aWindowsFrameIsLowercase() throws Exception {
        // l14
        writes("|#>{test::DB.T}#->extend(over(~grp, ~id->ascending(),"
                        + " rows(meta::pure::functions::relation::unbounded(), 0)), ~s:{p,w,r|$r.id}:y|$y->plus())",
                "sum(\"root\".id) over (partition by \"root\".grp order by \"root\".id asc"
                        + " rows between unbounded preceding and current row) as \"s\"");
    }
}
