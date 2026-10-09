// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.testcases.ReservedNames;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A table or schema named by a reserved word seeds and answers a query (PARK-16, docs/PARKED_WORK_LEDGER.md): the
 * statements that create, drop, fill and empty a table spell its names as a query references them
 * ({@code SqlDialect.physicalName}), so the seed and the query name one table. Before, a default-schema table
 * {@code order} was seeded with {@code Drop table if exists order;}, which DuckDB and H2 refuse. Postgres's case is
 * {@code PostgresArmTest}'s (it needs the embedded Postgres).
 */
class ReservedNamesSeedTest {

    @Test
    void onDuckDb() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            ReservedNames.seedsAndAnswers(ReservedNames.model("type: DuckDB; specification: DuckDB { }; auth: Test;"),
                    new com.legend.sql.dialect.DuckDb(), c);
        }
    }

    @Test
    void onH2() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:reservedNames")) {
            ReservedNames.seedsAndAnswers(ReservedNames.model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"),
                    new com.legend.sql.dialect.H2(), c);
        }
    }
}
