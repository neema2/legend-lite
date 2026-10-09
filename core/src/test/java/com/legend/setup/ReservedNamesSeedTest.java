// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A table or schema named by a reserved word seeds and answers a query (PARK-16, docs/PARKED_WORK_LEDGER.md): the
 * statements that create, drop, fill and empty a table spell its names as a query references them
 * ({@code SqlDialect.physicalName}), so the seed and the query name one table. Before, a default-schema table
 * {@code order} was seeded with {@code Drop table if exists order;}, which DuckDB and H2 refuse. Postgres's case is
 * {@code PostgresArmTest}'s (it needs the embedded Postgres).
 */
public class ReservedNamesSeedTest {

    /** A default-schema table {@code order} and a table in a schema {@code select}; the connection's type left open. */
    public static String model(String connection) {
        return """
                ###Relational
                Database s::DB
                (
                  Table order ( ID INTEGER PRIMARY KEY, NAME VARCHAR(10) )
                  Schema select ( Table T ( ID INTEGER PRIMARY KEY ) )
                )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; %s }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(connection);
    }

    /** The two tables' rows, in the seed's CSV blocks. */
    public static final String CSV = """
            default
            order
            ID,NAME
            1,a
            2,b
            -----
            select
            T
            ID
            7
            """;

    /** Seeds {@link #CSV} on {@code c} through the product's seed, and reads each table back by a query. */
    public static void seedsAndAnswers(String model, com.legend.sql.dialect.SqlDialect dialect, Connection c)
            throws SQLException {
        try (Statement s = c.createStatement()) {
            for (String sql : CsvSeed.sqls(CSV, "s::DB", Compiler.compileModel(model), dialect)) {
                s.execute(sql);
            }
        }
        ExecutionResult order = Execution.execute(model,
                "#>{s::DB.order}#->select(~[ID, NAME])->sort(~ID->ascending())", "s::RT", c);
        assertEquals(List.of(List.of(1, "a"), List.of(2, "b")), rows(order), dialect.getClass().getSimpleName());
        ExecutionResult t = Execution.execute(model, "#>{s::DB.select.T}#->select(~[ID])", "s::RT", c);
        assertEquals(List.of(List.of(7)), rows(t), dialect.getClass().getSimpleName());
    }

    private static List<List<Object>> rows(ExecutionResult r) {
        return java.util.Objects.requireNonNull(r).rows().stream()
                .map(row -> row.values().stream().map(v -> v instanceof Number n ? (Object) n.intValue() : v).toList())
                .toList();
    }

    @Test
    void onDuckDb() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            seedsAndAnswers(model("type: DuckDB; specification: DuckDB { }; auth: Test;"),
                    new com.legend.sql.dialect.DuckDb(), c);
        }
    }

    @Test
    void onH2() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:reservedNames")) {
            seedsAndAnswers(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"),
                    new com.legend.sql.dialect.H2(), c);
        }
    }
}
