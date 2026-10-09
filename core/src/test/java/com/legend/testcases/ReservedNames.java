// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import com.legend.setup.CsvSeed;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tables and a schema named by reserved words (PARK-16, docs/PARKED_WORK_LEDGER.md): their model, rows, and a seed that
 * must answer a query over each. {@code ReservedNamesSeedTest} runs them on DuckDB and H2, {@code PostgresArmTest} on
 * Postgres.
 */
public final class ReservedNames {

    private ReservedNames() {
    }

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
}
