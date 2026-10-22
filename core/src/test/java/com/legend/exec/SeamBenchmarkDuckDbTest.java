// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

/** {@link SeamBenchmark} on DuckDB: a generated relation of {@code seam.rows} rows (10 million by default). */
@Tag("benchmark")
final class SeamBenchmarkDuckDbTest {

    private static final long DUCKDB_ROWS = Long.getLong("seam.rows", 10_000_000L);

    @Test
    void duckdb() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            SeamBenchmark.exec(c, "CREATE VIEW bench AS SELECT i AS id, 'name' || CAST(i AS VARCHAR) AS name,"
                    + " CAST(i AS DECIMAL(18,2)) / 7 AS amount,"
                    + " TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i % 86400) SECOND AS ts,"
                    + " CASE WHEN i % 10 = 0 THEN NULL ELSE CAST(i % 1000 AS INTEGER) END AS bucket"
                    + " FROM range(" + DUCKDB_ROWS + ") t(i)");
            SeamBenchmark.run("duckdb", c, Databases.dialect(DatabaseType.DuckDB), false, DUCKDB_ROWS, true,
                    "concat(CAST(id AS VARCHAR), ',', name, ',', CAST(amount AS VARCHAR), ',',"
                            + " strftime(ts, '%Y-%m-%dT%H:%M:%S'), ',', coalesce(CAST(bucket AS VARCHAR), ''))",
                    "string_agg(line, chr(13) || chr(10))");
        }
    }
}
