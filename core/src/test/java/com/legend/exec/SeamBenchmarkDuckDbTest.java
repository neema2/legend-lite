// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

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
            SeamBenchmark.product("duckdb", c, DatabaseType.DuckDB, DUCKDB_ROWS, true,
                    "CAST(to_json(struct_pack(id := id, name := name, amount := amount,"
                            + " ts := strftime(ts, '%Y-%m-%dT%H:%M:%S'), bucket := bucket)) AS VARCHAR)",
                    exactTexts());
        }
    }

    /** The product's CSV and JSON rows spelled by DuckDB exactly as CsvWriter and JsonRowsWriter spell them (the
     *  payoff measurement, 2026-11-01: the checksums prove the bytes equal): a name quoted where CSV needs it, a
     *  timestamp's nine digits as {@code %f} (six) and three zeros -- DuckDB's {@code %n} writes no nanoseconds. */
    private static List<SeamBenchmark.ExactText> exactTexts() {
        String quoted = "CASE WHEN regexp_matches(name, '[,\"\n\r]') THEN '\"' || replace(name, '\"', '\"\"')"
                + " || '\"' ELSE name END";
        String ts = "(strftime(ts, '%Y-%m-%dT%H:%M:%S.%f') || '000+0000')";
        String csv = "SELECT concat(CAST(id AS VARCHAR), ',', " + quoted + ", ',',"
                + " CAST(CAST(amount AS DECIMAL(18,2)) AS VARCHAR), ',', " + ts + ", ',',"
                + " coalesce(CAST(bucket AS VARCHAR), '')) AS line FROM bench";
        String json = "SELECT CAST(json_object('id', id, 'name', name, 'amount', CAST(amount AS DECIMAL(18,2)),"
                + " 'ts', " + ts + ", 'bucket', bucket) AS VARCHAR) AS line FROM bench";
        return List.of(
                new SeamBenchmark.ExactText("4'' csv in the product's exact spelling, by the database, written out",
                        csv, "id,name,amount,ts,bucket\r\n", "", "\r\n", ""),
                new SeamBenchmark.ExactText("3'' json in the product's exact spelling, by the database, written out",
                        json, "[", ",", "", "]"));
    }
}
