// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

/** {@link SeamBenchmark} on H2: a generated relation of {@code seam.rows.h2} rows (2 million by default), as our
 *  sessions open it and with lazy query execution. */
@Tag("benchmark")
final class SeamBenchmarkH2Test {

    private static final long H2_ROWS = Long.getLong("seam.rows.h2", 2_000_000L);

    @Test
    void h2() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:seam" + H2Connector.SETTINGS)) {
            h2(c, "h2", false);   // H2 as our sessions open it buffers a result whole: what fails here is the finding
        }
    }

    /** H2 with lazy query execution: its driver otherwise buffers a result whole (the default's peak heap says so). */
    @Test
    void h2Lazy() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:seamlazy" + H2Connector.SETTINGS)) {
            SeamBenchmark.exec(c, "SET LAZY_QUERY_EXECUTION 1");
            h2(c, "h2 lazy", true);
        }
    }

    private static void h2(Connection c, String db, boolean mustDrain) throws Exception {
        SeamBenchmark.exec(c, "CREATE VIEW bench AS SELECT X AS id, 'name' || CAST(X AS VARCHAR) AS name,"
                + " CAST(X AS DECIMAL(18,2)) / 7 AS amount,"
                + " DATEADD('SECOND', MOD(X, 86400), TIMESTAMP '2024-01-01 00:00:00') AS ts,"
                + " CASE WHEN MOD(X, 10) = 0 THEN NULL ELSE CAST(MOD(X, 1000) AS INTEGER) END AS bucket"
                + " FROM SYSTEM_RANGE(0, " + (H2_ROWS - 1) + ")");
        SeamBenchmark.run(db, c, Databases.dialect(DatabaseType.H2), true, H2_ROWS, mustDrain,
                "CONCAT(CAST(id AS VARCHAR), ',', name, ',', CAST(amount AS VARCHAR), ',',"
                        + " FORMATDATETIME(ts, 'yyyy-MM-dd''T''HH:mm:ss'), ',', COALESCE(CAST(bucket AS VARCHAR), ''))",
                "LISTAGG(line, CHAR(13) || CHAR(10))");
    }
}
