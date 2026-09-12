// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.typed.Feature;
import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TWO SUBSTRING NAMES, the engine's way (relational lowering, PR #5045):
 * {@code substr} is ALWAYS corrected from Pure's 0-based, end-exclusive
 * indexes to SQL's 1-based start and length ("needs no flag");
 * {@code substring} is corrected ONLY under CORRECT_SQL_SUBSTRING_INDEXING
 * ("correcting substring unconditionally would change the results of
 * existing queries, hence the flag"). In Pure the two are one function
 * (substr's body calls substring) — the split lives in the lowering, which
 * is why substr is a platform native here and never the prelude's body.
 */
class SubstrIndexingTest {

    private static final String MODEL = """
            ###Pure
            Class t::Marker {}
            ###Relational
            Database t::Db ( Table T_X (id INTEGER PRIMARY KEY) )
            ###Connection
            RelationalDatabaseConnection t::Conn
            {
                store: t::Db;
                type: H2;
                specification: LocalH2 {};
                auth: DefaultH2 {};
            }
            ###Runtime
            Runtime t::RT
            {
                mappings: [];
                connections: [ t::Db: [ env: t::Conn ] ];
            }
            """;

    private static final ExecuteOptions CORRECTED =
            ExecuteOptions.NONE.withFeatures(Set.of(Feature.CORRECT_SQL_SUBSTRING_INDEXING));

    @Test
    @DisplayName("substr is corrected with no flag; substring only under the flag")
    void substrAlwaysCorrected() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            QueryService qs = new QueryService();
            // the PCT's own example: 'the quick brown fox'->substr(4, 9) == 'quick'
            assertEquals("quick", scalar(qs.execute(MODEL,
                    "|'the quick brown fox'->substr(4, 9)", "t::RT", c)));
            assertEquals("quick brown fox", scalar(qs.execute(MODEL,
                    "|'the quick brown fox'->substr(4)", "t::RT", c)));
            // substring passes its indexes straight to SQL by default: SQL's
            // substring(s, 4, 9) starts at the 4th character and takes 9
            assertEquals(" quick br", scalar(qs.execute(MODEL,
                    "|'the quick brown fox'->substring(4, 9)", "t::RT", c)));
            // … and is corrected under the flag
            assertEquals("quick", scalar(qs.execute(MODEL,
                    "|'the quick brown fox'->substring(4, 9)", "t::RT", c, CORRECTED)));
            // the flag never changes substr
            assertEquals("quick", scalar(qs.execute(MODEL,
                    "|'the quick brown fox'->substr(4, 9)", "t::RT", c, CORRECTED)));
        }
    }

    private static Object scalar(ExecutionResult r) {
        assertTrue(r instanceof ExecutionResult.Scalar,
                "expected a scalar frame, got " + r.getClass().getSimpleName());
        return ((ExecutionResult.Scalar) r).value();
    }
}
