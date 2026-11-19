// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Program;

import com.legend.testing.Lean;
import com.legend.testbackend.TestBackends;
import com.legend.platform.Feature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
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
 *
 * <p>Since A3 of the compatibility program, piece 2 (2026-11-19; the user's
 * word: "approved: push both A3 branches and make piece 2's moves"), lite's
 * core answers Pure's meaning for both names, with or without the flag
 * (S130): legend-engine's uncorrected {@code substring} is the compatibility
 * edge's. The pins below were re-pinned to Pure's answers that day.
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
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
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
    @DisplayName("substr and substring are Pure's, corrected with or without the flag")
    void substrAlwaysCorrected() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // the PCT's own example: 'the quick brown fox'->substr(4, 9) == 'quick'
            assertEquals("quick", scalar(Lean.run(MODEL,
                    "|'the quick brown fox'->substr(4, 9)", "t::RT", c)));
            assertEquals("quick brown fox", scalar(Lean.run(MODEL,
                    "|'the quick brown fox'->substr(4)", "t::RT", c)));
            // substring is Pure's with no flag (it passed its indexes straight to
            // SQL until A3 piece 2, " quick br")
            assertEquals("quick", scalar(Lean.run(MODEL,
                    "|'the quick brown fox'->substring(4, 9)", "t::RT", c)));
            // … and the same under the flag
            assertEquals("quick", scalar(Program.last(MODEL,
                    "|'the quick brown fox'->substring(4, 9)", "t::RT", c, CORRECTED)));
            // the flag never changes substr
            assertEquals("quick", scalar(Program.last(MODEL,
                    "|'the quick brown fox'->substr(4, 9)", "t::RT", c, CORRECTED)));
        }
    }

    private static Object scalar(Lean.Result r) {
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE, r.shape(), "expected a scalar frame");
        return r.one();
    }
}
