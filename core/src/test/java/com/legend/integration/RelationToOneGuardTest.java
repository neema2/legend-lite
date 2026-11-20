// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D2 (docs/GOALS_OF_RECORD_2026_11_07.md, the user, 2026-11-07: "sql guard is good"): a relation's {@code toOne()} at a
 * statement's root is checked by the DATABASE (the lowering's ToOneGuard: a window count left-joined onto one row, so
 * an empty relation raises too), with Pure's own message; one row passes through unchanged. Run on each lane's
 * database (DuckDB, H2, SQLite) and on the embedded Postgres ({@code RelationToOneGuardPostgresTest}).
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class RelationToOneGuardTest {

    private static final String ROWS = "#TDS\na\n1\n2\n#";

    /** The guard on {@code backend}'s database: two rows and none raise Pure's size error, one row is answered. */
    static void guarded(TestBackend backend) throws Exception {
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            assertSizeError(backend, model, c, "{|" + ROWS + "->toOne()}", 2);
            assertSizeError(backend, model, c, "{|" + ROWS + "->filter(r|$r.a > 5)->toOne()}", 0);
            Lean.Result one = Lean.run(model, "{|" + ROWS + "->filter(r|$r.a == 2)->toOne()}",
                    StorelessRuntime.RUNTIME, c);
            assertEquals(1, one.rowCount());
            assertEquals(List.of("a"), one.columns().stream().map(Lean.Column::name).toList());
            assertEquals(2L, ((Number) one.rows().get(0).get(0)).longValue());
        }
    }

    private static void assertSizeError(TestBackend backend, String model, Connection c, String query, int size)
            throws Exception {
        Exception e = assertThrows(Exception.class, () -> Lean.run(model, query, StorelessRuntime.RUNTIME, c), query);
        // the product's kind, in lite's words (D1.1), and its where, the bound (2026-11-15: a relation's row count
        // carried none until //core:product_raise_probe found it); the size is legend-pure's sentence's, the edge's
        assertTrue(String.valueOf(e.getMessage()).contains(com.legend.error.ErrorKind.SIZE_NOT_MULTIPLICITY.sentence()
                + ": [1]"), query + " (size " + size + "): "
                + e.getMessage());
        // the size, a row count only the database knows, prints <computed> in the edge's words, its formatter's after
        // the run (stage B's B1, 2026-11-20; D1, register S389): until B1 the edge's SQL wrote the count (the D part 1
        // audit's S5, 2026-11-15)
        Exception edge;
        try (Connection edgeSession = backend.open(com.legend.edge.PureText.UPSTREAM)) {   // the edge's database (the audit's J22)
            edge = assertThrows(Exception.class, () -> com.legend.testing.EdgeProgram.last(model, query,
                    StorelessRuntime.RUNTIME, edgeSession, com.legend.testing.EdgeProgram.upstream()), query);
        }
        String words = com.legend.edge.PureMessages.compile(edge);
        assertTrue(words.contains("Cannot cast a collection of size " + com.legend.edge.PureMessages.WITHHELD + " to"),
                query + " (size " + size + ") at the edge: " + words);
    }

    @Test
    @DisplayName("D2: a relation's toOne() at the root raises Pure's size error in the database for 0 and 2 rows")
    void onTheLane() throws Exception {
        guarded(LaneFloor.backend());
    }
}
