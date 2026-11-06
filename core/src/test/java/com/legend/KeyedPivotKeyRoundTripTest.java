// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A dynamic pivot's keys ROUND-TRIP through the keyed plan (the planner-driver seam program's S3a, 2026-10-31):
 * the keys step reads each key as the database's text of it, the runner binds that text back, and the database casts
 * it to the key's own type, so each key matches the very cells it was read from. The successor of
 * {@code DynamicPivotKeyLiteralH2Test} (audit finding D, 2026-08-18), which pinned the same three cases for the
 * staticizer the keyed plan replaced: a REAL key (a {@code Float} widened to a double once matched no row), a
 * timestamp at whole seconds ({@code LocalDateTime.toString()} once dropped {@code :00}) and a sub-second one.
 */
class KeyedPivotKeyRoundTripTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table T ( K %s, V INTEGER ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            %s
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    /** The pivot of {@code T} on {@code K} (of {@code kType} in the database, {@code modelType} in the store) summing
     *  {@code V}, over the rows {@code inserts}: each per-key column's name and value. */
    private static List<List<Object>> pivoted(DatabaseType type, String kType, String modelType, String inserts)
            throws Exception {
        try (Connection c = TestBackends.of(type).open()) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE T (K " + kType + ", V INTEGER)");
                st.execute("INSERT INTO T VALUES " + inserts);
            }
            Lean.Result r = Lean.run(MODEL.formatted(modelType, "{ type: " + type.name() + "; "
                    + com.legend.exec.Connectors.of(type).storelessDeclaration() + "; }"),
                    "|#>{local::DB.T}#->pivot(~[K], ~[s : x | $x.V : y | $y->plus()])", "local::RT", c);
            assertEquals(1, r.rows().size(), "one group (no static column): " + r.rows());
            List<List<Object>> out = new java.util.ArrayList<>();
            for (int i = 0; i < r.columns().size(); i++) {
                out.add(java.util.Arrays.asList(r.columns().get(i).name(), r.rows().get(0).get(i)));
            }
            return out;
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    @DisplayName("a REAL key matches its rows: text against text, the database's own")
    void aRealKeyRoundTrips(TestBackend backend) throws Exception {
        List<List<Object>> cols = pivoted(backend.type(), "REAL", "REAL", "(3.14, 1), (3.14, 2), (2.5, 4)");
        assertEquals(2, cols.size(), cols.toString());
        for (List<Object> col : cols) {
            String name = (String) col.get(0);
            long v = col.get(1) == null ? -1 : ((Number) col.get(1)).longValue();
            assertTrue(name.contains("3.14") && v == 3 || name.contains("2.5") && v == 4, cols.toString());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    @DisplayName("timestamp keys at whole seconds and below match their rows")
    void timestampKeysRoundTrip(TestBackend backend) throws Exception {
        List<List<Object>> cols = pivoted(backend.type(), "TIMESTAMP", "TIMESTAMP",
                "('2020-01-01 10:00:00', 5), ('2020-01-01 10:00:00.123', 7)");
        assertEquals(2, cols.size(), cols.toString());
        for (List<Object> col : cols) {
            String name = (String) col.get(0);
            long v = col.get(1) == null ? -1 : ((Number) col.get(1)).longValue();
            assertTrue(name.contains("10:00:00.123") ? v == 7 : name.contains("10:00:00") && v == 5,
                    cols.toString());
        }
    }
}
