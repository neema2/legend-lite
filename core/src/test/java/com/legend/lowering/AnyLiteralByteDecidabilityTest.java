// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The F10 slice-3 EXIT CRITERION, pinned (charter: "temporal/Decimal
 * equality inside Any is byte-decidable — different bytes, decidably
 * unequal, exactly pure's answer"). A cross-category literal mix rides
 * the LITERAL spelling carrier, whose six kinds are disjoint BY
 * GRAMMAR: {@code %2014-01-01} vs {@code '2014-01-01'} vs {@code 3.0D}
 * vs {@code 3.0} are different bytes. The json carrier ERASES exactly
 * these kinds (to_json of a date and of its string print are the SAME
 * bytes; D-suffix dies) — equality silently answered TRUE where pure
 * says FALSE (cross-kind equality is never true, X1-X4).
 */
class AnyLiteralByteDecidabilityTest {

    private static final String MODEL = """
            Class m::P { id: Integer[1]; }
            ###Relational
            Database s::DB ( Table P (ID INTEGER) )
            ###Mapping
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] P id: P.ID } )
            ###Connection
            RelationalDatabaseConnection s::DBDuckDB { store: s::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c0: s::DBDuckDB ] ]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE P (ID INTEGER)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static long size(String query) throws SQLException {
        ExecutionResult r = Execution.execute(MODEL, query, "m::RT", conn);
        return ((Number) ((ExecutionResult.Scalar) r).value()).longValue();
    }

    @Test
    @DisplayName("a StrictDate and its string print are DIFFERENT values")
    void dateVsStringIsDecidablyUnequal() throws SQLException {
        assertEquals(2L, size(
                "{| [%2014-01-01, '2014-01-01']->removeDuplicates()->size();}"));
    }

    @Test
    @DisplayName("Decimal, its string print, and the Float are THREE values")
    void decimalVsStringVsFloatAllDistinct() throws SQLException {
        assertEquals(3L, size(
                "{| [3.0D, '3.0', 3.0]->removeDuplicates()->size();}"));
    }

    @Test
    @DisplayName("genuine duplicates still collapse (same kind, same bytes)")
    void sameKindDuplicatesStillCollapse() throws SQLException {
        assertEquals(3L, size(
                "{| ['a', 1, %2014-01-01, 'a', 1]->removeDuplicates()->size();}"));
    }
}
