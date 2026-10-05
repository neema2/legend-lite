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
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * COMPILER_SHORTCUT_AUDIT_2026_08_21 §5 — the null-drop lives in the
 * LOWERER, not at egress. Pure's rule "a collection holds no empties"
 * means an optional-property collection read ({@code P.all().nick} over
 * a NULL-holding column) is the null-free collection IN SQL, so every
 * operation the compiler lowers into SQL ({@code size}, {@code at},
 * {@code indexOf}, {@code toOne}) sees the same collection the user
 * sees. Before this slice the rule was one Java line at egress
 * (Executor {@code if (v != null)}) and the SQL collection had three
 * more elements than the pure one: {@code size()} said 2 while
 * {@code toOne()} raised "size 3" in the same query.
 */
class OptionalCollectionNullDropTest {

    private static final String MODEL = """
            Class m::P { id: Integer[1]; nick: String[0..1]; }
            ###Relational
            Database s::DB ( Table P (ID INTEGER, NICK VARCHAR(50)) )
            ###Mapping
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] P
                id: P.ID, nick: P.NICK } )
            ###Connection
            RelationalDatabaseConnection s::DBDuckDB { store: s::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c0: s::DBDuckDB ] ]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE P (ID INTEGER, NICK VARCHAR)");
            st.execute("INSERT INTO P VALUES (1,NULL),(2,'Al'),(3,'Cee')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static Object run(String query) throws SQLException {
        ExecutionResult r = Execution.execute(MODEL, query, "m::RT", conn);
        return r instanceof ExecutionResult.Scalar s ? s.value()
                : r instanceof ExecutionResult.Collection c ? c.values() : r;
    }

    @Test
    @DisplayName("the collection itself: [Al, Cee] — no empties")
    void collectionHoldsNoEmpties() throws SQLException {
        assertEquals(List.of("Al", "Cee"),
                ((List<?>) run("{| m::P.all().nick->sort();}")));
    }

    @Test
    @DisplayName("size() agrees with the collection: 2")
    void sizeSeesTheDroppedNull() throws SQLException {
        assertEquals(2L,
                ((Number) run("{| m::P.all().nick->size();}")).longValue());
    }

    @Test
    @DisplayName("at(0)/at(1) index the null-free collection")
    void atIndexesNullFree() throws SQLException {
        assertEquals("Al", run("{| m::P.all().nick->sort()->at(0);}"));
        assertEquals("Cee", run("{| m::P.all().nick->sort()->at(1);}"));
    }

    @Test
    @DisplayName("indexOf works over the null-free collection")
    void indexOfNullFree() throws SQLException {
        assertEquals(1L, ((Number)
                run("{| m::P.all().nick->sort()->indexOf('Cee');}"))
                .longValue());
    }

    @Test
    @DisplayName("toOne() counts the pure collection: size 2, not 3")
    void toOneCountsPureCollection() {
        var ex = assertThrows(Exception.class,
                () -> run("{| m::P.all().nick->toOne();}"));
        assertTrue(ex.getMessage().contains("size 2")
                        || ex.getMessage().contains("2 to multiplicity"),
                "expected pure's size-2 cast error, got: " + ex.getMessage());
    }

    @Test
    @DisplayName("relation lane: map over a nullable column is the null-free collection")
    void relationLaneMapDropsEmpties() throws SQLException {
        assertEquals(List.of("Al", "Cee"),
                ((List<?>) run("{| #>{s::DB.P}#->map(r|$r.NICK)->sort();}")));
        assertEquals(2L, ((Number)
                run("{| #>{s::DB.P}#->map(r|$r.NICK)->size();}")).longValue());
    }

    @Test
    @DisplayName("relation lane: toOne() counts the null-free collection")
    void relationLaneToOneCountsPure() {
        var ex = assertThrows(Exception.class,
                () -> run("{| #>{s::DB.P}#->map(r|$r.NICK)->toOne();}"));
        assertTrue(ex.getMessage().contains("size 2")
                        || ex.getMessage().contains("2 to multiplicity"),
                "expected pure's size-2 cast error, got: " + ex.getMessage());
    }

    @Test
    @DisplayName("a [1] property projection stays unfiltered (no SQL noise)")
    void requiredPropertyProjectionUnchanged() throws SQLException {
        assertEquals(3L, ((Number)
                run("{| m::P.all().id->size();}")).longValue());
    }

    @Test
    @DisplayName("DEEP_AUDIT R2: a flowed toOne'd cell drops like the engine's SQLNull->[]")
    void flowedToOneCellDrops() throws SQLException {
        // pure-stamps [1] via the toOne wrap, but the engine lane emits
        // no guard (§7b witness) and drops the NULL client-side — the
        // cell mult tells the carrier's truth and the egress filter
        // fires; this hit the §5 WALL before the audit round
        assertEquals(List.of("Al", "Cee"), ((List<?>)
                run("{| m::P.all()->map(p|$p.nick->toOne())->sort();}")));
    }
}
