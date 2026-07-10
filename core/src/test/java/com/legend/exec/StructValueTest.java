package com.legend.exec;

import com.legend.Compiler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Class-instance VALUES in SQL (the STRUCT design): {@code ^Class(…)} lowers
 * to a struct with the MODEL's canonical layout — declared stored properties
 * in declaration order, an omitted optional a NULL field, never a shape
 * inferred from the instance's own value set. Composite results unwrap to
 * ordered field maps at the executor.
 */
class StructValueTest {

    private static final String MODEL = """
            Class m::Person
            {
              firstName: String[1];
              lastName: String[1];
              nick: String[0..1];
            }
            Database m::DB ( Table T (ID INTEGER) )
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private ExecutionResult run(String query) throws SQLException {
        return Compiler.execute(MODEL, query, conn);
    }

    @Test
    @DisplayName("literal field access inlines — no struct round-trip")
    void literalFieldAccess() throws SQLException {
        var r = run("|^m::Person(firstName='John', lastName='Doe').firstName");
        assertEquals("John", ((ExecutionResult.Scalar) r).value());
    }

    @Test
    @DisplayName("an instance VALUE root unwraps to the model's canonical field map")
    void instanceRootIsOrderedMap() throws SQLException {
        var r = run("|^m::Person(firstName='John', lastName='Doe')");
        Object v = ((ExecutionResult.Scalar) r).value();
        Map<?, ?> m = assertInstanceOf(Map.class, v);
        assertEquals(List.of("firstName", "lastName", "nick"),
                m.keySet().stream().map(Object::toString).toList(),
                "layout = DECLARED properties in DECLARATION order, not the value set");
        assertEquals("John", m.get("firstName"));
        assertNull(m.get("nick"), "omitted optional is a NULL field, still present");
    }

    @Test
    @DisplayName("map over a literal instance list reads fields per element")
    void mapOverInstanceList() throws SQLException {
        var r = run("|[^m::Person(firstName='Pierre', lastName='Doe'),"
                + " ^m::Person(firstName='Kevin', lastName='RoeDoe')]"
                + "->map(p|$p.lastName + ', ' + $p.firstName)");
        assertEquals(List.of("Doe, Pierre", "RoeDoe, Kevin"),
                ((ExecutionResult.Collection) r).values());
    }

    @Test
    @DisplayName("zip truncates to the shorter list and yields first/second pairs")
    void zipTruncatesAndYieldsPairs() throws SQLException {
        var r = run("|[1, 2, 3]->zip(['a', 'b'])");
        List<?> pairs = ((ExecutionResult.Collection) r).values();
        assertEquals(2, pairs.size(), "real pure zip TRUNCATES (DuckDB list_zip pads — wrong)");
        Map<?, ?> p0 = assertInstanceOf(Map.class, pairs.get(0));
        assertEquals(List.of("first", "second"),
                p0.keySet().stream().map(Object::toString).toList());
        assertEquals(1L, ((Number) p0.get("first")).longValue());
        assertEquals("a", p0.get("second"));
    }

    @Test
    @DisplayName("nested pairs nest their field maps")
    void nestedPairs() throws SQLException {
        var r = run("|[1, 2]->zip(['a', 'b'])->zip([10, 20])");
        List<?> pairs = ((ExecutionResult.Collection) r).values();
        assertEquals(2, pairs.size());
        Map<?, ?> outer = assertInstanceOf(Map.class, pairs.get(0));
        Map<?, ?> inner = assertInstanceOf(Map.class, outer.get("first"));
        assertEquals(1L, ((Number) inner.get("first")).longValue());
        assertEquals("a", inner.get("second"));
        assertEquals(10L, ((Number) outer.get("second")).longValue());
    }

    @Test
    @DisplayName("instance equality is structural (SQL struct equality)")
    void instanceEquality() throws SQLException {
        var r = run("|^m::Person(firstName='A', lastName='B') =="
                + " ^m::Person(firstName='A', lastName='B')");
        assertEquals(true, ((ExecutionResult.Scalar) r).value());
    }
}
