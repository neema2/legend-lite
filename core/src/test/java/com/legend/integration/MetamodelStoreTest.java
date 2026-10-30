package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The metamodel store end-to-end (METAMODEL_STORE_HANDOFF.md &sect;7.3):
 * {@code Class.all()} is an ordinary mapped-class query over the seeded
 * {@code metamodel.classes} table &mdash; a real {@code SELECT}, executed
 * by DuckDB, through the EXISTING store lane. These witnesses pin the
 * lane end-to-end (extent, filter, map), the D1 ambient-context rule's
 * boundary (user classes keep the loud wall), and the D2 identity
 * convention (the row carries {@code fqn} as key, {@code name} as the
 * print form).
 */
@DisplayName("Metamodel store: Class.all() through the store lane")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class MetamodelStoreTest {

    private static final String MODEL = """
            Class test::Person { name: String[1]; }
            Class test::Firm { legalName: String[1]; }
            """;

    private static final String METACLASS = "meta::pure::metamodel::type::Class";

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = LaneFloor.backend().open();
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    private Lean.Result run(String query) throws SQLException {
        return Program.last(StorelessRuntime.with(MODEL, LaneFloor.backend().type()), query, StorelessRuntime.RUNTIME, connection);
    }

    @Test
    @DisplayName("Class.all()->size() > 0 — the extent is non-empty with no ->from()")
    void classExtentIsNonEmpty() throws SQLException {
        var r = run(METACLASS + ".all()->size()");
        assertTrue(((Number) r.one()).longValue() > 0,
                "the metamodel extent must be non-empty; got " + r.one());
    }

    @Test
    @DisplayName("Class.all()->isEmpty() = false — the PCT testBasic shape")
    void classExtentIsEmptyIsFalse() throws SQLException {
        var r = run(METACLASS + ".all()->isEmpty()");
        assertEquals(Boolean.FALSE, r.one());
    }

    @Test
    @DisplayName("filter by .name finds a compiled-model class exactly once")
    void filterByNameOverCompiledModel() throws SQLException {
        var r = run(METACLASS
                + ".all()->filter(c | $c.name == 'Person')->size()");
        assertEquals(1L, ((Number) r.one()).longValue(),
                "exactly one class named Person in the extent");
    }

    @Test
    @DisplayName("map(c|$c.name) contains user and native class names")
    void mapNameContainment() throws SQLException {
        var r = run(METACLASS + ".all()->map(c | $c.name)");
        var values = r.values();
        assertTrue(values.contains("Person") && values.contains("Firm"),
                "user class names missing from the extent: " + values);
        assertTrue(values.contains("Any"),
                "native class names missing from the extent");
    }

    @Test
    @DisplayName("a user class with no execution context keeps the loud wall")
    void userClassWithoutContextStillWalls() {
        var ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.MappingResolutionException.class,
                () -> Program.last(MODEL, "test::Person.all()->size()", null, connection));
        // no runtime: where it would execute is undeclared, refused before anything runs
        assertEquals(Compiler.NO_RUNTIME, ex.getMessage());
    }

    @Test
    @DisplayName("the extent re-seeds per active context — a second model's classes appear")
    void extentFollowsTheActiveContext() throws SQLException {
        var r1 = run(METACLASS
                + ".all()->filter(c | $c.name == 'Trade')->size()");
        assertEquals(0L, ((Number) r1.one()).longValue());
        var r2 = Program.last(StorelessRuntime.with(MODEL + "Class test::Trade { id: Integer[1]; }\n", LaneFloor.backend().type()),
                METACLASS + ".all()->filter(c | $c.name == 'Trade')->size()", StorelessRuntime.RUNTIME,
                connection);
        assertEquals(1L, ((Number) r2.one()).longValue(),
                "the seed derives from the ACTIVE model context");
    }

    @Test
    @DisplayName("mixed chain (tracked + user getAll) keeps the wall")
    void mixedChainKeepsTheWall() {
        var ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.MappingResolutionException.class,
                () -> run("meta::pure::metamodel::type::Class.all()->size()"
                        + " + test::Person.all()->size()"));
        assertFalse(ex.getMessage().isEmpty());
    }
}
