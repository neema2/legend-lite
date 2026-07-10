package com.legend.resolver;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M-H4a &mdash; the GRAPH-serialize envelope (SNAPSHOT): implicit serialize
 * for bare class roots (leaf-only scalar tree, plan &sect;E10) and explicit
 * {@code graphFetch(...)->serialize(...)} with nested to-one / to-many
 * children as correlated JSON subqueries. Executed end to end &mdash; the
 * pins are on the JSON payload, DuckDB-produced.
 */
class ResolveSerializeTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; age: Integer[1]; }
            Class m::Firm { legal: String[1]; }
            Association m::Emp { employer: m::Firm[0..1]; staff: m::Person[*]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), AGE INTEGER, FID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID)
            )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] P
                name: P.NAME, age: P.AGE }
              *m::Firm: Relational { ~mainTable [s::DB] F legal: F.LEGAL }
              m::Emp: Relational { AssociationMapping ( employer: [s::DB] @PF, staff: [s::DB] @PF ) }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE P (NAME VARCHAR, AGE INTEGER, FID INTEGER)");
            st.execute("INSERT INTO P VALUES ('Ann', 30, 1), ('Bob', 40, NULL),"
                    + " ('Cat', 25, 1)");
            st.execute("CREATE TABLE F (ID INTEGER, LEGAL VARCHAR)");
            st.execute("INSERT INTO F VALUES (1, 'ACME'), (2, 'Empty Corp')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private String graph(String query) throws SQLException {
        ExecutionResult r = Compiler.execute(MODEL, query, "m::RT", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r,
                "a serialize/bare-class root must produce the GRAPH shape");
        return ((ExecutionResult.Graph) r).json();
    }

    @Test
    @DisplayName("27: BARE class root — implicit serialize, scalar leaves only")
    void bareRootImplicitSerialize() throws SQLException {
        String json = graph("m::Person.all()");
        assertTrue(json.contains("\"name\":\"Ann\"") && json.contains("\"age\":30"), json);
        assertTrue(json.startsWith("[") && json.endsWith("]"),
                "SNAPSHOT aggregates all rows into ONE JSON array: " + json);
        assertTrue(json.contains("Bob") && json.contains("Cat"), json);
    }

    @Test
    @DisplayName("28: explicit serialize with a leaf subset — pruned by construction")
    void explicitSerializeLeafSubset() throws SQLException {
        String json = graph("m::Person.all()->sortBy(p|$p.name)"
                + "->graphFetch(#{m::Person{name}}#)->serialize(#{m::Person{name}}#)"
                + "");
        assertEquals("[{\"name\":\"Ann\"},{\"name\":\"Bob\"},{\"name\":\"Cat\"}]", json);
        assertFalse(json.contains("age"), "un-fetched leaves must not serialize: " + json);
    }

    @Test
    @DisplayName("29: to-ONE child — nested object; NULL FK serializes null")
    void toOneChild() throws SQLException {
        String json = graph("m::Person.all()->sortBy(p|$p.name)"
                + "->graphFetch(#{m::Person{name, employer{legal}}}#)"
                + "->serialize(#{m::Person{name, employer{legal}}}#)");
        assertTrue(json.contains("\"employer\":{\"legal\":\"ACME\"}"), json);
        assertTrue(json.contains("\"name\":\"Bob\",\"employer\":null"),
                "no-employer row serializes null, not a missing key: " + json);
    }

    @Test
    @DisplayName("30: to-MANY child — nested array; empty collection is [], not null")
    void toManyChild() throws SQLException {
        String json = graph("m::Firm.all()->sortBy(f|$f.legal)"
                + "->graphFetch(#{m::Firm{legal, staff{name}}}#)"
                + "->serialize(#{m::Firm{legal, staff{name}}}#)");
        assertTrue(json.contains("\"legal\":\"ACME\""), json);
        assertTrue(json.contains("{\"name\":\"Ann\"}") && json.contains("{\"name\":\"Cat\"}"),
                "ACME's two staff serialize as array elements: " + json);
        assertTrue(json.contains("\"legal\":\"Empty Corp\",\"staff\":[]"),
                "an empty to-many is the EMPTY ARRAY, never null: " + json);
    }

    @Test
    @DisplayName("31: filter below the fetch — the resolver machinery composes")
    void filterThenSerialize() throws SQLException {
        String json = graph("m::Person.all()->filter(p|$p.age > 28)"
                + "->sortBy(p|$p.name)"
                + "->graphFetch(#{m::Person{name}}#)->serialize(#{m::Person{name}}#)"
                + "");
        assertEquals("[{\"name\":\"Ann\"},{\"name\":\"Bob\"}]", json);
    }
}
