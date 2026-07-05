package com.legend.exec;

import com.legend.Compiler;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.ExprType;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Phase K end-to-end: {@code Compiler.execute} — the core QueryService —
 * over a real connection, one test per {@link ResultShape}. The typed-result
 * contract is pinned: Pure types ride the result; consumers never sniff.
 */
class ExecutorTest {

    private static final String MODEL = """
            Database test::DB
            (
              Table T_PERSON (NAME VARCHAR(100) NOT NULL, AGE INTEGER NOT NULL)
            )
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (NAME VARCHAR NOT NULL, AGE INTEGER NOT NULL)");
            st.execute("INSERT INTO T_PERSON VALUES ('Ann', 25), ('Bob', 35)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    @DisplayName("SCALAR: single value, Pure type on the result")
    void scalarShape() throws SQLException {
        ExecutionResult r = Compiler.execute(MODEL, "1 + 2 * 3", conn);
        ExecutionResult.Scalar s = assertInstanceOf(ExecutionResult.Scalar.class, r);
        assertEquals(7L, ((Number) s.value()).longValue());
        assertEquals(Type.Primitive.INTEGER, s.returnType());
        assertEquals(1, r.rowCount());
    }

    @Test
    @DisplayName("COLLECTION: N rows x 1 column, element type on the result")
    void collectionShape() throws SQLException {
        ExecutionResult r = Compiler.execute(MODEL, "[10, 20, 30]", conn);
        ExecutionResult.Collection c = assertInstanceOf(ExecutionResult.Collection.class, r);
        assertEquals(List.of(10L, 20L, 30L),
                c.values().stream().map(v -> ((Number) v).longValue()).toList());
        assertEquals(Type.Primitive.INTEGER, c.returnType(), "ELEMENT type, not list type");
        assertEquals(3, r.rowCount());
    }

    @Test
    @DisplayName("TABULAR: typed columns from the plan's outputs, raw JDBC cells")
    void tabularShape() throws SQLException {
        ExecutionResult r = Compiler.execute(MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 30)", conn);
        ExecutionResult.Tabular t = assertInstanceOf(ExecutionResult.Tabular.class, r);
        assertEquals(List.of("NAME", "AGE"),
                t.columns().stream().map(Column::name).toList());
        assertEquals(Type.Primitive.STRING, t.columns().get(0).pureType(),
                "the Pure Type OBJECT from the typed plan outputs, not JDBC sniffing");
        assertEquals(Type.Primitive.INTEGER, t.columns().get(1).pureType());
        assertEquals(1, t.rowCount());
        assertEquals("Bob", t.rows().get(0).get(0));
        assertEquals(35, ((Number) t.rows().get(0).get(1)).intValue());
    }

    @Test
    @DisplayName("aggregate over empty input: one row, NULL cell, schema intact")
    void aggregateEmptyInput() throws SQLException {
        ExecutionResult r = Compiler.execute(MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 99)"
                        + "->aggregate(~m : x|$x.AGE : y|$y->max())", conn);
        ExecutionResult.Tabular t = assertInstanceOf(ExecutionResult.Tabular.class, r);
        assertEquals(1, t.rowCount());
        assertNull(t.rows().get(0).get(0));
    }

    @Test
    @DisplayName("shape classification is a closed switch over the type system")
    void shapeClassification() {
        assertEquals(ResultShape.GRAPH, ResultShape.of(new ExprType(
                new Type.ClassType("test::Person"), Multiplicity.Bounded.ZERO_MANY)));
        assertEquals(ResultShape.SCALAR, ResultShape.of(new ExprType(
                Type.Primitive.STRING, Multiplicity.Bounded.ONE)));
        assertEquals(ResultShape.COLLECTION, ResultShape.of(new ExprType(
                Type.Primitive.STRING, Multiplicity.Bounded.ZERO_MANY)));
    }
}
