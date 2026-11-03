package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.testbackend.TestBackends;
import com.legend.plan.ResultShape;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.ExprType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Phase K end-to-end: {@code Compiler.execute} — the core's execution front door —
 * over a real connection, one test per {@link ResultShape}. The typed-result
 * contract is pinned: Pure types ride the result; consumers never sniff.
 */
class ExecutorTest {

    private static final String MODEL = """
            ###Relational
            Database test::DB
            (
              Table T_PERSON (NAME VARCHAR(100) NOT NULL, AGE INTEGER NOT NULL)
              Table T_TREES (CITY VARCHAR(100) NOT NULL, YR INTEGER NOT NULL, TREES INTEGER NOT NULL)
            )
            ###Connection
            RelationalDatabaseConnection test::Conn { store: test::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: []; connections: [ test::DB: [ c: test::Conn ] ]; }
            """;

    /** The tables are this session's: its runtime declares the DuckDB it executes on. */
    private static final String RUNTIME = "test::RT";

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (NAME VARCHAR NOT NULL, AGE INTEGER NOT NULL)");
            st.execute("INSERT INTO T_PERSON VALUES ('Ann', 25), ('Bob', 35)");
            st.execute("CREATE TABLE T_TREES (CITY VARCHAR NOT NULL, YR INTEGER NOT NULL,"
                    + " TREES INTEGER NOT NULL)");
            st.execute("INSERT INTO T_TREES VALUES ('NYC', 2011, 100), ('SF', 2011, 50),"
                    + " ('NYC', 2012, 120)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    @DisplayName("SCALAR: single value, Pure type on the result")
    void scalarShape() throws SQLException {
        Lean.Result r = Lean.run(MODEL, "1 + 2 * 3", RUNTIME, conn);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE, r.shape());
        assertEquals(7L, ((Number) r.one()).longValue());
        assertEquals("Integer", r.columns().get(0).type());
        assertEquals(1, r.rowCount());
    }

    @Test
    @DisplayName("COLLECTION: N rows x 1 column, element type on the result")
    void collectionShape() throws SQLException {
        Lean.Result r = Lean.run(MODEL, "[10, 20, 30]", RUNTIME, conn);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE, r.shape());
        assertEquals(List.of(10L, 20L, 30L),
                r.values().stream().map(v -> ((Number) v).longValue()).toList());
        assertEquals("Integer", r.columns().get(0).type(), "ELEMENT type, not list type");
        assertEquals(3, r.rowCount());
    }

    @Test
    @DisplayName("TABULAR: typed columns from the plan's outputs, raw JDBC cells")
    void tabularShape() throws SQLException {
        Lean.Result r = Lean.run(MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 30)", RUNTIME, conn);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, r.shape());
        Lean.Result t = r;
        assertEquals(List.of("NAME", "AGE"),
                t.columns().stream().map(Lean.Column::name).toList());
        assertEquals("String", t.columns().get(0).type(),
                "the Pure Type OBJECT from the typed plan outputs, not JDBC sniffing");
        assertEquals("Integer", t.columns().get(1).type());
        assertEquals(1, t.rowCount());
        assertEquals("Bob", t.rows().get(0).get(0));
        assertEquals(35, ((Number) t.rows().get(0).get(1)).intValue());
    }

    @Test
    @DisplayName("aggregate over empty input: one row, NULL cell, schema intact")
    void aggregateEmptyInput() throws SQLException {
        Lean.Result r = Lean.run(MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 99)"
                        + "->aggregate(~m : x|$x.AGE : y|$y->max())", RUNTIME, conn);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, r.shape());
        Lean.Result t = r;
        assertEquals(1, t.rowCount());
        assertNull(t.rows().get(0).get(0));
    }

    @Test
    @DisplayName("PIVOT: dynamic '<value>__|__<agg>' columns inherit the aggregate template's type")
    void pivotDynamicColumnsInheritTemplateType() throws SQLException {
        Lean.Result r = Lean.run(MODEL,
                "#>{test::DB.T_TREES}#->pivot(~CITY, ~total : x|$x.TREES : y|$y->sum())", RUNTIME, conn);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, r.shape());
        Lean.Result t = r;
        // YR is the static group column; the city columns are data-derived —
        // their NAMES come from the result set, their TYPES from the schema's
        // aggregate template (never SQL-type sniffing).
        List<String> names = t.columns().stream().map(Lean.Column::name).toList();
        // engine presentation: pivot names carry their quotes
        assertEquals(List.of("YR", "'NYC__|__total'", "'SF__|__total'"),
                names);
        assertEquals("Integer", t.columns().get(0).type());
        assertEquals("Integer", t.columns().get(1).type(),
                "the sum template's Pure type rides the dynamic column");
        assertEquals("Integer", t.columns().get(2).type());
        assertEquals(2, t.rowCount());
    }

    /**
     * D18 F (the user, 2026-11-03): legend-engine's answer to a dynamic pivot rebuilds EVERY column of its header from
     * the database's report (RelationalResult with isResultColumnsDynamic: TDSColumn(label, labelTypePair's type), no
     * relationalType), so the JVM's writer does too -- DuckDB reports YR an INTEGER, a sum of INTEGERs a HUGEINT (an
     * Integer) and an average a DOUBLE (a Float), and no column carries a relational spelling. The values are the
     * database's, unchanged.
     */
    @Test
    @DisplayName("PIVOT: the engine's answer types a dynamic result's columns from the database's report")
    void pivotDynamicHeaderIsTheDatabasesReport() throws Exception {
        String query = "#>{test::DB.T_TREES}#->pivot(~CITY, ~[total : x|$x.TREES : y|$y->sum(), "
                + "mean : x|$x.TREES : y|$y->average()])";
        com.legend.TypedQuery typed = com.legend.Compiler.query(com.legend.Compiler.compileModel(MODEL), query);
        java.io.StringWriter json = new java.io.StringWriter();
        com.legend.Execution.run(com.legend.testing.LaneFloor.model(MODEL, conn), query, RUNTIME,
                com.legend.Execution.Source.given(com.legend.testbackend.TestSessions.over(conn)),
                PlanSessions.of(com.legend.testbackend.TestSessions.over(conn)),
                p -> com.legend.server.PureV1Api.engineWriter(typed, p, json));
        com.legend.json.Json.Obj answer = com.legend.json.Json.parseObject(json.toString());
        java.util.Map<String, String> types = new java.util.LinkedHashMap<>();
        for (com.legend.json.Json.Node c : ((com.legend.json.Json.Arr) ((com.legend.json.Json.Obj) answer.get("builder"))
                .get("columns")).items()) {
            com.legend.json.Json.Obj column = (com.legend.json.Json.Obj) c;
            assertEquals(java.util.Set.of("name", "type"), column.fields().keySet(), json.toString());
            types.put(column.getString("name"), column.getString("type"));
        }
        assertEquals(java.util.Map.of("YR", "Integer", "'NYC__|__total'", "Integer", "'NYC__|__mean'", "Float",
                "'SF__|__total'", "Integer", "'SF__|__mean'", "Float"), types, json.toString());
    }

    /**
     * D18 D (the user, 2026-11-03): a host answers a dynamic pivot as the JVM does -- the execute route's plan half
     * answers the keyed plan (HostPlans.query), the host runs its keys statement, the core writes the body for those
     * keys (PureV1Api.keyedBody), the host DESCRIBEs and runs it with the keys bound, and the core writes the answer
     * from the rows and the report (keyedAnswer): the JVM's own answer text, but for its execution trace's id.
     */
    @Test
    @DisplayName("PIVOT: a host's keyed run answers the JVM's text")
    void pivotOnAHostAnswersTheJvmsText() throws Exception {
        String query = "#>{test::DB.T_TREES}#->pivot(~CITY, ~[total : x|$x.TREES : y|$y->sum(), "
                + "mean : x|$x.TREES : y|$y->average()])->sort(~YR->ascending())";
        com.legend.TypedQuery typed = com.legend.Compiler.query(com.legend.Compiler.compileModel(MODEL), query);
        java.io.StringWriter jvm = new java.io.StringWriter();
        com.legend.Execution.run(com.legend.testing.LaneFloor.model(MODEL, conn), query, RUNTIME,
                com.legend.Execution.Source.given(com.legend.testbackend.TestSessions.over(conn)),
                PlanSessions.of(com.legend.testbackend.TestSessions.over(conn)),
                p -> com.legend.server.PureV1Api.engineWriter(typed, p, jvm));

        // the host: the plan half, the keys, the body, its report and its rows, the answer
        String lambda = com.legend.server.PureV1Api.grammarToJsonLambda("|" + query + "->from(" + RUNTIME + ")", false)
                .json();
        String request = "{\"function\":" + lambda + ",\"model\":"
                + com.legend.json.Json.toCompact(java.util.Map.of("_type", "text", "code", MODEL)) + "}";
        com.legend.server.PureV1Api.Answer planned = com.legend.server.PureV1Api.jsonPlan(request, List.of(MODEL));
        assertEquals(200, planned.status(), planned.json());
        var keyed = (com.legend.executionplan.ExecutionPlan.Keyed) com.legend.executionplan.PlanJson.read(planned.json())
                .root();
        List<List<String>> keys = new java.util.ArrayList<>();
        try (Statement st = conn.createStatement(); java.sql.ResultSet rs = st.executeQuery(keyed.keys().statement())) {
            while (rs.next()) {
                List<String> row = new java.util.ArrayList<>();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    row.add(rs.getString(c));
                }
                keys.add(row);
            }
        }
        String keysJson = com.legend.json.Json.toCompact(keys);
        com.legend.server.PureV1Api.Answer body = com.legend.server.PureV1Api.keyedBody(planned.json(), keysJson);
        assertEquals(200, body.status(), body.json());
        com.legend.json.Json.Obj written = com.legend.json.Json.parseObject(body.json());
        String statement = written.getString("statement");
        List<String> parameters = written.getStringArray("parameters");
        List<String> reported = new java.util.ArrayList<>();
        try (java.sql.PreparedStatement ps = conn.prepareStatement("DESCRIBE " + statement)) {
            for (int i = 0; i < parameters.size(); i++) {
                ps.setString(i + 1, parameters.get(i));
            }
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    reported.add(rs.getString("column_type"));
                }
            }
        }
        List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
        try (java.sql.PreparedStatement ps = conn.prepareStatement(statement)) {
            for (int i = 0; i < parameters.size(); i++) {
                ps.setString(i + 1, parameters.get(i));
            }
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
                    for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                        Object v = rs.getObject(c);
                        // as a host's driver hands a cell back (HostRows): a number as its digits, any other its text
                        row.put(rs.getMetaData().getColumnLabel(c), v == null || v instanceof Number ? v : v.toString());
                    }
                    rows.add(row);
                }
            }
        }
        com.legend.server.PureV1Api.Answer host = com.legend.server.PureV1Api.keyedAnswer(planned.json(), keysJson,
                com.legend.json.Json.toCompact(rows), com.legend.json.Json.toCompact(reported));
        assertEquals(200, host.status(), host.json());
        java.util.function.UnaryOperator<String> traced =
                t -> t.replaceAll("executionTraceID\\\\\" : \\\\\"[0-9a-f-]{36}", "ID");
        assertEquals(traced.apply(jvm.toString()), traced.apply(host.json()));
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
