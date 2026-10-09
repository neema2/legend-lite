// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.WireRender;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner's plans (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): a plan answers exactly as
 * today's wire and streaming paths answer the same query. Each query runs twice, on two fresh databases: through
 * {@link Execution} (its connection's declared test data established first, as the server establishes it), and through
 * its plan — the target's session statements, its setup steps, its statement — run here as each step describes itself
 * (the runner is step 3). The texts must be equal, byte for byte. Postgres's case is {@code PostgresArmTest}'s (it needs
 * the embedded Postgres).
 */
public class PlanMakerTest {

    private static final AtomicInteger H2_NAMES = new AtomicInteger();

    /** A table of three rows, declared as the connection's test data; {@code type} is the connection's database. */
    private static String model(String type) {
        return """
                Class s::Item { id: Integer[1]; name: String[0..1]; price: Decimal[0..1]; }
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20), PRICE DECIMAL(10,2) ) )
                ###Mapping
                Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T
                    id: [s::DB] T.ID, name: [s::DB] T.NAME, price: [s::DB] T.PRICE } )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME,PRICE\\n1,a,1.50\\n2,O\\'Brien,---null---\\n3,---null---,3.25\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type);
    }

    private static final String RELATION = "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())";
    private static final String NONE = "|#>{s::DB.T}#->filter(r|$r.ID > 99)->select(~[ID, NAME])";
    private static final String PROJECT = "|s::Item.all()->project(~[name: i|$i.name, price: i|$i.price])"
            + "->sort(~name->ascending())";
    private static final String GRAPH = "|s::Item.all()->graphFetch(#{s::Item{id, name, price}}#)"
            + "->serialize(#{s::Item{id, name, price}}#)";

    @Test
    void aRelationsPlanAnswersAsTodaysPaths_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (String query : List.of(RELATION, NONE, PROJECT)) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, query, output);
                }
            }
        }
    }

    @Test
    void aGraphFetchsPlanAnswersAsTodaysPaths_inJson() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            assertAnswersAsToday(type, GRAPH, TypedQuery.Output.JSON);
            assertAnswersAsToday(type, GRAPH, TypedQuery.Output.STREAMED_JSON);
            var refused = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> plan(type, GRAPH, TypedQuery.Output.CSV));
            assertTrue(refused.getMessage().contains("graph results have no CSV wire"), refused.getMessage());
        }
    }

    @Test
    void theTargetIsWhole_itsDatabaseVersionsSessionAndSetup() {
        ExecutionPlan.Target duck = target(plan(DatabaseType.DuckDB, RELATION, TypedQuery.Output.JSON));
        assertEquals("s::Conn", ((ExecutionPlan.Database.Declared) duck.database()).connection().qualifiedName());
        assertEquals(new ExecutionPlan.Servers.Every(), duck.servers());
        assertEquals(List.of("SET TimeZone='UTC'"), duck.session());
        // DuckDB's rows go to its bulk loader: the create, the rows, the copy and the drop
        ExecutionPlan.SetupStep.Rows rows = (ExecutionPlan.SetupStep.Rows) duck.setup().get(duck.setup().size() - 1);
        assertEquals(List.of(List.of("1", "a", "1.50"), java.util.Arrays.asList("2", "O'Brien", null),
                java.util.Arrays.asList("3", null, "3.25")), rows.rows());

        ExecutionPlan.Target h2 = target(plan(DatabaseType.H2, RELATION, TypedQuery.Output.JSON));
        assertEquals(new ExecutionPlan.Servers.Versions(List.of("2.1", "2.2")), h2.servers());
        assertEquals(List.of(), h2.session());
        // H2's rows are one INSERT
        assertInstanceOf(ExecutionPlan.SetupStep.Statement.class, h2.setup().get(h2.setup().size() - 1));
        assertTrue(h2.setup().stream().noneMatch(s -> s instanceof ExecutionPlan.SetupStep.Rows), h2.setup().toString());
    }

    @Test
    void aRuntimeOfModelDataRunsOnThePlatformsEngine() throws Exception {
        String model = """
                Class m::Raw { name: String[1]; }
                Class m::P { name: String[1]; }
                ###Mapping
                Mapping m::MM ( *m::P: Pure { ~src m::Raw name: $src.name } )
                ###Runtime
                Runtime m::RT { mappings: [m::MM]; connections: [ ModelStore: [ json: #{ JsonModelConnection {
                    class: m::Raw; url: 'data:application/json,[{"name":"b"},{"name":"a"}]'; } }# ] ]; }
                """;
        String query = "|m::P.all()->project(~[name: p|$p.name])->sort(~name->ascending())";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query)
                .executionPlan("m::RT", TypedQuery.Output.JSON);
        ExecutionPlan.Target t = target(plan);
        assertEquals(new ExecutionPlan.Database.Platform(Databases.PLATFORM), t.database());
        assertEquals(List.of(), t.setup());
        try (Connection today = DriverManager.getConnection("jdbc:duckdb:");
             Connection planned = DriverManager.getConnection("jdbc:duckdb:")) {
            StringWriter out = new StringWriter();
            Execution.executeWire(model, query, "m::RT", today, WireRender.Format.JSON, out);
            assertEquals(out.toString(), run(plan, planned));
        }
    }

    @Test
    void aRuntimeOfTwoDifferentConnectionsIsRefusedWhenThePlanIsMade() {
        // a second store with a connection of its own, another database of the same type: the query reads only the
        // first store, but the runtime binds both, and a query runs on one session (the server refuses it as it opens)
        String model = model("H2").replace("""
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """, """
                RelationalDatabaseConnection s::Other { store: s::DB2; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
                ###Relational
                Database s::DB2 ( Table U ( ID INTEGER PRIMARY KEY ) )
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ], s::DB2: [ c2: s::Other ] ]; }
                """);
        assertTrue(model.contains("c2: s::Other"), model);
        var refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Compiler.query(Compiler.compileModel(model), RELATION).executionPlan("s::RT", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("different connections: a query runs on one session"),
                refused.getMessage());
    }

    /** A query with parameters, and the same query with each parameter a {@code let} of its value -- how the server
     *  binds a request's values today ({@code PureV1Api.boundParameters}) -- with the values as Java values. */
    public record Parameterised(String parameters, String lets, String body, java.util.Map<String, Object> values) {

        /** The query with its parameters, as a plan is made from it. */
        public String withParameters() {
            return "{" + parameters + "|" + body + "}";
        }

        /** The query with each parameter a {@code let} of its value, as today's paths answer it. */
        public String withLets() {
            return "|" + lets + body + ";";
        }

        /** Whether a parameter's literal has no one type -- a Float's, a Decimal's, a Date's or a Number's: refused on
         *  H2, which types a parameter when it prepares the statement. */
        public boolean untypedOnH2() {
            return java.util.regex.Pattern.compile("\\b(Float|Decimal|Date|Number)\\[").matcher(parameters).find();
        }
    }

    private static final List<Parameterised> SCALARS = scalars("T");

    /** The scalar cases over {@code table} (ID, NAME, PRICE; three rows), every value of the parameter's Java type. */
    public static List<Parameterised> scalars(String table) {
        return List.of(
            new Parameterised("n: Integer[1]", "let n = 1;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID > $n)->select(~[ID, NAME])->sort(~ID->ascending())",
                    java.util.Map.of("n", 1L)),
            new Parameterised("s: String[1]", "let s = 'O\\'Brien';",
                    "#>{s::DB." + table + "}#->filter(r|$r.NAME == $s)->select(~[ID, NAME])", java.util.Map.of("s", "O'Brien")),
            new Parameterised("p: Decimal[1]", "let p = 2.00D;",
                    "#>{s::DB." + table + "}#->filter(r|$r.PRICE < $p)->select(~[ID])->sort(~ID->ascending())",
                    java.util.Map.of("p", new java.math.BigDecimal("2.00"))),
            // one parameter written twice: compared, and added to a column
            new Parameterised("n: Integer[1]", "let n = 2;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID != $n)->extend(~plus: r|$r.ID + $n)->select(~[ID, plus])"
                            + "->sort(~ID->ascending())", java.util.Map.of("n", 2L)),
            new Parameterised("d: StrictDate[1]", "let d = %2024-01-02;",
                    "#>{s::DB." + table + "}#->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())",
                    java.util.Map.of("d", java.time.LocalDate.of(2024, 1, 2))),
            new Parameterised("b: Boolean[1]", "let b = true;",
                    "#>{s::DB." + table + "}#->filter(r|$b)->select(~[ID])->sort(~ID->ascending())", java.util.Map.of("b", true)),
            // a Float is bound as a decimal (the numeric charter's Rule 1: a Float literal is a decimal in the database)
            new Parameterised("f: Float[1]", "let f = 1.1;",
                    "#>{s::DB." + table + "}#->extend(~x: r|$r.ID * $f)->select(~[ID, x])->sort(~ID->ascending())",
                    java.util.Map.of("f", new java.math.BigDecimal("1.1"))),
            new Parameterised("f: Float[1]", "let f = 1.1;",
                    "#>{s::DB." + table + "}#->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())",
                    java.util.Map.of("f", new java.math.BigDecimal("1.1"))),
            new Parameterised("p: Decimal[1]", "let p = 2.50D;",
                    "#>{s::DB." + table + "}#->extend(~[p: r|$p, x: r|$r.ID * $p])->select(~[ID, p, x])->sort(~ID->ascending())",
                    java.util.Map.of("p", new java.math.BigDecimal("2.50"))),
            // a parameter whose value decides its type: bound as its value's kind
            new Parameterised("d: Date[1]", "let d = %2024-01-02;",
                    "#>{s::DB." + table + "}#->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())",
                    java.util.Map.of("d", java.time.LocalDate.of(2024, 1, 2))),
            new Parameterised("n: Number[1]", "let n = 1;",
                    "#>{s::DB." + table + "}#->filter(r|$r.ID > $n)->select(~[ID])->sort(~ID->ascending())",
                    java.util.Map.of("n", 1L)),
            // two parameters
            new Parameterised("lo: Integer[1], hi: Integer[1]", "let lo = 1; let hi = 3;",
                    "#>{s::DB." + table + "}#->filter(r|($r.ID > $lo) && ($r.ID < $hi))->select(~[ID, NAME])",
                    java.util.Map.of("lo", 1L, "hi", 3L)));
    }

    /** A query with an optional parameter {@code x}: run with a value, its plan answers as the query with that value as
     *  a {@code let}; run with none, as the query with {@code x} written empty ({@code []}), which lite lowers as the
     *  engine does (an equality with an empty side is a null check: pureToSQLQuery's nullSafeEqualsOperation). */
    public record OptionalCase(String parameter, String body, String let, Object value) {

        public String withParameter() {
            return "{" + parameter + "|" + body + "}";
        }

        public String withValue() {
            return "|" + let + body + ";";
        }

        public String withNone() {
            return "|" + body.replace("$x", "[]");
        }
    }

    /** The optional cases over {@code table} (ID, NAME, PRICE; three rows, one NAME absent). */
    public static List<OptionalCase> optionals(String table) {
        String t = "#>{s::DB." + table + "}#";
        return List.of(
                new OptionalCase("x: String[0..1]", t + "->filter(r|$r.NAME == $x)->select(~[ID, NAME])"
                        + "->sort(~ID->ascending())", "let x = 'a';", "a"),
                new OptionalCase("x: Integer[0..1]", t + "->filter(r|$r.ID != $x)->select(~[ID])"
                        + "->sort(~ID->ascending())", "let x = 1;", 1L));
    }

    @Test
    void anOptionalParametersPlanAnswersAsTheQuery_withItsValueAndWithNone() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (OptionalCase q : optionals("T")) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, q.withParameter(), q.withValue(), java.util.Map.of("x", q.value()),
                            output);
                    assertAnswersAsToday(type, q.withParameter(), q.withNone(),
                            java.util.Collections.singletonMap("x", null), output);
                }
            }
        }
    }

    /** An account's status stored as a code: ACTIVE as 'A' or 'X' (one name, two codes), CLOSED as 'C'; one account
     *  has none, one a code the mapping does not know. {@code connection}: the connection's type, specification and
     *  authentication. */
    public static String enumModel(String connection, String table) {
        return """
                Enum s::Status { ACTIVE, CLOSED }
                Class s::Acct { id: Integer[1]; status: s::Status[0..1]; }
                ###Relational
                Database s::DB ( Table %2$s ( ID INTEGER PRIMARY KEY, ST VARCHAR(1) ) )
                ###Mapping
                Mapping s::M
                (
                  s::Status: EnumerationMapping St { ACTIVE: ['A', 'X'], CLOSED: 'C' }
                  *s::Acct: Relational { ~mainTable [s::DB] %2$s
                    id: [s::DB] %2$s.ID, status: EnumerationMapping St: [s::DB] %2$s.ST }
                )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; %1$s }
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(connection, table);
    }

    /** {@link #enumModel}'s rows, as a seed's CSV blocks. */
    public static String enumRows(String table) {
        return "default\n" + table + "\nID,ST\n1,A\n2,X\n3,C\n4,---null---\n5,Z\n";
    }

    /** {@link #enumModel} with its rows as an in-memory connection's declared test data. */
    private static String enumModel(DatabaseType type) {
        return enumModel("type: " + type.name() + "; specification: LocalH2 { testDataSetupCSV: '"
                + enumRows("A").replace("\n", "\\n") + "'; }; auth: DefaultH2;", "A");
    }

    /** The enumeration cases: compared with the mapped property (==, !=), and written as a value of its own. */
    public static List<Parameterised> enumerations() {
        return List.of(
                new Parameterised("st: s::Status[1]", "let st = s::Status.ACTIVE;",
                        "s::Acct.all()->filter(a|$a.status == $st)->project(~[id: a|$a.id])->sort(~id->ascending())",
                        java.util.Map.of("st", "ACTIVE")),
                new Parameterised("st: s::Status[1]", "let st = s::Status.CLOSED;",
                        "s::Acct.all()->filter(a|$a.status != $st)->project(~[id: a|$a.id])->sort(~id->ascending())",
                        java.util.Map.of("st", "CLOSED")),
                new Parameterised("st: s::Status[1]", "let st = s::Status.ACTIVE;",
                        "s::Acct.all()->filter(a|$a.id < 3)->project(~[id: a|$a.id, s: a|$st])->sort(~id->ascending())",
                        java.util.Map.of("st", "ACTIVE")));
    }

    @Test
    void anEnumerationParametersPlanAnswersAsTheQueryWithItsValue() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (Parameterised q : enumerations()) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(enumModel(type), type, q.withParameters(), q.withLets(),
                            q.values(), output);
                }
            }
        }
    }

    /** The list cases over {@code table} (ID, NAME, PRICE; three rows): a list parameter bound as one array. */
    public static List<Parameterised> lists(String table) {
        String t = "#>{s::DB." + table + "}#";
        return List.of(
                new Parameterised("ns: Integer[*]", "let ns = [1, 3];",
                        t + "->filter(r|$r.ID->in($ns))->select(~[ID, NAME])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of(1L, 3L))),
                new Parameterised("ns: Integer[*]", "let ns = [];",
                        t + "->filter(r|$r.ID->in($ns))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of())),
                new Parameterised("ns: Integer[*]", "let ns = [2, 3];",
                        t + "->filter(r|$ns->contains($r.ID))->select(~[ID])->sort(~ID->ascending())",
                        java.util.Map.of("ns", List.of(2L, 3L))),
                new Parameterised("ss: String[*]", "let ss = ['a', 'O\\'Brien'];",
                        t + "->filter(r|$r.ID->in([1, 2]) && $ss->contains($r.NAME->toOne()))->select(~[ID, NAME])"
                                + "->sort(~ID->ascending())", java.util.Map.of("ss", List.of("a", "O'Brien"))));
    }

    @Test
    void aListParametersPlanAnswersAsTheQueryWithItsValues_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (Parameterised q : lists("T")) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, q.withParameters(), q.withLets(), q.values(), output);
                }
            }
            for (TypedQuery.Output output : TypedQuery.Output.values()) {
                assertAnswersAsToday(enumModel(type), type, "{sts: s::Status[*]|s::Acct.all()->filter(a|$a.status->in($sts))"
                        + "->project(~[id: a|$a.id])->sort(~id->ascending())}", "|let sts = [s::Status.ACTIVE];"
                        + "s::Acct.all()->filter(a|$a.status->in($sts))->project(~[id: a|$a.id])->sort(~id->ascending());",
                        java.util.Map.of("sts", List.of("ACTIVE")), output);
            }
        }
    }

    @Test
    void aListIsOneArray_andAListOfDecimalsIsRefusedByName() {
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(DatabaseType.H2, lists("T").get(0).withParameters(),
                TypedQuery.Output.JSON).root()).sql();
        assertTrue(sql.statement().contains("= ANY(?)"), sql.statement());
        assertEquals(List.of(new ExecutionPlan.Slot("ns", "BIGINT")), sql.slots());
        var refused = assertThrows(com.legend.error.NotImplementedException.class, () -> plan(DatabaseType.DuckDB,
                "{ps: Decimal[*]|#>{s::DB.T}#->filter(r|$r.ID->in($ps))}", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("PARK-20"), refused.getMessage());
    }

    /** Compared with a mapped column, an enumeration's name is translated by a value table at that place, so the
     *  comparison reads the stored column and keeps its index (§9, measured: probes/enum-index-results.txt). */
    @Test
    void anEnumerationParameterIsComparedThroughAValueTable() {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(enumModel(DatabaseType.DuckDB)),
                enumerations().get(0).withParameters()).executionPlan("s::RT", TypedQuery.Output.JSON);
        String statement = ((ExecutionPlan.TextResult) plan.root()).sql().statement();
        assertTrue(statement.contains("IN (SELECT") && statement.contains("VALUES ('A', 'ACTIVE'), ('X', 'ACTIVE'),"
                + " ('C', 'CLOSED')"), statement);
        assertTrue(!statement.contains("THEN 'ACTIVE'"), "the column is read stored, never decoded: " + statement);
    }

    @Test
    void anOptionalParametersEqualityIsNullSafe() {
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(DatabaseType.DuckDB,
                optionals("T").get(0).withParameter(), TypedQuery.Output.JSON).root()).sql();
        assertTrue(sql.statement().contains("IS NOT DISTINCT FROM ?"), sql.statement());
    }

    @Test
    void aScalarParametersPlanAnswersAsTheQueryWithItsValue_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (Parameterised q : SCALARS) {
                if (type == DatabaseType.H2 && q.untypedOnH2()) {
                    continue;   // aParameterWithNoOneTypeIsRefusedOnH2ByName
                }
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, q.withParameters(), q.withLets(), q.values(), output);
                }
            }
        }
    }

    /** H2 types a parameter when it prepares the statement, and no type keeps a decimal value's own scale
     *  (probes/literal-results.txt), nor names a Date's or a Number's: refused on H2, by name. */
    @Test
    void aParameterWithNoOneTypeIsRefusedOnH2ByName() {
        for (Parameterised q : SCALARS) {
            if (q.untypedOnH2()) {
                var refused = assertThrows(com.legend.sql.dialect.DialectCapability.class,
                        () -> plan(DatabaseType.H2, q.withParameters(), TypedQuery.Output.JSON));
                assertTrue(refused.getMessage().contains("has no one type a statement names"),
                        refused.getMessage());
            }
        }
    }


    @Test
    void aScalarParameterIsDeclared_andBoundWhereItIsWritten() {
        ExecutionPlan plan = plan(DatabaseType.H2, "{n: Integer[1]|#>{s::DB.T}#->filter(r|$r.ID != $n)"
                + "->extend(~plus: r|$r.ID + $n)->select(~[ID, plus])}", TypedQuery.Output.JSON);
        assertEquals(List.of(new ExecutionPlan.Parameter("n", "Integer", new ExecutionPlan.Multiplicity(1, 1),
                List.of())), plan.parameters());
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan.root()).sql();
        assertEquals(List.of(new ExecutionPlan.Slot("n", null), new ExecutionPlan.Slot("n", null)), sql.slots());
        assertEquals(2, sql.statement().chars().filter(ch -> ch == '?').count(), sql.statement());
    }

    @Test
    void aClassParameterIsRefusedByName() {
        String model = model("DuckDB");
        var aClass = assertThrows(IllegalArgumentException.class, () -> Compiler.query(Compiler.compileModel(model),
                "{i: s::Item[1]|#>{s::DB.T}#->filter(r|$r.ID == $i.id)}").executionPlan("s::RT", TypedQuery.Output.JSON));
        assertTrue(aClass.getMessage().contains("a parameter's value is a plain value"), aClass.getMessage());
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static ExecutionPlan plan(DatabaseType type, String query, TypedQuery.Output output) {
        return Compiler.query(Compiler.compileModel(model(type.name())), query).executionPlan("s::RT", output);
    }

    private static ExecutionPlan.Target target(ExecutionPlan plan) {
        return ((ExecutionPlan.TextResult) plan.root()).sql().target();
    }

    private static void assertAnswersAsToday(DatabaseType type, String query, TypedQuery.Output output)
            throws Exception {
        assertAnswersAsToday(type, query, query, java.util.Map.of(), output);
    }

    /** {@code withParameters}'s plan, run with {@code values}, answers as today's path answers {@code query}. */
    private static void assertAnswersAsToday(DatabaseType type, String withParameters, String query,
            java.util.Map<String, Object> values, TypedQuery.Output output) throws Exception {
        assertAnswersAsToday(model(type.name()), type, withParameters, query, values, output);
    }

    /** {@link #assertAnswersAsToday(DatabaseType, String, String, java.util.Map, TypedQuery.Output)} over
     *  {@code model}, whose runtime is {@code s::RT}. */
    private static void assertAnswersAsToday(String model, DatabaseType type, String withParameters, String query,
            java.util.Map<String, Object> values, TypedQuery.Output output) throws Exception {
        String what = type + " " + output + " " + withParameters;
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), withParameters).executionPlan("s::RT", output);
        try (Connection today = fresh(type); Connection planned = fresh(type)) {
            var ctx = Compiler.compileModel(model);
            var dialect = Databases.dialect(type);
            com.legend.exec.SetupRunner.run(com.legend.setup.CsvSeed.declaredSteps("s::RT", ctx, dialect), today,
                    dialect, null);
            StringWriter out = new StringWriter();
            switch (output) {
                case CSV -> Execution.executeWire(model, query, "s::RT", today, WireRender.Format.CSV, out);
                case JSON -> Execution.executeWire(model, query, "s::RT", today, WireRender.Format.JSON, out);
                case STREAMED_JSON -> Execution.executeStreaming(model, query, "s::RT", today, out);
            }
            assertEquals(out.toString(), run(plan, planned, values), what);
        }
    }

    private static Connection fresh(DatabaseType type) throws SQLException {
        return switch (type) {
            case DuckDB -> DriverManager.getConnection("jdbc:duckdb:");
            case H2 -> DriverManager.getConnection("jdbc:h2:mem:planmaker" + H2_NAMES.incrementAndGet()
                    + com.legend.exec.H2Settings.SETTINGS);
            default -> throw new IllegalArgumentException(type.name());
        };
    }

    /** {@link #run(ExecutionPlan, Connection, java.util.Map)} for a plan of no parameters. */
    public static String run(ExecutionPlan plan, Connection c) throws SQLException {
        return run(plan, c, java.util.Map.of());
    }

    /** {@code plan} run on {@code c} as its steps describe themselves: the session statements, the setup (a rows step:
     *  its staging table created, the cells inserted as text, copied, dropped), then the statement's text, each slot
     *  bound to its parameter's value in {@code values} (a Java value of the parameter's type: the runner's
     *  conversion is step 3's). */
    public static String run(ExecutionPlan plan, Connection c, java.util.Map<String, Object> values)
            throws SQLException {
        ExecutionPlan.TextResult text = (ExecutionPlan.TextResult) plan.root();
        ExecutionPlan.Target target = text.sql().target();
        try (Statement st = c.createStatement()) {
            for (String s : target.session()) {
                st.execute(s);
            }
            for (ExecutionPlan.SetupStep step : target.setup()) {
                switch (step) {
                    case ExecutionPlan.SetupStep.Statement s -> st.execute(s.sql());
                    case ExecutionPlan.SetupStep.Rows r -> {
                        st.execute(r.createStaging());
                        String marks = String.join(", ", java.util.Collections.nCopies(r.rows().get(0).size(), "?"));
                        try (PreparedStatement insert = c.prepareStatement(
                                "insert into " + r.stagingTable() + " values (" + marks + ")")) {
                            for (List<String> row : r.rows()) {
                                for (int i = 0; i < row.size(); i++) {
                                    insert.setString(i + 1, row.get(i));
                                }
                                insert.execute();
                            }
                        }
                        st.execute(r.copy());
                        st.execute(r.dropStaging());
                    }
                }
            }
            assertEquals(plan.parameters().stream().map(ExecutionPlan.Parameter::name).sorted().toList(),
                    values.keySet().stream().sorted().toList(), "a value for every declared parameter");
            try (PreparedStatement statement = c.prepareStatement(text.sql().statement())) {
                List<ExecutionPlan.Slot> slots = text.sql().slots();
                for (int i = 0; i < slots.size(); i++) {
                    String name = slots.get(i).parameter();
                    Object value = values.get(name);
                    if (value == null) {
                        // an optional value's absence: a null of the parameter's declared type
                        statement.setNull(i + 1, nullType(plan.parameters().stream()
                                .filter(p -> p.name().equals(name)).findFirst().orElseThrow().type()));
                    } else if (value instanceof List<?> list) {
                        // a list: ONE array of its element type
                        statement.setArray(i + 1, c.createArrayOf(
                                java.util.Objects.requireNonNull(slots.get(i).arrayElementSqlType()), list.toArray()));
                    } else {
                        statement.setObject(i + 1, value);
                    }
                }
                return answer(text.format(), statement);
            }
        }
    }

    /** The JDBC type of an absent value of a declared Pure type. */
    private static int nullType(String pureType) {
        return switch (pureType) {
            case "Integer" -> java.sql.Types.BIGINT;
            case "String" -> java.sql.Types.VARCHAR;
            case "Boolean" -> java.sql.Types.BOOLEAN;
            case "StrictDate" -> java.sql.Types.DATE;
            case "DateTime" -> java.sql.Types.TIMESTAMP;
            case "Float", "Decimal" -> java.sql.Types.DECIMAL;
            default -> throw new IllegalArgumentException("no absent value of " + pureType + " in these tests");
        };
    }

    /** The text the database writes: one row's one cell, or one JSON object per row in the array's punctuation. */
    private static String answer(ExecutionPlan.Format format, PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            return switch (format) {
                case CSV, JSON -> {
                    assertTrue(rs.next(), "the database writes the whole text as one row");
                    yield rs.getString(1);
                }
                case JSON_PER_ROW -> {
                    List<String> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(rs.getString(1));
                    }
                    yield "[" + String.join(",", rows) + "]";
                }
            };
        }
    }
}
