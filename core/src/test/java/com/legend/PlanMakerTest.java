// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.WireRender;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testcases.PlanCases;
import com.legend.testcases.PlanCases.OptionalCase;
import com.legend.testcases.PlanCases.Parameterised;
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
 * (the runner is step 3; the cases and the run are {@link PlanCases}'). The texts must be equal, byte for byte.
 * Postgres's case is {@code PostgresArmTest}'s (it needs the embedded Postgres).
 */
class PlanMakerTest {

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
            assertEquals(out.toString(), PlanCases.run(plan, planned));
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
                ###Connection
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

    private static final List<Parameterised> SCALARS = PlanCases.scalars("T");

    @Test
    void anOptionalParametersPlanAnswersAsTheQuery_withItsValueAndWithNone() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (OptionalCase q : PlanCases.optionals("T")) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, q.withParameter(), q.withValue(), java.util.Map.of("x", q.value()),
                            output);
                    assertAnswersAsToday(type, q.withParameter(), q.withNone(),
                            java.util.Collections.singletonMap("x", null), output);
                }
            }
        }
    }

    /** {@link PlanCases#enumModel} with its rows as an in-memory connection's declared test data. */
    private static String enumModel(DatabaseType type) {
        return PlanCases.enumModel("type: " + type.name() + "; specification: LocalH2 { testDataSetupCSV: '"
                + PlanCases.enumRows("A").replace("\n", "\\n") + "'; }; auth: DefaultH2;", "A");
    }

    @Test
    void anEnumerationParametersPlanAnswersAsTheQueryWithItsValue() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (Parameterised q : PlanCases.enumerations()) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(enumModel(type), type, q.withParameters(), q.withLets(),
                            q.values(), output);
                }
            }
        }
    }

    @Test
    void aListParametersPlanAnswersAsTheQueryWithItsValues_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (Parameterised q : PlanCases.lists("T")) {
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
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(DatabaseType.H2, PlanCases.lists("T").get(0).withParameters(),
                TypedQuery.Output.JSON).root()).sql();
        assertTrue(sql.statement().contains("= ANY(?)"), sql.statement());
        assertEquals(List.of(new ExecutionPlan.Slot("ns", new ExecutionPlan.Binding.Array("BIGINT"))), sql.slots());
        var refused = assertThrows(com.legend.error.NotImplementedException.class, () -> plan(DatabaseType.DuckDB,
                "{ps: Decimal[*]|#>{s::DB.T}#->filter(r|$r.ID->in($ps))}", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("PARK-20"), refused.getMessage());
    }

    /** Compared with a mapped column, an enumeration's name is translated by a value table at that place, so the
     *  comparison reads the stored column and keeps its index (§9, measured: probes/enum-index-results.txt). */
    @Test
    void anEnumerationParameterIsComparedThroughAValueTable() {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(enumModel(DatabaseType.DuckDB)),
                PlanCases.enumerations().get(0).withParameters()).executionPlan("s::RT", TypedQuery.Output.JSON);
        String statement = ((ExecutionPlan.TextResult) plan.root()).sql().statement();
        assertTrue(statement.contains("IN (SELECT") && statement.contains("VALUES ('A', 'ACTIVE'), ('X', 'ACTIVE'),"
                + " ('C', 'CLOSED')"), statement);
        assertTrue(!statement.contains("THEN 'ACTIVE'"), "the column is read stored, never decoded: " + statement);
    }

    @Test
    void anOptionalParametersEqualityIsNullSafe() {
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(DatabaseType.DuckDB,
                PlanCases.optionals("T").get(0).withParameter(), TypedQuery.Output.JSON).root()).sql();
        assertTrue(sql.statement().contains("IS NOT DISTINCT FROM ?"), sql.statement());
    }

    @Test
    void aScalarParametersPlanAnswersAsTheQueryWithItsValue_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (Parameterised q : SCALARS) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, q.withParameters(), q.withLets(), q.values(), output);
                }
            }
        }
    }

    /** H2 types a parameter when it prepares the statement, and a Float's, a Decimal's, a Date's or a Number's type is
     *  its value's: H2's plan casts it to a TYPE HOLE the runner fills with the type H2 gives that value's literal
     *  (probes/value-typed-cast-results.txt); DuckDB's binds it bare, as the database types the value. Every scalar case
     *  above answers through it on H2. */
    @Test
    void aParameterTypedByItsValueIsCastToATypeHoleOnH2() {
        ExecutionPlan.Sql h2 = ((ExecutionPlan.TextResult) plan(DatabaseType.H2, "{f: Float[1]|#>{s::DB.T}#"
                + "->extend(~x: r|$r.ID * $f)->select(~[ID, x])}", TypedQuery.Output.JSON).root()).sql();
        assertEquals(1, h2.slots().size());
        ExecutionPlan.Binding.One one = (ExecutionPlan.Binding.One) h2.slots().get(0).binding();
        ExecutionPlan.TypeHole hole = java.util.Objects.requireNonNull(one.hole(), "a type hole");
        assertEquals("CAST(? AS )", h2.statement().substring(hole.at() - "CAST(? AS ".length(), hole.at() + 1),
                h2.statement());
        assertEquals(java.util.Map.of(
                ExecutionPlan.ValueKind.DECIMAL, new ExecutionPlan.TypeSpelling("NUMERIC",
                        ExecutionPlan.Digits.PRECISION_AND_SCALE),
                ExecutionPlan.ValueKind.FLOATING, new ExecutionPlan.TypeSpelling("DECFLOAT",
                        ExecutionPlan.Digits.PRECISION)), hole.types());
        assertEquals(ExecutionPlan.ValueKind.DECIMAL, hole.absent());
        assertEquals("DECIMAL", one.nullType());
        ExecutionPlan.Sql duck = ((ExecutionPlan.TextResult) plan(DatabaseType.DuckDB, "{f: Float[1]|#>{s::DB.T}#"
                + "->extend(~x: r|$r.ID * $f)->select(~[ID, x])}", TypedQuery.Output.JSON).root()).sql();
        assertEquals(List.of(new ExecutionPlan.Slot("f", new ExecutionPlan.Binding.One("DECIMAL", null))),
                duck.slots());
    }

    /** A DateTime parameter is cast to the type of its value's literal on every database, and passed as its text: H2's
     *  keeps nanoseconds (a plain TIMESTAMP keeps six digits and rounds the rest; probes/value-typed-cast-results.txt),
     *  DuckDB's is a TIMESTAMP_NS only when the value has digits finer than a microsecond, Postgres's keeps six and cuts
     *  the rest, as each literal writer does (probes/timestamp-results.txt). */
    @Test
    void aDateTimeParameterIsCastToItsLiteralsTypeOnEveryDatabase() {
        String query = "{t: DateTime[1]|#>{s::DB.T}#->extend(~t: r|$t)->select(~[ID, t])}";
        java.util.Map<DatabaseType, java.util.Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling>> expected =
                java.util.Map.of(
                        DatabaseType.H2, java.util.Map.of(
                                ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                                        ExecutionPlan.Digits.NONE),
                                ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                                        ExecutionPlan.Digits.NONE)),
                        DatabaseType.DuckDB, java.util.Map.of(
                                ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                        ExecutionPlan.Digits.NONE),
                                ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP_NS",
                                        ExecutionPlan.Digits.NONE)),
                        DatabaseType.Postgres, java.util.Map.of(
                                ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                        ExecutionPlan.Digits.NONE),
                                ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                        ExecutionPlan.Digits.NONE, 6)));
        expected.forEach((type, types) -> {
            ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(type, query, TypedQuery.Output.JSON).root()).sql();
            ExecutionPlan.Binding.One one = (ExecutionPlan.Binding.One) sql.slots().get(0).binding();
            ExecutionPlan.TypeHole hole = java.util.Objects.requireNonNull(one.hole(), type + ": a type hole");
            assertEquals(types, hole.types(), type.name());
            assertEquals(ExecutionPlan.ValueKind.DATE_TIME, hole.absent(), type.name());
            assertEquals("TIMESTAMP", one.nullType(), type.name());
        });
    }


    @Test
    void aScalarParameterIsDeclared_andBoundWhereItIsWritten() {
        ExecutionPlan plan = plan(DatabaseType.H2, "{n: Integer[1]|#>{s::DB.T}#->filter(r|$r.ID != $n)"
                + "->extend(~plus: r|$r.ID + $n)->select(~[ID, plus])}", TypedQuery.Output.JSON);
        assertEquals(List.of(new ExecutionPlan.Parameter("n", "Integer", new ExecutionPlan.Multiplicity(1, 1),
                List.of())), plan.parameters());
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan.root()).sql();
        ExecutionPlan.Slot n = new ExecutionPlan.Slot("n", new ExecutionPlan.Binding.One("BIGINT", null));
        assertEquals(List.of(n, n), sql.slots());
        assertEquals(2, sql.statement().chars().filter(ch -> ch == '?').count(), sql.statement());
    }

    /** What a plan does not bind yet is refused when the plan is made, by name (PARK-21): a class instance, an
     *  optional enumeration (its absence's answer unmeasured against legend-engine). */
    @Test
    void anUnboundParameterKindIsRefusedByName() {
        var aClass = assertThrows(com.legend.error.NotImplementedException.class, () -> plan(DatabaseType.DuckDB,
                "{i: s::Item[1]|#>{s::DB.T}#->filter(r|$r.ID == $i.id)}", TypedQuery.Output.JSON));
        assertTrue(aClass.getMessage().contains("a class instance is not bound") && aClass.getMessage().contains("PARK-21"),
                aClass.getMessage());
        var optionalEnum = assertThrows(com.legend.error.NotImplementedException.class, () -> Compiler.query(
                Compiler.compileModel(enumModel(DatabaseType.DuckDB)), "{st: s::Status[0..1]|s::Acct.all()"
                        + "->filter(a|$a.status == $st)->project(~[id: a|$a.id])}").executionPlan("s::RT", TypedQuery.Output.JSON));
        assertTrue(optionalEnum.getMessage().contains("an optional enumeration's absence")
                && optionalEnum.getMessage().contains("PARK-21"), optionalEnum.getMessage());
    }

    /** A list parameter is bound only as a whole list (in, contains); written where one value goes, it is refused by
     *  name, never bound as something it is not. */
    @Test
    void aListParameterWrittenWhereOneValueGoesIsRefusedByName() {
        var refused = assertThrows(com.legend.sql.dialect.DialectCapability.class, () -> plan(DatabaseType.DuckDB,
                "{ns: Integer[*]|#>{s::DB.T}#->extend(~n: r|$ns->size())->select(~[ID, n])}", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("is bound only as a whole list"), refused.getMessage());
    }

    /** A value's text — a scalar's, a collection's — is the one-column relation {@code value}, whole in every output
     *  (no row to stream), as today's paths write it. */
    @Test
    void aValuesPlanAnswersAsTodaysPaths_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (String query : List.of("|#>{s::DB.T}#->select(~[ID])->size()",
                    "|s::Item.all()->map(i|$i.id)->sort()")) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, query, output);
                }
            }
        }
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
            assertEquals(out.toString(), PlanCases.run(plan, planned, values), what);
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
}
