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
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner's plans (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): a plan answers exactly as
 * today's wire and streaming paths answer the same query. Each query runs twice, on two fresh databases: through
 * {@link Execution} (its connection's declared test data established first, as the server establishes it), and through
 * its plan — the target's session statements, its setup steps, its statement — run here as each step describes itself
 * (the runner is step 3; the cases and the run are {@link PlanCases}'). The texts must be equal, byte for byte.
 * Each in-process database answers; Postgres's case is {@code PostgresArmTest}'s (it needs the embedded Postgres). What
 * one database's plan says (its versions, its session, its bindings) is that database's test, {@code PlanMaker<Db>Test}
 * (the dialect program's Phase 6, 2026-10-16).
 */
class PlanMakerTest {

    /** The register of each database's known differences from today's paths, shrink-only: a row is its backend, the
     *  output, the query (today's form: its values as lets) and the plan's own answer, exactly, or its refusal
     *  ({@code refused: } and its message), escaped to one line; lines starting {@code #} are its reasons (the dialect
     *  program's Phase 6, 2026-10-16, when the cases first ran on SQLite; exact since the audit of the branch). */
    private static final String DIFFERENCES_FILE = "plan-maker/differences.tsv";

    private static final java.util.Map<String, String> DIFFERENCES = differences();

    /** The register's rows the cases reached, so that a row no case reaches fails ({@link #everyDifferenceIsACase}). */
    private static final java.util.Set<String> CONSULTED = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private static java.util.Map<String, String> differences() {
        java.util.Map<String, String> rows = new java.util.LinkedHashMap<>();
        try (var in = java.util.Objects.requireNonNull(PlanMakerTest.class.getClassLoader()
                .getResourceAsStream(DIFFERENCES_FILE), DIFFERENCES_FILE)) {
            for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] cells = line.split("\t");
                if (cells.length != 4) {
                    throw new IllegalStateException(DIFFERENCES_FILE + ": a row is backend, output, query, answer: "
                            + line);
                }
                rows.put(cells[0] + "\t" + cells[1] + "\t" + cells[2], cells[3]);
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return rows;
    }

    /** The register's rows, at most: 31 measured 2026-10-16, when the cases first ran on SQLite (the dialect
     *  program's Phase 6). Lowered with the rows; raised only with a dated reason. 31 -> 35 (2026-10-22, main's merge
     *  into the plan path branch): the branch's TDS text output runs every case, and SQLite refuses its eight list
     *  parameters as it refuses the other outputs' (it binds no array, +8); a Date or DateTime parameter answers in
     *  JSON on SQLite as today now (its four rows removed, -4). */
    private static final int MAX_DIFFERENCES = 35;

    /** The test methods run in this class, so that unreached rows are judged on a whole run only. */
    private static final java.util.concurrent.atomic.AtomicInteger RAN = new java.util.concurrent.atomic.AtomicInteger();

    @org.junit.jupiter.api.AfterEach
    void ran() {
        RAN.incrementAndGet();
    }

    @org.junit.jupiter.api.AfterAll
    static void everyDifferenceIsACase() {
        assertTrue(DIFFERENCES.size() <= MAX_DIFFERENCES, DIFFERENCES_FILE + " has " + DIFFERENCES.size()
                + " rows, above its ceiling " + MAX_DIFFERENCES + ": a row is added only with a dated reason");
        long declared = java.util.Arrays.stream(PlanMakerTest.class.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class)).count();
        if (RAN.get() < declared) {
            return;   // a run of some of the methods reaches some of the rows
        }
        java.util.Set<String> unreached = new java.util.TreeSet<>(DIFFERENCES.keySet());
        unreached.removeAll(CONSULTED);
        assertEquals(java.util.Set.of(), unreached, "rows of " + DIFFERENCES_FILE + " no case reaches: remove them");
    }

    /** The databases a plan is run on here: every in-process one. */
    static final List<DatabaseType> IN_PROCESS = TestBackends.inProcess().stream().map(TestBackend::type).toList();

    /** A table of three rows, declared as the connection's test data; {@code type} is the connection's database. */
    static String model(String type) {
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
        for (DatabaseType type : IN_PROCESS) {
            for (String query : List.of(RELATION, NONE, PROJECT)) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, query, output);
                }
            }
        }
    }

    @Test
    void aGraphFetchsPlanAnswersAsTodaysPaths_inJson() throws Exception {
        for (DatabaseType type : IN_PROCESS) {
            assertAnswersAsToday(type, GRAPH, TypedQuery.Output.JSON);
            assertAnswersAsToday(type, GRAPH, TypedQuery.Output.STREAMED_JSON);
            var refused = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> plan(type, GRAPH, TypedQuery.Output.CSV));
            assertTrue(refused.getMessage().contains("graph results have no CSV wire"), refused.getMessage());
        }
    }

    /** A double that is no number prints as Pure prints it ({@code NaN}; {@code Infinity} and {@code -Infinity}
     *  the same way), whatever the database writes for it: the channel A shadow run of 2026-10-18 found
     *  {@code acos(2.0)} printed {@code nan.0} on DuckDB (the plan path program's step 7). */
    @Test
    void aDoubleThatIsNoNumberPrintsAsPurePrintsIt() throws Exception {
        assertNoNumberPrintsAsPure(Databases.PLATFORM);    // and on H2: PlanMakerH2Test
    }

    /** {@link #aDoubleThatIsNoNumberPrintsAsPurePrintsIt} on {@code type}. */
    static void assertNoNumberPrintsAsPure(DatabaseType type) throws Exception {
        try (Connection c = fresh(type)) {
            assertEquals("value:Float[1]\nNaN",
                    PlanCases.run(plan(type, "|2.0->acos()", TypedQuery.Output.TDS_TEXT), c), type.name());
        }
    }

    /** The target names its connection on every database; its versions, session and rows are each database's own
     *  ({@code PlanMaker<Db>Test}). */
    @Test
    void theTargetNamesItsConnection() {
        for (DatabaseType type : IN_PROCESS) {
            ExecutionPlan.Target t = target(plan(type, RELATION, TypedQuery.Output.JSON));
            assertEquals("s::Conn", ((ExecutionPlan.Database.Declared) t.database()).connection().qualifiedName(),
                    type.name());
        }
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
        try (Connection today = TestBackends.platform().open();
             Connection planned = TestBackends.platform().open()) {
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
        for (DatabaseType type : IN_PROCESS) {
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
        for (DatabaseType type : IN_PROCESS) {
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
        for (DatabaseType type : IN_PROCESS) {
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

    /** A list of decimals is refused by name (a list's one array is each database's: {@code PlanMaker<Db>Test}). */
    @Test
    void aListOfDecimalsIsRefusedByName() {
        var refused = assertThrows(com.legend.error.NotImplementedException.class, () -> plan(Databases.PLATFORM,
                "{ps: Decimal[*]|#>{s::DB.T}#->filter(r|$r.ID->in($ps))}", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("PARK-20"), refused.getMessage());
    }

    /** Compared with a mapped column, an enumeration's name is translated by a value table at that place, so the
     *  comparison reads the stored column and keeps its index (§9, measured: probes/enum-index-results.txt). */
    @Test
    void anEnumerationParameterIsComparedThroughAValueTable() {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(enumModel(Databases.PLATFORM)),
                PlanCases.enumerations().get(0).withParameters()).executionPlan("s::RT", TypedQuery.Output.JSON);
        String statement = ((ExecutionPlan.TextResult) plan.root()).sql().statement();
        assertTrue(statement.contains("IN (SELECT") && statement.contains("VALUES ('A', 'ACTIVE'), ('X', 'ACTIVE'),"
                + " ('C', 'CLOSED')"), statement);
        assertTrue(!statement.contains("THEN 'ACTIVE'"), "the column is read stored, never decoded: " + statement);
    }

    @Test
    void anOptionalParametersEqualityIsNullSafe() {
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(Databases.PLATFORM,
                PlanCases.optionals("T").get(0).withParameter(), TypedQuery.Output.JSON).root()).sql();
        assertTrue(sql.statement().contains("IS NOT DISTINCT FROM ?"), sql.statement());
    }

    /** Trees planted by city and year, for a pivot whose columns the database names itself. */
    private static final String TREES = "#TDS\n city, year, treePlanted\n NYC, 2011, 5\n NYC, 2000, 15\n SAN, 2011, 2\n"
            + " SAN, 2000, 1\n SAN, 2000, 2\n#";

    /**
     * A DYNAMIC pivot's probe plan (2026-10-20, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9): a pivot no cast names has
     * columns its data names, so its plan reads the keys first (their text, and each generated column's name as the
     * output writes one, both by the database) and its body is written once per key in the regions the runner repeats,
     * each copy binding its key; every output and every database alike. Judged against the legacy probe route's text
     * on DuckDB, and the same answers on H2 and SQLite, which have no dynamic PIVOT (the row-as-JSON form this replaces
     * had no form there; DuckDB's streamed rows dropped the generated columns). Every carrier names a generated column
     * as the engine labels it, the quotes part of the name (the pivot homework, docs/PIVOT_UPSTREAM_HOMEWORK_2026_10_18.md).
     */
    @Test
    void aDynamicPivotsPlanReadsItsKeysFirst_everyOutputEveryDatabase() throws Exception {
        String query = TREES + "->pivot(~[year], ~['newCol' : x | $x.treePlanted : y | $y->plus()])"
                + "->sort(~city->ascending())";
        assertAnswersAsToday(model(Databases.PLATFORM.name()), Databases.PLATFORM, query, query, java.util.Map.of(),
                TypedQuery.Output.TDS_TEXT);
        String json = "[{\"city\":\"NYC\",\"'2000__|__newCol'\":15,\"'2011__|__newCol'\":5},"
                + "{\"city\":\"SAN\",\"'2000__|__newCol'\":3,\"'2011__|__newCol'\":2}]";
        for (DatabaseType type : IN_PROCESS) {
            String model = model(type.name());
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query).executionPlan("s::RT",
                    TypedQuery.Output.TDS_TEXT);
            assertTrue(plan.root() instanceof ExecutionPlan.Keyed, type + ": a keyed plan");
            com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, query);
            try (Connection c = fresh(type)) {
                String text = PlanCases.run(plan, c);
                assertTrue(text.startsWith("city:String[") && text.endsWith("]," + "'\\'2000__|__newCol\\'':Integer[1],"
                        + "'\\'2011__|__newCol\\'':Integer[1]\nNYC,15,5\nSAN,3,2"), type + ": " + text);
            }
            for (TypedQuery.Output output : List.of(TypedQuery.Output.CSV, TypedQuery.Output.JSON,
                    TypedQuery.Output.STREAMED_JSON)) {
                ExecutionPlan p = Compiler.query(Compiler.compileModel(model), query).executionPlan("s::RT", output);
                com.legend.testcases.PlanRoundTrip.assertRoundTrips(p, query);
                try (Connection c = fresh(type)) {
                    assertEquals(output == TypedQuery.Output.CSV
                            ? "city,'2000__|__newCol','2011__|__newCol'\r\nNYC,15,5\r\nSAN,3,2\r\n" : json,
                            PlanCases.run(p, c), type + " " + output);
                }
            }
        }
        // a STATIC pivot (the engine's form: one column, its values, one aggregate) has its columns from the typer,
        // named bare; both wires present them the same way, one statement
        String statics = TREES + "->pivot(~year, [2000, 2011], ~newCol : x | $x.treePlanted : y | $y->plus())"
                + "->sort(~city->ascending())";
        String model = model(Databases.PLATFORM.name());
        try (Connection c = fresh(Databases.PLATFORM)) {
            assertEquals("city,'2000__|__newCol','2011__|__newCol'\r\nNYC,15,5\r\nSAN,3,2\r\n", PlanCases.run(
                    Compiler.query(Compiler.compileModel(model), statics).executionPlan("s::RT", TypedQuery.Output.CSV), c));
            assertEquals(json, PlanCases.run(
                    Compiler.query(Compiler.compileModel(model), statics).executionPlan("s::RT", TypedQuery.Output.JSON), c));
        }
    }

    /**
     * A dynamic pivot's probe plan is made on every SQL database, each select's per-key projections one region: a dialect
     * writing its own select writes its per-key run through the base's projections (2026-10-21: Snowflake's wrote each
     * projection itself and refused its ten uncast pivots "a per-key name outside a per-key region", found on its lane;
     * no other test made a plan on a cloud database).
     */
    @Test
    void aDynamicPivotsProbePlanIsMadeOnEverySqlDatabase() {
        String query = TREES + "->pivot(~[year], ~['newCol' : x | $x.treePlanted : y | $y->plus()])"
                + "->sort(~city->ascending())";
        for (DatabaseType type : Databases.relational()) {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model(type.name())), query).executionPlan("s::RT",
                    TypedQuery.Output.TDS_TEXT);
            assertTrue(plan.root() instanceof ExecutionPlan.Keyed, type + ": a keyed plan");
            // every reference to a per-key column carries the runner's per-key name, never the bare stem (Snowflake
            // read the outer select's as a plain column, measured on its lane 2026-10-21)
            String body = ((ExecutionPlan.Keyed) plan.root()).body().statement();
            assertTrue(!java.util.regex.Pattern.compile("(?<!_pk_)_u0").matcher(body).find(), type + ": " + body);
            com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, type + " " + query);
        }
    }

    /**
     * A probe plan's keys run first and its body is written for them: no key (an empty source) answers the fixed
     * columns alone; a key written into the data between plans is the next run's column (2026-10-20).
     */
    @Test
    void aDynamicPivotsProbePlanAnswersForTheKeysItsRunFinds() throws Exception {
        String empty = "#TDS\n city, year, treePlanted\n NYC, 2011, 5\n#->filter(r | $r.city == 'none')"
                + "->pivot(~[year], ~['newCol' : x | $x.treePlanted : y | $y->plus()])";
        for (DatabaseType type : IN_PROCESS) {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model(type.name())), empty).executionPlan("s::RT",
                    TypedQuery.Output.TDS_TEXT);
            try (Connection c = fresh(type)) {
                String text = PlanCases.run(plan, c);
                assertTrue(text.startsWith("city:String[") && !text.contains(",") && !text.contains("\n"),
                        type + ": " + text);
            }
        }
    }

    /** The PCT's trees by city, country and year, with a coefficient (testPivot_SingleMultiple_Dynamic_Aggregation). */
    private static final String TREES_PCT = "#TDS\n city, country, year, treePlanted, coefficient\n NYC, USA, 2011, 5000, 1\n"
            + " NYC, USA, 2000, 5000, 2\n SAN, USA, 2000, 2000, 1\n SAN, USA, 2011, 100, 2\n LDN, UK, 2011, 3000, 2\n"
            + " SAN, USA, 2011, 2500, 1\n NYC, USA, 2000, 10000, 2\n NYC, USA, 2012, 7600, 1\n NYC, USA, 2012, 7600, 2\n#";

    /** The same trees without the coefficient (the PCT's cast cases): a column no aggregate reads stays a group key. */
    private static final String TREES_4 = "#TDS\n city, country, year, treePlanted\n NYC, USA, 2011, 5000\n NYC, USA, 2000, 5000\n"
            + " SAN, USA, 2000, 2000\n SAN, USA, 2011, 100\n LDN, UK, 2011, 3000\n SAN, USA, 2011, 2500\n"
            + " NYC, USA, 2000, 10000\n NYC, USA, 2012, 7600\n NYC, USA, 2012, 7600\n#";

    private static final String CAST_PIVOT = "->cast(@meta::pure::metamodel::relation::Relation<(city:String[0..1],"
            + " country:String[0..1], '\\'2000__|__newCol\\'':Integer[0..1], '\\'2011__|__newCol\\'':Integer[0..1],"
            + " '\\'2012__|__newCol\\'':Integer[0..1])>)";

    /**
     * Two holes the channel A shadow run found (2026-10-18; the plan path program's step 7): an aggregate over an
     * EXPRESSION removes every column it reads from the pivot's static columns (the engine's expected rows of
     * testPivot_SingleMultiple_Dynamic_Aggregation), and a cast naming a dynamic pivot's generated columns projects
     * them, so a filter, a sort or a groupBy over the cast answers through the plan as an ordinary relation.
     */
    @Test
    void aPivotOverAnExpressionAndACastOverADynamicPivotAnswerAsTheEngine() throws Exception {
        String model = model(Databases.PLATFORM.name());
        var ctx = Compiler.compileModel(model);
        try (Connection c = fresh(Databases.PLATFORM)) {
            String expression = TREES_PCT + "->pivot(~[year], ~[sum: x | $x.treePlanted->toOne() * $x.coefficient->toOne()"
                    + " : y | $y->plus(), count: x | 1 : y | $y->plus()])->sort(~city->ascending())";
            String text = PlanCases.run(Compiler.query(ctx, expression).executionPlan("s::RT", TypedQuery.Output.TDS_TEXT), c);
            assertTrue(text.startsWith("city:String[1],country:String[1],'\\'2000__|__sum\\'':Integer[1],"
                    + "'\\'2000__|__count\\'':Integer[1],'\\'2011__|__sum\\'':Integer[1],'\\'2011__|__count\\'':Integer[1],"
                    + "'\\'2012__|__sum\\'':Integer[1],'\\'2012__|__count\\'':Integer[1]\n"), text);
            assertTrue(text.endsWith("\nLDN,UK,null,null,6000,1,null,null\nNYC,USA,30000,2,5000,1,22800,2"
                    + "\nSAN,USA,2000,1,2700,2,null,null"), text);

            String pivot = TREES_4 + "->pivot(~[year], ~[newCol: x | $x.treePlanted : y | $y->plus()])" + CAST_PIVOT;
            String filtered = PlanCases.run(Compiler.query(ctx, pivot + "->filter(x | $x.city == 'NYC')")
                    .executionPlan("s::RT", TypedQuery.Output.TDS_TEXT), c);
            assertEquals("city:String[0..1],country:String[0..1],'\\'2000__|__newCol\\'':Integer[0..1],"
                    + "'\\'2011__|__newCol\\'':Integer[0..1],'\\'2012__|__newCol\\'':Integer[0..1]\nNYC,USA,15000,5000,15200",
                    filtered);
            String sorted = PlanCases.run(Compiler.query(ctx, pivot + "->sort(~city->ascending())")
                    .executionPlan("s::RT", TypedQuery.Output.TDS_TEXT), c);
            assertTrue(sorted.endsWith("\nLDN,UK,null,3000,null\nNYC,USA,15000,5000,15200\nSAN,USA,2000,2600,null"), sorted);
            String grouped = PlanCases.run(Compiler.query(ctx, pivot + "->groupBy(~[country], ~['\\'2011__|__newCol\\'': x |"
                    + " $x.'\\'2011__|__newCol\\'' : y | $y->plus()])->sort(~country->ascending())")
                    .executionPlan("s::RT", TypedQuery.Output.TDS_TEXT), c);
            assertEquals("country:String[0..1],'\\'2011__|__newCol\\'':Integer[1]\nUK,3000\nUSA,7600", grouped);
            ExecutionPlan csv = Compiler.query(ctx, pivot + "->filter(x | $x.city == 'NYC')").executionPlan("s::RT", TypedQuery.Output.CSV);
            assertEquals("city,country,'2000__|__newCol','2011__|__newCol','2012__|__newCol'\r\nNYC,USA,15000,5000,15200\r\n",
                    PlanCases.run(csv, c));
        }
    }

    /**
     * A cast over a DYNAMIC pivot on a database without a dynamic PIVOT (the plan path program's step 9, 2026-10-19:
     * the PCT's four composition cases, {@code test_Pivot_Filter} among them, refused on every lane but DuckDB with
     * "pivot reached a dialect without a PIVOT strategy"): the keys the cast names are the pivot's NAMED values
     * ({@code SqlSource.Pivot.named}), each the ON key's typed literal, which the dialect's static emulation pivots
     * on (H2's grouped CASE aggregation) with no row pinned -- the group whose key the cast does not name keeps its
     * row, its named cells null, as DuckDB's dynamic form answers through the same cast. DuckDB itself keeps the
     * dynamic form (engine-exact); the plan round-trips with the named values (format 7).
     */
    @Test
    void aCastOverADynamicPivotAnswersOnADatabaseWithoutADynamicPivot() throws Exception {
        String cast = "->cast(@meta::pure::metamodel::relation::Relation<(city:String[0..1], country:String[0..1],"
                + " '\\'2011__|__newCol\\'':Integer[0..1], '\\'2012__|__newCol\\'':Integer[0..1])>)";
        String query = TREES_4 + "->pivot(~[year], ~[newCol: x | $x.treePlanted : y | $y->plus()])" + cast
                + "->sort(~city->ascending())";
        String expected;
        try (Connection c = fresh(Databases.PLATFORM)) {
            ExecutionPlan dynamic = Compiler.query(Compiler.compileModel(model(Databases.PLATFORM.name())), query)
                    .executionPlan("s::RT", TypedQuery.Output.TDS_TEXT);
            assertTrue(((ExecutionPlan.TextResult) dynamic.root()).sql().statement().contains("PIVOT"),
                    "the platform's dynamic form");
            expected = PlanCases.run(dynamic, c);
        }
        // the 2000 group (SAN's 2000 trees, NYC's 15000) keeps its row with its named cells null
        assertEquals("city:String[0..1],country:String[0..1],'\\'2011__|__newCol\\'':Integer[0..1],"
                + "'\\'2012__|__newCol\\'':Integer[0..1]\nLDN,UK,3000,null\nNYC,USA,5000,15200\nSAN,USA,2600,null", expected);
        // every in-process database without a dynamic PIVOT (its dialect's static emulation; main's merge,
        // 2026-10-22: it named H2)
        int emulated = 0;
        for (DatabaseType type : IN_PROCESS) {
            if (!Databases.dialect(type).needsStaticPivot()) {
                continue;
            }
            emulated++;
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model(type.name())), query)
                    .executionPlan("s::RT", TypedQuery.Output.TDS_TEXT);
            com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, query);
            String statement = ((ExecutionPlan.TextResult) plan.root()).sql().statement();
            assertFalse(statement.contains("PIVOT"), type + ": " + statement);
            assertTrue(statement.contains("CASE WHEN") && statement.contains("= 2011") && statement.contains("= 2012"),
                    type + ": " + statement);
            try (Connection c = fresh(type)) {
                assertEquals(expected, PlanCases.run(plan, c), type.name());
            }
        }
        assertTrue(emulated > 0, "no in-process database emulates the pivot");
    }

    @Test
    void aScalarParametersPlanAnswersAsTheQueryWithItsValue_everyOutput() throws Exception {
        for (DatabaseType type : IN_PROCESS) {
            for (Parameterised q : SCALARS) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, q.withParameters(), q.withLets(), q.values(), output);
                }
            }
        }
    }

    /** A DateTime parameter is cast to the type of its value's literal on every database (each one's spelling is its
     *  own test's, {@code PlanMaker<Db>Test}): a type hole, absent a date-time, a null a TIMESTAMP. */
    @Test
    void aDateTimeParameterIsCastToATypeHoleOnEveryDatabase() {
        for (DatabaseType type : IN_PROCESS) {
            ExecutionPlan.Binding.One one = dateTimeBinding(type);
            ExecutionPlan.TypeHole hole = java.util.Objects.requireNonNull(one.hole(), type + ": a type hole");
            assertEquals(java.util.Set.of(ExecutionPlan.ValueKind.DATE_TIME, ExecutionPlan.ValueKind.DATE_TIME_NANOS),
                    hole.types().keySet(), type.name());
            assertEquals(ExecutionPlan.ValueKind.DATE_TIME, hole.absent(), type.name());
            assertEquals("TIMESTAMP", one.nullType(), type.name());
        }
    }

    /** The binding of a DateTime parameter, written as a column's value, on {@code type}. */
    static ExecutionPlan.Binding.One dateTimeBinding(DatabaseType type) {
        ExecutionPlan.Sql sql = ((ExecutionPlan.TextResult) plan(type, "{t: DateTime[1]|#>{s::DB.T}#->extend(~t: r|$t)"
                + "->select(~[ID, t])}", TypedQuery.Output.JSON).root()).sql();
        return (ExecutionPlan.Binding.One) sql.slots().get(0).binding();
    }

    /** What a plan does not bind yet is refused when the plan is made, by name (PARK-21): a class instance, an
     *  optional enumeration (its absence's answer unmeasured against legend-engine). */
    @Test
    void anUnboundParameterKindIsRefusedByName() {
        var aClass = assertThrows(com.legend.error.NotImplementedException.class, () -> plan(Databases.PLATFORM,
                "{i: s::Item[1]|#>{s::DB.T}#->filter(r|$r.ID == $i.id)}", TypedQuery.Output.JSON));
        assertTrue(aClass.getMessage().contains("a class instance is not bound") && aClass.getMessage().contains("PARK-21"),
                aClass.getMessage());
        var optionalEnum = assertThrows(com.legend.error.NotImplementedException.class, () -> Compiler.query(
                Compiler.compileModel(enumModel(Databases.PLATFORM)), "{st: s::Status[0..1]|s::Acct.all()"
                        + "->filter(a|$a.status == $st)->project(~[id: a|$a.id])}").executionPlan("s::RT", TypedQuery.Output.JSON));
        assertTrue(optionalEnum.getMessage().contains("an optional enumeration's absence")
                && optionalEnum.getMessage().contains("PARK-21"), optionalEnum.getMessage());
    }

    /** A list parameter is bound only as a whole list (in, contains); written where one value goes, it is refused by
     *  name, never bound as something it is not. */
    @Test
    void aListParameterWrittenWhereOneValueGoesIsRefusedByName() {
        var refused = assertThrows(com.legend.sql.dialect.DialectCapability.class, () -> plan(Databases.PLATFORM,
                "{ns: Integer[*]|#>{s::DB.T}#->extend(~n: r|$ns->size())->select(~[ID, n])}", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("is bound only as a whole list"), refused.getMessage());
    }

    /** A value's text — a scalar's, a collection's — is the one-column relation {@code value}, whole in every output
     *  (no row to stream), as today's paths write it. */
    @Test
    void aValuesPlanAnswersAsTodaysPaths_everyOutput() throws Exception {
        for (DatabaseType type : IN_PROCESS) {
            for (String query : List.of("|#>{s::DB.T}#->select(~[ID])->size()",
                    "|s::Item.all()->map(i|$i.id)->sort()")) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, query, output);
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    /** The plan path program's step 1a (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3): every result column carries its
     *  Pure type and multiplicity, the SQL type of its slot, and its store's declared type by provenance -- or none,
     *  said so: a computed column has no declared type, a mapped property reads its column's. */
    @Test
    void aColumnCarriesItsThreeTypeFacts() {
        List<ExecutionPlan.Column> cols = columns(plan(Databases.PLATFORM, RELATION, TypedQuery.Output.TDS_TEXT));
        assertEquals(List.of("ID", "NAME", "PRICE"), cols.stream().map(ExecutionPlan.Column::name).toList(),
                cols.toString());
        assertEquals(List.of("INTEGER", "VARCHAR(20)", "DECIMAL(10,2)"),
                cols.stream().map(ExecutionPlan.Column::store).toList(), cols.toString());
        assertEquals("String", cols.get(1).type(), cols.toString());
        assertEquals("VARCHAR", cols.get(1).slot(), cols.toString());
        assertEquals("DECIMAL(10,2)", cols.get(2).slot(), cols.toString());
        List<ExecutionPlan.Column> computed = columns(plan(Databases.PLATFORM,
                "|#>{s::DB.T}#->extend(~NEXT: r|$r.ID + 1)->select(~[NAME, NEXT])", TypedQuery.Output.CSV));
        assertEquals("VARCHAR(20)", computed.get(0).store(), computed.toString());
        assertEquals(null, computed.get(1).store(), computed.toString());
        assertEquals("Integer", computed.get(1).type(), computed.toString());
        List<ExecutionPlan.Column> project = columns(plan(Databases.PLATFORM, PROJECT, TypedQuery.Output.CSV));
        assertEquals(List.of("VARCHAR(20)", "DECIMAL(10,2)"),
                project.stream().map(ExecutionPlan.Column::store).toList(), project.toString());
        List<ExecutionPlan.Column> value = columns(plan(Databases.PLATFORM, "|1 + 2", TypedQuery.Output.TDS_TEXT));
        assertEquals(1, value.size(), value.toString());
        assertEquals("value", value.get(0).name(), value.toString());
        assertEquals("Integer", value.get(0).type(), value.toString());
        assertEquals(null, value.get(0).store(), value.toString());
        assertEquals(new ExecutionPlan.Multiplicity(1, 1), value.get(0).multiplicity(), value.toString());
    }

    /** The plan path program's step 1b (docs/PLAN_PATH_PROGRAM_2026_10_17.md §2, §3): a plan written to its JSON and
     *  read back is the same plan, its typed SQL tree included, and the tree read back renders the same statement
     *  with the same bindings on the plan's own dialect. */
    @Test
    void aPlansTypedTreeReadsBackAndRendersTheSameStatement() {
        for (DatabaseType type : IN_PROCESS) {
            for (String query : List.of(RELATION, NONE, PROJECT, "|1 + 2",
                    "|#>{s::DB.T}#->extend(~NEXT: r|$r.ID + 1)->select(~[NAME, NEXT])",
                    "|#>{s::DB.T}#->groupBy(~[NAME], ~[total: r|$r.PRICE : y|$y->sum()])->sort(~NAME->ascending())",
                    "|#>{s::DB.T}#->extend(over(~NAME->ascending()), ~rank: {p, w, r | $p->rank($w, $r)})",
                    "|#>{s::DB.T}#->select(~[ID])->concatenate(#>{s::DB.T}#->select(~[ID]))->sort(~ID->ascending())")) {
                for (TypedQuery.Output output : List.of(TypedQuery.Output.TDS_TEXT, TypedQuery.Output.CSV)) {
                    ExecutionPlan plan = plan(type, query, output);
                    com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, type + " " + output + " " + query);
                }
            }
            com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan(type, GRAPH, TypedQuery.Output.JSON),
                    type + " JSON " + GRAPH);
        }
    }

    private static List<ExecutionPlan.Column> columns(ExecutionPlan plan) {
        return ((ExecutionPlan.Relation) ((ExecutionPlan.TextResult) plan.root()).type()).columns();
    }

    /** The PCT's {@code getAll::testBasic} shape: the system metamodel store read by a plan. */
    private static final String CLASSES = "|meta::pure::metamodel::type::Class.all()->isEmpty()";

    /**
     * The system metamodel store's tables, in the form the plan's database takes (the plan path program's step 9):
     * on an engine in this process, the target's setup, in their schema (2026-10-18); on a SERVER engine, the
     * session's own TEMPORARY tables (the legacy system database's form, the user's design of 2026-10-11), made by
     * the session's statements and read by the query under their session names, with no setup for the user's
     * database (2026-10-19: every server lane had pinned {@code testBasic} by "the plan has setup for it: never run
     * on it"); on a server without temporary tables (Trino, MongoDB; 2026-10-20) the tables are the query's own
     * rows, inline.
     */
    @Test
    void theMetamodelStoresTablesAreThePlansSetupInProcessAndTheSessionsOwnTablesOnAServer() throws Exception {
        // every relational database, each by its dialect's facts (main's merge, 2026-10-22: it named DuckDB, Postgres
        // and Trino, one of each form)
        java.util.Set<String> forms = new java.util.TreeSet<>();
        for (DatabaseType type : Databases.relational()) {
            com.legend.sql.dialect.SqlDialect dialect = Databases.dialect(type);
            ExecutionPlan plan = plan(type, CLASSES, TypedQuery.Output.TDS_TEXT);
            com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, type + " " + CLASSES);
            ExecutionPlan.Target target = target(plan);
            String sql = ((ExecutionPlan.TextResult) plan.root()).sql().statement();
            if (dialect.inProcess()) {
                forms.add("setup");
                assertTrue(target.setup().stream().anyMatch(s -> s instanceof ExecutionPlan.SetupStep.Statement st
                        && st.sql().contains("classes")), type + ": " + target.setup());
                assertEquals(dialect.sessionSetup(), target.session(), type.name());
                assertFalse(sql.contains("metamodel_classes"), type + ": " + sql);
                if (type == Databases.PLATFORM) {
                    try (Connection c = fresh(type)) {
                        assertEquals("value:Boolean[1]\nfalse", PlanCases.run(plan, c));
                    }
                }
                continue;
            }
            assertTrue(target.setup().stream().noneMatch(s -> s instanceof ExecutionPlan.SetupStep.Statement st
                    && st.sql().contains("classes")), type + ": " + target.setup());
            if (!dialect.temporaryTables()) {
                forms.add("inline");
                assertTrue(target.session().stream().noneMatch(s -> s.contains("metamodel_classes")),
                        type + ": " + target.session());
                assertTrue(sql.contains("VALUES") && !sql.contains("metamodel_classes")
                        && !sql.contains("metamodel.classes"), type + ": " + sql);
                continue;
            }
            forms.add("session");
            List<String> session = target.session();
            // the dialect's own session statements first (a time zone), then the session's tables
            int create = -1;
            for (int i = 0; i < session.size(); i++) {
                if (session.get(i).contains("metamodel_classes")) {
                    create = i;
                    break;
                }
            }
            assertTrue(create >= 0, type + ": " + session);
            String made = session.get(create);
            assertTrue(made.toLowerCase(java.util.Locale.ROOT).contains("temp"), type + ": " + made);
            // its rows made with it (a temporary view over VALUES, Databricks') or inserted next
            if (!made.contains("VALUES")) {
                assertTrue(create + 1 < session.size(), type + ": " + session);
                String insert = session.get(create + 1);
                assertTrue(insert.toUpperCase(java.util.Locale.ROOT).startsWith("INSERT INTO")
                        && insert.contains("metamodel_classes"), type + ": " + insert.substring(0, Math.min(120,
                        insert.length())));
            }
            assertTrue(sql.contains("metamodel_classes") && !sql.contains("metamodel.classes"), type + ": " + sql);
        }
        assertEquals(java.util.Set.of("inline", "session", "setup"), forms);
    }

    static ExecutionPlan plan(DatabaseType type, String query, TypedQuery.Output output) {
        return Compiler.query(Compiler.compileModel(model(type.name())), query).executionPlan("s::RT", output);
    }

    static ExecutionPlan.Target target(ExecutionPlan plan) {
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
        String key = TestBackends.of(type).name() + "\t" + output + "\t" + query;
        String known = DIFFERENCES.get(key);
        CONSULTED.add(key);
        String what = type + " " + output + " " + withParameters;
        String[] answers;
        try {
            answers = answers(model, type, withParameters, query, values, output, what);
        } catch (RuntimeException refused) {
            if (known == null) {
                throw refused;
            }
            assertEquals(known, escaped("refused: " + refused.getMessage()), key + ": the plan's refusal moved");
            return;
        }
        if (answers == null) {
            return;    // a TDS text: judged by its values where it ran (PlanCases.assertTdsTextAnswersAsToday)
        }
        if (known == null) {
            assertEquals(answers[0], answers[1], what);
            return;
        }
        assertNotEquals(answers[0], answers[1], key + ": answers as today now; remove its row from " + DIFFERENCES_FILE);
        assertEquals(known, escaped(answers[1]), key + ": the plan's answer moved");
    }

    /** {@code s} on one register line: its backslashes, tabs, returns and new lines escaped. */
    private static String escaped(String s) {
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n");
    }

    /** Today's path's answer to {@code query} and the plan's to {@code withParameters} with {@code values}, each on a
     *  fresh database of {@code type}; a plan refused when it is made throws. A TDS text is judged here, by its values
     *  ({@code PlanCases.assertTdsTextAnswersAsToday}), and answers null. */
    private static String[] answers(String model, DatabaseType type,
            String withParameters, String query, java.util.Map<String, Object> values, TypedQuery.Output output,
            String what) throws Exception {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), withParameters).executionPlan("s::RT", output);
        try (Connection today = fresh(type); Connection planned = fresh(type)) {
            var ctx = Compiler.compileModel(model);
            var dialect = Databases.dialect(type);
            com.legend.exec.SetupRunner.run(com.legend.setup.CsvSeed.declaredSteps("s::RT", ctx, dialect), today,
                    dialect, null);
            if (output == TypedQuery.Output.TDS_TEXT) {
                PlanCases.assertTdsTextAnswersAsToday(model, query, "s::RT", today, plan, planned, values, what);
                return null;
            }
            StringWriter out = new StringWriter();
            switch (output) {
                case CSV -> Execution.executeWire(model, query, "s::RT", today, WireRender.Format.CSV, out);
                case JSON -> Execution.executeWire(model, query, "s::RT", today, WireRender.Format.JSON, out);
                case STREAMED_JSON -> Execution.executeStreaming(model, query, "s::RT", today, out);
                case TDS_TEXT -> throw new IllegalStateException("judged above");
            }
            return new String[] {out.toString(), PlanCases.run(plan, planned, values)};
        }
    }

    private static Connection fresh(DatabaseType type) throws SQLException {
        return TestBackends.of(type).open();
    }
}
