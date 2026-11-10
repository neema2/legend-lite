// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.Execution;
import com.legend.TestExecution;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.PlanSessions;
import com.legend.exec.ProductRun;
import com.legend.exec.SessionSource;
import com.legend.executionplan.DataStores;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DATA DELIVERY MATRIX (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5, step A1: written first, red before any code).
 * Every way data reaches a database, through every door, on each in-process database (DuckDB, H2, SQLite), with the
 * failure cases and a round trip of every Pure type a load carries. Each cell expects an ANSWER (the data's rows) or a
 * REFUSAL BY NAME (a cell that cannot apply: a named error of the product's or the database's -- never an internal
 * exception, never a wrong answer). The cells that fail today are pinned in {@link #RED}, the scoreboard's "matrix
 * cells red"; a cell that turns green fails this test until it is taken off the list, and a cell that turns red fails
 * it at once. The list only shrinks: each step of A ends with its cells off it.
 */
class DataDeliveryMatrixTest {

    /** The ways a request reaches the data (§5's doors). */
    enum Door {
        /** The product's read ({@code Execution.run}, on the caller's session, its declared data established). */
        READ,
        /** A product program that writes (a staged let read back). */
        WRITE,
        /** Plan generation ({@code TypedQuery.plan}, generatePlan's and arrowPlan's), and the execution plan written to
         *  its bundle, read back and run on a fresh session. */
        PLAN,
        /** A test body ({@code TestExecution.runProgram}). */
        TEST_BODY,
        /** A service's test suite ({@code ServiceTestRunner}). */
        SERVICE,
        /** The server's project road ({@code Execution.runOnProjectDatabases}). */
        SERVER
    }

    /** The data (§5's kinds). */
    enum Kind {
        CSV_TEST_DATA, SQL_TEST_DATA, DATA_SMALL, DATA_LARGE, CATALOG, FROM_SEED, CSV_FILE, DATA_SOURCE, FILE_SOURCE
    }

    /** The in-process databases of the loop (Postgres, run unprivileged, has its own target). */
    static final List<DatabaseType> DATABASES = List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite);

    /**
     * THE RED CELLS, measured 2026-11-10 on 886eadde + the design merge (A1), before any of A was built: each fails
     * today -- a crash, a wrong answer, a refusal that does not name its data, or a door that refuses what the design
     * answers -- and each is taken off by the step of A that fixes it (§6). The list only shrinks.
     */
    static final Set<String> RED = Set.of(
            // (A2 fixed SQLite's boolean -- a typed literal writes what the column takes -- and built the referee's H2
            // copy; the decoder reads SQLite's declared TIMESTAMP column as every other database's, the user's ruling of
            // 2026-11-10: the round trip green on SQLite)
            // A2 and A5: a from() naming its runtime is refused on the product's doors as a test's seed, and a test
            // body with no program runtime refuses it (the from()'s runtime is the query's own)
            "matrix:READ/FROM_SEED/DuckDB", "matrix:READ/FROM_SEED/H2", "matrix:READ/FROM_SEED/SQLite",
            "matrix:WRITE/FROM_SEED/DuckDB", "matrix:WRITE/FROM_SEED/H2", "matrix:WRITE/FROM_SEED/SQLite",
            "matrix:PLAN/FROM_SEED/DuckDB", "matrix:PLAN/FROM_SEED/H2", "matrix:PLAN/FROM_SEED/SQLite",
            "matrix:SERVER/FROM_SEED/DuckDB", "matrix:SERVER/FROM_SEED/H2", "matrix:SERVER/FROM_SEED/SQLite",
            "matrix:TEST_BODY/FROM_SEED/DuckDB", "matrix:TEST_BODY/FROM_SEED/H2", "matrix:TEST_BODY/FROM_SEED/SQLite",
            // (A3, 2026-11-10: SQLite refuses a file by name, as the design says, where it streamed it)
            // (A4, 2026-11-10: a bad payload's refusal names the data: source's payload, the plan's label on its load;
            // a bad value's names its table and column since A2: a cell its column's type cannot take is refused at
            // planning)
            // A5, one road: a data: source crashes where it is not the product read's answer (generatePlan's text,
            // a write, a test body) and fails a service; a file: source is admitted nowhere by default (the user's
            // ruling: any file) and crashes or is refused on every door but the read
            "matrix:PLAN/DATA_SOURCE/DuckDB", "matrix:WRITE/DATA_SOURCE/DuckDB", "matrix:TEST_BODY/DATA_SOURCE/DuckDB",
            "matrix:SERVICE/DATA_SOURCE/DuckDB",
            "matrix:READ/FILE_SOURCE/DuckDB", "matrix:WRITE/FILE_SOURCE/DuckDB", "matrix:PLAN/FILE_SOURCE/DuckDB",
            "matrix:TEST_BODY/FILE_SOURCE/DuckDB", "matrix:SERVICE/FILE_SOURCE/DuckDB",
            "matrix:SERVER/FILE_SOURCE/DuckDB",
            // A5: a write runs on the caller's session as it is, so a target that loads declared test data crashes
            // the door ("never skipped": an internal refusal), and a write's catalog read reads a user session
            "matrix:WRITE/CSV_TEST_DATA/DuckDB", "matrix:WRITE/CSV_TEST_DATA/H2", "matrix:WRITE/CSV_TEST_DATA/SQLite",
            "matrix:WRITE/SQL_TEST_DATA/DuckDB", "matrix:WRITE/SQL_TEST_DATA/H2", "matrix:WRITE/SQL_TEST_DATA/SQLite",
            "matrix:WRITE/CATALOG/DuckDB", "matrix:WRITE/CATALOG/H2", "matrix:WRITE/CATALOG/SQLite");

    // ---- the model ------------------------------------------------------------------------------------------------

    static final String ROWS_CSV = "ID,NAME\\n1,a\\n2,b\\n";
    static final List<List<String>> ROWS = List.of(List.of("1", "a"), List.of("2", "b"));
    static final List<List<String>> CATALOG_ROWS = List.of(List.of("P"));
    static final String RELATION = "#>{x::DB.T}#->select(~[ID, NAME])->sort(~ID->ascending())";
    static final String PAYLOAD = "data:application/json,[{\"ID\":1,\"NAME\":\"a\"},{\"ID\":2,\"NAME\":\"b\"}]";
    static final String FROM_JSON = "->extend(~[ID: r|cast(get($r.data, 'ID'), @meta::pure::metamodel::type::Integer),"
            + " NAME: r|cast(get($r.data, 'NAME'), @meta::pure::metamodel::type::String)])->select(~[ID, NAME])"
            + "->sort(~ID->ascending())->from(x::RT)";
    /** A test body's check of the rows, as upstream's {@code testLoadCsv} reads its load back: an {@code execute} of
     *  the class over the table, its values asserted. */
    static final String ASSERT_ROWS = "let res = execute(|x::P.all()->sortBy(p|$p.id), x::M, x::RT.runtimeValue,"
            + " []); assertEquals(['a', 'b'], $res.values.name);";
    static final String CATALOG = "meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'P')"
            + "->project(~[name: c|$c.name])";

    /** A cell's data as a model, a query and the rows it answers. */
    record Data(String model, String query, List<List<String>> rows, @com.legend.base.Nullable String runtime) {
    }

    /** The environment a cell runs in: its files' directory. */
    record Env(Path dir) {
        Path csv() {
            return dir.resolve("people.csv");
        }

        Path json() {
            return dir.resolve("people.json");
        }
    }

    static String connection(DatabaseType db, String spec) {
        return "RelationalDatabaseConnection x::Conn { store: x::DB; type: " + db + "; " + spec + " }\n";
    }

    static String localH2(String field) {
        return "specification: LocalH2 { " + field + "; }; auth: DefaultH2;";
    }

    static String model(String connection, String extra) {
        return """
                ###Pure
                Class x::P { id: Integer[1]; name: String[1]; }
                ###Relational
                Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
                ###Mapping
                Mapping x::M ( *x::P: Relational { ~mainTable [x::DB] T  id: [x::DB] T.ID, name: [x::DB] T.NAME } )
                ###Connection
                """ + connection + """
                ###Runtime
                Runtime x::RT { mappings: [ x::M ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """ + extra;
    }

    static String bareConnection(DatabaseType db) {
        return "RelationalDatabaseConnection x::Conn { store: x::DB; " + TestBackends.of(db).connectionBody() + " }\n";
    }

    /** {@code kind}'s data on {@code db}. */
    static Data data(Kind kind, DatabaseType db, Env env) throws java.io.IOException {
        return switch (kind) {
            case CSV_TEST_DATA -> new Data(model(connection(db, localH2("testDataSetupCSV: 'default\\nT\\n"
                    + ROWS_CSV + "'")), ""), "|" + RELATION, ROWS, "x::RT");
            case SQL_TEST_DATA -> new Data(model(connection(db, localH2("testDataSetupSqls: ['CREATE TABLE T (ID"
                    + " INTEGER PRIMARY KEY, NAME VARCHAR(20))', 'INSERT INTO T VALUES (1, \\'a\\'), (2, \\'b\\')']")),
                    ""), "|" + RELATION, ROWS, "x::RT");
            case DATA_SMALL -> new Data(model(bareConnection(db), dataElement(ROWS_CSV)), "|" + RELATION, ROWS,
                    "x::RT");
            case DATA_LARGE -> {
                StringBuilder rows = new StringBuilder("ID,NAME\\n1,a\\n2,b\\n");
                for (int i = 3; i <= 2000; i++) {
                    rows.append(i).append(",name number ").append(i).append("\\n");
                }
                yield new Data(model(bareConnection(db), dataElement(rows.toString())),
                        "|#>{x::DB.T}#->filter(r|$r.ID <= 2)->select(~[ID, NAME])->sort(~ID->ascending())", ROWS,
                        "x::RT");
            }
            case CATALOG -> new Data(model(bareConnection(db), ""), "|" + CATALOG, CATALOG_ROWS, "x::RT");
            case FROM_SEED -> new Data(model(connection(db, localH2("testDataSetupCSV: 'default\\nT\\n" + ROWS_CSV
                    + "'")), ""), "|x::P.all()->project(~[ID: p|$p.id, NAME: p|$p.name])->sort(~ID->ascending())"
                    + "->from(x::M, x::RT)", ROWS, null);
            case CSV_FILE -> {
                Files.writeString(env.csv(), "ID,NAME\n1,a\n2,b\n", StandardCharsets.UTF_8);
                yield new Data(model(bareConnection(db), ""),
                        "{|let connection = x::RT.runtimeValue.connectionStores.connection->toOne()"
                                + "->cast(@meta::external::store::relational::runtime::DatabaseConnection);"
                                + " meta::relational::functions::toDDL::dropAndCreateTableInDb(x::DB, 'T', $connection);"
                                + " meta::relational::metamodel::execute::loadCsvToDbTable('people.csv',"
                                + " x::DB->meta::relational::metamodel::schema('default')->toOne()->meta::relational::metamodel::table('T')->toOne(), $connection); "
                                + ASSERT_ROWS + "}", ROWS, "x::RT");
            }
            case DATA_SOURCE -> new Data(model(bareConnection(db), ""), "|sourceUrl('" + PAYLOAD + "')" + FROM_JSON,
                    ROWS, "x::RT");
            case FILE_SOURCE -> {
                Files.writeString(env.json(), "{\"ID\":1,\"NAME\":\"a\"}\n{\"ID\":2,\"NAME\":\"b\"}\n",
                        StandardCharsets.UTF_8);
                yield new Data(model(bareConnection(db), ""), "|sourceUrl('" + env.json().toUri() + "')" + FROM_JSON,
                        ROWS, "x::RT");
            }
        };
    }

    static String dataElement(String csv) {
        return "###Data\nData x::D\n{\n  Relational\n  #{\n    default.T:\n      '" + csv + "';\n  }#\n}\n";
    }

    // ---- what a cell should do ------------------------------------------------------------------------------------

    /** What a cell should do: answer its rows, or refuse by name. */
    sealed interface Expected permits Expected.Answer, Expected.Refusal, Expected.AnswerOrRefusal {
        record Answer() implements Expected {
        }

        /** A cell that cannot apply: a named error, never an internal one; where the design says what it names
         *  ({@code names}: a file's name), naming it. */
        record Refusal(String why, @com.legend.base.Nullable String names) implements Expected {
            Refusal(String why) {
                this(why, null);
            }
        }

        /** A cell whose database may lack the feature: its rows, or a named refusal. */
        record AnswerOrRefusal(String why) implements Expected {
        }
    }

    /** The design's expectation for {@code cell} (§1, §4). */
    static Expected expected(Door door, Kind kind, DatabaseType db) {
        boolean testDoor = door == Door.TEST_BODY;
        switch (kind) {
            case DATA_SMALL, DATA_LARGE -> {
                // a project's Data fills the server's project database, and a service suite names it by reference;
                // every other door reads a table nothing made
                return door == Door.SERVER || door == Door.SERVICE ? new Expected.Answer()
                        : new Expected.Refusal("a project's Data is the server's and a suite's, not this door's");
            }
            case CSV_FILE -> {
                if (!testDoor) {
                    return new Expected.Refusal("loadCsvToDbTable is a test body's effect");
                }
                // a file is a pointer the database reads: DuckDB's read_csv, H2's CSVREAD; any other database refuses
                return db == DatabaseType.DuckDB || db == DatabaseType.H2 ? new Expected.Answer()
                        : new Expected.Refusal(db + " reads no file", "people.csv");
            }
            case FILE_SOURCE -> {
                // DuckDB reads a JSON file; a database with no reader of it refuses the file by name (§1.3)
                return db == DatabaseType.DuckDB ? new Expected.Answer()
                        : new Expected.Refusal("no JSON file reader", "people.json");
            }
            case DATA_SOURCE -> {
                return db == DatabaseType.DuckDB ? new Expected.Answer()
                        : new Expected.AnswerOrRefusal("a JSON payload's values on a database whose JSON is not modeled");
            }
            case CSV_TEST_DATA, SQL_TEST_DATA -> {
                // test data rides a LocalH2 specification (upstream's one carrier of it); the server opens the
                // connection its runtime declares, and a LocalH2 specification on another database is no connection it
                // can open: refused by name. Every other door reads it on the session it is given.
                return door == Door.SERVER && db != DatabaseType.H2
                        ? new Expected.Refusal("a LocalH2 specification on " + db + " is no connection the server opens")
                        : new Expected.Answer();
            }
            default -> {
                return new Expected.Answer();
            }
        }
    }

    // ---- what a cell did ------------------------------------------------------------------------------------------

    /** What a cell did. */
    sealed interface Outcome permits Outcome.Rows, Outcome.Refused, Outcome.Crashed {
        record Rows(List<List<String>> rows) implements Outcome {
        }

        record Refused(String type, String message) implements Outcome {
        }

        record Crashed(String type, String message) implements Outcome {
        }
    }

    /** The internal exception types: a crash, never a refusal by name. */
    static final Set<Class<?>> INTERNAL = Set.of(IllegalStateException.class, IllegalArgumentException.class,
            NullPointerException.class, ClassCastException.class, UnsupportedOperationException.class,
            IndexOutOfBoundsException.class, ArrayIndexOutOfBoundsException.class, AssertionError.class,
            java.util.NoSuchElementException.class, StackOverflowError.class, java.io.UncheckedIOException.class);

    static Outcome outcomeOf(Throwable t) {
        Throwable root = t;
        String message = String.valueOf(t.getMessage());
        boolean internal = INTERNAL.contains(t.getClass());
        if (internal) {
            return new Outcome.Crashed(t.getClass().getSimpleName(), message);
        }
        return new Outcome.Refused(root.getClass().getSimpleName(), message);
    }

    static boolean passes(Expected expected, Outcome outcome, List<List<String>> rows) {
        return switch (expected) {
            case Expected.Answer a -> outcome instanceof Outcome.Rows r && r.rows().equals(rows);
            case Expected.Refusal r -> outcome instanceof Outcome.Refused f && (r.names() == null
                    || f.message().contains(r.names()));
            case Expected.AnswerOrRefusal ar -> outcome instanceof Outcome.Refused
                    || outcome instanceof Outcome.Rows r && r.rows().equals(rows);
        };
    }

    // ---- the doors ------------------------------------------------------------------------------------------------

    static List<List<String>> strings(Collector collector) {
        List<List<String>> out = new ArrayList<>();
        for (List<Object> row : collector.collected().rows()) {
            List<String> cells = new ArrayList<>();
            for (Object cell : row) {
                cells.add(String.valueOf(cell));
            }
            out.add(cells);
        }
        return out;
    }

    static Outcome run(Door door, Data data, DatabaseType db, Env env) {
        try {
            return new Outcome.Rows(switch (door) {
                case READ -> read(data, data.query(), db);
                case WRITE -> read(data, "{|let s = " + body(data.query()) + "->meta::legend::lite::stage(); $s;}", db);
                case PLAN -> plan(data, db);
                case TEST_BODY -> testBody(data, db, env);
                case SERVICE -> service(data, db);
                case SERVER -> server(data);
            });
        } catch (Throwable t) {
            return outcomeOf(t);
        }
    }

    /**
     * A fresh session on {@code db}, a cell's own: an in-process database's private instance, a server's new workspace
     * database (dropped when the session closes), so no cell reads another's tables.
     */
    static Connection session(DatabaseType db) throws java.sql.SQLException {
        TestBackend backend = TestBackends.of(db);
        return backend.inProcess() ? backend.open() : backend.openWorkspace();
    }

    /**
     * The doors a server database's leg does not run, and why: the server's project road opens the connection the
     * model DECLARES, and a declared test connection to the embedded server authenticates as the operating system's
     * user, for whom that server has no role (measured 2026-11-10, A1: "role ubuntu does not exist" under the
     * unprivileged run). A test-harness limit, not a product refusal: the in-process leg runs the door.
     */
    static boolean runs(Door door, DatabaseType db) {
        return door != Door.SERVER || TestBackends.of(db).inProcess();
    }

    /** A query's text without its lambda bar. */
    static String body(String query) {
        return query.startsWith("|") ? query.substring(1) : query;
    }

    static List<List<String>> read(Data data, String query, DatabaseType db) throws Exception {
        try (Connection c = session(db)) {
            Collector collector = new Collector();
            Execution.run(Compiler.query(Compiler.compileModel(data.model()), query), data.runtime(),
                    SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                    ProductRun.Establish.DECLARED_SETUP, new Output.Rows(collector));
            return strings(collector);
        }
    }

    static List<List<String>> plan(Data data, DatabaseType db) throws Exception {
        TypedQuery typed = Compiler.query(Compiler.compileModel(data.model()), data.query());
        // generatePlan's and arrowPlan's text plan: it renders, or refuses by name what it cannot carry
        try {
            typed.plan(data.runtime());
        } catch (com.legend.error.LegendCompileException | com.legend.error.NotImplementedException
                | com.legend.sql.DialectCapability refused) {
            // a door that cannot carry the data refuses it by name (§4): the execution plan below must still answer
        }
        // the execution plan as it travels: its bundle written, read back, and run on a fresh session
        PlanJson.Bundle bundle = PlanJson.writeBundle(typed.executionPlan(data.runtime()));
        ExecutionPlan back = PlanJson.read(bundle.json(), bundle.blobs());
        try (Connection c = session(db)) {
            Collector collector = new Collector();
            ProductRun.run(back, SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                    ProductRun.Establish.DECLARED_SETUP, new Output.Rows(collector), 1024);
            return strings(collector);
        }
    }

    static List<List<String>> testBody(Data data, DatabaseType db, Env env) throws Exception {
        ModelContext ctx = Compiler.compileModel(data.model());
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(data.query());
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        com.legend.protocol.spec.ValueSpecification resolved = Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx);
        try (Connection c = session(db)) {
            return testBodyOn(c, data, env.dir());
        }
    }

    static List<List<String>> server(Data data) throws Exception {
        Collector collector = new Collector();
        Execution.runOnProjectDatabases(Compiler.query(Compiler.compileModel(data.model()), data.query()),
                data.runtime(), new Output.Rows(collector));
        return strings(collector);
    }

    /**
     * A service whose query is the cell's, its suite's one test expecting the cell's rows as JSON objects: the suite's
     * data the cell's (a CSV's rows as the connection's, a project's Data by reference), else none.
     */
    static List<List<String>> service(Data data, DatabaseType db) throws Exception {
        String query = data.query().startsWith("{") ? data.query() : data.query();
        StringBuilder expected = new StringBuilder("[");
        for (int i = 0; i < data.rows().size(); i++) {
            List<String> row = data.rows().get(i);
            expected.append(i == 0 ? "" : ",").append(row.size() == 1 ? "{\\\"name\\\":\\\"" + row.get(0) + "\\\"}"
                    : "{\\\"ID\\\":" + row.get(0) + ",\\\"NAME\\\":\\\"" + row.get(1) + "\\\"}");
        }
        expected.append("]");
        String suiteData = data.model().contains("###Data") ? """
                      data:
                      [
                        connections:
                        [
                          c:
                            Reference
                            #{
                              x::D
                            }#
                        ]
                      ]
                """ : "";
        String model = data.model() + """
                ###Service
                Service x::S
                {
                  pattern: '/s';
                  documentation: '';
                  execution: Single
                  {
                    query: %s;
                    mapping: x::M;
                    runtime: x::RT;
                  }
                  testSuites:
                  [
                    suite:
                    {
                %s
                      tests:
                      [
                        t:
                        {
                          serializationFormat: PURE_TDSOBJECT;
                          asserts:
                          [
                            rows:
                              EqualToJson
                              #{
                                expected:
                                  ExternalFormat
                                  #{
                                    contentType: 'application/json';
                                    data: '%s';
                                  }#;
                              }#
                          ]
                        }
                      ]
                    }
                  ]
                }
                """.formatted(query, suiteData, expected);
        ModelContext ctx = Compiler.compileModel(model);
        com.legend.model.ServiceDefinition svc = com.legend.testing.Own.model(model).elements().stream()
                .filter(e -> e instanceof com.legend.model.ServiceDefinition s && s.qualifiedName().equals("x::S"))
                .map(e -> (com.legend.model.ServiceDefinition) e).findFirst().orElseThrow();
        try (com.legend.test.ServiceTestRunner runner = new com.legend.test.ServiceTestRunner(ctx,
                () -> {
                    try {
                        return TestSessions.owning(session(db));
                    } catch (java.sql.SQLException e) {
                        throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
                    }
                }, com.legend.test.ServiceTestRunner.Sessions.FRESH_PER_TEST, db)) {
            List<com.legend.test.ServiceTestRunner.Result> results = runner.run(svc);
            if (results.size() != 1) {
                throw new AssertionError("one test result, not " + results);
            }
            var r = results.get(0);
            if (r.status() != com.legend.test.ServiceTestRunner.Status.PASS) {
                // a suite's failure as the reason it gives: a refusal where it names one, else a wrong answer
                throw new ServiceFailed(String.valueOf(r.reason()));
            }
            return data.rows();
        }
    }

    /** A service test that did not pass, its reason the runner's. */
    static final class ServiceFailed extends Exception {
        ServiceFailed(String reason) {
            super(reason);
        }
    }

    // ---- the matrix -----------------------------------------------------------------------------------------------

    @Test
    void theMatrix(@TempDir Path dir) throws Exception {
        matrix(DATABASES, dir, RED);
    }

    /** Every cell on each of {@code databases}, judged against {@code pinned} (a lane's own red list). */
    static void matrix(List<DatabaseType> databases, Path dir, Set<String> pinned) throws Exception {
        Map<String, String> red = new LinkedHashMap<>();
        Map<String, String> green = new LinkedHashMap<>();
        for (Door door : Door.values()) {
            for (Kind kind : Kind.values()) {
                for (DatabaseType db : databases) {
                    if (!runs(door, db)) {
                        continue;
                    }
                    Path cellDir = Files.createDirectories(dir.resolve(door + "-" + kind + "-" + db));
                    Env env = new Env(cellDir);
                    String id = door + "/" + kind + "/" + db;
                    Data data = data(kind, db, env);
                    Outcome outcome = run(door, data, db, env);
                    Expected expected = expected(door, kind, db);
                    String line = expected.getClass().getSimpleName() + " <- " + describe(outcome);
                    if (passes(expected, outcome, data.rows())) {
                        green.put(id, line);
                    } else {
                        red.put(id, line);
                    }
                }
            }
        }
        report("matrix", red, green, pinned);
    }

    // ---- the failure cases ---------------------------------------------------------------------------------------

    /** The failure cases' store: a table named so a refusal can be read for naming it (§3 rule 5). */
    static String peopleModel(DatabaseType db, String spec) {
        return """
                ###Pure
                Class x::P { id: Integer[1]; name: String[1]; }
                ###Relational
                Database x::DB ( Table PEOPLE (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
                ###Mapping
                Mapping x::M ( *x::P: Relational { ~mainTable [x::DB] PEOPLE  id: [x::DB] PEOPLE.ID, name: [x::DB] PEOPLE.NAME } )
                ###Connection
                RelationalDatabaseConnection x::Conn { store: x::DB; type: %s; %s }
                ###Runtime
                Runtime x::RT { mappings: [ x::M ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """.formatted(db, spec);
    }

    static final String PEOPLE = "|#>{x::DB.PEOPLE}#->select(~[ID, NAME])->sort(~ID->ascending())";

    static String peopleCsv(String rows) {
        return localH2("testDataSetupCSV: 'default\\nPEOPLE\\nID,NAME\\n" + rows + "'");
    }

    /** A failure case's verdict: green or why it is red. */
    static String verdict(boolean pass, String why) {
        return (pass ? "ok " : "FAILS ") + why;
    }

    /** A refusal that names {@code what} (§3 rule 5), not an internal exception. */
    static boolean refusesNaming(Outcome o, String what) {
        return o instanceof Outcome.Refused r && r.message().toLowerCase(java.util.Locale.ROOT)
                .contains(what.toLowerCase(java.util.Locale.ROOT));
    }

    static Outcome attempt(java.util.concurrent.Callable<List<List<String>>> run) {
        try {
            return new Outcome.Rows(run.call());
        } catch (Throwable t) {
            return outcomeOf(t);
        }
    }

    static List<List<String>> readOn(Connection c, String model, String query) throws Exception {
        Collector collector = new Collector();
        Execution.run(Compiler.query(Compiler.compileModel(model), query), "x::RT",
                SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                ProductRun.Establish.DECLARED_SETUP, new Output.Rows(collector));
        return strings(collector);
    }

    static List<List<String>> body(Connection c, String model, String body, Path dir) throws Exception {
        Data data = new Data(model, body, List.of(), "x::RT");
        return testBodyOn(c, data, dir);
    }

    static List<List<String>> testBodyOn(Connection c, Data data, Path dir) throws Exception {
        ModelContext ctx = Compiler.compileModel(data.model());
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(data.query());
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        com.legend.protocol.spec.ValueSpecification resolved = Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx);
        ExecuteOptions options = new ExecuteOptions(null, DataStores.resources(dir), Set.of());
        Collector collector = new Collector();
        List<String> failed = new ArrayList<>();
        int[] verdicts = {0};
        com.legend.testexecutionplan.AssertListener listener = new com.legend.testexecutionplan.AssertListener() {
            @Override
            public void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
                verdicts[0]++;
                if (!pass) {
                    failed.add(assertName + ": " + detail);
                }
            }
        };
        TestExecution.runProgram(Compiler.queryResolved(ctx, resolved), data.runtime(),
                SessionSource.given(TestSessions.over(c)), listener, null, options, new Output.Rows(collector));
        if (verdicts[0] > 0) {
            // a body that asserts is judged by its verdicts: its rows are the data's where every assert passes
            if (!failed.isEmpty()) {
                throw new AssertionError("asserts failed: " + failed);
            }
            return data.rows();
        }
        return strings(collector);
    }

    static final String LOAD_PEOPLE = "{|let connection = x::RT.runtimeValue.connectionStores.connection->toOne()"
            + "->cast(@meta::external::store::relational::runtime::DatabaseConnection);"
            + " meta::relational::functions::toDDL::dropAndCreateTableInDb(x::DB, 'PEOPLE', $connection);"
            + " meta::relational::metamodel::execute::loadCsvToDbTable('people.csv', x::DB->meta::relational::metamodel"
            + "::schema('default')->toOne()->meta::relational::metamodel::table('PEOPLE')->toOne(), $connection); "
            + "let res = execute(|x::P.all()->sortBy(p|$p.id), x::M, x::RT.runtimeValue,"
            + " []); assertEquals(['a', 'b'], $res.values.name);}";

    @Test
    void theFailureCases(@TempDir Path dir) throws Exception {
        failures(DATABASES, dir, RED);
    }

    /** The failure cases on each of {@code databases} (the server's kept database's on DuckDB, where it is one). */
    static void failures(List<DatabaseType> databases, Path dir, Set<String> pinned) throws Exception {
        Map<String, String> red = new LinkedHashMap<>();
        Map<String, String> green = new LinkedHashMap<>();
        for (DatabaseType db : databases) {
            // 1. A BAD VALUE: refused naming the table, and the session takes the next load cleanly (§3 rules 1, 2, 5)
            try (Connection c = session(db)) {
                Outcome bad = attempt(() -> readOn(c, peopleModel(db, peopleCsv("abc,x\\n")), PEOPLE));
                boolean named = refusesNaming(bad, "PEOPLE");
                Outcome next = attempt(() -> readOn(c, peopleModel(db, peopleCsv("1,a\\n2,b\\n")), PEOPLE));
                put(named ? green : red, "BAD_VALUE/" + db, verdict(named, describe(bad)));
                boolean clean = next instanceof Outcome.Rows r && r.rows().equals(ROWS);
                put(clean ? green : red, "FAIL_THEN_CLEAN/" + db, verdict(clean, describe(next)));
            }
            // 2. A MALFORMED RECORD in a CSV file: refused naming the file (§3 rule 5); a database that reads no file
            // refuses the file by name all the same. The record is WIDE, which every reader refuses (A3, 2026-11-10:
            // until then an unclosed quote, which H2's CSVREAD reads as the rest of the file, a ruled divergence, S186;
            // each reader's edges, that one included, are pinned by DataLoadsTest.eachReadersEdgesArePinned)
            Path malformed = Files.createDirectories(dir.resolve("malformed-" + db));
            Files.writeString(malformed.resolve("people.csv"), "ID,NAME\n1,a,extra\n", StandardCharsets.UTF_8);
            try (Connection c = session(db)) {
                Outcome o = attempt(() -> body(c, peopleModel(db, TestBackends.of(db).specificationAndAuth() + ";"), LOAD_PEOPLE, malformed));
                boolean named = refusesNaming(o, "people.csv");
                put(named ? green : red, "MALFORMED_RECORD/" + db, verdict(named, describe(o)));
            }
            // 3. A MISSING FILE: refused naming it
            Path empty = Files.createDirectories(dir.resolve("missing-" + db));
            try (Connection c = session(db)) {
                Outcome o = attempt(() -> body(c, peopleModel(db, TestBackends.of(db).specificationAndAuth() + ";"), LOAD_PEOPLE, empty));
                boolean named = refusesNaming(o, "people.csv");
                put(named ? green : red, "MISSING_FILE/" + db, verdict(named, describe(o)));
            }
            // 4. A FILE CHANGED between planning and the run: the plan names its bytes, so the run refuses other
            // bytes by name; a database that reads no file refuses it at planning
            Path changing = Files.createDirectories(dir.resolve("changed-" + db));
            Files.writeString(changing.resolve("people.csv"), "ID,NAME\n1,a\n2,b\n", StandardCharsets.UTF_8);
            Outcome changed = attempt(() -> {
                String model = peopleModel(db, TestBackends.of(db).specificationAndAuth() + ";");
                ModelContext ctx = Compiler.compileModel(model);
                var parsed = Compiler.parseQuery(LOAD_PEOPLE);
                var statements = ((com.legend.protocol.spec.LambdaFunction) parsed).body();
                var resolved = Compiler.resolveQuery(statements, new com.legend.model.ImportScope(List.of()), ctx);
                ExecuteOptions options = new ExecuteOptions(null, DataStores.resources(changing), Set.of());
                var plan = TestExecution.planBody(Compiler.queryResolved(ctx, resolved), "x::RT", options);
                Files.writeString(changing.resolve("people.csv"), "ID,NAME\n1,z\n", StandardCharsets.UTF_8);
                try (Connection c = session(db)) {
                    Collector collector = new Collector();
                    TestExecution.runBody(plan, TestSessions.over(c), null, null, options, new Output.Rows(collector));
                    return strings(collector);
                }
            });
            boolean refusedChange = refusesNaming(changed, "people.csv");
            put(refusedChange ? green : red, "CHANGED_FILE/" + db, verdict(refusedChange, describe(changed)));
        }
        if (!databases.contains(DatabaseType.DuckDB)) {
            report("failure", red, green, pinned);
            return;
        }
        // 5. A FAILED PAYLOAD on the server's kept project database, then a clean one (§3 rules 2, 4): DuckDB reads JSON
        String model = model(bareConnection(DatabaseType.DuckDB), "");
        Outcome badPayload = attempt(() -> server(new Data(model, "|sourceUrl('data:application/json,[{\"ID\":1,"
                + "\"NAME\":\"a\"},{not json}]')" + FROM_JSON, ROWS, "x::RT")));
        boolean namedPayload = refusesNaming(badPayload, "data:");
        put(namedPayload ? green : red, "BAD_PAYLOAD/DuckDB", verdict(namedPayload, describe(badPayload)));
        Outcome goodPayload = attempt(() -> server(new Data(model, "|sourceUrl('" + PAYLOAD + "')" + FROM_JSON, ROWS,
                "x::RT")));
        boolean cleanAfter = goodPayload instanceof Outcome.Rows r && r.rows().equals(ROWS);
        put(cleanAfter ? green : red, "PAYLOAD_THEN_CLEAN/DuckDB", verdict(cleanAfter, describe(goodPayload)));
        // 6. TWO CONCURRENT REQUESTS on the server's kept project database, each its own payload's rows (§3 rule 4)
        String other = "data:application/json,[{\"ID\":1,\"NAME\":\"x\"},{\"ID\":2,\"NAME\":\"y\"}]";
        List<List<String>> otherRows = List.of(List.of("1", "x"), List.of("2", "y"));
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        List<java.util.concurrent.Future<Outcome>> futures = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
            String payload = i % 2 == 0 ? PAYLOAD : other;
            futures.add(pool.submit(() -> attempt(() -> server(new Data(model, "|sourceUrl('" + payload + "')"
                    + FROM_JSON, ROWS, "x::RT")))));
        }
        int wrong = 0;
        String firstWrong = "";
        for (int i = 0; i < futures.size(); i++) {
            Outcome o = futures.get(i).get();
            List<List<String>> want = i % 2 == 0 ? ROWS : otherRows;
            if (!(o instanceof Outcome.Rows r && r.rows().equals(want))) {
                if (wrong++ == 0) {
                    firstWrong = describe(o);
                }
            }
        }
        pool.shutdown();
        put(wrong == 0 ? green : red, "CONCURRENT/DuckDB", verdict(wrong == 0, wrong + " of " + CONCURRENT_REQUESTS
                + " wrong; " + firstWrong));
        report("failure", red, green, pinned);
    }

    /** Requests the concurrency case sends, four at a time. */
    static final int CONCURRENT_REQUESTS = 80;

    /**
     * RACES: cells whose failure is a race, red on most runs and not every one, so their outcome is reported and not
     * compared (a pin either way would be flaky). Measured 2026-11-10 (A1): two requests on the server's kept project
     * database drop and create one payload's {@code legend_data_*} table at once ("already exists"; the correctness
     * audit's S8). A4 (§3 rule 4: a kept session serves one request at a time, and a payload's table lives and dies with
     * its request) fixed it, 2026-11-10, and the cell is compared strictly since. The list only shrinks.
     */
    static final Set<String> RACES = Set.of();

    static void put(Map<String, String> into, String id, String line) {
        into.put(id, line);
    }

    // ---- the type round trip ----------------------------------------------------------------------------------------

    /** Every Pure type a load carries, one column each (§5: "every Pure type through a load"). */
    static final String TYPES_MODEL = """
            ###Relational
            Database x::DB ( Table TYPES (S VARCHAR(20), I INTEGER, B BIGINT, F DOUBLE, D DECIMAL(10,2), BO BIT, DT DATE,
                TS TIMESTAMP) )
            ###Connection
            RelationalDatabaseConnection x::Conn { store: x::DB; type: %s; %s }
            ###Runtime
            Runtime x::RT { mappings: [ ]; connections: [ x::DB: [ c: x::Conn ] ]; }
            """;

    static final String TYPES_CSV = "S,I,B,F,D,BO,DT,TS\\nx,1,12345678901,1.5,2.25,true,2020-01-02,2020-01-02 03:04:05\\n";
    static final String TYPES_QUERY = "|#>{x::DB.TYPES}#->select(~[S, I, B, F, D, BO, DT, TS])";
    static final List<String> TYPES_ROW = List.of("x", "1", "12345678901", "1.5", "2.25", "true", "2020-01-02",
            "2020-01-02T03:04:05.000000000+0000");

    @Test
    void theTypeRoundTrip() throws Exception {
        roundTrip(DATABASES, RED, true);
    }

    /** Every Pure type through a load on each of {@code databases}, and (where {@code referee}) on the referee's H2. */
    static void roundTrip(List<DatabaseType> databases, Set<String> pinned, boolean referee) throws Exception {
        Map<String, String> red = new LinkedHashMap<>();
        Map<String, String> green = new LinkedHashMap<>();
        for (DatabaseType db : databases) {
            String testData = TYPES_MODEL.formatted(db, localH2("testDataSetupCSV: 'default\\nTYPES\\n" + TYPES_CSV
                    + "'"));
            // through a connection's test data, on the product's read
            Outcome read = attempt(() -> {
                try (Connection c = session(db)) {
                    return readOn(c, testData, TYPES_QUERY);
                }
            });
            check(red, green, "TEST_DATA/" + db, read);
            // through a project's Data, on the server's project road
            String projectData = TYPES_MODEL.formatted(db, TestBackends.of(db).specificationAndAuth() + ";")
                    + "###Data\nData x::D\n{\n  Relational\n  #{\n    default.TYPES:\n      '" + TYPES_CSV
                    + "';\n  }#\n}\n";
            if (runs(Door.SERVER, db)) {
                Outcome served = attempt(() -> server(new Data(projectData, TYPES_QUERY, List.of(), "x::RT")));
                check(red, green, "PROJECT_DATA/" + db, served);
            }
        }
        if (referee) {
            // the referee's own H2 copy of a load's rows (§1.1, A2): a DuckDB session's test data planned, its copy --
            // the platform's DDL and the rows' inserts written in the referee's dialect -- replayed on an H2 database
            // of its own, and the table read back there as the product reads it
            Outcome copy = attempt(() -> {
                ModelContext ctx = Compiler.compileModel(TYPES_MODEL.formatted(DatabaseType.DuckDB, localH2(
                        "testDataSetupCSV: 'default\\nTYPES\\n" + TYPES_CSV + "'")));
                com.legend.setup.CsvSeed.Planned planned = com.legend.setup.CsvSeed.planned(
                        com.legend.setup.CsvSeed.declaredSteps("x::RT", ctx, com.legend.database.Databases.dialect(
                                DatabaseType.DuckDB)), com.legend.database.Databases.dialect(DatabaseType.DuckDB),
                        com.legend.database.Databases.dialect(com.legend.database.Databases.REPLAY_REFEREE));
                try (Connection c = session(DatabaseType.H2)) {
                    try (java.sql.Statement st = c.createStatement()) {
                        for (String statement : planned.refereeCopy()) {
                            st.execute(statement);
                        }
                    }
                    return readOn(c, TYPES_MODEL.formatted(DatabaseType.H2, TestBackends.of(DatabaseType.H2)
                            .specificationAndAuth() + ";"), TYPES_QUERY);
                }
            });
            check(red, green, "REFEREE/H2", copy);
        }
        report("roundtrip", red, green, pinned);
    }

    static void check(Map<String, String> red, Map<String, String> green, String id, Outcome o) {
        boolean pass = o instanceof Outcome.Rows r && r.rows().equals(List.of(TYPES_ROW));
        put(pass ? green : red, id, verdict(pass, describe(o)));
    }

    static String describe(Outcome o) {
        String s = switch (o) {
            case Outcome.Rows r -> "rows " + r.rows();
            case Outcome.Refused r -> "refused " + r.type() + ": " + r.message();
            case Outcome.Crashed c -> "CRASHED " + c.type() + ": " + c.message();
        };
        s = s.replace('\n', ' ');
        return s.length() > 300 ? s.substring(0, 300) + "..." : s;
    }

    static void report(String what, Map<String, String> red, Map<String, String> green, Set<String> pinnedRed) {
        System.out.println("=== " + what + ": " + red.size() + " red, " + green.size() + " green");
        red.forEach((id, line) -> System.out.println("RED   " + id + "  " + line));
        green.forEach((id, line) -> System.out.println("GREEN " + id + "  " + line));
        Set<String> nowRed = new TreeSet<>(red.keySet());
        for (String race : RACES) {
            if (race.startsWith(what + ":")) {
                String id = race.substring(what.length() + 1);
                System.out.println("RACE  " + id + " " + (nowRed.remove(id) ? "red this run" : "green this run")
                        + " (not compared until A4)");
                green.remove(id);
            }
        }
        Set<String> pinned = new TreeSet<>();
        for (String id : pinnedRed) {
            if (id.startsWith(what + ":")) {
                pinned.add(id.substring(what.length() + 1));
            }
        }
        Set<String> newlyRed = new TreeSet<>(nowRed);
        newlyRed.removeAll(pinned);
        Set<String> newlyGreen = new TreeSet<>(pinned);
        newlyGreen.removeAll(nowRed);
        assertTrue(newlyRed.isEmpty() && newlyGreen.isEmpty(), what + ": cells red that are not pinned " + newlyRed
                + "; cells pinned red that pass (take them off RED) " + newlyGreen);
    }
}
