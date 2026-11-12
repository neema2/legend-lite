// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A SNAPSHOT'S COPY, AS ONE PLAN (the final audit's 1(b), 2026-11-12; {@link SnapCopy}, through the hosts' boundary,
 * {@code Boundary.snapCopy}): the pull planned in the call, the copy one writing sequence on the host's DuckDB -- the
 * schema made where one is named, the table dropped, the table made AS the pull's own statement -- every statement's
 * target carrying DuckDB's transaction; a pull reading a {@code data:} payload binds it as the pull does, the copy's
 * statement the pull's own written from its tree; run as a host runs it, the copy holds the pull's rows, and a copy
 * that fails after its drop leaves the table as it was; every refusal by name.
 */
class DuckDbSnapCopyTest {

    /** A pull over a JSON payload, model to model, on the platform's DuckDB: its plan binds the payload. */
    private static final String JSON_MODEL = """
            ###Pure
            Class m::Raw { name: String[1]; n: Integer[1]; }
            Class m::P { name: String[1]; n: Integer[1]; }
            ###Mapping
            Mapping m::MM ( *m::P: Pure { ~src m::Raw name: $src.name, n: $src.n } )
            ###Runtime
            Runtime m::RT { mappings: [m::MM]; connections: [ ModelStore: [ json: #{ JsonModelConnection {
                class: m::Raw; url: 'data:application/json,[{"name":"b","n":2},{"name":"a","n":1}]'; } }# ] ]; }
            """;

    private static final String JSON_PULL = "|m::P.all()->project(~[name: p|$p.name, n: p|$p.n])";

    /** A pull over the host's own tables (one of which the host's database does not hold). */
    private static final String TABLES = """
            ###Relational
            Database x::DB ( Table people ( name VARCHAR(10), n INTEGER ) Table missing ( id INTEGER ) )
            ###Connection
            RelationalDatabaseConnection x::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime x::RT { mappings: []; connections: [ x::DB: [ c1: x::Conn ] ]; }
            """;

    private static Map<String, Object> request(String model, String runtime, String pull, String schema, String table) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("databaseType", "DuckDB");
        r.put("model", model);
        r.put("runtime", runtime);
        r.put("lambda", com.legend.parser.SpecParser.lambdaJson(pull, false));
        if (schema != null) {
            r.put("schema", schema);
        }
        r.put("table", table);
        return r;
    }

    private static ExecutionPlan copy(Map<String, Object> request) {
        String json = planner.Boundary.snapCopy(com.legend.json.Json.toCompact(request));
        ExecutionPlan plan = PlanJson.read(json);
        assertEquals(json, PlanJson.write(plan), "the plan survives its JSON");
        return plan;
    }

    private static List<ExecutionPlan.Sql> statements(ExecutionPlan plan) {
        List<ExecutionPlan.Sql> out = new ArrayList<>();
        for (ExecutionPlan.Node step : ((ExecutionPlan.Sequence) plan.root()).steps()) {
            out.add(((ExecutionPlan.Effect) step).sql());
        }
        return out;
    }

    /** One writing sequence: the table dropped, then made AS the pull; a named schema made first. Every statement's
     *  target is the host's database, carrying DuckDB's transaction. */
    @Test
    void aCopyIsOneWritingSequenceInTheDatabasesTransaction() {
        ExecutionPlan.Transaction duck = Targets.of(new ExecutionPlan.Database.Platform(
                com.legend.executionplan.DatabaseType.DuckDB, null), new DuckDb()).transaction();
        assertTrue(duck != null && duck.begin().equals(List.of("BEGIN TRANSACTION")), String.valueOf(duck));
        ExecutionPlan plain = copy(request(TABLES, "x::RT", "|#>{x::DB.people}#", null, "copy"));
        assertTrue(plain.writes());
        List<ExecutionPlan.Sql> sqls = statements(plain);
        assertEquals(2, sqls.size());
        assertEquals("DROP TABLE IF EXISTS \"copy\"", sqls.get(0).statement());
        assertTrue(sqls.get(1).statement().startsWith("CREATE TABLE \"copy\" AS SELECT "), sqls.get(1).statement());
        ExecutionPlan named = copy(request(TABLES, "x::RT", "|#>{x::DB.people}#", "snap", "copy"));
        List<String> texts = statements(named).stream().map(ExecutionPlan.Sql::statement).toList();
        assertEquals(List.of("CREATE SCHEMA IF NOT EXISTS \"snap\"", "DROP TABLE IF EXISTS \"snap\".\"copy\""),
                texts.subList(0, 2));
        assertTrue(texts.get(2).startsWith("CREATE TABLE \"snap\".\"copy\" AS SELECT "), texts.get(2));
        for (ExecutionPlan plan : List.of(plain, named)) {
            for (ExecutionPlan.Sql sql : statements(plan)) {
                assertTrue(sql.target().database() instanceof ExecutionPlan.Database.Platform p
                        && p.type() == com.legend.executionplan.DatabaseType.DuckDB, String.valueOf(sql.target()));
                assertEquals(duck, sql.target().transaction(), "the database's transaction, on every statement");
                assertTrue(sql.target().loads().isEmpty());
            }
        }
    }

    /** A pull reading a {@code data:} payload: the copy binds it as the pull's own plan does -- its slots and its
     *  content the pull's, its statement the pull's statement, written from its tree under the copy's making. */
    @Test
    void aPayloadPullsCopyBindsWhatThePullBinds() {
        ExecutionPlan.Sql pulled = ((ExecutionPlan.TdsResult) com.legend.Compiler.query(com.legend.Compiler
                .compileModel(JSON_MODEL), JSON_PULL).executionPlan("m::RT").root()).sql();
        assertTrue(!pulled.slots().isEmpty() && pulled.slots().stream()
                .allMatch(s -> s.binding() instanceof ExecutionPlan.Binding.Payload), String.valueOf(pulled.slots()));
        ExecutionPlan.Sql copy = statements(copy(request(JSON_MODEL, "m::RT", JSON_PULL, null, "people"))).get(1);
        assertEquals(pulled.slots(), copy.slots());
        assertEquals(pulled.content(), copy.content());
        assertEquals("CREATE TABLE \"people\" AS " + pulled.statement(), copy.statement());
        assertEquals(copy.slots().size(), copy.statement().chars().filter(ch -> ch == '?').count(),
                "one ? per slot, in order");
    }

    /** Run as a host runs it -- the target's transaction around the steps, each payload bound as its slot says: the copy
     *  holds the pull's rows; a pull of the copy into a schema holds its filtered rows; and a copy whose statement fails
     *  after its drop leaves the table as it was. */
    @Test
    void runAsAHostTheCopyHoldsThePullsRows_aFailedCopyLeavesTheTableAsItWas() throws Exception {
        try (Connection c = com.legend.exec.Connectors.of(ConnectionPlans.type(
                com.legend.model.ConnectionDefinition.DatabaseType.DuckDB)).openPrivate(null)) {
            runAsAHost(c, copy(request(JSON_MODEL, "m::RT", JSON_PULL, null, "people")));
            assertEquals(List.of(List.of("a", "1"), List.of("b", "2")),
                    rows(c, "SELECT name, n FROM people ORDER BY name"));
            runAsAHost(c, copy(request(TABLES, "x::RT", "|#>{x::DB.people}#->filter(r|$r.n > 1)", "snap", "big")));
            assertEquals(List.of(List.of("b", "2")), rows(c, "SELECT name, n FROM snap.big"));
            SQLException failed = assertThrows(SQLException.class, () -> runAsAHost(c,
                    copy(request(TABLES, "x::RT", "|#>{x::DB.missing}#", null, "people"))));
            assertTrue(failed.getMessage().contains("missing"), failed.getMessage());
            assertEquals(List.of(List.of("a", "1"), List.of("b", "2")),
                    rows(c, "SELECT name, n FROM people ORDER BY name"), "the drop rolled back with the copy");
        }
    }

    /** Every refusal by name: a statement or any other key in the request; a pull that binds a declared parameter; one
     *  that is not one statement; one that answers no relation (a database whose host opens no transaction:
     *  {@code SnapCopyTransactionTest}). */
    @Test
    void eachRefusalIsByName() {
        Map<String, Object> withSql = request(TABLES, "x::RT", "|#>{x::DB.people}#", null, "t");
        withSql.put("sql", "SELECT 1");
        assertEquals("a snapshot's copy has no key sql (names and fields only, never a value)",
                assertThrows(IllegalArgumentException.class, () -> copy(withSql)).getMessage());
        Map<String, Object> withRows = request(TABLES, "x::RT", "|#>{x::DB.people}#", null, "t");
        withRows.put("rows", List.of());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> copy(withRows)).getMessage()
                .startsWith("a snapshot's copy has no key rows"));
        assertEquals("a snapshot's copy binds its pull's data: payloads alone, and 'least' binds a declared parameter",
                assertThrows(NotImplementedException.class, () -> copy(request(TABLES, "x::RT",
                        "{least: Integer[1]|#>{x::DB.people}#->filter(r|$r.n > $least)}", null, "t"))).getMessage());
        // a pivot whose columns its data names: a keyed result, its keys read first
        assertEquals("the pull copied into t is not one statement: Keyed",
                assertThrows(NotImplementedException.class, () -> copy(request(TABLES, "x::RT",
                        "|#>{x::DB.people}#->pivot(~[name], ~['total' : x | $x.n : y | $y->plus()])", null, "t")))
                        .getMessage());
        assertEquals("the pull copied into t answers no relation: its answer is VALUE",
                assertThrows(NotImplementedException.class, () -> copy(request(TABLES, "x::RT",
                        "|#>{x::DB.people}#->size()", null, "t"))).getMessage());
    }

    /** {@code plan}'s steps sent as a host's runner sends them: the target's transaction opened, each statement with
     *  its payloads bound (each slot's content, as its text), committed -- or rolled back, and the failure told. */
    private static void runAsAHost(Connection c, ExecutionPlan plan) throws SQLException {
        List<ExecutionPlan.Sql> sqls = statements(plan);
        ExecutionPlan.Transaction tx = java.util.Objects.requireNonNull(sqls.get(0).target().transaction());
        try (Statement s = c.createStatement()) {
            for (String begin : tx.begin()) {
                s.execute(begin);
            }
            try {
                for (ExecutionPlan.Sql sql : sqls) {
                    try (PreparedStatement p = c.prepareStatement(sql.statement())) {
                        for (int i = 0; i < sql.slots().size(); i++) {
                            String digest = ((ExecutionPlan.Binding.Payload) sql.slots().get(i).binding()).digest();
                            p.setString(i + 1, sql.content().get(digest));
                        }
                        p.execute();
                    }
                }
                for (String commit : tx.commit()) {
                    s.execute(commit);
                }
            } catch (SQLException e) {
                for (String rollback : tx.rollback()) {
                    s.execute(rollback);
                }
                throw e;
            }
        }
    }

    private static List<List<String>> rows(Connection c, String sql) throws SQLException {
        List<List<String>> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int n = r.getMetaData().getColumnCount();
            while (r.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    row.add(r.getString(i));
                }
                out.add(row);
            }
        }
        return out;
    }
}
