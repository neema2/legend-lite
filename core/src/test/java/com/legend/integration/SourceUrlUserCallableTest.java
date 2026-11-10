package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies {@code sourceUrl(...)} works as a user-callable relation source
 * (not just inside synthesised mapping bodies). Users can write
 * {@code sourceUrl('data:application/json,[...]')->extend(...)->...} directly.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class SourceUrlUserCallableTest {

    private static final String MIN_MODEL = """
            ###Pure
            Class store::Marker {}
            ###Relational
            Database store::Db ( Table T_X (id INTEGER PRIMARY KEY) )
            ###Connection
            RelationalDatabaseConnection store::Conn
            {
                store: store::Db;
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }
            ###Runtime
            Runtime test::RT
            {
                mappings: [];
                connections: [ store::Db: [ env: store::Conn ] ];
            }
            """;

    @Test
    void userCallable_dataUri_extendsAndProjects() throws Exception {
        try (Connection c = LaneFloor.backend().open()) {
            String q = "sourceUrl('data:application/json,[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]')"
                    + "->extend(~[a:r|cast(get($r.data, 'a'), @meta::pure::metamodel::type::Integer)])"
                    + "->select(~[a])"
                    + "->from(test::RT)";
            Lean.Result r = Lean.run(MIN_MODEL, q, "test::RT", c);
            assertEquals(2, r.rows().size(), "expected 2 unnested rows");
        }
    }

    private static final String PAYLOAD = "data:application/json,[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]";

    private static String source(String url) {
        return "sourceUrl('" + url.replace("'", "\\'") + "')->extend(~[a:r|cast(get($r.data, 'a'),"
                + " @meta::pure::metamodel::type::Integer)])->select(~[a])";
    }

    /** A data: source is a load into a session table named by its content's digest (L6b, 2026-11-10: data written in
     *  the model travels with the plan, never a SQL literal): the statement names the table and holds no payload; the
     *  target's load drops, creates (TEMPORARY: the session's own) and fills it. */
    @Test
    void aDataSourceIsALoadIntoASessionTableNamedByItsDigest() {
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(MIN_MODEL),
                source(PAYLOAD) + "->from(test::RT)").executionPlan("test::RT");
        String sql = plan.answerSql().statement();
        assertTrue(!sql.contains("\"a\":1"), "no payload in the statement: " + sql);
        String table = "legend_data_" + com.legend.cache.Hash.ofUtf8(PAYLOAD.substring(PAYLOAD.indexOf(',') + 1)).hex()
                .substring(0, 16);
        assertTrue(sql.contains(table), sql);
        var loads = plan.answerSql().target().loads();
        assertEquals(1, loads.size(), "one load: " + loads);
        var all = loads.get(0).steps();
        assertEquals(3, all.size(), String.valueOf(all));
        String drop = ((com.legend.executionplan.ExecutionPlan.LoadStep.Statement) all.get(0)).sql();
        String create = ((com.legend.executionplan.ExecutionPlan.LoadStep.Statement) all.get(1)).sql();
        assertTrue(drop.contains(table) && drop.toUpperCase(java.util.Locale.ROOT).startsWith("DROP"), drop);
        assertTrue(create.toUpperCase(java.util.Locale.ROOT).contains("TEMPORARY") && create.contains(table), create);
        // the elements the load's bundle, each bound into the table, never a literal (the checkpoint audits)
        var rows = (com.legend.executionplan.ExecutionPlan.LoadStep.File) all.get(2);
        String elements = loads.get(0).content().get(rows.ref().digest());
        assertTrue(elements.contains("{\\\"a\\\":1,\\\"b\\\":\\\"x\\\"}"), elements);
        assertEquals(com.legend.executionplan.ExecutionPlan.Load.Cadence.PER_ACQUISITION, loads.get(0).cadence());
    }

    /** Two data: sources of equal content read one table in a session, and a data: source inside a union (each arm a
     *  subquery) still reads its table: the union answers both arms' rows (L6b's tests, the orchestrator's three). */
    @Test
    void equalSourcesShareOneTableAndAUnionReadsIt() throws Exception {
        String q = source(PAYLOAD) + "->concatenate(" + source(PAYLOAD) + ")->from(test::RT)";
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(MIN_MODEL),
                q).executionPlan("test::RT");
        long creates = plan.answerSql().target().loads().get(0).steps().stream()
                .filter(com.legend.executionplan.ExecutionPlan.LoadStep.Statement.class::isInstance)
                .map(s -> ((com.legend.executionplan.ExecutionPlan.LoadStep.Statement) s).sql())
                .filter(s -> s.toUpperCase(java.util.Locale.ROOT).startsWith("CREATE")).count();
        assertEquals(1, creates, "one table for two equal sources");
        try (Connection c = LaneFloor.backend().open()) {
            Lean.Result r = Lean.run(MIN_MODEL, q, "test::RT", c);
            assertEquals(4, r.rows().size(), "both arms read the one table: " + r.rows());
        }
    }
}
