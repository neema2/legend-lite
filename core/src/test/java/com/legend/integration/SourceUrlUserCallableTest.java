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

    /** A data: source is ONE BOUND PARAMETER of its statement (the JSON pass, 2026-11-11; until then a load into a
     *  session table named by its content's digest, L6b, and its release, A4): the statement holds no payload, one slot
     *  binds it by its digest, the statement's content holds its text, and the target loads nothing. */
    @Test
    void aDataSourceIsOneBoundParameterOfItsStatement() {
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(MIN_MODEL),
                source(PAYLOAD) + "->from(test::RT)").executionPlan("test::RT");
        com.legend.executionplan.ExecutionPlan.Sql sql = plan.answerSql();
        assertTrue(!sql.statement().contains("\"a\":1") && !sql.statement().contains("legend_data_"),
                "no payload and no session table in the statement: " + sql.statement());
        String digest = com.legend.cache.Hash.ofUtf8(PAYLOAD.substring(PAYLOAD.indexOf(',') + 1)).hex();
        String text = PAYLOAD.substring(PAYLOAD.indexOf(',') + 1);
        // its label planned (the B checkpoint's tenets audit, LOW 4, 2026-11-11): what the database's refusal is told
        // under, its digest's head and its size, never written by the runner from the value it binds
        assertEquals(List.of(new com.legend.executionplan.ExecutionPlan.Binding.Payload(digest,
                "the statement reading the data: payload " + digest.substring(0, 16) + " (" + text.length()
                        + " characters)")),
                sql.slots().stream().map(com.legend.executionplan.ExecutionPlan.Slot::binding).toList());
        assertEquals(java.util.Map.of(digest, text), sql.content());
        assertEquals(List.of(), sql.target().loads(), "no load");
    }

    /** Two data: sources of equal content are one text in the plan (the JSON pass, 2026-11-11: each its statement's
     *  bound parameter, both naming the one digest, the statement's content holding it once), and a data: source inside
     *  a union (each arm a subquery) still reads its payload: the union answers both arms' rows. */
    @Test
    void equalSourcesAreOneTextAndAUnionReadsIt() throws Exception {
        String q = source(PAYLOAD) + "->concatenate(" + source(PAYLOAD) + ")->from(test::RT)";
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(MIN_MODEL),
                q).executionPlan("test::RT");
        com.legend.executionplan.ExecutionPlan.Sql sql = plan.answerSql();
        String digest = com.legend.cache.Hash.ofUtf8(PAYLOAD.substring(PAYLOAD.indexOf(',') + 1)).hex();
        assertEquals(List.of(digest, digest), sql.slots().stream().map(slot ->
                ((com.legend.executionplan.ExecutionPlan.Binding.Payload) slot.binding()).digest()).toList());
        assertEquals(1, sql.content().size(), "one text for two equal sources: " + sql.content().keySet());
        try (Connection c = LaneFloor.backend().open()) {
            Lean.Result r = Lean.run(MIN_MODEL, q, "test::RT", c);
            assertEquals(4, r.rows().size(), "both arms read the payload: " + r.rows());
        }
    }
}
