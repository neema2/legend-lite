package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies {@code sourceUrl(...)} works as a user-callable relation source
 * (not just inside synthesised mapping bodies). Users can write
 * {@code sourceUrl('data:application/json,[...]')->extend(...)->...} directly.
 */
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
                type: H2;
                specification: LocalH2 {};
                auth: DefaultH2 {};
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
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            String q = "sourceUrl('data:application/json,[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]')"
                    + "->extend(~[a:r|cast(get($r.data, 'a'), @meta::pure::metamodel::type::Integer)])"
                    + "->select(~[a])"
                    + "->from(test::RT)";
            ExecutionResult r = new QueryService().execute(MIN_MODEL, q, "test::RT", c);
            assertEquals(2, r.rows().size(), "expected 2 unnested rows");
        }
    }
}
