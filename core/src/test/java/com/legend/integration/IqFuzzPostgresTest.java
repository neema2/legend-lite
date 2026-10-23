// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.EmbeddedPostgres;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The fuzzer's differential between DuckDB and the embedded Postgres 16 (the IQ scale program,
 * docs/IQ_SCALE_PROGRAM_2026_10_22.md P2.2, {@link IqFuzzDifferential}): the same random islands answered by both,
 * compared as multisets. It needs no account, so it runs where the Postgres lanes run. Manual
 * ({@code //core:iq_fuzz_postgres_test}): {@code -Diq.fuzz.seed} (default 1), {@code -Diq.fuzz.count} (default 500),
 * {@code -Diq.fuzz.depth} (default 3), {@code -Diq.fuzz.profile} (default {@code iq17-default}).
 */
class IqFuzzPostgresTest {

    @Test
    void duckDbAndPostgresAnswerRandomIslandsAlike() throws Exception {
        long seed = Long.getLong("iq.fuzz.seed", 1L);
        int count = Integer.getInteger("iq.fuzz.count", 500);
        int depth = Integer.getInteger("iq.fuzz.depth", 3);
        String profile = System.getProperty("iq.fuzz.profile", "iq17-default");
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        Set<String> names = IqParity.tableNames(tables);
        List<IqFuzzDifferential.Query> queries = IqFuzzDifferential.queries(seed, count, depth, tables);
        Map<IqFuzzDifferential.Query, IqFuzzDifferential.Answer> duck = IqFuzzDifferential.onDuckDb(queries, profile,
                tables, names);
        Map<IqFuzzDifferential.Query, IqFuzzDifferential.Answer> pg = new LinkedHashMap<>();
        EmbeddedPostgres postgres = EmbeddedPostgres.shared();
        try (Connection admin = DriverManager.getConnection(postgres.jdbcUrl("postgres"))) {
            IqParity.seed(TestBackends.of(DatabaseType.Postgres), admin, tables);
            grantToThisAccount(admin, tables);
            String model = IqParity.model(tables, String.format(Locale.ROOT, "type: Postgres;\n    specification:"
                    + " Static { host: '127.0.0.1'; port: %d; name: 'postgres'; };\n    auth: Test;",
                    postgres.port()));
            QueryService service = new QueryService();
            for (var e : duck.entrySet()) {
                if (!e.getValue().kind().equals("REFUSED")) {
                    pg.put(e.getKey(), IqFuzzDifferential.answer(service, admin, model, e.getKey(), profile, tables,
                            names));
                }
            }
        }
        IqFuzzDifferential.report("postgres", seed, count, depth, profile, duck, pg);
    }

    /** A connection with {@code auth: Test} logs in as this operating-system account (as IqParityPostgresTest): it
     *  needs a login role that reads the tables. */
    private static void grantToThisAccount(Connection admin, List<SqlParity.Table> tables) throws SQLException {
        String user = System.getProperty("user.name");
        String account = "\"" + user.replace("\"", "\"\"") + "\"";
        try (PreparedStatement exists = admin.prepareStatement("SELECT 1 FROM pg_roles WHERE rolname = ?")) {
            exists.setString(1, user);
            try (ResultSet r = exists.executeQuery(); Statement s = admin.createStatement()) {
                if (!r.next()) {
                    s.execute("CREATE ROLE " + account + " LOGIN");
                }
                for (SqlParity.Table t : tables) {
                    s.execute("GRANT SELECT ON " + t.name() + " TO " + account);
                }
            }
        }
    }
}
