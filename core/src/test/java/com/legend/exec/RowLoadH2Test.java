// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.setup.RowLoad;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A row load on H2, which has no bulk loader: the insert text, never the bulk path ({@code RowLoadDuckDbTest} is
 * DuckDB's; both were {@code RowLoadTest} until the dialect program's Phase 6, 2026-10-16).
 */
// reads process-wide counters (Census) that every query bumps: runs alone if JUnit ever runs classes in parallel
@org.junit.jupiter.api.parallel.Isolated
class RowLoadH2Test {

    private static final String DDL = "CREATE TABLE s.t (i INTEGER, v VARCHAR, d DATE,"
            + " n DECIMAL(10,2), b BOOLEAN, ts TIMESTAMP, f DOUBLE PRECISION)";

    private static List<String> contents(Connection c) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM s.t ORDER BY i NULLS LAST, v")) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int k = 1; k <= n; k++) {
                    row.append(k == 1 ? "" : "|").append(rs.getString(k));
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    @Test
    @DisplayName("a connection's declared CSV test data loads on its own database, rows exact")
    void declaredCsvTestDataLoadsOnItsDatabase() throws Exception {
        String model = """
                Class x::Firm { name: String[1]; since: StrictDate[0..1]; size: Integer[0..1]; }
                ###Relational
                Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32), SINCE DATE, SIZE INTEGER) )
                ###Mapping
                Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM
                    name: [x::DB] FIRM.NAME, since: [x::DB] FIRM.SINCE, size: [x::DB] FIRM.SIZE } )
                ###Connection
                RelationalDatabaseConnection x::Conn
                {
                    store: x::DB;
                    type: H2;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME,SINCE,SIZE\\n1,Acme,2020-01-02,10\\n2,O\\'Brien,,---null---\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
                """;
        // declared test data is a LocalH2 connection's (as legend-engine's): it loads on the H2 session its
        // runtime declares -- the server opens one for it (ConnectionResolver) -- which takes the insert
        // text; DuckDB's bulk path is systemSeedIsBulk's
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            long before = Census.count(Census.Key.BULK_LOADS);
            var r = Lean.established(model, "|x::Firm.all()->project(~[name: f|$f.name,"
                    + " since: f|$f.since, size: f|$f.size])->sort(~name->ascending())", "x::RT", c);
            assertEquals(List.of("Acme|2020-01-02|10", "O'Brien|null|null"), r.rows().stream()
                    .map(row -> row.get(0) + "|" + row.get(1) + "|" + row.get(2)).toList());
            assertEquals(before, Census.count(Census.Key.BULK_LOADS), "H2 has no bulk loader");
        }
    }

    @Test
    @DisplayName("an engine without a bulk loader takes the insert text")
    void h2TakesText() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open()) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA s");
                st.execute(DDL);
            }
            long before = Census.count(Census.Key.BULK_LOADS);
            // the one INSERT the plan side writes for a database with no bulk loader, run by the plan runner (the
            // doors, 2026-10-29; SetupRunner's load until then)
            RowLoad load = new RowLoad("s", "t", com.legend.sql.NameQuoting.BARE, List.of("i", "v"), java.util.Set.of(), 2, List.of(List.of("1", "a")));
            PlanRunner.setUp(List.of(new com.legend.executionplan.ExecutionPlan.SetupStep.Statement(
                    new com.legend.sql.dialect.H2Modern().render(load.values()))),
                    new com.legend.executionplan.ExecutionPlan.Target(
                            new com.legend.executionplan.ExecutionPlan.Database.Platform(DatabaseType.H2),
                            new com.legend.executionplan.ExecutionPlan.Servers.Every(), List.of(), List.of()),
                    PlanSessions.of(com.legend.testbackend.TestSessions.over(c)), null);
            assertEquals(before, Census.count(Census.Key.BULK_LOADS));
            assertEquals(List.of("1|a|null|null|null|null|null"), contents(c));
        }
    }
}
