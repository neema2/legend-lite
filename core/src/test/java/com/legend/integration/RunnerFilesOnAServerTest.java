// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.Sessions;
import com.legend.executionplan.DataStores;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A FILE'S LOAD ON A DATABASE THAT READS NO FILE OF THE RUNNER'S (S372, the user's ruling pending; track Q2, the
 * orchestrator's go of 2026-11-19): a file load's database reads the runner's copy ({@code Delivery.Reads}), and a database on a server --
 * the warehouse's DuckDB, an H2 server -- reads its own machine's files. Refused by name at both ends: at PLANNING, for a
 * run whose target the caller says runs on a server ({@code ExecuteOptions.withTargetOnAServer}: the warehouse lane's);
 * at the RUN, on a session made as a server's ({@code Sessions.Reach}, said where the session is made, never read off its
 * driver) -- nothing sent. The same body plans and loads where the database runs in this process.
 */
class RunnerFilesOnAServerTest {

    /** The platform's own database, which reads a file itself (its dialect spells the load). */
    private static final DatabaseType DB = TestBackends.platform().type();

    /** A table made, the file loaded into it, its rows asserted (the delivery matrix's body, DataDeliveryMatrixTest). */
    private static final String LOAD_PEOPLE = "{|let connection = x::RT.runtimeValue.connectionStores.connection->toOne()"
            + "->cast(@meta::external::store::relational::runtime::DatabaseConnection);"
            + " meta::relational::functions::toDDL::dropAndCreateTableInDb(x::DB, 'PEOPLE', $connection);"
            + " meta::relational::metamodel::execute::loadCsvToDbTable('people.csv', x::DB->meta::relational::metamodel"
            + "::schema('default')->toOne()->meta::relational::metamodel::table('PEOPLE')->toOne(), $connection); "
            + "let res = execute(|x::P.all()->sortBy(p|$p.id), x::M, x::RT.runtimeValue,"
            + " []); assertEquals(['a', 'b'], $res.values.name);}";

    private static String model() {
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
                """.formatted(DB, TestBackends.of(DB).specificationAndAuth() + ";");
    }

    @Test
    void aTargetOnAServerRefusesTheFileLoadAtPlanning(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("people.csv"), "ID,NAME\n1,a\n2,b\n", StandardCharsets.UTF_8);
        ExecuteOptions options = new ExecuteOptions(com.legend.testexecutionplan.NoEdge.NONE, DataStores.resources(dir),
                Set.of());
        // the platform's own target reads the runner's files: the body plans
        TestExecution.planBody(body(), "x::RT", options);
        // a target on a server does not: refused by name, the file named, before anything is planned to run
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> TestExecution.planBody(body(), "x::RT", options.withTargetOnAServer()));
        assertTrue(e.getMessage().contains("people.csv") && e.getMessage().contains("runs on a server")
                && e.getMessage().contains("(S372)"), e.getMessage());
    }

    @Test
    void aSessionOnAServerIsSentNoFileLoad(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("people.csv"), "ID,NAME\n1,a\n2,b\n", StandardCharsets.UTF_8);
        ExecuteOptions options = new ExecuteOptions(com.legend.testexecutionplan.NoEdge.NONE, DataStores.resources(dir),
                Set.of());
        var plan = TestExecution.planBody(body(), "x::RT", options);
        try (Connection c = TestBackends.of(DB).open()) {
            // a session handed in as a server's: the plan's load is refused by name at the run, nothing sent
            RuntimeException e = assertThrows(RuntimeException.class, () -> TestExecution.runBody(plan,
                    Sessions.borrowed(c, Sessions.Reach.SERVER), null, com.legend.testexec.NoEdgeRun.NONE, options,
                    new Output.Rows(new Collector())));
            assertTrue(e.getMessage().contains("people.csv") && e.getMessage().contains("runs on a server")
                    && e.getMessage().contains("(S372)"), e.getMessage());
            try (Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(*) FROM PEOPLE")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "nothing loaded: the table the body made holds no row");
            }
        }
        // the same plan on a session in this process loads the file and answers its rows
        try (Connection c = TestBackends.of(DB).open()) {
            Collector collector = new Collector();
            TestExecution.runBody(plan, Sessions.borrowed(c, Sessions.Reach.THIS_PROCESS), null,
                    com.legend.testexec.NoEdgeRun.NONE, options, new Output.Rows(collector));
        }
    }

    /** The matrix's load-then-read body on the platform's database: a table made, the file loaded, its rows asserted. */
    private static com.legend.TypedQuery body() {
        ModelContext ctx = Compiler.compileModel(model());
        var parsed = Compiler.parseQuery(LOAD_PEOPLE);
        var statements = ((com.legend.protocol.spec.LambdaFunction) parsed).body();
        var resolved = Compiler.resolveQuery(statements, new com.legend.model.ImportScope(List.of()), ctx);
        return Compiler.queryResolved(ctx, resolved);
    }
}
