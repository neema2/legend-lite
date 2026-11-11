// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testplan.PlanningOptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EVERY CATALOG STATEMENT ON THE PROCESS'S IN-PROCESS CATALOG (B4, 2026-11-11, the user's ruling "B4 sounds good for
 * now", reconfirmed that day: the server forms of the system metamodel store deleted, census M5 and M6): on every
 * relational database, a query, a program and a test body reading the store alone are planned onto the catalog of the
 * runtime's own engine where the platform runs it in process, the platform's own engine's for a server -- written in
 * that catalog's dialect, so the plan is the one the catalog's own engine makes -- and a statement of a body on a server
 * reading the store alone is routed there beside the body's own database. Planning only: the embedded and cloud
 * servers run these on their lanes (the Postgres matrix's catalog cells, {@code DataDeliveryMatrixPostgresTest}).
 */
class CatalogOnEveryDatabaseTest {

    private static final String CATALOG = "meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'Item')"
            + "->project(~[name: c|$c.name])";

    private static String model(DatabaseType type) {
        return """
                Class s::Item { id: Integer[1]; name: String[0..1]; }
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20) ) )
                ###Mapping
                Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T id: [s::DB] T.ID, name: [s::DB] T.NAME } )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
    }

    /** The engine of the catalog a runtime on {@code type} reads the store on: its own in process, else the
     *  platform's. */
    private static DatabaseType catalogOf(DatabaseType type) {
        return Databases.dialect(type).inProcess() ? type : Databases.PLATFORM;
    }

    private static boolean onTheCatalogOf(ExecutionPlan.Target target, DatabaseType type) {
        return target.database() instanceof ExecutionPlan.Database.System s
                && com.legend.planner.ConnectionPlans.model(s.type()) == catalogOf(type);
    }

    /** A product query and a writing program reading the store alone: on the catalog, its plan the catalog's own
     *  engine's. */
    @Test
    void aQueryAndAProgramReadingTheStoreRunOnTheCatalog() {
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(model(type));
            var onCatalog = Compiler.compileModel(model(catalogOf(type)));
            for (String query : List.of("|" + CATALOG,
                    "{|let s = " + CATALOG + "->meta::legend::lite::stage(); $s;}")) {
                ExecutionPlan plan = Compiler.query(ctx, query).executionPlan("s::RT");
                for (ExecutionPlan.Sql sql : sqls(plan.root())) {
                    assertTrue(onTheCatalogOf(sql.target(), type), type + " " + query + ": " + sql.target());
                }
                assertEquals(com.legend.executionplan.PlanJson.write(Compiler.query(onCatalog, query)
                        .executionPlan("s::RT")), com.legend.executionplan.PlanJson.write(plan), type + " " + query);
            }
        }
    }

    /** A test body reading the store alone, under every judge: every statement of its program and its asserts the
     *  catalog's, as the catalog's own engine plans them. */
    @Test
    void aSystemRoutedTestBodyRunsOnTheCatalogUnderEveryJudge() {
        String body = "{| assertEquals(['Item'], " + CATALOG + "->map(r|$r.name));}";
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(model(type));
            var onCatalog = Compiler.compileModel(model(catalogOf(type)));
            for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
                ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(mode);
                TestPlan plan = TestExecution.planBody(Compiler.query(ctx, body), "s::RT", options);
                List<ExecutionPlan.Sql> sqls = sqls(plan);
                assertFalse(sqls.isEmpty(), type + " " + mode);
                for (ExecutionPlan.Sql sql : sqls) {
                    assertTrue(onTheCatalogOf(sql.target(), type), type + " " + mode + ": " + sql.target());
                }
                // every statement and its target as the catalog's own engine plans them (the plan's session is the
                // body's runtime's own, which a system-routed body's statements do not use)
                List<ExecutionPlan.Sql> catalogs = sqls(TestExecution.planBody(Compiler.query(onCatalog, body),
                        "s::RT", options));
                assertEquals(catalogs.stream().map(q -> q.statement() + " @ " + q.target()).toList(),
                        sqls.stream().map(q -> q.statement() + " @ " + q.target()).toList(), type + " " + mode);
            }
        }
    }

    /** A body reading the user's store and, in an assert of its own, the system store: that assert's statement routed
     *  to the catalog beside the body's database (per-statement routing), written as the catalog's own engine writes
     *  it; the other on the body's database. */
    @Test
    void aStatementReadingTheStoreAloneIsRoutedToTheCatalogBesideItsBody() {
        // (the host judge's: the database judge's verdict groups on two sessions are not planned, by name)
        String body = "{| assertEquals('M', s::M.name); assertSize(s::Item.all(), 0);}";
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(model(type));
            var onCatalog = Compiler.compileModel(model(catalogOf(type)));
            TestPlan plan = TestExecution.planBody(Compiler.query(ctx, body), "s::RT", ExecuteOptions.NONE);
            List<String> routed = new ArrayList<>();
            boolean own = false;
            for (ExecutionPlan.Sql sql : sqls(plan)) {
                if (sql.target().database() instanceof ExecutionPlan.Database.System) {
                    assertTrue(onTheCatalogOf(sql.target(), type), type + ": " + sql.target());
                    routed.add(sql.statement());
                } else {
                    own = true;
                }
            }
            assertFalse(routed.isEmpty(), type + ": no statement routed to the catalog");
            assertTrue(own, type + ": no statement on the body's own database");
            List<String> catalogs = new ArrayList<>();
            for (ExecutionPlan.Sql sql : sqls(TestExecution.planBody(Compiler.query(onCatalog, body), "s::RT",
                    ExecuteOptions.NONE))) {
                if (sql.target().database() instanceof ExecutionPlan.Database.System) {
                    catalogs.add(sql.statement());
                }
            }
            assertEquals(catalogs, routed, type.name());
        }
    }

    /**
     * A user's table named as a metamodel table ({@code results}, {@code classes}) is the user's (the B4 audit's
     * finding 1, 2026-11-11): a writing program reading it stays on the runtime's own database -- the decision is the
     * statement's resolved stores', never a table's name. A database whose dialect stages no query's rows refuses the
     * program by its own name.
     */
    @Test
    void aUsersTableNamedAsAMetamodelTableIsTheUsers() {
        for (String table : List.of("results", "classes")) {
            for (DatabaseType type : Databases.relational()) {
                var ctx = Compiler.compileModel(model(type).replace("Table T", "Table " + table)
                        .replace("[s::DB] T", "[s::DB] " + table));
                ExecutionPlan plan;
                try {
                    plan = Compiler.query(ctx, "{| let r = s::Item.all()->project(~[n:x|$x.name])"
                            + "->meta::legend::lite::stage(); $r;}").executionPlan("s::RT");
                } catch (com.legend.sql.DialectCapability refused) {
                    assertTrue(refused.getMessage().contains("temporary table"), type + ": " + refused.getMessage());
                    continue;
                }
                for (ExecutionPlan.Sql sql : sqls(plan.root())) {
                    assertFalse(sql.target().database() instanceof ExecutionPlan.Database.System,
                            type + " " + table + ": " + sql.statement());
                }
            }
        }
    }

    /**
     * A writing program reading the system store in one statement and the user's store in another is refused by name
     * on every database, whichever comes first (the B4 audit's finding 3): as the second statement is prepared, before
     * it is written -- the catalog's read first, on the catalog's pass; the user's first, on the runtime's (where the
     * runtime's dialect stages no query's rows, its own refusal of the user's statement comes first).
     */
    @Test
    void aProgramReadingTheStoreBesideTheUsersIsRefusedByName() {
        String catalog = "let c = " + CATALOG + "->meta::legend::lite::stage();";
        String user = "let u = s::Item.all()->project(~[n:x|$x.name])->meta::legend::lite::stage();";
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(model(type));
            for (String program : List.of("{| " + catalog + " " + user + " $u;}", "{| " + user + " " + catalog
                    + " $c;}")) {
                RuntimeException refused = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                        () -> Compiler.query(ctx, program).executionPlan("s::RT"), type + " " + program);
                boolean named = refused instanceof com.legend.error.NotImplementedException
                        && refused.getMessage().contains("system metamodel store beside another source");
                boolean userFirstOnItsOwn = program.indexOf(user) < program.indexOf(catalog)
                        && refused instanceof com.legend.sql.DialectCapability
                        && refused.getMessage().contains("temporary table");
                assertTrue(named || userFirstOnItsOwn, type + " " + program + ": " + refused);
            }
        }
    }

    /** Every SQL statement {@code node} sends. */
    private static List<ExecutionPlan.Sql> sqls(ExecutionPlan.Node node) {
        return switch (node) {
            case ExecutionPlan.TdsResult t -> List.of(t.sql());
            case ExecutionPlan.Check c -> List.of(c.sql());
            case ExecutionPlan.Update u -> List.of(u.sql());
            case ExecutionPlan.Effect e -> List.of(e.sql());
            case ExecutionPlan.Keyed k -> List.of(k.keys(), k.body());
            case ExecutionPlan.Load l -> List.of();
            case ExecutionPlan.Constant c -> List.of();
            case ExecutionPlan.Sequence q -> q.steps().stream().flatMap(n -> sqls(n).stream()).toList();
        };
    }

    /** Every SQL statement a test plan's steps send. */
    private static List<ExecutionPlan.Sql> sqls(TestPlan plan) {
        List<ExecutionPlan.Sql> out = new ArrayList<>();
        for (TestPlan.TestStep step : plan.steps()) {
            switch (step) {
                case TestPlan.Run r -> out.addAll(sqls(r.node()));
                case TestPlan.FrameRun f -> out.add(f.check().sql());
                case TestPlan.Verdicts v -> TestPlan.statements(v).forEach(n -> out.addAll(sqls(n)));
                case TestPlan.Branch b -> TestPlan.statements(b).forEach(n -> out.addAll(sqls(n)));
            }
        }
        return out;
    }
}
