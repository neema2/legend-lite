// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.planner.ConnectionPlans;
import com.legend.planner.SystemRoutes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test body's environment ON THE PROCESS'S CATALOG (B4, 2026-11-11, the audit's finding 4): retargeted onto the
 * catalog it is written in the catalog's dialect, and back on the body's own target in the runtime's again -- the
 * runtime's own where the platform runs it in process, the planning half untouched there -- and a system-routed body
 * reads a data: source in the catalog's dialect, by its statement alone (the JSON pass, 2026-11-11).
 */
class ExecEnvCatalogTest {

    private static String model(DatabaseType type) {
        return """
                Class s::Item { id: Integer[1]; }
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
                ###Mapping
                Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T id: [s::DB] T.ID } )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; type: %s; specification: LocalH2 { }; auth: DefaultH2; }
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
    }

    @Test
    void anEnvironmentOnTheCatalogIsWrittenInItsDialectAndBackInTheRuntimes() {
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(model(type));
            BodyDriver.ExecEnv env = BodyDriver.ExecEnv.of(ctx, "s::RT", Databases.dialect(type),
                    ExecuteOptions.NONE.planning());
            SystemRoutes.Route route = SystemRoutes.route(ctx, "s::RT");
            Class<?> catalog = Databases.dialect(ConnectionPlans.model(route.engine())).getClass();
            BodyDriver.ExecEnv on = env.withTarget(BodyDriver.systemTarget(ctx, route));
            assertEquals(catalog, on.dialect().getClass(), type + ": on the catalog");
            BodyDriver.ExecEnv back = on.withTarget(env.target());
            assertEquals(env.target(), back.target(), type.name());
            assertEquals(env.dialect().getClass(), back.dialect().getClass(), type + ": back on the body's own");
            if (route.ownEngine()) {
                assertSame(env.plan(), on.plan(), type + ": in process, the planning half untouched");
            }
            // a system-routed body on the catalog reads a data: source in the catalog's dialect, by its statement
            // alone (the JSON pass, 2026-11-11: until then one set of session tables shared with the body's own)
            BodyDriver.ExecEnv body = env.onCatalogBody(route);
            assertEquals(catalog, body.dialect().getClass(), type + ": the body on the catalog");
            com.legend.sql.SqlQuery read = body.read(com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.SourceUrl(
                    "data:application/json,[1]", "t0", java.util.List.of(new com.legend.sql.OutputCol("data",
                            com.legend.sql.SqlType.Scalar.JSON, true, com.legend.sql.OutputCol.Origin.DERIVED)),
                    com.legend.sql.SqlSource.SourceUrl.Form.ELEMENTS)));
            assertTrue(!body.dialect().renderStatement(read).sql().toLowerCase(java.util.Locale.ROOT)
                    .contains("legend_data_"),
                    type + ": no session table");
        }
    }

    /** A side moved onto its catalog keeps the plan's style (the D part 1 audit's S1, 2026-11-15): on a server lane,
     *  where the catalog is not the runtime's own engine, the edge's style writes the catalog's statements in the
     *  edge's dialect, the product's in the product's. */
    @Test
    void aSideOnTheCatalogKeepsThePlansStyle() {
        int routed = 0;
        for (DatabaseType type : Databases.relational()) {
            var ctx = Compiler.compileModel(model(type));
            SystemRoutes.Route route = SystemRoutes.route(ctx, "s::RT");
            if (route.ownEngine()) {
                continue;
            }
            routed++;
            DatabaseType engine = ConnectionPlans.model(route.engine());
            var edge = com.legend.planner.PlanEnv.of(ctx, "s::RT", com.legend.edge.PureText.UPSTREAM.dialect(type),
                    com.legend.edge.PureText.UPSTREAM, java.util.Set.of(), g -> "");
            assertEquals(com.legend.edge.PureText.dialect(engine).getClass(),
                    SystemRoutes.onCatalog(edge, route).dialect().getClass(), type + ": the edge's catalog dialect");
            var product = com.legend.planner.PlanEnv.of(ctx, "s::RT", Databases.dialect(type),
                    com.legend.database.PlanStyle.PRODUCT, java.util.Set.of(), g -> "");
            assertEquals(Databases.dialect(engine).getClass(),
                    SystemRoutes.onCatalog(product, route).dialect().getClass(), type + ": the product's");
        }
        assertTrue(routed > 0, "a server lane routes to a catalog of another engine");
    }
}
