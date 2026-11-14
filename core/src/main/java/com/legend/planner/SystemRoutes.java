// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedSpec;

/**
 * The METAMODEL SYSTEM ROUTE, planned (the planner-driver seam program's S2h, 2026-10-31; the body driver decided it
 * while it opened the session, until then): a body that reads the graph's system metamodel store runs on the process's
 * CATALOG ({@link SystemCatalog}) -- the catalog of the database its runtime executes on where the platform runs it in
 * process, DuckDB's for a server (B4, 2026-11-11, the user's ruling: every statement reading the store runs on the
 * in-process catalog; the server session's temporary tables and their renames are deleted) -- and its plan names the
 * store's tables by their own names, written in the catalog's dialect. A body reading the system store AND a user store
 * has no single session: refused by name. The session itself is exec's ({@code SystemDatabase}), which only opens it:
 * the store's tables and rows are the plan's own loads ({@link SystemCatalog#target}, {@link SystemCatalog#seeds}).
 */
public final class SystemRoutes {

    private SystemRoutes() {
    }

    /** A body's route to the system store: the engine of the catalog it reads ({@link SystemCatalog#engine}), the
     *  store, and whether that engine is the runtime's own (in process: the body's dialect is the catalog's). */
    public record Route(com.legend.executionplan.DatabaseType engine, com.legend.model.DatabaseDefinition store,
            boolean ownEngine) {

        /** The dialect a statement on this route is written in: its catalog's. */
        public com.legend.sql.dialect.SqlDialect dialect() {
            return com.legend.database.Databases.dialect(ConnectionPlans.model(engine));
        }
    }

    /** {@code body}'s route, or null when it reads no system store. */
    public static @com.legend.base.Nullable Route of(java.util.List<TypedSpec> body, PlanEnv env) {
        java.util.Set<String> stores = stores(body);
        if (!stores.contains(com.legend.builtin.SystemMetamodel.STORE_FQN)) {
            return null;
        }
        CrossStoreGuard.oneSession(stores);
        return route(env.ctx(), env.runtimeFqn());
    }

    /** The route of a body on {@code runtimeFqn} that reads the system store: the catalog of the database the runtime
     *  executes on. */
    public static Route route(com.legend.compiler.element.ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn) {
        var store = ctx.findDatabase(com.legend.builtin.SystemMetamodel.STORE_FQN).orElseThrow(() ->
                new IllegalStateException("the system metamodel store is not in the model — injection regressed"));
        var runtime = ConnectionPlans.type(com.legend.Compiler.executesOn(ctx, runtimeFqn).type());
        var engine = SystemCatalog.engine(runtime);
        return new Route(engine, store, engine == runtime);
    }

    /** The stores {@code body}, resolved, reads, in name order. */
    public static java.util.Set<String> stores(java.util.List<TypedSpec> body) {
        java.util.Set<String> stores = new java.util.TreeSet<>();
        for (TypedSpec n : body) {
            BodyPlanner.collectStores(n, stores);
        }
        return stores;
    }

    /** {@code env} written in {@code route}'s catalog's dialect where the catalog is not the runtime's own engine (a
     *  test body's on a server, B4); unchanged where it is, or where {@code env} names no dialect (the product's
     *  statement road, whose caller renders). */
    public static PlanEnv onCatalog(PlanEnv env, Route route) {
        return env.dialect() == null || route.ownEngine() ? env : env.withDialect(route.dialect());
    }
}
