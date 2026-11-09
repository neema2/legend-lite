// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedSpec;

/**
 * The METAMODEL SYSTEM ROUTE, planned (the planner-driver seam program's S2h, 2026-10-31; the body driver decided it
 * while it opened the session, until then): a body that reads the graph's system metamodel store runs on the system
 * database of the database its runtime executes on, and its plan names that database's tables -- the store's own
 * names on a database the platform runs in process (its dialect's {@code inProcess}, which
 * {@code ConnectorsTest.aDatabaseRunsInProcessOnBothSidesAlike} holds equal to the connector's), else the system
 * session's temporary tables ({@code Ddl.systemTable}). A body reading the system store AND a user store has no single
 * session: refused by name. The session itself -- one per graph per engine -- is the driver's ({@code SystemDatabase}),
 * which only opens it: the store's tables and rows are the plan's own seed step ({@link #seeds}; the walker-deletion
 * program, 2026-11-03: until then the holder wrote them when it opened the session, a send outside every plan).
 */
public final class SystemRoutes {

    private SystemRoutes() {
    }

    /** A body's route to the system database: the database it executes on and the store. */
    public record Route(com.legend.model.ConnectionDefinition.DatabaseType engine,
            com.legend.model.DatabaseDefinition store) {
    }

    /** Whether {@code runtimeFqn} executes on an engine the platform runs IN PROCESS (DuckDB, H2, SQLite), where the
     *  system store's tables keep their own names ({@link #renames} is empty): the engines the test door's program may
     *  name the graph's system database on (the test-is-the-product plan's M10a; a server engine's temporary names
     *  wait for the program road's renames). */
    public static boolean inProcess(com.legend.compiler.element.ModelContext ctx, String runtimeFqn) {
        return com.legend.database.Databases.dialect(com.legend.Compiler.executesOn(ctx, runtimeFqn).type()).inProcess();
    }

    /** {@code body}'s route, or null when it reads no system store. */
    public static @com.legend.base.Nullable Route of(java.util.List<TypedSpec> body, PlanEnv env) {
        java.util.Set<String> stores = stores(body);
        String storeFqn = com.legend.builtin.SystemMetamodel.STORE_FQN;
        if (!stores.contains(storeFqn)) {
            return null;
        }
        CrossStoreGuard.oneSession(stores);
        var store = env.ctx().findDatabase(storeFqn).orElseThrow(() ->
                new IllegalStateException("the system metamodel store is"
                        + " not in the model — injection regressed"));
        return new Route(com.legend.Compiler.executesOn(env.ctx(), env.runtimeFqn()).type(), store);
    }

    /** The stores {@code body}, resolved, reads, in name order. */
    public static java.util.Set<String> stores(java.util.List<TypedSpec> body) {
        java.util.Set<String> stores = new java.util.TreeSet<>();
        for (TypedSpec n : body) {
            BodyPlanner.collectStores(n, stores);
        }
        return stores;
    }

    /** The store's tables as {@code route}'s system session names them: none renamed on a database the platform runs
     *  in process, each the session's temporary table otherwise. A database with no dialect is refused by name. */
    public static java.util.Map<String, String> renames(Route route) {
        java.util.Map<String, String> renames = new java.util.LinkedHashMap<>();
        if (!com.legend.database.Databases.dialect(route.engine()).inProcess()) {
            for (var schema : route.store().schemas()) {
                for (var def : schema.tables()) {
                    renames.put(schema.name() + "." + def.name(),
                            com.legend.setup.Ddl.systemTable(schema.name(), def.name()));
                }
            }
        }
        return java.util.Collections.unmodifiableMap(renames);
    }

    /** The graph's system store's KEY (the plan-as-data design's step 7, 2026-11-08, 2.6): a hash of its content -- each
     *  table's schema, name and definition, and the graph's rows for it -- in no dialect's spelling, so it names the
     *  store on every engine; the plan's {@code Database.System} carries it on a server engine (the process's catalog
     *  is named by the platform's, {@link SystemCatalog#target}), and the run finds the store's session by it. Knowledge, derived
     *  once per graph. */
    public static String graphKey(com.legend.compiler.element.ModelContext ctx) {
        return ctx.derived(GraphKey.class, c -> new GraphKey(SystemCatalog.hashOf(SystemCatalog.store(c), table -> MetamodelSeeds.rows(table, c)))).key;
    }

    private static final class GraphKey {
        private final String key;

        GraphKey(String key) {
            this.key = key;
        }
    }

    /** {@code env} with {@code systemRenames} (a route's {@link #renames}) added to its table renames (a conflicting
     *  rename throws — never a silent pick). */
    public static PlanEnv routed(PlanEnv env, java.util.Map<String, String> systemRenames) {
        if (systemRenames.isEmpty()) {
            return env;
        }
        java.util.Map<String, String> renames = new java.util.LinkedHashMap<>(env.tableReplace());
        systemRenames.forEach((from, to) -> {
            String had = renames.putIfAbsent(from, to);
            if (had != null && !had.equals(to)) {
                throw new IllegalStateException("table '" + from + "' is renamed both '" + had + "' and '"
                        + to + "'");
            }
        });
        return env.withTableReplace(renames);
    }
}
