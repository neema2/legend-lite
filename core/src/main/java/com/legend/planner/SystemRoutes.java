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
 * session: refused by name. The session itself -- opened and seeded once per graph -- is the driver's
 * ({@code SystemDatabase}), which checks that it names the tables as planned here.
 */
public final class SystemRoutes {

    private SystemRoutes() {
    }

    /** A body's route to the system database: the database it executes on and the store. */
    public record Route(com.legend.model.ConnectionDefinition.DatabaseType engine,
            com.legend.model.DatabaseDefinition store) {
    }

    /** {@code body}'s route, or null when it reads no system store. */
    public static @com.legend.base.Nullable Route of(java.util.List<TypedSpec> body, PlanEnv env) {
        java.util.Set<String> stores = new java.util.TreeSet<>();
        for (TypedSpec n : body) {
            BodyPlanner.collectStores(n, stores);
        }
        String storeFqn = com.legend.builtin.SystemMetamodel.STORE_FQN;
        if (!stores.contains(storeFqn)) {
            return null;
        }
        if (stores.size() > 1) {
            throw new com.legend.error.NotImplementedException("a query reading"
                    + " the system metamodel store AND a user store " + stores
                    + " has no single session — the system database is"
                    + " separate from user connections");
        }
        var store = env.ctx().findDatabase(storeFqn).orElseThrow(() ->
                new IllegalStateException("the system metamodel store is"
                        + " not in the model — injection regressed"));
        return new Route(com.legend.Compiler.executesOn(env.ctx(), env.runtimeFqn()).type(), store);
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
