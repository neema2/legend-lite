// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * THE SYSTEM CATALOG, planned (L4b, 2026-11-09; docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.1-4.5, the implementation
 * plan of docs/PLAN_AS_DATA_2026_11_07.md, hole 2): the one maker of a target naming a system database and of the system
 * metamodel store's loads. The store is the PROCESS'S CATALOG on a database the platform runs in process -- its tables
 * and the platform's rows one ONCE load, each model's own rows a registration of inserts in the run's transaction --
 * whatever database holds the user's data (B4, 2026-11-11, the user's ruling "B4 sounds good for now", reconfirmed that
 * day: every statement reading the store runs on the in-process catalog; the server forms -- the whole store as a
 * server session's temporary tables, or as a query's inline rows -- are deleted). Knowledge, derived once per graph and
 * engine; the routing a body takes to it is {@link SystemRoutes}'.
 */
public final class SystemCatalog {

    private SystemCatalog() {
    }

    /**
     * The engine of the process's catalog a program on {@code runtime}'s database reads the system metamodel store on
     * (B4, 2026-11-11): the runtime's own where the platform runs it in process (DuckDB, H2, SQLite: an H2 lane keeps
     * exercising the metamodel's statements on H2), else the platform's own engine ({@code Databases.PLATFORM}) -- one
     * catalog per process for every server database, never a second road on the server. A statement reading the store
     * alone is planned in this engine's dialect ({@link #dialect}); one reading the store beside a user's table stays
     * refused by name.
     */
    public static com.legend.executionplan.DatabaseType engine(com.legend.executionplan.DatabaseType runtime) {
        return com.legend.database.Databases.dialect(ConnectionPlans.model(runtime)).inProcess() ? runtime
                : ConnectionPlans.type(com.legend.database.Databases.PLATFORM);
    }

    /** The dialect a statement on {@code runtime}'s database that reads the catalog is written in: its catalog's
     *  engine's ({@link #engine}). */
    public static com.legend.sql.dialect.SqlDialect dialect(com.legend.executionplan.DatabaseType runtime) {
        return com.legend.database.Databases.dialect(ConnectionPlans.model(engine(runtime)));
    }

    /**
     * The target naming the process's CATALOG a program on {@code runtime}'s database reads the metamodel store on (L4b,
     * 2026-11-09, docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.1-4.5; B4, 2026-11-11: on every database, its engine
     * {@link #engine}'s): named by the platform's content key, made once with the store's tables and the platform's
     * rows (its ONCE load), each program registering its model's own rows in its run's transaction, rolled back when the
     * run ends ({@link #seeds}); its session statements its engine's dialect's. The one maker of a system target.
     */
    public static com.legend.executionplan.ExecutionPlan.Target target(com.legend.compiler.element.ModelContext ctx,
            com.legend.executionplan.DatabaseType runtime) {
        com.legend.executionplan.DatabaseType type = engine(runtime);
        com.legend.sql.dialect.SqlDialect dialect = dialect(type);
        var bare = Targets.of(new com.legend.executionplan.ExecutionPlan.Database.System(type, Platform.KEY), dialect);
        var once = com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE;
        Seeded platform = ctx.derived(SeedMemo.class, c -> new SeedMemo()).platform.computeIfAbsent(type, t -> {
            // the catalog's own tables and rows, made once here, outside any run's transaction (until the JSON pass,
            // 2026-11-11, H2's schema for a body's data: session tables too)
            var steps = steps(store(ctx), dialect, true, Platform::rows);
            return new Seeded(LoadPlanner.identity(once, steps.steps()), steps);
        });
        return new com.legend.executionplan.ExecutionPlan.Target(bare.database(), bare.session(),
                java.util.List.of(platform.load(once, bare)), bare.raiseReadBack(), bare.settings());
    }

    /** The graph's system metamodel store's load on {@code system} (the catalog's target, {@link #target}), written in
     *  its engine's dialect: the model's REGISTRATION -- its own rows ({@code MetamodelSeeds} less the platform's),
     *  inserts with no DDL, {@code PER_TRANSACTION} -- or null where the model adds no row. Made by the load planner,
     *  named by its content as every load is (L4a, 2026-11-09). Knowledge, derived once per graph and engine. */
    public static com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable Load seeds(
            com.legend.compiler.element.ModelContext ctx, com.legend.executionplan.ExecutionPlan.Target system) {
        if (!(system.database() instanceof com.legend.executionplan.ExecutionPlan.Database.System s)) {
            throw new IllegalArgumentException("the system store's seeds go to the graph's system database, not "
                    + system.database());
        }
        var cadence = com.legend.executionplan.ExecutionPlan.Load.Cadence.PER_TRANSACTION;
        Seeded seeded = ctx.derived(SeedMemo.class, c -> new SeedMemo()).steps.computeIfAbsent(s.type(), engine -> {
            var steps = steps(store(ctx), catalogDialect(engine), false, table -> ownRows(ctx, table));
            return new Seeded(steps.isEmpty() ? "" : LoadPlanner.identity(cadence, steps.steps()), steps);
        });
        return seeded.steps().isEmpty() ? null : seeded.load(cadence, system);
    }

    /** The dialect of the catalog on {@code engine} (a system target's engine, always one the platform runs in process:
     *  {@link #engine}); another refused by name. */
    private static com.legend.sql.dialect.SqlDialect catalogDialect(com.legend.executionplan.DatabaseType engine) {
        com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(ConnectionPlans.model(engine));
        if (!dialect.inProcess()) {
            throw new IllegalArgumentException("a system database on " + engine + ": the process's catalog is made on a"
                    + " database the platform runs in process");
        }
        return dialect;
    }

    /** One engine's steps, with the bundle they read, and their load's identity ({@link LoadPlanner#identity}),
     *  digested once per graph. */
    private record Seeded(String identity, com.legend.setup.LoadSteps steps) {

        com.legend.executionplan.ExecutionPlan.Load load(com.legend.executionplan.ExecutionPlan.Load.Cadence cadence,
                com.legend.executionplan.ExecutionPlan.Target target) {
            return LoadPlanner.load(identity, cadence, steps.steps(), target, steps.content());
        }
    }

    /**
     * THE PLATFORM'S CATALOG (L4b): the system store's rows of a model of no element of its own -- the platform's
     * metamodel classes, the store's own mapping and tables -- the same in every model's rows, unchanged
     * (docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 1.2, the probe {@code SplitCheck}: 0 of them altered in the stress
     * model's), and their content's key, which names the process's catalog on every engine. Constants of the platform,
     * derived once per process when first asked.
     */
    private static final class Platform {
        static final java.util.Map<String, java.util.List<java.util.List<String>>> ROWS;
        static final String KEY;

        static {
            com.legend.compiler.element.ModelContext platform = com.legend.Compiler.compileModel("");
            java.util.Map<String, java.util.List<java.util.List<String>>> rows = new java.util.LinkedHashMap<>();
            for (var schema : store(platform).schemas()) {
                for (var def : schema.tables()) {
                    java.util.List<java.util.List<String>> table = new java.util.ArrayList<>();
                    for (java.util.List<String> row : MetamodelSeeds.rows(def.name(), platform)) {
                        table.add(java.util.Collections.unmodifiableList(new java.util.ArrayList<>(row)));
                    }
                    rows.put(def.name(), java.util.Collections.unmodifiableList(table));
                }
            }
            ROWS = java.util.Collections.unmodifiableMap(rows);
            KEY = "platform:" + hashOf(store(platform), Platform::rows);
        }

        /** The platform's rows of {@code table}; a table the store does not declare refused by name. */
        static java.util.List<java.util.List<String>> rows(String table) {
            return java.util.Objects.requireNonNull(ROWS.get(table), () -> "no system table " + table);
        }

        private Platform() {
        }
    }

    /** {@code ctx}'s own rows of {@code table}: its rows less the platform's, as multisets by content (each platform
     *  row once per its count), in their own order. A platform row the model's rows lack is refused by name: the model
     *  would change the platform's catalog, which a registration of inserts cannot express. */
    private static java.util.List<java.util.List<String>> ownRows(com.legend.compiler.element.ModelContext ctx,
            String table) {
        java.util.Map<java.util.List<String>, Integer> platform = new java.util.HashMap<>();
        for (java.util.List<String> row : Platform.rows(table)) {
            platform.merge(row, 1, Integer::sum);
        }
        java.util.List<java.util.List<String>> own = new java.util.ArrayList<>();
        for (java.util.List<String> row : MetamodelSeeds.rows(table, ctx)) {
            Integer n = platform.get(row);
            if (n == null) {
                own.add(row);
            } else if (n == 1) {
                platform.remove(row);
            } else {
                platform.put(row, n - 1);
            }
        }
        if (!platform.isEmpty()) {
            throw new IllegalStateException("system table " + table + ": the model's rows lack " + platform.size()
                    + " of the platform's, e.g. " + platform.keySet().iterator().next() + " -- a model that changes the"
                    + " platform's catalog has no registration");
        }
        return own;
    }

    static com.legend.model.DatabaseDefinition store(com.legend.compiler.element.ModelContext ctx) {
        return ctx.findDatabase(com.legend.builtin.SystemMetamodel.STORE_FQN).orElseThrow(() ->
                new IllegalStateException("the system metamodel store is not in the model — injection regressed"));
    }

    /** {@code store}'s tables as steps in {@code dialect} (a catalog's, in process), each table's rows {@code rows}
     *  gives, under their own names: with their DDL ({@code Ddl.metamodelSteps}, {@code ddl}), else the rows alone
     *  ({@code Ddl.metamodelRowSteps}). */
    private static com.legend.setup.LoadSteps steps(
            com.legend.model.DatabaseDefinition store, com.legend.sql.dialect.SqlDialect dialect, boolean ddl,
            java.util.function.Function<String, java.util.List<java.util.List<String>>> rows) {
        com.legend.setup.LoadSteps.Builder out = new com.legend.setup.LoadSteps.Builder();
        for (var schema : store.schemas()) {
            for (var def : schema.tables()) {
                java.util.List<java.util.List<String>> table = rows.apply(def.name());
                out.add(ddl ? com.legend.setup.Ddl.metamodelSteps(def, schema.name(), table, dialect)
                        : com.legend.setup.Ddl.metamodelRowSteps(def, schema.name(), table, dialect));
            }
        }
        return out.build();
    }

    static String hashOf(com.legend.model.DatabaseDefinition store,
            java.util.function.Function<String, java.util.List<java.util.List<String>>> rowsOf) {
        StringBuilder content = new StringBuilder();
        for (var schema : store.schemas()) {
            for (var def : schema.tables()) {
                content.append(schema.name()).append('\u0001').append(def).append('\u0001');
                for (java.util.List<String> row : rowsOf.apply(def.name())) {
                    for (String cell : row) {
                        content.append(cell == null ? "\u0000" : cell).append('\u0002');
                    }
                    content.append('\u0003');
                }
            }
        }
        // two 64-bit hashes over the content's characters, in plain long arithmetic: the planner runs in the browser
        // too (TeaVM, //wasm:planner), which has no java.security (the slice 2 lanes, 2026-11-08)
        String text = content.toString();
        long fnv = 0xcbf29ce484222325L;
        long mix = 0x6a09e667f3bcc909L;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            fnv = (fnv ^ c) * 0x100000001b3L;
            mix = (mix + c) * 0x9e3779b97f4a7c15L;
            mix ^= mix >>> 29;
        }
        StringBuilder hex = new StringBuilder(32);
        for (long h : new long[] {fnv, mix}) {
            for (int shift = 60; shift >= 0; shift -= 4) {
                hex.append(Character.forDigit((int) ((h >>> shift) & 0xF), 16));
            }
        }
        return hex.toString();
    }

    /** The store's steps and their identity, per engine, of one graph ({@code ModelContext.derived}): its own
     *  ({@code steps}) and the platform's catalog's ({@code platform}). */
    private static final class SeedMemo {
        private final java.util.Map<com.legend.executionplan.DatabaseType, Seeded> steps =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.Map<com.legend.executionplan.DatabaseType, Seeded> platform =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.Map<com.legend.executionplan.DatabaseType, Seeded> acquired =
                new java.util.concurrent.ConcurrentHashMap<>();
    }

    /**
     * The process's CATALOG as a PRODUCT query on {@code runtime}'s database names it (L4's product half, 2026-11-10,
     * docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.4, the one-request model; B4, 2026-11-11: on every database):
     * {@link #target}'s, with the model's registration -- its own rows, as {@link #seeds} writes them -- as the target's
     * acquisition load ({@code PER_ACQUISITION}), which the catalog's lease runs inside the run's turn, rolled back when
     * the run ends ({@code SystemDatabase.Leases}). A model that adds no row registers nothing.
     */
    public static com.legend.executionplan.ExecutionPlan.Target forQuery(com.legend.compiler.element.ModelContext ctx,
            com.legend.executionplan.DatabaseType runtime) {
        var catalog = target(ctx, runtime);
        com.legend.executionplan.DatabaseType type = engine(runtime);
        var acquisition = com.legend.executionplan.ExecutionPlan.Load.Cadence.PER_ACQUISITION;
        Seeded own = ctx.derived(SeedMemo.class, c -> new SeedMemo()).acquired.computeIfAbsent(type, engine -> {
            var steps = steps(store(ctx), catalogDialect(engine), false, table -> ownRows(ctx, table));
            return new Seeded(steps.isEmpty() ? "" : LoadPlanner.identity(acquisition, steps.steps()), steps);
        });
        if (own.steps().isEmpty()) {
            return catalog;
        }
        java.util.List<com.legend.executionplan.ExecutionPlan.Load> loads = new java.util.ArrayList<>(catalog.loads());
        loads.add(own.load(acquisition, catalog.bare()));
        return new com.legend.executionplan.ExecutionPlan.Target(catalog.database(), catalog.session(), loads,
                catalog.raiseReadBack(), catalog.settings());
    }
}
