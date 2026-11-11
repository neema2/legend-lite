// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * THE SYSTEM CATALOG, planned (L4b, 2026-11-09; docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.1-4.5, the implementation
 * plan of docs/PLAN_AS_DATA_2026_11_07.md, hole 2): the one maker of a target naming a system database and of the system
 * metamodel store's loads. On a database the platform runs in process, the process's catalog -- its tables and the
 * platform's rows one ONCE load, each model's own rows a registration of inserts in the run's transaction; on a server
 * engine, the graph's own store, whole, in its session's temporary tables. Knowledge, derived once per graph and engine;
 * the routing a body takes to it is {@link SystemRoutes}'.
 */
public final class SystemCatalog {

    private SystemCatalog() {
    }

    /**
     * The target naming the system database a program over {@code ctx} reads the metamodel store on, for {@code type}'s
     * engine and {@code session}'s statements (L4b, 2026-11-09, docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.1-4.5): on a
     * database the platform runs in process, the process's CATALOG -- named by the platform's content key, made once with
     * the store's tables and the platform's rows (its ONCE load), each program registering its model's own rows in its
     * run's transaction, rolled back when the run ends ({@link #seeds}); on a server engine, the graph's own system
     * database on the host's system connection, named by the graph's key, its whole store in temporary tables (the
     * design's check 3). The one maker of a system target.
     */
    public static com.legend.executionplan.ExecutionPlan.Target target(com.legend.compiler.element.ModelContext ctx,
            com.legend.executionplan.DatabaseType type, java.util.List<String> session) {
        com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(ConnectionPlans.model(type));
        if (!dialect.inProcess()) {
            return new com.legend.executionplan.ExecutionPlan.Target(
                    new com.legend.executionplan.ExecutionPlan.Database.System(type, SystemRoutes.graphKey(ctx)), session,
                    java.util.List.of());
        }
        var bare = new com.legend.executionplan.ExecutionPlan.Target(
                new com.legend.executionplan.ExecutionPlan.Database.System(type, Platform.KEY), session,
                java.util.List.of());
        var once = com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE;
        Seeded platform = ctx.derived(SeedMemo.class, c -> new SeedMemo()).platform.computeIfAbsent(type, t -> {
            // the catalog's own tables and rows, and the schema a body's data: session tables live in (H2's), made
            // once here, outside any run's transaction: a body routed to the catalog sends no statement that commits
            // the run's registration, which the run rolls back (madeOnce)
            var steps = steps(store(ctx), dialect, true, Platform::rows).then(com.legend.setup.LoadSteps.of(
                    dialect.dataSchema().stream().<com.legend.executionplan.ExecutionPlan.LoadStep>map(
                            com.legend.executionplan.ExecutionPlan.LoadStep.Statement::new).toList()));
            return new Seeded(LoadPlanner.identity(once, steps.steps()), steps);
        });
        return new com.legend.executionplan.ExecutionPlan.Target(bare.database(), session,
                java.util.List.of(platform.load(once, bare)));
    }

    /** Whether {@code target}'s ONCE load -- the process's catalog's, {@link #target} -- sends each of
     *  {@code statements} ({@code SqlDialect.dataSchema}): what a program routed to it need not send itself. */
    static boolean madeOnce(com.legend.executionplan.ExecutionPlan.Target target, java.util.List<String> statements) {
        java.util.Set<String> once = new java.util.HashSet<>();
        for (var load : target.loads()) {
            if (load.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE) {
                for (var step : load.steps()) {
                    if (step instanceof com.legend.executionplan.ExecutionPlan.LoadStep.Statement s) {
                        once.add(s.sql());
                    }
                }
            }
        }
        return once.containsAll(statements);
    }

    /** The graph's system metamodel store's load on {@code system} (a target {@link #target} made), written in
     *  {@code dialect} (the program's): on the process's catalog, the model's REGISTRATION -- its own rows
     *  ({@code MetamodelSeeds} less the platform's), inserts with no DDL, {@code PER_TRANSACTION} -- or null where the
     *  model adds no row; on a server engine's system database, every table of the store, its DDL and the graph's rows,
     *  as the session's temporary tables read by their system names ({@code Ddl.metamodelTemporary}, {@link SystemRoutes#renames}),
     *  a seed ({@code BEFORE_STEP}) the session holds once. Made by the load planner, named by its content as every load
     *  is (L4a, 2026-11-09). Knowledge, derived once per graph and engine. */
    public static com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable Load seeds(
            com.legend.compiler.element.ModelContext ctx, com.legend.executionplan.ExecutionPlan.Target system,
            com.legend.sql.dialect.SqlDialect dialect) {
        if (!(system.database() instanceof com.legend.executionplan.ExecutionPlan.Database.System s)) {
            throw new IllegalArgumentException("the system store's seeds go to the graph's system database, not "
                    + system.database());
        }
        var cadence = dialect.inProcess() ? com.legend.executionplan.ExecutionPlan.Load.Cadence.PER_TRANSACTION
                : com.legend.executionplan.ExecutionPlan.Load.Cadence.BEFORE_STEP;
        Seeded seeded = ctx.derived(SeedMemo.class, c -> new SeedMemo()).steps.computeIfAbsent(s.type(), engine -> {
            var steps = dialect.inProcess() ? steps(store(ctx), dialect, false, table -> ownRows(ctx, table))
                    : steps(store(ctx), dialect, true, table -> MetamodelSeeds.rows(table, ctx));
            return new Seeded(steps.isEmpty() ? "" : LoadPlanner.identity(cadence, steps.steps()), steps);
        });
        return seeded.steps().isEmpty() ? null : seeded.load(cadence, system);
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

    /** {@code store}'s tables as steps in {@code dialect}, each table's rows {@code rows} gives: on a database the platform
     *  runs in process under their own names ({@code Ddl.metamodelSteps}, with {@code ddl}; else the rows alone,
     *  {@code Ddl.metamodelRowSteps}), else as the session's temporary tables ({@code Ddl.metamodelTemporary}). */
    private static com.legend.setup.LoadSteps steps(
            com.legend.model.DatabaseDefinition store, com.legend.sql.dialect.SqlDialect dialect, boolean ddl,
            java.util.function.Function<String, java.util.List<java.util.List<String>>> rows) {
        com.legend.setup.LoadSteps.Builder out = new com.legend.setup.LoadSteps.Builder();
        for (var schema : store.schemas()) {
            for (var def : schema.tables()) {
                java.util.List<java.util.List<String>> table = rows.apply(def.name());
                if (dialect.inProcess()) {
                    out.add(ddl ? com.legend.setup.Ddl.metamodelSteps(def, schema.name(), table, dialect)
                            : com.legend.setup.Ddl.metamodelRowSteps(def, schema.name(), table, dialect));
                } else {
                    for (String statement : com.legend.setup.Ddl.metamodelTemporary(def, schema.name(), table,
                            dialect)) {
                        out.statement(statement);
                    }
                }
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
     * The process's CATALOG as a PRODUCT query names it (L4's product half, 2026-11-10,
     * docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.4, the one-request model): {@link #target}'s, with the model's
     * registration -- its own rows, as {@link #seeds} writes them -- as the target's acquisition load
     * ({@code PER_ACQUISITION}), which the catalog's lease runs inside the run's turn, rolled back when the run ends
     * ({@code SystemDatabase.Leases}). A model that adds no row registers nothing. On a database the platform runs in
     * process only (a server engine's catalog is M5's, not designed).
     */
    public static com.legend.executionplan.ExecutionPlan.Target forQuery(com.legend.compiler.element.ModelContext ctx,
            com.legend.executionplan.DatabaseType type, java.util.List<String> session,
            com.legend.sql.dialect.SqlDialect dialect) {
        if (!dialect.inProcess()) {
            throw new IllegalArgumentException("a product query's catalog on " + type + ": the process's catalog is"
                    + " made on a database the platform runs in process");
        }
        var catalog = target(ctx, type, session);
        var acquisition = com.legend.executionplan.ExecutionPlan.Load.Cadence.PER_ACQUISITION;
        Seeded own = ctx.derived(SeedMemo.class, c -> new SeedMemo()).acquired.computeIfAbsent(type, engine -> {
            var steps = steps(store(ctx), dialect, false, table -> ownRows(ctx, table));
            return new Seeded(steps.isEmpty() ? "" : LoadPlanner.identity(acquisition, steps.steps()), steps);
        });
        if (own.steps().isEmpty()) {
            return catalog;
        }
        java.util.List<com.legend.executionplan.ExecutionPlan.Load> loads = new java.util.ArrayList<>(catalog.loads());
        loads.add(own.load(acquisition, catalog.bare()));
        return new com.legend.executionplan.ExecutionPlan.Target(catalog.database(), catalog.session(), loads);
    }
}
