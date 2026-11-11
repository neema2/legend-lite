// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.cache.Hash;
import com.legend.executionplan.ExecutionPlan;

import java.util.List;

/**
 * THE LOAD PLANNER (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.2 and P2, L2, 2026-11-09): the one place a plan's
 * {@link ExecutionPlan.Load} is made, and the one identity scheme every load takes -- the SHA-256 of a canonical
 * encoding of its steps (each field length-prefixed, a cell's absence marked, a file by its reference's digest, size
 * and format and its delivery), carried in the plan and never recomputed at the run. Equal steps name one load; steps
 * that load differently never do. A target's declared test data ({@link ExecutionPlan.Load.Cadence#PER_ACQUISITION}),
 * a program's seeds ({@code BEFORE_STEP}) and its own loads ({@code AT_STEP}) are all made here.
 */
public final class LoadPlanner {

    private LoadPlanner() {
    }

    /** {@code steps} as a load of {@code cadence} into {@code target}, named by its content ({@link #identity}). */
    public static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target) {
        return new ExecutionPlan.Load(identity(cadence, steps), cadence, steps, target, java.util.Map.of());
    }

    /** {@code steps} as a load of {@code cadence} into {@code target} whose identity was digested already
     *  ({@link #identity}, memoized by its caller: a project's fill, per model). */
    static ExecutionPlan.Load load(String identity, ExecutionPlan.Load.Cadence cadence,
            List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target) {
        return new ExecutionPlan.Load(identity, cadence, steps, target, java.util.Map.of());
    }

    /** {@link #load(String, ExecutionPlan.Load.Cadence, List, ExecutionPlan.Target)} reading {@code content}, the
     *  bundle its CONTENT file steps name (L6c). */
    static ExecutionPlan.Load load(String identity, ExecutionPlan.Load.Cadence cadence,
            List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target, java.util.Map<String, String> content) {
        return new ExecutionPlan.Load(identity, cadence, steps, target, content);
    }

    /** {@code steps} as a load of {@code cadence} into {@code target} reading {@code content}, named by its steps'
     *  content (the digests stand for the bundle's bytes). */
    static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target, java.util.Map<String, String> content) {
        return new ExecutionPlan.Load(identity(cadence, steps), cadence, steps, target, content);
    }

    /** {@code steps} as a load of {@code cadence} into {@code target}, reading their bundle (A2: a table's rows are a
     *  content blob of inserts), named by their content. */
    public static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, com.legend.setup.LoadSteps steps,
            ExecutionPlan.Target target) {
        return load(cadence, steps.steps(), target, steps.content());
    }

    /**
     * A PROJECT'S DATA ROWS as the ONCE load its own database is made with, alone as a plan, or null for a model with
     * none (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "the hosts run the plan's loads"; census
     * M14): the load a server's project plan names its database with (the same steps, content and identity:
     * {@code PlanMaker}'s project fill), for a host that makes a project's database itself -- the tab's DuckDB,
     * Python's -- and runs it there, its statements blob checked against its digest. Until then the host was handed the
     * fill's statements as text ({@code ProjectData.fill}) and ran them outside any plan. A model whose Data rows cannot
     * make the database is refused by name ({@code ProjectData.refusal}).
     */
    public static @com.legend.base.Nullable ExecutionPlan projectData(com.legend.compiler.element.ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, com.legend.executionplan.DatabaseType type) {
        com.legend.setup.LoadSteps fill = com.legend.setup.CsvSeed.planned(
                com.legend.setup.ProjectData.fillSteps(ctx, dialect), dialect);
        if (fill.isEmpty()) {
            return null;
        }
        ExecutionPlan.Target database = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(type,
                new ExecutionPlan.Project(Hash.ofUtf8(com.legend.setup.ProjectData.contentKey(ctx)).hex(), null)),
                dialect.sessionSetup(), List.of());
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.ONCE, fill, database));
    }

    /** Where a host's own file goes: a table of its own (a person's upload), or a model's declared table (a person's
     *  file put into Studio's workspace, over the model's own rows). */
    public sealed interface HostFileInto permits NewTable, DeclaredTable {
    }

    /** A table of the file's own, {@code table} (a person's name, quoted), made or replaced from the file's columns. */
    public record NewTable(String table) implements HostFileInto {
        public NewTable {
            java.util.Objects.requireNonNull(table, "table");
        }
    }

    /** The table {@code schema.table} the Database {@code database} of {@code ctx} declares, made as declared and filled
     *  from the file, each declared column from the file's column of its name. */
    public record DeclaredTable(com.legend.compiler.element.ModelContext ctx, String database, String schema,
            String table) implements HostFileInto {
    }

    /**
     * A HOST'S OWN FILE READ INTO ITS DATABASE, as a plan (B3, 2026-11-11, the orchestrator's go: "uploads and Studio's
     * file puts as planned loads, the file a pointer"; the design's B3 row: "a dropped file is a pointer DuckDB reads"):
     * one {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load -- kept until the person resets it -- whose file step names
     * the host's file by reference ({@link com.legend.executionplan.DataRef.Namespace#HOST}: its name, digest, size and
     * format) and is delivered as its database reads it, the statement's {@code ?} the file as the host's store binds it.
     * A table of its own is made or replaced from the file (its types the reader's: a CSV detected, Parquet's, JSON's
     * records); a declared table is made as the model declares it (its DDL the seed's, a header alone) and filled from
     * the file (a CSV's cells as text, a Parquet's as typed, the database converting each to its column's type). The
     * host runs the plan (engine-client's runLoad); exec has no store of a host's files, so it refuses one by name.
     */
    public static ExecutionPlan hostFile(com.legend.sql.dialect.SqlDialect dialect,
            com.legend.executionplan.DatabaseType type, com.legend.executionplan.DataRef file, HostFileInto into) {
        if (file.namespace() != com.legend.executionplan.DataRef.Namespace.HOST) {
            throw new IllegalArgumentException("a host's file load names a HOST file: " + file);
        }
        com.legend.sql.dialect.HostOperations host = com.legend.sql.dialect.HostOperations.of(dialect);
        List<ExecutionPlan.LoadStep> steps = new java.util.ArrayList<>();
        java.util.Map<String, String> content = java.util.Map.of();
        com.legend.sql.dialect.HostOperation read = switch (into) {
            case NewTable n -> new com.legend.sql.dialect.HostOperation.MakeRelation(
                    com.legend.sql.dialect.HostOperation.Kind.TABLE, com.legend.sql.dialect.HostOperation.Name.of(n.table()),
                    true, null, List.of(), new com.legend.sql.dialect.HostOperation.Source.HostFile(switch (file.format()) {
                        case com.legend.executionplan.DataRef.DetectedCsv d -> com.legend.sql.dialect.HostOperation.FileFormat.CSV;
                        case com.legend.executionplan.DataRef.Parquet p -> com.legend.sql.dialect.HostOperation.FileFormat.PARQUET;
                        case com.legend.executionplan.DataRef.Json j -> com.legend.sql.dialect.HostOperation.FileFormat.JSON;
                        case com.legend.executionplan.DataRef.Csv c -> throw new IllegalArgumentException("a table of a"
                                + " file's own detects a CSV's columns: its format is detectedCsv, not a declared CSV");
                        case com.legend.executionplan.DataRef.Statements st -> throw new IllegalArgumentException(
                                "a host's file is no statements blob");
                    }));
            case DeclaredTable d -> {
                com.legend.model.DatabaseDefinition.TableDefinition def = d.ctx().findTableDefinition(d.database(),
                        "default".equals(d.schema()) ? d.table() : d.schema() + "." + d.table()).orElseThrow(() ->
                        new IllegalArgumentException("a file fills " + d.schema() + "." + d.table() + ", which the"
                                + " Database " + d.database() + " declares not"));
                // the table as the model declares it, empty: the seed's statements for its header alone
                com.legend.setup.LoadSteps made = com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.steps(
                        d.schema() + "\n" + d.table() + "\n" + String.join(",", def.columns().stream()
                                .map(com.legend.model.DatabaseDefinition.ColumnDefinition::name).toList()),
                        d.database(), d.ctx(), dialect), dialect);
                steps.addAll(made.steps());
                content = made.content();
                yield new com.legend.sql.dialect.HostOperation.Insert(
                        com.legend.sql.dialect.HostOperation.Name.declared(d.schema(), d.table()),
                        def.columns().stream().map(c -> new com.legend.sql.dialect.HostOperation.Column(c.name(), c.name()))
                                .toList(),
                        new com.legend.sql.dialect.HostOperation.Source.HostFile(switch (file.format()) {
                            case com.legend.executionplan.DataRef.Csv c -> com.legend.sql.dialect.HostOperation.FileFormat.CSV_TEXT;
                            case com.legend.executionplan.DataRef.Parquet p -> com.legend.sql.dialect.HostOperation.FileFormat.PARQUET;
                            case com.legend.executionplan.DataRef.DetectedCsv dc -> throw new IllegalArgumentException(
                                    "a declared table's CSV is read as text, cast into its declared columns: its format"
                                            + " is csv, not detectedCsv");
                            case com.legend.executionplan.DataRef.Json j -> throw new IllegalArgumentException(
                                    "a declared table is filled from a .csv or a .parquet file, not JSON");
                            case com.legend.executionplan.DataRef.Statements st -> throw new IllegalArgumentException(
                                    "a host's file is no statements blob");
                        }));
            }
        };
        // the host's own file, read by its database's statements (setup.LoadSteps: the planner's makers make a step)
        steps.addAll(com.legend.setup.LoadSteps.hostFile(file, host.statements(read)).steps());
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(type, null),
                dialect.sessionSetup(), List.of());
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.HOST_SESSION, steps, target, content));
    }

    /** {@code steps} as the loads a target establishes on acquisition: none for no step, else one load into
     *  {@code target}'s own database and session. */
    public static List<ExecutionPlan.Load> targetLoads(com.legend.setup.LoadSteps steps,
            ExecutionPlan.Target target) {
        return steps.isEmpty() ? List.of()
                : List.of(load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, steps, target.bare()));
    }

    /** {@code target} establishing {@code steps} on acquisition (none: the target as it is). */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, com.legend.setup.LoadSteps steps) {
        return new ExecutionPlan.Target(target.database(), target.session(), targetLoads(steps, target));
    }

    /** {@link #withLoads(ExecutionPlan.Target, com.legend.setup.LoadSteps)} for steps that read no blob. */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, List<ExecutionPlan.LoadStep> steps) {
        return withLoads(target, com.legend.setup.LoadSteps.of(steps));
    }

    /** A load's name: the SHA-256 of its canonical content ({@link ExecutionPlan.Load#canonical}: its cadence and its
     *  steps' content), so equal loads share it and loads that run differently never do. Exec recomputes it before it
     *  runs a load and refuses one whose identity is not its content's (the M1 tenets audit's note, 2026-11-09). */
    public static String identity(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps) {
        return ExecutionPlan.Load.IDENTITY_PREFIX + Hash.ofUtf8(ExecutionPlan.Load.canonical(cadence, steps)).hex();
    }

    /** The test data {@code runtime}'s connections declare, as load steps final for {@code dialect}'s database: a plan's
     *  target's acquisition load. Its steps are {@code CsvSeed.declaredSteps}', which {@link #seeds} plans too, beside a
     *  statement's other seeds, in one load (B2a, 2026-11-11: until then the target's in {@code PlanMaker}, the seeds'
     *  in the test planner's {@code Seeds.planned}; the B2a audit's note: the javadoc said seeds called this). */
    public static com.legend.setup.LoadSteps declared(String runtime,
            com.legend.compiler.element.ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        return com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect), dialect);
    }

    /**
     * {@code statement}'s SEEDS as a program's load steps, final for {@code dialect}'s database (B2a, 2026-11-11: the
     * load planner's, moved from the test planner's {@code Seeds.planned}), with the referee's own copy of them, written
     * in {@code referee}'s dialect (A2; the edge's, until B2b replays the plan's loads): each {@code from()}'s inline test
     * data and named runtime's declared data, then the program's {@code runtime}'s -- each runtime's once, and each
     * from()'s inline data once (the correctness audit's C3, 2026-11-10: a from()'s runtime that is the program's too was
     * planned twice, its setup SQL failing "already exists", its rows doubled; the A delta audit's note 10, 2026-11-11:
     * two from()s of one instance runtime too).
     */
    public static com.legend.setup.CsvSeed.Planned seeds(com.legend.compiler.spec.typed.TypedSpec statement,
            @com.legend.base.Nullable String runtime, com.legend.compiler.element.ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, com.legend.sql.dialect.SqlDialect referee) {
        List<com.legend.setup.CsvSeed.Step> setups = new java.util.ArrayList<>();
        java.util.Set<String> declared = new java.util.HashSet<>();
        java.util.Set<List<Object>> inline = new java.util.HashSet<>();
        for (com.legend.compiler.spec.typed.TypedFrom fr : com.legend.compiler.spec.typed.ExecutionContext.froms(
                statement)) {
            // (the setup SQL's language asked only where there is setup SQL: a connection with none may declare none)
            if (inline.add(List.of(fr.sqlSetups(), fr.csvSetups(),
                    fr.sqlSetups().isEmpty() ? "" : String.valueOf(fr.context().rawLanguage())))) {
                for (String sql : fr.sqlSetups()) {
                    // written in the database the from()'s connection declares (decision D2)
                    setups.add(new com.legend.setup.CsvSeed.Step.Sql(sql, fr.context().rawLanguage()));
                }
                setups.addAll(com.legend.setup.CsvSeed.setupSteps(fr, ctx, dialect));
            }
            // an ELEMENT runtime named by the from(): its declared connections' test data (the instance form above
            // carries the same facts on the instance)
            fr.context().runtime().ifPresent(ref -> {
                if (declared.add(ref.fullPath())) {
                    setups.addAll(com.legend.setup.CsvSeed.declaredSteps(ref.fullPath(), ctx, dialect));
                }
            });
        }
        // the program's runtime: its declared connections' test data seeds the session the same way
        if (runtime != null && declared.add(runtime)) {
            setups.addAll(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect));
        }
        return com.legend.setup.CsvSeed.planned(setups, dialect, referee);
    }
}
